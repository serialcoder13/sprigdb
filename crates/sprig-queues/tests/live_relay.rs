//! Live tests through two gateways of one DC (the queue owner's
//! `QueuePeer` relay on 7403), messages in MariaDB.
//!
//! ```text
//! SPRIG_AMQP_ADDR=127.0.0.1:43672 SPRIG_AMQP_ADDR2=127.0.0.1:43682 \
//! SPRIG_AMQP_USER=acme.app SPRIG_AMQP_PASSWORD=sprig-dev-app \
//! SPRIG_ETCD=http://127.0.0.1:43379 \
//! SPRIG_KILL_GW1='kill -9 $(cat <dir>/gateway.pid)' \
//! SPRIG_START_GW1='deploy/compose/gateway-dev.sh restart' \
//! SPRIG_KILL_GW2='kill -9 $(cat <dir>/gw2.pid)' \
//! SPRIG_START_GW2='source <dir>/env.sh; gw2' \
//! cargo test --release -p sprig-queues --test live_relay -- --nocapture --test-threads 1
//! ```
//!
//! Without `SPRIG_AMQP_ADDR2` every test returns at once.
//!
//! - publish on one gateway, consume, get, purge and delete on the other;
//! - `kill -9` of the owner's gateway while a publisher and a consumer on
//!   the other gateway keep going: every confirmed message arrives, none
//!   twice without `redelivered`;
//! - direct reply-to and an exclusive reply queue across the gateways;
//! - throughput through the gateway that doesn't own the queue
//!   (`SPRIG_THROUGHPUT=n` messages; release build).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions, QueueDeleteOptions, QueuePurgeOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn uniq(p: &str) -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("r9queues2_{p}_{t}")
}

fn addrs() -> Option<[String; 2]> {
    Some([var("SPRIG_AMQP_ADDR")?, var("SPRIG_AMQP_ADDR2")?])
}

async fn connect(addr: &str) -> Connection {
    let (u, p) = (
        var("SPRIG_AMQP_USER").unwrap(),
        var("SPRIG_AMQP_PASSWORD").unwrap(),
    );
    for _ in 0..240 {
        if let Ok(c) = Connection::connect(
            &format!("amqp://{u}:{p}@{addr}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("can't connect to {addr}");
}

fn sh(cmd: &str) {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let st = std::process::Command::new("bash")
        .arg("-c")
        .arg(cmd)
        .current_dir(root)
        .status()
        .unwrap();
    assert!(st.success(), "{cmd}: {st}");
}

async fn declare(ch: &Channel, q: &str) {
    ch.queue_declare(
        q.into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
}

async fn publish(ch: &Channel, q: &str, body: &str, id: &str) -> bool {
    match ch
        .basic_publish(
            "".into(),
            q.into(),
            BasicPublishOptions::default(),
            body.as_bytes(),
            BasicProperties::default()
                .with_delivery_mode(2)
                .with_message_id(id.into()),
        )
        .await
    {
        Ok(c) => matches!(c.await, Ok(c) if c.is_ack()),
        Err(_) => false,
    }
}

/// The gateway holding the newest queue-owner lease (the queue a test
/// just declared and published to), from etcd.
fn newest_owner() -> Option<String> {
    let etcd = var("SPRIG_ETCD")?;
    let b64 = |s: &str| {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(s)
    };
    let body = format!(
        "{{\"key\":\"{}\",\"range_end\":\"{}\"}}",
        b64("/sprig/dc/dc-a/queue-owners/"),
        b64("/sprig/dc/dc-a/queue-owners0")
    );
    let out = std::process::Command::new("curl")
        .args([
            "-fsS",
            "-X",
            "POST",
            &format!("{etcd}/v3/kv/range"),
            "-d",
            &body,
        ])
        .output()
        .ok()?;
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    let kvs = v.get("kvs")?.as_array()?;
    let newest = kvs
        .iter()
        .filter(|k| {
            // Queue leases only: the forwarders' objects have bit 62 set.
            let key = k.get("key").and_then(|x| x.as_str()).unwrap_or_default();
            let key = String::from_utf8(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key)
                    .unwrap_or_default(),
            )
            .unwrap_or_default();
            key.rsplit('/')
                .next()
                .and_then(|o| o.parse::<u64>().ok())
                .is_some_and(|o| o < (1 << 62))
        })
        .max_by_key(|k| {
            k.get("create_revision")
                .and_then(|r| r.as_str())
                .and_then(|r| r.parse::<u64>().ok())
                .unwrap_or(0)
        })?;
    let val = newest.get("value")?.as_str()?;
    let raw = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, val).ok()?;
    let j: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    Some(j.get("gateway_id")?.as_str()?.to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_on_one_gateway_consume_on_the_other() {
    let Some([a, b]) = addrs() else { return };
    let ca = connect(&a).await;
    let cb = connect(&b).await;
    let pa = ca.create_channel().await.unwrap();
    let q = uniq("cross");
    declare(&pa, &q).await;
    pa.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for i in 0..200 {
        assert!(publish(&pa, &q, &format!("m-{i}"), &format!("m-{i}")).await);
    }
    eprintln!("owner of {q}: {:?}", newest_owner());
    let cbh = cb.create_channel().await.unwrap();
    let g = cbh
        .basic_get(q.as_str().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*g.data, b"m-0");
    g.ack(BasicAckOptions::default()).await.unwrap();
    cbh.basic_qos(50, BasicQosOptions::default()).await.unwrap();
    let mut consumer = cbh
        .basic_consume(
            q.as_str().into(),
            "c".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    for i in 1..200 {
        let d = tokio::time::timeout(Duration::from_secs(20), consumer.next())
            .await
            .expect("delivery in time")
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&d.data), format!("m-{i}"));
        assert!(!d.redelivered);
        d.ack(BasicAckOptions::default()).await.unwrap();
    }
    // The other way round, with purge and delete.
    let pb = cb.create_channel().await.unwrap();
    pb.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let q2 = uniq("cross2");
    declare(&pb, &q2).await;
    for i in 0..30 {
        assert!(publish(&pb, &q2, &format!("x-{i}"), &format!("x-{i}")).await);
    }
    let pa2 = ca.create_channel().await.unwrap();
    assert_eq!(
        pa2.queue_purge(q2.as_str().into(), QueuePurgeOptions::default())
            .await
            .unwrap(),
        30
    );
    pa2.queue_delete(q2.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    let pb2 = cb.create_channel().await.unwrap();
    let n = pb2
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 0);
    let _ = ca.close(200, "bye".into()).await;
    let _ = cb.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpc_across_gateways() {
    let Some([a, b]) = addrs() else { return };
    let ca = connect(&a).await;
    let cb = connect(&b).await;
    let rpc = uniq("rpc");
    let req = ca.create_channel().await.unwrap();
    declare(&req, &rpc).await;
    let resp = cb.create_channel().await.unwrap();
    let mut requests = resp
        .basic_consume(
            rpc.as_str().into(),
            "server".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let resp2 = resp.clone();
    tokio::spawn(async move {
        while let Some(Ok(d)) = requests.next().await {
            let to = d.properties.reply_to().clone().unwrap();
            let mut body = b"re:".to_vec();
            body.extend_from_slice(&d.data);
            let _ = resp2
                .basic_publish(
                    "".into(),
                    to.as_str().into(),
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default(),
                )
                .await;
        }
    });
    let mut replies = req
        .basic_consume(
            "amq.rabbitmq.reply-to".into(),
            "rr".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let t0 = Instant::now();
    for i in 0..20 {
        req.basic_publish(
            "".into(),
            rpc.as_str().into(),
            BasicPublishOptions::default(),
            format!("q{i}").as_bytes(),
            BasicProperties::default().with_reply_to("amq.rabbitmq.reply-to".into()),
        )
        .await
        .unwrap();
        let d = tokio::time::timeout(Duration::from_secs(10), replies.next())
            .await
            .expect("direct reply in time")
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&d.data), format!("re:q{i}"));
    }
    eprintln!(
        "direct reply-to across gateways: 20 round trips, {:.1} ms each",
        t0.elapsed().as_secs_f64() * 50.0
    );
    let rq = req
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let rname = rq.name().to_string();
    let mut mine = req
        .basic_consume(
            rname.as_str().into(),
            "mine".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let t0 = Instant::now();
    for i in 0..20 {
        req.basic_publish(
            "".into(),
            rpc.as_str().into(),
            BasicPublishOptions::default(),
            format!("t{i}").as_bytes(),
            BasicProperties::default().with_reply_to(rname.as_str().into()),
        )
        .await
        .unwrap();
        let d = tokio::time::timeout(Duration::from_secs(10), mine.next())
            .await
            .expect("queue reply in time")
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&d.data), format!("re:t{i}"));
    }
    eprintln!(
        "exclusive reply queue across gateways: 20 round trips, {:.1} ms each",
        t0.elapsed().as_secs_f64() * 50.0
    );
    let del = cb.create_channel().await.unwrap();
    del.queue_delete(rpc.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    let _ = ca.close(200, "bye".into()).await;
    let _ = cb.close(200, "bye".into()).await;
}

/// Publishes numbered messages on a gateway, reconnecting and republishing
/// what isn't confirmed (a new message id per attempt).
async fn publisher(addr: String, q: String, n: u32) -> HashSet<u32> {
    let mut confirmed = HashSet::new();
    let mut conn = connect(&addr).await;
    let mut ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let (mut i, mut attempt) = (0u32, 0u32);
    while i < n {
        attempt += 1;
        if publish(&ch, &q, &i.to_string(), &format!("{i}.{attempt}")).await {
            confirmed.insert(i);
            i += 1;
            continue;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !ch.status().connected() {
            if !conn.status().connected() {
                conn = connect(&addr).await;
            }
            ch = conn.create_channel().await.unwrap();
            ch.confirm_select(ConfirmSelectOptions::default())
                .await
                .unwrap();
        }
    }
    let _ = conn.close(200, "bye".into()).await;
    confirmed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_the_owner_mid_stream_clients_on_the_other_gateway_lose_nothing() {
    let Some([a, b]) = addrs() else { return };
    let (Some(kill1), Some(start1), Some(kill2), Some(start2)) = (
        var("SPRIG_KILL_GW1"),
        var("SPRIG_START_GW1"),
        var("SPRIG_KILL_GW2"),
        var("SPRIG_START_GW2"),
    ) else {
        eprintln!("skipped: SPRIG_KILL_GW1/2 and SPRIG_START_GW1/2 not set");
        return;
    };
    let q = uniq("kill");
    {
        let c = connect(&a).await;
        let ch = c.create_channel().await.unwrap();
        declare(&ch, &q).await;
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        assert!(publish(&ch, &q, "warmup", "warmup").await);
        let _ = c.close(200, "bye".into()).await;
    }
    let owner = newest_owner().unwrap();
    // Clients on the gateway that doesn't own the queue.
    let (here, kill, start) = if owner == "gw-1" {
        (b.clone(), kill1, start1)
    } else {
        (a.clone(), kill2, start2)
    };
    eprintln!("owner {owner}; clients on {here}");
    let total = 3000u32;
    let publisher = tokio::spawn(publisher(here.clone(), q.clone(), total));
    let cc = connect(&here).await;
    let ch = cc.create_channel().await.unwrap();
    ch.basic_qos(50, BasicQosOptions::default()).await.unwrap();
    let mut consumer = ch
        .basic_consume(
            q.as_str().into(),
            "c".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut seen: HashMap<String, Vec<bool>> = HashMap::new();
    let mut numbers = HashSet::new();
    let (mut killed_at, mut deliveries, mut redelivered) = (None, 0u64, 0u64);
    let mut gap = Duration::ZERO;
    let mut last = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(240);
    while numbers.len() < total as usize + 1 && Instant::now() < deadline {
        let Ok(Some(Ok(d))) = tokio::time::timeout(Duration::from_secs(5), consumer.next()).await
        else {
            continue;
        };
        if killed_at.is_some() {
            gap = gap.max(last.elapsed());
        }
        last = Instant::now();
        deliveries += 1;
        if d.redelivered {
            redelivered += 1;
        }
        let mid = d
            .properties
            .message_id()
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        seen.entry(mid).or_default().push(d.redelivered);
        numbers.insert(String::from_utf8_lossy(&d.data).into_owned());
        d.ack(BasicAckOptions::default()).await.unwrap();
        if killed_at.is_none() && numbers.len() > total as usize / 3 {
            sh(&kill);
            killed_at = Some(Instant::now());
            eprintln!("killed the owner ({owner}) with kill -9");
        }
    }
    let confirmed = publisher.await.unwrap();
    let missing: Vec<u32> = confirmed
        .iter()
        .filter(|n| !numbers.contains(&n.to_string()))
        .copied()
        .collect();
    eprintln!(
        "{} confirmed, {deliveries} deliveries ({redelivered} redelivered), longest pause after the kill {:.1} s, new owner {:?}",
        confirmed.len(),
        gap.as_secs_f64(),
        newest_owner()
    );
    sh(&start);
    assert!(killed_at.is_some());
    assert!(missing.is_empty(), "confirmed and lost: {missing:?}");
    for (mid, flags) in &seen {
        let fresh = flags.iter().filter(|r| !**r).count();
        assert!(fresh <= 1, "{mid} delivered fresh {fresh} times: {flags:?}");
    }
    let c = connect(&here).await;
    let ch = c.create_channel().await.unwrap();
    let _ = ch
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await;
    let _ = cc.close(200, "bye".into()).await;
    let _ = c.close(200, "bye".into()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throughput_through_the_gateway_without_the_owner() {
    let Some([a, b]) = addrs() else { return };
    let Some(n) = var("SPRIG_THROUGHPUT").and_then(|v| v.parse::<u32>().ok()) else {
        eprintln!("skipped: SPRIG_THROUGHPUT not set");
        return;
    };
    let q = uniq("tput");
    {
        let c = connect(&a).await;
        let ch = c.create_channel().await.unwrap();
        declare(&ch, &q).await;
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        assert!(publish(&ch, &q, "warmup", "warmup").await);
        let g = ch
            .basic_get(q.as_str().into(), BasicGetOptions::default())
            .await
            .unwrap()
            .unwrap();
        g.ack(BasicAckOptions::default()).await.unwrap();
        let _ = c.close(200, "bye".into()).await;
    }
    let owner = newest_owner().unwrap();
    // `SPRIG_THROUGHPUT_ON_OWNER=1`: through the owner's gateway instead,
    // for comparison.
    let on_owner = var("SPRIG_THROUGHPUT_ON_OWNER").is_some();
    let here = if (owner == "gw-1") == on_owner { a } else { b };
    eprintln!("owner {owner}; publishing and consuming through {here}");
    let c = connect(&here).await;
    let ch = c.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let body = vec![b'x'; 128];
    let t0 = Instant::now();
    let mut pending = futures::stream::FuturesUnordered::new();
    for _ in 0..n {
        let conf = ch
            .basic_publish(
                "".into(),
                q.as_str().into(),
                BasicPublishOptions::default(),
                &body,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap();
        pending.push(conf);
        while pending.len() >= 2000 {
            let c = pending.next().await.unwrap().unwrap();
            assert!(c.is_ack());
        }
    }
    while let Some(c) = pending.next().await {
        assert!(c.unwrap().is_ack());
    }
    let pub_rate = f64::from(n) / t0.elapsed().as_secs_f64();
    let cch = c.create_channel().await.unwrap();
    cch.basic_qos(1000, BasicQosOptions::default())
        .await
        .unwrap();
    let mut consumer = cch
        .basic_consume(
            q.as_str().into(),
            "c".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let t0 = Instant::now();
    let mut got = 0u32;
    while got < n {
        let d = tokio::time::timeout(Duration::from_secs(30), consumer.next())
            .await
            .expect("delivery in time")
            .unwrap()
            .unwrap();
        got += 1;
        d.ack(BasicAckOptions::default()).await.unwrap();
    }
    let con_rate = f64::from(n) / t0.elapsed().as_secs_f64();
    eprintln!(
        "through the {} gateway, {n} messages of 128 bytes: {pub_rate:.0} publishes/s with confirms, {con_rate:.0} deliveries/s with acks",
        if on_owner { "owner's" } else { "non-owner" }
    );
    let _ = cch.close(200, "bye".into()).await;
    let ch2 = c.create_channel().await.unwrap();
    let _ = ch2
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await;
    let _ = c.close(200, "bye".into()).await;
}

/// `sprig.mq_*` in another vhost (the optional last argument), and the
/// session's commit hook: an `mq_ack` committed in a transaction is
/// settled at its owner at once (`sprig_queue_acked_total{in_txn="yes"}`
/// on either gateway), not after the owner's 5-second check.
/// `SPRIG_MYSQL_URL` (gateway 1), `SPRIG_METRICS` (both gateways'
/// telemetry addresses, comma-separated); the vhost `mq2` must exist as a
/// database.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_vhost_argument_and_the_commit_hook() {
    use mysql_async::prelude::Queryable;
    let (Some([_, b]), Some(url), Some(metrics)) =
        (addrs(), var("SPRIG_MYSQL_URL"), var("SPRIG_METRICS"))
    else {
        return;
    };
    let acked = || -> f64 {
        metrics
            .split(',')
            .filter_map(|m| {
                let out = std::process::Command::new("curl")
                    .args(["-fsS", &format!("http://{m}/metrics")])
                    .output()
                    .ok()?;
                let text = String::from_utf8_lossy(&out.stdout).into_owned();
                text.lines()
                    .find(|l| l.starts_with("sprig_queue_acked_total{in_txn=\"yes\"}"))
                    .and_then(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            })
            .sum()
    };
    let (u, p) = (
        var("SPRIG_AMQP_USER").unwrap(),
        var("SPRIG_AMQP_PASSWORD").unwrap(),
    );
    let c = Connection::connect(
        &format!("amqp://{u}:{p}@{b}/mq2"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let ch = c.create_channel().await.unwrap();
    let q = uniq("sqlvh");
    declare(&ch, &q).await;
    let mut s = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
        .await
        .unwrap();
    let routed: Option<String> = s
        .query_first(format!(
            "CALL sprig.mq_publish('', '{q}', 'hello', '{{}}', 1, 'mq2')"
        ))
        .await
        .unwrap();
    assert_eq!(routed.as_deref(), Some("1"));
    // Not in vhost `/`: mandatory and unroutable there.
    let e = s
        .query_drop(format!("CALL sprig.mq_publish('', '{q}', 'x', '{{}}', 1)"))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("39255"), "{e}");
    let before = acked();
    s.query_drop("START TRANSACTION").await.unwrap();
    let row: Option<(String, String)> = s
        .query_first(format!("CALL sprig.mq_get('{q}', 1, 60000, 'mq2')"))
        .await
        .unwrap()
        .map(|r: mysql_async::Row| {
            (
                r.get::<String, _>(0).unwrap(),
                r.get::<String, _>(1).unwrap(),
            )
        });
    let (token, body) = row.unwrap();
    assert_eq!(body, "hello");
    s.query_drop(format!("CALL sprig.mq_ack('{token}')"))
        .await
        .unwrap();
    s.query_drop("COMMIT").await.unwrap();
    let t0 = Instant::now();
    while acked() < before + 1.0 {
        assert!(
            t0.elapsed() < Duration::from_secs(4),
            "the owner wasn't told before its own 5 s check"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    eprintln!(
        "commit hook: the owner settled the acknowledgement {} ms after COMMIT",
        t0.elapsed().as_millis()
    );
    let n = ch
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    assert_eq!(n, 0);
    let _ = c.close(200, "bye".into()).await;
}
