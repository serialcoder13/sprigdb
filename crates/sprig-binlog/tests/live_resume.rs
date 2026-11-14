//! Resuming on another node from a GTID position after a
//! failure: reading from a replica with the same GTIDs, switching Galera
//! nodes, and following a promoted replica; and refusing (1236) a position
//! the new node never had.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

mod common;

use std::time::Duration;

use common::{drop_schema, gtid_pos, opts, schema_name, source, sql, urls};
use mysql_async::prelude::Queryable;
use sprig_binlog::{BinlogStream, Event, GtidPos, Value};
use sprig_common::ErrorKind;

/// The ids of the next `n` committed groups of `schema` (all ids if `n` is
/// `None`, until the stream reaches `to`).
async fn read_ids(s: &mut BinlogStream, schema: &str, n: Option<usize>, to: &GtidPos) -> Vec<i64> {
    let mut out = Vec::new();
    let mut pending = Vec::new();
    let mut groups = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if n.is_none() && s.position().reached(to) {
            return out;
        }
        let ev = tokio::time::timeout_at(deadline, s.next())
            .await
            .expect("timed out")
            .unwrap();
        match ev {
            Event::Rows { table, rows, .. } if table.schema == schema => {
                for r in rows {
                    if let Some(Value::Int(id)) = r.after.as_ref().and_then(|i| i.get(0)) {
                        pending.push(*id);
                    }
                }
            }
            Event::Xid(_) if !pending.is_empty() => {
                out.append(&mut pending);
                groups += 1;
                if Some(groups) == n {
                    return out;
                }
            }
            _ => {}
        }
    }
}

fn restrict(p: &GtidPos, domain: Option<u32>) -> GtidPos {
    let mut p = p.clone();
    if let Some(d) = domain {
        p.retain_domains(|x| x == d);
    }
    p
}

async fn wait_reached(url: &str, to: &GtidPos) {
    let mut c = sql(url).await;
    for _ in 0..300 {
        if gtid_pos(&mut c).await.reached(to) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{url} never reached {to}");
}

async fn insert_each(c: &mut mysql_async::Conn, schema: &str, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        c.query_drop(format!("INSERT INTO {schema}.t VALUES ({id})"))
            .await
            .unwrap();
    }
}

/// Writes 30 transactions on `writer`, then reads them in three legs, each
/// leg on the next node, resuming from the previous leg's position.
async fn switch_nodes(writer: &str, nodes: &[String], domain: Option<u32>) {
    let schema = schema_name();
    let mut c = sql(writer).await;
    c.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE TABLE {schema}.t (id INT PRIMARY KEY)"))
        .await
        .unwrap();
    let from = restrict(&gtid_pos(&mut c).await, domain);
    insert_each(&mut c, &schema, 1..=30).await;
    let to = restrict(&gtid_pos(&mut c).await, domain);
    for n in nodes {
        wait_reached(n, &to).await;
    }
    let mut ids = Vec::new();
    let mut pos = from;
    for (leg, node) in nodes.iter().enumerate() {
        let mut s = BinlogStream::connect(&source(node), &pos, &opts(&schema))
            .await
            .unwrap_or_else(|e| panic!("leg {leg} on {node} from {pos}: {e}"));
        let last = leg + 1 == nodes.len();
        let got = read_ids(&mut s, &schema, (!last).then_some(10), &to).await;
        ids.extend(got);
        pos = restrict(s.position(), domain);
    }
    assert_eq!(ids, (1..=30).collect::<Vec<_>>(), "nodes {nodes:?}");
    drop_schema(writer, &schema).await;
}

/// Primary, then replica 1, then replica 2 (and back): the replicas'
/// binlogs (`log_slave_updates`) hold the same GTIDs.
#[tokio::test]
async fn resume_across_primary_and_replicas() {
    let Some(u) = urls(common::REPLICATION, module_path!()) else {
        return;
    };
    let mut nodes = u.clone();
    switch_nodes(&u[0], &nodes, None).await;
    nodes.reverse();
    switch_nodes(&u[0], &nodes, None).await;
}

/// Galera: positions keep only `wsrep_gtid_domain_id`.
#[tokio::test]
async fn resume_across_galera_nodes() {
    let Some(u) = urls(common::GALERA, module_path!()) else {
        return;
    };
    let mut c = sql(&u[0]).await;
    let domain: u32 = c
        .query_first("SELECT @@wsrep_gtid_domain_id")
        .await
        .unwrap()
        .unwrap();
    let nodes = vec![u[1].clone(), u[2].clone(), u[0].clone()];
    switch_nodes(&u[0], &nodes, Some(domain)).await;
}

/// A throwaway primary and replica (`SPRIG_TEST_BINLOG_SCRATCH_PAIRS`:
/// `primary,replica;primary,replica`): the replica is promoted while a
/// reader is mid-stream; the reader continues on it. A position holding a
/// transaction the promoted node never received fails with 1236. The test
/// writes in a fresh GTID domain and repairs replication afterwards.
#[tokio::test]
async fn follow_a_promoted_replica_and_refuse_a_lost_position() {
    let Some(pairs) = std::env::var(PAIRS).ok().filter(|v| !v.is_empty()) else {
        let _ = urls(PAIRS, module_path!());
        return;
    };
    for pair in pairs.split(';') {
        let (primary, replica) = pair.split_once(',').unwrap();
        promote(primary.trim(), replica.trim()).await;
    }
}

const PAIRS: &str = "SPRIG_TEST_BINLOG_SCRATCH_PAIRS";

async fn promote(primary: &str, replica: &str) {
    let schema = schema_name();
    let domain = 1000 + (std::process::id() % 50_000);
    let mut p = sql(primary).await;
    p.query_drop(format!("SET SESSION gtid_domain_id = {domain}"))
        .await
        .unwrap();
    p.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    p.query_drop(format!("CREATE TABLE {schema}.t (id INT PRIMARY KEY)"))
        .await
        .unwrap();
    let from = gtid_pos(&mut p).await;
    insert_each(&mut p, &schema, 1..=10).await;
    let replicated = gtid_pos(&mut p).await;
    wait_reached(replica, &replicated).await;

    // A reader on the primary takes 5 groups.
    let mut s = BinlogStream::connect(&source(primary), &from, &opts(&schema))
        .await
        .unwrap();
    let mut ids = read_ids(&mut s, &schema, Some(5), &replicated).await;
    let mid = s.position().clone();
    drop(s);

    // Failover: the replica stops replicating and takes writes.
    let mut r = sql(replica).await;
    r.query_drop("STOP SLAVE").await.unwrap();
    let lost_from = gtid_pos(&mut p).await;
    p.query_drop(format!("INSERT INTO {schema}.t VALUES (999)"))
        .await
        .unwrap();
    let lost = gtid_pos(&mut p).await;
    r.query_drop(format!("SET SESSION gtid_domain_id = {domain}"))
        .await
        .unwrap();
    insert_each(&mut r, &schema, 11..=13).await;
    let end = gtid_pos(&mut r).await;

    // The reader resumes on the promoted node.
    let mut s = BinlogStream::connect(&source(replica), &mid, &opts(&schema))
        .await
        .unwrap();
    ids.extend(read_ids(&mut s, &schema, None, &end).await);
    assert_eq!(ids, (1..=13).collect::<Vec<_>>(), "{primary} -> {replica}");
    let d = s.position().get(domain).copied().unwrap();
    assert_eq!(d.seq, end.get(domain).unwrap().seq);
    drop(s);

    // A reader that saw the lost transaction can't resume there.
    let mut s = BinlogStream::connect(&source(primary), &lost_from, &opts(&schema))
        .await
        .unwrap();
    assert_eq!(read_ids(&mut s, &schema, Some(1), &lost).await, vec![999]);
    let seen_lost = s.position().clone();
    drop(s);
    let err = match BinlogStream::connect(&source(replica), &seen_lost, &opts(&schema)).await {
        Ok(mut s) => s.next().await.map(|_| ()).unwrap_err(),
        Err(e) => e,
    };
    assert!(
        matches!(err.kind, ErrorKind::Backend { errno: 1236, .. }),
        "{err}"
    );
    eprintln!("{replica}: lossy position refused: {}", err.message);

    // Repair: both nodes diverged in the test's domain, so the replica
    // ignores that domain from now on and replicates the others again.
    let row: mysql_async::Row = r.query_first("SHOW SLAVE STATUS").await.unwrap().unwrap();
    let ignored: String = row
        .get::<Option<String>, _>("Replicate_Ignore_Domain_Ids")
        .flatten()
        .unwrap_or_default();
    let mut list: Vec<String> = ignored
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    list.push(domain.to_string());
    r.query_drop(format!(
        "CHANGE MASTER TO IGNORE_DOMAIN_IDS = ({})",
        list.join(",")
    ))
    .await
    .unwrap();
    r.query_drop("START SLAVE").await.unwrap();
    drop_schema(primary, &schema).await;
    let mut done = gtid_pos(&mut p).await;
    done.retain_domains(|d| d != domain);
    wait_reached(replica, &done).await;
}
