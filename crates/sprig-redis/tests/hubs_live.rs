//! Pub/sub and blocked-client wake-ups across two gateways (chapter 40,
//! "Pub/sub and keyspace notifications", "Blocking commands"): the hubs
//! over the peer service.
//!
//! - `SPRIG_TEST_REDIS_SPRIG`, `SPRIG_TEST_REDIS_SPRIG_B`: the same
//!   tenant's login on two gateways' Redis listeners
//!   (`redis://acme.app:pw@127.0.0.1:23380`, `...:23390`).
//! - `SPRIG_TEST_REDIS_SPRIG2`: another tenant on the first gateway
//!   (isolation).
//!
//! Skipped when unset. Uses keys `r6hub:*`, removed at the end.

#![allow(
    clippy::format_push_string,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::type_complexity,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::items_after_statements,
    clippy::needless_update,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_truncation
)]

use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

/// A reply, RESP2.
#[derive(Debug, Clone, PartialEq, Eq)]
enum R {
    S(String),
    E(String),
    I(i64),
    B(Option<Vec<u8>>),
    A(Option<Vec<R>>),
}

impl R {
    fn text(&self) -> String {
        match self {
            Self::S(s) | Self::E(s) => s.clone(),
            Self::B(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Self::I(i) => i.to_string(),
            other => format!("{other:?}"),
        }
    }
}

/// A raw RESP2 connection.
struct Raw {
    r: BufReader<tokio::net::tcp::OwnedReadHalf>,
    w: tokio::net::tcp::OwnedWriteHalf,
}

impl Raw {
    async fn open(url: &str) -> Self {
        let body = url.trim_start_matches("redis://");
        let (userinfo, host) = body.rsplit_once('@').unwrap();
        let (u, p) = userinfo.split_once(':').unwrap();
        let s = TcpStream::connect(host).await.unwrap();
        s.set_nodelay(true).unwrap();
        let (r, w) = s.into_split();
        let mut c = Self {
            r: BufReader::new(r),
            w,
        };
        assert_eq!(c.cmd(&["AUTH", u, p]).await, R::S("OK".into()));
        c
    }

    async fn send(&mut self, args: &[&str]) {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{a}\r\n", a.len()));
        }
        self.w.write_all(out.as_bytes()).await.unwrap();
    }

    async fn line(&mut self) -> String {
        let mut l = String::new();
        self.r.read_line(&mut l).await.unwrap();
        l.trim_end_matches("\r\n").to_owned()
    }

    async fn read(&mut self) -> R {
        let l = self.line().await;
        let (t, rest) = l.split_at(1);
        match t {
            "+" => R::S(rest.into()),
            "-" => R::E(rest.into()),
            ":" => R::I(rest.parse().unwrap()),
            "$" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return R::B(None);
                }
                let mut b = vec![0u8; n as usize + 2];
                tokio::io::AsyncReadExt::read_exact(&mut self.r, &mut b)
                    .await
                    .unwrap();
                b.truncate(n as usize);
                R::B(Some(b))
            }
            "*" => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return R::A(None);
                }
                let mut v = Vec::new();
                for _ in 0..n {
                    v.push(Box::pin(self.read()).await);
                }
                R::A(Some(v))
            }
            other => panic!("unexpected reply {other}{rest}"),
        }
    }

    async fn cmd(&mut self, args: &[&str]) -> R {
        self.send(args).await;
        self.read().await
    }

    async fn read_within(&mut self, d: Duration) -> Option<R> {
        tokio::time::timeout(d, self.read()).await.ok()
    }
}

fn texts(r: &R) -> Vec<String> {
    match r {
        R::A(Some(v)) => v.iter().map(R::text).collect(),
        other => vec![other.text()],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_and_wakeups_across_gateways() {
    let (Some(a), Some(b)) = (
        var("SPRIG_TEST_REDIS_SPRIG"),
        var("SPRIG_TEST_REDIS_SPRIG_B"),
    ) else {
        eprintln!("SPRIG_TEST_REDIS_SPRIG(_B) not set: skipped");
        return;
    };
    // Subscribers: two on gateway B (a channel, a pattern), one on A.
    let mut sub_b1 = Raw::open(&b).await;
    let r = sub_b1.cmd(&["SUBSCRIBE", "r6hub:news"]).await;
    assert_eq!(texts(&r), ["subscribe", "r6hub:news", "1"]);
    let mut sub_b2 = Raw::open(&b).await;
    let r = sub_b2.cmd(&["PSUBSCRIBE", "r6hub:n*"]).await;
    assert_eq!(texts(&r), ["psubscribe", "r6hub:n*", "1"]);
    let mut sub_a = Raw::open(&a).await;
    let r = sub_a.cmd(&["SUBSCRIBE", "r6hub:news"]).await;
    assert_eq!(texts(&r), ["subscribe", "r6hub:news", "1"]);
    let mut sub_bs = Raw::open(&b).await;
    let r = sub_bs.cmd(&["SSUBSCRIBE", "r6hub:{orders}"]).await;
    assert_eq!(texts(&r), ["ssubscribe", "r6hub:{orders}", "1"]);
    // Another tenant's subscriber of the same name hears nothing.
    let mut other = match var("SPRIG_TEST_REDIS_SPRIG2") {
        Some(u) => {
            let mut o = Raw::open(&u).await;
            o.cmd(&["SUBSCRIBE", "r6hub:news"]).await;
            Some(o)
        }
        None => None,
    };

    let mut pubc = Raw::open(&a).await;
    let mut publish_ms = Vec::new();
    for i in 0..20 {
        let t = Instant::now();
        let n = pubc.cmd(&["PUBLISH", "r6hub:news", &format!("m{i}")]).await;
        publish_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(n, R::I(3), "receivers of message {i}");
    }
    for i in 0..20 {
        let m = sub_b1.read_within(Duration::from_secs(5)).await.unwrap();
        assert_eq!(texts(&m), ["message", "r6hub:news", &format!("m{i}")]);
        let m = sub_b2.read_within(Duration::from_secs(5)).await.unwrap();
        assert_eq!(
            texts(&m),
            ["pmessage", "r6hub:n*", "r6hub:news", &format!("m{i}")]
        );
        let m = sub_a.read_within(Duration::from_secs(5)).await.unwrap();
        assert_eq!(texts(&m), ["message", "r6hub:news", &format!("m{i}")]);
    }
    if let Some(o) = &mut other {
        assert!(o.read_within(Duration::from_millis(300)).await.is_none());
    }
    // Sharded channels have their own hubs.
    assert_eq!(
        pubc.cmd(&["SPUBLISH", "r6hub:{orders}", "o1"]).await,
        R::I(1)
    );
    let m = sub_bs.read_within(Duration::from_secs(5)).await.unwrap();
    assert_eq!(texts(&m), ["smessage", "r6hub:{orders}", "o1"]);
    // PUBSUB sees every gateway.
    let r = pubc.cmd(&["PUBSUB", "NUMSUB", "r6hub:news"]).await;
    assert_eq!(texts(&r), ["r6hub:news", "2"]);
    assert_eq!(pubc.cmd(&["PUBSUB", "NUMPAT"]).await, R::I(1));
    let r = pubc.cmd(&["PUBSUB", "CHANNELS", "r6hub:*"]).await;
    assert_eq!(texts(&r), ["r6hub:news"]);
    let r = pubc.cmd(&["PUBSUB", "SHARDCHANNELS"]).await;
    assert!(texts(&r).contains(&"r6hub:{orders}".to_owned()));
    // Unsubscribing on B: the hub stops sending there.
    let r = sub_b1.cmd(&["UNSUBSCRIBE", "r6hub:news"]).await;
    assert_eq!(texts(&r), ["unsubscribe", "r6hub:news", "0"]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(pubc.cmd(&["PUBLISH", "r6hub:news", "last"]).await, R::I(2));

    // A client blocked on B is woken by a push on A, well before the
    // safety poll (redis.block_poll_ms, 1 s). First, each gateway makes
    // sure database 0's tables exist (first use runs DDL).
    let mut warm_b = Raw::open(&b).await;
    warm_b.cmd(&["EXISTS", "r6hub:warm"]).await;
    pubc.cmd(&["EXISTS", "r6hub:warm"]).await;
    let mut wake_ms = Vec::new();
    for round in 0..5 {
        let mut blocked = Raw::open(&b).await;
        let key = format!("r6hub:q{round}");
        blocked.send(&["BLPOP", &key, "10"]).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let t = Instant::now();
        assert_eq!(pubc.cmd(&["RPUSH", &key, "job"]).await, R::I(1));
        let r = blocked.read_within(Duration::from_secs(5)).await.unwrap();
        wake_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(texts(&r), [key.as_str(), "job"]);
    }
    // BZPOPMIN woken by ZADD, the other way round.
    let mut pub_b = Raw::open(&b).await;
    let mut push_ms = Vec::new();
    for round in 0..3 {
        let mut blocked = Raw::open(&a).await;
        blocked.send(&["BZPOPMIN", "r6hub:z", "10"]).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let t = Instant::now();
        assert_eq!(pub_b.cmd(&["ZADD", "r6hub:z", "1.5", "m"]).await, R::I(1));
        push_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        let r = blocked.read_within(Duration::from_secs(5)).await.unwrap();
        wake_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(texts(&r), ["r6hub:z", "m", "1.5"], "round {round}");
    }
    eprintln!(
        "ZADD replies: {:?} ms",
        push_ms.iter().map(|x| x.round()).collect::<Vec<_>>()
    );
    publish_ms.sort_by(f64::total_cmp);
    eprintln!(
        "publish across gateways: median {:.1} ms, max {:.1} ms; wake-ups: {:?} ms",
        publish_ms[publish_ms.len() / 2],
        publish_ms[publish_ms.len() - 1],
        wake_ms.iter().map(|x| x.round()).collect::<Vec<_>>()
    );
    for (i, w) in wake_ms.iter().enumerate() {
        assert!(*w < 900.0, "wake-up {i} took {w} ms: the poll, not the hub");
    }
    let mut keys = vec!["DEL".to_owned(), "r6hub:z".to_owned()];
    keys.extend((0..5).map(|r| format!("r6hub:q{r}")));
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    pubc.cmd(&refs).await;
}
