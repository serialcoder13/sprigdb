//! Differential tests against a reference Redis (chapter 40, "Testing"),
//! through a real client (`redis`, BSD-3-Clause).
//!
//! - `SPRIG_TEST_REDIS_REF`: a reference Redis, such as `127.0.0.1:26380`.
//! - `SPRIG_TEST_REDIS_SPRIG`: a SprigDB gateway's Redis listener, as a URL
//!   with a tenant login (`redis://acme.app:pw@127.0.0.1:26379`). When
//!   unset, the tests start SprigDB's Redis front end in process over a
//!   MariaDB given by `SPRIG_TEST_KV_MYSQL` (`mysql://root:pw@127.0.0.1:13301`),
//!   in database `r6redis_kv`, which they drop at the end.
//!
//! Each test is skipped when its variables are unset. Both servers are
//! flushed before each test; never point these at a server holding data.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::wildcard_enum_match_arm,
    clippy::match_wildcard_for_single_variants
)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use redis::Value;
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_redis::{Frame, Limits};
use sprig_redis::session::{Auth, Login};
use sprig_redis::{Config, Env, Session};

struct My(mysql_async::Conn);

fn sql_err(e: &mysql_async::Error) -> SqlError {
    match e {
        mysql_async::Error::Server(s) => SqlError {
            errno: Some(s.code),
            message: s.message.clone(),
        },
        other => SqlError {
            errno: None,
            message: other.to_string(),
        },
    }
}

#[async_trait]
impl SqlRunner for My {
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        let rows: Vec<mysql_async::Row> = self.0.query(sql).await.map_err(|e| sql_err(&e))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect()
            })
            .collect())
    }

    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError> {
        self.0.query_drop(sql).await.map_err(|e| sql_err(&e))?;
        Ok(self.0.affected_rows())
    }
}

struct TestAuth {
    url: String,
}

#[async_trait]
impl Auth for TestAuth {
    async fn login(&self, user: Option<&[u8]>, pass: &[u8]) -> Result<Login, Frame> {
        if pass != b"pw" {
            return Err(sprig_redis::session::wrongpass());
        }
        let opts = mysql_async::Opts::from_url(&self.url).unwrap();
        let conn = mysql_async::Conn::new(opts)
            .await
            .map_err(|e| Frame::err(&e.to_string()))?;
        Ok(Login {
            sql: Box::new(My(conn)),
            scope: "r6redis-test".into(),
            user: user.map_or("default".into(), |u| {
                String::from_utf8_lossy(u).into_owned()
            }),
        })
    }
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

const TEST_DB: &str = "r6redis_kv";

/// The in-process database: `SPRIG_TEST_KV_DB`, else [`TEST_DB`] (agents
/// running the suite at once each set their own).
fn test_db() -> String {
    var("SPRIG_TEST_KV_DB").unwrap_or_else(|| TEST_DB.to_owned())
}

/// The SprigDB URL: a gateway, or an in-process server.
async fn sprig_url() -> Option<String> {
    if let Some(u) = var("SPRIG_TEST_REDIS_SPRIG") {
        return Some(u);
    }
    let mysql = var("SPRIG_TEST_KV_MYSQL")?;
    let env = Arc::new(Env::new(
        Config {
            logical_database: test_db(),
            block_poll: Duration::from_millis(200),
            ..Config::default()
        },
        "r6redis-test",
    ));
    let auth: Arc<dyn Auth> = Arc::new(TestAuth { url: mysql });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, peer)) = listener.accept().await else {
                return;
            };
            let s = Session::new(Arc::clone(&env), Arc::clone(&auth), &peer.to_string());
            tokio::spawn(sprig_redis::conn::serve(
                sock,
                s,
                Limits::default(),
                std::future::pending(),
            ));
        }
    });
    Some(format!("redis://app:pw@{addr}"))
}

async fn drop_test_db() {
    if var("SPRIG_TEST_REDIS_SPRIG").is_some() {
        return;
    }
    if let Some(u) = var("SPRIG_TEST_KV_MYSQL") {
        let mut c = mysql_async::Conn::new(mysql_async::Opts::from_url(&u).unwrap())
            .await
            .unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS {}", test_db()))
            .await
            .unwrap();
        c.disconnect().await.unwrap();
    }
}

async fn conn(url: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// A reply as comparable text.
fn show(r: &redis::RedisResult<Value>) -> String {
    match r {
        Ok(v) => show_v(v),
        Err(e) => format!(
            "ERR[{}] {}",
            e.code().unwrap_or("?"),
            e.detail().unwrap_or("")
        ),
    }
}

fn show_v(v: &Value) -> String {
    match v {
        Value::Nil => "nil".into(),
        Value::Int(i) => format!(":{i}"),
        Value::BulkString(b) => format!("${}", String::from_utf8_lossy(b)),
        Value::SimpleString(s) => format!("+{s}"),
        Value::Okay => "+OK".into(),
        Value::Array(a) | Value::Set(a) => {
            format!("[{}]", a.iter().map(show_v).collect::<Vec<_>>().join(", "))
        }
        Value::Map(m) => format!(
            "{{{}}}",
            m.iter()
                .map(|(k, v)| format!("{}: {}", show_v(k), show_v(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Double(d) => format!(",{d}"),
        Value::Boolean(b) => format!("#{b}"),
        Value::VerbatimString { text, .. } => format!("={text}"),
        Value::BigNumber(n) => format!("({n}"),
        Value::Push { kind, data } => format!(">{kind:?}{}", show_v(&Value::Array(data.clone()))),
        Value::ServerError(e) => format!("ERR[{}] {}", e.code(), e.details().unwrap_or("")),
        other => format!("{other:?}"),
    }
}

/// Sorts an array reply (unordered collections).
fn sorted(v: Value) -> Value {
    match v {
        Value::Array(mut a) | Value::Set(mut a) => {
            a.sort_by_key(show_v);
            Value::Array(a)
        }
        Value::Map(mut m) => {
            m.sort_by_key(|(k, _)| show_v(k));
            Value::Map(m)
        }
        other => other,
    }
}

/// Sorts a flat `[k, v, k, v]` array by pairs.
fn sorted_pairs(v: Value) -> Value {
    match v {
        Value::Array(a) => {
            let mut pairs: Vec<(Value, Value)> = a
                .chunks(2)
                .map(|c| (c[0].clone(), c.get(1).cloned().unwrap_or(Value::Nil)))
                .collect();
            pairs.sort_by_key(|(k, _)| show_v(k));
            Value::Array(pairs.into_iter().flat_map(|(k, v)| [k, v]).collect())
        }
        Value::Map(mut m) => {
            m.sort_by_key(|(k, _)| show_v(k));
            Value::Map(m)
        }
        other => other,
    }
}

/// How a reply is compared.
fn normalize(cmd: &[&str], r: redis::RedisResult<Value>) -> String {
    let name = cmd[0].to_ascii_lowercase();
    let r = r.map(|v| match name.as_str() {
        "smembers" | "sunion" | "sinter" | "sdiff" | "keys" | "hkeys" | "hvals" | "zunion"
            if !cmd.iter().any(|a| a.eq_ignore_ascii_case("withscores")) =>
        {
            sorted(v)
        }
        "hgetall" => sorted_pairs(v),
        "ttl" => match v {
            Value::Int(i) if i > 0 => Value::SimpleString("positive".into()),
            o => o,
        },
        "pttl" => match v {
            Value::Int(i) if i > 0 => Value::Int((i + 500) / 1000),
            o => o,
        },
        "expiretime" | "pexpiretime" => match v {
            Value::Int(i) if i > 0 => Value::SimpleString("future".into()),
            o => o,
        },
        _ => v,
    });
    show(&r)
}

async fn run(c: &mut redis::aio::MultiplexedConnection, cmd: &[&str]) -> redis::RedisResult<Value> {
    let mut q = redis::cmd(cmd[0]);
    for a in &cmd[1..] {
        q.arg(*a);
    }
    q.query_async::<Value>(c).await
}

/// Runs `cmds` on both servers and returns the mismatches.
async fn compare(
    sprig: &mut redis::aio::MultiplexedConnection,
    reference: &mut redis::aio::MultiplexedConnection,
    cmds: &[Vec<&str>],
) -> Vec<String> {
    let mut bad = Vec::new();
    for c in cmds {
        let a = normalize(c, run(sprig, c).await);
        let b = normalize(c, run(reference, c).await);
        if a != b {
            bad.push(format!("{}\n   sprig: {a}\n   redis: {b}", c.join(" ")));
        }
    }
    bad
}

fn split(lines: &str) -> Vec<Vec<&str>> {
    lines
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| l.split(' ').filter(|s| !s.is_empty()).collect())
        .collect()
}

/// Commands with deterministic replies over every type, errors included.
const CORPUS: &str = r"
# strings
SET s1 hello
GET s1
GET nokey
SET s1 world XX GET
SET s1 x NX
SET s2 v NX
SET s2 v2 XX
SET s3 v EX 100
TTL s3
SET s3 v KEEPTTL
TTL s3
SET s3 v PX 100000
PTTL s3
SET s3 v EX 0
SET s3 v EX -1
SET s3 v EX abc
SET s3 v NX XX
SET s3 v EX 10 PX 10
SET s4 v GET
SETNX s1 a
SETNX s5 a
SETEX s6 100 v
SETEX s6 0 v
PSETEX s7 100000 v
GETSET s1 new
GETSET nokey2 a
GETDEL nokey3
GETDEL s7
GET s7
GETEX s1
GETEX s1 EX 50
TTL s1
GETEX s1 PERSIST
TTL s1
GETEX s1 EX 0
MSET a 1 b 2 c 3
MGET a b c nokey
MSET a 1 b
MSETNX a 1 z 2
MSETNX y 1 z 2
MGET y z
APPEND a xyz
APPEND newapp abc
STRLEN a
STRLEN nokey
INCR counter
INCR counter
INCRBY counter 10
DECR counter
DECRBY counter 5
INCRBY counter abc
INCR s1
SET big 9223372036854775807
INCR big
SET neg -9223372036854775808
DECR neg
DECRBY counter -9223372036854775808
INCRBYFLOAT f 1.5
INCRBYFLOAT f 2.25
INCRBYFLOAT f -10
INCRBYFLOAT f abc
INCRBYFLOAT s1 1
SET fl 1e3
INCRBYFLOAT fl 1
SET str abcdefghij
GETRANGE str 0 3
GETRANGE str -3 -1
GETRANGE str 5 100
GETRANGE str 8 2
GETRANGE str -100 -50
GETRANGE nokey 0 5
SUBSTR str 1 2
SETRANGE str 2 XY
GET str
SETRANGE str 15 Z
GET str
SETRANGE nosr 0 ''
SETRANGE str -1 x
SETBIT bits 7 1
SETBIT bits 0 1
GETBIT bits 7
GETBIT bits 100
SETBIT bits 2 2
BITCOUNT bits
BITCOUNT str 1 3
BITCOUNT str 0 10 BIT
BITCOUNT str 1
BITPOS bits 1
BITPOS bits 0
BITPOS str 1 2
BITPOS str 0 0 -1 BIT
BITPOS nokey 0
SET k1 abc
SET k2 abd
BITOP AND dest k1 k2
GET dest
BITOP XOR dest k1 k2 nokey
BITOP NOT dest k1 k2
BITOP NOT dest k1
BITFIELD bf SET u8 0 255 GET u8 0 INCRBY u8 0 10 OVERFLOW SAT INCRBY u8 0 300 OVERFLOW FAIL INCRBY i8 8 200
BITFIELD bf GET i16 #1
BITFIELD_RO bf GET u4 0
LCS k1 k2
LCS k1 k2 LEN
SET ohmytext ohmytext
SET mynewtext mynewtext
LCS ohmytext mynewtext
LCS ohmytext mynewtext IDX MINMATCHLEN 4 WITHMATCHLEN
# keys
EXISTS a b nokey a
DEL a nokey b
EXISTS a b
TYPE s1
TYPE nokey
EXPIRE s1 100
TTL s1
EXPIRE s1 200 NX
EXPIRE s1 50 GT
EXPIRE s1 500 GT
EXPIRE s1 50 LT
EXPIRE s1 50 XX NX
EXPIRE s1 50 GT LT
EXPIRE s1 50 BOGUS
EXPIRE nokey 100
PEXPIRE s1 100000
PTTL s1
PERSIST s1
PERSIST s1
TTL s1
TTL nokey
PTTL nokey
EXPIREAT s1 99999999999
TTL s1
EXPIRETIME s1
PEXPIRETIME s1
EXPIRETIME nokey
EXPIRE s1 -1
EXISTS s1
SET r1 v
RENAME r1 r2
GET r2
RENAME nokey x
RENAMENX r2 s2
RENAMENX r2 r3
COPY r3 r4
COPY r3 r4
COPY r3 r4 REPLACE
COPY r3 r3
GET r4
MOVE r4 1
MOVE r4 1
MOVE r3 0
SELECT 1
GET r4
SELECT 0
TOUCH r3 nokey r3
OBJECT ENCODING counter
OBJECT ENCODING r3
OBJECT ENCODING nokey
OBJECT REFCOUNT r3
# hashes
HSET h f1 v1 f2 v2
HSET h f1 x f3 v3
HSET h f1
HGET h f1
HGET h nof
HGET nokey f
HMGET h f1 nof f2
HGETALL h
HKEYS h
HVALS h
HLEN h
HEXISTS h f1
HEXISTS h nof
HDEL h f2 nof
HDEL h f2
HSETNX h f1 z
HSETNX h f9 z
HINCRBY h cnt 5
HINCRBY h cnt -2
HINCRBY h f1 1
HINCRBYFLOAT h fl 1.5
HINCRBYFLOAT h fl 0.25
HSTRLEN h f1
HSTRLEN h nof
HMSET h a 1 b 2
HGETALL h
HGET s2 f
HSET s2 f v
HDEL h f1 f3 f9 cnt fl a b
EXISTS h
# lists
RPUSH l a b c
LPUSH l z y
LRANGE l 0 -1
LRANGE l 1 2
LRANGE l -2 -1
LRANGE l 5 10
LRANGE l -100 100
LLEN l
LINDEX l 0
LINDEX l -1
LINDEX l 10
LSET l 1 Y
LSET l 10 x
LSET nokey 0 x
LINSERT l BEFORE b NEW
LINSERT l AFTER c END
LINSERT l AFTER zz x
LINSERT l MIDDLE b x
LRANGE l 0 -1
RPUSH l a a b a
LREM l 2 a
LRANGE l 0 -1
LREM l -1 a
LRANGE l 0 -1
LREM l 0 b
LRANGE l 0 -1
LPOS l c
LPOS l nothere
RPUSH l2 a b c a b c a
LPOS l2 a RANK 2
LPOS l2 a RANK -1
LPOS l2 a COUNT 0
LPOS l2 a COUNT 2 RANK 2
LPOS l2 a MAXLEN 3 COUNT 0
LPOS l2 a RANK 0
LTRIM l2 1 4
LRANGE l2 0 -1
LTRIM l2 5 10
EXISTS l2
LPOP l
RPOP l
LPOP l 2
RPOP l 10
LPOP l
LPOP l 0
LPOP nokey 2
RPUSH src 1 2 3
LMOVE src dst LEFT RIGHT
LMOVE src dst RIGHT LEFT
LRANGE dst 0 -1
LMOVE src src LEFT RIGHT
LRANGE src 0 -1
RPOPLPUSH src dst
LMOVE nokey dst LEFT LEFT
LMOVE src dst UP LEFT
LMPOP 2 nokey dst LEFT COUNT 2
LMPOP 1 nokey RIGHT
LMPOP 0 dst LEFT
LPUSHX nolist a
RPUSHX dst a
LPUSH s2 x
BLPOP dst 0.01
BLPOP nokey 0.05
BRPOP dst nokey 0.1
BLMOVE dst dst2 LEFT RIGHT 0.1
BLMPOP 0.05 1 nokey LEFT
BLPOP nokey -1
# sets
SADD st a b c
SADD st a d
SMEMBERS st
SCARD st
SISMEMBER st a
SISMEMBER st z
SMISMEMBER st a z b
SREM st a z
SMEMBERS st
SADD st2 c d e
SINTER st st2
SUNION st st2
SDIFF st st2
SINTER st nokey
SUNION st nokey
SDIFF nokey st
SINTERSTORE st3 st st2
SMEMBERS st3
SUNIONSTORE st3 st st2
SDIFFSTORE st3 st st2
SMEMBERS st3
SINTERSTORE st3 st nokey
EXISTS st3
SINTERCARD 2 st st2
SINTERCARD 2 st st2 LIMIT 1
SINTERCARD 0 st
SMOVE st st2 b
SMOVE st st2 zz
SMEMBERS st2
SADD s2 x
SINTER st s2
SADD ints 1 2 3
OBJECT ENCODING ints
# sorted sets
ZADD z 1 a 2 b 3 c
ZADD z 1.5 a
ZADD z NX 10 a 4 d
ZADD z XX 20 a 30 nope
ZADD z CH 5 a 5 b
ZADD z GT 1 a
ZADD z LT 1 a
ZADD z GT CH 100 a
ZADD z INCR 1 b
ZADD z INCR 1 b 2 c
ZADD z NX XX 1 a
ZADD z GT LT 1 a
ZADD z 1 a 2
ZADD z abc a
ZADD z -inf ninf +inf pinf
ZRANGE z 0 -1 WITHSCORES
ZSCORE z a
ZSCORE z nope
ZMSCORE z a nope b
ZINCRBY z 2.5 c
ZINCRBY z 1 newm
ZCARD z
ZCOUNT z 2 10
ZCOUNT z (2 10
ZCOUNT z -inf +inf
ZCOUNT z abc 1
ZRANK z c
ZREVRANK z c
ZRANK z nope
ZRANK z c WITHSCORE
ZRANGE z 0 2
ZRANGE z -2 -1 WITHSCORES
ZREVRANGE z 0 1 WITHSCORES
ZRANGEBYSCORE z 2 6
ZRANGEBYSCORE z (2 +inf WITHSCORES
ZRANGEBYSCORE z -inf +inf LIMIT 1 2
ZREVRANGEBYSCORE z +inf 3
ZRANGE z 2 6 BYSCORE
ZRANGE z 6 2 BYSCORE REV
ZRANGE z (1 +inf BYSCORE LIMIT 0 2 WITHSCORES
ZRANGE z 0 -1 LIMIT 0 1
ZADD lex 0 a 0 b 0 c 0 d 0 e
ZRANGEBYLEX lex - +
ZRANGEBYLEX lex [b (d
ZRANGEBYLEX lex (b +
ZREVRANGEBYLEX lex + [c
ZRANGE lex [b [d BYLEX
ZRANGE lex - + BYLEX LIMIT 1 2
ZRANGEBYLEX lex b d
ZLEXCOUNT lex [b [d
ZREMRANGEBYLEX lex [a [b
ZRANGE lex 0 -1
ZREMRANGEBYRANK z 0 0
ZREMRANGEBYSCORE z 4 6
ZRANGE z 0 -1 WITHSCORES
ZPOPMIN z
ZPOPMAX z
ZPOPMIN z 2
ZPOPMAX nokey
ZPOPMIN z -1
ZADD z2 1 x 2 y 3 z
ZADD z3 10 y 20 z 30 w
ZUNION 2 z2 z3 WITHSCORES
ZINTER 2 z2 z3 WITHSCORES
ZDIFF 2 z2 z3 WITHSCORES
ZUNION 2 z2 z3 WEIGHTS 2 3 AGGREGATE MAX WITHSCORES
ZINTER 2 z2 z3 AGGREGATE MIN WITHSCORES
ZUNIONSTORE zout 2 z2 z3
ZRANGE zout 0 -1 WITHSCORES
ZINTERSTORE zout 2 z2 z3 WEIGHTS 1 0
ZRANGE zout 0 -1 WITHSCORES
ZDIFFSTORE zout 2 z3 z2
ZRANGE zout 0 -1 WITHSCORES
ZINTERCARD 2 z2 z3
ZUNION 0 z2
ZUNION 2 z2
SADD zs y q
ZUNION 2 z2 zs WITHSCORES
ZRANGESTORE zr z2 0 1
ZRANGE zr 0 -1 WITHSCORES
ZRANGESTORE zr z2 (1 +inf BYSCORE
ZRANGE zr 0 -1
ZMPOP 2 nokey z2 MIN COUNT 2
ZMPOP 1 nokey MAX
BZPOPMIN z3 0.1
BZPOPMAX nokey 0.05
BZMPOP 0.1 1 z3 MAX COUNT 5
ZADD s2 1 x
# sort
RPUSH sl 3 1 2 10
SORT sl
SORT sl DESC
SORT sl ALPHA
SORT sl LIMIT 1 2
SORT sl BY nosort
SORT sl STORE slout
LRANGE slout 0 -1
RPUSH sla b a c
SORT sla
SORT sla ALPHA DESC
# geo
GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania
GEOADD Sicily 200 38 Bad
GEOPOS Sicily Palermo Catania NonExisting
GEODIST Sicily Palermo Catania
GEODIST Sicily Palermo Catania km
GEODIST Sicily Palermo Catania mi
GEODIST Sicily Palermo nope
GEOHASH Sicily Palermo Catania
GEORADIUS Sicily 15 37 200 km WITHDIST ASC
GEORADIUS Sicily 15 37 200 km WITHCOORD WITHDIST WITHHASH ASC
GEORADIUSBYMEMBER Sicily Palermo 200 km ASC
GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC WITHDIST
GEOSEARCH Sicily FROMMEMBER Palermo BYBOX 400 400 km DESC WITHCOORD
GEOSEARCH Sicily FROMLONLAT 15 37 BYBOX 100 100 km
GEOSEARCHSTORE geoout Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC COUNT 1
ZRANGE geoout 0 -1 WITHSCORES
GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 parsecs
# scans on small keys
HSCAN h 0
SSCAN st 0 MATCH c*
ZSCAN z2 0
# misc
PING
PING hello
ECHO hi
DBSIZE
DEL zz1 zz2
UNLINK sl sla
NOSUCHCOMMAND a b
GET
";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corpus_matches_redis() {
    let (Some(refaddr), Some(url)) = (var("SPRIG_TEST_REDIS_REF"), sprig_url().await) else {
        eprintln!("SPRIG_TEST_REDIS_REF or SprigDB not set: skipped");
        return;
    };
    let mut r = conn(&format!("redis://{refaddr}")).await;
    let mut s = conn(&url).await;
    run(&mut r, &["FLUSHALL"]).await.unwrap();
    run(&mut s, &["FLUSHALL"]).await.unwrap();
    let cmds = split(CORPUS);
    let bad = compare(&mut s, &mut r, &cmds).await;
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!("corpus: {} commands, {} mismatches", cmds.len(), bad.len());
    run(&mut s, &["FLUSHALL"]).await.unwrap();
    drop_test_db().await;
    assert!(bad.is_empty(), "{} of {} differ", bad.len(), cmds.len());
}

/// A small deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[(self.next() % xs.len() as u64) as usize]
    }
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
}

/// One random command over a small keyspace of every type.
fn random_cmd(g: &mut Rng) -> Vec<String> {
    let keys = ["k0", "k1", "k2", "k3", "k4", "{t}a", "{t}b", "k7"];
    let vals = ["a", "b", "c", "1", "2", "-3", "10", "x y", ""];
    let k = g.pick(&keys);
    let k2 = g.pick(&keys);
    let val = |g: &mut Rng| g.pick(&vals).to_owned();
    let n = |g: &mut Rng, lo: i64, hi: i64| g.int(lo, hi).to_string();
    let c: Vec<String> = match g.next() % 60 {
        0 => vec!["SET".into(), k.into(), val(g)],
        1 => vec!["GET".into(), k.into()],
        2 => vec!["APPEND".into(), k.into(), val(g)],
        3 => vec!["INCRBY".into(), k.into(), n(g, -5, 5)],
        4 => vec!["DEL".into(), k.into(), k2.into()],
        5 => vec!["EXISTS".into(), k.into(), k2.into()],
        6 => vec!["TYPE".into(), k.into()],
        7 => vec!["HSET".into(), k.into(), val(g), val(g), val(g), val(g)],
        8 => vec!["HDEL".into(), k.into(), val(g), val(g)],
        9 => vec!["HGET".into(), k.into(), val(g)],
        10 => vec!["HGETALL".into(), k.into()],
        11 => vec!["HLEN".into(), k.into()],
        12 => vec!["HINCRBY".into(), k.into(), val(g), n(g, -3, 3)],
        13 => vec!["LPUSH".into(), k.into(), val(g), val(g)],
        14 => vec!["RPUSH".into(), k.into(), val(g)],
        15 => vec!["LPOP".into(), k.into()],
        16 => vec!["RPOP".into(), k.into(), n(g, 0, 3)],
        17 => vec!["LRANGE".into(), k.into(), n(g, -4, 3), n(g, -3, 5)],
        18 => vec!["LINDEX".into(), k.into(), n(g, -4, 4)],
        19 => vec!["LSET".into(), k.into(), n(g, -3, 3), val(g)],
        20 => vec![
            "LINSERT".into(),
            k.into(),
            g.pick(&["BEFORE", "AFTER"]).into(),
            val(g),
            val(g),
        ],
        21 => vec!["LREM".into(), k.into(), n(g, -2, 2), val(g)],
        22 => vec!["LTRIM".into(), k.into(), n(g, -3, 2), n(g, -2, 4)],
        23 => vec!["LLEN".into(), k.into()],
        24 => vec![
            "LMOVE".into(),
            k.into(),
            k2.into(),
            g.pick(&["LEFT", "RIGHT"]).into(),
            g.pick(&["LEFT", "RIGHT"]).into(),
        ],
        25 => vec!["SADD".into(), k.into(), val(g), val(g), val(g)],
        26 => vec!["SREM".into(), k.into(), val(g), val(g)],
        27 => vec!["SMEMBERS".into(), k.into()],
        28 => vec!["SISMEMBER".into(), k.into(), val(g)],
        29 => vec!["SCARD".into(), k.into()],
        30 => vec!["SINTER".into(), k.into(), k2.into()],
        31 => vec!["SUNIONSTORE".into(), k.into(), k.into(), k2.into()],
        32 => vec!["SDIFF".into(), k.into(), k2.into()],
        33 => vec!["SMOVE".into(), k.into(), k2.into(), val(g)],
        34 => vec![
            "ZADD".into(),
            k.into(),
            n(g, -3, 3),
            val(g),
            n(g, -3, 3),
            val(g),
        ],
        35 => vec![
            "ZADD".into(),
            k.into(),
            g.pick(&["NX", "XX", "GT", "LT", "CH"]).into(),
            n(g, -3, 3),
            val(g),
        ],
        36 => vec!["ZINCRBY".into(), k.into(), n(g, -2, 2), val(g)],
        37 => vec!["ZREM".into(), k.into(), val(g)],
        38 => vec![
            "ZRANGE".into(),
            k.into(),
            n(g, -3, 2),
            n(g, -2, 4),
            "WITHSCORES".into(),
        ],
        39 => vec![
            "ZRANGEBYSCORE".into(),
            k.into(),
            g.pick(&["-inf", "(0", "1", "-2"]).into(),
            g.pick(&["+inf", "2", "(3"]).into(),
            "WITHSCORES".into(),
        ],
        40 => vec!["ZRANK".into(), k.into(), val(g)],
        41 => vec!["ZSCORE".into(), k.into(), val(g)],
        42 => vec!["ZCARD".into(), k.into()],
        43 => vec!["ZPOPMIN".into(), k.into(), n(g, 1, 2)],
        44 => vec!["ZREMRANGEBYRANK".into(), k.into(), n(g, -2, 1), n(g, -1, 2)],
        45 => vec![
            "ZUNIONSTORE".into(),
            k.into(),
            "2".into(),
            k.into(),
            k2.into(),
        ],
        46 => vec![
            "ZINTERSTORE".into(),
            k.into(),
            "2".into(),
            k.into(),
            k2.into(),
            "AGGREGATE".into(),
            g.pick(&["SUM", "MIN", "MAX"]).into(),
        ],
        47 => vec!["RENAME".into(), k.into(), k2.into()],
        48 => vec!["COPY".into(), k.into(), k2.into(), "REPLACE".into()],
        49 => vec!["EXPIRE".into(), k.into(), n(g, 100, 200)],
        50 => vec!["PERSIST".into(), k.into()],
        51 => vec!["TTL".into(), k.into()],
        52 => vec!["SETRANGE".into(), k.into(), n(g, 0, 5), val(g)],
        53 => vec!["GETRANGE".into(), k.into(), n(g, -3, 2), n(g, -2, 4)],
        54 => vec!["HSETNX".into(), k.into(), val(g), val(g)],
        55 => vec!["LPOS".into(), k.into(), val(g)],
        56 => vec!["ZRANGEBYLEX".into(), k.into(), "-".into(), "+".into()],
        57 => vec!["SINTERSTORE".into(), k2.into(), k.into(), k2.into()],
        58 => vec!["MSET".into(), k.into(), val(g), k2.into(), val(g)],
        _ => vec!["STRLEN".into(), k.into()],
    };
    c
}

/// Every key's type and content, for comparing final states.
async fn dump(c: &mut redis::aio::MultiplexedConnection) -> Vec<String> {
    let keys = run(c, &["KEYS", "*"]).await.unwrap();
    let Value::Array(keys) = sorted(keys) else {
        panic!()
    };
    let mut out = Vec::new();
    for k in keys {
        let Value::BulkString(k) = k else { continue };
        let k = String::from_utf8_lossy(&k).into_owned();
        let t = show(&run(c, &["TYPE", &k]).await);
        let body = match t.as_str() {
            "+string" => show(&run(c, &["GET", &k]).await),
            "+hash" => show(&Ok(sorted_pairs(run(c, &["HGETALL", &k]).await.unwrap()))),
            "+list" => show(&run(c, &["LRANGE", &k, "0", "-1"]).await),
            "+set" => show(&Ok(sorted(run(c, &["SMEMBERS", &k]).await.unwrap()))),
            "+zset" => show(&run(c, &["ZRANGE", &k, "0", "-1", "WITHSCORES"]).await),
            _ => String::new(),
        };
        out.push(format!("{k} {t} {body}"));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_ops_match_redis() {
    let (Some(refaddr), Some(url)) = (var("SPRIG_TEST_REDIS_REF"), sprig_url().await) else {
        eprintln!("SPRIG_TEST_REDIS_REF or SprigDB not set: skipped");
        return;
    };
    let seeds: Vec<u64> = var("SPRIG_TEST_SEEDS").map_or_else(
        || vec![1, 2, 3],
        |s| s.split(',').filter_map(|x| x.parse().ok()).collect(),
    );
    let ops: usize = var("SPRIG_TEST_OPS")
        .and_then(|s| s.parse().ok())
        .unwrap_or(400);
    let mut r = conn(&format!("redis://{refaddr}")).await;
    let mut s = conn(&url).await;
    let mut total_bad = 0;
    let mut total = 0;
    for seed in seeds {
        run(&mut r, &["FLUSHALL"]).await.unwrap();
        run(&mut s, &["FLUSHALL"]).await.unwrap();
        let mut g = Rng(seed);
        let mut bad = Vec::new();
        let started = std::time::Instant::now();
        for _ in 0..ops {
            let c = random_cmd(&mut g);
            let refs: Vec<&str> = c.iter().map(String::as_str).collect();
            bad.extend(compare(&mut s, &mut r, &[refs]).await);
        }
        let ds = dump(&mut s).await;
        let dr = dump(&mut r).await;
        if ds != dr {
            bad.push(format!(
                "final state differs:\n sprig {ds:?}\n redis {dr:?}"
            ));
        }
        for b in bad.iter().take(20) {
            eprintln!("seed {seed} MISMATCH {b}");
        }
        eprintln!(
            "seed {seed}: {ops} ops in {:.1}s, {} mismatches, {} keys at the end",
            started.elapsed().as_secs_f64(),
            bad.len(),
            ds.len()
        );
        total += ops;
        total_bad += bad.len();
    }
    run(&mut s, &["FLUSHALL"]).await.unwrap();
    drop_test_db().await;
    assert_eq!(total_bad, 0, "{total_bad} of {total} differ");
}

/// Runs a list of commands given as argument vectors on both servers.
async fn compare_args(
    s: &mut redis::aio::MultiplexedConnection,
    r: &mut redis::aio::MultiplexedConnection,
    cmds: &[Vec<String>],
) -> Vec<String> {
    let mut bad = Vec::new();
    for c in cmds {
        let refs: Vec<&str> = c.iter().map(String::as_str).collect();
        bad.extend(compare(s, r, &[refs]).await);
    }
    bad
}

fn v(xs: &[&str]) -> Vec<String> {
    xs.iter().map(|x| (*x).to_owned()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scripts_match_redis() {
    let (Some(refaddr), Some(url)) = (var("SPRIG_TEST_REDIS_REF"), sprig_url().await) else {
        eprintln!("SPRIG_TEST_REDIS_REF or SprigDB not set: skipped");
        return;
    };
    let mut r = conn(&format!("redis://{refaddr}")).await;
    let mut s = conn(&url).await;
    for c in [&mut r, &mut s] {
        run(c, &["FLUSHALL"]).await.unwrap();
        run(c, &["SCRIPT", "FLUSH"]).await.unwrap();
        run(c, &["FUNCTION", "FLUSH"]).await.unwrap();
    }
    let lib = "#!lua name=mylib\n\
               redis.register_function('echo1', function(keys, args) return args[1] end)\n\
               redis.register_function{function_name='getk', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}\n\
               redis.register_function('setk', function(keys, args) return redis.call('SET', keys[1], args[1]) end)";
    let cmds = vec![
        v(&["EVAL", "return 1", "0"]),
        v(&["EVAL", "return {1,2,{3,'x'}}", "0"]),
        v(&["EVAL", "return {1,2,nil,4}", "0"]),
        v(&[
            "EVAL",
            "return redis.call('SET', KEYS[1], ARGV[1])",
            "1",
            "k",
            "v",
        ]),
        v(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]),
        v(&["EVAL", "return redis.call('INCR', KEYS[1])", "1", "cnt"]),
        v(&["EVAL", "return redis.call('INCR', KEYS[1])", "1", "k"]),
        v(&[
            "EVAL",
            "local r = redis.pcall('INCR', KEYS[1]); return r",
            "1",
            "k",
        ]),
        v(&[
            "EVAL",
            "local r = redis.pcall('INCR', KEYS[1]); return type(r)",
            "1",
            "k",
        ]),
        v(&["EVAL", "error('boom')", "0"]),
        v(&["EVAL", "\n\nerror('line three')", "0"]),
        v(&["EVAL", "return redis.error_reply('MY err')", "0"]),
        v(&["EVAL", "return redis.status_reply('FINE')", "0"]),
        v(&["EVAL", "return {err='x'}", "0"]),
        v(&["EVAL", "return true", "0"]),
        v(&["EVAL", "return false", "0"]),
        v(&["EVAL", "return nil", "0"]),
        v(&["EVAL", "return 3.99", "0"]),
        v(&["EVAL", "return -3.99", "0"]),
        v(&["EVAL", "return KEYS[1]..ARGV[1]", "1", "a", "b"]),
        v(&["EVAL", "return #KEYS + #ARGV", "2", "a", "b", "c"]),
        v(&["EVAL", "return redis.sha1hex('')", "0"]),
        v(&[
            "EVAL",
            "return redis.call('HSET', KEYS[1], 'a', 1, 'b', 2.5)",
            "1",
            "h",
        ]),
        v(&["EVAL", "return redis.call('HGET', KEYS[1], 'b')", "1", "h"]),
        v(&[
            "EVAL",
            "return redis.call('LRANGE', KEYS[1], 0, -1)",
            "1",
            "nolist",
        ]),
        v(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "nokey"]),
        v(&[
            "EVAL",
            "return redis.call('ZADD', KEYS[1], 1.5, 'm')",
            "1",
            "z",
        ]),
        v(&[
            "EVAL",
            "return redis.call('ZSCORE', KEYS[1], 'm')",
            "1",
            "z",
        ]),
        v(&[
            "EVAL",
            "redis.setresp(3); return redis.call('ZSCORE', KEYS[1], 'm')",
            "1",
            "z",
        ]),
        v(&["EVAL", "return redis.call('NOSUCH')", "0"]),
        v(&["EVAL", "return redis.call('GET')", "0"]),
        v(&["EVAL", "return redis.call()", "0"]),
        v(&["EVAL", "return redis.call('MULTI')", "0"]),
        v(&["EVAL", "return cjson.encode({1,2,3})", "0"]),
        v(&["EVAL", "return cjson.decode('[1,2,3]')[2]", "0"]),
        v(&["EVAL", "return 1", "-1"]),
        v(&["EVAL", "return 1", "5", "a"]),
        v(&["EVAL", "return 1", "x"]),
        v(&["EVAL", "return '1", "0"]),
        v(&["EVALSHA", "e0e1f9fabfc9d4800c877a703b823ac0578ff8db", "0"]),
        v(&["EVALSHA", "ffffffffffffffffffffffffffffffffffffffff", "0"]),
        v(&[
            "SCRIPT",
            "EXISTS",
            "e0e1f9fabfc9d4800c877a703b823ac0578ff8db",
            "ffff",
        ]),
        v(&["SCRIPT", "LOAD", "return 'loaded'"]),
        v(&["EVALSHA", "3c03ea6af1d4d6dc4b06b5b5b0a4ea8f6c0c7a8d", "0"]),
        v(&[
            "EVAL_RO",
            "return redis.call('SET', KEYS[1], 'x')",
            "1",
            "k",
        ]),
        v(&["EVAL_RO", "return redis.call('GET', KEYS[1])", "1", "k"]),
        v(&[
            "EVAL",
            "#!lua flags=no-writes\nreturn redis.call('SET', KEYS[1], 'x')",
            "1",
            "k",
        ]),
        v(&["EVAL", "#!lua\nreturn redis.call('GET', KEYS[1])", "1", "k"]),
        v(&["EVAL", "#!lua flags=bogus\nreturn 1", "0"]),
        v(&["SCRIPT", "KILL"]),
        v(&["FUNCTION", "LOAD", lib]),
        v(&["FUNCTION", "LOAD", lib]),
        v(&["FCALL", "echo1", "0", "hello"]),
        v(&["FCALL", "setk", "1", "fk", "fv"]),
        v(&["FCALL", "getk", "1", "fk"]),
        v(&["FCALL_RO", "getk", "1", "fk"]),
        v(&["FCALL_RO", "setk", "1", "fk", "x"]),
        v(&["FCALL", "nosuch", "0"]),
        v(&["FUNCTION", "LOAD", "return 1"]),
        v(&["FUNCTION", "LOAD", "#!lua name=empty\nlocal x = 1"]),
        v(&["FUNCTION", "DELETE", "nolib"]),
        v(&["FUNCTION", "DELETE", "mylib"]),
        v(&["FCALL", "echo1", "0", "hello"]),
        v(&["FUNCTION", "KILL"]),
    ];
    let bad = compare_args(&mut s, &mut r, &cmds).await;
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!("scripts: {} commands, {} mismatches", cmds.len(), bad.len());
    // Budgets: an endless loop is stopped and rolled back (SprigDB only;
    // Redis would answer BUSY to others instead).
    run(&mut s, &["SET", "budget", "before"]).await.unwrap();
    let t = std::time::Instant::now();
    let e = run(
        &mut s,
        &[
            "EVAL",
            "redis.call('SET', KEYS[1], 'during'); while true do pcall(function() end) end",
            "1",
            "budget",
        ],
    )
    .await;
    eprintln!("endless loop: {} after {:?}", show(&e), t.elapsed());
    assert!(show(&e).contains("39850"), "{}", show(&e));
    assert_eq!(show(&run(&mut s, &["GET", "budget"]).await), "$before");
    for c in [&mut r, &mut s] {
        run(c, &["FLUSHALL"]).await.unwrap();
        run(c, &["SCRIPT", "FLUSH"]).await.unwrap();
        run(c, &["FUNCTION", "FLUSH"]).await.unwrap();
    }
    drop_test_db().await;
    assert!(bad.is_empty(), "{} of {} differ", bad.len(), cmds.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactions_match_redis() {
    let (Some(refaddr), Some(url)) = (var("SPRIG_TEST_REDIS_REF"), sprig_url().await) else {
        eprintln!("SPRIG_TEST_REDIS_REF or SprigDB not set: skipped");
        return;
    };
    let rurl = format!("redis://{refaddr}");
    let mut bad = Vec::new();
    for (name, u) in [("redis", rurl.as_str()), ("sprig", url.as_str())] {
        let mut a = conn(u).await;
        let mut b = conn(u).await;
        run(&mut a, &["FLUSHALL"]).await.unwrap();
        let mut log = Vec::new();
        for c in [
            vec!["MULTI"],
            vec!["SET", "t", "1"],
            vec!["INCR", "t"],
            vec!["LPUSH", "t", "x"],
            vec!["GET", "t"],
            vec!["EXEC"],
            vec!["MULTI"],
            vec!["SET", "t", "1"],
            vec!["NOSUCH"],
            vec!["EXEC"],
            vec!["GET", "t"],
            vec!["EXEC"],
            vec!["DISCARD"],
            vec!["MULTI"],
            vec!["MULTI"],
            vec!["WATCH", "x"],
            vec!["DISCARD"],
            vec!["WATCH", "w"],
            vec!["MULTI"],
            vec!["SET", "w", "mine"],
            vec!["EXEC"],
            vec!["WATCH", "w"],
            vec!["UNWATCH"],
            vec!["MULTI"],
            vec!["GET", "w"],
            vec!["EXEC"],
        ] {
            log.push(show(&run(&mut a, &c).await));
        }
        // WATCH, then another client writes: EXEC returns nil.
        log.push(show(&run(&mut a, &["WATCH", "w"]).await));
        log.push(show(&run(&mut b, &["SET", "w", "theirs"]).await));
        log.push(show(&run(&mut a, &["MULTI"]).await));
        log.push(show(&run(&mut a, &["SET", "w", "mine2"]).await));
        log.push(show(&run(&mut a, &["EXEC"]).await));
        log.push(show(&run(&mut a, &["GET", "w"]).await));
        // WATCH a missing key that another client then creates.
        log.push(show(&run(&mut a, &["WATCH", "nw"]).await));
        log.push(show(&run(&mut b, &["SET", "nw", "1"]).await));
        log.push(show(&run(&mut a, &["MULTI"]).await));
        log.push(show(&run(&mut a, &["GET", "nw"]).await));
        log.push(show(&run(&mut a, &["EXEC"]).await));
        // A watched key that expires counts as changed.
        log.push(show(&run(&mut a, &["SET", "ex", "1", "PX", "50"]).await));
        log.push(show(&run(&mut a, &["WATCH", "ex"]).await));
        tokio::time::sleep(Duration::from_millis(120)).await;
        log.push(show(&run(&mut a, &["MULTI"]).await));
        log.push(show(&run(&mut a, &["SET", "ex", "2"]).await));
        log.push(show(&run(&mut a, &["EXEC"]).await));
        eprintln!("{name}: {log:?}");
        bad.push(log);
    }
    if let Some(mut c) = Some(conn(&url).await) {
        run(&mut c, &["FLUSHALL"]).await.unwrap();
    }
    drop_test_db().await;
    assert_eq!(bad[0], bad[1]);
}

/// Reads one RESP reply from `s`, returning its raw bytes.
async fn read_reply(s: &mut tokio::net::TcpStream, buf: &mut Vec<u8>) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    loop {
        if let Some(n) = reply_len(buf, 0) {
            return buf.drain(..n).collect();
        }
        let mut tmp = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(10), s.read(&mut tmp))
            .await
            .expect("reply timed out")
            .unwrap();
        assert!(n > 0, "connection closed");
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// The length of the reply at `at`, when complete.
fn reply_len(b: &[u8], at: usize) -> Option<usize> {
    let nl = b[at..].windows(2).position(|w| w == b"\r\n")? + at;
    let head = std::str::from_utf8(&b[at + 1..nl]).ok()?;
    let after = nl + 2;
    match *b.get(at)? {
        b'+' | b'-' | b':' | b'_' | b',' | b'#' | b'(' => Some(after - at),
        b'$' | b'=' | b'!' => {
            let n: i64 = head.parse().ok()?;
            if n < 0 {
                return Some(after - at);
            }
            let end = after + usize::try_from(n).ok()? + 2;
            (b.len() >= end).then_some(end - at)
        }
        b'*' | b'~' | b'>' | b'%' => {
            let n: i64 = head.parse().ok()?;
            if n < 0 {
                return Some(after - at);
            }
            let items = if b[at] == b'%' { n * 2 } else { n };
            let mut p = after;
            for _ in 0..items {
                p += reply_len(b, p)?;
            }
            Some(p - at)
        }
        _ => None,
    }
}

fn wire(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

/// A raw connection; logs in to SprigDB with the URL's user and password.
async fn raw(addr: &str, sprig: Option<(&str, &str)>) -> (tokio::net::TcpStream, Vec<u8>) {
    use tokio::io::AsyncWriteExt;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut buf = Vec::new();
    if let Some((u, p)) = sprig {
        s.write_all(&wire(&["AUTH", u, p])).await.unwrap();
        let r = read_reply(&mut s, &mut buf).await;
        assert_eq!(r, b"+OK\r\n");
    }
    (s, buf)
}

fn host_of(url: &str) -> String {
    url.rsplit('@')
        .next()
        .unwrap_or(url)
        .trim_start_matches("redis://")
        .to_owned()
}

/// `(user, password)` of a `redis://user:pw@host` URL.
fn creds_of(url: &str) -> (String, String) {
    let body = url.trim_start_matches("redis://");
    let userinfo = body.rsplit_once('@').map_or("", |x| x.0);
    let (u, p) = userinfo.split_once(':').unwrap_or(("", userinfo));
    (u.to_owned(), p.to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_bytes_pubsub_and_blocking_match_redis() {
    use tokio::io::AsyncWriteExt;
    let (Some(refaddr), Some(url)) = (var("SPRIG_TEST_REDIS_REF"), sprig_url().await) else {
        eprintln!("SPRIG_TEST_REDIS_REF or SprigDB not set: skipped");
        return;
    };
    let saddr = host_of(&url);
    let (cu, cp) = creds_of(&url);
    let mut logs = Vec::new();
    for (sprig, addr) in [(false, refaddr.as_str()), (true, saddr.as_str())] {
        let mut log: Vec<String> = Vec::new();
        let login = sprig.then_some((cu.as_str(), cp.as_str()));
        let (mut a, mut ab) = raw(addr, login).await;
        let (mut b, mut bb) = raw(addr, login).await;
        let send = async |s: &mut tokio::net::TcpStream, buf: &mut Vec<u8>, args: &[&str]| {
            s.write_all(&wire(args)).await.unwrap();
            String::from_utf8_lossy(&read_reply(s, buf).await).into_owned()
        };
        log.push(send(&mut a, &mut ab, &["FLUSHALL"]).await);
        // RESP2 and RESP3 shapes, byte for byte.
        for cmd in [
            &["ZADD", "z", "1.5", "a", "2", "b", "-inf", "c"][..],
            &["ZRANGE", "z", "0", "-1", "WITHSCORES"],
            &["ZSCORE", "z", "a"],
            &["ZPOPMIN", "z"],
            &["HSET", "h", "f", "v"],
            &["HGETALL", "h"],
            &["SADD", "s", "m"],
            &["SMEMBERS", "s"],
            &["GET", "nokey"],
            &["LPOP", "nokey", "2"],
            &["INCRBYFLOAT", "f", "0.1"],
            &["EXISTS", "h"],
            &["GEOADD", "g", "13.361389", "38.115556", "P"],
            &["GEOPOS", "g", "P"],
            &["CONFIG", "GET", "maxmemory"],
        ] {
            log.push(send(&mut a, &mut ab, cmd).await);
        }
        let hello = send(&mut a, &mut ab, &["HELLO", "3"]).await;
        log.push(hello.contains("proto").to_string());
        for cmd in [
            &["ZRANGE", "z", "0", "-1", "WITHSCORES"][..],
            &["ZSCORE", "z", "b"],
            &["ZPOPMIN", "z"],
            &["HGETALL", "h"],
            &["SMEMBERS", "s"],
            &["GET", "nokey"],
            &["LPOP", "nokey", "2"],
            &["GEOPOS", "g", "P"],
            &["CONFIG", "GET", "maxmemory"],
            &["EVAL", "return {double=1.5}", "0"],
            &[
                "EVAL",
                "redis.setresp(3); return redis.call('HGETALL', KEYS[1])",
                "1",
                "h",
            ],
            &["HELLO", "2"],
        ] {
            let r = send(&mut a, &mut ab, cmd).await;
            log.push(if cmd[0] == "HELLO" {
                r.contains("proto").to_string()
            } else {
                r
            });
        }
        // Pub/sub (RESP2): subscribe, publish, pattern, ping in subscribed mode.
        log.push(send(&mut b, &mut bb, &["SUBSCRIBE", "ch1"]).await);
        log.push(send(&mut b, &mut bb, &["PSUBSCRIBE", "ch*"]).await);
        log.push(send(&mut a, &mut ab, &["PUBLISH", "ch1", "hello"]).await);
        log.push(String::from_utf8_lossy(&read_reply(&mut b, &mut bb).await).into_owned());
        log.push(String::from_utf8_lossy(&read_reply(&mut b, &mut bb).await).into_owned());
        log.push(send(&mut a, &mut ab, &["PUBSUB", "NUMSUB", "ch1", "ch2"]).await);
        log.push(send(&mut a, &mut ab, &["PUBSUB", "NUMPAT"]).await);
        log.push(send(&mut b, &mut bb, &["PING"]).await);
        log.push(send(&mut b, &mut bb, &["GET", "x"]).await);
        log.push(send(&mut b, &mut bb, &["UNSUBSCRIBE"]).await);
        log.push(send(&mut b, &mut bb, &["PUNSUBSCRIBE"]).await);
        log.push(send(&mut b, &mut bb, &["GET", "x"]).await);
        // Blocking: b waits, a pushes.
        b.write_all(&wire(&["BLPOP", "q", "5"])).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let t = std::time::Instant::now();
        log.push(send(&mut a, &mut ab, &["RPUSH", "q", "job"]).await);
        log.push(String::from_utf8_lossy(&read_reply(&mut b, &mut bb).await).into_owned());
        let woke = t.elapsed();
        eprintln!(
            "{} BLPOP woke after {woke:?}",
            if sprig { "sprig" } else { "redis" }
        );
        assert!(woke < Duration::from_millis(1500));
        log.push(send(&mut b, &mut bb, &["BLPOP", "q", "0.2"]).await);
        log.push(send(&mut a, &mut ab, &["FLUSHALL"]).await);
        logs.push(log);
    }
    let mut bad = 0;
    for (i, (x, y)) in logs[0].iter().zip(&logs[1]).enumerate() {
        if x != y {
            bad += 1;
            eprintln!("MISMATCH #{i}\n   redis: {x:?}\n   sprig: {y:?}");
        }
    }
    eprintln!("wire: {} replies, {bad} mismatches", logs[0].len());
    drop_test_db().await;
    assert_eq!(bad, 0);
}

/// A random command of the guarded-batch set (R6 round 3, R23), with the
/// options and values that reach every branch of the batches.
fn random_batched(g: &mut Rng) -> Vec<String> {
    let keys = ["b:s", "b:h", "b:l", "b:z", "b:x", "{b}:y"];
    let k = g.pick(&keys).to_owned();
    let vals = [
        "1", "0", "-0", "007", "12\n", "-5", "x", "", "9223372036854775806",
        "9223372036854775807", "-9223372036854775808", "18446744073709551616", "3.5",
    ];
    let v = |g: &mut Rng| g.pick(&vals).to_owned();
    let s = |x: &str| x.to_owned();
    match g.int(0, 21) {
        0 | 1 => {
            let mut c = vec![s("SET"), k, v(g)];
            match g.int(0, 6) {
                0 => c.push(s("NX")),
                1 => c.push(s("XX")),
                2 => c.push(s("GET")),
                3 => c.extend([s("PX"), s(g.pick(&["30", "100000"]))]),
                4 => c.push(s("KEEPTTL")),
                5 => c.extend([s("EX"), s("100"), s("GET"), s("XX")]),
                _ => {}
            }
            c
        }
        2 => vec![s("INCR"), k],
        3 => vec![s("DECR"), k],
        4 => vec![s("INCRBY"), k, s(g.pick(&["5", "-7", "9223372036854775807", "-9223372036854775808"]))],
        5 => vec![s("DECRBY"), k, s(g.pick(&["3", "-9223372036854775807"]))],
        6 | 7 => {
            let mut c = vec![s(g.pick(&["HSET", "HMSET"])), k];
            for _ in 0..g.int(1, 3) {
                c.push(s(g.pick(&["f", "g", "h", ""])));
                c.push(v(g));
            }
            c
        }
        8 | 9 => {
            let mut c = vec![s(g.pick(&["LPUSH", "RPUSH", "LPUSHX", "RPUSHX"])), k];
            for _ in 0..g.int(1, 3) {
                c.push(v(g));
            }
            c
        }
        10 | 11 => {
            let mut c = vec![s(g.pick(&["LPOP", "RPOP"])), k];
            if g.int(0, 1) == 1 {
                c.push(s(g.pick(&["0", "1", "2", "5"])));
            }
            c
        }
        12..=14 => {
            let mut c = vec![s("ZADD"), k];
            for f in ["NX", "XX", "GT", "LT", "CH"] {
                if g.int(0, 4) == 0 {
                    c.push(s(f));
                }
            }
            let mut used = Vec::new();
            for _ in 0..g.int(1, 3) {
                let m = g.pick(&["a", "b", "c", ""]);
                if used.contains(&m) {
                    continue;
                }
                used.push(m);
                c.push(s(g.pick(&["1", "2", "-1.5", "+inf", "-inf", "0", "-0", "1e300"])));
                c.push(s(m));
            }
            c
        }
        15 => vec![s("PEXPIRE"), k, s(g.pick(&["20", "100000"]))],
        16 => vec![s("TYPE"), k],
        17 => vec![s("GET"), k],
        18 => vec![s("LRANGE"), k, s("0"), s("-1")],
        19 => vec![s("ZRANGE"), k, s("0"), s("-1"), s("WITHSCORES")],
        20 => vec![s("HGETALL"), k],
        _ => vec![s("DEL"), k],
    }
}

/// The guarded batches (R6 round 3) against Redis: random commands of
/// the batched set with every option, wrong types, expired keys and
/// integer edge cases, then the final state; then 8 clients incrementing
/// one counter and pushing to one list at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn batched_writes_match_redis() {
    let (Some(refaddr), Some(url)) = (var("SPRIG_TEST_REDIS_REF"), sprig_url().await) else {
        eprintln!("SPRIG_TEST_REDIS_REF or SprigDB not set: skipped");
        return;
    };
    let ops: usize = var("SPRIG_TEST_OPS")
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let mut r = conn(&format!("redis://{refaddr}")).await;
    let mut s = conn(&url).await;
    let mut total_bad = 0;
    for seed in [11u64, 12, 13] {
        run(&mut r, &["FLUSHALL"]).await.unwrap();
        run(&mut s, &["FLUSHALL"]).await.unwrap();
        let mut g = Rng(seed);
        let mut bad = Vec::new();
        for _ in 0..ops {
            let c = random_batched(&mut g);
            let refs: Vec<&str> = c.iter().map(String::as_str).collect();
            bad.extend(compare(&mut s, &mut r, &[refs]).await);
            // A short expiry (20-30 ms) passes on both servers before the
            // next command, so they never race it.
            if c.iter().any(|a| a == "20" || a == "30") {
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        }
        let ds = dump(&mut s).await;
        let dr = dump(&mut r).await;
        if ds != dr {
            bad.push(format!("final state differs:\n sprig {ds:?}\n redis {dr:?}"));
        }
        for b in bad.iter().take(20) {
            eprintln!("seed {seed} MISMATCH {b}");
        }
        eprintln!("batched seed {seed}: {ops} ops, {} mismatches", bad.len());
        total_bad += bad.len();
    }
    // Concurrency: 8 clients x 50 INCR and RPUSH on two new keys.
    run(&mut s, &["FLUSHALL"]).await.unwrap();
    let mut tasks = Vec::new();
    for c in 0..8 {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut s = conn(&url).await;
            for i in 0..50 {
                run(&mut s, &["INCR", "b:ctr"]).await.unwrap();
                run(&mut s, &["RPUSH", "b:q", &format!("{c}-{i}")]).await.unwrap();
                run(&mut s, &["ZADD", "b:zz", &i.to_string(), &format!("{c}-{i}")]).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let ctr = show(&run(&mut s, &["GET", "b:ctr"]).await);
    let len = show(&run(&mut s, &["LLEN", "b:q"]).await);
    let zc = show(&run(&mut s, &["ZCARD", "b:zz"]).await);
    eprintln!("concurrent: counter {ctr}, list {len}, zset {zc}");
    if ctr != "$400" || len != ":400" || zc != ":400" {
        total_bad += 1;
    }
    run(&mut s, &["FLUSHALL"]).await.unwrap();
    drop_test_db().await;
    assert_eq!(total_bad, 0, "{total_bad} differ");
}
