//! Change-topic records (chapter 45, "Change topics and envelopes"; build
//! step 6): one feed event of the watched object becomes one record.
//!
//! ```json
//! {"op": "u", "before": {...}, "after": {...},
//!  "source": {"tenant": 42, "db": "shop", "object": "orders", "model": "table",
//!             "shard": "g3/s17", "gtid": "0-11-90412", "hlc": 7587861234,
//!             "txn": "0-11-90412"},
//!  "ts_ms": 1762508100123}
//! ```
//!
//! The key is the JSON of the primary key. `op` is `c`, `u` or `d`; DDL is
//! `ddl` with the logical statement, and `TRUNCATE` is `t`, both on
//! partition 0. Internal columns never appear (the feed removes them).
//!
//! **As built:** SQL tables only (typed tables and hierarchies are tables
//! here: their rows arrive as rows of their physical tables). Collections,
//! key-value namespaces, Redis structures and graphs wait for their
//! models' feed decoding (R5, R6, R8). `source.tenant` is the tenant id.
//! `DECIMAL` values are JSON numbers (trailing zeros are lost: `49.90` is
//! `49.9`); binary strings and spatial values are base64; temporal values
//! are their MariaDB text.

use serde_json::{Map, Value as J, json};
use sprig_binlog::Value;
use sprig_feed::{ChangeEvent, Image, Op};

use crate::model::{NewRecord, PartitionNo, Source};
use crate::partition;

/// A binlog value as JSON.
#[must_use]
pub fn json_of(v: &Value) -> J {
    match v {
        Value::Null => J::Null,
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) => json!(f),
        Value::Double(d) => json!(d),
        Value::Decimal(s) => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map_or_else(|| J::String(s.clone()), J::Number),
        Value::Bit(b) | Value::Set(b) => json!(b),
        Value::Year(y) => json!(y),
        Value::Enum(e) => json!(e),
        Value::Date(d) => J::String(d.to_string()),
        Value::Time(t) => J::String(t.to_string()),
        Value::DateTime(d) => J::String(d.to_string()),
        Value::Timestamp(t) => J::String(t.to_string()),
        Value::String(b) => J::String(String::from_utf8_lossy(b).into_owned()),
        Value::Bytes(b) | Value::Geometry(b) => J::String(base64(b)),
    }
}

/// Standard base64 with padding.
#[must_use]
pub fn base64(b: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for c in b.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        out.push(char::from(T[(n >> 18) as usize & 63]));
        out.push(char::from(T[(n >> 12) as usize & 63]));
        out.push(if c.len() > 1 {
            char::from(T[(n >> 6) as usize & 63])
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            char::from(T[n as usize & 63])
        } else {
            '='
        });
    }
    out
}

/// An image as a JSON object.
#[must_use]
pub fn image_json(img: &Image) -> J {
    let mut m = Map::new();
    for (k, v) in &img.0 {
        m.insert(k.clone(), json_of(v));
    }
    J::Object(m)
}

/// What a change topic needs to know about its source.
#[derive(Debug, Clone)]
pub struct ChangeCtx {
    /// Partitions of the topic.
    pub partitions: u32,
    /// The tenant's name, if known (else its id is used).
    pub tenant_name: Option<String>,
}

/// The record of change event `e`, or `None` for an event the topic
/// doesn't carry.
#[must_use]
pub fn record_of(e: &ChangeEvent, cx: &ChangeCtx) -> Option<NewRecord> {
    let (op, partition_zero) = match e.op {
        Op::Insert => ("c", false),
        Op::Update => ("u", false),
        Op::Delete => ("d", false),
        Op::Ddl(_) => ("ddl", true),
        Op::Truncate => ("t", true),
    };
    let gtid = format!("{}-{}-{}", e.txn.domain, e.txn.server, e.txn.seq);
    let ts_ms = i64::try_from((e.hlc >> 32) * 1000).unwrap_or(0);
    let tenant = cx
        .tenant_name
        .clone()
        .map_or_else(|| json!(e.tenant.0), J::String);
    let mut body = json!({
        "op": op,
        "before": e.before.as_ref().map_or(J::Null, image_json),
        "after": e.after.as_ref().map_or(J::Null, image_json),
        "source": {
            "tenant": tenant,
            "db": e.db_name,
            "object": e.table,
            "model": "table",
            "shard": format!("g{}/s{}", e.shard.group.0, e.shard.shard.0),
            "gtid": gtid,
            "hlc": e.hlc,
            "txn": gtid,
        },
        "ts_ms": ts_ms,
    });
    if let Some(ddl) = &e.ddl {
        body["ddl"] = J::String(ddl.clone());
    }
    let key = if partition_zero || e.key.0.is_empty() {
        None
    } else {
        Some(image_json(&e.key).to_string().into_bytes())
    };
    let partition = if partition_zero {
        PartitionNo(0)
    } else {
        let src = Source::of_event(&e.id).to_bytes();
        partition::of_record(None, key.as_deref(), &src, cx.partitions)
    };
    Some(NewRecord {
        partition,
        ts_ms,
        key,
        value: Some(body.to_string().into_bytes()),
        headers: None,
        source: Source::of_event(&e.id),
    })
}

#[cfg(test)]
mod tests {
    use sprig_binlog::{Gtid, GtidPos};
    use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
    use sprig_feed::{EventId, ResumeToken, ShardRef};

    use super::*;

    fn event(op: Op) -> ChangeEvent {
        let cluster = ClusterId::new("dc-a-c1").unwrap();
        let gtid = Gtid {
            domain: 0,
            server: 11,
            seq: 90412,
        };
        ChangeEvent {
            id: EventId {
                cluster: cluster.clone(),
                gtid,
                row: 0,
            },
            token: ResumeToken {
                tenant: TenantId(42),
                clamp: 0,
                cluster,
                segment: 0,
                pos: GtidPos::new(),
                until: None,
                progress: false,
            },
            tenant: TenantId(42),
            db: DbId(1),
            db_name: "shop".into(),
            object: 9,
            table: "orders".into(),
            shard: ShardRef {
                group: GroupId(3),
                shard: ShardId(17),
            },
            key: Image(vec![("order_id".into(), Value::Int(7001))]),
            op,
            before: Some(Image(vec![
                ("order_id".into(), Value::Int(7001)),
                ("status".into(), Value::String(b"open".to_vec())),
                ("amount".into(), Value::Decimal("49.90".into())),
            ])),
            after: Some(Image(vec![
                ("order_id".into(), Value::Int(7001)),
                ("status".into(), Value::String(b"paid".to_vec())),
                ("amount".into(), Value::Decimal("49.90".into())),
            ])),
            delta: None,
            hlc: 1_762_508_100 << 32,
            txn: gtid,
            ddl: None,
            hint: None,
        }
    }

    #[test]
    fn update_envelope() {
        let cx = ChangeCtx {
            partitions: 12,
            tenant_name: Some("acme".into()),
        };
        let r = record_of(&event(Op::Update), &cx).unwrap();
        assert_eq!(r.key.as_deref(), Some(&br#"{"order_id":7001}"#[..]));
        assert_eq!(r.partition, partition::for_key(br#"{"order_id":7001}"#, 12));
        let v: J = serde_json::from_slice(r.value.as_ref().unwrap()).unwrap();
        assert_eq!(v["op"], "u");
        assert_eq!(v["before"]["status"], "open");
        assert_eq!(v["after"]["status"], "paid");
        assert_eq!(v["after"]["amount"], 49.9);
        assert_eq!(v["source"]["tenant"], "acme");
        assert_eq!(v["source"]["shard"], "g3/s17");
        assert_eq!(v["source"]["gtid"], "0-11-90412");
        assert_eq!(v["ts_ms"], 1_762_508_100_000_i64);
        let d = record_of(&event(Op::Truncate), &cx).unwrap();
        assert_eq!(d.partition, PartitionNo(0));
        assert!(d.key.is_none());
    }

    #[test]
    fn base64_matches_rfc4648() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
    }
}
