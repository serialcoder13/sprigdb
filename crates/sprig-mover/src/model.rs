//! What the mover moves: tables, rows, keys, row changes and transactions
//! as the copy, the stream and the applier see them (chapter 10, "The
//! watermark copy", "The applier").
//!
//! Values are `mysql_async` values, the binary protocol's: chunk reads
//! return them, the applier sends them as prepared-statement parameters
//! (floating-point bits stay exact), and the stream converts binlog values
//! to them ([`crate::mysql::stream`]). `ENUM`, `SET` and `BIT` columns are
//! read and written as numbers on both paths, `TIMESTAMP` as UTC date and
//! time under `time_zone = '+00:00'`, `DECIMAL` as its text.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::JobId;

/// One value.
pub type Value = mysql_async::Value;

/// One row: every column of the table, in table order.
pub type Row = Vec<Value>;

/// One column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMeta {
    /// The name.
    pub name: String,
    /// A generated column (`VIRTUAL` or `STORED`): never copied, MariaDB
    /// computes it on the target.
    #[serde(default)]
    pub generated: bool,
    /// `ENUM`, `SET` or `BIT`: read as `col + 0` so chunk rows and binlog
    /// rows carry the same number.
    #[serde(default)]
    pub numeric: bool,
}

/// One table of a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMeta {
    /// The source schema.
    pub schema: String,
    /// The table.
    pub name: String,
    /// Columns in table order.
    pub columns: Vec<ColumnMeta>,
    /// Primary-key columns, as indexes into `columns`, in key order.
    pub pk: Vec<usize>,
    /// `SHOW CREATE TABLE` on the source (empty in the simulation).
    #[serde(default)]
    pub create_sql: String,
}

impl TableMeta {
    /// `schema.table`, the job's key for the table.
    #[must_use]
    pub fn id(&self) -> TableId {
        TableId::new(&self.schema, &self.name)
    }

    /// The primary-key values of `row`.
    #[must_use]
    pub fn key_values(&self, row: &[Value]) -> Row {
        self.pk
            .iter()
            .map(|&i| row.get(i).cloned().unwrap_or(Value::NULL))
            .collect()
    }

    /// The canonical bytes of `row`'s primary key.
    #[must_use]
    pub fn key_of(&self, row: &[Value]) -> KeyBytes {
        let mut out = Vec::with_capacity(16 * self.pk.len());
        for &i in &self.pk {
            encode_value(row.get(i).unwrap_or(&Value::NULL), &mut out);
        }
        KeyBytes(out)
    }

    /// The canonical bytes of key values (as [`Self::key_values`] returns).
    #[must_use]
    pub fn key_of_values(values: &[Value]) -> KeyBytes {
        let mut out = Vec::with_capacity(16 * values.len());
        for v in values {
            encode_value(v, &mut out);
        }
        KeyBytes(out)
    }

    /// Indexes of the columns the applier writes (not generated).
    pub fn copy_columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.generated)
            .map(|(i, _)| i)
    }
}

/// `schema.table`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableId(pub String);

impl TableId {
    /// From its parts.
    #[must_use]
    pub fn new(schema: &str, table: &str) -> Self {
        Self(format!("{schema}.{table}"))
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A primary key as canonical bytes: chunk rows and binlog rows of the same
/// key give the same bytes (integers by value whatever their width or
/// signedness, strings and binary as stored, temporal values by field).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyBytes(pub Vec<u8>);

/// Appends `v`'s canonical bytes to `out`: a tag, then the payload.
#[allow(clippy::many_single_char_names)] // date and time fields
pub fn encode_value(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::NULL => out.push(0),
        Value::Int(i) => {
            out.push(1);
            out.extend_from_slice(&i128::from(*i).to_be_bytes());
        }
        Value::UInt(u) => {
            out.push(1);
            out.extend_from_slice(&i128::from(*u).to_be_bytes());
        }
        Value::Float(f) => {
            out.push(3);
            out.extend_from_slice(&f64::from(*f).to_bits().to_be_bytes());
        }
        Value::Double(d) => {
            out.push(3);
            out.extend_from_slice(&d.to_bits().to_be_bytes());
        }
        Value::Bytes(b) => {
            out.push(2);
            out.extend_from_slice(&u32::try_from(b.len()).unwrap_or(u32::MAX).to_be_bytes());
            out.extend_from_slice(b);
        }
        Value::Date(y, mo, d, h, mi, s, us) => {
            out.push(4);
            out.extend_from_slice(&y.to_be_bytes());
            out.extend_from_slice(&[*mo, *d, *h, *mi, *s]);
            out.extend_from_slice(&us.to_be_bytes());
        }
        Value::Time(neg, d, h, mi, s, us) => {
            out.push(5);
            out.push(u8::from(*neg));
            out.extend_from_slice(&d.to_be_bytes());
            out.extend_from_slice(&[*h, *mi, *s]);
            out.extend_from_slice(&us.to_be_bytes());
        }
    }
}

/// A watermark row's kind (chapter 10, `__sprig_watermark.kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WmKind {
    /// Before a chunk read.
    Low,
    /// After a chunk read.
    High,
    /// A lag tick.
    Tick,
}

impl WmKind {
    /// The `ENUM` member.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            WmKind::Low => "low",
            WmKind::High => "high",
            WmKind::Tick => "tick",
        }
    }

    /// From the `ENUM` member name or its 1-based index.
    #[must_use]
    pub fn parse(s: &[u8]) -> Option<Self> {
        match s {
            b"low" | b"1" => Some(WmKind::Low),
            b"high" | b"2" => Some(WmKind::High),
            b"tick" | b"3" => Some(WmKind::Tick),
            _ => None,
        }
    }
}

/// A watermark the stream recognized as this job's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Watermark {
    /// The job.
    pub job: JobId,
    /// The job's stream.
    pub stream: u16,
    /// Low, high or tick.
    pub kind: WmKind,
    /// The upsert's sequence number.
    pub seq: u64,
}

/// One row change on a table of the job.
#[derive(Debug, Clone, PartialEq)]
pub struct RowChange {
    /// The table.
    pub table: Arc<TableMeta>,
    /// The row before (updates, deletes).
    pub before: Option<Row>,
    /// The row after (inserts, updates).
    pub after: Option<Row>,
}

/// A statement that changes the schema of a job's table or schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDdl {
    /// The default database it ran in.
    pub db: String,
    /// The statement.
    pub sql: String,
}

/// One committed source transaction, filtered to the job: its row changes,
/// its DDL, and the job's watermarks it wrote.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceTxn {
    /// The GTID; `None` only in tests.
    pub gtid: Option<sprig_binlog::Gtid>,
    /// Row changes to the job's tables, in order.
    pub changes: Vec<RowChange>,
    /// The job's watermarks.
    pub watermarks: Vec<Watermark>,
    /// DDL touching the job's schemas.
    pub ddl: Option<SourceDdl>,
    /// An XA phase: the changes of a `Prepare` group wait for its commit.
    pub xa: Option<XaPhase>,
}

/// Where an XA transaction is (chapter 10, "The applier", XA).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XaPhase {
    /// `XA PREPARE`: the group's changes, held until the outcome.
    Prepare(String),
    /// `XA COMMIT`: apply the held changes.
    Commit(String),
    /// `XA ROLLBACK`: drop them.
    Rollback(String),
}

/// What a stream yields.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamItem {
    /// A committed transaction (possibly with nothing of the job's in it,
    /// so the position still advances).
    Txn(SourceTxn),
    /// Nothing new for a while (a heartbeat): a chance to flush and check
    /// timers.
    Idle,
}

/// One table's copy progress, stored with the position (chapter 10,
/// `__sprig_mover_pos.copy_state`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCopy {
    /// The last copied key, as hex of its canonical bytes. The values are
    /// kept too, so the next chunk's `WHERE` can use them.
    #[serde(default)]
    pub cursor: Option<Vec<KeyPart>>,
    /// Every row was copied.
    #[serde(default)]
    pub done: bool,
}

/// One key value, stored in JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "t", content = "v")]
pub enum KeyPart {
    /// `NULL`.
    Null,
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// Bytes, hex.
    Bytes(String),
    /// A double's bits.
    Double(u64),
    /// Date and time fields.
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// Time fields.
    Time(bool, u32, u8, u8, u8, u32),
}

impl KeyPart {
    /// From a value.
    #[must_use]
    #[allow(clippy::many_single_char_names)] // date and time fields
    pub fn from_value(v: &Value) -> Self {
        match v {
            Value::NULL => KeyPart::Null,
            Value::Int(i) => KeyPart::Int(*i),
            Value::UInt(u) => KeyPart::UInt(*u),
            Value::Float(f) => KeyPart::Double(f64::from(*f).to_bits()),
            Value::Double(d) => KeyPart::Double(d.to_bits()),
            Value::Bytes(b) => KeyPart::Bytes(hex(b)),
            Value::Date(y, mo, d, h, mi, s, us) => KeyPart::Date(*y, *mo, *d, *h, *mi, *s, *us),
            Value::Time(n, d, h, mi, s, us) => KeyPart::Time(*n, *d, *h, *mi, *s, *us),
        }
    }

    /// Back to a value.
    #[must_use]
    #[allow(clippy::many_single_char_names)] // date and time fields
    pub fn to_value(&self) -> Value {
        match self {
            KeyPart::Null => Value::NULL,
            KeyPart::Int(i) => Value::Int(*i),
            KeyPart::UInt(u) => Value::UInt(*u),
            KeyPart::Double(b) => Value::Double(f64::from_bits(*b)),
            KeyPart::Bytes(h) => Value::Bytes(unhex(h)),
            KeyPart::Date(y, mo, d, h, mi, s, us) => Value::Date(*y, *mo, *d, *h, *mi, *s, *us),
            KeyPart::Time(n, d, h, mi, s, us) => Value::Time(*n, *d, *h, *mi, *s, *us),
        }
    }
}

/// Lower-case hex.
#[must_use]
pub fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn unhex(h: &str) -> Vec<u8> {
    h.as_bytes()
        .chunks(2)
        .filter_map(|p| std::str::from_utf8(p).ok())
        .filter_map(|p| u8::from_str_radix(p, 16).ok())
        .collect()
}

/// Every table's copy progress, by [`TableId`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyState {
    /// Per table.
    pub tables: BTreeMap<String, TableCopy>,
}

impl CopyState {
    /// Whether every listed table is done.
    #[must_use]
    pub fn all_done<'a>(&self, tables: impl IntoIterator<Item = &'a TableId>) -> bool {
        tables
            .into_iter()
            .all(|t| self.tables.get(&t.0).is_some_and(|c| c.done))
    }
}

/// The target's position row (chapter 10, `__sprig_mover_pos`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PosRow {
    /// The owning worker's fencing token.
    pub token: u64,
    /// The applied source position.
    pub gtid_pos: sprig_binlog::GtidPos,
    /// Copy progress.
    pub copy: CopyState,
}

/// One statement's worth of applier work: consecutive same-kind changes
/// to one table.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOp {
    /// `INSERT ... ON DUPLICATE KEY UPDATE` of whole rows.
    Upsert {
        /// The table.
        table: Arc<TableMeta>,
        /// Full rows.
        rows: Vec<Row>,
    },
    /// `DELETE ... WHERE pk = ? OR ...`.
    Delete {
        /// The table.
        table: Arc<TableMeta>,
        /// Key values (as [`TableMeta::key_values`]).
        keys: Vec<Row>,
    },
}

impl ApplyOp {
    /// Rows the op touches.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            ApplyOp::Upsert { rows, .. } => rows.len(),
            ApplyOp::Delete { keys, .. } => keys.len(),
        }
    }

    /// Whether it touches none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A per-row digest for verification: the key and a 64-bit hash of the
/// copied columns, computed by MariaDB on both sides.
#[derive(Debug, Clone, PartialEq)]
pub struct RowDigest {
    /// The key's canonical bytes.
    pub key: KeyBytes,
    /// The key's values.
    pub key_values: Row,
    /// The hash.
    pub hash: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_keys_compare_by_value() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        encode_value(&Value::Int(5), &mut a);
        encode_value(&Value::UInt(5), &mut b);
        assert_eq!(a, b);
        let mut c = Vec::new();
        encode_value(&Value::Int(-1), &mut c);
        let mut d = Vec::new();
        encode_value(&Value::UInt(u64::MAX), &mut d);
        assert_ne!(c, d);
    }

    #[test]
    fn composite_keys_are_self_delimiting() {
        let t = TableMeta {
            schema: "s".into(),
            name: "t".into(),
            columns: vec![
                ColumnMeta {
                    name: "a".into(),
                    generated: false,
                    numeric: false,
                },
                ColumnMeta {
                    name: "b".into(),
                    generated: false,
                    numeric: false,
                },
            ],
            pk: vec![0, 1],
            create_sql: String::new(),
        };
        let r1 = vec![Value::Bytes(b"ab".to_vec()), Value::Bytes(b"c".to_vec())];
        let r2 = vec![Value::Bytes(b"a".to_vec()), Value::Bytes(b"bc".to_vec())];
        assert_ne!(t.key_of(&r1), t.key_of(&r2));
        assert_eq!(t.key_of(&r1), TableMeta::key_of_values(&t.key_values(&r1)));
    }

    #[test]
    fn key_parts_round_trip() {
        for v in [
            Value::NULL,
            Value::Int(-7),
            Value::UInt(u64::MAX),
            Value::Bytes(vec![0, 1, 0xff]),
            Value::Double(1.5),
            Value::Date(2026, 11, 8, 1, 2, 3, 4),
            Value::Time(true, 1, 2, 3, 4, 5),
        ] {
            let p = KeyPart::from_value(&v);
            let back = serde_json::from_str::<KeyPart>(&serde_json::to_string(&p).unwrap())
                .unwrap()
                .to_value();
            assert_eq!(back, v);
        }
    }
}
