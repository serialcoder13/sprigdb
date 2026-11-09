//! `errant_extract` (chapter 25, "Diverged transactions are extracted and
//! reported, never merged", steps 3-5): the row images of a diverged
//! node's transactions, read with `sprig-binlog`, sealed per tenant, and
//! written to an extract store.
//!
//! ```text
//! read(node, range):
//!   dump the node's binlog (non-blocking) from the position just before the
//!   range, in the cluster's GTID domain; keep the groups whose GTID is in
//!   the range: per row change the schema, table, kind, before and after
//!   images (JSON) and the equivalent SQL (column names from the node's
//!   information_schema; positional `@n` where unknown)
//! seal(tenant):
//!   "SPRIGERR1\n", a header line (tenant, key version, cluster, node, job),
//!   then frames of at most 1 MiB of JSON lines, each AES-256-GCM under the
//!   tenant's extract key with a random nonce; the frame's index and a
//!   final flag are in its associated data, so a reordered or cut-short
//!   file doesn't open
//! verify: every file read back, opened, and its rows counted per table
//!   against the binlog's counts
//! ```
//!
//! Each tenant's events go to its own file under its own key
//! (`t<id>_...` schemas; everything else, `__sprig_sys` included, is tenant
//! 0, the platform's). Keys are the worker's master keyring's (chapter 12's
//! file format) for the purpose `errant extract`, so a tenant's extract
//! key opens nothing else.
//!
//! **Stores.** [`ExtractStore`] is the seam: [`LocalDirStore`] writes under
//! a directory (`[worker] extract_dir`); an object-storage store
//! (chapter 27's buckets, kept in the tenant's allowed DCs) implements the
//! same three calls later.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use sprig_binlog::{BinlogSource, BinlogStream, Event, Gtid, GtidPos, RowsKind, StreamOptions, Value};
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigError};

use super::primary_replica::DivergedRange;

/// The job kind.
pub const KIND: &str = "errant_extract";
/// The keyring purpose of extract keys.
pub const PURPOSE: &[u8] = b"errant extract";
/// The first bytes of a sealed extract.
pub const MAGIC: &[u8] = b"SPRIGERR1\n";
/// Plaintext per frame.
const FRAME: usize = 1 << 20;

fn bad(msg: impl Into<String>) -> SprigError {
    SprigError::internal(msg.into())
}

/// What the job record carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrantExtractSpec {
    /// The cluster.
    pub cluster: ClusterId,
    /// The diverged node.
    pub node: String,
    /// Its diverged transactions.
    pub range: DivergedRange,
    /// The failover the node diverged at (Unix ms): names the extract's
    /// directory.
    pub failover_at_unix_ms: u64,
}

/// One file of an extract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractFile {
    /// The tenant (0: the platform's schemas).
    pub tenant: u64,
    /// Where the store keeps it.
    pub location: String,
    /// The master key version it is sealed under.
    pub key_version: u32,
    /// Transactions with rows of this tenant.
    pub transactions: u64,
    /// Row changes.
    pub rows: u64,
    /// Sealed size.
    pub bytes: u64,
    /// SHA-256 of the sealed file, hex.
    pub sha256: String,
}

/// What the job reports, and the failover record keeps (`errant.extract`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExtractSummary {
    /// The job.
    pub job_id: String,
    /// The extract's directory in the store.
    pub location: String,
    /// One file per tenant.
    pub files: Vec<ExtractFile>,
    /// Diverged transactions read (all of the range).
    pub transactions: u64,
    /// Row changes written.
    pub rows: u64,
    /// Row changes per `schema.table`.
    pub tables: BTreeMap<String, u64>,
    /// First and last commit time, Unix seconds (the binlog's timestamps).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_commit_unix: Option<u32>,
    /// See `first_commit_unix`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_commit_unix: Option<u32>,
    /// Every file was read back, opened and recounted.
    pub verified: bool,
}

/// One row change of the extract (a JSON line).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtractRow {
    /// The transaction's GTID.
    pub gtid: String,
    /// Its commit time, Unix seconds.
    pub at: u32,
    /// Schema.
    pub schema: String,
    /// Table.
    pub table: String,
    /// `insert`, `update` or `delete`.
    pub kind: String,
    /// The row before (updates, deletes): column name to value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Map<String, Json>>,
    /// The row after (inserts, updates).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<serde_json::Map<String, Json>>,
    /// The equivalent statement, for review and replay by hand.
    pub sql: String,
}

/// A diverged transaction with its row changes, before column names.
#[derive(Debug, Clone, PartialEq)]
pub struct RawTxn {
    /// GTID.
    pub gtid: Gtid,
    /// Commit time, Unix seconds.
    pub at: u32,
    /// `(schema, table, kind, before, after)` per row change.
    pub changes: Vec<RawChange>,
    /// Statements that aren't row events (DDL), with their schema.
    pub statements: Vec<(String, String)>,
}

/// One row change, before column names.
#[derive(Debug, Clone, PartialEq)]
pub struct RawChange {
    /// Schema.
    pub schema: String,
    /// Table.
    pub table: String,
    /// What happened.
    pub kind: RowsKind,
    /// Before image (positional; `None` = not logged).
    pub before: Option<Vec<Option<Value>>>,
    /// After image.
    pub after: Option<Vec<Option<Value>>>,
    /// Column names the binlog carried (`binlog_row_metadata = FULL`).
    pub names: Vec<Option<String>>,
}

/// The tenant a schema belongs to: `t<id>_...`, else 0 (the platform).
#[must_use]
pub fn tenant_of(schema: &str) -> u64 {
    schema
        .strip_prefix('t')
        .and_then(|r| r.split_once('_'))
        .and_then(|(id, _)| id.parse().ok())
        .unwrap_or(0)
}

/// The dump's start: the position just before the range, in its domain
/// only (other domains may be unknown to the node).
#[must_use]
pub fn start_position(range: &DivergedRange, applied: &str) -> GtidPos {
    let applied: GtidPos = applied.parse().unwrap_or_default();
    match applied.get(range.domain) {
        Some(g) if g.seq == range.after_seq => {
            format!("{}-{}-{}", g.domain, g.server, g.seq)
                .parse()
                .unwrap_or_default()
        }
        _ if range.after_seq == 0 => GtidPos::default(),
        _ => format!("{}-{}-{}", range.domain, range.server, range.after_seq)
            .parse()
            .unwrap_or_default(),
    }
}

/// Reads the diverged transactions from the node's binlog.
///
/// # Errors
/// The dump failed (1236 when the start isn't in the node's binlogs).
pub async fn read_diverged(
    src: &BinlogSource,
    range: &DivergedRange,
    start: &GtidPos,
    server_id: u32,
    within: Duration,
) -> Result<Vec<RawTxn>> {
    let mut opts = StreamOptions::new(server_id);
    opts.non_blocking = true;
    let mut stream = BinlogStream::connect(src, start, &opts).await?;
    let mut out: Vec<RawTxn> = Vec::new();
    let mut current: Option<RawTxn> = None;
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let ev = tokio::time::timeout_at(deadline, stream.next())
            .await
            .map_err(|_| bad("the binlog read took too long"))??;
        let at = stream.last_header().timestamp;
        match ev {
            Event::Gtid { gtid, .. } => {
                if let Some(t) = current.take() {
                    out.push(t);
                }
                if range.contains(&gtid) {
                    current = Some(RawTxn {
                        gtid,
                        at,
                        changes: Vec::new(),
                        statements: Vec::new(),
                    });
                }
            }
            Event::Rows {
                table, kind, rows, ..
            } => {
                if let Some(t) = current.as_mut() {
                    let names: Vec<Option<String>> =
                        table.columns.iter().map(|c| c.name.clone()).collect();
                    for r in rows {
                        t.changes.push(RawChange {
                            schema: table.schema.clone(),
                            table: table.table.clone(),
                            kind,
                            before: r.before.map(|i| i.0),
                            after: r.after.map(|i| i.0),
                            names: names.clone(),
                        });
                    }
                }
            }
            Event::Query { db, sql, .. } => {
                if let Some(t) = current.as_mut() {
                    let w = sql.trim().to_ascii_uppercase();
                    if !(w == "BEGIN" || w == "COMMIT" || w.starts_with("XA ")) {
                        t.statements.push((db, sql));
                    }
                }
            }
            Event::Xid(_) => {
                if let Some(t) = current.take() {
                    out.push(t);
                }
            }
            Event::DumpEnd | Event::Stop => break,
            _ => {}
        }
        if out
            .last()
            .is_some_and(|t| t.gtid.domain == range.domain && t.gtid.seq >= range.through_seq)
            && current.is_none()
        {
            break;
        }
    }
    if let Some(t) = current.take() {
        out.push(t);
    }
    Ok(out)
}

/// A value as JSON: numbers as numbers, strings as text when UTF-8,
/// binary as `{"hex": ...}`.
#[must_use]
pub fn value_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) => json!(f),
        Value::Double(d) => json!(d),
        Value::Decimal(s) => json!(s),
        Value::Bit(b) | Value::Set(b) => json!(b),
        Value::Year(y) => json!(y),
        Value::Enum(e) => json!(e),
        Value::Date(d) => json!(d.to_string()),
        Value::Time(t) => json!(t.to_string()),
        Value::DateTime(d) => json!(d.to_string()),
        Value::Timestamp(t) => json!(t.to_string()),
        Value::String(b) => match std::str::from_utf8(b) {
            Ok(s) => json!(s),
            Err(_) => json!({ "hex": hex(b) }),
        },
        Value::Bytes(b) | Value::Geometry(b) => json!({ "hex": hex(b) }),
    }
}

/// A value as a SQL literal.
#[must_use]
pub fn value_sql(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => u.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Decimal(s) => s.clone(),
        Value::Year(y) => y.to_string(),
        Value::Enum(e) => e.to_string(),
        Value::Date(d) => crate::sql::literal(&d.to_string()),
        Value::Time(t) => crate::sql::literal(&t.to_string()),
        Value::DateTime(d) => crate::sql::literal(&d.to_string()),
        Value::Timestamp(t) => crate::sql::literal(&t.to_string()),
        Value::String(b) => match std::str::from_utf8(b) {
            Ok(s) => crate::sql::literal(s),
            Err(_) => format!("X'{}'", hex(b)),
        },
        Value::Bytes(b) | Value::Geometry(b) => format!("X'{}'", hex(b)),
    }
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// The column names of a change: the binlog's, else `names` (the node's
/// `information_schema`, in ordinal order), else `@n`.
fn column_name(c: &RawChange, names: Option<&Vec<String>>, i: usize) -> String {
    c.names
        .get(i)
        .cloned()
        .flatten()
        .or_else(|| names.and_then(|n| n.get(i).cloned()))
        .unwrap_or_else(|| format!("@{}", i + 1))
}

fn image(
    c: &RawChange,
    names: Option<&Vec<String>>,
    img: Option<&Vec<Option<Value>>>,
) -> Option<Vec<(String, Value)>> {
    img.map(|cols| {
        cols.iter()
            .enumerate()
            .filter_map(|(i, v)| v.clone().map(|v| (column_name(c, names, i), v)))
            .collect()
    })
}

fn cond(cols: &[(String, Value)]) -> String {
    cols.iter()
        .map(|(n, v)| {
            if v.is_null() {
                format!("{} IS NULL", ident(n))
            } else {
                format!("{} = {}", ident(n), value_sql(v))
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The JSON-lines rows of the extract. `columns` maps `schema.table` to
/// its column names in ordinal order.
#[must_use]
pub fn rows_of(txns: &[RawTxn], columns: &BTreeMap<String, Vec<String>>) -> Vec<ExtractRow> {
    let mut out = Vec::new();
    for t in txns {
        for c in &t.changes {
            let names = columns.get(&format!("{}.{}", c.schema, c.table));
            let before = image(c, names, c.before.as_ref());
            let after = image(c, names, c.after.as_ref());
            let table = format!("{}.{}", ident(&c.schema), ident(&c.table));
            let (kind, sql) = match c.kind {
                RowsKind::Insert => {
                    let a = after.clone().unwrap_or_default();
                    (
                        "insert",
                        format!(
                            "INSERT INTO {table} ({}) VALUES ({});",
                            a.iter().map(|(n, _)| ident(n)).collect::<Vec<_>>().join(", "),
                            a.iter().map(|(_, v)| value_sql(v)).collect::<Vec<_>>().join(", ")
                        ),
                    )
                }
                RowsKind::Update => {
                    let a = after.clone().unwrap_or_default();
                    let b = before.clone().unwrap_or_default();
                    (
                        "update",
                        format!(
                            "UPDATE {table} SET {} WHERE {} LIMIT 1;",
                            a.iter()
                                .map(|(n, v)| format!("{} = {}", ident(n), value_sql(v)))
                                .collect::<Vec<_>>()
                                .join(", "),
                            cond(&b)
                        ),
                    )
                }
                RowsKind::Delete => {
                    let b = before.clone().unwrap_or_default();
                    ("delete", format!("DELETE FROM {table} WHERE {} LIMIT 1;", cond(&b)))
                }
            };
            let map = |img: Option<Vec<(String, Value)>>| {
                img.map(|cols| {
                    cols.iter()
                        .map(|(n, v)| (n.clone(), value_json(v)))
                        .collect::<serde_json::Map<_, _>>()
                })
            };
            out.push(ExtractRow {
                gtid: t.gtid.to_string(),
                at: t.at,
                schema: c.schema.clone(),
                table: c.table.clone(),
                kind: kind.into(),
                before: map(before),
                after: map(after),
                sql,
            });
        }
        for (db, sql) in &t.statements {
            out.push(ExtractRow {
                gtid: t.gtid.to_string(),
                at: t.at,
                schema: db.clone(),
                table: String::new(),
                kind: "statement".into(),
                before: None,
                after: None,
                sql: sql.clone(),
            });
        }
    }
    out
}

/// The header line of a sealed extract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractHeader {
    /// The tenant whose key seals it.
    pub tenant: u64,
    /// The master key version.
    pub key_version: u32,
    /// The cluster.
    pub cluster: String,
    /// The diverged node.
    pub node: String,
    /// The job.
    pub job_id: String,
}

fn aad(header: &[u8], index: u64, last: bool) -> Vec<u8> {
    let mut a = Vec::with_capacity(header.len() + 9);
    a.extend_from_slice(header);
    a.extend_from_slice(&index.to_be_bytes());
    a.push(u8::from(last));
    a
}

fn aead(key: &[u8; 32]) -> Result<LessSafeKey> {
    let k = UnboundKey::new(&AES_256_GCM, key).map_err(|_| bad("bad extract key"))?;
    Ok(LessSafeKey::new(k))
}

/// Seals `plain` (JSON lines) under `key`.
///
/// # Errors
/// The key or the random source failed.
pub fn seal(key: &[u8; 32], header: &ExtractHeader, plain: &[u8]) -> Result<Vec<u8>> {
    let k = aead(key)?;
    let mut h = serde_json::to_vec(header).map_err(|e| bad(e.to_string()))?;
    h.push(b'\n');
    let mut out = Vec::with_capacity(plain.len() + 64);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&h);
    let chunks: Vec<&[u8]> = if plain.is_empty() {
        vec![&[]]
    } else {
        plain.chunks(FRAME).collect()
    };
    let n = chunks.len();
    for (i, chunk) in chunks.into_iter().enumerate() {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).map_err(|e| bad(format!("random nonce: {e}")))?;
        let mut buf = chunk.to_vec();
        k.seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad(&h, i as u64, i + 1 == n)),
            &mut buf,
        )
        .map_err(|_| bad("seal failed"))?;
        let len = u32::try_from(buf.len()).map_err(|_| bad("frame too large"))?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&buf);
    }
    Ok(out)
}

/// The header of a sealed extract, without opening it (to pick the key).
///
/// # Errors
/// Not a sealed extract.
pub fn header_of(sealed: &[u8]) -> Result<ExtractHeader> {
    let rest = sealed
        .strip_prefix(MAGIC)
        .ok_or_else(|| bad("not a SprigDB errant extract"))?;
    let nl = rest
        .iter()
        .position(|b| *b == b'\n')
        .ok_or_else(|| bad("extract header cut short"))?;
    serde_json::from_slice(&rest[..nl]).map_err(|e| bad(format!("extract header: {e}")))
}

/// Opens a sealed extract: its header and its JSON lines.
///
/// # Errors
/// Not a sealed extract, the wrong key, or a file changed, reordered or
/// cut short.
pub fn open(key: &[u8; 32], sealed: &[u8]) -> Result<(ExtractHeader, Vec<u8>)> {
    let header = header_of(sealed)?;
    let rest = &sealed[MAGIC.len()..];
    let nl = rest.iter().position(|b| *b == b'\n').unwrap_or(0);
    let h = &rest[..=nl];
    let mut p = &rest[nl + 1..];
    let k = aead(key)?;
    let mut out = Vec::new();
    let mut i: u64 = 0;
    let mut ended = false;
    while !p.is_empty() {
        if ended || p.len() < 4 + NONCE_LEN {
            return Err(bad("extract frames malformed"));
        }
        let len = u32::from_be_bytes([p[0], p[1], p[2], p[3]]) as usize;
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&p[4..4 + NONCE_LEN]);
        let body = p
            .get(4 + NONCE_LEN..4 + NONCE_LEN + len)
            .ok_or_else(|| bad("extract frame cut short"))?;
        let next = &p[4 + NONCE_LEN + len..];
        let last = next.is_empty();
        let mut buf = body.to_vec();
        let plain = k
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad(h, i, last)),
                &mut buf,
            )
            .map_err(|_| bad("the extract doesn't open: wrong key, or changed or cut short"))?;
        out.extend_from_slice(plain);
        ended = last;
        i += 1;
        p = next;
    }
    if !ended {
        return Err(bad("the extract has no final frame"));
    }
    Ok((header, out))
}

/// SHA-256, hex.
#[must_use]
pub fn sha256_hex(b: &[u8]) -> String {
    hex(&Sha256::digest(b))
}

/// Where extracts are kept.
#[async_trait]
pub trait ExtractStore: Send + Sync {
    /// Writes `bytes` at `rel` (a `/`-separated relative path); returns its
    /// location.
    async fn put(&self, rel: &str, bytes: &[u8]) -> Result<String>;
    /// Reads what [`ExtractStore::put`] wrote at `location`.
    async fn get(&self, location: &str) -> Result<Vec<u8>>;
    /// The location of directory `rel`, for reports.
    fn location(&self, rel: &str) -> String;
}

/// A local directory as an extract store (`[worker] extract_dir`).
#[derive(Debug, Clone)]
pub struct LocalDirStore {
    root: PathBuf,
}

impl LocalDirStore {
    /// A store under `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, rel: &str) -> Result<PathBuf> {
        let mut p = self.root.clone();
        for part in rel.split('/') {
            if part.is_empty() || part == "." || part == ".." {
                return Err(bad(format!("bad extract path {rel}")));
            }
            p.push(part);
        }
        Ok(p)
    }
}

#[async_trait]
impl ExtractStore for LocalDirStore {
    async fn put(&self, rel: &str, bytes: &[u8]) -> Result<String> {
        let p = self.path(rel)?;
        if let Some(d) = p.parent() {
            tokio::fs::create_dir_all(d)
                .await
                .map_err(|e| bad(format!("{}: {e}", d.display())))?;
        }
        // Written aside, then renamed: a reader never sees half a file.
        let tmp = p.with_extension("partial");
        tokio::fs::write(&tmp, bytes)
            .await
            .map_err(|e| bad(format!("{}: {e}", tmp.display())))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ =
                tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).await;
        }
        tokio::fs::rename(&tmp, &p)
            .await
            .map_err(|e| bad(format!("{}: {e}", p.display())))?;
        Ok(p.display().to_string())
    }

    async fn get(&self, location: &str) -> Result<Vec<u8>> {
        let p = Path::new(location);
        if !p.starts_with(&self.root) {
            return Err(bad(format!("{location} isn't in this store")));
        }
        tokio::fs::read(p)
            .await
            .map_err(|e| bad(format!("{location}: {e}")))
    }

    fn location(&self, rel: &str) -> String {
        self.path(rel)
            .map_or_else(|_| rel.to_owned(), |p| p.display().to_string())
    }
}

/// Counts row changes per `schema.table` in JSON lines (the verification).
///
/// # Errors
/// A line that isn't an [`ExtractRow`].
pub fn count_rows(lines: &[u8]) -> Result<BTreeMap<String, u64>> {
    let mut m = BTreeMap::new();
    for line in lines.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let r: ExtractRow =
            serde_json::from_slice(line).map_err(|e| bad(format!("extract line: {e}")))?;
        if r.kind != "statement" {
            *m.entry(format!("{}.{}", r.schema, r.table)).or_default() += 1;
        }
    }
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> ExtractHeader {
        ExtractHeader {
            tenant: 42,
            key_version: 1,
            cluster: "fra1-c1".into(),
            node: "db1".into(),
            job_id: "j".into(),
        }
    }

    #[test]
    fn a_sealed_extract_opens_only_whole_and_with_its_key() {
        let key = [7u8; 32];
        let plain: Vec<u8> = (0..(3 * FRAME + 17)).map(|i| (i % 251) as u8).collect();
        let sealed = seal(&key, &header(), &plain).expect("seal");
        assert_eq!(header_of(&sealed).expect("header"), header());
        let (h, back) = open(&key, &sealed).expect("open");
        assert_eq!(h.tenant, 42);
        assert_eq!(back, plain);
        assert!(open(&[8u8; 32], &sealed).is_err(), "another key");
        // Cut after the first frame: no final frame.
        let first = MAGIC.len() + sealed[MAGIC.len()..].iter().position(|b| *b == b'\n').expect("nl") + 1;
        let len = u32::from_be_bytes(sealed[first..first + 4].try_into().expect("len")) as usize;
        let cut = &sealed[..first + 4 + NONCE_LEN + len];
        assert!(open(&key, cut).is_err(), "cut short");
        let mut flipped = sealed.clone();
        let n = flipped.len();
        flipped[n - 20] ^= 1;
        assert!(open(&key, &flipped).is_err(), "changed");
        // An empty extract still has its final frame.
        let empty = seal(&key, &header(), b"").expect("seal");
        assert_eq!(open(&key, &empty).expect("open").1, Vec::<u8>::new());
    }

    #[test]
    fn rows_turn_into_json_and_sql() {
        let gtid: Gtid = "1-101-7".parse().expect("gtid");
        let t = RawTxn {
            gtid,
            at: 1_700_000_000,
            changes: vec![
                RawChange {
                    schema: "t42_d1_g1_s1".into(),
                    table: "la".into(),
                    kind: RowsKind::Insert,
                    before: None,
                    after: Some(vec![Some(Value::Int(3)), Some(Value::String(b"it's".to_vec()))]),
                    names: vec![None, None],
                },
                RawChange {
                    schema: "t42_d1_g1_s1".into(),
                    table: "la".into(),
                    kind: RowsKind::Update,
                    before: Some(vec![Some(Value::Int(3)), Some(Value::Null)]),
                    after: Some(vec![Some(Value::Int(3)), Some(Value::Bytes(vec![0, 255]))]),
                    names: vec![Some("k".into()), Some("v".into())],
                },
                RawChange {
                    schema: "__sprig_sys".into(),
                    table: "hb".into(),
                    kind: RowsKind::Delete,
                    before: Some(vec![Some(Value::UInt(9))]),
                    after: None,
                    names: vec![None],
                },
            ],
            statements: vec![],
        };
        let cols = BTreeMap::from([("t42_d1_g1_s1.la".to_owned(), vec!["k".to_owned(), "v".to_owned()])]);
        let rows = rows_of(&[t], &cols);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].sql, "INSERT INTO `t42_d1_g1_s1`.`la` (`k`, `v`) VALUES (3, 'it''s');");
        assert_eq!(rows[0].after.as_ref().expect("after")["v"], json!("it's"));
        assert_eq!(
            rows[1].sql,
            "UPDATE `t42_d1_g1_s1`.`la` SET `k` = 3, `v` = X'00ff' WHERE `k` = 3 AND `v` IS NULL LIMIT 1;"
        );
        assert_eq!(rows[2].sql, "DELETE FROM `__sprig_sys`.`hb` WHERE `@1` = 9 LIMIT 1;");
        assert_eq!(tenant_of(&rows[0].schema), 42);
        assert_eq!(tenant_of(&rows[2].schema), 0);
        assert_eq!(tenant_of("test"), 0);
        let lines: Vec<u8> = rows
            .iter()
            .flat_map(|r| {
                let mut l = serde_json::to_vec(r).expect("json");
                l.push(b'\n');
                l
            })
            .collect();
        let counts = count_rows(&lines).expect("count");
        assert_eq!(counts["t42_d1_g1_s1.la"], 2);
        assert_eq!(counts["__sprig_sys.hb"], 1);
    }

    #[test]
    fn the_dump_starts_just_before_the_range() {
        let r = DivergedRange {
            domain: 1,
            server: 101,
            after_seq: 40,
            through_seq: 43,
        };
        assert_eq!(start_position(&r, "1-101-40,2-7-9").to_string(), "1-101-40");
        assert_eq!(start_position(&r, "").to_string(), "1-101-40");
        let fresh = DivergedRange { after_seq: 0, ..r };
        assert_eq!(start_position(&fresh, "").to_string(), "");
    }

    #[tokio::test]
    async fn the_local_store_keeps_paths_inside_its_root() {
        let dir = std::env::temp_dir().join(format!("sprig-extract-{}", std::process::id()));
        let s = LocalDirStore::new(&dir);
        let loc = s.put("fra1-c1/db1/1/t42.extract", b"x").await.expect("put");
        assert_eq!(s.get(&loc).await.expect("get"), b"x");
        assert!(s.put("../escape", b"x").await.is_err());
        assert!(s.get("/etc/passwd").await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
