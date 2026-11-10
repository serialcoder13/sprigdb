//! 3306: MySQL 8.4's `ONLY_FULL_GROUP_BY` and its two `ORDER BY` rules
//! (chapter 28, build step 8), applied before a tenant's statement is sent.
//!
//! The rule itself is `sprig_sql::compat::groupby`. This module gives it
//! table definitions: columns, nullability and unique keys, read from
//! MariaDB's `information_schema` on the session's own backend connection
//! (the backend account sees only its tenant's schemas), in one query for
//! every table a statement names. They are kept per gateway process by
//! tenant, logical database and table, for the tenant's catalog version
//! and at most [`TTL`].
//!
//! Most grouped queries need no definitions: the checker's first pass
//! accepts a query whose every expression is grouped by its text. A table
//! whose definition can't be read (a sharded database, a temporary table,
//! a name with quotes) counts as unknown, and the checker then accepts.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::client::PacketKind;
use sprig_sql::compat::groupby::{self, Check, ColumnMeta, Meta, Outcome, TableMeta};
use sprig_sql::lex::LexOptions;

use super::PassSession;
use crate::backend::StmtInfo;
use crate::pipeline::ExecCtx;

/// How long a definition is kept, at most.
const TTL: Duration = Duration::from_secs(300);
/// The cache is emptied when it holds more entries than this.
const MAX_ENTRIES: usize = 50_000;

type Key = (TenantId, String, String);
/// A table (logical database, table) and its definition, `None` when unreadable.
type Found = ((String, String), Option<Arc<TableMeta>>);
/// Raw rows per table: columns, key name to column names, a view.
type Raw = (Vec<ColumnMeta>, Vec<(String, Vec<String>)>, bool);

struct Entry {
    version: u64,
    at: Instant,
    meta: Option<Arc<TableMeta>>,
}

static CACHE: LazyLock<Mutex<HashMap<Key, Entry>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// The cache as the checker reads it, for one tenant at one catalog
/// version.
struct Cached {
    tenant: TenantId,
    version: u64,
}

impl Meta for Cached {
    fn table(&self, db: &str, table: &str) -> Option<Option<Arc<TableMeta>>> {
        let cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
        let e = cache.get(&(self.tenant, db.to_owned(), table.to_owned()))?;
        (e.version == self.version && e.at.elapsed() < TTL).then(|| e.meta.clone())
    }
}

fn store(tenant: TenantId, version: u64, found: Vec<Found>) {
    let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
    if cache.len() > MAX_ENTRIES {
        cache.clear();
    }
    let at = Instant::now();
    for ((db, table), meta) in found {
        cache.insert((tenant, db, table), Entry { version, at, meta });
    }
}

/// A name that can go in a string literal as is, under any `sql_mode`.
fn plain(s: &str) -> bool {
    !s.is_empty() && !s.contains(['\'', '\\']) && !s.chars().any(char::is_control)
}

impl PassSession {
    /// The 3306 check of one tenant statement (see the module docs):
    /// `Some(error)` when MySQL would refuse it.
    pub(super) async fn group_check(
        &mut self,
        cx: &ExecCtx<'_>,
        text: &[u8],
        lex: &LexOptions,
    ) -> Result<Option<SprigError>> {
        if self.persona != Persona::MySql84 || !groupby::may_need(text) {
            return Ok(None);
        }
        let only_full_group_by = self
            .bs
            .as_ref()
            .and_then(|b| b.vars().client_sql_mode.as_deref())
            .is_none_or(|m| {
                sprig_sql::compat::sqlmode::parse_mysql(m)
                    .is_ok_and(sprig_sql::compat::sqlmode::Mode::only_full_group_by)
            });
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        let cached = Cached {
            tenant: ts.tenant,
            version: ts.catalog().map_or(0, |c| c.version),
        };
        let mut loaded = false;
        loop {
            let outcome = {
                let c = Check {
                    current_db: current.as_deref(),
                    only_full_group_by,
                    meta: &cached,
                };
                groupby::check(text, lex, &c)
            };
            match outcome {
                Outcome::Refuse(r) => {
                    metrics::counter!("sprig_translator_rejected_total", "feature" => "only_full_group_by")
                        .increment(1);
                    let state: [u8; 5] = r.sqlstate.as_bytes().try_into().unwrap_or(*b"42000");
                    return Ok(Some(SprigError::standard(r.code, state, r.message)));
                }
                Outcome::Need(tables) if !loaded => {
                    loaded = true;
                    let found = self.read_definitions(cx, &tables).await;
                    store(cached.tenant, cached.version, found);
                }
                // Still missing after a read: accept (the module docs).
                Outcome::Accept | Outcome::Need(_) => return Ok(None),
            }
        }
    }

    /// Reads the definitions of `tables` (logical database, table) on the
    /// session's backend connection. Every table asked for gets an entry:
    /// `None` when it can't be read.
    async fn read_definitions(
        &mut self,
        cx: &ExecCtx<'_>,
        tables: &[(String, String)],
    ) -> Vec<Found> {
        let mut out: Vec<Found> = Vec::new();
        let mut asked: Vec<(String, String, (String, String))> = Vec::new();
        if let Some(ts) = self.tenant.as_deref() {
            for (db, table) in tables {
                match ts.schema_of(Some(db)) {
                    Some(phys) if plain(&phys) && plain(table) => {
                        asked.push((phys, table.clone(), (db.clone(), table.clone())));
                    }
                    _ => out.push(((db.clone(), table.clone()), None)),
                }
            }
        }
        if asked.is_empty() {
            return out;
        }
        let mut pairs = String::new();
        for (i, (s, t, _)) in asked.iter().enumerate() {
            let _ = write!(pairs, "{}('{s}','{t}')", if i == 0 { "" } else { "," });
        }
        let sql = format!(
            "SELECT 'c', c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.IS_NULLABLE, \
             CONCAT(c.EXTRA, '|', t.TABLE_TYPE), c.ORDINAL_POSITION \
             FROM information_schema.COLUMNS c JOIN information_schema.TABLES t \
             ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
             WHERE (c.TABLE_SCHEMA, c.TABLE_NAME) IN ({pairs}) \
             UNION ALL \
             SELECT 'k', TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, INDEX_NAME, '', SEQ_IN_INDEX \
             FROM information_schema.STATISTICS \
             WHERE NON_UNIQUE = 0 AND (TABLE_SCHEMA, TABLE_NAME) IN ({pairs}) \
             ORDER BY 1, 2, 3, 5, 7"
        );
        let rows = match self.backend_rows(cx, &sql).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "full-group-by: table definitions unreadable");
                out.extend(asked.into_iter().map(|(_, _, k)| (k, None)));
                return out;
            }
        };
        // (schema, table) -> its raw rows.
        let mut raw: HashMap<(String, String), Raw> = HashMap::new();
        for r in rows {
            let [kind, schema, table, col, a, b, _] = r.as_slice() else {
                continue;
            };
            let (Some(kind), Some(schema), Some(table), Some(col)) = (kind, schema, table, col)
            else {
                continue;
            };
            let e = raw.entry((schema.clone(), table.clone())).or_default();
            if kind == "c" {
                let extra = b.clone().unwrap_or_default();
                let (extra, ty) = extra.split_once('|').unwrap_or((&extra, ""));
                e.0.push(ColumnMeta {
                    name: col.clone(),
                    not_null: a.as_deref() == Some("NO"),
                    visible: !extra.to_ascii_uppercase().contains("INVISIBLE"),
                });
                e.2 = ty.eq_ignore_ascii_case("VIEW");
            } else {
                let key = a.clone().unwrap_or_default();
                match e.1.iter_mut().find(|(k, _)| *k == key) {
                    Some((_, cols)) => cols.push(col.clone()),
                    None => e.1.push((key, vec![col.clone()])),
                }
            }
        }
        for (phys, table, k) in asked {
            let meta = raw
                .remove(&(phys, table))
                .and_then(|(columns, keys, view)| {
                    if columns.is_empty() {
                        return None;
                    }
                    let keys = keys
                        .into_iter()
                        .filter_map(|(_, cols)| {
                            cols.iter()
                                .map(|c| columns.iter().position(|x| &x.name == c))
                                .collect::<Option<Vec<usize>>>()
                        })
                        .collect();
                    Some(Arc::new(TableMeta {
                        columns,
                        keys,
                        view,
                    }))
                });
            out.push((k, meta));
        }
        out
    }

    /// One statement of the gateway's own on the session's backend
    /// connection, its rows as text.
    pub(super) async fn backend_rows(
        &mut self,
        cx: &ExecCtx<'_>,
        sql: &str,
    ) -> Result<Vec<Vec<Option<String>>>> {
        let _ = self.bs(cx);
        let Some(bs) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        bs.query(&StmtInfo::read(), sql.as_bytes()).await?;
        let mut rows = Vec::new();
        let mut cols = 0usize;
        let mut err = None;
        while let Some(p) = bs.next().await? {
            match p.kind {
                PacketKind::ColumnCount(n) => cols = usize::try_from(n).unwrap_or(0),
                PacketKind::Row => {
                    let vals = sprig_proto_mysql::resultset::decode_text_row(&p.payload, cols)?;
                    rows.push(
                        vals.into_iter()
                            .map(|v| v.map(|b| String::from_utf8_lossy(b).into_owned()))
                            .collect(),
                    );
                }
                PacketKind::Err(e) => {
                    err = Some(SprigError::backend(e.code, e.sqlstate, e.message.clone()));
                }
                _ => {}
            }
        }
        let _ = bs.finish().await;
        match err {
            Some(e) => Err(e),
            None => Ok(rows),
        }
    }
}
