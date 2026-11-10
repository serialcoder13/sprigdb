//! Fan-in reads from SQL (chapter 42, "Fan-in: reading every member's
//! instance"):
//!
//! ```text
//! SELECT ... FROM [db.]s FAN IN [WHERE tenant IN ('a', 'b') ...]
//!   in an ancestor P of the members (the declarer or an intermediate
//!   member), with SELECT on s and SPRIG_FAN_IN (granted by name)
//!   the members: P's descendants (P too when it is one) holding an instance
//!     of the stream definition, inside the statement's `tenant IN (...)` or
//!     `tenant = '...'` filter when it has one; more than
//!     inherit.fan_in_sql_max_tenants: 1104 (name the members)
//!   a member whose residency sets residency.deny_remote_access is left out
//!     (counted in sprig_inherit_fanin_skipped_total)
//!   each member's newest FAN_IN_ROWS records, read as the provisioning
//!   account on its cluster, become one derived table with the columns
//!   tenant, partition_no, offset_no, ts, key, value, headers, which the
//!   statement reads in place of `s FAN IN` (members' row-level security
//!   doesn't bind it: Kiran's answer 5)
//! ```

#![allow(clippy::too_many_lines, clippy::items_after_statements)]

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};
use sprig_inherit::msg::{Chain, DefKind, Definition};
use sprig_sql::lex::{LexOptions, TokenKind, ident_value, lex, string_value};
use sprig_streams::StreamsState;

use super::catalog;
use crate::tenant::TenantServices;

/// Records read per member: the newest ones.
pub const FAN_IN_ROWS: usize = 1000;

/// `[db.]s FAN IN` in a statement: its bytes and the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanInRef {
    /// The bytes of `[db.]s FAN IN`.
    pub range: std::ops::Range<usize>,
    /// `db.`
    pub db: Option<String>,
    /// The stream.
    pub name: String,
}

/// The first `[db.]s FAN IN` of `sql`, and the tenants its `tenant IN
/// (...)` or `tenant = '...'` names (empty: none named).
#[must_use]
pub fn find(sql: &str, opts: &LexOptions) -> Option<(FanInRef, Vec<String>)> {
    let toks: Vec<_> = lex(sql.as_bytes(), opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let word = |i: usize, w: &str| {
        toks.get(i)
            .is_some_and(|t| t.kind == TokenKind::Word && sql[t.range()].eq_ignore_ascii_case(w))
    };
    let ident = |i: usize| {
        toks.get(i)
            .filter(|t| t.kind.is_ident_like())
            .map(|t| String::from_utf8_lossy(&ident_value(t.bytes(sql.as_bytes()))).into_owned())
    };
    let punct = |i: usize, p: &str| toks.get(i).is_some_and(|t| &sql[t.range()] == p);
    let mut found = None;
    for i in 0..toks.len() {
        if !(word(i, "fan") && word(i + 1, "in")) || i == 0 {
            continue;
        }
        // `name FAN IN` or `db . name FAN IN`.
        let name = ident(i - 1)?;
        let (start, db) = if i >= 3 && punct(i - 2, ".") {
            (toks[i - 3].start as usize, ident(i - 3))
        } else {
            (toks[i - 1].start as usize, None)
        };
        found = Some(FanInRef {
            range: start..toks[i + 1].end as usize,
            db,
            name,
        });
        break;
    }
    let found = found?;
    let mut tenants = Vec::new();
    for i in 0..toks.len() {
        if !word(i, "tenant") {
            continue;
        }
        let lit = |j: usize| {
            toks.get(j)
                .filter(|t| t.kind == TokenKind::String)
                .and_then(|t| string_value(t.bytes(sql.as_bytes()), false))
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        };
        if punct(i + 1, "=") {
            if let Some(v) = lit(i + 2) {
                tenants.push(v);
            }
        } else if word(i + 1, "in") && punct(i + 2, "(") {
            let mut j = i + 3;
            while let Some(v) = lit(j) {
                tenants.push(v);
                j += 1;
                if !punct(j, ",") {
                    break;
                }
                j += 1;
            }
        }
    }
    Some((found, tenants))
}

fn lit(b: Option<&[u8]>) -> String {
    b.map_or_else(|| "NULL".to_owned(), sprig_queues::sql::hex_literal)
}

/// The derived table standing for `def`'s instances below `reader` (see
/// the module docs), named `alias`; and the members read.
///
/// # Errors
/// 1104 over `inherit.fan_in_sql_max_tenants`; the members' clusters.
pub async fn derived(
    svc: &Arc<TenantServices>,
    reader: TenantId,
    def: &Definition,
    only: &[String],
    max: u64,
    alias: &str,
) -> Result<(String, usize)> {
    let mut members = Vec::new();
    for e in super::instances_of(svc, def.key()).await? {
        if e.kind != DefKind::Stream {
            continue;
        }
        let Some(d) = super::dir_entry(svc, e.tenant).await? else {
            continue;
        };
        if e.tenant != reader && !d.path_ids().contains(&reader.0) {
            continue;
        }
        if !only.is_empty() && !only.contains(&d.name) {
            continue;
        }
        if sprig_auth::TenantResidency::of(&d.residency).is_some_and(|r| r.deny_remote_access) {
            metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
                .increment(1);
            continue;
        }
        members.push((e, d.name));
    }
    if members.len() as u64 > max {
        return Err(SprigError::standard(
            1104,
            *b"42000",
            format!(
                "The fan-in read would read {} members' instances; inherit.fan_in_sql_max_tenants \
                 is {max}: name them with WHERE tenant IN (...)",
                members.len()
            ),
        ));
    }
    let mut selects = Vec::new();
    for (e, tname) in &members {
        let cat = catalog(svc, e.tenant).await?;
        let Some(s) = cat
            .ext
            .get::<StreamsState>(sprig_streams::MODULE)
            .and_then(|st| st.all().find(|s| s.id.0 == e.instance_id).cloned())
        else {
            continue;
        };
        let Some(log) = s.log.first() else { continue };
        let schema = log.slot.schema(e.tenant, s.db).to_string();
        let cluster = svc.locator.cluster(e.tenant).await?;
        let pool = svc.locator.ctl_pool(&cluster).await?;
        let mut c = pool
            .get_conn()
            .await
            .map_err(sprig_streams::maria::map_err)?;
        type R = (
            u32,
            u64,
            Option<String>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<String>,
        );
        let rows: Vec<R> = c
            .query(format!(
                "SELECT partition_no, offset_no, CAST(ts AS CHAR), k, v, CAST(headers AS CHAR) \
                 FROM {}.{} ORDER BY ts DESC LIMIT {FAN_IN_ROWS}",
                quote(&schema),
                quote(&s.log_table())
            ))
            .await
            .map_err(sprig_streams::maria::map_err)?;
        let tl = sprig_queues::sql::hex_literal(tname.as_bytes());
        for (p, o, ts, k, v, h) in rows {
            selects.push(format!(
                "SELECT CONVERT({tl} USING utf8mb4) AS tenant, {p} AS partition_no, {o} AS \
                 offset_no, {} AS ts, {} AS `key`, {} AS `value`, {} AS headers",
                ts.map_or_else(
                    || "NULL".to_owned(),
                    |t| format!(
                        "CAST({} AS DATETIME(6))",
                        sprig_queues::sql::hex_literal(t.as_bytes())
                    )
                ),
                lit(k.as_deref()),
                lit(v.as_deref()),
                h.map_or_else(
                    || "NULL".to_owned(),
                    |h| format!(
                        "CONVERT({} USING utf8mb4)",
                        sprig_queues::sql::hex_literal(h.as_bytes())
                    )
                ),
            ));
        }
    }
    if selects.is_empty() {
        // No records: an empty table with the columns.
        selects.push(
            "SELECT CAST(NULL AS CHAR(64)) AS tenant, 0 AS partition_no, 0 AS offset_no, \
             CAST(NULL AS DATETIME(6)) AS ts, CAST(NULL AS BINARY) AS `key`, CAST(NULL AS \
             BINARY) AS `value`, CAST(NULL AS CHAR) AS headers FROM DUAL WHERE FALSE"
                .to_owned(),
        );
    }
    Ok((
        format!("({}) AS {}", selects.join(" UNION ALL "), quote(alias)),
        members.len(),
    ))
}

/// The stream definition `[db.]name` a fan-in read in `chain`'s member
/// names: its own, or one it is a member of (an intermediate tenant).
#[must_use]
pub fn definition(
    chain: &Chain,
    own: &sprig_inherit::msg::MsgState,
    db: &str,
    name: &str,
) -> Option<Arc<Definition>> {
    own.def(db, DefKind::Stream, name)
        .cloned()
        .or_else(|| chain.resolve(db, DefKind::Stream, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_reference_and_the_tenant_filter() {
        let o = LexOptions::mariadb();
        let sql = "SELECT tenant, value FROM security.user_events FAN IN WHERE tenant IN ('a', 'b') ORDER BY ts";
        let (r, t) = find(sql, &o).unwrap();
        assert_eq!(&sql[r.range.clone()], "security.user_events FAN IN");
        assert_eq!(r.db.as_deref(), Some("security"));
        assert_eq!(r.name, "user_events");
        assert_eq!(t, vec!["a".to_owned(), "b".to_owned()]);
        let sql = "SELECT COUNT(*) FROM s FAN IN WHERE tenant = 'x'";
        let (r, t) = find(sql, &o).unwrap();
        assert_eq!(r.db, None);
        assert_eq!(t, vec!["x".to_owned()]);
        assert!(find("SELECT * FROM s", &o).is_none());
    }
}
