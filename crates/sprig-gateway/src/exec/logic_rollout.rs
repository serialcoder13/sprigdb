//! Staged rollouts at call time (`sprig_logic::rollout`): which version a
//! `CALL` runs, its numbers, and the automatic rollback.
//!
//! ```text
//! CALL [db.]f(...) in a tenant session:
//!   rollouts of the tenant (cached 5 s; a deploy here clears the cache)
//!   f has one → the version: the marker's (engine tier), or the same pick
//!               the wrapper makes (compiled), from the caller's account
//!   count the call (version, failed?, elapsed) in this gateway
//!   every 5 s per tenant: add the counts to logic_version_stats, then
//!   compare candidate and active (sprig_logic::rollout::verdict); a
//!   candidate that does worse is rolled back here, as ROLLOUT ABORT run
//!   by SprigDB for the module's owner, and logic_log says why
//! ```

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_catalog::CatalogLocator;
use sprig_logic::rollout::{self, Acc, Rollout};

use super::PassSession;
use crate::pipeline::ExecCtx;

/// How long a tenant's rollouts are cached.
const CACHE_TTL: Duration = Duration::from_secs(5);
/// How often a gateway adds a tenant's counts to the catalog.
const FLUSH_EVERY: Duration = Duration::from_secs(5);

/// One rollout as calls find it.
#[derive(Debug, Clone)]
struct Entry {
    db_id: u32,
    name: String,
    rollout: Rollout,
}

type Cache = HashMap<u64, (Instant, Arc<Vec<Entry>>)>;

static CACHE: LazyLock<Mutex<Cache>> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Default)]
struct TenantCounts {
    last_flush: Option<Instant>,
    accs: HashMap<(u64, u64), Acc>,
}

static COUNTS: LazyLock<Mutex<HashMap<u64, TenantCounts>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Forgets a tenant's cached rollouts (after a deploy, rollout change or
/// drop through this gateway).
pub(super) fn invalidate(tenant: u64) {
    if let Ok(mut c) = CACHE.lock() {
        c.remove(&tenant);
    }
}

/// The routine a `CALL` names: `(database, name)`, unquoted.
fn call_target(text: &[u8]) -> Option<(Option<String>, String)> {
    let s = std::str::from_utf8(text.get(..text.len().min(512))?).ok()?;
    let mut t = s.trim_start();
    while let Some(r) = t.strip_prefix("/*") {
        t = r.split_once("*/")?.1.trim_start();
    }
    let head = t.get(..4)?;
    if !head.eq_ignore_ascii_case("call") {
        return None;
    }
    let after = t[4..].chars().next()?;
    if !(after.is_whitespace() || after == '`') {
        return None;
    }
    let rest = t[4..].trim_start();
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for c in rest.chars() {
        match c {
            '`' => quoted = !quoted,
            '.' if !quoted => parts.push(std::mem::take(&mut cur)),
            c if !quoted && (c == '(' || c == ';' || c.is_whitespace()) => break,
            c => cur.push(c),
        }
    }
    parts.push(cur);
    match parts.as_slice() {
        [n] if !n.is_empty() => Some((None, n.clone())),
        [d, n] if !n.is_empty() => Some((Some(d.clone()), n.clone())),
        _ => None,
    }
}

impl PassSession {
    /// The tenant's rollouts, from the cache or the catalog.
    async fn rollouts(&mut self) -> Arc<Vec<Entry>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Arc::new(Vec::new());
        };
        let tenant = ts.tenant;
        if let Ok(c) = CACHE.lock()
            && let Some((at, v)) = c.get(&tenant.0)
            && at.elapsed() < CACHE_TTL
        {
            return Arc::clone(v);
        }
        let svc = Arc::clone(ts.services());
        let loaded = async {
            let ep = svc.locator.writer(tenant).await.ok()?;
            let mut conn = ep.pool.get_conn().await.ok()?;
            rollout::all(&mut conn, ep.store.schema()).await.ok()
        }
        .await;
        let list = Arc::new(
            loaded
                .unwrap_or_default()
                .into_iter()
                .map(|(db_id, name, r)| Entry {
                    db_id,
                    name: name.to_ascii_lowercase(),
                    rollout: r,
                })
                .collect::<Vec<_>>(),
        );
        if let Ok(mut c) = CACHE.lock() {
            c.insert(tenant.0, (Instant::now(), Arc::clone(&list)));
        }
        list
    }

    /// For a `CALL` of a function in a staged rollout: its module, and the
    /// version the wrapper will pick for this caller.
    pub(super) async fn rollout_pick(&mut self, original: &[u8]) -> Option<(u64, u64)> {
        if super::logic_test::in_test_run() {
            return None;
        }
        let (db, name) = call_target(original)?;
        let list = self.rollouts().await;
        if list.is_empty() {
            return None;
        }
        let current = self.current_logical();
        let ts = self.tenant.as_deref()?;
        let cat = ts.catalog()?;
        let db = db.or(current)?;
        let db_id = cat
            .database(&db)
            .or_else(|| cat.database(&db.to_ascii_lowercase()))?
            .db_id
            .0;
        let name = name.to_ascii_lowercase();
        let e = list.iter().find(|e| e.db_id == db_id && e.name == name)?;
        let account = sprig_auth::authz::backend::account(ts.tenant, ts.principal.user_id?);
        let cand =
            rollout::picks_candidate(e.rollout.spec(), &account, ts.tenant.0, e.rollout.module_id);
        Some((
            e.rollout.module_id,
            if cand {
                e.rollout.candidate_version
            } else {
                e.rollout.active_version
            },
        ))
    }

    /// Counts one call of `module` `version` (only modules in a rollout),
    /// and every [`FLUSH_EVERY`] adds the tenant's counts to the catalog
    /// and checks the candidates.
    pub(super) async fn rollout_count(
        &mut self,
        cx: &mut ExecCtx<'_>,
        module: u64,
        version: u64,
        elapsed: Duration,
        failed: bool,
    ) {
        let Some(tenant) = self.tenant.as_deref().map(|ts| ts.tenant) else {
            return;
        };
        let due = {
            let Ok(mut all) = COUNTS.lock() else {
                return;
            };
            let t = all.entry(tenant.0).or_default();
            t.accs
                .entry((module, version))
                .or_default()
                .add(elapsed, failed);
            let due = t.last_flush.is_none_or(|l| l.elapsed() >= FLUSH_EVERY);
            if due {
                t.last_flush = Some(Instant::now());
                Some(std::mem::take(&mut t.accs))
            } else {
                None
            }
        };
        let Some(accs) = due else {
            return;
        };
        if let Err(e) = self.rollout_flush(cx, accs).await {
            tracing::warn!(error = %e, "logic rollout: the numbers weren't added");
        }
    }

    async fn rollout_flush(
        &mut self,
        cx: &mut ExecCtx<'_>,
        accs: HashMap<(u64, u64), Acc>,
    ) -> sprig_common::Result<()> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(());
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let catalog = ts.catalog().cloned();
        let ep = svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            sprig_common::SprigError::new(
                sprig_common::SprigCode::CatalogUnavailable,
                "catalog unreachable",
            )
            .with_source(e)
        })?;
        let schema = ep.store.schema().to_owned();
        let rows: Vec<((u64, u64), Acc)> = accs.into_iter().collect();
        rollout::flush(&mut conn, &schema, &rows).await?;
        let mut modules: Vec<u64> = rows.iter().map(|((m, _), _)| *m).collect();
        modules.sort_unstable();
        modules.dedup();
        let store = sprig_logic::LogicStore::new(&schema);
        for m in modules {
            let Some(r) = rollout::get(&mut conn, &schema, m).await? else {
                continue;
            };
            let stats = rollout::stats(&mut conn, &schema, m).await?;
            let Some(cand) = stats.iter().find(|s| s.version == r.candidate_version) else {
                continue;
            };
            let active = stats.iter().find(|s| s.version == r.active_version);
            let Some(why) = rollout::verdict(active, cand) else {
                continue;
            };
            let Some(module) = store.module_by_id(&mut conn, m).await? else {
                continue;
            };
            let Some(db) = catalog
                .as_ref()
                .and_then(|c| c.databases.get(sprig_common::ids::DbId(module.db_id)))
                .map(|d| d.name.clone())
            else {
                continue;
            };
            let text = format!(
                "ALTER {} `{}`.`{}` ROLLOUT ABORT",
                module.kind.to_ascii_uppercase(),
                db.replace('`', "``"),
                module.name.replace('`', "``")
            );
            tracing::warn!(module = m, candidate = r.candidate_version, %why, "logic rollout: rolling back the candidate");
            let owner = sprig_common::ids::UserId(module.created_by);
            let done =
                Box::pin(self.logic_deploy_with(cx, &Bytes::from(text.into_bytes()), Some(owner)))
                    .await;
            invalidate(tenant.0);
            let (level, msg) = match &done {
                Ok(_) => (
                    "warn",
                    format!(
                        "rollout of version {} rolled back by itself (error 39870): {why}; version {} \
                         answers every caller",
                        r.candidate_version, r.active_version
                    ),
                ),
                Err(e) => (
                    "error",
                    format!(
                        "rollout of version {} should roll back ({why}) but couldn't: {}",
                        r.candidate_version, e.message
                    ),
                ),
            };
            let _ = store
                .log(
                    &mut conn,
                    m,
                    Some(r.candidate_version),
                    level,
                    "deploy",
                    &msg,
                )
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn call_targets() {
        assert_eq!(
            call_target(b"CALL shop.quote(7, 3)"),
            Some((Some("shop".into()), "quote".into()))
        );
        assert_eq!(
            call_target(b"  /* x */ call `my.db`.`q`()"),
            Some((Some("my.db".into()), "q".into()))
        );
        assert_eq!(call_target(b"CALL quote"), Some((None, "quote".into())));
        assert_eq!(call_target(b"SELECT 1"), None);
        assert_eq!(call_target(b"CALLX f()"), None);
    }
}
