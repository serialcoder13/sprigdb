//! Chapter 12's workload store as the `auto_index` loop reads it: trigger
//! evaluation's window ([`StoreSource`]), the observer's latencies and
//! usage samples ([`ObserveSource`]), and the binding of a sealed sample
//! into a statement in the shards' physical names ([`bind_sample`]), which
//! validation replays.
//!
//! **Watching.** Chapter 13 asks chapter 12 to watch every qualifying
//! fingerprint (probe rows examined, capture samples). The recorder lives
//! in each gateway and the controller has no channel to it yet, so
//! [`StoreSource::watch`] only logs. Gateways watch on their own every
//! fingerprint whose p95 is above `workload.watch_p95_ms` (50 ms), which
//! covers the slow trigger (p95 above `auto_index.p95_threshold_ms`, 100
//! ms); a fingerprint qualifying only through the top-N or fast trigger
//! below that p95 is probed at `workload.examined_sample_rate` and gets
//! samples only once it is watched.
//!
//! **Names.** A fingerprint's text is in logical names with tables
//! qualified by their logical database; a sample's context holds the
//! session's logical database. Both are mapped with chapter 07's rewriter
//! over the tenant's [`NameMap`] (built from the catalog snapshot the way
//! the gateway builds it): R0 has one shard per database, so a database
//! stands for its first shard schema.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use sprig_advisor::autoindex::observe::{self, FpLatency, UsageSample};
use sprig_advisor::autoindex::qualify::{self, SourcedWindow};
use sprig_advisor::autoindex::replay::{
    ReplayStatement, SampleValue as ReplayValue, SessionContext, StatementRole, bind_literals,
};
use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_catalog::store::DbState;
use sprig_catalog::{CatalogLocator, ddl::shard_schemas};
use sprig_common::ids::{Fingerprint, ObjectId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::Persona;
use sprig_settings::Registry;
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{LogicalDb, MemoryNameMap, NameMap, Purpose, RewriteCtx, rewrite};
use sprig_workload::fingerprint::FpKind;
use sprig_workload::keyring::TenantKeyring;
use sprig_workload::samples::OpenedSample;
use sprig_workload::store::{WorkloadStore, WorkloadTables};
use sprig_workload::window::{FpWindowStats, SampledSlot};

/// The tenant's names from its catalog, as the gateway maps them
/// (`sprig-gateway`'s `tenant::names::name_map`): every live database with
/// its shard schemas.
#[must_use]
pub fn name_map(cat: &TenantCatalog) -> MemoryNameMap {
    let mut dbs: Vec<_> = cat
        .databases
        .iter()
        .filter(|d| d.state == DbState::Active)
        .collect();
    dbs.sort_by_key(|d| d.db_id);
    let mut map = MemoryNameMap::new(cat.tenant).with_version(cat.version);
    for d in dbs {
        map.insert(LogicalDb {
            id: d.db_id,
            name: d.name.clone(),
            shards: shard_schemas(cat.tenant, d),
            reference: None,
        });
    }
    map
}

/// The tenant's catalog snapshot.
///
/// # Errors
/// The catalog is unreachable.
pub async fn tenant_catalog(
    locator: &dyn CatalogLocator,
    tenant: TenantId,
) -> Result<TenantCatalog> {
    let ep = locator.writer(tenant).await?;
    let snap = ep.store.snapshot(&ep.pool, None).await?;
    Ok(TenantCatalog::build(
        Registry::global()?,
        tenant,
        Loaded::core(snap),
    ))
}

/// `sql_mode` flags the lexer needs.
fn lex_for(sql_mode: Option<&str>) -> LexOptions {
    let lex = LexOptions::for_persona(Persona::MariaDb);
    let Some(m) = sql_mode else { return lex };
    let m = m.to_ascii_uppercase();
    lex.ansi_quotes(m.contains("ANSI_QUOTES") || m.split(',').any(|x| x.trim() == "ANSI"))
        .no_backslash_escapes(m.contains("NO_BACKSLASH_ESCAPES"))
}

/// One sample's values, as replay binds them: each literal exactly as it
/// was captured ([`ReplayValue::Raw`]), a collapsed list as a list.
fn values_of(s: &OpenedSample) -> Vec<ReplayValue> {
    let raw = |v: &sprig_workload::window::SampleValue| {
        ReplayValue::Raw(String::from_utf8_lossy(&v.text).into_owned())
    };
    s.payload
        .literals
        .slots
        .iter()
        .map(|slot| match slot {
            SampledSlot::One(v) => raw(v),
            SampledSlot::List(vs) => ReplayValue::List(vs.iter().map(raw).collect()),
        })
        .collect()
}

/// Binds sample `s` into fingerprint `fp`'s normalized `text` and rewrites
/// it to physical names: a statement replay can run. `None` when the
/// sample doesn't bind (counts differ, a value that isn't one literal) or
/// a name doesn't map (a database dropped since); the reason is logged
/// without values.
#[must_use]
pub fn bind_sample(
    map: &dyn NameMap,
    fp: Fingerprint,
    text: &str,
    role: StatementRole,
    s: &OpenedSample,
) -> Option<ReplayStatement> {
    let c = &s.payload.context;
    let lex = lex_for(c.sql_mode.as_deref());
    let logical = c.database.as_deref();
    let physical = match logical {
        Some(db) => match map.physical(db, Purpose::Session).ok()?.single() {
            Some(p) => Some(p.to_string()),
            None => return None,
        },
        None => None,
    };
    let context = SessionContext {
        database: physical,
        sql_mode: c.sql_mode.clone(),
        time_zone: c.time_zone.clone(),
        charset: c.charset.clone(),
        collation: c.collation.clone(),
    };
    let bound = match bind_literals(text, &values_of(s), lex.no_backslash_escapes) {
        Ok(b) => b,
        Err(e) => {
            tracing::debug!(fingerprint = %fp, sample = s.sample_id, error = %e,
                "a sample didn't bind; skipped");
            return None;
        }
    };
    let ctx = RewriteCtx::new(map, Persona::MariaDb)
        .lex(lex)
        .current_db(logical);
    let sql = match rewrite(bound.as_bytes(), &ctx) {
        Ok(r) => r.sql_lossy(),
        Err(e) => {
            tracing::debug!(fingerprint = %fp, sample = s.sample_id, errno = e.errno(),
                "a sample's names didn't map; skipped");
            return None;
        }
    };
    Some(ReplayStatement {
        fingerprint: fp,
        sample_id: s.sample_id,
        role,
        sql,
        context,
    })
}

/// Opens up to `max` samples of each fingerprint in `fps` and binds them
/// in physical names, in `fps` order. Samples that don't open or bind are
/// skipped.
///
/// # Errors
/// The catalog's.
pub async fn statements(
    locator: &dyn CatalogLocator,
    keyring: &dyn TenantKeyring,
    map: &dyn NameMap,
    tenant: TenantId,
    fps: &[(Fingerprint, StatementRole)],
    max: usize,
) -> Result<Vec<ReplayStatement>> {
    if fps.is_empty() {
        return Ok(Vec::new());
    }
    let ep = locator.reader(tenant).await?;
    let mut conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(
            sprig_common::SprigCode::CatalogUnavailable,
            "catalog unavailable",
        )
        .with_source(e)
    })?;
    let t = WorkloadTables::new(ep.store.schema());
    let ids: Vec<Fingerprint> = fps.iter().map(|(f, _)| *f).collect();
    let texts: BTreeMap<Fingerprint, String> = t
        .fingerprints(&mut conn, &ids)
        .await?
        .into_iter()
        .map(|r| (r.id, r.text.to_string()))
        .collect();
    let mut out = Vec::new();
    for (fp, role) in fps {
        let Some(text) = texts.get(fp) else { continue };
        let opened = t.open_samples(&mut conn, keyring, tenant, *fp, max).await?;
        out.extend(
            opened
                .iter()
                .filter_map(|s| bind_sample(map, *fp, text, *role, s)),
        );
    }
    Ok(out)
}

/// Trigger evaluation's [`qualify::WorkloadSource`] over chapter 12's
/// store (`CatalogWorkloadStore::window`). Every run is a client run:
/// Studio (chapter 44) doesn't exist yet.
pub struct StoreSource {
    store: Arc<dyn WorkloadStore>,
}

impl std::fmt::Debug for StoreSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreSource").finish_non_exhaustive()
    }
}

impl StoreSource {
    /// A source over `store`.
    #[must_use]
    pub fn new(store: Arc<dyn WorkloadStore>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl qualify::WorkloadSource for StoreSource {
    async fn window(
        &self,
        tenant: TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Result<Vec<SourcedWindow>> {
        Ok(self
            .store
            .window(tenant, from, to)
            .await?
            .into_iter()
            .map(SourcedWindow::client)
            .collect())
    }

    fn watch(&self, tenant: TenantId, fp: Fingerprint) {
        // No channel to the gateways' recorders yet (module docs).
        tracing::debug!(%tenant, fingerprint = %fp, "auto_index: watch asked (gateways watch by p95)");
    }
}

/// The observer's [`observe::WorkloadSource`] over chapter 12's store and
/// the tenant's catalog (for names), with the sample keys for usage
/// `EXPLAIN`s.
pub struct ObserveSource {
    store: Arc<dyn WorkloadStore>,
    locator: Arc<dyn CatalogLocator>,
    keyring: Option<Arc<dyn TenantKeyring>>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for ObserveSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObserveSource")
            .field("keyring", &self.keyring.is_some())
            .finish_non_exhaustive()
    }
}

impl ObserveSource {
    /// A source over `store`; without `keyring` usage is unknown (no
    /// sample opens), so nothing is dropped as unused.
    #[must_use]
    pub fn new(
        store: Arc<dyn WorkloadStore>,
        locator: Arc<dyn CatalogLocator>,
        keyring: Option<Arc<dyn TenantKeyring>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            locator,
            keyring,
            clock,
        }
    }
}

fn on_object(w: &FpWindowStats, object: ObjectId) -> bool {
    w.kind == FpKind::Sql && w.shape.object_ids().contains(&object)
}

#[async_trait::async_trait]
impl observe::WorkloadSource for ObserveSource {
    async fn latency(
        &self,
        tenant: TenantId,
        object: ObjectId,
        since: SystemTime,
        until: SystemTime,
    ) -> Result<Option<BTreeMap<Fingerprint, FpLatency>>> {
        let w = self.store.window(tenant, since, until).await?;
        Ok(Some(
            w.iter()
                .filter(|w| on_object(w, object) && w.runs() > 0)
                .map(|w| {
                    (
                        w.id,
                        FpLatency {
                            runs: w.runs(),
                            p95_ms: w.p95_ms(),
                            mean_ms: w.mean_ms(),
                            uses_index: None,
                            write: w.shape.writes.is_some(),
                        },
                    )
                })
                .collect(),
        ))
    }

    async fn samples(
        &self,
        tenant: TenantId,
        object: ObjectId,
        n: usize,
        since: SystemTime,
    ) -> Result<Option<Vec<UsageSample>>> {
        let Some(keyring) = &self.keyring else {
            return Ok(None);
        };
        let mut w: Vec<FpWindowStats> = self
            .store
            .window(tenant, since, self.clock.wall())
            .await?
            .into_iter()
            .filter(|w| on_object(w, object) && w.shape.writes.is_none())
            .collect();
        w.sort_by(|a, b| b.total_ms().total_cmp(&a.total_ms()).then(a.id.cmp(&b.id)));
        w.truncate(n);
        let cat = tenant_catalog(&*self.locator, tenant).await?;
        let map = name_map(&cat);
        let fps: Vec<(Fingerprint, StatementRole)> = w
            .iter()
            .map(|f| (f.id, StatementRole::Regression))
            .collect();
        let bound = statements(&*self.locator, &**keyring, &map, tenant, &fps, 1).await?;
        Ok(Some(
            bound
                .into_iter()
                .filter_map(|s| {
                    Some(UsageSample {
                        fingerprint: s.fingerprint,
                        schema: s.context.database?,
                        sql: s.sql,
                    })
                })
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::{DbId, TenantId};
    use sprig_common::names::SchemaName;
    use sprig_workload::fingerprint::LiteralKind;
    use sprig_workload::samples::{SampleContext, SamplePayload};
    use sprig_workload::window::{SampleValue, SampledLiterals};

    use super::*;

    fn map() -> MemoryNameMap {
        let mut m = MemoryNameMap::new(TenantId(7));
        m.insert(LogicalDb {
            id: DbId(1),
            name: "shop".into(),
            shards: vec!["t7_d1_g1_s1".parse::<SchemaName>().unwrap()],
            reference: None,
        });
        m
    }

    fn sample(slots: Vec<SampledSlot>, sql_mode: Option<&str>) -> OpenedSample {
        OpenedSample {
            fingerprint: Fingerprint(1),
            sample_id: 9,
            captured_at: SystemTime::UNIX_EPOCH,
            latency_us: 0,
            rows_examined: None,
            rows_returned: 0,
            payload: SamplePayload {
                literals: SampledLiterals { slots },
                context: SampleContext {
                    database: Some("shop".into()),
                    db_id: DbId(1),
                    sql_mode: sql_mode.map(str::to_owned),
                    time_zone: Some("SYSTEM".into()),
                    charset: Some("utf8mb4".into()),
                    collation: None,
                },
            },
        }
    }

    fn v(kind: LiteralKind, t: &str) -> SampleValue {
        SampleValue {
            kind,
            text: t.as_bytes().to_vec(),
        }
    }

    #[test]
    fn samples_bind_in_physical_names() {
        let s = sample(
            vec![
                SampledSlot::One(v(LiteralKind::Number, "42")),
                SampledSlot::List(vec![
                    v(LiteralKind::String, "'open'"),
                    v(LiteralKind::String, "'hold'"),
                ]),
                SampledSlot::One(v(LiteralKind::Number, "20")),
            ],
            None,
        );
        let st = bind_sample(
            &map(),
            Fingerprint(1),
            "SELECT id FROM shop.orders WHERE customer_id = ? AND status IN (...) LIMIT ?",
            StatementRole::Target,
            &s,
        )
        .unwrap();
        assert_eq!(
            st.sql,
            "SELECT id FROM `t7_d1_g1_s1`.orders WHERE customer_id = 42 AND status IN \
             ('open', 'hold') LIMIT 20"
        );
        assert_eq!(st.context.database.as_deref(), Some("t7_d1_g1_s1"));
        assert_eq!(st.sample_id, 9);
    }

    #[test]
    fn backslashes_follow_the_samples_sql_mode() {
        // Captured under NO_BACKSLASH_ESCAPES: one backslash, replayed so.
        let s = sample(
            vec![SampledSlot::One(v(LiteralKind::String, "'a\\b'"))],
            Some("NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES"),
        );
        let st = bind_sample(
            &map(),
            Fingerprint(1),
            "SELECT 1 FROM shop.t WHERE s = ?",
            StatementRole::Target,
            &s,
        );
        // A raw literal that only lexes under backslash escapes is refused
        // rather than replayed with another value.
        if let Some(st) = st {
            assert!(st.sql.ends_with("s = 'a\\b'"), "{}", st.sql);
        }
    }

    #[test]
    fn unknown_databases_and_bad_values_skip_the_sample() {
        let mut s = sample(vec![SampledSlot::One(v(LiteralKind::Number, "1"))], None);
        s.payload.context.database = Some("gone".into());
        assert!(
            bind_sample(
                &map(),
                Fingerprint(1),
                "SELECT ?",
                StatementRole::Target,
                &s
            )
            .is_none()
        );
        let s = sample(
            vec![SampledSlot::One(v(LiteralKind::Number, "1 OR 1"))],
            None,
        );
        assert!(
            bind_sample(
                &map(),
                Fingerprint(1),
                "SELECT ?",
                StatementRole::Target,
                &s
            )
            .is_none()
        );
    }
}
