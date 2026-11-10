//! Moving a tenant to another data center (chapter 24, "Orchestrating a
//! move", "One-command moves"; build step 4): `sprigctl tenant move
//! <tenant> --to <dc>` and the `migration` loop. A tenant pinned with
//! `sprigctl tenant pin` (chapter 11's pin) or with `pinned_dc` in its
//! directory entry doesn't move.
//!
//! ```text
//! start(T, Y), on the leader of T's home DC X:
//! 1. refuse (39550) unless T is active, not pinned (either pin), homed in X, not
//!    already in Y, and no mover job of T is open in X; Y must be a
//!    registered DC (39503) with an active cluster (or --cluster)
//! 2. budget = --max-pause-ms, else migration.write_pause_target_ms
//! 3. directory: state migrating, migration {id, from, to, clusters,
//!    budget, started_ms}            (a compare on the entry's revision)
//! 4. a mover job in Y's tier: the whole tenant (catalog and every
//!    database) from its cluster in X to the cluster in Y, with the
//!    budget as its hold; Y's workers pull from X
//! 5. migration.job = the job id
//! the job: copy, catch up, verify, then cutover with the hold on X's map
//! (every gateway, in every DC, follows it), fence X, apply to the fence
//! position, and commit: X's map, Y's map and the directory in one
//! transaction (sprig-shard::home), the commit point.
//! migration loop, on X's leader, per tenant migrating from X:
//!   job rolled back, or no job 2 minutes after the start -> directory back
//!   to active (the source was never fenced past the rollback);
//!   otherwise wait.
//! on every leader, per tenant homed here with an unobserved last_move:
//!   count the move and its pause in the metrics, mark it observed.
//! ```
//!
//! **Where the book differs (R0).** A tenant is one placement unit (its
//! catalog and every database on one cluster, decision N4), so the parent
//! job and its `Group` children are one mover job whose commit is the
//! home change; R1's shards make it several. The hold is the source map's
//! hold, which every gateway follows wherever it is: it stands in for
//! `Traffic.Hold` and `Release`. The cutover's lag estimate is the
//! mover's own: it cuts over once lag stays under `mover.ready_max_lag_ms`
//! for `mover.ready_sustain_seconds`, and aborts any attempt that would
//! hold longer than the budget (three attempts, then it rolls back).
//! `migration.write_pause_target_ms` resolves for the tenant at its home
//! DC: its own catalog rows over the dc and global scopes (decision RF3,
//! `crate::tenant_scopes`). Chapter 24's automatic moves
//! (`crate::dc_migrate`) start through the same [`DcMoves::start_caused`].

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_cluster::record::AdminState;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId, JobId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{MetaStore, Tier, TierView};
use sprig_mover::{JobFilter, JobSpec, JobState, JobStatus, JobStore, MetaMover, Mover};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};
use sprig_shard::home::{self, HomeEntry};
use sprig_shard::{Placement, TenantMaps};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::traffic::TrafficWindows;

/// `migration.write_pause_target_ms`.
pub static WRITE_PAUSE_TARGET_MS: Setting<u64> = Setting {
    key: "migration.write_pause_target_ms",
    default: 5000,
    scopes: Scopes(0b11_0100),
    hard: HardBounds::range(200, 60_000),
    apply: Apply::Live,
    operator_only: true,
    owner: 24,
    doc: "Tenant-wide write-pause budget of a move between DCs",
};
sprig_settings::register!(WRITE_PAUSE_TARGET_MS);

/// A start without a job id this long is abandoned.
const SUBMIT_GRACE_MS: u64 = 120_000;

/// 39550.
fn refused(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::TenantMoveRefused, msg)
}

/// `POST /admin/v1/tenants/{name}:move`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveTenantRequest {
    /// The target DC.
    pub to: String,
    /// Only check and show the plan.
    #[serde(default)]
    pub dry_run: bool,
    /// The write-pause budget, overriding `migration.write_pause_target_ms`.
    #[serde(default)]
    pub max_pause_ms: Option<u64>,
    /// The target cluster; default: the active cluster of the DC with the
    /// lowest id.
    #[serde(default)]
    pub cluster: Option<String>,
}

/// A move as `sprigctl tenant move` shows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MoveView {
    /// The tenant.
    pub tenant: String,
    /// Its id.
    pub tenant_id: TenantId,
    /// The directory state.
    pub state: String,
    /// The home DC now.
    pub home_dc: DcId,
    /// The directory epoch.
    pub epoch: u64,
    /// The move in progress or the last one (`migration` record).
    pub migration: Option<Value>,
    /// Whether this was a dry run.
    #[serde(default)]
    pub dry_run: bool,
    /// The mover job, when known.
    pub job: Option<JobStatus>,
    /// The longest write hold of the job's cutover attempts, ms.
    pub pause_ms: Option<u64>,
    /// Start to commit, ms (a finished move).
    pub duration_ms: Option<u64>,
    /// The last move (`last_move`).
    pub last_move: Option<Value>,
    /// Why the last attempt was rolled back, if it was.
    pub last_error: Option<Value>,
}

/// The move service of one controller.
pub struct DcMoves {
    dc: DcId,
    store: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    live: Option<LiveSettings>,
    /// Traffic windows of tenants homed here.
    pub traffic: Arc<TrafficWindows>,
    /// Tenants whose move from here the loop is watching.
    watching: Mutex<HashSet<TenantId>>,
    /// Tenants' own catalog rows (decision RF3): tenant-scope
    /// `migration.*` values win over the home DC's.
    scopes: Option<Arc<crate::tenant_scopes::TenantScopes>>,
    /// The automatic decision's soft state (`crate::dc_migrate`).
    pub decisions: Arc<crate::dc_migrate::DecisionState>,
}

impl std::fmt::Debug for DcMoves {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DcMoves")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

fn str_of(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_owned)
}

impl DcMoves {
    /// The service of a controller in `dc`; `store` is its DC tier (the
    /// one etcd), `global` the global tier.
    #[must_use]
    pub fn new(
        dc: DcId,
        store: Arc<dyn MetaStore>,
        global: Arc<dyn MetaStore>,
        clock: Arc<dyn Clock>,
        live: Option<LiveSettings>,
    ) -> Self {
        Self {
            dc,
            store,
            global,
            clock,
            live,
            traffic: Arc::new(TrafficWindows::default()),
            watching: Mutex::new(HashSet::new()),
            scopes: None,
            decisions: Arc::new(crate::dc_migrate::DecisionState::default()),
        }
    }

    /// The global tier.
    #[must_use]
    pub fn global(&self) -> Arc<dyn MetaStore> {
        Arc::clone(&self.global)
    }

    /// This DC's tier (the one etcd).
    #[must_use]
    pub fn store(&self) -> &Arc<dyn MetaStore> {
        &self.store
    }

    /// The clock.
    #[must_use]
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// A float setting at this DC's scope.
    pub fn setting_f64(&self, s: &'static Setting<f64>) -> f64 {
        self.live
            .as_ref()
            .map_or(s.default, |l| l.get(s, &SettingCtx::process(&self.dc)))
    }

    /// A text setting at this DC's scope.
    pub fn setting_str(&self, s: &'static Setting<std::borrow::Cow<'static, str>>) -> String {
        self.live
            .as_ref()
            .map_or_else(
                || s.default.clone(),
                |l| l.get(s, &SettingCtx::process(&self.dc)),
            )
            .into_owned()
    }

    /// Whether chapter 11's pin (`sprigctl tenant pin`) holds `tenant` in
    /// this DC.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn chapter11_pinned(&self, tenant: TenantId) -> Result<bool> {
        Ok(sprig_balancer::pins::PinStore::new(Arc::clone(&self.store))
            .get(tenant)
            .await?
            .is_some())
    }

    /// Moves between DCs in progress that touch `a`, and that touch `b`
    /// (as source or target): `migration.max_concurrent_per_dc`'s count.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn moves_touching(&self, a: &DcId, b: &DcId) -> Result<(u64, u64)> {
        let dir = crate::tree::Directory::new(Arc::clone(&self.global));
        let (mut na, mut nb) = (0, 0);
        for e in dir.list().await? {
            if e.body().state != crate::tree::TenantState::Migrating {
                continue;
            }
            let Some(h) = home::read_home(&*self.global, e.body().tenant_id).await? else {
                continue;
            };
            if h.state != "migrating" {
                continue;
            }
            let from = h.home_dc.clone();
            let to = h.migration_to();
            if from == *a || to.as_ref() == Some(a) {
                na += 1;
            }
            if from == *b || to.as_ref() == Some(b) {
                nb += 1;
            }
        }
        Ok((na, nb))
    }

    /// Resolves tenant-scope keys with each tenant's own catalog rows too
    /// (decision RF3).
    #[must_use]
    pub fn with_tenant_scopes(mut self, s: Arc<crate::tenant_scopes::TenantScopes>) -> Self {
        self.scopes = Some(s);
        self
    }

    /// `s` for `tenant` at this (its home) DC: its own catalog rows (tenant,
    /// ancestors) over the dc and global scopes.
    pub async fn tenant_setting<T: sprig_settings::SettingValue>(
        &self,
        tenant: TenantId,
        s: &'static Setting<T>,
    ) -> T {
        let Some(l) = self.live.as_ref() else {
            return s.default.clone();
        };
        let rows = match &self.scopes {
            Some(sc) => sc.get(tenant).await,
            None => None,
        };
        crate::tenant_scopes::get(
            &l.snapshot(),
            rows.as_deref(),
            s,
            &SettingCtx::process(&self.dc).for_tenant(tenant, &[]),
        )
    }

    /// This controller's DC.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc
    }

    /// A setting at this DC's scope.
    pub fn setting(&self, s: &'static Setting<u64>) -> u64 {
        self.live
            .as_ref()
            .map_or(s.default, |l| l.get(s, &SettingCtx::process(&self.dc)))
    }

    fn mover_in(&self, dc: &DcId) -> MetaMover {
        MetaMover::new(JobStore::new(
            TierView::over(&self.store, Tier::Dc(dc.clone())),
            Arc::clone(&self.clock),
        ))
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(
            self.clock
                .wall()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }

    async fn entry(&self, tenant: TenantId) -> Result<HomeEntry> {
        home::read_home(&*self.global, tenant)
            .await?
            .ok_or_else(|| {
                SprigError::new(SprigCode::TenantNotReady, format!("no tenant {tenant}"))
            })
    }

    /// The target cluster in `to`: `asked`, or the active cluster with
    /// the lowest id. A cluster in `retained` still holds the fenced source
    /// of an earlier move of the tenant (`mover.source_retain_hours`), whose
    /// schemas a new copy can't take: it is skipped, or refused if asked.
    async fn target_cluster(
        &self,
        to: &DcId,
        asked: Option<&str>,
        retained: &HashSet<ClusterId>,
        allowed: Option<&std::collections::BTreeSet<DcId>>,
    ) -> Result<ClusterId> {
        let reg = ClusterRegistry::new(
            TierView::over(&self.store, Tier::Global),
            TierView::over(&self.store, Tier::Dc(to.clone())),
        )?;
        let mut active: Vec<ClusterId> = reg
            .list()
            .await?
            .into_iter()
            .filter(|c| c.body().admin_state == AdminState::Active)
            // Chapter 26's cluster rule: every copy inside the residency.
            .filter(|c| crate::residency::eligible(c.body(), allowed))
            .map(|c| c.body().cluster_id.clone())
            .collect();
        active.sort();
        match asked {
            Some(a) => {
                let id = ClusterId::new(a).map_err(|e| refused(format!("--cluster {a}: {e}")))?;
                if retained.contains(&id) {
                    Err(refused(format!(
                        "{id} still holds the retained source of the tenant's last move; \
                         choose another cluster"
                    )))
                } else if active.contains(&id) {
                    Ok(id)
                } else {
                    Err(refused(format!("{id} isn't an active cluster of {to}")))
                }
            }
            None => active
                .into_iter()
                .find(|c| !retained.contains(c))
                .ok_or_else(|| {
                    refused(format!(
                        "{to} has no active cluster without a retained copy of the tenant"
                    ))
                }),
        }
    }

    /// Starts moving `tenant` (named `name`) to `req.to` (see the module
    /// docs). The caller has checked that this controller leads its DC.
    ///
    /// # Errors
    /// 39550 when the move can't start; 39503 for an unregistered DC; the
    /// stores' errors.
    pub async fn start(
        &self,
        tenant: TenantId,
        name: &str,
        req: &MoveTenantRequest,
    ) -> Result<MoveView> {
        self.start_caused(tenant, name, req, "operator").await
    }

    /// [`Self::start`], recording `cause` in the migration record
    /// (`operator`, or `residency` for chapter 26's compliance moves).
    ///
    /// # Errors
    /// As [`Self::start`].
    #[allow(clippy::too_many_lines)] // the checks, then the three steps
    pub async fn start_caused(
        &self,
        tenant: TenantId,
        name: &str,
        req: &MoveTenantRequest,
        cause: &str,
    ) -> Result<MoveView> {
        let to = DcId::new(req.to.as_str())
            .map_err(|e| SprigError::new(SprigCode::UnknownDc, format!("--to {}: {e}", req.to)))?;
        let dcs = crate::dc::list(&*self.global).await?;
        match dcs.iter().find(|d| d.dc_id == to) {
            Some(d) if matches!(d.state.as_str(), "active" | "joining") => {}
            Some(d) => return Err(refused(format!("dc {to} is {}", d.state))),
            None => {
                return Err(SprigError::new(
                    SprigCode::UnknownDc,
                    format!("no data center {to} is registered"),
                ));
            }
        }
        let e = self.entry(tenant).await?;
        if e.home_dc != self.dc {
            return Err(refused(format!(
                "tenant {name} is homed in {}: ask a controller of {}",
                e.home_dc, e.home_dc
            )));
        }
        if e.home_dc == to {
            return Err(refused(format!("tenant {name} is already homed in {to}")));
        }
        if let Some(p) = &e.pinned_dc {
            return Err(refused(format!(
                "tenant {name} is pinned to {p}: unpin it first"
            )));
        }
        // Chapter 26: never to a DC outside the tenant's residency (the
        // commit checks again, against the entry it commits).
        if !e.residency_allows(&to) {
            let allowed: Vec<String> = e
                .allowed_dcs()
                .unwrap_or_default()
                .iter()
                .map(ToString::to_string)
                .collect();
            return Err(refused(format!(
                "tenant {name}'s residency allows {}, not {to}",
                if allowed.is_empty() {
                    "no data center".to_owned()
                } else {
                    allowed.join(", ")
                }
            )));
        }
        // `sprigctl tenant pin` (chapter 11's pin record in this DC).
        if let Some(p) = sprig_balancer::pins::PinStore::new(Arc::clone(&self.store))
            .get(tenant)
            .await?
        {
            return Err(refused(format!(
                "tenant {name} is pinned to {} (sprigctl tenant unpin {name})",
                p.cluster
            )));
        }
        if e.state != "active" {
            return Err(refused(format!("tenant {name} is {}", e.state)));
        }
        let open = self
            .mover_in(&self.dc)
            .list(&JobFilter {
                tenant: Some(tenant),
                active_only: true,
            })
            .await?;
        // A committed job that only retains its source doesn't block.
        if let Some(j) = open.iter().find(|j| !j.state.is_committed()) {
            return Err(refused(format!(
                "mover job {} of tenant {name} is still open",
                j.id.0
            )));
        }
        // Chapter 20 (R1): no move beside the tenant's shard work, and no
        // whole move of a tenant with table groups.
        self.check_shard_work(tenant, name).await?;
        let (map, _) = TenantMaps::new(Arc::clone(&self.store))
            .load(tenant)
            .await?;
        let from_cluster = Placement::of(&map)?.cluster;
        let retained: HashSet<ClusterId> = open
            .iter()
            .filter(|j| j.state.is_committed())
            .filter_map(|j| j.source.clone())
            .collect();
        let to_cluster = self
            .target_cluster(
                &to,
                req.cluster.as_deref(),
                &retained,
                e.allowed_dcs().as_ref(),
            )
            .await?;
        let budget = match req.max_pause_ms {
            Some(ms) => ms,
            None => self.tenant_setting(tenant, &WRITE_PAUSE_TARGET_MS).await,
        }
        .clamp(200, 60_000);
        let id = uuid::Uuid::now_v7().simple().to_string();
        let migration = json!({
            "id": id, "from": self.dc.as_str(), "to": to.as_str(),
            "source_cluster": from_cluster.as_str(), "target_cluster": to_cluster.as_str(),
            "max_pause_ms": budget, "started_ms": self.now_ms(), "cause": cause,
            "job": Value::Null,
        });
        if req.dry_run {
            let mut v = self.view(name, &e, None).await;
            v.migration = Some(migration);
            v.dry_run = true;
            return Ok(v);
        }
        // 3. The directory first: from here the tenant is `migrating`, so
        //    nothing else starts a move, and the commit checks for it.
        let dir = home::update_home(&*self.global, tenant, |cur| {
            if cur.state != "active" || cur.home_dc != self.dc || cur.pinned_dc.is_some() {
                return Err(refused(format!(
                    "tenant {name} changed meanwhile (state {}, home {})",
                    cur.state, cur.home_dc
                )));
            }
            cur.raw.insert("state".into(), json!("migrating"));
            cur.raw.insert("migration".into(), migration.clone());
            Ok(true)
        })
        .await?;
        self.watch(tenant);
        // The sharding loop admits shard work only for `active` tenants and
        // re-reads the directory after it starts some: checking again after
        // the directory says `migrating` closes the race between the two.
        if let Err(err) = self.check_shard_work(tenant, name).await {
            self.revert(tenant, &id, &format!("the move couldn't start: {err}"))
                .await?;
            return Err(err);
        }
        // 4. The job in Y's tier.
        let mut spec =
            JobSpec::move_tenant(tenant, from_cluster, to_cluster).with_max_hold_ms(budget);
        spec.requested_by = Some(format!("migration:{id}"));
        let job = match self.mover_in(&to).submit(spec).await {
            Ok(j) => j,
            Err(err) => {
                self.revert(tenant, &id, &format!("the job couldn't start: {err}"))
                    .await?;
                return Err(err);
            }
        };
        // 5. Record the job.
        let job_text = job.0.to_string();
        let dir = home::update_home(&*self.global, tenant, |cur| {
            match cur.migration.as_ref().and_then(|m| str_of(m, "id")) {
                Some(cur_id) if cur_id == id => {}
                _ => return Ok(false),
            }
            if let Some(Value::Object(m)) = cur.raw.get_mut("migration") {
                m.insert("job".into(), json!(job_text));
            }
            Ok(true)
        })
        .await
        .unwrap_or(dir);
        tracing::info!(tenant = %name, from = %self.dc, %to, job = %job.0, budget_ms = budget, "tenant move started");
        metrics::counter!("sprig_controller_migrations_total", "cause" => cause.to_owned(), "result" => "started")
            .increment(1);
        Ok(self.view(name, &dir, None).await)
    }

    /// Refuses the move while a split, merge or shard move of `tenant`
    /// runs here, or the advisor applies its first design (39470), and for
    /// a tenant with table groups (39472): the move would carry the
    /// catalog's map, not the groups' shard maps (chapters 10 and 20).
    async fn check_shard_work(&self, tenant: TenantId, name: &str) -> Result<()> {
        let js = JobStore::new(
            TierView::over(&self.store, Tier::Dc(self.dc.clone())),
            Arc::clone(&self.clock),
        );
        let groups = js.sharded_groups(tenant).await?;
        if !groups.is_empty() {
            return Err(sprig_mover::record::sharded_tenant(tenant, &groups));
        }
        let busy = match js.group_locks(tenant).await?.into_iter().next() {
            Some((_, job)) => Some(job),
            None => js.tenant_lock_holder(tenant).await?.filter(|h| {
                // A job of this tenant's own (checked above as open) is not
                // shard work; anything else holding the lock is.
                h.parse::<JobId>().is_err()
            }),
        };
        if let Some(h) = busy {
            return Err(SprigError::new(
                SprigCode::ShardBusy,
                format!(
                    "tenant {name} can't move now: {h} holds its shards; retry when it finishes"
                ),
            ));
        }
        Ok(())
    }

    fn watch(&self, tenant: TenantId) {
        self.watching
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant);
    }

    /// Puts a migrating tenant back to active after a failed move.
    async fn revert(&self, tenant: TenantId, id: &str, why: &str) -> Result<()> {
        let at = self.now_ms();
        home::update_home(&*self.global, tenant, |cur| {
            let same = cur.state == "migrating"
                && cur
                    .migration
                    .as_ref()
                    .and_then(|m| str_of(m, "id"))
                    .as_deref()
                    == Some(id);
            if !same {
                return Ok(false);
            }
            let m = cur.migration.clone().unwrap_or(Value::Null);
            cur.raw.insert("state".into(), json!("active"));
            cur.raw.insert("migration".into(), Value::Null);
            cur.raw.insert(
                "last_migration_error".into(),
                json!({"at_ms": at, "error": why, "migration": m}),
            );
            Ok(true)
        })
        .await?;
        tracing::warn!(%tenant, why, "tenant move rolled back");
        metrics::counter!("sprig_controller_migrations_total", "cause" => "operator", "result" => "rolled_back")
            .increment(1);
        Ok(())
    }

    /// Pins `tenant` to its home DC (`pinned_dc` in its directory entry),
    /// or unpins it: a pinned tenant doesn't move between DCs.
    /// `sprigctl tenant pin|unpin` calls it with chapter 11's cluster pin.
    ///
    /// # Errors
    /// 39550 to pin a tenant that is moving; the stores' errors.
    pub async fn pin(&self, tenant: TenantId, name: &str, pin: bool) -> Result<MoveView> {
        let e = home::update_home(&*self.global, tenant, |cur| {
            if pin && cur.state == "migrating" {
                return Err(refused(format!("tenant {name} is moving")));
            }
            let v = if pin {
                json!(cur.home_dc.as_str())
            } else {
                Value::Null
            };
            cur.raw.insert("pinned_dc".into(), v);
            Ok(true)
        })
        .await?;
        Ok(self.view(name, &e, None).await)
    }

    /// The move in progress, or the last one.
    ///
    /// # Errors
    /// The stores' errors.
    pub async fn status(&self, tenant: TenantId, name: &str) -> Result<MoveView> {
        let e = self.entry(tenant).await?;
        Ok(self.view(name, &e, None).await)
    }

    /// The job of a migration record (in its target DC's tier).
    async fn job_of(&self, m: &Value) -> Option<JobStatus> {
        let to = DcId::new(str_of(m, "to")?).ok()?;
        let id: JobId = str_of(m, "job")?.parse().ok()?;
        self.mover_in(&to).status(&id).await.ok()
    }

    async fn view(&self, name: &str, e: &HomeEntry, job: Option<JobStatus>) -> MoveView {
        let current = e.migration.clone().or_else(|| {
            e.last_move
                .as_ref()
                .and_then(|l| l.get("migration").cloned())
        });
        let job = match (job, &current) {
            (Some(j), _) => Some(j),
            (None, Some(m)) => self.job_of(m).await,
            (None, None) => None,
        };
        let pause_ms = job
            .as_ref()
            .and_then(|j| j.progress.holds_ms.iter().copied().max());
        let duration_ms = e.last_move.as_ref().and_then(|l| {
            let at = l.get("at_ms")?.as_u64()?;
            let started = l.get("migration")?.get("started_ms")?.as_u64()?;
            Some(at.saturating_sub(started))
        });
        MoveView {
            tenant: name.to_owned(),
            tenant_id: TenantId(e.raw.get("tenant_id").and_then(Value::as_u64).unwrap_or(0)),
            state: e.state.clone(),
            home_dc: e.home_dc.clone(),
            epoch: e.epoch,
            migration: e.migration.clone(),
            dry_run: false,
            job,
            pause_ms,
            duration_ms: if e.migration.is_some() {
                None
            } else {
                duration_ms
            },
            last_move: e.last_move.clone(),
            last_error: e
                .raw
                .get("last_migration_error")
                .cloned()
                .filter(|v| !v.is_null()),
        }
    }

    /// One step of the loop for `tenant` (see the module docs).
    async fn reconcile_tenant(&self, tenant: TenantId) -> Result<Outcome> {
        let Some(e) = home::read_home(&*self.global, tenant).await? else {
            return Ok(Outcome::Done);
        };
        if e.home_dc == self.dc && e.state == "migrating" {
            self.watch(tenant);
            let Some(m) = e.migration.clone() else {
                return Ok(Outcome::Done);
            };
            let id = str_of(&m, "id").unwrap_or_default();
            let started = m.get("started_ms").and_then(Value::as_u64).unwrap_or(0);
            match self.job_of(&m).await {
                Some(j) if j.state == JobState::RolledBack => {
                    let why = j
                        .error
                        .unwrap_or_else(|| "the mover rolled back".to_owned());
                    self.revert(tenant, &id, &why).await?;
                    return Ok(Outcome::Done);
                }
                None if str_of(&m, "job").is_none()
                    && self.now_ms().saturating_sub(started) > SUBMIT_GRACE_MS =>
                {
                    self.revert(tenant, &id, "no mover job was recorded")
                        .await?;
                    return Ok(Outcome::Done);
                }
                _ => {}
            }
            return Ok(Outcome::RequeueAfter(Duration::from_secs(5)));
        }
        // Homed here after a move: count it once.
        if e.home_dc == self.dc
            && let Some(lm) = &e.last_move
            && lm.get("to").and_then(Value::as_str) == Some(self.dc.as_str())
            && !lm.get("observed").and_then(Value::as_bool).unwrap_or(false)
        {
            let v = self.view("", &e, None).await;
            if let Some(p) = v.pause_ms {
                #[allow(clippy::cast_precision_loss)]
                metrics::histogram!("sprig_controller_migration_write_pause_seconds")
                    .record(p as f64 / 1000.0);
            }
            if let Some(d) = v.duration_ms {
                #[allow(clippy::cast_precision_loss)]
                metrics::histogram!("sprig_controller_migration_phase_seconds", "phase" => "total")
                    .record(d as f64 / 1000.0);
            }
            metrics::counter!("sprig_controller_migrations_total", "cause" => "operator", "result" => "done")
                .increment(1);
            tracing::info!(%tenant, pause_ms = ?v.pause_ms, duration_ms = ?v.duration_ms, "tenant move finished");
            home::update_home(&*self.global, tenant, |cur| {
                if let Some(Value::Object(l)) = cur.raw.get_mut("last_move") {
                    l.insert("observed".into(), json!(true));
                    return Ok(true);
                }
                Ok(false)
            })
            .await?;
        }
        self.watching
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&tenant);
        Ok(Outcome::Done)
    }
}

/// The `migration` loop (DC leader): see the module docs.
#[derive(Clone)]
pub struct MigrationLoop {
    moves: Arc<DcMoves>,
}

impl MigrationLoop {
    /// The loop over `moves`.
    #[must_use]
    pub fn new(moves: Arc<DcMoves>) -> Self {
        Self { moves }
    }
}

#[async_trait::async_trait]
impl Reconciler for MigrationLoop {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "migration"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        crate::tree::Directory::new(Arc::clone(&self.moves.global))
            .tenant_of(&ev.key)
            .into_iter()
            .collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        let dir = crate::tree::Directory::new(Arc::clone(&self.moves.global));
        let mut keys: Vec<TenantId> = dir
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| e.home_dc == self.moves.dc)
            .map(|e| e.tenant_id)
            .collect();
        keys.extend(
            self.moves
                .watching
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .copied(),
        );
        keys.sort_unstable();
        keys.dedup();
        Ok(keys)
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        self.moves.reconcile_tenant(*key).await
    }
}

#[cfg(test)]
mod tests;
