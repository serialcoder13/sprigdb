//! The Admin API (`sprig.admin.v1`, chapter 37): the `Clusters` service
//! of chapter 09 step 9 and the `DataCenters` service of chapter 23 step 7,
//! as the REST face carries them.
//!
//! [`AdminService`] implements the calls; [`http`] serves them as JSON
//! under `/admin/v1/`. Reads are answered by any controller; calls that
//! write cluster records (`RegisterCluster`, `ProbeCluster`,
//! `DrainCluster`) only by the DC leader, with its fencing token on every
//! write. Elsewhere they fail with 39504, naming the leader.
//!
//! | Call | Route |
//! |---|---|
//! | `Clusters.RegisterCluster` | `POST /admin/v1/clusters` |
//! | `Clusters.ListClusters` | `GET /admin/v1/clusters` |
//! | `Clusters.GetCluster` | `GET /admin/v1/clusters/{cluster_id}` |
//! | `Clusters.ProbeCluster` | `POST /admin/v1/clusters/{cluster_id}:probe` |
//! | `Clusters.DrainCluster` | `POST /admin/v1/clusters/{cluster_id}:drain` |
//! | `DataCenters.RegisterDc` | `POST /admin/v1/dcs` |
//! | `DataCenters.ListDcs` | `GET /admin/v1/dcs` |
//! | `DataCenters.GetDc` | `GET /admin/v1/dcs/{dc_id}` |
//! | `Tenants.CreateTenant` | `POST /admin/v1/tenants` |
//! | `Tenants.ListTenants` | `GET /admin/v1/tenants` |
//! | `Tenants.GetTenant` | `GET /admin/v1/tenants/{name}` |
//! | `Tenants.DeleteTenant` | `DELETE /admin/v1/tenants/{name}` |
//! | `Users.CreateUser` (interim) | `POST /admin/v1/tenants/{name}/users` ([`crate::users`]) |
//! | `Settings.*` | `/admin/v1/settings[/{key}]`, `/admin/v1/tenants/{name}/settings` ([`crate::settings_admin`]) |
//!
//! Settings writes at `global`, `dc` and `cluster` scope are answered only
//! by the DC leader (or, for `global`, a controller leading the global
//! tier) and carry its fencing compare; writes at tenant, database and
//! object scope are catalog changes and are answered by any controller.
//! `CreateTenant` and `DeleteTenant` write the DC's tier (the catalog's
//! shard map) with the DC leader's token, so they too are answered only by
//! the leader. Tenant calls are served when the controller was given a
//! [`TenantAdmin`] (chapter 08 step 9).
//!
//! Where the book differs: chapter 37 serves this API from every gateway
//! on 8080/8443, as gRPC and REST generated from protobuf, with platform
//! OIDC tokens and mutual TLS. Until the gateway's HTTP listeners and the
//! protobuf code generation exist, the controller serves the REST face on
//! `[controller] admin_addr` (default `127.0.0.1:7480`), authenticated by
//! an optional bearer token (`[controller] admin_token_file`).

pub mod http;
pub mod types;

use std::sync::Arc;

use sprig_cluster::probe::ProbeDepth;
use sprig_cluster::record::{AdminState, ProvisionerKind};
use sprig_cluster::registry::{ClusterRegistry, StoredCluster};
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, MetaStore, Tier, elect};
use sprig_provision::{ExternalProvisioner, RegisterCluster, unsupported};
use tokio::sync::watch;

use crate::dc::{self, DcKind, DcRegistration, Registered};
use crate::monitor::ClusterMonitor;
use crate::runtime::{Status, Stores};
use crate::settings_admin::SettingsAdmin;
use crate::tenants::TenantAdmin;
use crate::users::UserAdmin;
use types::{
    ClusterView, CreateTenantRequest, CreateTenantResponse, GetDcResponse, ListClustersResponse,
    ListDcsResponse, ListSettingKeysQuery, ListSettingKeysResponse, ListTenantsResponse,
    ProbeClusterRequest, ProbeClusterResponse, RegisterClusterRequest, RegisterClusterResponse,
    RegisterDcRequest, RegisterDcResponse, ScopeRef, SettingContextQuery, SettingExplanation,
    SettingWrite, SettingWriteResponse, TenantSettingsResponse, TenantView,
};

/// 39792 `ER_SPRIG_ADMIN_REQUEST_INVALID`.
pub fn invalid(message: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, message)
}

fn not_found(id: &ClusterId, dc: &DcId) -> SprigError {
    SprigError::new(
        SprigCode::ClusterNotFound,
        format!("no cluster {id} in dc {dc}"),
    )
}

/// The Admin API's calls, over one controller's stores.
pub struct AdminService {
    stores: Stores,
    monitor: Arc<ClusterMonitor>,
    status: watch::Receiver<Status>,
    clock: Arc<dyn Clock>,
    tenants: Option<Arc<dyn TenantAdmin>>,
    users: Option<Arc<dyn UserAdmin>>,
    settings: Option<Arc<dyn SettingsAdmin>>,
}

impl std::fmt::Debug for AdminService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminService")
            .field("dc", self.dc())
            .finish_non_exhaustive()
    }
}

impl AdminService {
    /// The service of a controller: its stores, its cluster monitor (which
    /// also holds the registry and settings) and its leadership status.
    #[must_use]
    pub fn new(
        stores: Stores,
        monitor: Arc<ClusterMonitor>,
        status: watch::Receiver<Status>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            stores,
            monitor,
            status,
            clock,
            tenants: None,
            users: None,
            settings: None,
        }
    }

    /// The same service with the tenant calls.
    #[must_use]
    pub fn with_tenants(mut self, tenants: Arc<dyn TenantAdmin>) -> Self {
        self.tenants = Some(tenants);
        self
    }

    /// The same service with the (interim) user calls.
    #[must_use]
    pub fn with_users(mut self, users: Arc<dyn UserAdmin>) -> Self {
        self.users = Some(users);
        self
    }

    /// `Users.CreateUser` (interim, [`crate::users`]): a user with a
    /// password in a tenant's catalog. Any controller answers it: the
    /// change is the catalog's.
    ///
    /// # Errors
    /// As [`UserAdmin::create`].
    pub async fn create_user(
        &self,
        tenant: &str,
        req: crate::users::CreateUserRequest,
    ) -> Result<crate::users::CreateUserResponse> {
        self.users
            .as_ref()
            .ok_or_else(|| invalid("this controller doesn't manage users"))?
            .create(tenant, req)
            .await
    }

    /// The same service with the settings calls.
    #[must_use]
    pub fn with_settings(mut self, settings: Arc<dyn SettingsAdmin>) -> Self {
        self.settings = Some(settings);
        self
    }

    fn settings(&self) -> Result<&Arc<dyn SettingsAdmin>> {
        self.settings
            .as_ref()
            .ok_or_else(|| invalid("this controller doesn't serve settings"))
    }

    /// `Settings.ListKeys`.
    ///
    /// # Errors
    /// As [`SettingsAdmin::list_keys`].
    pub async fn list_setting_keys(
        &self,
        q: &ListSettingKeysQuery,
    ) -> Result<ListSettingKeysResponse> {
        self.settings()?.list_keys(q).await
    }

    /// `Settings.GetSetting` and `ExplainSetting`.
    ///
    /// # Errors
    /// As [`SettingsAdmin::explain`].
    pub async fn explain_setting(
        &self,
        key: &str,
        q: &SettingContextQuery,
    ) -> Result<SettingExplanation> {
        self.settings()?.explain(key, q).await
    }

    /// Every settings write. `global`, `dc` and `cluster` need the leader's
    /// guard: the global leader's for `global` when this controller leads
    /// the global tier, else the DC leader's (R0, single-etcd mode).
    ///
    /// # Errors
    /// 39504 off the leader for etcd scopes; as [`SettingsAdmin::write`].
    pub async fn write_setting(&self, key: &str, w: SettingWrite) -> Result<SettingWriteResponse> {
        let s = self.settings()?;
        let global_token = self.status.borrow().global_leader;
        let guard = if !w.scope.is_etcd() {
            Vec::new()
        } else if let (ScopeRef::Global, Some(token)) = (&w.scope, global_token) {
            vec![Cmp::CreateRev(
                self.stores.global.tier().key("leader")?,
                token,
            )]
        } else {
            self.leader_guard().await?
        };
        s.write(key, &w, guard).await
    }

    /// A tenant's own settings rows and its copies of its ancestors'.
    ///
    /// # Errors
    /// As [`SettingsAdmin::tenant_settings`].
    pub async fn tenant_settings(&self, name: &str) -> Result<TenantSettingsResponse> {
        self.settings()?.tenant_settings(name).await
    }

    fn tenants(&self) -> Result<&Arc<dyn TenantAdmin>> {
        self.tenants
            .as_ref()
            .ok_or_else(|| invalid("this controller doesn't manage tenants"))
    }

    /// `Tenants.CreateTenant`: the directory entry, then the bootstrap.
    ///
    /// # Errors
    /// 39504 off the leader; as [`TenantAdmin::create`].
    pub async fn create_tenant(&self, req: CreateTenantRequest) -> Result<CreateTenantResponse> {
        let t = self.tenants()?;
        let guard = self.leader_guard().await?;
        t.create(req, guard).await
    }

    /// `Tenants.ListTenants`.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn list_tenants(&self) -> Result<ListTenantsResponse> {
        self.tenants()?.list().await
    }

    /// `Tenants.GetTenant`.
    ///
    /// # Errors
    /// 39072 for an unknown name.
    pub async fn get_tenant(&self, name: &str) -> Result<TenantView> {
        self.tenants()?.show(name).await
    }

    /// `Tenants.DeleteTenant`: `deleting`, then cleanup.
    ///
    /// # Errors
    /// 39504 off the leader; as [`TenantAdmin::delete`].
    pub async fn delete_tenant(&self, name: &str) -> Result<TenantView> {
        let t = self.tenants()?;
        let guard = self.leader_guard().await?;
        t.delete(name, guard).await
    }

    /// The data center this controller serves.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        self.monitor.registry().dc()
    }

    fn registry(&self) -> &ClusterRegistry {
        self.monitor.registry()
    }

    /// The DC leader's guard, if this controller leads.
    async fn leader_guard(&self) -> Result<Vec<Cmp>> {
        let key = self.stores.dc.tier().key("leader")?;
        let token = self.status.borrow().dc_leader;
        if let Some(token) = token {
            return Ok(vec![Cmp::CreateRev(key, token)]);
        }
        let who = elect::current_leader(&*self.stores.dc, &key)
            .await
            .ok()
            .flatten()
            .map(|l| l.record.holder);
        Err(SprigError::new(
            SprigCode::NotLeader,
            match who {
                Some(h) => format!(
                    "this controller doesn't lead dc {}; send the request to the leader, {h}",
                    self.dc()
                ),
                None => format!("dc {} has no leader yet; try again", self.dc()),
            },
        ))
    }

    async fn view(&self, s: StoredCluster) -> ClusterView {
        let health_report = self.monitor.last_report(&s.body().cluster_id).await;
        ClusterView {
            record: s.record.body,
            mod_revision: s.mod_revision,
            health_report,
            tuning_plan: Vec::new(),
        }
    }

    fn parse_id(raw: &str) -> Result<ClusterId> {
        ClusterId::new(raw).map_err(|e| invalid(format!("cluster id {raw:?}: {e}")))
    }

    /// `Clusters.RegisterCluster`: prechecks, then the record, its first
    /// profile and `active` (chapter 27, "Registering an external
    /// cluster").
    ///
    /// # Errors
    /// 39792 for a request naming another DC; 39503 for an unregistered
    /// DC; 39504 off the leader; 39510 for a provisioner other than
    /// `external`; 39105 for a name in use; 39511 when a precheck fails.
    pub async fn register_cluster(
        &self,
        req: RegisterClusterRequest,
    ) -> Result<RegisterClusterResponse> {
        let dc = req.dc.clone().unwrap_or_else(|| self.dc().clone());
        if &dc != self.dc() {
            return Err(invalid(format!(
                "this controller serves dc {}; register clusters of {dc} through its controllers",
                self.dc()
            )));
        }
        if req.provisioner != ProvisionerKind::External {
            return Err(unsupported(
                req.provisioner,
                sprig_provision::ProvisionOp::Register,
            ));
        }
        let key = Tier::Global.key(&format!("dcs/{dc}"))?;
        if self
            .stores
            .global
            .get(&key, Consistency::Linearizable)
            .await?
            .is_none()
        {
            return Err(SprigError::new(
                SprigCode::UnknownDc,
                format!("dc {dc} isn't registered: sprigctl dc register {dc}"),
            ));
        }
        let guard = self.leader_guard().await?;
        if self
            .registry()
            .list()
            .await?
            .iter()
            .any(|c| c.body().name == req.name && c.body().admin_state != AdminState::Retired)
        {
            return Err(SprigError::new(
                SprigCode::ClusterExists,
                format!("a cluster named {:?} already exists in {dc}", req.name),
            ));
        }
        let provisioner = ExternalProvisioner::new(
            self.registry().clone().with_guard(guard),
            self.monitor.settings_for(None),
        )
        .with_clock(Arc::clone(&self.clock));
        let rc = RegisterCluster {
            dc,
            name: req.name,
            topology: req.topology,
            nodes: req.nodes,
            secrets: req.secrets,
            capacity: req.capacity,
            declared: req.declared,
        };
        if req.dry_run {
            let precheck = provisioner.precheck_report(&rc).await;
            return Ok(RegisterClusterResponse {
                cluster: None,
                precheck,
            });
        }
        let (stored, precheck) = provisioner.register_report(rc).await?;
        tracing::info!(cluster = %stored.body().cluster_id, name = %stored.body().name,
            advisories = precheck.advisories.len(), "cluster registered");
        self.monitor.adopt(&stored, precheck.probes.clone()).await;
        Ok(RegisterClusterResponse {
            cluster: Some(self.view(stored).await),
            precheck,
        })
    }

    /// `Clusters.ListClusters`.
    ///
    /// # Errors
    /// 39500 if etcd is unreachable.
    pub async fn list_clusters(&self) -> Result<ListClustersResponse> {
        let mut clusters = Vec::new();
        for s in self.registry().list().await? {
            clusters.push(self.view(s).await);
        }
        clusters.sort_by_key(|c| c.record.cluster_number);
        Ok(ListClustersResponse {
            dc: self.dc().clone(),
            clusters,
        })
    }

    /// `Clusters.GetCluster`.
    ///
    /// # Errors
    /// 39104 for an unknown id; 39792 for a malformed one.
    pub async fn get_cluster(&self, id: &str) -> Result<ClusterView> {
        let id = Self::parse_id(id)?;
        match self.registry().get(&id).await? {
            Some(s) => Ok(self.view(s).await),
            None => Err(not_found(&id, self.dc())),
        }
    }

    /// `Clusters.ProbeCluster`: one pass now, full or (with `full`) deep,
    /// published with the loop's hysteresis.
    ///
    /// # Errors
    /// 39504 off the leader; 39104 for an unknown or retired cluster.
    pub async fn probe_cluster(
        &self,
        id: &str,
        req: ProbeClusterRequest,
    ) -> Result<ProbeClusterResponse> {
        let id = Self::parse_id(id)?;
        let guard = self.leader_guard().await?;
        let depth = if req.full {
            ProbeDepth::Deep
        } else {
            ProbeDepth::Full
        };
        let Some(r) = self.monitor.pass(&id, Some(depth), guard).await? else {
            return Err(not_found(&id, self.dc()));
        };
        let stored = r
            .cluster
            .ok_or_else(|| SprigError::internal("a pass without its record"))?;
        Ok(ProbeClusterResponse {
            cluster: self.view(stored).await,
            depth,
            changes: r.changes,
            held: r.held,
        })
    }

    /// `Clusters.DrainCluster`: no new placements; the balancer moves the
    /// shards away (chapter 11) and the cluster becomes `drained`.
    ///
    /// # Errors
    /// 39504 off the leader; 39104 unknown; 39106 unless `active` (a
    /// draining cluster is returned as is).
    pub async fn drain_cluster(&self, id: &str) -> Result<ClusterView> {
        let id = Self::parse_id(id)?;
        let guard = self.leader_guard().await?;
        let registry = self.registry().clone().with_guard(guard);
        let Some(cur) = registry.get(&id).await? else {
            return Err(not_found(&id, self.dc()));
        };
        let state = cur.body().admin_state;
        let stored = match state {
            AdminState::Draining => cur,
            AdminState::Active => {
                let gaps = cur.body().gaps.clone();
                registry
                    .set_admin_state(&id, AdminState::Draining, gaps)
                    .await?
            }
            other => {
                return Err(SprigError::new(
                    SprigCode::ClusterStateInvalid,
                    format!("cluster {id} is {other}; only an active cluster can drain"),
                ));
            }
        };
        tracing::info!(cluster = %id, "draining");
        Ok(self.view(stored).await)
    }

    /// `DataCenters.RegisterDc`: writes `dcs/<dc_id>` as `joining`, once.
    ///
    /// # Errors
    /// 39792 for an unknown kind; the store's errors.
    pub async fn register_dc(&self, req: RegisterDcRequest) -> Result<RegisterDcResponse> {
        let kind: DcKind = req
            .kind
            .parse()
            .map_err(|e: SprigError| invalid(e.message))?;
        let r = dc::register(
            &*self.stores.global,
            DcRegistration {
                dc: req.dc,
                kind,
                tags: req.tags,
                global_voter: req.global_voter,
            },
        )
        .await?;
        Ok(match r {
            Registered::Created(record) => RegisterDcResponse {
                created: true,
                record,
            },
            Registered::Exists(record) => RegisterDcResponse {
                created: false,
                record,
            },
        })
    }

    /// `DataCenters.ListDcs`.
    ///
    /// # Errors
    /// The store's errors.
    pub async fn list_dcs(&self) -> Result<ListDcsResponse> {
        Ok(ListDcsResponse {
            dcs: dc::list(&*self.stores.global).await?,
        })
    }

    /// `DataCenters.GetDc`, with the leader when it is this DC.
    ///
    /// # Errors
    /// 39503 for an unregistered DC; 39792 for a malformed id.
    pub async fn get_dc(&self, id: &str) -> Result<GetDcResponse> {
        let dc = DcId::new(id).map_err(|e| invalid(format!("dc id {id:?}: {e}")))?;
        let tier: Option<Arc<dyn MetaStore>> =
            (&dc == self.dc()).then(|| Arc::clone(&self.stores.dc));
        dc::show(&*self.stores.global, tier, &dc)
            .await?
            .ok_or_else(|| {
                SprigError::new(SprigCode::UnknownDc, format!("dc {dc} isn't registered"))
            })
    }
}

#[cfg(test)]
mod tests;
