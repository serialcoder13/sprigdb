//! Inherited connections and sinks (chapter 51, "Inherited sinks and
//! connections (`FOR EACH TENANT`)"; chapter 42; R9, r9more).
//!
//! ```text
//! CREATE CONNECTION c FOR EACH TENANT [scope] TYPE k WITH (url = REQUIRED, ...)
//!   CREDENTIALS (token = REQUIRED)        a definition in A: options as written
//!                                         (REQUIRED, or templates with {tenant}
//!                                         and {tenant_id}); credentials only REQUIRED
//! CREATE SINK s FOR EACH TENANT [scope] FROM STREAM d ... TO ... CONNECTION c ...
//!                                         a definition in A over an inherited stream
//!                                         d and inherited connection c of one database
//! ALTER INHERITED CONNECTION c [FOR TENANT t | FOR SUBTREE [t]] SET (...)
//!   [CREDENTIALS (...)]                   options at any level; credentials for one
//!                                         member, sealed under its key
//! ALTER INHERITED SINK s ... SET (...) | RESET | DISABLE | ENABLE
//!
//! ensure_for_stream(T, stream definition D, T's instance of D):
//!   for each sink definition S reaching T whose source is D:
//!     config := for each connection definition C that S names: C's options
//!               with the nearest overrides, templates expanded for T; the
//!               credentials T was given
//!     unchanged since the last deploy (signature): done
//!     a REQUIRED option or credential missing: the instance waits
//!       (inherited_instances row, physical.waiting_config, 39241); nothing reads
//!     else, as T's sprig_inherited (S's GRANTS plus SELECT on D's instance):
//!       C's instance: an ordinary connection of T (create, or SET and SET
//!         CREDENTIALS when the config changed); allowlist and residency are T's
//!       S's instance: an ordinary sink of T over D's instance (create, or SET
//!         its options); a new one starts at the beginning of D's instance when
//!         that came after S, else at the definition's START AT
//!       DISABLE pauses it (its position stays); ENABLE resumes it
//! ```
//!
//! The instances are ordinary connections and sinks of the member, built
//! from the definition's statement as a syntax tree (never as text, so a
//! credential is never in a statement), and checked like the member's own.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, Freshness, TenantCatalog};
use sprig_common::ids::{DbId, TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::store::MsgStore;
use sprig_inherit::msg::{
    Cause, Chain, DefKey, DefKind, DefState, Definition, GrantItem, Instance, InstanceState, What,
};
use sprig_sinks::grammar::{self as sg, OptVal};
use sprig_sinks::meta::{PlatformConnection, SinkIndex};
use sprig_sinks::model::{SinkId, SinkState, StartAt, TaskRow};
use sprig_sinks::policy::{Residency, Unpinned};
use sprig_sinks::secrets::SinkKeyring;
use sprig_sinks::sql::{Change, Handled, Host};
use sprig_sinks::store::SinksState;
use zeroize::Zeroizing;

use super::streams::StreamInstance;
use super::{catalog, state_of};
use crate::tenant::TenantServices;

/// An option of a connection definition, as written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "lowercase")]
pub enum OptSpec {
    /// A quoted string (may hold `{tenant}` and `{tenant_id}`).
    Text(String),
    /// A bare word or number.
    Bare(String),
    /// `REQUIRED`: each member sets it.
    Required,
}

impl From<&OptVal> for OptSpec {
    fn from(v: &OptVal) -> Self {
        match v {
            OptVal::Text(s) => Self::Text(s.clone()),
            OptVal::Bare(s) => Self::Bare(s.clone()),
            OptVal::Required => Self::Required,
        }
    }
}

/// `inherited_defs.definition` of a `FOR EACH TENANT` connection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnSpec {
    /// `webhook`, `kafka`, `s3`, `mysql` or `postgresql`.
    pub kind: String,
    /// `WITH (...)` in order.
    pub options: Vec<(String, OptSpec)>,
    /// `CREDENTIALS (name = REQUIRED, ...)`.
    pub required_credentials: Vec<String>,
}

/// `inherited_defs.definition` of a `FOR EACH TENANT` sink.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkSpec {
    /// The `CREATE SINK` statement without its `FOR EACH TENANT` clause.
    pub text: String,
    /// The database of the sink, its stream and its connections.
    pub db: String,
    /// The source stream definition's name.
    pub source: String,
    /// The connections it names: (name, platform).
    pub connections: Vec<(String, bool)>,
    /// `ALTER SINK ... SET` on the definition: options over the statement's.
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

/// The sink definitions reaching the member whose source is `stream`.
#[must_use]
pub fn on_stream(chain: &Chain, stream: &Definition) -> Vec<Arc<Definition>> {
    chain
        .reaching(DefKind::Sink)
        .into_iter()
        .filter(|d| d.state == DefState::Active)
        .filter(|d| {
            let Ok(spec) = serde_json::from_value::<SinkSpec>(d.definition.clone()) else {
                return false;
            };
            chain
                .resolve(&spec.db, DefKind::Stream, &spec.source)
                .is_some_and(|s| s.key() == stream.key())
        })
        .collect()
}

/// A member's configuration of one connection definition.
struct ConnConfig {
    def: Arc<Definition>,
    kind: String,
    options: Vec<(String, String)>,
    credentials: Vec<(String, Zeroizing<String>)>,
    missing: Vec<String>,
}

/// Expands `{tenant}` and `{tenant_id}`.
fn expand(v: &str, name: &str, id: TenantId) -> String {
    v.replace("{tenant}", name)
        .replace("{tenant_id}", &id.0.to_string())
}

fn cat_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
}

/// The sealed form of member credentials in an override:
/// `{"<name>": {"kv": <key version>, "b": "<base64>"}}`, under the
/// member's sink key with the connection definition's id as the
/// connection (chapter 51, "Credentials").
///
/// # Errors
/// 39216 without a key.
pub fn seal_credentials(
    keys: &dyn SinkKeyring,
    member: TenantId,
    conn_def: u64,
    creds: &[(String, String)],
) -> Result<Value> {
    let mut m = Map::new();
    for (k, v) in creds {
        let (kv, sealed) = sprig_sinks::secrets::seal(keys, member, conn_def, k, 0, v)?;
        m.insert(
            k.clone(),
            json!({"kv": kv, "b": base64::engine::general_purpose::STANDARD.encode(sealed)}),
        );
    }
    Ok(Value::Object(m))
}

fn open_credentials(
    keys: &dyn SinkKeyring,
    member: TenantId,
    conn_def: u64,
    v: &Value,
) -> Result<Vec<(String, Zeroizing<String>)>> {
    let mut out = Vec::new();
    let Some(m) = v.as_object() else {
        return Ok(out);
    };
    for (k, x) in m {
        let kv = x
            .get("kv")
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(0);
        let b = x
            .get("b")
            .and_then(Value::as_str)
            .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::SinkCredentialsInvalid,
                    format!("credential {k} of an inherited connection is malformed"),
                )
            })?;
        let plain = sprig_sinks::secrets::open(keys, member, conn_def, k, 0, kv, &b)?;
        out.push((k.clone(), plain));
    }
    Ok(out)
}

/// The member's configuration of connection definition `c`.
fn config_of(
    chain: &Chain,
    c: &Arc<Definition>,
    name: &str,
    keys: Option<&dyn SinkKeyring>,
) -> Result<ConnConfig> {
    let spec: ConnSpec = serde_json::from_value(c.definition.clone()).unwrap_or_default();
    let t = chain.member();
    let mut options = Vec::new();
    let mut missing = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (k, v) in &spec.options {
        seen.insert(k.clone());
        let o = chain
            .effective(c.key(), What::Options, Some(k))
            .and_then(|(v, _)| v.as_str().map(str::to_owned));
        match (o, v) {
            (Some(o), _) => options.push((k.clone(), expand(&o, name, t))),
            (None, OptSpec::Required) => missing.push(k.clone()),
            (None, OptSpec::Text(s) | OptSpec::Bare(s)) => {
                options.push((k.clone(), expand(s, name, t)));
            }
        }
    }
    // Options the definition doesn't name, set by an override.
    for l in &chain.levels {
        let Some(st) = &l.state else { continue };
        for o in st
            .overrides
            .iter()
            .filter(|o| o.def == c.key() && o.what == What::Options)
        {
            if let Some(m) = o.value.as_object() {
                for k in m.keys() {
                    if seen.insert(k.clone())
                        && let Some((v, _)) = chain.effective(c.key(), What::Options, Some(k))
                        && let Some(s) = v.as_str()
                    {
                        options.push((k.clone(), expand(s, name, t)));
                    }
                }
            }
        }
    }
    let mut credentials = Vec::new();
    if let Some((v, _)) = chain.effective(c.key(), What::Credentials, None) {
        match keys {
            Some(k) => credentials = open_credentials(k, t, c.object_id, &v)?,
            None => {
                return Err(SprigError::new(
                    SprigCode::SinkCredentialsInvalid,
                    "this gateway has no sink credential key ([gateway] workload_key_file)",
                ));
            }
        }
    }
    for r in &spec.required_credentials {
        if !credentials.iter().any(|(k, _)| k == r) {
            missing.push(format!("credential {r}"));
        }
    }
    Ok(ConnConfig {
        def: Arc::clone(c),
        kind: spec.kind,
        options,
        credentials,
        missing,
    })
}

/// What the instance's derived state depends on.
fn signature(chain: &Chain, s: &Definition, conns: &[ConnConfig]) -> String {
    let mut out = format!("{}:{}", s.key(), s.def_version);
    let mut keys = vec![s.key()];
    for c in conns {
        let _ = write!(out, "|c{}:{}", c.def.key(), c.def.def_version);
        keys.push(c.def.key());
    }
    for l in &chain.levels {
        let Some(st) = &l.state else { continue };
        for o in st.overrides.iter().filter(|o| keys.contains(&o.def)) {
            let _ = write!(
                out,
                "|o{}:{}:{}:{}",
                o.def,
                o.target,
                o.set_at_ms,
                o.what.as_str()
            );
        }
    }
    let _ = write!(out, "|d{}", chain.disabled(s.key()));
    out
}

/// See the module docs. Errors of one member's sink (its webhook not on
/// its allowlist, a name it uses itself) are kept in the instance row and
/// logged; they never fail the publish that triggered the check.
pub async fn ensure_for_stream(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    stream_def: &Definition,
    si: &StreamInstance,
) {
    for s in on_stream(chain, stream_def) {
        if let Err(e) = ensure_one(svc, chain, &s, si).await {
            tracing::warn!(tenant = %si.tenant, def = %s.key(), error = %e, "inherited sink not deployed");
            let _ = record(
                svc,
                si.tenant,
                &s,
                json!({"error": e.to_string(), "code": e.code.number()}),
                InstanceState::Creating,
            )
            .await;
        }
    }
}

/// Writes the member's instance row of sink definition `s`.
async fn record(
    svc: &Arc<TenantServices>,
    t: TenantId,
    s: &Definition,
    physical: Value,
    state: InstanceState,
) -> Result<()> {
    let ep = svc.locator.writer(t).await?;
    let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
    let store = MsgStore::new(ep.store.schema());
    if store.instance(&mut conn, s.key()).await?.is_none() {
        let id = ep.store.next_object_id(&mut conn).await?.0;
        store
            .claim(
                &mut conn,
                &Instance {
                    def: s.key(),
                    instance_id: id,
                    kind: DefKind::Sink,
                    def_version: s.def_version,
                    definition: s.definition.clone(),
                    state: InstanceState::Creating,
                    cause: Cause::Declare,
                    physical: None,
                    created_at_ms: 0,
                },
            )
            .await?;
    }
    use mysql_async::prelude::Queryable;
    conn.exec_drop(
        format!(
            "UPDATE {}.inherited_instances SET state = ?, def_version = ?, definition = ?, \
             physical = ?, last_used_at = UTC_TIMESTAMP(3) WHERE def_tenant_id = ? AND \
             def_object_id = ?",
            sprig_common::names::quote(ep.store.schema())
        ),
        (
            state.as_str(),
            s.def_version,
            s.definition.to_string(),
            physical.to_string(),
            s.key().tenant.0,
            s.key().object,
        ),
    )
    .await
    .map_err(cat_err)
}

#[allow(clippy::too_many_lines)]
async fn ensure_one(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    s: &Arc<Definition>,
    si: &StreamInstance,
) -> Result<()> {
    let t = si.tenant;
    let spec: SinkSpec = serde_json::from_value(s.definition.clone()).unwrap_or_default();
    let name = super::tenant_name(svc, t).await?;
    let keys = crate::exec::sinks::keyring();
    let mut conns = Vec::new();
    for (c, platform) in &spec.connections {
        if *platform {
            continue;
        }
        let d = chain
            .resolve(&spec.db, DefKind::Connection, c)
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::SinkNotFound,
                    format!(
                        "the inherited connection {}.{c} of sink {} doesn't reach this tenant",
                        spec.db, s.name
                    ),
                )
            })?;
        conns.push(config_of(chain, &d, &name, keys.as_deref())?);
    }
    let sig = signature(chain, s, &conns);
    let ep = svc.locator.writer(t).await?;
    let row = {
        let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
        let store = MsgStore::new(ep.store.schema());
        if store.migrated(&mut conn).await? {
            store.instance(&mut conn, s.key()).await?
        } else {
            None
        }
    };
    let phys = row.as_ref().and_then(|r| r.physical.clone());
    if phys
        .as_ref()
        .and_then(|p| p.get("applied"))
        .and_then(Value::as_str)
        == Some(sig.as_str())
    {
        return Ok(());
    }
    let missing: Vec<String> = conns
        .iter()
        .flat_map(|c| c.missing.iter().map(|m| format!("{}: {m}", c.def.name)))
        .collect();
    if !missing.is_empty() {
        metrics::counter!("sprig_inherit_refused_total", "reason" => "waiting_config").increment(1);
        tracing::info!(tenant = %t, def = %s.key(), missing = ?missing, "inherited sink waits for its configuration (39241)");
        return record(
            svc,
            t,
            s,
            json!({
                "waiting_config": missing,
                "code": 39241,
                "applied": sig,
            }),
            InstanceState::Creating,
        )
        .await;
    }
    // The member's sprig_inherited: the definition's GRANTS, and reading
    // the stream's instance.
    let first = phys.as_ref().and_then(|p| p.get("sink")).is_none();
    let extra = vec![GrantItem {
        privileges: vec!["SELECT".into()],
        db: spec.db.clone(),
        name: spec.source.clone(),
    }];
    let run_as = super::triggers::ensure_run_as(svc, t, s, first, &extra).await?;
    let mut host = DeployHost::new(svc, t, run_as, &spec.db).await?;
    let db_id = host
        .db_id(&spec.db)
        .ok_or_else(|| SprigError::internal("a stream instance without its database"))?;
    // The connections.
    for c in &conns {
        let cur = host.state().connection(db_id, &c.def.name).cloned();
        let options: Vec<(String, OptVal)> = c
            .options
            .iter()
            .map(|(k, v)| (k.clone(), OptVal::Text(v.clone())))
            .collect();
        let creds: Vec<(String, String)> = c
            .credentials
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().to_owned()))
            .collect();
        let qn = sg::QName {
            db: Some(spec.db.clone()),
            name: c.def.name.clone(),
        };
        match cur {
            Some(cur) if cur.owner_user_id != run_as.0 => {
                return Err(super::not_overridable(format!(
                    "this tenant has its own connection {}.{}; the inherited one can't be deployed \
                     under that name",
                    spec.db, c.def.name
                )));
            }
            Some(cur) => {
                // Keys no longer configured go back to their default.
                let gone: Vec<String> = cur
                    .options
                    .keys()
                    .filter(|k| !c.options.iter().any(|(o, _)| o == *k))
                    .cloned()
                    .collect();
                if !gone.is_empty() {
                    sprig_sinks::sql::handle_stmt(
                        sg::Stmt::AlterConnection(qn.clone(), sg::AlterConnection::Reset(gone)),
                        &mut host,
                    )
                    .await?;
                }
                sprig_sinks::sql::handle_stmt(
                    sg::Stmt::AlterConnection(qn.clone(), sg::AlterConnection::Set(options)),
                    &mut host,
                )
                .await?;
                if !creds.is_empty() {
                    sprig_sinks::sql::handle_stmt(
                        sg::Stmt::AlterConnection(qn, sg::AlterConnection::Credentials(creds)),
                        &mut host,
                    )
                    .await?;
                }
            }
            None => {
                let kind = sprig_sinks::model::ConnKind::parse(&c.kind).ok_or_else(|| {
                    SprigError::internal(format!("connection kind {} unknown", c.kind))
                })?;
                sprig_sinks::sql::handle_stmt(
                    sg::Stmt::CreateConnection(sg::CreateConnection {
                        name: qn,
                        if_not_exists: false,
                        kind,
                        options,
                        credentials: creds,
                        required_credentials: Vec::new(),
                    }),
                    &mut host,
                )
                .await?;
            }
        }
    }
    // The sink.
    let Some(sg::Stmt::CreateSink(mut cs)) = sg::parse(&spec.text)? else {
        return Err(SprigError::internal("a sink definition that doesn't parse"));
    };
    cs.name.db = Some(spec.db.clone());
    let mut opts: BTreeMap<String, String> = sg::options_map(&cs.options);
    for (k, v) in &spec.options {
        opts.insert(k.clone(), v.clone());
    }
    for (k, v) in chain
        .effective(s.key(), What::Settings, None)
        .and_then(|(v, _)| v.as_object().cloned())
        .unwrap_or_default()
    {
        let k = k.strip_prefix("sink.").unwrap_or(&k).to_owned();
        match v {
            Value::Null => {
                opts.remove(&k);
            }
            Value::String(x) => {
                opts.insert(k, x);
            }
            other => {
                opts.insert(k, other.to_string());
            }
        }
    }
    let disabled = chain.disabled(s.key());
    let cur = host.state().sink(db_id, &s.name).cloned();
    let sink_id = match cur {
        Some(cur) if cur.owner_user_id != run_as.0 => {
            return Err(super::not_overridable(format!(
                "this tenant has its own sink {}.{}; the inherited one can't be deployed under \
                 that name",
                spec.db, s.name
            )));
        }
        Some(cur) => {
            let gone: Vec<String> = cur
                .options
                .keys()
                .filter(|k| !opts.contains_key(*k))
                .cloned()
                .collect();
            let qn = cs.name.clone();
            if !gone.is_empty() {
                sprig_sinks::sql::handle_stmt(
                    sg::Stmt::AlterSink(qn.clone(), sg::AlterSink::Reset(gone)),
                    &mut host,
                )
                .await?;
            }
            if opts != cur.options {
                let set: Vec<(String, OptVal)> = opts
                    .iter()
                    .map(|(k, v)| (k.clone(), OptVal::Text(v.clone())))
                    .collect();
                sprig_sinks::sql::handle_stmt(
                    sg::Stmt::AlterSink(qn.clone(), sg::AlterSink::Set(set)),
                    &mut host,
                )
                .await?;
            }
            let paused_by_us = phys
                .as_ref()
                .and_then(|p| p.get("disabled"))
                .and_then(Value::as_bool)
                == Some(true);
            if disabled && cur.state != SinkState::Paused {
                sprig_sinks::sql::handle_stmt(
                    sg::Stmt::AlterSink(qn, sg::AlterSink::Pause),
                    &mut host,
                )
                .await?;
            } else if !disabled && paused_by_us && cur.state == SinkState::Paused {
                sprig_sinks::sql::handle_stmt(
                    sg::Stmt::AlterSink(qn, sg::AlterSink::Resume),
                    &mut host,
                )
                .await?;
            }
            cur.id.0
        }
        None => {
            // Like inherited triggers: an instance made after the
            // definition starts at its beginning (every record came after
            // the definition), else where the definition says.
            if si.created || si.stream.created_at_ms >= s.created_at_ms {
                cs.start_at = StartAt::Earliest;
            }
            cs.options = opts
                .iter()
                .map(|(k, v)| (k.clone(), OptVal::Text(v.clone())))
                .collect();
            let qn = cs.name.clone();
            sprig_sinks::sql::handle_stmt(sg::Stmt::CreateSink(cs), &mut host).await?;
            if disabled {
                sprig_sinks::sql::handle_stmt(
                    sg::Stmt::AlterSink(qn, sg::AlterSink::Pause),
                    &mut host,
                )
                .await?;
            }
            let id = host
                .state()
                .sink(db_id, &s.name)
                .map(|d| d.id.0)
                .ok_or_else(|| SprigError::internal("the sink instance vanished"))?;
            metrics::counter!("sprig_inherit_instance_creations_total", "kind" => "sink", "cause" => "declare")
                .increment(1);
            id
        }
    };
    let conn_ids: Vec<Value> = conns
        .iter()
        .filter_map(|c| host.state().connection(db_id, &c.def.name).map(|d| json!(d.id.0)))
        .collect();
    tracing::info!(tenant = %t, def = %s.key(), sink = sink_id, disabled, "inherited sink deployed");
    record(
        svc,
        t,
        s,
        json!({
            "sink": sink_id,
            "connections": conn_ids,
            "disabled": disabled,
            "applied": sig,
        }),
        InstanceState::Active,
    )
    .await
}

/// Drops member `t`'s instance of sink definition `s` (its ordinary sink,
/// and connections of `s` no other sink of `t` uses); true when there was
/// one.
///
/// # Errors
/// The catalog's.
pub async fn drop_instance(
    svc: &Arc<TenantServices>,
    t: TenantId,
    s: &Definition,
    conn_names: &[String],
) -> Result<bool> {
    let ep = svc.locator.writer(t).await?;
    let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
    let store = MsgStore::new(ep.store.schema());
    if !store.migrated(&mut conn).await? {
        return Ok(false);
    }
    let Some(row) = store.instance(&mut conn, s.key()).await? else {
        return Ok(false);
    };
    drop(conn);
    let spec: SinkSpec = serde_json::from_value(s.definition.clone()).unwrap_or_default();
    let cat = catalog(svc, t).await?;
    let run_as = sprig_auth::cache::TenantAuth::of(&cat)
        .and_then(|a| a.accounts(super::RUN_AS_USER).first().map(|u| u.row.user_id));
    if let Some(run_as) = run_as
        && row
            .physical
            .as_ref()
            .and_then(|p| p.get("sink"))
            .is_some()
    {
        let mut host = DeployHost::new(svc, t, run_as, &spec.db).await?;
        if let Some(db_id) = host.db_id(&spec.db) {
            if host
                .state()
                .sink(db_id, &s.name)
                .is_some_and(|d| d.owner_user_id == run_as.0)
            {
                sprig_sinks::sql::handle_stmt(
                    sg::Stmt::DropSink {
                        name: sg::QName {
                            db: Some(spec.db.clone()),
                            name: s.name.clone(),
                        },
                        if_exists: true,
                        with_dead_letters: true,
                    },
                    &mut host,
                )
                .await?;
            }
            for c in conn_names {
                let st = host.state();
                let Some(cd) = st.connection(db_id, c) else {
                    continue;
                };
                if cd.owner_user_id == run_as.0 && st.users_of(cd.id).is_empty() {
                    sprig_sinks::sql::handle_stmt(
                        sg::Stmt::DropConnection {
                            name: sg::QName {
                                db: Some(spec.db.clone()),
                                name: c.clone(),
                            },
                            if_exists: true,
                        },
                        &mut host,
                    )
                    .await?;
                }
            }
        }
    }
    let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
    store.delete_instance(&mut conn, s.key()).await?;
    Ok(true)
}

/// The members holding an instance of sink definition `s` (those with an
/// instance of its source stream).
///
/// # Errors
/// The catalog's.
pub async fn members_of(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    s: &Definition,
) -> Result<Vec<TenantId>> {
    let spec: SinkSpec = serde_json::from_value(s.definition.clone()).unwrap_or_default();
    let Some(d) = chain
        .resolve(&spec.db, DefKind::Stream, &spec.source)
        .or_else(|| {
            chain
                .levels
                .first()
                .and_then(|l| l.state.as_ref())
                .and_then(|st| st.def(&spec.db, DefKind::Stream, &spec.source).cloned())
        })
    else {
        return Ok(Vec::new());
    };
    Ok(super::instances_of(svc, d.key())
        .await?
        .into_iter()
        .map(|e| e.tenant)
        .collect())
}

/// The source stream definition of sink definition `s`, from `chain`
/// (the declarer's own rows included).
#[must_use]
pub fn source_of(chain: &Chain, s: &Definition) -> Option<DefKey> {
    let spec: SinkSpec = serde_json::from_value(s.definition.clone()).unwrap_or_default();
    chain
        .resolve(&spec.db, DefKind::Stream, &spec.source)
        .or_else(|| {
            chain
                .levels
                .first()
                .and_then(|l| l.state.as_ref())
                .and_then(|st| st.def(&spec.db, DefKind::Stream, &spec.source).cloned())
        })
        .map(|d| d.key())
}

/// A sink-statement host acting in member `t` as its `sprig_inherited`.
struct DeployHost {
    svc: Arc<TenantServices>,
    cat: Arc<TenantCatalog>,
    user: UserId,
    db: String,
    migrated: bool,
}

impl DeployHost {
    async fn new(svc: &Arc<TenantServices>, t: TenantId, user: UserId, db: &str) -> Result<Self> {
        Ok(Self {
            svc: Arc::clone(svc),
            cat: catalog(svc, t).await?,
            user,
            db: db.to_owned(),
            migrated: false,
        })
    }

    fn lctn(&self) -> bool {
        self.svc.settings.get(
            &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
            &self.cat.setting_ctx(&self.svc.dc),
            Some(&self.cat.settings),
        ) == 1
    }

    async fn conn(&self) -> Result<(mysql_async::Conn, String)> {
        let ep = self.svc.locator.writer(self.cat.tenant).await?;
        let c = ep.pool.get_conn().await.map_err(cat_err)?;
        Ok((c, ep.store.schema().to_owned()))
    }
}

#[async_trait]
impl Host for DeployHost {
    fn tenant(&self) -> TenantId {
        self.cat.tenant
    }

    fn user_id(&self) -> Option<u32> {
        Some(self.user.0)
    }

    fn current_db(&self) -> Option<String> {
        Some(self.db.clone())
    }

    fn db_id(&self, name: &str) -> Option<u32> {
        self.cat
            .database(&database_key(name, self.lctn()))
            .map(|d| d.db_id.0)
    }

    fn db_name(&self, id: u32) -> Option<String> {
        self.cat.databases.get(DbId(id)).map(|d| d.name.clone())
    }

    fn state(&self) -> Arc<SinksState> {
        Arc::new(
            self.cat
                .ext
                .get::<SinksState>(sprig_sinks::store::MODULE)
                .cloned()
                .unwrap_or_default(),
        )
    }

    fn stream(&self, db: u32, name: &str) -> Option<(u64, u32)> {
        let st = self
            .cat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)?;
        st.get(DbId(db), name, self.lctn())
            .map(|s| (s.id.0, s.partitions))
    }

    fn table(&self, db: u32, name: &str) -> Option<u64> {
        self.cat
            .object(
                DbId(db),
                Namespace::Relation,
                &object_key(ObjectKind::Table, name, self.lctn()),
            )
            .filter(|o| o.state == ObjectState::Active)
            .map(|o| o.object_id.0)
    }

    fn sink_admin(&self) -> bool {
        // The declarer held SPRIG_SINK_ADMIN; the member's sprig_inherited
        // runs what it declared.
        true
    }

    fn tenant_admin(&self) -> bool {
        false
    }

    fn can_select(&self, _db: &str, _name: &str) -> bool {
        // Granted to sprig_inherited with the instance.
        true
    }

    fn setting(&self, key: &str, db: u32) -> Option<String> {
        crate::exec::sinks::setting_of(&self.svc, &self.cat, key, db)
    }

    fn residency(&self) -> Box<dyn Residency> {
        Box::new(Unpinned)
    }

    fn keyring(&self) -> Option<Arc<dyn SinkKeyring>> {
        crate::exec::sinks::keyring()
    }

    fn user_by_name(&self, name: &str) -> Option<u32> {
        let auth = sprig_auth::TenantAuth::of(&self.cat)?;
        auth.accounts(name).first().map(|u| u.row.user_id.0)
    }

    fn user_name(&self, id: u32) -> Option<String> {
        let auth = sprig_auth::TenantAuth::of(&self.cat)?;
        auth.user(UserId(id)).map(|u| u.row.name.clone())
    }

    async fn next_id(&mut self) -> Result<u64> {
        let ep = self.svc.locator.writer(self.cat.tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(cat_err)?;
        Ok(ep.store.next_object_id(&mut conn).await?.0)
    }

    async fn commit(&mut self, change: Change) -> Result<()> {
        let tenant = self.cat.tenant;
        if !self.migrated {
            let (mut conn, schema) = self.conn().await?;
            sprig_sinks::store::migrate(&mut conn, &schema).await?;
            self.migrated = true;
        }
        let mut c = self.svc.catalog.change(tenant, VersionKind::Catalog).await?;
        let keys = crate::exec::sinks::keyring();
        let applied = {
            let (store, tx) = c.tx().parts();
            sprig_sinks::sql::apply(tx, store.schema(), &change, keys.as_deref(), tenant, self.user.0)
                .await
        };
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        tracing::info!(tenant = %tenant, version = v, "inherited sink catalog change");
        self.cat = self
            .svc
            .cache
            .get(tenant, Freshness::Cached { min_version: v })
            .await?;
        Ok(())
    }

    async fn tasks(&mut self, sink: SinkId) -> Result<Vec<TaskRow>> {
        let (mut c, schema) = self.conn().await?;
        let store = sprig_sinks::store::SinksStore::new(schema);
        if !store.migrated(&mut c).await? {
            return Ok(Vec::new());
        }
        store.tasks(&mut c, sink).await
    }

    async fn live_states(
        &mut self,
    ) -> Result<std::collections::HashMap<u64, (SinkState, Option<String>)>> {
        let (mut c, schema) = self.conn().await?;
        let store = sprig_sinks::store::SinksStore::new(schema);
        if !store.migrated(&mut c).await? {
            return Ok(std::collections::HashMap::new());
        }
        store.states(&mut c).await
    }

    async fn index(&mut self, sink: SinkId, ix: Option<SinkIndex>) -> Result<()> {
        let dc = self.svc.locator.dc_store();
        match ix {
            Some(ix) => sprig_sinks::meta::index_put(&dc, &ix).await,
            None => sprig_sinks::meta::index_delete(&dc, self.cat.tenant.0, sink.0).await,
        }
    }

    async fn platform(&mut self, name: &str) -> Result<Option<(PlatformConnection, bool)>> {
        let Some(p) = sprig_sinks::meta::platform_get(&self.svc.global, name).await? else {
            return Ok(None);
        };
        let ancestors: Vec<u64> = self.cat.ancestry.iter().map(|a| a.id.0).collect();
        let granted = sprig_sinks::meta::platform_granted(
            &self.svc.global,
            name,
            self.cat.tenant.0,
            &ancestors,
        )
        .await?;
        Ok(Some((p, granted)))
    }

    async fn platform_granted(&mut self) -> Result<Vec<PlatformConnection>> {
        Ok(Vec::new())
    }
}
