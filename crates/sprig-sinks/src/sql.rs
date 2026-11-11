//! The sink statements in a tenant session (chapter 51, build steps 2–4,
//! 15 and 18): the checks, the catalog change, the DC index entry the
//! controller starts tasks from, and the `SHOW` results. The gateway
//! implements [`Host`]; nothing here knows its session types.
//!
//! Checks, in order (chapter 51, "Security notes"):
//! - creating, altering or dropping sinks and connections needs
//!   `SPRIG_SINK_ADMIN` (granted by name only; 39212);
//! - naming a connection needs `USAGE` on it, its ownership, or
//!   `SPRIG_TENANT_ADMIN` (39212); a platform connection needs the
//!   tenant's grant (39233) and `USAGE` for users who aren't tenant or
//!   sink administrators;
//! - the owner needs `SELECT` on the source (39212);
//! - `sink.enabled`, `sink.allowed_kinds` (39230), `sink.max_per_tenant`
//!   (39226);
//! - every host a connection names is on `sink.destination_allowlist` and
//!   satisfies the tenant's residency (39213, 39214);
//! - credentials the type doesn't take (39216), `EXACTLY ONCE` where the
//!   destination can't give it (39217), options (39218), formats (39231).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use serde_json::{Map, Value};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode};
use zeroize::Zeroizing;

use crate::grammar::{self, AlterConnection, AlterSink, ConnName, OptVal, Stmt, TargetAst};
use crate::meta::{PlatformConnection, SinkIndex};
use crate::model::{
    ConnKind, ConnRef, ConnectionDef, ConnectionId, Guarantee, ScriptLanguage, SinkDef, SinkId,
    SinkState, SourceKind, SourceSpec, StartAt, TargetKind, TargetSpec, TaskRow, bad_option, err,
};
use crate::policy::{Destinations, Residency, url_host_port};
use crate::secrets::SinkKeyring;
use crate::store::{SinksState, SinksStore, Usage};

/// A catalog change one statement makes.
#[derive(Debug)]
pub enum Change {
    /// `CREATE CONNECTION`, with credentials to seal.
    InsertConnection(ConnectionDef, Vec<(String, Zeroizing<String>)>),
    /// `ALTER CONNECTION SET | RESET`.
    UpdateConnection(ConnectionDef),
    /// `ALTER CONNECTION SET CREDENTIALS`.
    Credentials(ConnectionId, Vec<(String, Zeroizing<String>)>),
    /// `DROP CONNECTION`.
    DropConnection(ConnectionDef),
    /// `CREATE SINK`.
    InsertSink(SinkDef),
    /// `ALTER SINK SET | RESET | OWNER TO | SET SCRIPT`.
    UpdateSink(SinkDef),
    /// `ALTER SINK RESET TO`: the definition, and its positions dropped.
    ResetSink(SinkDef),
    /// `ALTER SINK PAUSE | RESUME`.
    State(SinkId, SinkState, Option<String>),
    /// `DROP SINK`.
    DropSink(SinkDef),
    /// `GRANT | REVOKE USAGE`.
    Usage(Usage, bool),
}

/// What the statement layer needs from a tenant session.
#[async_trait]
pub trait Host: Send {
    /// The tenant.
    fn tenant(&self) -> TenantId;
    /// The session's user id.
    fn user_id(&self) -> Option<u32>;
    /// The current database.
    fn current_db(&self) -> Option<String>;
    /// A logical database's id.
    fn db_id(&self, name: &str) -> Option<u32>;
    /// A logical database's name.
    fn db_name(&self, id: u32) -> Option<String>;
    /// The sinks module in the session's snapshot.
    fn state(&self) -> Arc<SinksState>;
    /// A stream (or change topic) of `db`: its id and partitions.
    fn stream(&self, db: u32, name: &str) -> Option<(u64, u32)>;
    /// A table of `db`: its object id.
    fn table(&self, db: u32, name: &str) -> Option<u64>;
    /// The session holds `SPRIG_SINK_ADMIN`.
    fn sink_admin(&self) -> bool;
    /// The session holds `SPRIG_TENANT_ADMIN`.
    fn tenant_admin(&self) -> bool;
    /// The session may `SELECT` from `name` in `db`.
    fn can_select(&self, db: &str, name: &str) -> bool;
    /// A resolved `sink.*` setting (tenant, then database scope) as text.
    fn setting(&self, key: &str, db: u32) -> Option<String>;
    /// The tenant's residency policy.
    fn residency(&self) -> Box<dyn Residency>;
    /// The sink credential keyring.
    fn keyring(&self) -> Option<Arc<dyn SinkKeyring>>;
    /// A user of the tenant by name.
    fn user_by_name(&self, name: &str) -> Option<u32>;
    /// A user's name.
    fn user_name(&self, id: u32) -> Option<String>;
    /// A new object id.
    async fn next_id(&mut self) -> Result<u64>;
    /// Applies `change` in one versioned catalog change (through
    /// [`apply`]) and refreshes the session's snapshot.
    async fn commit(&mut self, change: Change) -> Result<()>;
    /// A sink's tasks.
    async fn tasks(&mut self, sink: SinkId) -> Result<Vec<TaskRow>>;
    /// Every sink's state and reason as the catalog holds them now.
    async fn live_states(
        &mut self,
    ) -> Result<std::collections::HashMap<u64, (SinkState, Option<String>)>>;
    /// Writes (or with `None` removes) the sink's DC index entry.
    async fn index(&mut self, sink: SinkId, ix: Option<SinkIndex>) -> Result<()>;
    /// A platform connection and whether the tenant holds a grant.
    async fn platform(&mut self, name: &str) -> Result<Option<(PlatformConnection, bool)>>;
    /// Every platform connection granted to the tenant.
    async fn platform_granted(&mut self) -> Result<Vec<PlatformConnection>>;
    /// The session holds `SPRIG_FAN_IN`.
    fn fan_in(&self) -> bool {
        false
    }
    /// The live logic function `name` of `db`: its runtime and source text
    /// (`TRANSFORM`, R9). `None`: no such function.
    ///
    /// # Errors
    /// The catalog's.
    async fn function_source(&mut self, _db: u32, _name: &str) -> Result<Option<(String, String)>> {
        Ok(None)
    }
    /// Version `version` (`None`: the active one) of the live logic
    /// function `name` of `db`: its runtime, version, source and recorded
    /// signature (`TRANSFORM` with `transform_version` and `EXACTLY ONCE`,
    /// R9 r9fwd). The default asks [`Self::function_source`] and knows no
    /// version or signature.
    ///
    /// # Errors
    /// The catalog's.
    async fn function_version(
        &mut self,
        db: u32,
        name: &str,
        version: Option<u64>,
    ) -> Result<Option<(String, u64, String, Value)>> {
        if version.is_some() {
            return Ok(None);
        }
        Ok(self
            .function_source(db, name)
            .await?
            .map(|(r, s)| (r, 0, s, Value::Null)))
    }
    /// The change topic (`t.changes`) of table object `table` in `db`: its
    /// stream id and partitions, when the table's change topic is on.
    fn change_stream(&self, _db: u32, _table: u64) -> Option<(u64, u32)> {
        None
    }
    /// The inherited stream definition `db.name` a fan-in sink of this
    /// tenant may read (declared here or above, the tenant in reach of its
    /// members), with `UNDER t` resolved to a member below this tenant.
    /// `None`: no such definition.
    ///
    /// # Errors
    /// `UNDER` names no tenant below this one; the catalog's.
    async fn fan_in_stream(
        &mut self,
        _db: &str,
        _name: &str,
        _under: Option<&str>,
    ) -> Result<Option<FanInDef>> {
        Ok(None)
    }

    /// The durable queue `name` of database `db` (its vhost): its id
    /// (R9, r9queue). `None` when there is none, or this host can't tell.
    fn queue(&self, _db: u32, _name: &str) -> Option<u64> {
        None
    }

    /// What `TEST CONNECTION` and `TEST SINK` open connections and read
    /// sources through, as a sink task would (R9, r9tests). `None`: this
    /// host can't run them.
    fn sink_env(&self) -> Option<Arc<dyn crate::job::SinkEnv>> {
        None
    }

    /// Up to `n` messages of queue `queue` in database `db`, browsed (not
    /// taken, not locked) with the session's rights, as sink records
    /// (`TEST SINK`, R9 r9tests).
    ///
    /// # Errors
    /// The queue engine's; by default, that this host has none.
    async fn sample_queue(
        &mut self,
        _db: &str,
        _queue: &str,
        _n: usize,
    ) -> Result<Vec<crate::model::SinkRecord>> {
        Err(err(
            SprigCode::SinkOptionInvalid,
            "this gateway can't read queues for TEST SINK",
        ))
    }

    /// Whether the user may `DELETE` (consume) `db.name` (a queue).
    fn can_delete(&self, db: &str, name: &str) -> bool {
        self.can_select(db, name)
    }

    /// The `FOR EACH TENANT` queue definition `db.name` a fan-in sink of
    /// this tenant may read (`SPRIG_FAN_IN` checked), and `UNDER t`'s id
    /// (R9, r9queue); `partitions` is 0.
    ///
    /// # Errors
    /// `UNDER` names no tenant below this one; the catalog's.
    async fn fan_in_queue(
        &mut self,
        _db: &str,
        _name: &str,
        _under: Option<&str>,
    ) -> Result<Option<FanInDef>> {
        Ok(None)
    }
}

/// An inherited stream definition a fan-in sink reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanInDef {
    /// The declaring tenant.
    pub def_tenant: u64,
    /// The definition's object id.
    pub def_object: u64,
    /// Its partitions.
    pub partitions: u32,
    /// `UNDER t`'s tenant id.
    pub under: Option<u64>,
}

/// What a statement answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Handled {
    /// Not a sink statement.
    NotSink,
    /// OK.
    Ok {
        /// Rows affected.
        affected: u64,
    },
    /// A result set of text columns.
    Rows {
        /// Column names.
        columns: Vec<String>,
        /// Rows.
        rows: Vec<Vec<Option<String>>>,
    },
}

fn need_admin(h: &dyn Host) -> Result<()> {
    if h.sink_admin() {
        Ok(())
    } else {
        Err(err(
            SprigCode::SinkPrivilege,
            "Access denied; you need the SPRIG_SINK_ADMIN privilege for this operation",
        ))
    }
}

fn db_of(h: &dyn Host, db: Option<&str>) -> Result<(u32, String)> {
    let name = db
        .map(str::to_owned)
        .or_else(|| h.current_db())
        .ok_or_else(|| {
            sprig_common::SprigError::standard(1046, *b"3D000", "No database selected")
        })?;
    let id = h.db_id(&name).ok_or_else(|| {
        sprig_common::SprigError::standard(1049, *b"42000", format!("Unknown database '{name}'"))
    })?;
    Ok((id, name))
}

fn opts_map(opts: &[(String, OptVal)]) -> Result<Map<String, Value>> {
    let mut m = Map::new();
    for (k, v) in opts {
        match v {
            OptVal::Required => {
                return Err(bad_option(format!(
                    "{k} = REQUIRED is for FOR EACH TENANT connections, whose members each set it"
                )));
            }
            OptVal::Text(s) | OptVal::Bare(s) => {
                m.insert(k.clone(), Value::String(s.clone()));
            }
        }
    }
    Ok(m)
}

/// The `host:port` destinations a connection names.
///
/// # Errors
/// 39218 for missing or bad endpoint options.
pub fn hosts_of(kind: ConnKind, o: &Map<String, Value>) -> Result<Vec<(String, u16)>> {
    let get = |k: &str| o.get(k).and_then(Value::as_str).map(str::to_owned);
    let need = |k: &str| {
        get(k).ok_or_else(|| bad_option(format!("{kind} connections need the option {k}")))
    };
    let host_port = |hp: &str, default: u16| -> Result<(String, u16)> {
        let hp = hp.trim();
        match hp.rsplit_once(':') {
            Some((h, p)) if !h.contains(':') || h.starts_with('[') => Ok((
                h.trim_start_matches('[').trim_end_matches(']').to_owned(),
                p.parse()
                    .map_err(|_| bad_option(format!("bad port in '{hp}'")))?,
            )),
            _ => Ok((hp.to_owned(), default)),
        }
    };
    Ok(match kind {
        ConnKind::Webhook => {
            let (h, p, _) = url_host_port(&need("url")?)?;
            let mut v = vec![(h, p)];
            if let Some(t) = get("token_url") {
                let (h, p, _) = url_host_port(&t)?;
                v.push((h, p));
            }
            v
        }
        ConnKind::Kafka => need("bootstrap")?
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| host_port(s, 9092))
            .collect::<Result<_>>()?,
        ConnKind::S3 => {
            let (h, p, _) = url_host_port(&need("endpoint")?)?;
            need("bucket")?;
            vec![(h, p)]
        }
        ConnKind::Mysql | ConnKind::Postgresql => {
            let h = need("host")?;
            let def = if kind == ConnKind::Mysql { 3306 } else { 5432 };
            let p = match o.get("port") {
                None => def,
                Some(v) => v
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| v.to_string())
                    .parse()
                    .map_err(|_| bad_option("port must be a number"))?,
            };
            vec![(h, p)]
        }
    })
}

fn destinations(h: &dyn Host, db: u32) -> Result<Destinations> {
    Ok(Destinations {
        allow: crate::policy::parse_allowlist(
            &h.setting("destination_allowlist", db)
                .unwrap_or_else(|| "[]".into()),
        )?,
        residency: h.residency(),
        platform_hosts: None,
        metadata_allowed: h.setting("platform_identity", db).as_deref() == Some("full"),
    })
}

/// Checks a connection's hosts and returns them with the residency tags.
fn check_hosts(
    h: &dyn Host,
    db: u32,
    kind: ConnKind,
    options: &Map<String, Value>,
) -> Result<(Vec<String>, Option<Map<String, Value>>)> {
    let d = destinations(h, db)?;
    let mut hosts = Vec::new();
    let mut tags = None;
    for (host, port) in hosts_of(kind, options)? {
        let c = d.check(&host, port, Some(kind), Some(options))?;
        if tags.is_none() {
            tags = c.tags;
        }
        hosts.push(format!("{host}:{port}"));
    }
    if kind == ConnKind::S3 {
        let mode = options
            .get("credentials")
            .and_then(Value::as_str)
            .unwrap_or("static");
        let allowed = h
            .setting("platform_identity", db)
            .unwrap_or_else(|| "off".into());
        let ok = match mode {
            "static" => true,
            "assume_role" => allowed == "assume_role" || allowed == "full",
            "web_identity" | "instance" => allowed == "full",
            other => return Err(bad_option(format!("unknown S3 credentials mode {other}"))),
        };
        if !ok {
            return Err(err(
                SprigCode::SinkPlatformIdentityDenied,
                format!("S3 credentials = {mode} needs sink.platform_identity (now {allowed})"),
            ));
        }
    }
    Ok((hosts, tags))
}

fn secrets_of(kind: ConnKind, c: &[(String, String)]) -> Result<Vec<(String, Zeroizing<String>)>> {
    let names: Vec<&str> = c.iter().map(|(k, _)| k.as_str()).collect();
    crate::secrets::check_names(kind, &names)?;
    Ok(c.iter()
        .map(|(k, v)| (k.clone(), Zeroizing::new(v.clone())))
        .collect())
}

/// Handles one statement.
///
/// # Errors
/// The statement's error, as the client sees it.
pub async fn handle(sql: &str, h: &mut dyn Host) -> Result<Handled> {
    let Some(st) = grammar::parse(sql)? else {
        return Ok(Handled::NotSink);
    };
    handle_stmt(st, h).await
}

/// Handles one parsed statement (also how an inherited connection or sink
/// is deployed in a member, R9: built from its definition, so credentials
/// never become statement text).
///
/// # Errors
/// The statement's error, as the client sees it.
#[allow(clippy::too_many_lines)]
pub async fn handle_stmt(st: Stmt, h: &mut dyn Host) -> Result<Handled> {
    let ok = Ok(Handled::Ok { affected: 0 });
    match st {
        Stmt::CreateConnection(c) => {
            need_admin(h)?;
            let (db, _) = db_of(h, c.name.db.as_deref())?;
            if h.state().connection(db, &c.name.name).is_some() {
                if c.if_not_exists {
                    return ok;
                }
                return Err(err(
                    SprigCode::SinkExists,
                    format!("connection {} exists", c.name.name),
                ));
            }
            let options = opts_map(&c.options)?;
            if let Some(k) = c.required_credentials.first() {
                return Err(bad_option(format!(
                    "{k} = REQUIRED is for FOR EACH TENANT connections, whose members each set it"
                )));
            }
            let (hosts, tags) = check_hosts(h, db, c.kind, &options)?;
            let secrets = secrets_of(c.kind, &c.credentials)?;
            if !secrets.is_empty() && h.keyring().is_none() {
                return Err(err(
                    SprigCode::SinkCredentialsInvalid,
                    "this gateway has no sink credential key ([gateway] workload_key_file)",
                ));
            }
            let id = h.next_id().await?;
            let def = ConnectionDef {
                id: ConnectionId(id),
                db_id: db,
                name: c.name.name.clone(),
                kind: c.kind,
                options,
                hosts,
                residency_tags: tags,
                owner_user_id: h.user_id().unwrap_or(0),
                secrets: Vec::new(),
            };
            h.commit(Change::InsertConnection(def, secrets)).await?;
            ok
        }
        Stmt::AlterConnection(name, a) => {
            need_admin(h)?;
            let (db, _) = db_of(h, name.db.as_deref())?;
            let st = h.state();
            let cur = st.connection(db, &name.name).ok_or_else(|| {
                err(
                    SprigCode::SinkNotFound,
                    format!("no connection {}", name.name),
                )
            })?;
            match a {
                AlterConnection::Credentials(c) => {
                    let secrets = secrets_of(cur.kind, &c)?;
                    if h.keyring().is_none() {
                        return Err(err(
                            SprigCode::SinkCredentialsInvalid,
                            "this gateway has no sink credential key",
                        ));
                    }
                    h.commit(Change::Credentials(cur.id, secrets)).await?;
                    metrics::counter!("sprig_sink_credential_rotations_total").increment(1);
                }
                other => {
                    let mut def = (**cur).clone();
                    match other {
                        AlterConnection::Set(o) => {
                            for (k, v) in opts_map(&o)? {
                                def.options.insert(k, v);
                            }
                        }
                        AlterConnection::Reset(keys) => {
                            for k in keys {
                                def.options.remove(&k.to_ascii_lowercase());
                            }
                        }
                        AlterConnection::Credentials(_) => {}
                    }
                    let (hosts, tags) = check_hosts(h, db, def.kind, &def.options)?;
                    def.hosts = hosts;
                    def.residency_tags = tags;
                    h.commit(Change::UpdateConnection(def)).await?;
                }
            }
            ok
        }
        Stmt::DropConnection { name, if_exists } => {
            need_admin(h)?;
            let (db, _) = db_of(h, name.db.as_deref())?;
            let st = h.state();
            let Some(cur) = st.connection(db, &name.name) else {
                if if_exists {
                    return ok;
                }
                return Err(err(
                    SprigCode::SinkNotFound,
                    format!("no connection {}", name.name),
                ));
            };
            let users = st.users_of(cur.id);
            if let Some(s) = users.first() {
                return Err(err(
                    SprigCode::SinkConnectionInUse,
                    format!("connection {} is used by sink {}", name.name, s.name),
                ));
            }
            h.commit(Change::DropConnection((**cur).clone())).await?;
            ok
        }
        Stmt::ShowConnections(db) => {
            let filter = db.map(|d| db_of(h, Some(&d))).transpose()?.map(|x| x.0);
            let st = h.state();
            let rows = st
                .connections
                .values()
                .filter(|c| filter.is_none_or(|d| d == c.db_id))
                .map(|c| {
                    vec![
                        h.db_name(c.db_id),
                        Some(c.name.clone()),
                        Some(c.kind.as_str().to_owned()),
                        Some(c.hosts.join(",")),
                        Some(
                            c.secrets
                                .iter()
                                .map(|s| format!("{} (v{})", s.name, s.version))
                                .collect::<Vec<_>>()
                                .join(", "),
                        ),
                        c.residency_tags
                            .as_ref()
                            .map(|t| Value::Object(t.clone()).to_string()),
                    ]
                })
                .collect();
            Ok(Handled::Rows {
                columns: [
                    "Database",
                    "Name",
                    "Type",
                    "Hosts",
                    "Credentials",
                    "Residency",
                ]
                .map(String::from)
                .to_vec(),
                rows,
            })
        }
        Stmt::ShowCreateConnection(name) => {
            let (db, _) = db_of(h, name.db.as_deref())?;
            let st = h.state();
            let c = st.connection(db, &name.name).ok_or_else(|| {
                err(
                    SprigCode::SinkNotFound,
                    format!("no connection {}", name.name),
                )
            })?;
            Ok(Handled::Rows {
                columns: vec!["Connection".into(), "Create Connection".into()],
                rows: vec![vec![Some(c.name.clone()), Some(render_connection(c, h))]],
            })
        }
        Stmt::ShowPlatformConnections => {
            let all = h.platform_granted().await?;
            let admin = h.tenant_admin() || h.sink_admin();
            let me = h.user_id().unwrap_or(u32::MAX);
            let st = h.state();
            let rows = all
                .iter()
                .filter(|p| {
                    admin
                        || st
                            .usage
                            .iter()
                            .any(|u| u.platform && u.connection == p.name && u.user_id == me)
                })
                .map(|p| {
                    let v = p.tenant_view();
                    vec![
                        Some(p.name.clone()),
                        Some(p.kind.as_str().to_owned()),
                        v.get("description")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        v.get("tags").map(Value::to_string),
                        v.get("options").map(Value::to_string),
                    ]
                })
                .collect();
            Ok(Handled::Rows {
                columns: ["Name", "Type", "Description", "Tags", "Options"]
                    .map(String::from)
                    .to_vec(),
                rows,
            })
        }
        Stmt::Usage {
            grant,
            conn,
            db,
            user,
        } => {
            if !(h.tenant_admin() || h.sink_admin()) {
                return Err(err(
                    SprigCode::SinkPrivilege,
                    "granting USAGE on a connection needs SPRIG_SINK_ADMIN or SPRIG_TENANT_ADMIN",
                ));
            }
            let (dbid, _) = db_of(h, db.as_deref())?;
            if conn.platform {
                match h.platform(&conn.name).await? {
                    Some((_, true)) => {}
                    Some((_, false)) | None => {
                        return Err(err(
                            SprigCode::SinkPlatformConnectionNotGranted,
                            format!(
                                "platform connection {} isn't granted to this tenant",
                                conn.name
                            ),
                        ));
                    }
                }
            } else if h.state().connection(dbid, &conn.name).is_none() {
                return Err(err(
                    SprigCode::SinkNotFound,
                    format!("no connection {}", conn.name),
                ));
            }
            let uid = h.user_by_name(&user).ok_or_else(|| {
                sprig_common::SprigError::standard(
                    1133,
                    *b"28000",
                    format!("Can't find any matching row in the user table for '{user}'"),
                )
            })?;
            h.commit(Change::Usage(
                Usage {
                    db_id: if conn.platform { 0 } else { dbid },
                    connection: conn.name,
                    platform: conn.platform,
                    user_id: uid,
                },
                grant,
            ))
            .await?;
            ok
        }
        Stmt::CreateSink(c) => create_sink(h, *c).await,
        Stmt::AlterSink(name, a) => {
            need_admin(h)?;
            let (db, dbname) = db_of(h, name.db.as_deref())?;
            let st = h.state();
            let cur = st
                .sink(db, &name.name)
                .ok_or_else(|| err(SprigCode::SinkNotFound, format!("no sink {}", name.name)))?;
            let mut def = (**cur).clone();
            match a {
                AlterSink::Pause => {
                    h.commit(Change::State(
                        def.id,
                        SinkState::Paused,
                        Some("user: ALTER SINK PAUSE".into()),
                    ))
                    .await?;
                }
                AlterSink::Resume => {
                    if def.state == SinkState::Dropping {
                        return Err(err(SprigCode::SinkState, "the sink is being dropped"));
                    }
                    if def.state == SinkState::Failed {
                        return Err(err(
                            SprigCode::SinkState,
                            "a failed sink resumes with ALTER SINK ... RESET TO",
                        ));
                    }
                    h.commit(Change::State(def.id, SinkState::Running, None))
                        .await?;
                }
                AlterSink::Set(o) => {
                    for (k, v) in grammar::options_map(&o) {
                        check_option(&k, &v)?;
                        def.options.insert(k, v);
                    }
                    h.commit(Change::UpdateSink(def.clone())).await?;
                    publish_index(h, &def).await?;
                }
                AlterSink::Reset(keys) => {
                    for k in keys {
                        def.options.remove(k.strip_prefix("sink.").unwrap_or(&k));
                    }
                    h.commit(Change::UpdateSink(def.clone())).await?;
                    publish_index(h, &def).await?;
                }
                AlterSink::ResetTo(at) => {
                    def.start_at = at;
                    def.state = SinkState::Running;
                    def.state_reason = None;
                    h.commit(Change::ResetSink(def.clone())).await?;
                    publish_index(h, &def).await?;
                }
                AlterSink::Owner(u) => {
                    let uid = h.user_by_name(&u).ok_or_else(|| {
                        sprig_common::SprigError::standard(
                            1133,
                            *b"28000",
                            format!("Can't find any matching row in the user table for '{u}'"),
                        )
                    })?;
                    def.owner_user_id = uid;
                    h.commit(Change::UpdateSink(def.clone())).await?;
                    publish_index(h, &def).await?;
                }
                AlterSink::Script(src) => {
                    let TargetSpec::Script { source, .. } = &mut def.target else {
                        return Err(bad_option(format!("{} isn't a script sink", def.name)));
                    };
                    *source = Some(src);
                    h.commit(Change::UpdateSink(def.clone())).await?;
                    publish_index(h, &def).await?;
                }
            }
            let _ = dbname;
            ok
        }
        Stmt::DropSink {
            name, if_exists, ..
        } => {
            need_admin(h)?;
            let (db, _) = db_of(h, name.db.as_deref())?;
            let st = h.state();
            let Some(cur) = st.sink(db, &name.name) else {
                if if_exists {
                    return ok;
                }
                return Err(err(
                    SprigCode::SinkNotFound,
                    format!("no sink {}", name.name),
                ));
            };
            let def = (**cur).clone();
            h.index(def.id, None).await?;
            h.commit(Change::DropSink(def)).await?;
            ok
        }
        Stmt::ShowSinks(db) => {
            let filter = db.map(|d| db_of(h, Some(&d))).transpose()?.map(|x| x.0);
            let live = h.live_states().await?;
            let st = h.state();
            let rows = st
                .sinks
                .values()
                .filter(|s| filter.is_none_or(|d| d == s.db_id))
                .map(|s| {
                    vec![
                        h.db_name(s.db_id),
                        Some(s.name.clone()),
                        Some(format!("{} {}", s.source.kind(), s.source.name())),
                        Some(s.target.kind().as_str().to_owned()),
                        s.target_connection().map(|c| c.name().to_owned()),
                        Some(s.guarantee.as_str().to_owned()),
                        Some(
                            live.get(&s.id.0)
                                .map_or(s.state, |l| l.0)
                                .as_str()
                                .to_owned(),
                        ),
                        live.get(&s.id.0)
                            .map_or_else(|| s.state_reason.clone(), |l| l.1.clone()),
                    ]
                })
                .collect();
            Ok(Handled::Rows {
                columns: [
                    "Database",
                    "Name",
                    "Source",
                    "Target",
                    "Connection",
                    "Guarantee",
                    "State",
                    "Reason",
                ]
                .map(String::from)
                .to_vec(),
                rows,
            })
        }
        Stmt::ShowCreateSink(name) => {
            let (db, _) = db_of(h, name.db.as_deref())?;
            let st = h.state();
            let s = st
                .sink(db, &name.name)
                .ok_or_else(|| err(SprigCode::SinkNotFound, format!("no sink {}", name.name)))?;
            Ok(Handled::Rows {
                columns: vec!["Sink".into(), "Create Sink".into()],
                rows: vec![vec![Some(s.name.clone()), Some(render_sink(s))]],
            })
        }
        Stmt::TestConnection(c, db) => test_connection(h, &c, db.as_deref()).await,
        Stmt::TestSink { name, sample } => test_sink(h, &name, sample).await,
        Stmt::ShowSinkStatus(name) => {
            let (db, _) = db_of(h, name.db.as_deref())?;
            let st = h.state();
            let s = st
                .sink(db, &name.name)
                .ok_or_else(|| err(SprigCode::SinkNotFound, format!("no sink {}", name.name)))?
                .clone();
            let tasks = h.tasks(s.id).await?;
            let live = h.live_states().await?;
            let (state, reason) = live
                .get(&s.id.0)
                .cloned()
                .unwrap_or((s.state, s.state_reason.clone()));
            let mut rows = vec![vec![
                Some("sink".into()),
                Some(state.as_str().into()),
                None,
                None,
                None,
                None,
                reason,
            ]];
            for t in tasks {
                rows.push(vec![
                    Some(t.task_no.0.to_string()),
                    Some(t.state.as_str().into()),
                    Some(t.epoch.0.to_string()),
                    t.worker_id,
                    Some(t.delivered.to_string()),
                    Some(t.source_part.to_string()),
                    t.last_error,
                ]);
            }
            Ok(Handled::Rows {
                columns: [
                    "Task",
                    "State",
                    "Epoch",
                    "Worker",
                    "Delivered",
                    "Reads",
                    "Last error",
                ]
                .map(String::from)
                .to_vec(),
                rows,
            })
        }
    }
}

/// Target options known to be numbers.
fn no_env() -> sprig_common::SprigError {
    err(
        SprigCode::SinkOptionInvalid,
        "this gateway can't run TEST CONNECTION or TEST SINK (it has no sink environment)",
    )
}

/// `TEST [PLATFORM] CONNECTION` (R9, r9tests; [`crate::probe`]): the
/// rights to name the connection in a sink, then each step's result.
async fn test_connection(h: &mut dyn Host, c: &ConnName, db: Option<&str>) -> Result<Handled> {
    let (db_id, db_name) = db_of(h, db)?;
    let (r, _) = resolve_conn(h, db_id, c).await?;
    let env = h.sink_env().ok_or_else(no_env)?;
    let steps = crate::probe::test_connection(env.as_ref(), h.tenant(), &r, db_id, &db_name).await?;
    metrics::counter!("sprig_sink_connection_tests_total",
        "result" => if steps.iter().all(|s| s.ok) { "ok" } else { "failed" })
    .increment(1);
    Ok(Handled::Rows {
        columns: ["Step", "Result", "Detail", "Elapsed ms"]
            .map(String::from)
            .to_vec(),
        rows: steps
            .into_iter()
            .map(|s| {
                vec![
                    Some(s.step),
                    Some(if s.ok { "ok" } else { "failed" }.to_owned()),
                    Some(s.detail),
                    Some(s.elapsed_ms.to_string()),
                ]
            })
            .collect(),
    })
}

/// `TEST SINK` (R9, r9tests; [`crate::probe`]): `SPRIG_SINK_ADMIN` or the
/// sink's ownership; a stream source also needs `SELECT` on it (the sample
/// is read for the caller), a queue is browsed with the caller's rights.
async fn test_sink(h: &mut dyn Host, name: &grammar::QName, sample: u32) -> Result<Handled> {
    let (db, db_name) = db_of(h, name.db.as_deref())?;
    let def = h
        .state()
        .sink(db, &name.name)
        .ok_or_else(|| err(SprigCode::SinkNotFound, format!("no sink {}", name.name)))?
        .clone();
    let me = h.user_id().unwrap_or(u32::MAX);
    if !(h.sink_admin() || def.owner_user_id == me) {
        need_admin(h)?;
    }
    let env = h.sink_env().ok_or_else(no_env)?;
    let n = sample as usize;
    let records = match &def.source {
        SourceSpec::Stream {
            id,
            name: s,
            partitions,
        } => {
            if !h.can_select(&db_name, s) {
                return Err(err(
                    SprigCode::SinkPrivilege,
                    format!("TEST SINK reads stream {s} for you: you need SELECT on it"),
                ));
            }
            crate::probe::sample_stream(env.as_ref(), h.tenant(), *id, *partitions, n).await?
        }
        SourceSpec::Queue { name: q, .. } => h.sample_queue(&db_name, q, n).await?,
        other => {
            return Err(err(
                SprigCode::SinkOptionInvalid,
                format!(
                    "TEST SINK samples stream and queue sources; this sink reads {}",
                    other.kind()
                ),
            ));
        }
    };
    let rows = crate::probe::dry_run(env.as_ref(), h.tenant(), &def, records).await?;
    metrics::counter!("sprig_sink_dry_runs_total").increment(1);
    Ok(Handled::Rows {
        columns: ["Position", "Outcome", "Key", "Value", "Headers", "Detail"]
            .map(String::from)
            .to_vec(),
        rows: rows
            .into_iter()
            .map(|r| {
                vec![
                    Some(r.position),
                    Some(r.outcome.to_owned()),
                    r.key,
                    r.value,
                    Some(r.headers),
                    r.detail,
                ]
            })
            .collect(),
    })
}

fn check_option(k: &str, v: &str) -> Result<()> {
    if crate::settings::u64_key(k).is_some() && v.trim().parse::<u64>().is_err() {
        return Err(bad_option(format!("{k} must be a whole number, not '{v}'")));
    }
    Ok(())
}

async fn publish_index(h: &mut dyn Host, def: &SinkDef) -> Result<()> {
    let per_task = h
        .setting("partitions_per_task", def.db_id)
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let per_task = def
        .options
        .get("partitions_per_task")
        .and_then(|v| v.parse().ok())
        .unwrap_or(per_task);
    let ix = SinkIndex {
        tenant: h.tenant().0,
        sink: def.id.0,
        tasks: crate::job::task_count(def, per_task),
        definition_ver: def.definition_ver,
    };
    h.index(def.id, Some(ix)).await
}

/// Resolves a connection a sink names, checking `USAGE`.
async fn resolve_conn(h: &mut dyn Host, db: u32, c: &ConnName) -> Result<(ConnRef, ConnKind)> {
    let me = h.user_id().unwrap_or(u32::MAX);
    let st = h.state();
    if c.platform {
        let Some((p, granted)) = h.platform(&c.name).await? else {
            return Err(err(
                SprigCode::SinkNotFound,
                format!("no platform connection {}", c.name),
            ));
        };
        if !granted {
            return Err(err(
                SprigCode::SinkPlatformConnectionNotGranted,
                format!(
                    "platform connection {} isn't granted to this tenant",
                    c.name
                ),
            ));
        }
        if !(h.tenant_admin() || st.has_usage(0, &c.name, true, me)) {
            return Err(err(
                SprigCode::SinkPrivilege,
                format!("you need USAGE on platform connection {}", c.name),
            ));
        }
        return Ok((
            ConnRef::Platform {
                name: c.name.clone(),
            },
            p.kind,
        ));
    }
    let def = st
        .connection(db, &c.name)
        .ok_or_else(|| err(SprigCode::SinkNotFound, format!("no connection {}", c.name)))?;
    if !(h.tenant_admin() || def.owner_user_id == me || st.has_usage(db, &c.name, false, me)) {
        return Err(err(
            SprigCode::SinkPrivilege,
            format!("you need USAGE on connection {}", c.name),
        ));
    }
    Ok((
        ConnRef::Tenant {
            id: def.id,
            name: def.name.clone(),
        },
        def.kind,
    ))
}

#[allow(clippy::too_many_lines)]
async fn create_sink(h: &mut dyn Host, c: grammar::CreateSink) -> Result<Handled> {
    need_admin(h)?;
    let (db, dbname) = db_of(h, c.name.db.as_deref())?;
    if h.state().sink(db, &c.name.name).is_some() {
        if c.if_not_exists {
            return Ok(Handled::Ok { affected: 0 });
        }
        return Err(err(
            SprigCode::SinkExists,
            format!("sink {} exists", c.name.name),
        ));
    }
    let max: usize = h
        .setting("max_per_tenant", db)
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    if h.state().sinks.len() >= max {
        return Err(err(
            SprigCode::SinkQuota,
            format!("the tenant has {max} sinks (sink.max_per_tenant)"),
        ));
    }
    // The source, readable by the owner.
    let source = match &c.source {
        grammar::SourceAst::Stream(q) => {
            let (sdb, sdbname) = db_of(h, q.db.as_deref().or(Some(&dbname)))?;
            let (id, partitions) = h.stream(sdb, &q.name).ok_or_else(|| {
                sprig_common::SprigError::new(
                    SprigCode::StreamNotFound,
                    format!("unknown stream {}", q.name),
                )
            })?;
            if !h.can_select(&sdbname, &q.name) {
                return Err(err(
                    SprigCode::SinkPrivilege,
                    format!("you need SELECT on stream {}", q.name),
                ));
            }
            SourceSpec::Stream {
                id,
                name: q.name.clone(),
                partitions,
            }
        }
        grammar::SourceAst::FanIn { stream: q, under } => {
            let sdbname = q.db.clone().unwrap_or_else(|| dbname.clone());
            if !h.fan_in() {
                return Err(err(
                    SprigCode::SinkPrivilege,
                    "Access denied; you need (at least one of) the SPRIG_FAN_IN privilege(s) for a fan-in sink",
                ));
            }
            if !h.can_select(&sdbname, &q.name) {
                return Err(err(
                    SprigCode::SinkPrivilege,
                    format!("you need SELECT on stream {}", q.name),
                ));
            }
            let d = h
                .fan_in_stream(&sdbname, &q.name, under.as_deref())
                .await?
                .ok_or_else(|| {
                    sprig_common::SprigError::new(
                        SprigCode::StreamNotFound,
                        format!(
                            "no FOR EACH TENANT stream {sdbname}.{} in reach for FAN IN",
                            q.name
                        ),
                    )
                })?;
            if c.guarantee == Guarantee::ExactlyOnce {
                return Err(bad_option(
                    "EXACTLY ONCE isn't built for fan-in sinks yet: they deliver at least once, each record with its tenant-prefixed idempotency key",
                ));
            }
            SourceSpec::FanIn {
                def_tenant: d.def_tenant,
                def_object: d.def_object,
                db: sdbname,
                name: q.name.clone(),
                partitions: d.partitions,
                under: d.under,
            }
        }
        grammar::SourceAst::Queue(q) => {
            // A durable queue of the database's vhost (chapter 50), which
            // the sink consumes: SELECT and DELETE on it (R9, r9queue).
            let (qdb, qdbname) = db_of(h, q.db.as_deref().or(Some(&dbname)))?;
            let id = h.queue(qdb, &q.name).ok_or_else(|| {
                err(
                    SprigCode::SinkOptionInvalid,
                    format!("no durable queue {} in vhost {qdbname}", q.name),
                )
            })?;
            if !h.can_select(&qdbname, &q.name) || !h.can_delete(&qdbname, &q.name) {
                return Err(err(
                    SprigCode::SinkPrivilege,
                    format!(
                        "you need SELECT and DELETE on queue {} (a sink consumes it)",
                        q.name
                    ),
                ));
            }
            if qdb != db {
                return Err(bad_option(
                    "a queue sink's queue must be in the sink's database (its vhost)",
                ));
            }
            SourceSpec::Queue {
                id,
                name: q.name.clone(),
            }
        }
        grammar::SourceAst::FanInQueue { queue: q, under } => {
            let qdbname = q.db.clone().unwrap_or_else(|| dbname.clone());
            if !h.fan_in() {
                return Err(err(
                    SprigCode::SinkPrivilege,
                    "Access denied; you need (at least one of) the SPRIG_FAN_IN privilege(s) for a fan-in sink",
                ));
            }
            if !h.can_select(&qdbname, &q.name) || !h.can_delete(&qdbname, &q.name) {
                return Err(err(
                    SprigCode::SinkPrivilege,
                    format!(
                        "you need SELECT and DELETE on queue {} (a sink consumes it)",
                        q.name
                    ),
                ));
            }
            let d = h
                .fan_in_queue(&qdbname, &q.name, under.as_deref())
                .await?
                .ok_or_else(|| {
                    err(
                        SprigCode::SinkOptionInvalid,
                        format!(
                            "no FOR EACH TENANT queue {qdbname}.{} in reach for FAN IN",
                            q.name
                        ),
                    )
                })?;
            if c.guarantee == Guarantee::ExactlyOnce {
                return Err(bad_option(
                    "EXACTLY ONCE isn't built for fan-in sinks yet: they deliver at least once, each record with its tenant-prefixed idempotency key",
                ));
            }
            SourceSpec::FanInQueue {
                def_tenant: d.def_tenant,
                def_object: d.def_object,
                db: qdbname,
                name: q.name.clone(),
                under: d.under,
            }
        }
        grammar::SourceAst::Changes(ts) => {
            // Read through each table's change topic (chapter 45), which
            // must be on.
            let mut objects = Vec::new();
            for t in ts {
                let (tdb, tdbname) = db_of(h, t.db.as_deref().or(Some(&dbname)))?;
                let id = h.table(tdb, &t.name).ok_or_else(|| {
                    sprig_common::SprigError::standard(
                        1146,
                        *b"42S02",
                        format!("Table '{tdbname}.{}' doesn't exist", t.name),
                    )
                })?;
                if !h.can_select(&tdbname, &t.name) {
                    return Err(err(
                        SprigCode::SinkPrivilege,
                        format!("you need SELECT on table {}", t.name),
                    ));
                }
                if h.change_stream(tdb, id).is_none() {
                    return Err(bad_option(format!(
                        "CHANGES OF {} reads the table's change topic, which is off: ALTER TABLE {} SPRIG SET (stream.change_topic = on)",
                        t.name, t.name
                    )));
                }
                objects.push((id, t.name.clone()));
            }
            if c.guarantee == Guarantee::ExactlyOnce && objects.len() > 1 {
                return Err(bad_option(
                    "EXACTLY ONCE with CHANGES OF more than one table isn't built yet",
                ));
            }
            SourceSpec::Changes { objects }
        }
    };
    // TRANSFORM f: a live JavaScript, KIRun or Lua function of the sink's
    // database whose text uses no data or outside calls (R9, r9more).
    // R9 (r9fwd): `transform_version = n` pins a version, and `EXACTLY
    // ONCE` needs the version declared `DETERMINISTIC`.
    if let Some(f) = &c.transform {
        let pinned = grammar::options_map(&c.options)
            .get("transform_version")
            .map(|v| {
                v.trim().parse::<u64>().map_err(|_| {
                    bad_option(format!(
                        "transform_version must be a whole number, not '{v}'"
                    ))
                })
            })
            .transpose()?
            .filter(|v| *v > 0);
        match h.function_version(db, f, pinned).await? {
            None => {
                return Err(err(
                    SprigCode::SinkTransformInvalid,
                    match pinned {
                        Some(v) => format!(
                            "TRANSFORM {f}: no version {v} of function {f} in database {dbname}"
                        ),
                        None => format!("TRANSFORM {f}: no function {f} in database {dbname}"),
                    },
                ));
            }
            Some((runtime, version, source, sig)) => {
                crate::transform::check(f, &runtime, &source)?;
                if c.guarantee == Guarantee::ExactlyOnce {
                    crate::transform::check_deterministic(f, version, &sig)?;
                }
            }
        }
    }
    // DEAD LETTER TO STREAM d: a stream of this tenant (its events go
    // there as well as to sink_dead_letters, which sprigctl replays from).
    let dead_letter = match &c.dead_letter {
        None => None,
        Some(grammar::DeadAst::Stream(n)) => {
            let (dl_db, dl_dbname, dl_name) = match n.split_once('.') {
                Some((d, s)) => {
                    let (dl_id, dl_nm) = db_of(h, Some(d))?;
                    (dl_id, dl_nm, s.to_owned())
                }
                None => {
                    let (dl_id, dl_nm) = db_of(h, Some(&dbname))?;
                    (dl_id, dl_nm, n.clone())
                }
            };
            let (stream_id, _) = h.stream(dl_db, &dl_name).ok_or_else(|| {
                sprig_common::SprigError::new(
                    SprigCode::StreamNotFound,
                    format!("unknown dead-letter stream {dl_dbname}.{dl_name}"),
                )
            })?;
            Some(crate::model::DeadLetter::Stream {
                id: stream_id,
                name: dl_name,
            })
        }
        Some(_) => {
            return Err(bad_option(
                "DEAD LETTER TO QUEUE and EXCHANGE aren't built yet; DEAD LETTER TO STREAM is",
            ));
        }
    };
    if let Some(f) = &c.filter {
        crate::filter::parse(f)?;
    }
    let options = grammar::options_map(&c.options);
    for (k, v) in &options {
        check_option(k, v)?;
    }
    // The target and its connections.
    let (target, connections) = match c.target {
        TargetAst::Webhook { conn, path } => {
            let (r, k) = resolve_conn(h, db, &conn).await?;
            (TargetSpec::Webhook { path }, vec![(r, k)])
        }
        TargetAst::Kafka { conn, topic, key } => {
            let (r, k) = resolve_conn(h, db, &conn).await?;
            (TargetSpec::Kafka { topic, key }, vec![(r, k)])
        }
        TargetAst::S3 { conn, prefix } => {
            let (r, k) = resolve_conn(h, db, &conn).await?;
            (TargetSpec::S3 { prefix }, vec![(r, k)])
        }
        TargetAst::Database { conn, table } => {
            let (r, k) = resolve_conn(h, db, &conn).await?;
            (TargetSpec::Database { table }, vec![(r, k)])
        }
        TargetAst::Script {
            language,
            source,
            function,
            using,
        } => {
            if function.is_some() {
                return Err(bad_option(
                    "TO SCRIPT FUNCTION isn't built yet; use TO SCRIPT LANGUAGE ... AS $$ ... $$",
                ));
            }
            let mut v = Vec::new();
            for u in &using {
                v.push(resolve_conn(h, db, u).await?);
            }
            (
                TargetSpec::Script {
                    language: language.unwrap_or(ScriptLanguage::Javascript),
                    source,
                    function,
                },
                v,
            )
        }
    };
    let kind = target.kind();
    crate::policy::check_enabled(
        h.setting("enabled", db)
            .is_none_or(|v| crate::target::truthy(&v)),
        &h.setting("allowed_kinds", db)
            .unwrap_or_else(|| "webhook,kafka,s3,database,script".into()),
        kind,
    )?;
    if kind != TargetKind::Script
        && let Some((_, ck)) = connections.first()
        && !kind.accepts(*ck)
    {
        return Err(bad_option(format!(
            "a {kind} target can't use a {ck} connection"
        )));
    }
    // Exactly once only where the destination can give it.
    if c.guarantee == Guarantee::ExactlyOnce {
        let ok = match (source.kind(), kind) {
            (_, TargetKind::Webhook | TargetKind::Script) => false,
            (SourceKind::Queue, TargetKind::Kafka | TargetKind::S3) => false,
            _ => true,
        };
        if !ok {
            return Err(err(
                SprigCode::SinkExactlyOnceUnsupported,
                format!(
                    "EXACTLY ONCE isn't possible from a {} to a {kind} target",
                    source.kind()
                ),
            ));
        }
    }
    if kind == TargetKind::S3 {
        let fmt = options.get("format").map_or("jsonl", String::as_str);
        if !matches!(fmt, "jsonl" | "csv" | "parquet") {
            return Err(err(
                SprigCode::SinkFormatUnsupported,
                format!("S3 format {fmt}"),
            ));
        }
        if fmt != "jsonl"
            && !options.contains_key("columns")
            && source.kind() != SourceKind::Changes
        {
            return Err(err(
                SprigCode::SinkFormatUnsupported,
                format!("{fmt} from a stream or queue needs the columns option"),
            ));
        }
    }
    let id = h.next_id().await?;
    let mut opts: BTreeMap<String, String> = options;
    opts.insert("__db_name".into(), dbname.clone());
    let def = SinkDef {
        id: SinkId(id),
        db_id: db,
        name: c.name.name.clone(),
        source,
        filter_sql: c.filter,
        projection: c.projection,
        transform: c.transform.clone(),
        target,
        connections: connections.into_iter().map(|(r, _)| r).collect(),
        guarantee: c.guarantee,
        dead_letter,
        start_at: c.start_at,
        owner_user_id: h.user_id().unwrap_or(0),
        state: SinkState::Running,
        state_reason: None,
        definition_ver: 1,
        options: opts,
    };
    h.commit(Change::InsertSink(def.clone())).await?;
    publish_index(h, &def).await?;
    Ok(Handled::Ok { affected: 0 })
}

fn q(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// `SHOW CREATE CONNECTION`: credentials as `'***'` with their version and
/// who set them when.
#[must_use]
pub fn render_connection(c: &ConnectionDef, h: &dyn Host) -> String {
    let mut s = format!(
        "CREATE CONNECTION `{}` TYPE {}",
        c.name,
        c.kind.as_str().to_uppercase()
    );
    if !c.options.is_empty() {
        let o: Vec<String> = c
            .options
            .iter()
            .map(|(k, v)| format!("{k} = {}", q(v.as_str().unwrap_or(&v.to_string()))))
            .collect();
        s.push_str(&format!(" WITH ({})", o.join(", ")));
    }
    if !c.secrets.is_empty() {
        let o: Vec<String> = c
            .secrets
            .iter()
            .map(|x| {
                format!(
                    "{} = '***' /* version {}, set {} by {} */",
                    x.name,
                    x.version,
                    x.set_at,
                    h.user_name(x.set_by)
                        .unwrap_or_else(|| format!("user {}", x.set_by))
                )
            })
            .collect();
        s.push_str(&format!(" CREDENTIALS ({})", o.join(", ")));
    }
    s
}

/// `SHOW CREATE SINK`.
#[must_use]
pub fn render_sink(d: &SinkDef) -> String {
    let mut s = format!("CREATE SINK `{}` FROM ", d.name);
    match &d.source {
        SourceSpec::Stream { name, .. } => s.push_str(&format!("STREAM `{name}`")),
        SourceSpec::Queue { name, .. } => s.push_str(&format!("QUEUE `{name}`")),
        SourceSpec::Changes { objects } => s.push_str(&format!(
            "CHANGES OF {}",
            objects
                .iter()
                .map(|o| format!("`{}`", o.1))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        SourceSpec::FanIn {
            db, name, under, ..
        } => {
            s.push_str(&format!("STREAM `{db}`.`{name}` FAN IN"));
            if let Some(u) = under {
                s.push_str(&format!(" UNDER {u}"));
            }
        }
        SourceSpec::FanInQueue {
            db, name, under, ..
        } => {
            s.push_str(&format!("QUEUE `{db}`.`{name}` FAN IN"));
            if let Some(u) = under {
                s.push_str(&format!(" UNDER {u}"));
            }
        }
    }
    if let Some(f) = &d.filter_sql {
        s.push_str(&format!(" WHERE {f}"));
    }
    match &d.projection {
        Some(crate::model::Projection::Only { columns }) => {
            s.push_str(&format!(" COLUMNS ({})", columns.join(", ")));
        }
        Some(crate::model::Projection::Except { columns }) => {
            s.push_str(&format!(" EXCEPT COLUMNS ({})", columns.join(", ")));
        }
        None => {}
    }
    if let Some(f) = &d.transform {
        s.push_str(&format!(" TRANSFORM `{f}`"));
    }
    let conn = |r: Option<&ConnRef>| match r {
        Some(ConnRef::Platform { name }) => format!("PLATFORM CONNECTION `{name}`"),
        Some(ConnRef::Tenant { name, .. }) => format!("CONNECTION `{name}`"),
        None => String::new(),
    };
    match &d.target {
        TargetSpec::Webhook { path } => {
            s.push_str(&format!(" TO WEBHOOK {}", conn(d.connections.first())));
            if let Some(p) = path {
                s.push_str(&format!(" PATH {}", q(p)));
            }
        }
        TargetSpec::Kafka { topic, key } => {
            s.push_str(&format!(
                " TO KAFKA {} TOPIC {}",
                conn(d.connections.first()),
                q(topic)
            ));
            if let Some(k) = key {
                s.push_str(&format!(" KEY {}", q(k)));
            }
        }
        TargetSpec::S3 { prefix } => {
            s.push_str(&format!(
                " TO S3 {} PREFIX {}",
                conn(d.connections.first()),
                q(prefix)
            ));
        }
        TargetSpec::Database { table } => {
            s.push_str(&format!(
                " TO DATABASE {} TABLE {}",
                conn(d.connections.first()),
                q(table)
            ));
        }
        TargetSpec::Script {
            language, source, ..
        } => {
            s.push_str(&format!(" TO SCRIPT LANGUAGE {}", language.as_str()));
            if !d.connections.is_empty() {
                s.push_str(&format!(
                    " USING CONNECTIONS ({})",
                    d.connections
                        .iter()
                        .map(|c| format!("`{}`", c.name()))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            s.push_str(&format!(" AS $$ {} $$", source.as_deref().unwrap_or("")));
        }
    }
    s.push_str(match d.guarantee {
        Guarantee::AtLeastOnce => " GUARANTEE AT LEAST ONCE",
        Guarantee::ExactlyOnce => " GUARANTEE EXACTLY ONCE",
    });
    s.push_str(&match d.start_at {
        StartAt::Latest => " START AT LATEST".to_owned(),
        StartAt::Earliest => " START AT EARLIEST".to_owned(),
        StartAt::Snapshot => " START AT SNAPSHOT".to_owned(),
        StartAt::Timestamp { ms } => format!(" START AT TIMESTAMP '{ms}'"),
    });
    let o: Vec<String> = d
        .options
        .iter()
        .filter(|(k, _)| !k.starts_with("__"))
        .map(|(k, v)| format!("{k} = {}", q(v)))
        .collect();
    if !o.is_empty() {
        s.push_str(&format!(" WITH ({})", o.join(", ")));
    }
    s
}

/// Applies a change inside the caller's catalog transaction (the gateway
/// opens a versioned catalog change around it).
///
/// # Errors
/// The catalog's; 39216 when sealing fails.
pub async fn apply<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    change: &Change,
    keys: Option<&dyn SinkKeyring>,
    tenant: TenantId,
    user: u32,
) -> Result<()> {
    let store = SinksStore::new(schema);
    match change {
        Change::InsertConnection(def, secrets) => {
            store.insert_connection(q, def).await?;
            put_secrets(q, &store, keys, tenant, def.id, secrets, user).await?;
        }
        Change::UpdateConnection(def) => store.update_connection(q, def).await?,
        Change::Credentials(id, secrets) => {
            put_secrets(q, &store, keys, tenant, *id, secrets, user).await?;
        }
        Change::DropConnection(def) => store.delete_connection(q, def).await?,
        Change::InsertSink(def) => store.insert_sink(q, def).await?,
        Change::UpdateSink(def) => store.update_sink(q, def).await?,
        Change::ResetSink(def) => {
            store.update_sink(q, def).await?;
            sprig_streams::StreamsStore::new(schema)
                .delete_group(q, &def.group())
                .await?;
        }
        Change::State(id, s, r) => store.set_state(q, *id, *s, r.as_deref()).await?,
        Change::DropSink(def) => {
            store.delete_sink(q, def.id).await?;
            let streams = sprig_streams::StreamsStore::new(schema);
            // A tenant whose catalog never held a stream (a fan-in sink's
            // declarer whose task never ran) has no group tables.
            if streams.migrated(q).await.unwrap_or(true) {
                streams.delete_group(q, &def.group()).await?;
            }
        }
        Change::Usage(u, grant) => store.set_usage(q, u, *grant, user).await?,
    }
    Ok(())
}

async fn put_secrets<Q: Queryable + Send>(
    q: &mut Q,
    store: &SinksStore,
    keys: Option<&dyn SinkKeyring>,
    tenant: TenantId,
    id: ConnectionId,
    secrets: &[(String, Zeroizing<String>)],
    user: u32,
) -> Result<()> {
    if secrets.is_empty() {
        return Ok(());
    }
    let keys = keys.ok_or_else(|| {
        err(
            SprigCode::SinkCredentialsInvalid,
            "no sink credential key on this gateway",
        )
    })?;
    for (name, value) in secrets {
        let version = store.next_secret_version(q, id, name).await?;
        let (kv, sealed) = crate::secrets::seal(keys, tenant, id.0, name, version, value)?;
        store
            .put_secret(q, id, name, version, kv, &sealed, user)
            .await?;
    }
    Ok(())
}
