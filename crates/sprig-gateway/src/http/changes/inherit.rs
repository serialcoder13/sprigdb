//! Change streams over inheritable tables: a subscriber sees the records of
//! its own space's merged view, never the layer rows behind them.
//!
//! ```text
//! map(event of table t, subscriber in space s of its tenant):
//!   a layer row of space s' (the key carries _sprig_space):
//!     s' not s or an ancestor of s                → nothing (another space's)
//!   a parent tenant's layer row (its feed is tailed from the stream's
//!     start, along the edges into this tenant): only its root space's
//!     rows, of a table this tenant inherits, as from above the root
//!   a copy row (__sprig_ih_<object>, a materialized edge into the tenant)
//!     → an event of the object
//!   read the record in s now, through the subscriber's session (its space,
//!     privileges and row-level security; path columns hidden), with
//!     SPRIG_ORIGIN()
//!   present:
//!     an update at an ancestor whose changed fields are all overridden
//!       nearer to s, or that changed no visible field  → nothing
//!     an insert whose fields come from s' or below    → insert
//!     else                                            → update (after: the
//!                                                       merged record)
//!   absent: the record was visible to the subscriber just before the
//!     change → delete; else nothing (decision RL64)
//! ```
//!
//! "Visible before" is what the stream last knew of the key ([`Seen`]:
//! the record read after its last event, while the tenant's catalog
//! version holds), else, for a key the stream hasn't seen change, a probe
//! in the subscriber's session (`exec::inherit_probe`): its merged view
//! with the changed row as it was before, filtered by its policies. A
//! `delete` thus never reveals a key the subscriber couldn't see: one a
//! nearer space hid or overrode out of its policies, one outside a
//! share's filter, one its policies never showed.
//!
//! The emitted record is the merged one read at map time, by design:
//! events of one key in one batch carry its state at the end of the batch.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use serde_json::{Map, Value};
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::Result;
use sprig_common::ids::{ObjectId, TenantId};
use sprig_feed::{
    ChangeEvent, Clusters, FeedItem, Op, PlacementView, StartAt, TailOptions, TenantTail,
};
use sprig_inherit::{InheritState, SpaceId};
use tokio::sync::mpsc;

use super::super::Core;
use super::super::sql::{Caller, SqlCall, collect};

/// The layer and copy columns an event's key carries.
const LAYER_KEYS: [&str; 2] = ["_sprig_space", "_sprig_share"];

/// What an event of an inheritable object is to the subscriber.
#[derive(Debug, Clone)]
pub(super) struct Mapped {
    /// The record's key, without the layer columns.
    pub key: Map<String, Value>,
    /// The space whose layer changed; `None` for a copy (a level above the
    /// tenant's root).
    pub space: Option<SpaceId>,
    /// A copy row's `_sprig_share`.
    pub share: Option<Value>,
}

/// How an event is handled.
pub(super) enum Class {
    /// Not an inheritable object's: as before.
    Plain,
    /// Another space's change, or a copy of an object the tenant lost.
    Drop,
    /// The subscriber's view may have changed.
    Inherit(Mapped),
}

/// The subscriber's tenant and space, for one batch.
pub(super) struct Mapper {
    cat: std::sync::Arc<TenantCatalog>,
    space: SpaceId,
    tenant_name: String,
}

impl Mapper {
    /// The mapper of `caller`, when its tenant has inheritable objects.
    pub(super) async fn load(core: &Core, caller: &Caller) -> Option<Self> {
        let svc = core.engine.tenants.clone()?;
        let tenant = caller.principal.tenant?;
        let cat = svc
            .cache
            .get(tenant, Freshness::Cached { min_version: 0 })
            .await
            .ok()?;
        let st = cat.ext.get::<InheritState>(sprig_inherit::MODULE)?;
        st.objects().next()?;
        let space = caller
            .principal
            .user_id
            .map_or(sprig_inherit::ROOT_SPACE, |u| st.home(u.0));
        let tenant_name = caller
            .principal
            .tenant_name
            .clone()
            .unwrap_or_else(|| tenant.to_string());
        Some(Self {
            cat,
            space,
            tenant_name,
        })
    }

    fn state(&self) -> Option<&InheritState> {
        self.cat.ext.get::<InheritState>(sprig_inherit::MODULE)
    }

    /// Classifies `e` (`own`: from this tenant's feed, else a parent
    /// tenant's); a copy row's event is renamed to its object's table.
    pub(super) fn classify(&self, e: &mut ChangeEvent, own: bool) -> Class {
        let Some(st) = self.state() else {
            return if own { Class::Plain } else { Class::Drop };
        };
        if !own {
            return self.from_above(st, e);
        }
        let Some(db) = self
            .cat
            .database(&sprig_catalog::ddl::database_key(&e.db_name, false))
        else {
            return Class::Plain;
        };
        let copy = e
            .table
            .strip_prefix("__sprig_ih_")
            .and_then(|n| n.parse::<u64>().ok());
        let obj = match copy {
            Some(id) => match st.object(ObjectId(id)) {
                Some(o) if o.db == db.db_id => o,
                _ => return Class::Drop,
            },
            None => match st.object_named_ci(db.db_id, &e.table) {
                Some(o) => o,
                None => return Class::Plain,
            },
        };
        if matches!(e.op, Op::Ddl(_) | Op::Truncate) {
            return if copy.is_some() {
                Class::Drop
            } else {
                Class::Plain
            };
        }
        let mut key = Map::new();
        let mut space = None;
        let mut share = None;
        for (n, v) in &e.key.0 {
            if n == "_sprig_share" {
                if let Value::Object(m) = sprig_streams::envelope::image_json(&sprig_feed::Image(
                    vec![(n.clone(), v.clone())],
                )) {
                    share = m.get(n).cloned();
                }
                continue;
            }
            if n == "_sprig_space" {
                space = match sprig_streams::envelope::image_json(&sprig_feed::Image(vec![(
                    n.clone(),
                    v.clone(),
                )])) {
                    Value::Object(m) => m.get(n).and_then(Value::as_u64).map(SpaceId),
                    _ => None,
                };
                continue;
            }
            if LAYER_KEYS.contains(&n.as_str()) {
                continue;
            }
            if let Value::Object(m) =
                sprig_streams::envelope::image_json(&sprig_feed::Image(vec![(
                    n.clone(),
                    v.clone(),
                )]))
            {
                key.extend(m);
            }
        }
        if copy.is_none() {
            let Some(s) = space else {
                return Class::Plain;
            };
            // Another branch's change, or a space below the subscriber's.
            if !st.is_ancestor_or_self(s, self.space) {
                return Class::Drop;
            }
        }
        e.table.clone_from(&obj.table);
        Class::Inherit(Mapped {
            key,
            space,
            share: copy.and(share),
        })
    }

    /// A parent tenant's event: its root space's layer rows of a table this
    /// tenant inherits.
    #[allow(clippy::wrong_self_convention)] // "from" the parent tenants
    fn from_above(&self, st: &InheritState, e: &ChangeEvent) -> Class {
        if matches!(e.op, Op::Ddl(_) | Op::Truncate) || e.table.starts_with("__sprig") {
            return Class::Drop;
        }
        let Some(db) = self
            .cat
            .database(&sprig_catalog::ddl::database_key(&e.db_name, false))
        else {
            return Class::Drop;
        };
        if st.object_named_ci(db.db_id, &e.table).is_none() {
            return Class::Drop;
        }
        let mut key = Map::new();
        for (n, v) in &e.key.0 {
            let Value::Object(m) = sprig_streams::envelope::image_json(&sprig_feed::Image(vec![(
                n.clone(),
                v.clone(),
            )])) else {
                continue;
            };
            if n == "_sprig_space" {
                if m.get(n).and_then(Value::as_u64) != Some(sprig_inherit::ROOT_SPACE.0) {
                    return Class::Drop;
                }
                continue;
            }
            if !LAYER_KEYS.contains(&n.as_str()) {
                key.extend(m);
            }
        }
        Class::Inherit(Mapped {
            key,
            space: None,
            share: None,
        })
    }

    /// The tenant catalog's version (policies and grants change it).
    pub(super) fn version(&self) -> u64 {
        self.cat.version
    }

    /// The names of the spaces strictly above `s` in the tenant, and the
    /// tenant's ancestors' names (origins a field inherited from).
    fn above(&self, s: Option<SpaceId>) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = self.cat.ancestry.iter().map(|a| a.name.clone()).collect();
        let (Some(st), Some(s)) = (self.state(), s) else {
            return out;
        };
        for x in st.path(s) {
            if x == s {
                break;
            }
            if let Some(sp) = st.space(x, &self.tenant_name) {
                out.insert(sp.name);
            }
        }
        out
    }

    /// The names of the spaces strictly below `s` down to the subscriber's
    /// (overrides nearer to the subscriber).
    fn nearer(&self, s: Option<SpaceId>) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let Some(st) = self.state() else {
            return out;
        };
        let path = st.path(self.space);
        let from = s.and_then(|s| path.iter().position(|x| *x == s));
        for x in path.iter().skip(from.map_or(0, |i| i + 1)) {
            if let Some(sp) = st.space(*x, &self.tenant_name) {
                out.insert(sp.name);
            }
        }
        out
    }
}

/// A merged record: its columns as JSON, and each field's origin.
pub(super) type Record = (Map<String, Value>, Map<String, Value>);

fn sql_lit(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_owned(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''")),
        other => format!(
            "'{}'",
            other.to_string().replace('\\', "\\\\").replace('\'', "''")
        ),
    }
}

/// A key's identity in the maps below.
pub(super) fn key_id(table: &str, key: &Map<String, Value>) -> String {
    format!(
        "{}\u{1}{}",
        table.to_lowercase(),
        Value::Object(key.clone())
    )
}

fn numeric(mysql_type: u32) -> bool {
    // DECIMAL, TINY, SHORT, LONG, FLOAT, DOUBLE, LONGLONG, INT24, YEAR,
    // NEWDECIMAL.
    matches!(mysql_type, 0..=5 | 8 | 9 | 13 | 246)
}

/// Reads the merged records of `keys` (table, key) in the subscriber's
/// space, through its own session.
///
/// # Errors
/// The statement's (a table the subscriber can't read: 1142).
pub(super) async fn read(
    core: &Core,
    caller: &Caller,
    db: &str,
    keys: &[(String, Map<String, Value>)],
) -> Result<HashMap<String, Record>> {
    let mut by_table: HashMap<String, Vec<&Map<String, Value>>> = HashMap::new();
    for (t, k) in keys {
        by_table.entry(t.clone()).or_default().push(k);
    }
    let mut out = HashMap::new();
    for (table, ks) in by_table {
        let Some(first) = ks.first() else {
            continue;
        };
        let cols: Vec<String> = first.keys().cloned().collect();
        if cols.is_empty() {
            continue;
        }
        let lhs = if cols.len() == 1 {
            sprig_common::names::quote(&cols[0])
        } else {
            format!(
                "({})",
                cols.iter()
                    .map(|c| sprig_common::names::quote(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let tuples: Vec<String> = ks
            .iter()
            .map(|k| {
                let vals: Vec<String> = cols
                    .iter()
                    .map(|c| sql_lit(k.get(c).unwrap_or(&Value::Null)))
                    .collect();
                if vals.len() == 1 {
                    vals[0].clone()
                } else {
                    format!("({})", vals.join(", "))
                }
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let sql = format!(
            "SELECT *, SPRIG_ORIGIN() FROM {} WHERE {lhs} IN ({})",
            sprig_common::names::quote(&table),
            tuples.join(", ")
        );
        let (tx, rx) = mpsc::channel(4);
        let engine = std::sync::Arc::clone(&core.engine);
        let call = SqlCall {
            database: db.to_owned(),
            sql,
            multi: false,
        };
        let c = caller.clone();
        let task = tokio::spawn(async move { engine.run(&c, call, None, tx).await });
        let resp = collect(rx, 64 << 20, None).await;
        let _ = task.await;
        let resp = resp?;
        let Some(res) = resp.results.first() else {
            continue;
        };
        let n = res.columns.len();
        for row in &res.rows {
            let mut rec = Map::new();
            let mut origins = Map::new();
            for (i, (c, v)) in res.columns.iter().zip(&row.values).enumerate() {
                let text = sprig_api::results::cell_bytes(v, false)
                    .map(|b| String::from_utf8_lossy(&b).into_owned());
                if i + 1 == n {
                    if let Some(Value::Object(m)) =
                        text.as_deref().and_then(|t| serde_json::from_str(t).ok())
                    {
                        origins = m;
                    }
                    continue;
                }
                let j = match text {
                    None => Value::Null,
                    Some(t) if numeric(c.mysql_type) => serde_json::from_str(&t)
                        .ok()
                        .filter(Value::is_number)
                        .unwrap_or(Value::String(t)),
                    Some(t) => Value::String(t),
                };
                rec.insert(c.name.clone(), j);
            }
            let key: Map<String, Value> = cols
                .iter()
                .filter_map(|c| rec.get(c).map(|v| (c.clone(), v.clone())))
                .collect();
            out.insert(key_id(&table, &key), (rec, origins));
        }
    }
    Ok(out)
}

/// The event an inheritable object's change is to the subscriber, given
/// its merged record now (`None`: not in its view) and whether the record
/// was visible to it just before the change. `None`: nothing to send.
pub(super) fn decide(
    mapper: &Mapper,
    e: &ChangeEvent,
    m: &Mapped,
    rec: Option<&Record>,
    was_visible: bool,
    full: bool,
    out: &mut super::WatchEvent,
) -> bool {
    out.key = Value::Object(m.key.clone()).to_string();
    let Some((row, origins)) = rec else {
        if was_visible {
            out.op = "delete".into();
            return true;
        }
        return false;
    };
    let origin = |f: &str| {
        origins
            .get(f)
            .and_then(Value::as_str)
            .map(|o| o.trim_end_matches(" (orphaned)").to_owned())
    };
    let visible: Vec<String> = e
        .delta
        .as_ref()
        .map(|d| d.iter().filter(|c| row.contains_key(*c)).cloned().collect())
        .unwrap_or_default();
    match e.op {
        Op::Update => {
            if visible.is_empty() {
                // Paths, bits or a hide's state: nothing the subscriber
                // sees changed.
                return false;
            }
            if m.space != Some(mapper.space) {
                let nearer = mapper.nearer(m.space);
                if visible
                    .iter()
                    .all(|f| origin(f).is_some_and(|o| nearer.contains(&o)))
                {
                    return false;
                }
            }
            out.op = "update".into();
            let changed: Map<String, Value> = row
                .iter()
                .filter(|(k, _)| visible.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            out.update = Value::Object(changed).to_string();
            if full {
                out.after = Value::Object(row.clone()).to_string();
            }
        }
        Op::Insert => {
            let above = mapper.above(m.space);
            let inherited = m.space.is_some()
                && row
                    .keys()
                    .any(|f| origin(f).is_some_and(|o| above.contains(&o)));
            if inherited {
                out.op = "update".into();
                out.update = Value::Object(row.clone()).to_string();
                if full {
                    out.after = Value::Object(row.clone()).to_string();
                }
            } else {
                out.op = "insert".into();
                out.after = Value::Object(row.clone()).to_string();
            }
        }
        // An override, hide or own row gone while the record stays: its
        // values may have changed.
        Op::Delete => {
            out.op = "update".into();
            out.update = Value::Object(row.clone()).to_string();
            if full {
                out.after = Value::Object(row.clone()).to_string();
            }
        }
        Op::Ddl(_) | Op::Truncate => return false,
    }
    true
}

/// The most keys [`Seen`] holds; past it, it starts again (probes cover
/// the keys it forgot).
const SEEN_MAX: usize = 100_000;

/// What the stream last knew of each key's visibility (RL64): set from the
/// record read after each of the key's events, cleared when the tenant's
/// catalog version moves (a policy or grant may have changed what the
/// subscriber sees).
#[derive(Debug, Default)]
pub(super) struct Seen {
    version: u64,
    keys: HashMap<String, bool>,
}

impl Seen {
    /// Starts a batch read at catalog `version`.
    pub(super) fn at(&mut self, version: u64) {
        if version != self.version {
            self.keys.clear();
            self.version = version;
        }
    }

    /// The key's visibility after its last event, if the stream knows it.
    pub(super) fn get(&self, id: &str) -> Option<bool> {
        self.keys.get(id).copied()
    }

    /// Records the key's visibility after an event.
    pub(super) fn set(&mut self, id: String, visible: bool) {
        if self.keys.len() >= SEEN_MAX && !self.keys.contains_key(&id) {
            self.keys.clear();
        }
        self.keys.insert(id, visible);
    }
}

/// The probe of event `e` (mapped as `m`): its record before the change
/// ([`crate::exec::inherit_probe`]).
pub(super) fn probe_item(e: &ChangeEvent, m: &Mapped) -> crate::exec::inherit_probe::ProbeItem {
    let img = |i: Option<&sprig_feed::Image>| match i.map(sprig_streams::envelope::image_json) {
        Some(Value::Object(o)) => Some(o),
        _ => None,
    };
    let before = img(e.before.as_ref());
    let columns = if before.is_none() {
        img(e.after.as_ref())
            .map(|a| a.keys().cloned().collect())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    crate::exec::inherit_probe::ProbeItem {
        table: e.table.clone(),
        key: m.key.clone(),
        space: m.space.filter(|_| m.share.is_none()).map(|s| s.0),
        share: m.share.clone(),
        before,
        columns,
    }
}

/// Runs `req` in the subscriber's session: whether each probe's record was
/// visible to it before its change (`None`: its view doesn't read the
/// changed row).
///
/// # Errors
/// The statement's.
pub(super) async fn probe(
    core: &Core,
    caller: &Caller,
    req: &crate::exec::inherit_probe::ProbeRequest,
) -> Result<Vec<Option<bool>>> {
    let (tx, rx) = mpsc::channel(4);
    let engine = std::sync::Arc::clone(&core.engine);
    let call = SqlCall {
        database: req.db.clone(),
        sql: crate::exec::inherit_probe::statement(req),
        multi: false,
    };
    let c = caller.clone();
    let task = tokio::spawn(async move { engine.run(&c, call, None, tx).await });
    let resp = collect(rx, 64 << 20, None).await;
    let _ = task.await;
    let resp = resp?;
    let rows: Vec<Vec<Option<String>>> = resp
        .results
        .first()
        .map(|r| {
            r.rows
                .iter()
                .map(|row| {
                    row.values
                        .iter()
                        .map(|v| {
                            sprig_api::results::cell_bytes(v, false)
                                .map(|b| String::from_utf8_lossy(&b).into_owned())
                        })
                        .collect()
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(crate::exec::inherit_probe::answer(req, &rows))
}

/// Tails the feeds of `tenant`'s ancestors that its inheritable objects
/// read through (nearest first, while an edge continues), from now; their
/// items in one channel. `None` when there is none.
pub(super) async fn ancestor_tails(
    svc: &crate::tenant::TenantServices,
    tenant: TenantId,
    clusters: &Arc<dyn Clusters>,
    placement: &Arc<dyn PlacementView>,
    names: &Arc<dyn sprig_feed::names::Resolver>,
) -> Option<mpsc::Receiver<Result<FeedItem>>> {
    let cat = svc
        .cache
        .get(tenant, Freshness::Cached { min_version: 0 })
        .await
        .ok()?;
    let st = cat.ext.get::<InheritState>(sprig_inherit::MODULE)?;
    if st.objects().next().is_none() || cat.ancestry.is_empty() {
        return None;
    }
    let mut line: Vec<Arc<TenantCatalog>> = Vec::new();
    for a in cat.ancestry.iter() {
        line.push(
            svc.cache
                .get(a.id, Freshness::Cached { min_version: 0 })
                .await
                .ok()?,
        );
    }
    line.push(Arc::clone(&cat));
    let mut tenants = Vec::new();
    let mut j = line.len() - 1;
    while j >= 1 {
        let (parent, child) = (&line[j - 1], &line[j]);
        let Some(cs) = child.ext.get::<InheritState>(sprig_inherit::MODULE) else {
            break;
        };
        let edge = cs.objects().any(|o| {
            child.databases.get(o.db).is_some_and(|d| {
                matches!(
                    sprig_inherit::plan::tenant_edge(parent, child, &d.name, &o.table, false),
                    Ok(Some(_))
                )
            })
        });
        if !edge {
            break;
        }
        tenants.push(parent.tenant);
        j -= 1;
    }
    if tenants.is_empty() {
        return None;
    }
    let (tx, rx) = mpsc::channel(256);
    for t in tenants {
        let tail = TenantTail::start(
            t,
            StartAt::Now,
            Arc::clone(clusters),
            Arc::clone(placement),
            Arc::clone(names),
            TailOptions {
                progress_every: std::time::Duration::from_millis(500),
                ..TailOptions::default()
            },
        )
        .await;
        let mut tail = match tail {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(tenant = %t, error = %e, "a parent tenant's feed can't be tailed for a change stream");
                continue;
            }
        };
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                match tail.next().await {
                    Ok(i) => {
                        if tx.send(Ok(i)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(tenant = %t, error = %e, "a parent tenant's feed stopped for a change stream");
                        return;
                    }
                }
            }
        });
    }
    Some(rx)
}

/// The next item of the stream's own feed or of a parent tenant's
/// (`true`: its own).
pub(super) async fn next(
    tail: &mut super::OwnFeed,
    above: &mut Option<mpsc::Receiver<Result<FeedItem>>>,
) -> (bool, Result<FeedItem>) {
    match above {
        None => (true, tail.next().await),
        Some(rx) => tokio::select! {
            i = tail.next() => (true, i),
            Some(i) = rx.recv() => (false, i),
        },
    }
}
