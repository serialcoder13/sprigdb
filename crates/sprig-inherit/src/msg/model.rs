//! Definitions, overrides and instances of inherited messaging objects, and
//! the two rules every kind shares: membership (who a definition reaches)
//! and override resolution (nearest level wins).

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::cache::ExtState;
use sprig_common::ids::TenantId;

/// What a definition declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefKind {
    /// A stream (chapter 45).
    Stream,
    /// An exchange (chapter 50).
    Exchange,
    /// A queue (chapter 50).
    Queue,
    /// A binding between two inherited objects (chapter 50).
    Binding,
    /// A trigger on an inherited stream or queue (chapter 41).
    Trigger,
    /// A connection (chapter 51).
    Connection,
    /// A sink (chapter 51).
    Sink,
}

impl DefKind {
    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            DefKind::Stream => "stream",
            DefKind::Exchange => "exchange",
            DefKind::Queue => "queue",
            DefKind::Binding => "binding",
            DefKind::Trigger => "trigger",
            DefKind::Connection => "connection",
            DefKind::Sink => "sink",
        }
    }

    /// Parses a stored or written name (any case).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "stream" => DefKind::Stream,
            "exchange" => DefKind::Exchange,
            "queue" => DefKind::Queue,
            "binding" => DefKind::Binding,
            "trigger" => DefKind::Trigger,
            "connection" => DefKind::Connection,
            "sink" => DefKind::Sink,
            _ => return None,
        })
    }
}

/// `CHILDREN` or `DESCENDANTS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Depth {
    /// Every depth (the default).
    #[default]
    Descendants,
    /// Direct children only.
    Children,
}

/// The `FOR EACH TENANT` scope, as stored (`inherited_defs.scope`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Scope {
    /// `CHILDREN` or `DESCENDANTS`.
    #[serde(default)]
    pub depth: Depth,
    /// `UNDER t`: only that descendant's subtree (its id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub under: Option<u64>,
    /// `INCLUDING SELF`.
    #[serde(default, rename = "self")]
    pub including_self: bool,
}

impl Scope {
    /// Whether `t` is a member of a definition with this scope declared in
    /// `declarer`, where `ancestry` is `t`'s ancestors, root first, parent
    /// last (`t` itself excluded).
    #[must_use]
    pub fn reaches(&self, declarer: TenantId, t: TenantId, ancestry: &[TenantId]) -> bool {
        if t == declarer {
            return self.including_self;
        }
        let Some(pos) = ancestry.iter().position(|a| *a == declarer) else {
            return false;
        };
        if self.depth == Depth::Children && pos + 1 != ancestry.len() {
            return false;
        }
        match self.under {
            None => true,
            Some(u) => u == t.0 || ancestry[pos + 1..].iter().any(|a| a.0 == u),
        }
    }
}

/// A definition's lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefState {
    /// In use.
    Active,
    /// Dropped; instances drain until `drop_after`.
    Dropping,
    /// Gone (the row is kept until its instances are).
    Dropped,
}

impl DefState {
    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            DefState::Active => "active",
            DefState::Dropping => "dropping",
            DefState::Dropped => "dropped",
        }
    }

    /// Parses the stored name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "active" => DefState::Active,
            "dropping" => DefState::Dropping,
            "dropped" => DefState::Dropped,
            _ => return None,
        })
    }
}

/// An `inherited_defs` row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Definition {
    /// The declaring tenant (not stored: the catalog it was read from).
    pub declarer: TenantId,
    /// Its id, from the declarer's object counter.
    pub object_id: u64,
    /// The kind.
    pub kind: DefKind,
    /// The database name instances live in.
    pub db_name: String,
    /// The name.
    pub name: String,
    /// Who it reaches.
    pub scope: Scope,
    /// The declaration, kind-specific (see each kind's module).
    pub definition: Value,
    /// Run-as privileges (triggers, sinks): `[{"privileges": [...], "db":
    /// "...", "name": "..."}]`.
    pub grants: Option<Value>,
    /// +1 on every change.
    pub def_version: u64,
    /// Lifecycle.
    pub state: DefState,
    /// End of the drop grace, ms since the epoch.
    pub drop_after_ms: Option<u64>,
    /// The declaring user.
    pub created_by: u32,
    /// When it was declared, ms since the epoch.
    pub created_at_ms: u64,
    /// The catalog version of its last change.
    pub changed_version: u64,
}

impl Definition {
    /// `(declarer, object id)`: how members name it.
    #[must_use]
    pub const fn key(&self) -> DefKey {
        DefKey {
            tenant: self.declarer,
            object: self.object_id,
        }
    }
}

/// A definition's identity across the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DefKey {
    /// The declaring tenant.
    pub tenant: TenantId,
    /// Its object id there.
    pub object: u64,
}

impl std::fmt::Display for DefKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.tenant.0, self.object)
    }
}

/// What an override changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum What {
    /// Settings: a JSON object of keys.
    Settings,
    /// A trigger's body: `{"text": <the declaration>}`.
    Body,
    /// A connection's options.
    Options,
    /// A connection's credentials (references only).
    Credentials,
    /// `"disabled"` or `"enabled"`.
    State,
}

impl What {
    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            What::Settings => "settings",
            What::Body => "body",
            What::Options => "options",
            What::Credentials => "credentials",
            What::State => "state",
        }
    }

    /// Parses the stored name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "settings" => What::Settings,
            "body" => What::Body,
            "options" => What::Options,
            "credentials" => What::Credentials,
            "state" => What::State,
            _ => return None,
        })
    }
}

/// `self` or `subtree`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppliesTo {
    /// The target tenant alone.
    #[serde(rename = "self")]
    Own,
    /// The target and every descendant of it.
    Subtree,
}

impl AppliesTo {
    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            AppliesTo::Own => "self",
            AppliesTo::Subtree => "subtree",
        }
    }
}

/// An `inherited_overrides` row, with the tenant whose catalog holds it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Override {
    /// The tenant that made it (whose catalog holds it).
    pub made_in: TenantId,
    /// The definition.
    pub def: DefKey,
    /// The tenant it is for.
    pub target: TenantId,
    /// It alone, or its subtree.
    pub applies_to: AppliesTo,
    /// What it changes.
    pub what: What,
    /// The value.
    pub value: Value,
    /// Who set it.
    pub set_by: u32,
    /// When, ms since the epoch.
    pub set_at_ms: u64,
}

/// An instance's lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceState {
    /// Being created (its physical part may be missing).
    Creating,
    /// In use.
    Active,
    /// Its definition is being dropped, or the member left the scope.
    Dropping,
    /// Kept by the member as its own object.
    Detached,
}

impl InstanceState {
    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            InstanceState::Creating => "creating",
            InstanceState::Active => "active",
            InstanceState::Dropping => "dropping",
            InstanceState::Detached => "detached",
        }
    }

    /// Parses the stored name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "creating" => InstanceState::Creating,
            "active" => InstanceState::Active,
            "dropping" => InstanceState::Dropping,
            "detached" => InstanceState::Detached,
            _ => return None,
        })
    }
}

/// What created an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    /// A publish into it.
    Publish,
    /// A consumer.
    Consume,
    /// A declare (AMQP), or its source instance becoming active.
    Declare,
    /// `inherit.instance_precreate`.
    Precreate,
}

impl Cause {
    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Cause::Publish => "publish",
            Cause::Consume => "consume",
            Cause::Declare => "declare",
            Cause::Precreate => "precreate",
        }
    }

    /// Parses the stored name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "publish" => Cause::Publish,
            "consume" => Cause::Consume,
            "declare" => Cause::Declare,
            "precreate" => Cause::Precreate,
            _ => return None,
        })
    }
}

/// An `inherited_instances` row (in the member).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    /// The definition.
    pub def: DefKey,
    /// The instance's id in the member: a stream id, a queue or exchange
    /// object id, a trigger's binding id.
    pub instance_id: u64,
    /// The kind.
    pub kind: DefKind,
    /// The definition version it last ran.
    pub def_version: u64,
    /// A copy of that version's declaration.
    pub definition: Value,
    /// Lifecycle.
    pub state: InstanceState,
    /// What created it.
    pub cause: Cause,
    /// Kind-specific physical facts (`{"db": ..., "name": ...}`).
    pub physical: Option<Value>,
    /// When, ms since the epoch.
    pub created_at_ms: u64,
}

/// The module's rows in one tenant's snapshot.
#[derive(Debug, Clone, Default)]
pub struct MsgState {
    /// Whether the module's tables exist.
    pub migrated: bool,
    /// Definitions this tenant declared, by `(db name key, kind, name)`.
    pub defs: BTreeMap<(String, DefKind, String), Arc<Definition>>,
    /// Overrides made in this tenant.
    pub overrides: Vec<Arc<Override>>,
    /// This tenant's instances, by definition.
    pub instances: BTreeMap<DefKey, Arc<Instance>>,
}

impl ExtState for MsgState {
    fn bytes(&self) -> usize {
        self.defs
            .values()
            .map(|d| 256 + d.definition.to_string().len())
            .sum::<usize>()
            + self.overrides.len() * 160
            + self.instances.len() * 200
    }
}

/// The lookup form of a database name in this module's indexes: lower
/// case, whatever the tenants' `lower_case_table_names` (a definition is
/// read by members whose setting may differ from the declarer's).
#[must_use]
pub fn db_key(name: &str) -> String {
    name.to_lowercase()
}

impl MsgState {
    /// Whether the tenant has nothing of this module.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.defs.is_empty() && self.overrides.is_empty() && self.instances.is_empty()
    }

    /// The definition this tenant declared as `kind` `db.name`.
    #[must_use]
    pub fn def(&self, db: &str, kind: DefKind, name: &str) -> Option<&Arc<Definition>> {
        self.defs.get(&(db_key(db), kind, name.to_owned()))
    }

    /// The definition with object id `object`.
    #[must_use]
    pub fn def_by_id(&self, object: u64) -> Option<&Arc<Definition>> {
        self.defs.values().find(|d| d.object_id == object)
    }

    /// Definitions of `kind` in database `db`.
    pub fn defs_in<'a>(
        &'a self,
        db: &'a str,
        kind: DefKind,
    ) -> impl Iterator<Item = &'a Arc<Definition>> + 'a {
        let k = db_key(db);
        self.defs
            .iter()
            .filter(move |((d, kd, _), _)| *d == k && *kd == kind)
            .map(|(_, v)| v)
    }

    /// The instance whose id in this tenant is `id`, of `kind`.
    #[must_use]
    pub fn instance_by_id(&self, kind: DefKind, id: u64) -> Option<&Arc<Instance>> {
        self.instances
            .values()
            .find(|i| i.kind == kind && i.instance_id == id)
    }
}

/// One level of a member's chain: a tenant and its module state.
#[derive(Debug, Clone)]
pub struct Level {
    /// The tenant.
    pub tenant: TenantId,
    /// Its rows (`None`: the module isn't migrated there).
    pub state: Option<Arc<MsgState>>,
}

/// A member's view of the tree: itself, then its ancestors nearest first.
#[derive(Debug, Clone)]
pub struct Chain {
    /// `[T, parent, ..., root]`.
    pub levels: Vec<Level>,
}

impl Chain {
    /// The member.
    #[must_use]
    pub fn member(&self) -> TenantId {
        self.levels.first().map_or(TenantId(0), |l| l.tenant)
    }

    /// The member's ancestors root first, parent last.
    #[must_use]
    pub fn ancestry(&self) -> Vec<TenantId> {
        self.levels.iter().skip(1).rev().map(|l| l.tenant).collect()
    }

    /// The ancestors of the tenant at `index` in the chain, root first.
    fn ancestry_of(&self, index: usize) -> Vec<TenantId> {
        self.levels
            .iter()
            .skip(index + 1)
            .rev()
            .map(|l| l.tenant)
            .collect()
    }

    /// The nearest definition of `kind` `db.name` that reaches the member
    /// (its own declaration with `INCLUDING SELF` included).
    #[must_use]
    pub fn resolve(&self, db: &str, kind: DefKind, name: &str) -> Option<Arc<Definition>> {
        let t = self.member();
        let anc = self.ancestry();
        self.levels.iter().find_map(|l| {
            let s = l.state.as_ref()?;
            let d = s.def(db, kind, name)?;
            d.scope.reaches(l.tenant, t, &anc).then(|| Arc::clone(d))
        })
    }

    /// A definition declared at some level of the chain, by key.
    #[must_use]
    pub fn def(&self, key: DefKey) -> Option<Arc<Definition>> {
        self.levels
            .iter()
            .find(|l| l.tenant == key.tenant)
            .and_then(|l| l.state.as_ref())
            .and_then(|s| s.def_by_id(key.object))
            .cloned()
    }

    /// Every definition of `kind` that reaches the member, nearest
    /// declarer first, one per name (nearer shadows farther).
    #[must_use]
    pub fn reaching(&self, kind: DefKind) -> Vec<Arc<Definition>> {
        let t = self.member();
        let anc = self.ancestry();
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for l in &self.levels {
            let Some(s) = &l.state else { continue };
            for d in s.defs.values() {
                if d.kind != kind || !d.scope.reaches(l.tenant, t, &anc) {
                    continue;
                }
                if seen.insert((d.db_name.clone(), d.name.clone())) {
                    out.push(Arc::clone(d));
                }
            }
        }
        out
    }

    /// Whether `t` is a proper ancestor of the member: the
    /// publisher-may-publish-down rule.
    #[must_use]
    pub fn is_proper_ancestor(&self, t: TenantId) -> bool {
        self.levels.iter().skip(1).any(|l| l.tenant == t)
    }

    /// Whether a tenant at chain index `i` is a member of `def`.
    #[must_use]
    pub fn reaches_level(&self, def: &Definition, i: usize) -> bool {
        let Some(l) = self.levels.get(i) else {
            return false;
        };
        def.scope
            .reaches(def.declarer, l.tenant, &self.ancestry_of(i))
    }

    /// The effective value of `what` (and settings key `key`) for the
    /// member's instance of `def`, nearest level first (chapter 42,
    /// "Overrides: nearest level wins"):
    ///
    /// 1. an override targeting the member alone, or the member's own
    ///    subtree override: the member's own catalog first, then the
    ///    nearest ancestor's;
    /// 2. for each tenant between the member and the declarer, nearest
    ///    first: an override for that tenant's subtree;
    /// 3. `None`: the definition's own value.
    ///
    /// Returns the value and the tenant whose override it is.
    #[must_use]
    pub fn effective(
        &self,
        def: DefKey,
        what: What,
        key: Option<&str>,
    ) -> Option<(Value, TenantId)> {
        let t = self.member();
        let rows: Vec<&Arc<Override>> = self
            .levels
            .iter()
            .filter_map(|l| l.state.as_ref())
            .flat_map(|s| s.overrides.iter())
            .filter(|o| o.def == def && o.what == what)
            .collect();
        let pick = |o: &Override| -> Option<Value> {
            match key {
                None => Some(o.value.clone()),
                Some(k) => o.value.get(k).cloned(),
            }
        };
        // Levels, nearest first, as indexes into the chain.
        let level_of = |x: TenantId| self.levels.iter().position(|l| l.tenant == x);
        // Step 1: for the member alone (from any level) or the member's own
        // subtree override; made nearer wins.
        let mut step1: Vec<&Arc<Override>> =
            rows.iter().copied().filter(|o| o.target == t).collect();
        step1.sort_by_key(|o| level_of(o.made_in).unwrap_or(usize::MAX));
        if let Some((v, o)) = step1.iter().find_map(|o| pick(o).map(|v| (v, o))) {
            return Some((v, o.made_in));
        }
        // Step 2: subtree overrides of tenants above the member, nearest
        // target first; the declarer's own subtree override counts too
        // (it applies to the whole scope).
        for l in self.levels.iter().skip(1) {
            let mut at: Vec<&Arc<Override>> = rows
                .iter()
                .copied()
                .filter(|o| o.target == l.tenant && o.applies_to == AppliesTo::Subtree)
                .collect();
            at.sort_by_key(|o| level_of(o.made_in).unwrap_or(usize::MAX));
            if let Some((v, o)) = at.iter().find_map(|o| pick(o).map(|v| (v, o))) {
                return Some((v, o.made_in));
            }
            if l.tenant == def.tenant {
                break;
            }
        }
        None
    }

    /// Whether the member's instance of `def` is disabled by an override.
    #[must_use]
    pub fn disabled(&self, def: DefKey) -> bool {
        self.effective(def, What::State, None)
            .is_some_and(|(v, _)| v.as_str() == Some("disabled"))
    }
}

/// Kind-specific declaration of a stream definition
/// (`inherited_defs.definition` of kind `stream`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamSpec {
    /// `KEY (...)`.
    #[serde(default)]
    pub key: Vec<String>,
    /// `PARTITIONS n`.
    pub partitions: u32,
    /// `RETENTION '...'` in hours.
    #[serde(default)]
    pub retention_hours: Option<u64>,
    /// `delete` or `compact`.
    #[serde(default = "delete")]
    pub cleanup: String,
    /// Stream settings set on the definition (`ALTER STREAM ... SET`).
    #[serde(default)]
    pub settings: BTreeMap<String, Value>,
}

fn delete() -> String {
    "delete".to_owned()
}

/// Kind-specific declaration of a trigger definition.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TriggerSpec {
    /// The declaration without its `FOR EACH TENANT` clause, as the
    /// member's deploy parses it (`CREATE TRIGGER g ON STREAM db.s
    /// LANGUAGE ... AS ...`).
    pub text: String,
    /// `stream` or `queue`.
    pub source_kind: String,
    /// The source's database name.
    pub source_db: String,
    /// The source's name.
    pub source_name: String,
    /// The trigger's own database (its module's database in members).
    pub db: String,
}

/// One item of `GRANTS (...)`: privileges on `db.name` (`*` for every
/// object of the database).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantItem {
    /// Privilege names, upper case (`INSERT`, `UPDATE`, ...).
    pub privileges: Vec<String>,
    /// The database.
    pub db: String,
    /// The object, or `*`.
    pub name: String,
}

/// The parsed `grants` column.
#[must_use]
pub fn grant_items(v: Option<&Value>) -> Vec<GrantItem> {
    v.and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default()
}

/// Kind-specific declaration of an exchange definition (chapter 50,
/// "Inherited exchanges, queues and bindings").
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExchangeSpec {
    /// `direct`, `fanout`, `topic`, `headers` or `x-delayed-message`.
    pub kind: String,
    /// Durable (always, for definitions).
    #[serde(default = "yes")]
    pub durable: bool,
    /// Internal: not published to by clients.
    #[serde(default)]
    pub internal: bool,
    /// Arguments (`alternate-exchange`, `x-delayed-type`), JSON.
    #[serde(default)]
    pub arguments: BTreeMap<String, Value>,
}

/// Kind-specific declaration of a queue definition.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct QueueSpec {
    /// `x-*` arguments as declared (`x-queue-type`, `x-message-ttl`, ...).
    #[serde(default)]
    pub arguments: BTreeMap<String, Value>,
}

/// Kind-specific declaration of a binding definition.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BindingSpec {
    /// The source exchange (a definition's name).
    pub source: String,
    /// The destination's name.
    pub dest: String,
    /// `queue` or `exchange`.
    pub dest_kind: String,
    /// The binding key.
    #[serde(default)]
    pub key: String,
    /// Arguments (headers exchanges).
    #[serde(default)]
    pub arguments: BTreeMap<String, Value>,
}

impl BindingSpec {
    /// The name a binding definition is stored under (unique per database
    /// with the kind): `source>dest_kind:dest:key`.
    #[must_use]
    pub fn def_name(&self) -> String {
        format!(
            "{}>{}:{}:{}",
            self.source, self.dest_kind, self.dest, self.key
        )
    }
}

fn yes() -> bool {
    true
}

/// The queue arguments a member may override (chapter 42, "What can be
/// overridden"; the ones a policy of the vhost could set).
pub const QUEUE_OVERRIDABLE: &[&str] = &[
    "x-message-ttl",
    "x-expires",
    "x-max-length",
    "x-max-length-bytes",
    "x-overflow",
    "x-delivery-limit",
    "x-dead-letter-exchange",
    "x-dead-letter-routing-key",
    "x-consumer-timeout",
];
