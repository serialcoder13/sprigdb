//! Spaces, inheritable objects and shares in memory (chapter 42, "Data
//! structures and storage"): one tenant's [`InheritState`], loaded into
//! every catalog snapshot by [`crate::store::InheritExtension`].

use std::collections::BTreeMap;
use std::sync::Arc;

use sprig_common::ids::{DbId, ObjectId, TenantId};

/// The root space of every tenant.
pub const ROOT_SPACE: SpaceId = SpaceId(1);

/// `_sprig_ovr` of an `own` or `orphan` row: every field.
pub const ALL_BITS: u64 = u64::MAX;

/// A space, unique inside its tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpaceId(pub u64);

impl std::fmt::Display for SpaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// `_sprig_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LayerState {
    /// An addition: the record exists from this level down.
    Own,
    /// Field overrides of an inherited record.
    Ovr,
    /// The inherited record is hidden from this level down.
    Hide,
    /// An override whose inherited record was deleted (kept).
    Orphan,
}

impl LayerState {
    /// The column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Own => "own",
            Self::Ovr => "ovr",
            Self::Hide => "hide",
            Self::Orphan => "orphan",
        }
    }

    /// From the column value.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "own" => Some(Self::Own),
            "ovr" => Some(Self::Ovr),
            "hide" => Some(Self::Hide),
            "orphan" => Some(Self::Orphan),
            _ => None,
        }
    }
}

/// How an edge's merged view is computed (federated edges are not built).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MergeMode {
    /// By MariaDB at query time, from SQL the gateway generates.
    #[default]
    ReadThrough,
    /// From copies the change-feed worker keeps ([`crate::copy`]).
    Materialized,
    /// Through a Spider table on the parent's cluster (not built).
    Federated,
}

impl MergeMode {
    /// The column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadThrough => "read_through",
            Self::Materialized => "materialized",
            Self::Federated => "federated",
        }
    }

    /// From the column value.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "read_through" => Some(Self::ReadThrough),
            "materialized" => Some(Self::Materialized),
            "federated" => Some(Self::Federated),
            _ => None,
        }
    }
}

/// One space.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Space {
    /// The id; 1 is the root.
    pub id: SpaceId,
    /// The name, unique in the tenant (the root's is the tenant's name).
    pub name: String,
    /// The parent in this tenant; `None` for the root.
    pub parent: Option<SpaceId>,
    /// Depth inside the tenant (the root is 0).
    pub depth: u16,
}

/// A user `UNIQUE` key, widened with `_sprig_space` in the table.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UniqueKey {
    /// The index name.
    pub name: String,
    /// Its columns, as the user declared them.
    pub columns: Vec<String>,
}

/// An inheritable object (a table in R6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InheritableObject {
    /// `objects.object_id`.
    pub object_id: ObjectId,
    /// Its database.
    pub db: DbId,
    /// Its name (from `objects`).
    pub table: String,
    /// The identity key's columns.
    pub key: Vec<String>,
    /// Every user column in table order, the key's included.
    pub columns: Vec<String>,
    /// Field slots of the non-key columns, by lowercase name; never reused.
    pub slots: BTreeMap<String, u8>,
    /// User `UNIQUE` keys, as declared.
    pub uniques: Vec<UniqueKey>,
    /// `TAG FIELD`, if any.
    pub tag_field: Option<String>,
    /// Bumped on every change of the row.
    pub version: u64,
    /// The `AUTO_INCREMENT` column, if any: its values come from the
    /// tree's one sequence ([`crate::ids`]).
    pub auto: Option<String>,
    /// The foreign keys the gateway keeps for it ([`crate::links`]).
    pub links: Vec<Link>,
    /// Created by SprigDB in this tenant because its parent tenant shares
    /// it (Kiran's decision 1, 11 November 2026), rather than declared here.
    pub adopted: bool,
    /// The root table of the type tree this table belongs to (chapter 32,
    /// `INHERITS`): the tables of one tree inherit together, like a
    /// foreign-key group, without links (the type layer keeps their rows
    /// in step itself).
    pub type_root: Option<String>,
}

/// A foreign key of an inheritable object, kept by the gateway against
/// each space's merged view (never MariaDB's: the referenced row may live
/// in an ancestor's layer, in another schema).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Link {
    /// The constraint name.
    pub name: String,
    /// The referencing columns.
    pub columns: Vec<String>,
    /// The referenced table, in the same database: an inheritable object
    /// whose key is `ref_columns`.
    pub ref_table: String,
    /// The referenced columns (the referenced object's key, in order).
    pub ref_columns: Vec<String>,
}

/// What `inherit_ext` holds per object (format 2 of the module).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectExt {
    /// [`InheritableObject::auto`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto: Option<String>,
    /// [`InheritableObject::links`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Link>,
    /// [`InheritableObject::adopted`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub adopted: bool,
    /// [`InheritableObject::type_root`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_root: Option<String>,
}

impl InheritableObject {
    /// The fields `inherit_ext` stores.
    #[must_use]
    pub fn ext(&self) -> ObjectExt {
        ObjectExt {
            auto: self.auto.clone(),
            links: self.links.clone(),
            adopted: self.adopted,
            type_root: self.type_root.clone(),
        }
    }

    /// Whether this object and `other` are tables of one type tree.
    #[must_use]
    pub fn same_tree(&self, other: &InheritableObject) -> bool {
        self.db == other.db
            && match (&self.type_root, &other.type_root) {
                (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
                _ => false,
            }
    }

    /// Whether this object references `table` (by name, any case).
    #[must_use]
    pub fn references(&self, table: &str) -> bool {
        self.links
            .iter()
            .any(|l| l.ref_table.eq_ignore_ascii_case(table))
    }

    /// Whether `col` is part of the key.
    #[must_use]
    pub fn is_key(&self, col: &str) -> bool {
        self.key.iter().any(|k| k.eq_ignore_ascii_case(col))
    }

    /// The `_sprig_ovr` bit of non-key column `col` (slot 63 holds every
    /// column past the 63rd); 0 for a key or unknown column.
    #[must_use]
    pub fn bit(&self, col: &str) -> u64 {
        self.slots
            .get(&col.to_lowercase())
            .map_or(0, |s| 1u64 << u32::from((*s).min(63)))
    }

    /// The column as declared, by any spelling.
    #[must_use]
    pub fn column(&self, col: &str) -> Option<&str> {
        self.columns
            .iter()
            .find(|c| c.eq_ignore_ascii_case(col))
            .map(String::as_str)
    }

    /// The non-key columns, in table order.
    pub fn values(&self) -> impl Iterator<Item = &str> {
        self.columns
            .iter()
            .filter(|c| !self.is_key(c))
            .map(String::as_str)
    }

    /// Slots for columns that have none yet (new columns), never reusing
    /// a slot: the result is `None` when nothing changes.
    #[must_use]
    pub fn with_slots_for(&self, columns: &[String]) -> Option<Self> {
        let mut next = self
            .slots
            .values()
            .copied()
            .max()
            .map_or(0u8, |m| m.saturating_add(1));
        let mut out = self.clone();
        let mut changed = out.columns != columns;
        out.columns = columns.to_vec();
        for c in columns {
            if self.is_key(c) || out.slots.contains_key(&c.to_lowercase()) {
                continue;
            }
            out.slots.insert(c.to_lowercase(), next.min(63));
            next = next.saturating_add(1);
            changed = true;
        }
        changed.then_some(out)
    }
}

/// Where a share goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShareTarget {
    /// A child space in the same tenant.
    Space(SpaceId),
    /// A child tenant (its root space).
    Tenant(TenantId),
    /// Every current and future child (spaces and, from the root, child
    /// tenants).
    AllChildren,
}

/// A share, as its parent's catalog holds it (`role = 'granted'`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Share {
    /// The id, unique in the parent tenant.
    pub id: u64,
    /// The name, unique in the parent tenant.
    pub name: String,
    /// The shared object (the parent's).
    pub object_id: ObjectId,
    /// The space that shares.
    pub from: SpaceId,
    /// Where it goes.
    pub to: ShareTarget,
    /// The parent's row filter, over logical column names.
    pub filter: Option<String>,
    /// The shared columns (the key always included); `None` = all.
    pub columns: Option<Vec<String>>,
    /// The child's narrowing (same-tenant shares; across tenants it is in
    /// the child's catalog, [`Take`]).
    pub take: Option<String>,
    /// `WITH RESHARE`.
    pub reshare: bool,
    /// +1 on every filter change.
    pub version: u64,
    /// The creating user (the cross-tenant share view's definer).
    pub created_by: u32,
    /// How the edges of this share are computed (`shares.merge_mode`).
    pub mode: MergeMode,
    /// Set on a share SprigDB derives for another table of the shared
    /// table's foreign-key group (Kiran's decision 1): the id of the share
    /// the user created. Derived shares are never stored.
    pub via: Option<u64>,
}

/// The top bit marks a derived share's id ([`Share::via`]).
pub const DERIVED_SHARE: u64 = 1 << 63;

/// The id of the share derived from share `base` for object `o`: unique
/// per pair, never a stored share's.
#[must_use]
pub fn derived_share_id(base: u64, o: ObjectId) -> u64 {
    DERIVED_SHARE | ((base & 0x7fff_ffff) << 32) | (o.0 & 0xffff_ffff)
}

impl Share {
    /// Whether column `col` of object `o` is shared.
    #[must_use]
    pub fn shares_column(&self, o: &InheritableObject, col: &str) -> bool {
        o.is_key(col)
            || self
                .columns
                .as_ref()
                .is_none_or(|cs| cs.iter().any(|c| c.eq_ignore_ascii_case(col)))
    }
}

/// A child tenant's narrowing of a share it receives from its parent
/// tenant (`role = 'received'` rows).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Take {
    /// The parent's share id.
    pub share_id: u64,
    /// The parent tenant.
    pub peer_tenant: TenantId,
    /// The child's object.
    pub object_id: ObjectId,
    /// The narrowing.
    pub take: Option<String>,
}

/// One tenant's spaces, inheritable objects and shares.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InheritState {
    /// Whether the module's tables exist.
    pub migrated: bool,
    /// Whether format 2's `inherit_ext` exists too.
    pub ext_ready: bool,
    spaces: BTreeMap<SpaceId, Space>,
    objects: BTreeMap<ObjectId, Arc<InheritableObject>>,
    shares: Vec<Arc<Share>>,
    takes: Vec<Take>,
    homes: BTreeMap<u32, SpaceId>,
    /// Objects made inheritable before `inherit_ext` (no row there yet):
    /// their `AUTO_INCREMENT` column is learnt at their next insert.
    ext_missing: std::collections::BTreeSet<ObjectId>,
}

impl InheritState {
    /// A catalog without the module's tables.
    #[must_use]
    pub fn unmigrated() -> Self {
        Self::default()
    }

    /// A loaded state.
    #[must_use]
    pub fn new(
        spaces: Vec<Space>,
        objects: Vec<InheritableObject>,
        shares: Vec<Share>,
        takes: Vec<Take>,
        homes: Vec<(u32, SpaceId)>,
    ) -> Self {
        let mut st = Self {
            migrated: true,
            ext_ready: true,
            spaces: spaces.into_iter().map(|s| (s.id, s)).collect(),
            objects: objects
                .into_iter()
                .map(|o| (o.object_id, Arc::new(o)))
                .collect(),
            shares: shares.into_iter().map(Arc::new).collect(),
            takes,
            homes: homes.into_iter().collect(),
            ext_missing: std::collections::BTreeSet::new(),
        };
        st.derive_group_shares();
        st
    }

    /// Whether object `o` has no `inherit_ext` row yet.
    #[must_use]
    pub fn ext_missing(&self, o: ObjectId) -> bool {
        self.ext_missing.contains(&o)
    }

    /// Records the objects without an `inherit_ext` row (the loader).
    pub fn set_ext_missing(&mut self, ids: impl IntoIterator<Item = ObjectId>) {
        self.ext_missing = ids.into_iter().collect();
    }

    /// The objects linked to `o` by foreign keys, either way, through the
    /// closure (`o` included): its foreign-key group (Kiran's decision 1).
    #[must_use]
    pub fn group_of(&self, o: ObjectId) -> Vec<ObjectId> {
        let Some(start) = self.objects.get(&o) else {
            return vec![o];
        };
        let mut out = vec![o];
        let mut frontier = vec![Arc::clone(start)];
        while let Some(x) = frontier.pop() {
            for y in self.objects.values() {
                if out.contains(&y.object_id) || y.db != x.db {
                    continue;
                }
                if x.references(&y.table) || y.references(&x.table) || x.same_tree(y) {
                    out.push(y.object_id);
                    frontier.push(Arc::clone(y));
                }
            }
        }
        out.sort();
        out
    }

    /// Every object whose links reference `o` (the referencing side of
    /// `o`'s foreign keys), with the links.
    #[must_use]
    pub fn referencing(&self, o: &InheritableObject) -> Vec<(Arc<InheritableObject>, Link)> {
        let mut out = Vec::new();
        for x in self.objects.values().filter(|x| x.db == o.db) {
            for l in &x.links {
                if l.ref_table.eq_ignore_ascii_case(&o.table) {
                    out.push((Arc::clone(x), l.clone()));
                }
            }
        }
        out
    }

    /// An object by database and name in any case.
    #[must_use]
    pub fn object_named_ci(&self, db: DbId, table: &str) -> Option<&Arc<InheritableObject>> {
        self.objects
            .values()
            .find(|o| o.db == db && o.table.eq_ignore_ascii_case(table))
    }

    /// Adds, for every stored share of an object, a derived share of each
    /// other object of its foreign-key group that has no stored share of
    /// its own from the same space to the same target: sharing one table
    /// shares the group, unfiltered and with every column (Kiran's
    /// decision 1).
    fn derive_group_shares(&mut self) {
        let stored: Vec<Arc<Share>> = self.shares.clone();
        let mut add = Vec::new();
        for s in &stored {
            for m in self.group_of(s.object_id) {
                if m == s.object_id {
                    continue;
                }
                let own = stored
                    .iter()
                    .any(|x| x.object_id == m && x.from == s.from && x.to == s.to);
                let dup = add
                    .iter()
                    .any(|x: &Share| x.object_id == m && x.from == s.from && x.to == s.to);
                if own || dup {
                    continue;
                }
                add.push(Share {
                    id: derived_share_id(s.id, m),
                    name: s.name.clone(),
                    object_id: m,
                    from: s.from,
                    to: s.to,
                    filter: None,
                    columns: None,
                    take: None,
                    reshare: s.reshare,
                    version: s.version,
                    created_by: s.created_by,
                    mode: s.mode,
                    via: Some(s.id),
                });
            }
        }
        self.shares.extend(add.into_iter().map(Arc::new));
    }

    /// Whether the tenant uses inheritance at all (objects or shares).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.shares.is_empty() && self.spaces.len() <= 1
    }

    /// A space by id; the root is always there (named `root_name` when
    /// it has no row yet).
    #[must_use]
    pub fn space(&self, id: SpaceId, root_name: &str) -> Option<Space> {
        if let Some(s) = self.spaces.get(&id) {
            return Some(s.clone());
        }
        (id == ROOT_SPACE).then(|| Space {
            id: ROOT_SPACE,
            name: root_name.to_owned(),
            parent: None,
            depth: 0,
        })
    }

    /// A space by name (case-insensitive).
    #[must_use]
    pub fn space_named(&self, name: &str, root_name: &str) -> Option<Space> {
        if let Some(s) = self
            .spaces
            .values()
            .find(|s| s.name.eq_ignore_ascii_case(name))
        {
            return Some(s.clone());
        }
        (name.eq_ignore_ascii_case(root_name) && !self.spaces.contains_key(&ROOT_SPACE)).then(
            || Space {
                id: ROOT_SPACE,
                name: root_name.to_owned(),
                parent: None,
                depth: 0,
            },
        )
    }

    /// Every space (the root included once it has a row).
    pub fn spaces(&self) -> impl Iterator<Item = &Space> {
        self.spaces.values()
    }

    /// The direct child spaces of `id`.
    #[must_use]
    pub fn children(&self, id: SpaceId) -> Vec<SpaceId> {
        self.spaces
            .values()
            .filter(|s| s.parent == Some(id))
            .map(|s| s.id)
            .collect()
    }

    /// `id`'s parent space.
    #[must_use]
    pub fn parent(&self, id: SpaceId) -> Option<SpaceId> {
        self.spaces.get(&id).and_then(|s| s.parent)
    }

    /// The path from the root to `id`, both included.
    #[must_use]
    pub fn path(&self, id: SpaceId) -> Vec<SpaceId> {
        let mut out = vec![id];
        let mut cur = id;
        while let Some(p) = self.parent(cur) {
            if out.contains(&p) {
                break;
            }
            out.push(p);
            cur = p;
        }
        out.reverse();
        out
    }

    /// Whether `a` is `b` or one of its ancestors.
    #[must_use]
    pub fn is_ancestor_or_self(&self, a: SpaceId, b: SpaceId) -> bool {
        self.path(b).contains(&a)
    }

    /// `id`'s descendants, `id` excluded, parents before children.
    #[must_use]
    pub fn descendants(&self, id: SpaceId) -> Vec<SpaceId> {
        let mut out = Vec::new();
        let mut frontier = vec![id];
        while let Some(s) = frontier.pop() {
            for c in self.children(s) {
                if !out.contains(&c) && c != id {
                    out.push(c);
                    frontier.push(c);
                }
            }
        }
        out.sort_by_key(|s| self.spaces.get(s).map_or(0, |x| x.depth));
        out
    }

    /// An inheritable object.
    #[must_use]
    pub fn object(&self, id: ObjectId) -> Option<&Arc<InheritableObject>> {
        self.objects.get(&id)
    }

    /// Every inheritable object.
    pub fn objects(&self) -> impl Iterator<Item = &Arc<InheritableObject>> {
        self.objects.values()
    }

    /// An inheritable object by database and name.
    #[must_use]
    pub fn object_named(
        &self,
        db: DbId,
        table: &str,
        lctn: bool,
    ) -> Option<&Arc<InheritableObject>> {
        self.objects.values().find(|o| {
            o.db == db
                && if lctn {
                    o.table.eq_ignore_ascii_case(table)
                } else {
                    o.table == table
                }
        })
    }

    /// Every share this tenant granted.
    pub fn shares(&self) -> impl Iterator<Item = &Arc<Share>> {
        self.shares.iter()
    }

    /// A share by name.
    #[must_use]
    pub fn share_named(&self, name: &str) -> Option<&Arc<Share>> {
        self.shares
            .iter()
            .find(|s| s.via.is_none() && s.name.eq_ignore_ascii_case(name))
    }

    /// The shares of object `o`.
    pub fn shares_of(&self, o: ObjectId) -> impl Iterator<Item = &Arc<Share>> {
        self.shares.iter().filter(move |s| s.object_id == o)
    }

    /// The share carrying object `o` from `child`'s parent space into
    /// `child` (a share naming the child wins over `ALL CHILDREN`).
    #[must_use]
    pub fn edge(&self, o: ObjectId, child: SpaceId) -> Option<&Arc<Share>> {
        let parent = self.parent(child)?;
        let mut all = None;
        for s in self.shares_of(o).filter(|s| s.from == parent) {
            match s.to {
                ShareTarget::Space(c) if c == child => return Some(s),
                ShareTarget::AllChildren => all = Some(s),
                _ => {}
            }
        }
        all
    }

    /// The share carrying object `o` from this tenant's root to child
    /// tenant `child`.
    #[must_use]
    pub fn tenant_edge(&self, o: ObjectId, child: TenantId) -> Option<&Arc<Share>> {
        let mut all = None;
        for s in self.shares_of(o).filter(|s| s.from == ROOT_SPACE) {
            match s.to {
                ShareTarget::Tenant(t) if t == child => return Some(s),
                ShareTarget::AllChildren => all = Some(s),
                _ => {}
            }
        }
        all
    }

    /// This tenant's narrowing of share `share_id` from its parent tenant.
    #[must_use]
    pub fn take(&self, share_id: u64) -> Option<&Take> {
        self.takes.iter().find(|t| t.share_id == share_id)
    }

    /// Every narrowing of a received share.
    pub fn takes(&self) -> impl Iterator<Item = &Take> {
        self.takes.iter()
    }

    /// A user's home space (the root when unset).
    #[must_use]
    pub fn home(&self, user: u32) -> SpaceId {
        self.homes.get(&user).copied().unwrap_or(ROOT_SPACE)
    }

    /// Every explicit home space.
    pub fn homes(&self) -> impl Iterator<Item = (u32, SpaceId)> + '_ {
        self.homes.iter().map(|(u, s)| (*u, *s))
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! The chapter's worked example: `platform` (1) → `acme` (7) → `pune`
    //! (12); share 31 (1→7) filters `region = 'IN'`, share 32 (7→12) has
    //! no filter.

    use super::*;

    pub(crate) fn products() -> InheritableObject {
        let columns: Vec<String> = ["id", "name", "colour", "price", "region"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        InheritableObject {
            object_id: ObjectId(100),
            db: DbId(1),
            table: "products".into(),
            key: vec!["id".into()],
            slots: [("name", 0u8), ("colour", 1), ("price", 2), ("region", 3)]
                .iter()
                .map(|(c, s)| ((*c).to_owned(), *s))
                .collect(),
            columns,
            uniques: Vec::new(),
            tag_field: None,
            version: 1,
            auto: None,
            links: Vec::new(),
            adopted: false,
            type_root: None,
        }
    }

    pub(crate) fn state() -> InheritState {
        let sp = |id: u64, name: &str, parent: Option<u64>, depth: u16| Space {
            id: SpaceId(id),
            name: name.into(),
            parent: parent.map(SpaceId),
            depth,
        };
        let share = |id: u64, from: u64, to: u64, filter: Option<&str>| Share {
            id,
            name: format!("s{id}"),
            object_id: ObjectId(100),
            from: SpaceId(from),
            to: ShareTarget::Space(SpaceId(to)),
            filter: filter.map(str::to_owned),
            columns: None,
            take: None,
            reshare: true,
            version: 1,
            created_by: 1,
            mode: MergeMode::ReadThrough,
            via: None,
        };
        InheritState::new(
            vec![
                sp(1, "platform", None, 0),
                sp(7, "acme", Some(1), 1),
                sp(12, "pune", Some(7), 2),
            ],
            vec![products()],
            vec![
                share(31, 1, 7, Some("region = 'IN'")),
                share(32, 7, 12, None),
            ],
            Vec::new(),
            Vec::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_edges_and_bits() {
        let s = fixtures::state();
        assert_eq!(
            s.path(SpaceId(12)),
            vec![SpaceId(1), SpaceId(7), SpaceId(12)]
        );
        assert_eq!(s.descendants(ROOT_SPACE), vec![SpaceId(7), SpaceId(12)]);
        assert!(s.is_ancestor_or_self(SpaceId(7), SpaceId(12)));
        assert!(!s.is_ancestor_or_self(SpaceId(12), SpaceId(7)));
        assert_eq!(s.edge(ObjectId(100), SpaceId(7)).unwrap().id, 31);
        assert_eq!(s.edge(ObjectId(100), SpaceId(12)).unwrap().id, 32);
        assert!(s.edge(ObjectId(100), ROOT_SPACE).is_none());
        let o = fixtures::products();
        assert_eq!(o.bit("colour"), 2);
        assert_eq!(o.bit("ID"), 0);
        let wider = o
            .with_slots_for(&[
                "id".into(),
                "name".into(),
                "colour".into(),
                "price".into(),
                "region".into(),
                "sku".into(),
            ])
            .unwrap();
        assert_eq!(wider.bit("sku"), 16);
        assert!(wider.with_slots_for(&wider.columns.clone()).is_none());
    }

    #[test]
    fn a_share_carries_the_foreign_key_group() {
        let obj = |id: u64, name: &str, links: &[&str]| InheritableObject {
            object_id: ObjectId(id),
            db: DbId(1),
            table: name.into(),
            key: vec!["id".into()],
            columns: vec!["id".into(), "r".into()],
            slots: [("r".to_owned(), 0u8)].into_iter().collect(),
            uniques: Vec::new(),
            tag_field: None,
            version: 1,
            auto: Some("id".into()),
            links: links
                .iter()
                .map(|t| Link {
                    name: format!("{name}_ibfk_1"),
                    columns: vec!["r".into()],
                    ref_table: (*t).into(),
                    ref_columns: vec!["id".into()],
                })
                .collect(),
            adopted: false,
            type_root: None,
        };
        let share = Share {
            id: 5,
            name: "s".into(),
            object_id: ObjectId(1),
            from: ROOT_SPACE,
            to: ShareTarget::AllChildren,
            filter: Some("r > 1".into()),
            columns: None,
            take: None,
            reshare: true,
            version: 1,
            created_by: 1,
            mode: MergeMode::ReadThrough,
            via: None,
        };
        // t1 -> t2, t3 -> t1, t9 alone.
        let st = InheritState::new(
            Vec::new(),
            vec![
                obj(1, "t1", &["t2"]),
                obj(2, "t2", &[]),
                obj(3, "t3", &["T1"]),
                obj(9, "t9", &[]),
            ],
            vec![share],
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(
            st.group_of(ObjectId(2)),
            vec![ObjectId(1), ObjectId(2), ObjectId(3)]
        );
        let t2 = st.tenant_edge(ObjectId(2), TenantId(77)).unwrap();
        assert_eq!(t2.via, Some(5));
        assert_eq!(t2.filter, None, "a group member comes whole (Q1)");
        assert_eq!(t2.id, derived_share_id(5, ObjectId(2)));
        assert!(st.tenant_edge(ObjectId(3), TenantId(77)).is_some());
        assert!(st.tenant_edge(ObjectId(9), TenantId(77)).is_none());
        // Derived shares can't be named.
        assert_eq!(st.share_named("s").unwrap().object_id, ObjectId(1));
        assert_eq!(st.referencing(&obj(1, "t1", &[])).len(), 1);
    }
}
