//! The merged view as a Rust model (chapter 42, "Testing": the reference
//! model). Spaces of one tree, across tenants, with integer keys and text
//! fields; the live suites run random writes at random layers through a
//! gateway and compare every space's reads with this model.
//!
//! The model follows the chapter's semantics. A parent delete orphans
//! overrides synchronously only inside a tenant. Across tenants, without
//! the copy worker the override is kept but becomes invisible; with it
//! ([`Model::async_orphans`], the state once the feed drains) an override
//! in a tenant's root space becomes an orphan as soon as no level above
//! still has the record, ignoring share filters ([`crate::copy`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::model::{ALL_BITS, LayerState};

/// A record's fields (the key is separate), `None` for SQL `NULL`.
pub type Fields = Vec<Option<String>>;

/// A share filter over a record.
pub type Filter = Arc<dyn Fn(i64, &Fields) -> bool + Send + Sync>;

/// The edge into a space.
#[derive(Clone)]
pub struct EdgeModel {
    /// The parent's filter and the child's narrowing, together.
    pub filter: Option<Filter>,
    /// Shared field indexes; `None` = all.
    pub columns: Option<Vec<usize>>,
}

/// One space.
#[derive(Clone)]
pub struct SpaceModel {
    /// Its parent.
    pub parent: Option<u64>,
    /// Its tenant (orphaning is synchronous inside one).
    pub tenant: u64,
    /// The edge from the parent, if the parent shares the object.
    pub edge: Option<EdgeModel>,
}

/// One layer row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerRow {
    /// `_sprig_state`.
    pub state: LayerState,
    /// `_sprig_ovr`.
    pub ovr: u64,
    /// The stored fields.
    pub fields: Fields,
}

/// A view: key to fields.
pub type View = BTreeMap<i64, Fields>;

/// Why a write failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelError {
    /// 1062.
    Duplicate(i64),
}

/// The model of one object.
#[derive(Clone)]
pub struct Model {
    /// Number of fields.
    pub fields: usize,
    /// Spaces by global id.
    pub spaces: BTreeMap<u64, SpaceModel>,
    /// Rows by (space, key).
    pub rows: BTreeMap<(u64, i64), LayerRow>,
    /// `inherit.parent_delete = follow`.
    pub follow: bool,
    /// The copy worker orphans across tenants (after every write).
    pub async_orphans: bool,
}

fn bit(i: usize) -> u64 {
    1u64 << i.min(63)
}

impl Model {
    /// An empty model with `fields` fields.
    #[must_use]
    pub fn new(fields: usize) -> Self {
        Self {
            fields,
            spaces: BTreeMap::new(),
            rows: BTreeMap::new(),
            follow: false,
            async_orphans: false,
        }
    }

    /// Whether the record `k` exists at `s`, ignoring share filters.
    fn exists_unfiltered(&self, s: u64, k: i64) -> bool {
        match self.rows.get(&(s, k)).map(|r| r.state) {
            Some(LayerState::Hide) => false,
            Some(LayerState::Own | LayerState::Orphan) => true,
            _ => {
                let Some(sp) = self.spaces.get(&s) else {
                    return false;
                };
                match (sp.parent, &sp.edge) {
                    (Some(p), Some(_)) => self.exists_unfiltered(p, k),
                    _ => false,
                }
            }
        }
    }

    /// The copy worker's orphan pass, parents first: overrides in spaces
    /// whose parent is in another tenant, of records that no longer exist
    /// above, become orphans with their stored values.
    pub fn settle(&mut self) {
        if !self.async_orphans {
            return;
        }
        let mut roots: Vec<u64> = self
            .spaces
            .iter()
            .filter(|(_, x)| {
                x.edge.is_some()
                    && x.parent
                        .and_then(|p| self.spaces.get(&p))
                        .is_some_and(|p| p.tenant != x.tenant)
            })
            .map(|(id, _)| *id)
            .collect();
        roots.sort_by_key(|d| self.depth(*d));
        for d in roots {
            let Some(p) = self.spaces.get(&d).and_then(|x| x.parent) else {
                continue;
            };
            let keys: Vec<i64> = self
                .rows
                .range((d, i64::MIN)..=(d, i64::MAX))
                .filter(|(_, r)| r.state == LayerState::Ovr)
                .map(|((_, k), _)| *k)
                .collect();
            for k in keys {
                if !self.exists_unfiltered(p, k)
                    && let Some(r) = self.rows.get_mut(&(d, k))
                {
                    r.state = LayerState::Orphan;
                    r.ovr = ALL_BITS;
                }
            }
        }
    }

    /// Adds a space.
    pub fn space(&mut self, id: u64, parent: Option<u64>, tenant: u64, edge: Option<EdgeModel>) {
        self.spaces.insert(
            id,
            SpaceModel {
                parent,
                tenant,
                edge,
            },
        );
    }

    fn children(&self, s: u64) -> Vec<u64> {
        self.spaces
            .iter()
            .filter(|(_, x)| x.parent == Some(s))
            .map(|(id, _)| *id)
            .collect()
    }

    fn depth(&self, s: u64) -> usize {
        let mut d = 0;
        let mut cur = s;
        while let Some(p) = self.spaces.get(&cur).and_then(|x| x.parent) {
            d += 1;
            cur = p;
        }
        d
    }

    /// The rows `s` inherits (its parent's view through the edge); `None`
    /// without an edge.
    #[must_use]
    pub fn inherited(&self, s: u64) -> Option<View> {
        let sp = self.spaces.get(&s)?;
        let edge = sp.edge.as_ref()?;
        let parent = sp.parent?;
        let pv = self.view(parent);
        Some(
            pv.into_iter()
                .filter(|(k, f)| edge.filter.as_ref().is_none_or(|flt| flt(*k, f)))
                .map(|(k, f)| {
                    let f = match &edge.columns {
                        None => f,
                        Some(cs) => f
                            .iter()
                            .enumerate()
                            .map(|(i, v)| if cs.contains(&i) { v.clone() } else { None })
                            .collect(),
                    };
                    (k, f)
                })
                .collect(),
        )
    }

    /// `view(o, s)`.
    #[must_use]
    pub fn view(&self, s: u64) -> View {
        let layer: BTreeMap<i64, &LayerRow> = self
            .rows
            .range((s, i64::MIN)..=(s, i64::MAX))
            .map(|((_, k), r)| (*k, r))
            .collect();
        let mut out = View::new();
        let p = self.inherited(s).unwrap_or_default();
        for (k, pf) in &p {
            match layer.get(k) {
                Some(r) if r.state == LayerState::Hide => {}
                Some(r) => {
                    let f = (0..self.fields)
                        .map(|i| {
                            if r.ovr & bit(i) != 0 {
                                r.fields[i].clone()
                            } else {
                                pf[i].clone()
                            }
                        })
                        .collect();
                    out.insert(*k, f);
                }
                None => {
                    out.insert(*k, pf.clone());
                }
            }
        }
        for (k, r) in &layer {
            if matches!(r.state, LayerState::Own | LayerState::Orphan) && !p.contains_key(k) {
                out.insert(*k, r.fields.clone());
            }
        }
        out
    }

    /// `INSERT` in `s`.
    ///
    /// # Errors
    /// [`ModelError::Duplicate`] for a visible key.
    pub fn insert(&mut self, s: u64, key: i64, fields: Fields) -> Result<(), ModelError> {
        if self.view(s).contains_key(&key) {
            return Err(ModelError::Duplicate(key));
        }
        self.rows.insert(
            (s, key),
            LayerRow {
                state: LayerState::Own,
                ovr: ALL_BITS,
                fields,
            },
        );
        self.settle();
        Ok(())
    }

    /// `UPDATE ... SET` in `s` of the visible records `pred` selects;
    /// returns (matched, changed).
    pub fn update(
        &mut self,
        s: u64,
        pred: &dyn Fn(i64, &Fields) -> bool,
        set: &[(usize, Option<String>)],
        record: bool,
    ) -> (usize, usize) {
        let v = self.view(s);
        let (mut matched, mut changed) = (0, 0);
        for (k, merged) in v.iter().filter(|(k, f)| pred(**k, f)) {
            matched += 1;
            if set.iter().any(|(i, val)| &merged[*i] != val) {
                changed += 1;
            }
            let bits = if record {
                ALL_BITS
            } else {
                set.iter().fold(0, |b, (i, _)| b | bit(*i))
            };
            match self.rows.get_mut(&(s, *k)) {
                Some(r) if r.state != LayerState::Hide => {
                    for (i, val) in set {
                        r.fields[*i].clone_from(val);
                    }
                    if record && r.state == LayerState::Ovr {
                        r.fields.clone_from(merged);
                        for (i, val) in set {
                            r.fields[*i].clone_from(val);
                        }
                    }
                    r.ovr |= bits;
                }
                _ => {
                    let mut f = merged.clone();
                    for (i, val) in set {
                        f[*i].clone_from(val);
                    }
                    self.rows.insert(
                        (s, *k),
                        LayerRow {
                            state: LayerState::Ovr,
                            ovr: bits,
                            fields: f,
                        },
                    );
                }
            }
        }
        self.settle();
        (matched, changed)
    }

    /// `DELETE` in `s` of the visible records `pred` selects; returns the
    /// count.
    pub fn delete(&mut self, s: u64, pred: &dyn Fn(i64, &Fields) -> bool) -> usize {
        let v = self.view(s);
        let keys: Vec<i64> = v
            .iter()
            .filter(|(k, f)| pred(**k, f))
            .map(|(k, _)| *k)
            .collect();
        let inherited = self.inherited(s).unwrap_or_default();
        let tenant = self.spaces.get(&s).map_or(0, |x| x.tenant);
        // Descendants in the same tenant, parents first.
        let mut desc = Vec::new();
        let mut frontier = vec![s];
        while let Some(x) = frontier.pop() {
            for c in self.children(x) {
                if self.spaces.get(&c).map(|y| y.tenant) == Some(tenant) {
                    desc.push(c);
                    frontier.push(c);
                }
            }
        }
        desc.sort_by_key(|d| self.depth(*d));
        let mut before: BTreeMap<u64, View> = BTreeMap::new();
        for d in &desc {
            if let Some(p) = self.spaces.get(d).and_then(|x| x.parent) {
                before.entry(p).or_insert_with(|| self.view(p));
            }
        }
        for k in &keys {
            if inherited.contains_key(k) {
                let fields = v.get(k).cloned().unwrap_or_default();
                match self.rows.get_mut(&(s, *k)) {
                    Some(r) => {
                        r.state = LayerState::Hide;
                        r.ovr = 0;
                    }
                    None => {
                        self.rows.insert(
                            (s, *k),
                            LayerRow {
                                state: LayerState::Hide,
                                ovr: 0,
                                fields,
                            },
                        );
                    }
                }
            } else {
                self.rows.remove(&(s, *k));
            }
        }
        let keyset: BTreeSet<i64> = keys.iter().copied().collect();
        for d in &desc {
            let Some(p) = self.spaces.get(d).and_then(|x| x.parent) else {
                continue;
            };
            let after = self.view(p);
            for k in &keyset {
                let is_ovr = self
                    .rows
                    .get(&(*d, *k))
                    .is_some_and(|r| r.state == LayerState::Ovr);
                if !is_ovr {
                    continue;
                }
                let Some(was) = before.get(&p).and_then(|b| b.get(k)) else {
                    continue;
                };
                if after.contains_key(k) {
                    continue;
                }
                if self.follow {
                    self.rows.remove(&(*d, *k));
                } else if let Some(r) = self.rows.get_mut(&(*d, *k)) {
                    for (i, w) in was.iter().enumerate().take(self.fields) {
                        if r.ovr & bit(i) == 0 {
                            r.fields[i].clone_from(w);
                        }
                    }
                    r.state = LayerState::Orphan;
                    r.ovr = ALL_BITS;
                }
            }
        }
        self.settle();
        keys.len()
    }

    /// `SPRIG RESET` in `s` of the layer rows `pred` selects (on their
    /// stored fields); `cols`: only those fields.
    pub fn reset(&mut self, s: u64, pred: &dyn Fn(i64, &Fields) -> bool, cols: Option<&[usize]>) {
        let keys: Vec<i64> = self
            .rows
            .range((s, i64::MIN)..=(s, i64::MAX))
            .filter(|((_, k), r)| r.state != LayerState::Own && pred(*k, &r.fields))
            .map(|((_, k), _)| *k)
            .collect();
        for k in keys {
            match cols {
                None => {
                    self.rows.remove(&(s, k));
                }
                Some(cs) => {
                    let bits = cs.iter().fold(0, |b, i| b | bit(*i));
                    let drop = match self.rows.get_mut(&(s, k)) {
                        Some(r) if r.state == LayerState::Ovr => {
                            r.ovr &= !bits;
                            r.ovr == 0
                        }
                        _ => false,
                    };
                    if drop {
                        self.rows.remove(&(s, k));
                    }
                }
            }
        }
        self.settle();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(v: &[&str]) -> Fields {
        v.iter().map(|s| Some((*s).to_owned())).collect()
    }

    /// The chapter's worked example: name 0, colour 1, price 2, region 3.
    #[test]
    fn worked_example() {
        let mut m = Model::new(4);
        let india: Filter = Arc::new(|_, f: &Fields| f[3].as_deref() == Some("IN"));
        m.space(1, None, 42, None);
        m.space(
            7,
            Some(1),
            42,
            Some(EdgeModel {
                filter: Some(Arc::clone(&india)),
                columns: None,
            }),
        );
        m.space(
            12,
            Some(7),
            42,
            Some(EdgeModel {
                filter: None,
                columns: None,
            }),
        );
        m.insert(1, 1, f(&["P1", "red", "10.00", "IN"])).unwrap();
        m.insert(1, 2, f(&["P2", "blue", "20.00", "IN"])).unwrap();
        m.insert(1, 3, f(&["P3", "green", "30.00", "US"])).unwrap();
        m.update(7, &|k, _| k == 2, &[(1, Some("navy".into()))], false);
        m.insert(7, 9, f(&["P9", "black", "15.00", "IN"])).unwrap();
        assert_eq!(m.delete(12, &|k, _| k == 1), 1);
        assert_eq!(m.view(7).keys().copied().collect::<Vec<_>>(), vec![1, 2, 9]);
        assert_eq!(m.view(12).keys().copied().collect::<Vec<_>>(), vec![2, 9]);
        assert_eq!(
            m.insert(12, 2, f(&["x", "x", "1", "IN"])),
            Err(ModelError::Duplicate(2))
        );
        // 2. The platform changes P1 and P2.
        m.update(1, &|k, _| k == 1, &[(1, Some("orange".into()))], false);
        m.update(1, &|k, _| k == 2, &[(2, Some("22.00".into()))], false);
        assert_eq!(m.view(7)[&2], f(&["P2", "navy", "22.00", "IN"]));
        assert!(!m.view(12).contains_key(&1));
        // 3. The platform deletes P2: acme's override is orphaned.
        m.delete(1, &|k, _| k == 2);
        assert_eq!(m.rows[&(7, 2)].state, LayerState::Orphan);
        assert_eq!(m.view(7)[&2], f(&["P2", "navy", "22.00", "IN"]));
        assert_eq!(m.view(12)[&2], f(&["P2", "navy", "22.00", "IN"]));
        // 5. Acme resets P2: it leaves both views.
        m.reset(7, &|k, _| k == 2, None);
        assert!(!m.view(7).contains_key(&2));
        assert!(!m.view(12).contains_key(&2));
    }

    /// "A later change in the middle": a → b → c.
    #[test]
    fn later_change_in_the_middle() {
        let mut m = Model::new(3);
        let all = || {
            Some(EdgeModel {
                filter: None,
                columns: None,
            })
        };
        m.space(10, None, 10, None);
        m.space(20, Some(10), 20, all());
        m.space(30, Some(20), 30, all());
        m.insert(10, 5, f(&["Desk", "oak", "100"])).unwrap();
        m.update(20, &|_, _| true, &[(2, Some("90".into()))], false);
        m.update(30, &|_, _| true, &[(1, Some("white".into()))], false);
        assert_eq!(m.view(30)[&5], f(&["Desk", "white", "90"]));
        m.update(
            20,
            &|_, _| true,
            &[
                (0, Some("Desk Pro".into())),
                (1, Some("walnut".into())),
                (2, Some("95".into())),
            ],
            false,
        );
        assert_eq!(m.view(30)[&5], f(&["Desk Pro", "white", "95"]));
        m.reset(20, &|_, _| true, Some(&[2]));
        assert_eq!(m.view(30)[&5], f(&["Desk Pro", "white", "100"]));
        m.update(10, &|_, _| true, &[(2, Some("120".into()))], false);
        assert_eq!(m.view(30)[&5], f(&["Desk Pro", "white", "120"]));
        // Across tenants a parent delete hides the child's override; the
        // copy worker then orphans it, and c inherits the orphan.
        let mut w = m.clone();
        m.delete(10, &|_, _| true);
        assert!(m.view(20).is_empty());
        assert_eq!(m.rows[&(20, 5)].state, LayerState::Ovr);
        w.async_orphans = true;
        w.delete(10, &|_, _| true);
        assert_eq!(w.rows[&(20, 5)].state, LayerState::Orphan);
        assert_eq!(w.view(20)[&5], f(&["Desk Pro", "walnut", "95"]));
        // c's own override stays one: b still has the record.
        assert_eq!(w.rows[&(30, 5)].state, LayerState::Ovr);
        assert_eq!(w.view(30)[&5], f(&["Desk Pro", "white", "95"]));
    }
}
