//! Memoized resolution results (chapter 08: "resolved values are memoized
//! per `(key, ctx)` until a version changes").
//!
//! A [`Memo`] lives next to immutable rows: inside a [`TenantSettings`]
//! (one tenant at one catalog version) or a [`Settings`] (process-level
//! contexts). Each result records the etcd snapshot's
//! [stamp](crate::MemSettings::stamp) it was computed against, so it is
//! valid exactly for the pair (tenant snapshot version, etcd snapshot).
//!
//! Lookups take a read lock and allocate nothing; inserts clone the
//! context's DC and cluster ids once. The memo holds at most [`CAP`]
//! results and is cleared when full, which keeps it bounded without LRU
//! bookkeeping on the read path.
//!
//! [`TenantSettings`]: super::TenantSettings
//! [`Settings`]: super::Settings

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};

use super::Resolved;
use crate::scope::SettingCtx;

/// Results kept per memo before it is cleared.
pub(crate) const CAP: usize = 4096;

/// The cheap part of a memo key; the DC and cluster are compared per entry.
type Head = (
    &'static str,
    Option<TenantId>,
    Option<DbId>,
    Option<ObjectId>,
);

#[derive(Debug)]
struct Slot {
    dc: DcId,
    cluster: Option<ClusterId>,
    stamp: u64,
    resolved: Resolved,
}

#[derive(Debug, Default)]
struct Table {
    map: HashMap<Head, Vec<Slot>>,
    len: usize,
}

/// Memoized results of one set of rows.
#[derive(Debug, Default)]
pub(crate) struct Memo {
    table: RwLock<Table>,
}

fn head(key: &'static str, ctx: &SettingCtx<'_>) -> Head {
    (key, ctx.tenant, ctx.db, ctx.object)
}

fn same_place(s: &Slot, ctx: &SettingCtx<'_>) -> bool {
    s.dc == *ctx.dc && s.cluster.as_ref() == ctx.cluster
}

impl Memo {
    /// The result for `key` in `ctx` computed against etcd stamp `stamp`.
    pub(crate) fn get(
        &self,
        key: &'static str,
        ctx: &SettingCtx<'_>,
        stamp: u64,
    ) -> Option<Resolved> {
        let t = self.table.read().unwrap_or_else(PoisonError::into_inner);
        t.map
            .get(&head(key, ctx))?
            .iter()
            .find(|s| s.stamp == stamp && same_place(s, ctx))
            .map(|s| s.resolved.clone())
    }

    /// Records a result, replacing an older one for the same context.
    pub(crate) fn put(&self, key: &'static str, ctx: &SettingCtx<'_>, stamp: u64, r: &Resolved) {
        let mut t = self.table.write().unwrap_or_else(PoisonError::into_inner);
        if t.len >= CAP {
            t.map.clear();
            t.len = 0;
        }
        let slots = t.map.entry(head(key, ctx)).or_default();
        if let Some(s) = slots.iter_mut().find(|s| same_place(s, ctx)) {
            s.stamp = stamp;
            s.resolved = r.clone();
            return;
        }
        slots.push(Slot {
            dc: ctx.dc.clone(),
            cluster: ctx.cluster.cloned(),
            stamp,
            resolved: r.clone(),
        });
        t.len += 1;
    }

    /// Number of memoized results.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.table
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len
    }
}
