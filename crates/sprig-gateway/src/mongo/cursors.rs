//! Cursors.
//!
//! Ids are positive `i64`s: bits 47-62 hold 16 bits of a hash of the
//! gateway id, bits 0-46 are random. A cursor keeps the engine's
//! resumable plan and no backend connection. `getMore` and `killCursors`
//! must come from the cursor's owner; an idle cursor dies after
//! `mongo.cursor_timeout_ms`; past `mongo.max_cursors_per_tenant` a new
//! cursor fails with 39630 `SprigCursorLimit`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_doc::engine::CursorPlan;
use sprig_proto_mongo::errors::{MongoError, code};

/// Who may use a cursor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Owner {
    /// The tenant.
    pub tenant: u64,
    /// The user.
    pub user: String,
}

/// One open cursor.
#[derive(Debug)]
pub struct Cursor {
    /// The plan.
    pub plan: CursorPlan,
    /// `db.collection` as the reply names it.
    pub ns: String,
    /// The owner.
    pub owner: Owner,
    /// `lsid.id`, when opened in a session.
    pub lsid: Option<Vec<u8>>,
    /// The default `getMore` batch size.
    pub batch_size: Option<usize>,
    /// `noCursorTimeout`.
    pub no_timeout: bool,
    /// A change stream's feed; `plan` is unused.
    pub stream: Option<super::changes::ChangeStream>,
    /// A tailable `awaitData` cursor: `getMore` waits up to its
    /// `maxTimeMS` for new documents.
    pub await_data: bool,
    last_used: Instant,
}

/// The cursors of this gateway.
#[derive(Debug, Default)]
pub struct CursorTable {
    map: Mutex<HashMap<i64, Arc<tokio::sync::Mutex<Option<Cursor>>>>>,
    gateway_bits: i64,
}

impl CursorTable {
    /// A table whose ids carry `gateway_id`'s hash.
    #[must_use]
    pub fn new(gateway_id: &str) -> Self {
        let h = sprig_doc::meta::fnv64(gateway_id.as_bytes()) & 0xFFFF;
        Self {
            map: Mutex::new(HashMap::new()),
            gateway_bits: i64::try_from(h).unwrap_or(0) << 47,
        }
    }

    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<i64, Arc<tokio::sync::Mutex<Option<Cursor>>>>> {
        self.map.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Opens a cursor; returns its id.
    ///
    /// # Errors
    /// 39630 past `max_per_tenant` open cursors of the tenant.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        &self,
        plan: CursorPlan,
        ns: String,
        owner: Owner,
        lsid: Option<Vec<u8>>,
        batch_size: Option<usize>,
        no_timeout: bool,
        max_per_tenant: usize,
    ) -> Result<i64, MongoError> {
        let mut m = self.lock();
        let tenant_open = m.len();
        if tenant_open >= max_per_tenant {
            return Err(MongoError::new(
                code::SPRIG_CURSOR_LIMIT,
                format!(
                    "too many open cursors on this gateway (mongo.max_cursors_per_tenant = {max_per_tenant})"
                ),
            ));
        }
        loop {
            let mut r = [0u8; 8];
            let _ = getrandom::fill(&mut r);
            let id = self.gateway_bits | (i64::from_le_bytes(r) & ((1i64 << 47) - 1));
            if id == 0 || m.contains_key(&id) {
                continue;
            }
            m.insert(
                id,
                Arc::new(tokio::sync::Mutex::new(Some(Cursor {
                    plan,
                    ns,
                    owner,
                    lsid,
                    batch_size,
                    no_timeout,
                    stream: None,
                    await_data: false,
                    last_used: Instant::now(),
                }))),
            );
            metrics::gauge!("sprig_mongo_cursors_open", "kind" => "find")
                .set(f64::from(u32::try_from(m.len()).unwrap_or(u32::MAX)));
            return Ok(id);
        }
    }

    /// Opens a change-stream cursor over `stream`; returns its id.
    ///
    /// # Errors
    /// As [`open`](Self::open).
    pub fn open_stream(
        &self,
        stream: super::changes::ChangeStream,
        ns: String,
        owner: Owner,
        lsid: Option<Vec<u8>>,
        batch_size: Option<usize>,
        max_per_tenant: usize,
    ) -> Result<i64, MongoError> {
        let mut plan = CursorPlan::from_docs(
            sprig_doc::engine::Ns {
                db: String::new(),
                coll: String::new(),
            },
            Vec::new(),
        );
        plan.exhausted = false;
        // Change-stream metrics.
        let scope = super::status::stream_scope(&ns);
        let id = self.open(plan, ns, owner, lsid, batch_size, false, max_per_tenant)?;
        super::status::stream_opened(id, scope);
        if let Some(c) = self.lock().get(&id).cloned()
            && let Ok(mut g) = c.try_lock()
            && let Some(cur) = g.as_mut()
        {
            cur.stream = Some(stream);
        }
        Ok(id)
    }

    /// Marks cursor `id` as a tailable `awaitData` cursor.
    pub fn set_await_data(&self, id: i64) {
        if let Some(c) = self.lock().get(&id).cloned()
            && let Ok(mut g) = c.try_lock()
            && let Some(cur) = g.as_mut()
        {
            cur.await_data = true;
        }
    }

    /// The cursor `id`, checked against `owner`.
    ///
    /// # Errors
    /// 43 `CursorNotFound`; 13 `Unauthorized` for another user's cursor.
    pub fn get(
        &self,
        id: i64,
        owner: &Owner,
    ) -> Result<Arc<tokio::sync::Mutex<Option<Cursor>>>, MongoError> {
        let c = self.lock().get(&id).cloned();
        let Some(c) = c else {
            let other = (id >> 47) != (self.gateway_bits >> 47);
            return Err(MongoError::new(
                code::CURSOR_NOT_FOUND,
                if other {
                    format!(
                        "cursor id {id} not found: it was opened on another gateway; connect with loadBalanced=true to keep cursors on one gateway"
                    )
                } else {
                    format!("cursor id {id} not found")
                },
            ));
        };
        if let Ok(g) = c.try_lock()
            && let Some(cur) = g.as_ref()
            && &cur.owner != owner
        {
            return Err(MongoError::new(
                code::UNAUTHORIZED,
                format!("cursor id {id} belongs to another user"),
            ));
        }
        Ok(c)
    }

    /// Marks a cursor used.
    pub fn touch(c: &mut Cursor) {
        c.last_used = Instant::now();
    }

    /// Removes a cursor. Returns whether it existed.
    pub fn remove(&self, id: i64) -> bool {
        let mut m = self.lock();
        let had = m.remove(&id).is_some();
        super::status::cursor_closed(id);
        metrics::gauge!("sprig_mongo_cursors_open", "kind" => "find")
            .set(f64::from(u32::try_from(m.len()).unwrap_or(u32::MAX)));
        had
    }

    /// Kills idle cursors older than `timeout`; returns how many.
    pub fn reap(&self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut m = self.lock();
        let dead: Vec<i64> = m
            .iter()
            .filter_map(|(id, c)| {
                let g = c.try_lock().ok()?;
                let cur = g.as_ref()?;
                (!cur.no_timeout && now.duration_since(cur.last_used) > timeout).then_some(*id)
            })
            .collect();
        for id in &dead {
            m.remove(id);
        }
        if !dead.is_empty() {
            metrics::counter!("sprig_mongo_cursor_timeouts_total", "kind" => "find")
                .increment(dead.len() as u64);
        }
        dead.len()
    }

    /// Kills the cursors of session `lsid`.
    pub fn kill_session(&self, lsid: &[u8]) {
        let mut m = self.lock();
        m.retain(|_, c| match c.try_lock() {
            Ok(g) => g
                .as_ref()
                .is_none_or(|cur| cur.lsid.as_deref() != Some(lsid)),
            Err(_) => true,
        });
    }

    /// The open cursors' ids (for change-stream metrics).
    #[must_use]
    pub fn ids(&self) -> Vec<i64> {
        self.lock().keys().copied().collect()
    }

    /// The open cursors (for `currentOp`'s change streams).
    #[must_use]
    pub fn entries(&self) -> Vec<(i64, Arc<tokio::sync::Mutex<Option<Cursor>>>)> {
        self.lock()
            .iter()
            .map(|(k, v)| (*k, Arc::clone(v)))
            .collect()
    }

    /// Open cursors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// No open cursors.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use sprig_doc::engine::{CursorPlan, Ns};

    use super::*;

    fn plan() -> CursorPlan {
        CursorPlan::from_docs(
            Ns {
                db: "d".into(),
                coll: "c".into(),
            },
            vec![doc! {"a": 1}],
        )
    }

    #[test]
    fn ids_owners_limits() {
        let t = CursorTable::new("gw-1");
        let alice = Owner {
            tenant: 1,
            user: "alice".into(),
        };
        let bob = Owner {
            tenant: 1,
            user: "bob".into(),
        };
        let id = t
            .open(plan(), "d.c".into(), alice.clone(), None, None, false, 2)
            .unwrap();
        assert!(id > 0);
        assert_eq!(id >> 47, t.gateway_bits >> 47);
        assert!(t.get(id, &alice).is_ok());
        assert_eq!(t.get(id, &bob).unwrap_err().code, code::UNAUTHORIZED);
        assert_eq!(
            t.get(id + 1, &alice).unwrap_err().code,
            code::CURSOR_NOT_FOUND
        );
        t.open(
            plan(),
            "d.c".into(),
            alice.clone(),
            Some(vec![1]),
            None,
            false,
            2,
        )
        .unwrap();
        assert_eq!(
            t.open(plan(), "d.c".into(), alice, None, None, false, 2)
                .unwrap_err()
                .code,
            code::SPRIG_CURSOR_LIMIT
        );
        t.kill_session(&[1]);
        assert_eq!(t.len(), 1);
        assert_eq!(t.reap(Duration::ZERO), 1);
        assert!(!t.remove(id));
    }
}
