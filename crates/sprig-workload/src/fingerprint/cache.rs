//! The SQL fingerprinter: normalizer, digest and a per-gateway cache of
//! known fingerprints with their shapes.
//!
//! Shapes cost a parse (tens of microseconds), so each is extracted once,
//! on the first sight of its fingerprint on this gateway, and shared by
//! every later execution. The cache also detects collisions (chapter 12):
//! the same id with a different text gets the id of seed 1 and the
//! `collision` flag.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex, OnceLock};

use sprig_common::ids::TenantId;

use super::{
    Fingerprint, FingerprintId, FpFlags, FpKind, Reserved, SqlCtx, digest_seeded,
    normalize::normalize_with,
};
use crate::shape::{ExtractCtx, Shape, StmtType, TableResolver, extract};

/// A fingerprint this gateway has seen: what every execution shares.
#[derive(Debug)]
struct Known {
    text: Arc<str>,
    shape: Arc<Shape>,
    flags: FpFlags,
}

/// Number of cache shards (a power of two).
const SHARDS: usize = 64;

/// The cache's default capacity, in fingerprints for all tenants.
pub const DEFAULT_CACHE_ENTRIES: usize = 200_000;

type Shard = Mutex<HashMap<(TenantId, u64), Arc<Known>>>;

/// Fingerprints SQL statements (chapter 12, `Fingerprinter::sql`). One per
/// gateway, shared by every session.
pub struct SqlFingerprinter {
    shards: Box<[Shard]>,
    per_shard: usize,
    hasher: std::hash::RandomState,
}

impl std::fmt::Debug for SqlFingerprinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlFingerprinter")
            .field("per_shard", &self.per_shard)
            .finish_non_exhaustive()
    }
}

impl Default for SqlFingerprinter {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_ENTRIES)
    }
}

fn empty_shape(t: StmtType) -> Arc<Shape> {
    static EMPTY: OnceLock<Arc<Shape>> = OnceLock::new();
    if t == StmtType::Other {
        Arc::clone(EMPTY.get_or_init(|| Arc::new(Shape::empty(StmtType::Other))))
    } else {
        Arc::new(Shape::empty(t))
    }
}

impl SqlFingerprinter {
    /// A fingerprinter whose cache keeps at most `entries` fingerprints.
    #[must_use]
    pub fn new(entries: usize) -> Self {
        let shards = (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect();
        Self {
            shards,
            per_shard: (entries / SHARDS).max(16),
            hasher: std::hash::RandomState::new(),
        }
    }

    fn shard(&self, key: &(TenantId, u64)) -> &Shard {
        let i = usize::try_from(self.hasher.hash_one(key) % SHARDS as u64).unwrap_or(0);
        &self.shards[i]
    }

    /// Forgets a tenant's shapes, after its catalog changed (a table was
    /// created, dropped or altered), so they are extracted again with the
    /// new names and ids.
    pub fn invalidate_tenant(&self, tenant: TenantId) {
        for s in &*self.shards {
            if let Ok(mut m) = s.lock() {
                m.retain(|k, _| k.0 != tenant);
            }
        }
    }

    /// How many fingerprints the cache holds.
    #[must_use]
    pub fn cached(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().map(|m| m.len()).unwrap_or(0))
            .sum()
    }

    /// Fingerprints one statement of `tenant` (chapter 12, "SQL
    /// fingerprints"). `sql` is the statement as the client sent it, with
    /// logical names. Never fails: a statement that can't be lexed gets the
    /// reserved "unparsed" fingerprint.
    pub fn sql(
        &self,
        tenant: TenantId,
        sql: &[u8],
        ctx: &SqlCtx<'_>,
        resolver: &dyn TableResolver,
    ) -> Fingerprint {
        let norm = normalize_with(
            sql,
            &ctx.lex,
            ctx.current_db,
            ctx.lower_case_table_names,
            false,
        );
        let caller_fn = ctx.caller.map(|c| c.function_id);
        if norm.unparsed {
            let flags = FpFlags {
                unparsed: true,
                ..FpFlags::default()
            };
            return Fingerprint {
                id: Reserved::Unparsed.id(FpKind::Sql),
                kind: FpKind::Sql,
                text: Arc::from(Reserved::Unparsed.text()),
                shape: empty_shape(StmtType::Other),
                literals: Vec::new(),
                front_end: ctx.front_end,
                caller: ctx.caller,
                db_id: ctx.db_id,
                flags,
                in_list_max: 0,
                list_slots: Vec::new(),
                sensitive: norm.sensitive,
            };
        }
        let (stored, truncated) = norm.stored_text(ctx.max_text_bytes);
        let mut id = digest_seeded(FpKind::Sql, ctx.db_id, caller_fn, &norm.text, 0);
        let mut known = self.lookup(tenant, id, stored);
        let mut collision = false;
        if let Some(Err(())) = known {
            // Same id, other text: the second text takes seed 1.
            collision = true;
            id = digest_seeded(FpKind::Sql, ctx.db_id, caller_fn, &norm.text, 1);
            known = self.lookup(tenant, id, stored);
        }
        let known = if let Some(Ok(k)) = known {
            k
        } else {
            let full = normalize_with(
                sql,
                &ctx.lex,
                ctx.current_db,
                ctx.lower_case_table_names,
                true,
            );
            let ectx = ExtractCtx {
                lex: ctx.lex,
                current_db: ctx.current_db,
                lower_case_table_names: ctx.lower_case_table_names,
            };
            let shape = extract(sql, &full, &ectx, resolver);
            let k = Arc::new(Known {
                text: Arc::from(stored),
                shape: Arc::new(shape),
                flags: FpFlags {
                    truncated,
                    collision,
                    ..FpFlags::default()
                },
            });
            self.insert(tenant, id, &k);
            k
        };
        Fingerprint {
            id,
            kind: FpKind::Sql,
            text: Arc::clone(&known.text),
            shape: Arc::clone(&known.shape),
            literals: norm.literals,
            front_end: ctx.front_end,
            caller: ctx.caller,
            db_id: ctx.db_id,
            flags: known.flags,
            in_list_max: norm.in_list_max,
            list_slots: norm.list_slots,
            sensitive: norm.sensitive,
        }
    }

    /// `Some(Ok)` when known with this text, `Some(Err)` when known with
    /// another text (a collision), `None` when unknown.
    fn lookup(
        &self,
        tenant: TenantId,
        id: FingerprintId,
        text: &str,
    ) -> Option<Result<Arc<Known>, ()>> {
        let key = (tenant, id.0);
        let m = self.shard(&key).lock().ok()?;
        let k = m.get(&key)?;
        Some(if &*k.text == text {
            Ok(Arc::clone(k))
        } else {
            Err(())
        })
    }

    fn insert(&self, tenant: TenantId, id: FingerprintId, k: &Arc<Known>) {
        let key = (tenant, id.0);
        let Ok(mut m) = self.shard(&key).lock() else {
            return;
        };
        if m.len() >= self.per_shard {
            // Full: drop an eighth of the shard, in hash order (arbitrary,
            // so no tenant can keep the others out).
            let drop: Vec<(TenantId, u64)> =
                m.keys().take(self.per_shard / 8 + 1).copied().collect();
            for d in drop {
                m.remove(&d);
            }
        }
        m.insert(key, Arc::clone(k));
    }
}
