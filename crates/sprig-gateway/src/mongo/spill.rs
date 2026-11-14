//! Spilling's settings and the command's `allowDiskUse`. The engine's side
//! is `sprig_doc::spill`.
//!
//! - `doc.agg_stage_memory_bytes`, `doc.max_sort_bytes` and
//!   `doc.graph_lookup_max_bytes` are read when the front end starts and
//!   again before each `find` and `aggregate`, so they apply live;
//! - `doc.spill_dir` names the directory, read at start and again before
//!   each command: a changed directory is set up for the next spill
//!   (files already open stay where they are);
//! - `doc.spill_max_bytes` and `doc.spill_tenant_max_bytes` cap the
//!   bytes on disk, read live with each `aggregate`;
//! - `doc.allow_disk_use_by_default` is MongoDB's
//!   `allowDiskUseByDefault`: an `aggregate` without `allowDiskUse` may
//!   spill.

use bson::{Bson, Document};
use sprig_common::ids::DcId;
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

use super::Text;

macro_rules! doc_key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: Scopes::PROCESS,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 31,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

doc_key!(
    /// `doc.agg_stage_memory_bytes`: memory per blocking stage.
    AGG_STAGE_MEMORY_BYTES: u64 = "doc.agg_stage_memory_bytes", 104_857_600, HardBounds::range(4096, 1_099_511_627_776),
    "Memory per blocking aggregation stage; past it the stage spills or answers 292"
);
doc_key!(
    /// `doc.max_sort_bytes`: `find`'s in-memory sort.
    MAX_SORT_BYTES: u64 = "doc.max_sort_bytes", 104_857_600, HardBounds::range(4096, 1_099_511_627_776),
    "In-memory sort limit"
);
doc_key!(
    /// `doc.graph_lookup_max_bytes`: one `$graphLookup` search's memory.
    GRAPH_LOOKUP_MAX_BYTES: u64 = "doc.graph_lookup_max_bytes", 104_857_600, HardBounds::range(4096, 1_099_511_627_776),
    "$graphLookup memory per input document"
);
doc_key!(
    /// `doc.spill_dir`: where stages spill; empty: the system temporary
    /// directory's `sprigdb-spill`.
    SPILL_DIR: Text = "doc.spill_dir", std::borrow::Cow::Borrowed(""), HardBounds::NONE,
    "Directory for spill files"
);
doc_key!(
    /// `doc.spill_max_bytes`: spill files on disk per gateway (0: none).
    SPILL_MAX_BYTES: u64 = "doc.spill_max_bytes", 10_737_418_240, HardBounds::range(0, 1_099_511_627_776 * 16),
    "Spill bytes on disk per gateway"
);
doc_key!(
    /// `doc.spill_tenant_max_bytes`: spill files on disk per tenant on one
    /// gateway (0: none).
    SPILL_TENANT_MAX_BYTES: u64 = "doc.spill_tenant_max_bytes", 4_294_967_296, HardBounds::range(0, 1_099_511_627_776 * 16),
    "Spill bytes on disk per tenant"
);

/// `doc.allow_disk_use_by_default`.
pub static ALLOW_DISK_USE_BY_DEFAULT: Setting<bool> = Setting {
    key: "doc.allow_disk_use_by_default",
    default: true,
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 31,
    doc: "aggregate may spill when allowDiskUse is not given (MongoDB's allowDiskUseByDefault)",
};
sprig_settings::register!(ALLOW_DISK_USE_BY_DEFAULT);

/// The engine's memory settings, read at start.
#[must_use]
pub fn engine_config(live: Option<&(LiveSettings, DcId)>) -> sprig_doc::engine::EngineConfig {
    let mut cfg = sprig_doc::engine::EngineConfig::default();
    if let Some((l, dc)) = live {
        let cx = SettingCtx::process(dc);
        cfg.agg_stage_bytes =
            usize::try_from(l.get(&AGG_STAGE_MEMORY_BYTES, &cx)).unwrap_or(cfg.agg_stage_bytes);
        cfg.max_sort_bytes =
            usize::try_from(l.get(&MAX_SORT_BYTES, &cx)).unwrap_or(cfg.max_sort_bytes);
        sprig_doc::agg::set_graph_lookup_max_bytes(
            usize::try_from(l.get(&GRAPH_LOOKUP_MAX_BYTES, &cx)).unwrap_or(usize::MAX),
        );
    }
    cfg
}

/// The gateway's id and the `doc.spill_dir` its spill directory was last
/// set up under, for [`follow`].
static CONFIGURED: std::sync::Mutex<Option<(String, String)>> = std::sync::Mutex::new(None);

/// Sets up this gateway's spill directory (removing those of gateways
/// that are gone).
pub fn start(live: Option<&(LiveSettings, DcId)>, gateway_id: &str) {
    let mut cfg = sprig_doc::spill::SpillConfig::default();
    let mut dir = String::new();
    if let Some((l, dc)) = live {
        let cx = SettingCtx::process(dc);
        dir = l.get(&SPILL_DIR, &cx).into_owned();
        if !dir.is_empty() {
            cfg.dir = std::path::PathBuf::from(&dir);
        }
        cfg.tenant_max_bytes = l.get(&SPILL_TENANT_MAX_BYTES, &cx);
        cfg.gateway_max_bytes = l.get(&SPILL_MAX_BYTES, &cx);
    }
    match sprig_doc::spill::configure(&cfg, gateway_id) {
        Ok(p) => tracing::info!(dir = %p.display(), "spill directory ready"),
        Err(e) => tracing::warn!(error = %e, "spill directory unavailable; spills will fail"),
    }
    if let Ok(mut c) = CONFIGURED.lock() {
        *c = Some((gateway_id.to_owned(), dir));
    }
}

/// Applies the current memory limits and, when `doc.spill_dir` changed,
/// sets up the new directory (these keys are declared live).
fn follow(l: &LiveSettings, cx: &SettingCtx<'_>) {
    let n = |s: &'static Setting<u64>| usize::try_from(l.get(s, cx)).unwrap_or(usize::MAX);
    sprig_doc::engine::set_memory_limits(n(&AGG_STAGE_MEMORY_BYTES), n(&MAX_SORT_BYTES));
    sprig_doc::agg::set_graph_lookup_max_bytes(n(&GRAPH_LOOKUP_MAX_BYTES));
    let dir = l.get(&SPILL_DIR, cx);
    let Ok(mut c) = CONFIGURED.lock() else {
        return;
    };
    let Some((gateway, last)) = c.as_mut() else {
        return;
    };
    if *last == dir {
        return;
    }
    let cfg = sprig_doc::spill::SpillConfig {
        dir: if dir.is_empty() {
            sprig_doc::spill::SpillConfig::default().dir
        } else {
            std::path::PathBuf::from(dir.as_ref())
        },
        tenant_max_bytes: l.get(&SPILL_TENANT_MAX_BYTES, cx),
        gateway_max_bytes: l.get(&SPILL_MAX_BYTES, cx),
    };
    match sprig_doc::spill::configure(&cfg, gateway) {
        Ok(p) => tracing::info!(dir = %p.display(), "spill directory changed"),
        Err(e) => tracing::warn!(error = %e, "new spill directory unavailable; spills will fail"),
    }
    *last = dir.into_owned();
}

/// The command's spilling context: its `allowDiskUse` (else the default
/// setting) and tenant; the caps refreshed from the live settings.
///
/// # Errors
/// 14 for an `allowDiskUse` that isn't a boolean.
pub fn ctx_for(
    body: &Document,
    cmd: &str,
    tenant: u64,
    live: Option<&(LiveSettings, DcId)>,
) -> Result<sprig_doc::spill::SpillCtx, MongoError> {
    let default = live.is_none_or(|(l, dc)| {
        let cx = SettingCtx::process(dc);
        follow(l, &cx);
        sprig_doc::spill::set_caps(
            l.get(&SPILL_TENANT_MAX_BYTES, &cx),
            l.get(&SPILL_MAX_BYTES, &cx),
        );
        l.get(&ALLOW_DISK_USE_BY_DEFAULT, &cx)
    });
    let allow = match body.get("allowDiskUse") {
        None | Some(Bson::Null) => default,
        Some(Bson::Boolean(b)) => {
            if !*b {
                sprig_doc::spill::note_disallowed();
            }
            *b
        }
        // `find` words it differently (MongoDB 8.0).
        Some(other) if cmd == "find" => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                format!(
                    "Field 'allowDiskUse' should be a boolean value, but found: {}",
                    sprig_doc::update::type_name(other)
                ),
            ));
        }
        Some(other) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field '{cmd}.allowDiskUse' is the wrong type '{}', expected type 'bool'",
                    sprig_doc::update::type_name(other)
                ),
            ));
        }
    };
    Ok(sprig_doc::spill::SpillCtx::new(allow, tenant))
}

/// MongoDB 8.2's spill fields for one pipeline stage, if it spilled.
#[must_use]
pub fn stage_fields(stats: &[sprig_doc::spill::StageSpill], stage: usize) -> Document {
    let s = stats.iter().find(|s| s.stage == stage);
    let n = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
    bson::doc! {
        "usedDisk": s.is_some(),
        "spills": n(s.map_or(0, |s| s.spills)),
        "spilledBytes": n(s.map_or(0, |s| s.bytes)),
        "spilledRecords": n(s.map_or(0, |s| s.records)),
        "spilledDataStorageSize": n(s.map_or(0, |s| s.storage)),
    }
}
