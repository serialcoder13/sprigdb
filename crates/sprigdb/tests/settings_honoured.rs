//! Every registered setting is honoured as declared, as far as the source
//! shows it mechanically (bugfix "setfix", 13 Nov 2026):
//!
//! - **read**: something besides its declaration and registration names
//!   the setting's static or its key, so a value someone sets does
//!   something;
//! - **tenant scope**: a key that tenants (or their ancestors, databases
//!   or objects) may set is read somewhere with a tenant context
//!   (`for_tenant`, a catalog's `setting_ctx` or `object_ctx`, a tenant's
//!   `TenantSettings`), not only at the process scopes, where a tenant's
//!   value is never seen.
//!
//! The keys come from the server's own registry (`sprigdb docs-settings`);
//! the reads from the workspace's non-test sources. Each check is a
//! heuristic over files, so it can miss a gap (a file that resolves one key
//! per tenant and another only per process passes both), but what it flags
//! is real. Known gaps are listed below with what was decided; the test
//! fails on a new gap and on a listed gap that is gone (remove it from the
//! list). Apply modes (`live` read only at start) can't be told from the
//! text; they are covered by behaviour tests where they were fixed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use sprig_docs::reference::SettingDoc;

/// Registered keys nothing reads yet (setfix's audit, 13 Nov 2026;
/// decisions/bugfix-interim.md): registered ahead of their code, kept for
/// the design's sake until each is built or unregistered.
const KNOWN_UNREAD: &[(&str, &str)] = &[
    (
        "search.max_lag_ms",
        "copy states (stale after lag) not built",
    ),
    ("queue.descendant_publish", "AMQP publishing up not built"),
    ("queue.descendant_key", "AMQP publishing up not built"),
    (
        "sink.kafka.offsets_topic",
        "read only through the sink.* option merge, by key",
    ),
    ("amqp.cert_login_field", "registered ahead of its code"),
    (
        "auto_index.field_column_class_pct",
        "registered ahead of its code",
    ),
    (
        "auto_index.lookup_scatter_pct",
        "registered ahead of its code",
    ),
    (
        "auto_index.wildcard_recommend_paths",
        "registered ahead of its code",
    ),
    ("decisions.retention_days", "registered ahead of its code"),
    ("failover.quarantine_days", "registered ahead of its code"),
    ("graph.traversal_tier", "registered ahead of its code"),
    ("inherit.instance_precreate", "registered ahead of its code"),
    ("inherit.precreate_rate", "registered ahead of its code"),
    (
        "inherit.sync_orphan_max_rows",
        "registered ahead of its code",
    ),
    ("mover.max_pause_minutes", "registered ahead of its code"),
    ("mover.recovery_rate_pct", "registered ahead of its code"),
    (
        "mover.throttle_p99_increase_pct",
        "registered ahead of its code",
    ),
    ("mover.verify_mode", "registered ahead of its code"),
    ("mover.xa_wait_ms", "registered ahead of its code"),
    (
        "recovery.max_latency_increase_pct",
        "registered ahead of its code",
    ),
    (
        "recovery.rebuild_threshold_gb",
        "registered ahead of its code",
    ),
    ("recovery.step_max_lag_ms", "registered ahead of its code"),
    ("sink.platform_secret_dir", "registered ahead of its code"),
    ("stream.sequencer_batch_ms", "registered ahead of its code"),
    (
        "tenant.ancestry_cache_seconds",
        "registered ahead of its code",
    ),
    (
        "tenant.copy_max_descendants",
        "registered ahead of its code",
    ),
];

/// Keys tenants may set whose reading files show no tenant context: real
/// gaps (read at dc and global only, so a tenant's value does nothing), and
/// keys read per tenant in a way the scan can't see.
const KNOWN_TENANT_GAPS: &[(&str, &str)] = &[
    ("amqp.heartbeat_seconds", "gap: read at dc and global only"),
    ("amqp.sasl_mechanisms", "gap: read at dc and global only"),
    ("amqp10.legacy_addresses", "gap: read at dc and global only"),
    ("amqp10.link_credit", "gap: read at dc and global only"),
    (
        "api.stream.heartbeat_seconds",
        "gap: read at dc and global only",
    ),
    (
        "api.stream.max_per_tenant",
        "gap: read at dc and global only",
    ),
    ("api.txn.idle_timeout_ms", "gap: read at dc and global only"),
    ("api.txn.max_lifetime_ms", "gap: read at dc and global only"),
    (
        "api.txn.max_open_per_tenant",
        "gap: read at dc and global only",
    ),
    ("audit.retention_days", "gap: read at dc and global only"),
    ("catalog.ddl_wait_ms", "gap: read at dc and global only"),
    (
        "control.tenant_jobs_per_hour",
        "gap: read at dc and global only",
    ),
    (
        "gateway.max_allowed_packet_bytes",
        "gap: read at dc and global only",
    ),
    (
        "gateway.max_prepared_per_session",
        "gap: read at dc and global only",
    ),
    (
        "kafka.group_session_timeout_max_ms",
        "gap: read at dc and global only",
    ),
    ("kafka.sasl_mechanisms", "gap: read at dc and global only"),
    ("studio.map_tile_url", "gap: read at dc and global only"),
    (
        "telemetry.trace_sample_ratio",
        "gap: read at dc and global only",
    ),
    ("telemetry.trace_slow_ms", "gap: read at dc and global only"),
    (
        "workload.max_in_list_values",
        "gap: read at dc and global only",
    ),
    (
        "workload.max_samples_per_fingerprint",
        "gap: read at dc and global only",
    ),
    ("workload.max_text_bytes", "gap: read at dc and global only"),
    (
        "workload.sample_retention_days",
        "gap: read at dc and global only",
    ),
    (
        "sink.auto_resume",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.batch_max_bytes",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.batch_max_records",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.batch_max_wait_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.credential_previous_hours",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.db.batch_rows",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.db.seen_retention_hours",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.dead_letter_retention_hours",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.fanin_instances_per_task",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.fanin_refresh_seconds",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.http_connect_timeout_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.http_timeout_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.kafka.transaction_timeout_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.lag_alert_seconds",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.lanes",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.max_inflight_bytes",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.max_per_tenant",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.partitions_per_task",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.pause_after_minutes",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.queue_lease_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.record_max_attempts",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.retry_initial_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.s3.file_max_age_seconds",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.s3.file_max_bytes",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.s3.file_max_records",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.script_budget_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.script_db_budget_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.script_db_max_queries",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.script_db_max_rows",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.script_max_fetches",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.tenant_bytes_per_sec",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "sink.transform_budget_ms",
        "not a gap: the sink.* option merge reads the tenant's rows",
    ),
    (
        "auth.password_history",
        "not a gap: SettingsPolicy resolves per tenant",
    ),
    (
        "auth.password_lock_minutes",
        "not a gap: SettingsPolicy resolves per tenant",
    ),
    (
        "auth.password_policy",
        "not a gap: SettingsPolicy resolves per tenant",
    ),
    (
        "auth.password_require_current",
        "not a gap: SettingsPolicy resolves per tenant",
    ),
    (
        "auth.password_reuse_interval_days",
        "not a gap: SettingsPolicy resolves per tenant",
    ),
    (
        "approvals.digest_hour_utc",
        "not a gap: approvals resolve per tenant",
    ),
    (
        "approvals.reminder",
        "not a gap: approvals resolve per tenant",
    ),
    (
        "approvals.stream_database",
        "not a gap: approvals resolve per tenant",
    ),
    (
        "approvals.webhook_connection",
        "not a gap: approvals resolve per tenant",
    ),
    (
        "failover.standby_mode",
        "not a gap: approvals resolve per tenant",
    ),
    (
        "graph.auto_partition",
        "not a gap: approvals resolve per tenant",
    ),
    (
        "tenant.ancestry_mode",
        "not a gap: read from the tenant's own rows",
    ),
];

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A source file's non-test text: test modules, test files and the
/// documentation crates are left out.
fn sources() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name().to_string_lossy().into_owned();
            if p.is_dir() {
                if !matches!(name.as_str(), "tests" | "benches" | "examples" | "target") {
                    walk(&p, out);
                }
            } else if p.extension().is_some_and(|e| e == "rs")
                && !(name == "tests.rs" || name.ends_with("_tests.rs") || name.starts_with("test_"))
            {
                out.push(p);
            }
        }
    }
    let mut files = Vec::new();
    let crates = workspace().join("crates");
    for e in std::fs::read_dir(&crates).expect("crates/").flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with("sprig-docs") || name == "sprig-testkit" {
            continue;
        }
        walk(&e.path().join("src"), &mut files);
    }
    files
        .into_iter()
        .filter_map(|p| {
            let t = std::fs::read_to_string(&p).ok()?;
            // A file's own inline test module goes last by convention
            // (`#[cfg(test)] mod tests;` declarations, often near the top,
            // are skipped: their files are left out above).
            let cut = t
                .match_indices("#[cfg(test)]\n")
                .map(|(i, m)| i + m.len())
                .find(|&at| {
                    let line = t[at..].lines().next().unwrap_or("");
                    line.trim_start_matches("pub ").starts_with("mod ")
                        && line.trim_end().ends_with('{')
                })
                .unwrap_or(t.len());
            Some((p, t[..cut].to_owned()))
        })
        .collect()
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// Positions of `word` in `text` as a whole identifier.
fn find_word(text: &str, word: &str) -> Vec<usize> {
    let b = text.as_bytes();
    text.match_indices(word)
        .map(|(i, _)| i)
        .filter(|&i| {
            (i == 0 || !is_ident(b[i - 1])) && b.get(i + word.len()).is_none_or(|c| !is_ident(*c))
        })
        .collect()
}

/// The static a key literal declares: the nearest `ALL_CAPS` identifier
/// before it (`static NAME: Setting<..> = Setting { key: "k"`,
/// `key!(NAME, "k", ..)`, `NAME: u32 = "k", ..`).
fn ident_before(text: &str, at: usize) -> Option<String> {
    let b = text.as_bytes();
    let start = at.saturating_sub(400);
    let mut i = at;
    while i > start {
        i -= 1;
        if is_ident(b[i]) && (i + 1 >= b.len() || !is_ident(b[i + 1])) {
            let end = i + 1;
            let mut s = i;
            while s > 0 && is_ident(b[s - 1]) {
                s -= 1;
            }
            let w = &text[s..end];
            let caps = w.len() > 1
                && w.bytes().next().is_some_and(|c| c.is_ascii_uppercase())
                && w.bytes()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_');
            if caps {
                return Some(w.to_owned());
            }
            i = s;
        }
    }
    None
}

/// Whether the text resolves settings for a tenant somewhere.
fn tenant_context(text: &str) -> bool {
    [
        "for_tenant(",
        "setting_ctx(",
        "object_ctx(",
        "TenantSettings",
        "cat.settings",
        ".settings)",
        "watch_resolved(",
        "tenant_get(",
        "OwnedCtx",
        // A resolver handed a getter: the caller picks the context, which
        // this scan can't follow (not counted as a gap).
        "impl Fn(&",
        "dyn Fn(&",
        "impl Fn(&'static",
        "SettingCtx<'_>",
    ]
    .iter()
    .any(|m| text.contains(m))
}

struct Decl {
    file: usize,
    ident: String,
    at: usize,
}

#[test]
#[allow(clippy::too_many_lines)] // one scan, then its two checks
fn registered_settings_are_honoured_as_declared() {
    let out = Command::new(env!("CARGO_BIN_EXE_sprigdb"))
        .arg("docs-settings")
        .output()
        .expect("run sprigdb docs-settings");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let docs: Vec<SettingDoc> = serde_json::from_slice(&out.stdout).expect("settings JSON");
    assert!(docs.len() > 300, "{} keys", docs.len());
    let src = sources();

    // Where each key is declared.
    let mut decls: BTreeMap<&str, Decl> = BTreeMap::new();
    for d in &docs {
        let lit = format!("\"{}\"", d.key);
        'files: for (fi, (_, t)) in src.iter().enumerate() {
            if !t.contains("register!(") {
                continue;
            }
            for (at, _) in t.match_indices(&lit) {
                if let Some(ident) = ident_before(t, at)
                    && (t.contains(&format!("register!({ident})")) || t.contains("register!($"))
                {
                    decls.insert(
                        &d.key,
                        Decl {
                            file: fi,
                            ident,
                            at,
                        },
                    );
                    break 'files;
                }
            }
        }
    }

    let mut unread = BTreeSet::new();
    let mut tenant_gaps = BTreeSet::new();
    for d in &docs {
        let Some(decl) = decls.get(d.key.as_str()) else {
            // Declared in a way this scan doesn't read (counted below).
            continue;
        };
        let lit = format!("\"{}\"", d.key);
        // Files that name the setting other than to declare it.
        let mut readers = Vec::new();
        for (fi, (_, t)) in src.iter().enumerate() {
            let mut n = 0;
            for at in find_word(t, &decl.ident) {
                let before = &t[at.saturating_sub(10)..at];
                let declared_here = fi == decl.file
                    && (before.ends_with("static ") || before.ends_with("register!("));
                // The macro argument that declares it.
                let macro_arg = fi == decl.file && at < decl.at && decl.at - at < 400;
                if !(declared_here || macro_arg) {
                    n += 1;
                }
            }
            for (at, _) in t.match_indices(&lit) {
                if !(fi == decl.file && at == decl.at) {
                    n += 1;
                }
            }
            if n > 0 {
                readers.push(fi);
            }
        }
        if readers.is_empty() {
            unread.insert(d.key.clone());
            continue;
        }
        let tenant_settable = d
            .scopes
            .iter()
            .any(|s| matches!(s.as_str(), "tenant" | "database" | "object"));
        if tenant_settable && !readers.iter().any(|&fi| tenant_context(&src[fi].1)) {
            tenant_gaps.insert(d.key.clone());
        }
    }

    let undeclared: Vec<&str> = docs
        .iter()
        .map(|d| d.key.as_str())
        .filter(|k| !decls.contains_key(k))
        .collect();
    assert!(
        undeclared.len() * 8 < docs.len(),
        "the scan found no declaration for {} keys (is the declaration form new?): {undeclared:?}",
        undeclared.len()
    );

    let check = |what: &str, found: &BTreeSet<String>, known: &[(&str, &str)]| {
        let known: BTreeSet<&str> = known.iter().map(|(k, _)| *k).collect();
        let new: Vec<&String> = found
            .iter()
            .filter(|k| !known.contains(k.as_str()))
            .collect();
        let gone: Vec<&&str> = known.iter().filter(|k| !found.contains(**k)).collect();
        assert!(
            new.is_empty() && gone.is_empty(),
            "{what}: new {new:?} (read them as declared, or narrow the declaration); \
             no longer a gap {gone:?} (remove them from the list)"
        );
    };
    check("unread settings", &unread, KNOWN_UNREAD);
    check(
        "tenant-settable settings read only at process scope",
        &tenant_gaps,
        KNOWN_TENANT_GAPS,
    );
}
