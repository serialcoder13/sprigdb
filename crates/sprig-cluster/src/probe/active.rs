//! Active probes: statements whose success is the fact (chapter 09, "XA
//! round trip", "Features", "PL/SQL routines", "Privileges"). They run on
//! one writable node, at registration, after a version change, daily and on
//! `sprigctl cluster probe --full`.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::ids::JobId;

use crate::sql::{NodeEndpoint, ProbeError, ProbeSession, literal};
use crate::sys;

/// Feature flags (chapter 09, "Capability profile"). Each is set by one
/// statement's success, except `alter_two_phase` and `performance_schema`,
/// which come from variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct FeatureFlags {
    /// `KEY ... IGNORED`.
    pub ignored_indexes: bool,
    /// `ALTER INDEX ... NOT IGNORED, ALGORITHM=INSTANT`.
    pub instant_ignored_toggle: bool,
    /// `VECTOR` columns with a vector index.
    pub vector: bool,
    /// `ADD SPATIAL INDEX ... ALGORITHM=INPLACE, LOCK=NONE`.
    pub online_spatial_index: bool,
    /// `binlog_alter_two_phase` exists.
    pub alter_two_phase: bool,
    /// `performance_schema` is on.
    pub performance_schema: bool,
    /// `JSON_SCHEMA_VALID` rejects an invalid and accepts a valid document.
    pub json_schema_valid: bool,
    /// `JSON_TABLE`.
    pub json_table: bool,
    /// An index on a virtual generated column.
    pub vcol_index: bool,
    /// ... built online.
    pub vcol_index_online: bool,
    /// A package compiled under `sql_mode=ORACLE` returns the right value.
    pub oracle_routines: bool,
    /// ... and is callable from a session not in `ORACLE` mode
    /// (chapter 09, open question 5).
    pub oracle_routines_any_mode: bool,
    /// The probing account can create a trigger that fires. With the binary
    /// log on, MariaDB refuses `CREATE TRIGGER` without `SUPER` unless
    /// `log_bin_trust_function_creators = ON` (1419; PL/SQL spike).
    #[serde(default)]
    pub trigger_create: bool,
}

/// What the active probes found.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ActiveProbes {
    /// `__sprig_sys` and its tables exist (created if needed).
    pub sys_schema: bool,
    /// The XA round trip passed: prepare, disconnect, recover, commit; and
    /// prepare, disconnect, recover, roll back.
    pub xa: bool,
    /// Why XA failed, for the registration report.
    pub xa_detail: Option<String>,
    /// Feature statements.
    pub features: FeatureFlags,
    /// `SET SESSION sql_log_bin` worked.
    pub sql_log_bin_settable: bool,
    /// Galera only: `SET SESSION wsrep_OSU_method` worked.
    pub osu_method_settable: Option<bool>,
    /// The binlog dump probe ([`super::dump`]), run as `sprig_repl` on the
    /// same node. `None`: not run (no `sprig_repl` endpoint was given).
    #[serde(default)]
    pub binlog_dump: Option<super::dump::DumpProbe>,
}

/// Runs every active probe against `ep`.
///
/// # Errors
/// Only for failures that say nothing about capabilities: a lost connection
/// or a timeout. A refused statement just clears its flag.
pub async fn run(
    ep: &NodeEndpoint,
    timeout: Duration,
    galera: bool,
) -> Result<ActiveProbes, ProbeError> {
    let mut out = ActiveProbes::default();
    // Each run names its own scratch objects and probe row, so concurrent
    // probers don't collide.
    #[allow(clippy::cast_possible_truncation)]
    let tag = nonce() as u32;
    let mut s = ProbeSession::connect(ep, timeout).await?;
    out.sys_schema = ok(sys::ensure(&mut s).await)?;

    out.sql_log_bin_settable = ok(s.exec("SET SESSION sql_log_bin = 0").await)?
        && ok(s.exec("SET SESSION sql_log_bin = 1").await)?;
    if galera {
        out.osu_method_settable = Some(
            ok(s.exec("SET SESSION wsrep_OSU_method = 'RSU'").await)?
                && ok(s.exec("SET SESSION wsrep_OSU_method = 'TOI'").await)?,
        );
    }
    if out.sys_schema {
        out.features = features(&mut s, tag).await?;
    }
    s.close().await;

    if out.sys_schema {
        let (routines, any_mode) = oracle_routines(ep, timeout, tag).await?;
        out.features.oracle_routines = routines;
        out.features.oracle_routines_any_mode = any_mode;
        out.features.trigger_create = trigger_create(ep, timeout, tag).await?;
        match xa_round_trip(ep, timeout, tag).await? {
            Ok(()) => out.xa = true,
            Err(why) => out.xa_detail = Some(why),
        }
    } else {
        out.xa_detail = Some("__sprig_sys could not be created".to_owned());
    }
    Ok(out)
}

/// `Ok(true)` if the statement ran, `Ok(false)` if the server refused it,
/// `Err` for anything else.
fn ok(r: Result<(), ProbeError>) -> Result<bool, ProbeError> {
    match r {
        Ok(()) => Ok(true),
        Err(e) if e.is_server_error() => {
            tracing::debug!(error = %e, "probe statement refused");
            Ok(false)
        }
        Err(e) => Err(e),
    }
}

/// Like [`ok`] for a query: `Ok(None)` if refused.
fn ok_val<T>(r: Result<T, ProbeError>) -> Result<Option<T>, ProbeError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.is_server_error() => {
            tracing::debug!(error = %e, "probe query refused");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

async fn features(s: &mut ProbeSession, tag: u32) -> Result<FeatureFlags, ProbeError> {
    let name = |k: &str| sys::scratch(&format!("{k}_{tag:08x}"));
    let (f, j, v) = (name("f"), name("j"), name("v"));
    let drop = format!("DROP TABLE IF EXISTS {f}, {j}, {v}");
    ok(s.exec(&drop).await)?;
    let mut flags = FeatureFlags {
        ignored_indexes: ok(s
            .exec(&format!(
                "CREATE TABLE {f} (id INT PRIMARY KEY, a INT, g POINT NOT NULL, \
                 KEY ka (a) IGNORED) ENGINE=InnoDB"
            ))
            .await)?,
        ..FeatureFlags::default()
    };
    if flags.ignored_indexes {
        flags.instant_ignored_toggle = ok(s
            .exec(&format!(
                "ALTER TABLE {f} ALTER INDEX ka NOT IGNORED, ALGORITHM=INSTANT"
            ))
            .await)?;
        flags.online_spatial_index = ok(s
            .exec(&format!(
                "ALTER TABLE {f} ADD SPATIAL INDEX sg (g), ALGORITHM=INPLACE, LOCK=NONE"
            ))
            .await)?;
    }
    let invalid = ok_val(
        s.scalar(r#"SELECT JSON_SCHEMA_VALID('{"type":"string"}', '1')"#)
            .await,
    )?;
    let valid = ok_val(
        s.scalar(r#"SELECT JSON_SCHEMA_VALID('{"type":"string"}', '"a"')"#)
            .await,
    )?;
    flags.json_schema_valid =
        invalid.flatten().as_deref() == Some("0") && valid.flatten().as_deref() == Some("1");
    let jt = ok_val(
        s.rows("SELECT x FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS (x INT PATH '$')) AS j")
            .await,
    )?;
    flags.json_table = jt.is_some_and(|rows| {
        let xs: Vec<Option<String>> = rows
            .into_iter()
            .map(|r| r.into_iter().next().and_then(|(_, v)| v))
            .collect();
        xs == [Some("1".to_owned()), Some("2".to_owned())]
    });
    if ok(s
        .exec(&format!(
            "CREATE TABLE {j} (id INT PRIMARY KEY, doc JSON NOT NULL, \
             s VARCHAR(64) AS (JSON_VALUE(doc, '$.s')) VIRTUAL INVISIBLE) ENGINE=InnoDB"
        ))
        .await)?
    {
        flags.vcol_index_online = ok(s
            .exec(&format!(
                "ALTER TABLE {j} ADD KEY ks (s), ALGORITHM=INPLACE, LOCK=NONE"
            ))
            .await)?;
        flags.vcol_index = flags.vcol_index_online
            || ok(s.exec(&format!("ALTER TABLE {j} ADD KEY ks (s)")).await)?;
    }
    flags.vector = ok(s
        .exec(&format!(
            "CREATE TABLE {v} (id INT PRIMARY KEY, e VECTOR(3) NOT NULL, \
             VECTOR INDEX (e)) ENGINE=InnoDB"
        ))
        .await)?;
    ok(s.exec(&drop).await)?;
    Ok(flags)
}

/// The package probe; returns (works, callable outside `ORACLE` mode).
async fn oracle_routines(
    ep: &NodeEndpoint,
    timeout: Duration,
    tag: u32,
) -> Result<(bool, bool), ProbeError> {
    let pkg = format!("__sprig_sys.sprig_probe_pkg_{tag:08x}");
    let pkg = pkg.as_str();
    let mut s = ProbeSession::connect(ep, timeout).await?;
    let mut works = ok(s.exec("SET SESSION sql_mode = 'ORACLE'").await)?
        && ok(s
            .exec(&format!(
                "CREATE OR REPLACE PACKAGE {pkg} AS FUNCTION f(a IN NUMBER) RETURN NUMBER; END"
            ))
            .await)?
        && ok(s
            .exec(&format!(
                "CREATE OR REPLACE PACKAGE BODY {pkg} AS \
                 FUNCTION f(a IN NUMBER) RETURN NUMBER AS v NUMBER := 0; \
                 BEGIN FOR i IN 1 .. a LOOP v := v + i; END LOOP; RETURN v; \
                 EXCEPTION WHEN OTHERS THEN RETURN -1; END; END"
            ))
            .await)?;
    if works {
        let v = ok_val(s.scalar(&format!("SELECT {pkg}.f(4)")).await)?.flatten();
        works = v.as_deref() == Some("10");
    }
    let mut any_mode = false;
    if works && ok(s.exec("SET SESSION sql_mode = DEFAULT").await)? {
        let v = ok_val(s.scalar(&format!("SELECT {pkg}.f(4)")).await)?.flatten();
        any_mode = v.as_deref() == Some("10");
    }
    // Probe routines are dropped after the probe (chapter 09, Security).
    ok(s.exec("SET SESSION sql_mode = 'ORACLE'").await)?;
    ok(s.exec(&format!("DROP PACKAGE IF EXISTS {pkg}")).await)?;
    s.close().await;
    Ok((works, any_mode))
}

/// The trigger probe (chapter 09, "PL/SQL routines"): an `ORACLE`-mode
/// `BEFORE INSERT` trigger must be accepted and fill its column.
async fn trigger_create(
    ep: &NodeEndpoint,
    timeout: Duration,
    tag: u32,
) -> Result<bool, ProbeError> {
    let t = format!("__sprig_sys.__sprig_probe_t_{tag:08x}");
    let t = t.as_str();
    let mut s = ProbeSession::connect(ep, timeout).await?;
    let mut works = ok(s.exec("SET SESSION sql_mode = 'ORACLE'").await)?
        && ok(s
            .exec(&format!("CREATE TABLE {t} (id INT PRIMARY KEY, a INT)"))
            .await)?
        && ok(s
            .exec(&format!(
                "CREATE OR REPLACE TRIGGER {t}_bi BEFORE INSERT ON {t} FOR EACH ROW \
                 BEGIN :NEW.a := :NEW.id * 2; END"
            ))
            .await)?
        && ok(s.exec(&format!("INSERT INTO {t} (id) VALUES (5)")).await)?;
    if works {
        let v = ok_val(s.scalar(&format!("SELECT a FROM {t} WHERE id = 5")).await)?.flatten();
        works = v.as_deref() == Some("10");
    }
    // Dropping the table drops its trigger (chapter 09, Security).
    ok(s.exec(&format!("DROP TABLE IF EXISTS {t}")).await)?;
    s.close().await;
    Ok(works)
}

/// Two XA passes over separate connections. The outer `Result` is for
/// transport failures; the inner one says why XA doesn't qualify.
async fn xa_round_trip(
    ep: &NodeEndpoint,
    timeout: Duration,
    row: u32,
) -> Result<Result<(), String>, ProbeError> {
    let first = nonce();
    if let Err(why) = xa_pass(ep, timeout, row, first, true).await? {
        return Ok(Err(why));
    }
    let second = nonce();
    if let Err(why) = xa_pass(ep, timeout, row, second, false).await? {
        return Ok(Err(why));
    }
    // After the rollback the committed nonce must still be there.
    let mut s = ProbeSession::connect(ep, timeout).await?;
    let v = ok_val(
        s.scalar(&format!(
            "SELECT v FROM {} WHERE id = {row}",
            sys::probe_table()
        ))
        .await,
    )?
    .flatten();
    ok(s.exec(&format!(
        "DELETE FROM {} WHERE id = {row}",
        sys::probe_table()
    ))
    .await)?;
    s.close().await;
    if v != Some(first.to_string()) {
        return Ok(Err(format!(
            "after XA ROLLBACK the probe row holds {v:?}, expected {first}"
        )));
    }
    Ok(Ok(()))
}

async fn xa_pass(
    ep: &NodeEndpoint,
    timeout: Duration,
    row: u32,
    nonce: u64,
    commit: bool,
) -> Result<Result<(), String>, ProbeError> {
    let xid = format!("sprig_probe_{nonce:016x}");
    let lit = literal(&xid);
    let mut a = ProbeSession::connect(ep, timeout).await?;
    let steps = [
        format!("XA START {lit}"),
        format!(
            "INSERT INTO {} (id, v, written_at) VALUES ({row}, {nonce}, UTC_TIMESTAMP(6)) \
             ON DUPLICATE KEY UPDATE v = VALUES(v), written_at = VALUES(written_at)",
            sys::probe_table()
        ),
        format!("XA END {lit}"),
        format!("XA PREPARE {lit}"),
    ];
    for step in &steps {
        if let Err(e) = a.exec(step).await {
            if !e.is_server_error() {
                return Err(e);
            }
            let _ = a.exec(&format!("XA ROLLBACK {lit}")).await;
            a.close().await;
            return Ok(Err(format!(
                "{}: {e}",
                step.split(' ').take(2).collect::<Vec<_>>().join(" ")
            )));
        }
    }
    // Disconnect with the branch prepared: it must survive.
    a.close().await;

    let mut b = ProbeSession::connect(ep, timeout).await?;
    let finish = if commit { "XA COMMIT" } else { "XA ROLLBACK" };
    let mut last = String::from("not seen in XA RECOVER");
    // The server detaches the branch asynchronously after the disconnect.
    for _ in 0..20 {
        let Some(rows) = ok_val(b.rows("XA RECOVER").await)? else {
            b.close().await;
            return Ok(Err("XA RECOVER refused".to_owned()));
        };
        let seen = rows.iter().any(|r| {
            r.iter()
                .any(|(c, v)| c == "data" && v.as_deref() == Some(xid.as_str()))
        });
        if seen {
            match b.exec(&format!("{finish} {lit}")).await {
                Ok(()) => {
                    last.clear();
                    break;
                }
                Err(e) if e.is_server_error() => last = format!("{finish}: {e}"),
                Err(e) => return Err(e),
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !last.is_empty() {
        b.close().await;
        return Ok(Err(last));
    }
    if commit {
        let v = ok_val(
            b.scalar(&format!(
                "SELECT v FROM {} WHERE id = {row}",
                sys::probe_table()
            ))
            .await,
        )?
        .flatten();
        if v != Some(nonce.to_string()) {
            b.close().await;
            return Ok(Err(format!("after XA COMMIT read {v:?}, expected {nonce}")));
        }
    }
    b.close().await;
    Ok(Ok(()))
}

/// A random-enough 63-bit value: the random bits of a UUIDv7.
fn nonce() -> u64 {
    #[allow(clippy::cast_possible_truncation)]
    let low = JobId::new_v7().0.as_u128() as u64;
    low & (u64::MAX >> 1)
}
