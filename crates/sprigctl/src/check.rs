//! `sprigctl check no-restart`: the live-change check runner (chapter 47,
//! "No restarts"; build step 6).
//!
//! Under light load (a few MySQL connections running `SELECT 1` through
//! the gateway), it applies each change the operator names, waits, checks
//! that it took effect, restores it, and checks again:
//!
//! | Change | Applied through | Took effect when |
//! |---|---|---|
//! | `--set key=value@scope` | Admin API `settings set` (restored to the old row, or reset) | the Admin API resolves the new value at that scope and the gateway counted no `sprig_settings_apply_failures_total` |
//! | `--listener name=host:port` | `listener.<name>.bind` at `--listener-scope` | the new address accepts connections and `sprig_gateway_listener_generation{listener}` rose |
//! | `--rotate-cert live.pem=new.pem` | the file is replaced (key alongside: `--rotate-key`) | some `sprig_gateway_cert_expiry_seconds` changed |
//!
//! Throughout, the gateway's `/readyz?verbose` must keep its process
//! identity (pid and start time: no restart) and stay ready, and no
//! statement may fail; a connection the gateway closes at a safe point
//! (the protocol's way of moving a session off a retired listener) is
//! counted as a reconnect, not a failure. It also reports `sprigdb.toml`
//! drift from `/readyz?verbose`. The 72-hour soak is
//! `deploy/compose/m07/soak.sh` (chapter 04 step 11); this runner is the
//! quick check an operator can run against any gateway.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use clap::{Arg, ArgAction, ArgMatches, Command};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use mysql_async::prelude::Queryable;
use serde::Serialize;
use sprig_controller::admin::types::{
    ResetSettingRequest, ScopeRef, SetSettingRequest, SettingExplanation, SettingWriteResponse,
};

use crate::client::AdminClient;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl check`.
pub(crate) fn group() -> Command {
    Command::new("check")
        .about("Operational checks (chapter 47)")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("no-restart")
                .about("Change settings, listeners and certificates under load; prove no restart and no failed statement")
                .arg(
                    Arg::new("telemetry")
                        .long("telemetry")
                        .required(true)
                        .help("The gateway's telemetry base URL, http://host:9180"),
                )
                .arg(
                    Arg::new("gateway")
                        .long("gateway")
                        .required(true)
                        .help("The gateway's MySQL or MariaDB listener for the load, host:port"),
                )
                .arg(Arg::new("user").long("user").required(true).help("Login, <tenant>.<user>"))
                .arg(
                    Arg::new("password")
                        .long("password")
                        .env("SPRIGCTL_GATEWAY_PASSWORD")
                        .hide_env_values(true)
                        .required(true)
                        .help("Its password"),
                )
                .arg(
                    Arg::new("tls-insecure")
                        .long("tls-insecure")
                        .action(ArgAction::SetTrue)
                        .help("Load over TLS accepting any certificate"),
                )
                .arg(
                    Arg::new("connections")
                        .long("connections")
                        .value_parser(clap::value_parser!(u32).range(1..=64))
                        .default_value("4")
                        .help("Load connections"),
                )
                .arg(
                    Arg::new("set")
                        .long("set")
                        .action(ArgAction::Append)
                        .help("A setting change, key=value@scope (scope as --scope takes it)"),
                )
                .arg(
                    Arg::new("listener")
                        .long("listener")
                        .action(ArgAction::Append)
                        .help("A listener rebind, name=host:port"),
                )
                .arg(
                    Arg::new("listener-scope")
                        .long("listener-scope")
                        .default_value("dc")
                        .help("Where listener.<name>.bind is written"),
                )
                .arg(
                    Arg::new("rotate-cert")
                        .long("rotate-cert")
                        .help("live.pem=new.pem: replace the gateway's certificate file"),
                )
                .arg(
                    Arg::new("rotate-key")
                        .long("rotate-key")
                        .help("live-key.pem=new-key.pem: its key, replaced first"),
                )
                .arg(
                    Arg::new("settle")
                        .long("settle")
                        .value_parser(clap::value_parser!(u64))
                        .default_value("5")
                        .help("Seconds to wait after each change"),
                ),
        )
}

/// One step's outcome.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Step {
    change: String,
    applied: bool,
    observed: String,
    restored: bool,
    failures: u64,
    reconnects: u64,
    ok: bool,
}

/// The whole run.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CheckReport {
    pid: u64,
    started_unix_ms: u64,
    restarted: bool,
    config_drift: bool,
    statements: u64,
    failures: u64,
    reconnects: u64,
    steps: Vec<Step>,
    passed: bool,
}

/// What `/readyz?verbose` says about the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity {
    pub pid: u64,
    pub started_unix_ms: u64,
    pub ready: bool,
    pub drift: bool,
}

/// Parses `/readyz?verbose`.
pub(crate) fn parse_identity(body: &str) -> Option<Identity> {
    let ready = body.lines().next()? == "ready";
    let process = body.lines().find(|l| l.starts_with("[+]process "))?;
    let field = |k: &str| {
        process
            .split_whitespace()
            .find_map(|w| w.strip_prefix(k)?.strip_prefix('=')?.parse::<u64>().ok())
    };
    Some(Identity {
        pid: field("pid")?,
        started_unix_ms: field("started_unix_ms")?,
        ready,
        drift: body.lines().any(|l| l.starts_with("[-]config drift")),
    })
}

/// The sum of a metric's samples whose labels contain `filter`.
pub(crate) fn metric_sum(text: &str, name: &str, filter: &str) -> f64 {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| {
            l.strip_prefix(name)
                .is_some_and(|r| r.starts_with('{') || r.starts_with(' '))
        })
        .filter(|l| l.contains(filter))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .fold(0.0, |a, b| a + b)
}

/// Every sample of `name`, as `(labels, value)`.
fn samples(text: &str, name: &str) -> Vec<(String, f64)> {
    text.lines()
        .filter_map(|l| {
            let r = l.strip_prefix(name)?;
            if !(r.starts_with('{') || r.starts_with(' ')) {
                return None;
            }
            let (labels, v) = r.rsplit_once(' ')?;
            Some((labels.to_owned(), v.parse().ok()?))
        })
        .collect()
}

async fn http_get(url: &str) -> Result<(u16, String), Failed> {
    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let uri: hyper::Uri = url.parse().map_err(|e| Failed(format!("{url}: {e}")))?;
    let resp = tokio::time::timeout(Duration::from_secs(10), client.get(uri))
        .await
        .map_err(|_| Failed(format!("{url}: no answer")))?
        .map_err(|e| Failed(format!("{url}: {e}")))?;
    let status = resp.status().as_u16();
    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| Failed(format!("{url}: {e}")))?
        .to_bytes();
    Ok((status, String::from_utf8_lossy(&body).into_owned()))
}

struct Load {
    ok: AtomicU64,
    failures: AtomicU64,
    reconnects: AtomicU64,
    stop: AtomicBool,
    last_error: std::sync::Mutex<Option<String>>,
}

fn opts(sub: &ArgMatches) -> Result<mysql_async::OptsBuilder, Failed> {
    let gw = sub
        .get_one::<String>("gateway")
        .cloned()
        .unwrap_or_default();
    let (host, port) = gw
        .rsplit_once(':')
        .and_then(|(h, p)| Some((h.to_owned(), p.parse::<u16>().ok()?)))
        .ok_or_else(|| Failed(format!("--gateway {gw:?}: expected host:port")))?;
    let mut o = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .user(sub.get_one::<String>("user").cloned())
        .pass(sub.get_one::<String>("password").cloned())
        .prefer_socket(false);
    if sub.get_flag("tls-insecure") {
        o = o.ssl_opts(
            mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        );
    }
    Ok(o)
}

async fn load_worker(o: mysql_async::OptsBuilder, load: Arc<Load>) {
    let mut conn: Option<mysql_async::Conn> = None;
    while !load.stop.load(Ordering::Relaxed) {
        if conn.is_none() {
            match mysql_async::Conn::new(o.clone()).await {
                Ok(c) => conn = Some(c),
                Err(e) => {
                    load.failures.fetch_add(1, Ordering::Relaxed);
                    *load
                        .last_error
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(e.to_string());
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            }
        }
        let Some(c) = conn.as_mut() else { continue };
        match c.query_drop("SELECT 1").await {
            Ok(()) => {
                load.ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                // A connection the gateway closed at a safe point: a new one,
                // and the statement once more.
                conn = None;
                let retry = match mysql_async::Conn::new(o.clone()).await {
                    Ok(mut c) => {
                        let r = c.query_drop("SELECT 1").await;
                        conn = Some(c);
                        r
                    }
                    Err(e) => Err(e),
                };
                if retry.is_ok() {
                    load.reconnects.fetch_add(1, Ordering::Relaxed);
                    load.ok.fetch_add(1, Ordering::Relaxed);
                } else {
                    load.failures.fetch_add(1, Ordering::Relaxed);
                    *load
                        .last_error
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(e.to_string());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if let Some(c) = conn {
        let _ = c.disconnect().await;
    }
}

fn split_change(s: &str) -> Result<(String, String, String), Failed> {
    let (kv, scope) = s
        .rsplit_once('@')
        .ok_or_else(|| Failed(format!("--set {s:?}: expected key=value@scope")))?;
    let (k, v) = kv
        .split_once('=')
        .ok_or_else(|| Failed(format!("--set {s:?}: expected key=value@scope")))?;
    Ok((k.to_owned(), v.to_owned(), scope.to_owned()))
}

/// Writes `value_text` (or restores `previous`) for `key` at `scope`.
async fn write_setting(
    c: &AdminClient,
    key: &str,
    scope: &ScopeRef,
    value: Option<&str>,
    previous: Option<&serde_json::Value>,
) -> Result<SettingWriteResponse, Failed> {
    let path = format!("/admin/v1/settings/{}", escape(key));
    let actor = Some("sprigctl-check".to_owned());
    let reason = "sprigctl check no-restart".to_owned();
    if let Some(v) = value {
        return Ok(c
            .put(
                &path,
                &SetSettingRequest {
                    scope: Some(scope.clone()),
                    value: None,
                    value_text: Some(v.to_owned()),
                    reason,
                    actor,
                },
            )
            .await?);
    }
    match previous {
        Some(p) => Ok(c
            .put(
                &path,
                &SetSettingRequest {
                    scope: Some(scope.clone()),
                    value: serde_json::from_value(p.clone()).ok(),
                    value_text: None,
                    reason,
                    actor,
                },
            )
            .await?),
        None => Ok(c
            .post(
                &format!("{path}:reset"),
                &ResetSettingRequest {
                    scope: Some(scope.clone()),
                    bounds: false,
                    all: false,
                    reason,
                    actor,
                },
            )
            .await?),
    }
}

fn get_query(scope: &ScopeRef) -> String {
    match scope {
        ScopeRef::Global | ScopeRef::Dc(None) => String::new(),
        ScopeRef::Dc(Some(d)) => format!("?dc={d}"),
        ScopeRef::Cluster(c) => format!("?cluster={c}"),
        ScopeRef::Tenant(t) => format!("?tenant={}", escape(t)),
        ScopeRef::Database { tenant, db } => {
            format!("?tenant={}&db={}", escape(tenant), escape(db))
        }
        ScopeRef::Object { tenant, db, object } => format!(
            "?tenant={}&db={}&object={}",
            escape(tenant),
            escape(db),
            escape(object)
        ),
    }
}

/// `sprigctl check no-restart`.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the run, step by step
pub(crate) async fn no_restart(
    c: &AdminClient,
    group: &ArgMatches,
    out: Output,
) -> Result<bool, Failed> {
    let Some(("no-restart", sub)) = group.subcommand() else {
        return Err(Failed("a subcommand is required".into()));
    };
    let tel = sub
        .get_one::<String>("telemetry")
        .cloned()
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_owned();
    let settle = Duration::from_secs(sub.get_one::<u64>("settle").copied().unwrap_or(5));
    let identity = || async {
        let (code, body) = http_get(&format!("{tel}/readyz?verbose")).await?;
        let id = parse_identity(&body)
            .ok_or_else(|| Failed(format!("{tel}/readyz?verbose: unexpected answer ({code})")))?;
        Ok::<Identity, Failed>(id)
    };
    let metrics = || async { http_get(&format!("{tel}/metrics")).await.map(|(_, b)| b) };
    let before = identity().await?;

    let load = Arc::new(Load {
        ok: AtomicU64::new(0),
        failures: AtomicU64::new(0),
        reconnects: AtomicU64::new(0),
        stop: AtomicBool::new(false),
        last_error: std::sync::Mutex::new(None),
    });
    let o = opts(sub)?;
    let n = sub.get_one::<u32>("connections").copied().unwrap_or(4);
    let workers: Vec<_> = (0..n)
        .map(|_| tokio::spawn(load_worker(o.clone(), Arc::clone(&load))))
        .collect();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut steps = Vec::new();
    let counts = |l: &Load| {
        (
            l.failures.load(Ordering::Relaxed),
            l.reconnects.load(Ordering::Relaxed),
        )
    };
    let alive = |id: Identity| {
        id.pid == before.pid && id.started_unix_ms == before.started_unix_ms && id.ready
    };

    for s in sub.get_many::<String>("set").into_iter().flatten() {
        let (key, value, scope_text) = split_change(s)?;
        let scope: ScopeRef = scope_text.parse().map_err(Failed)?;
        let (f0, r0) = counts(&load);
        let m0 = metric_sum(&metrics().await?, "sprig_settings_apply_failures_total", "");
        let w = write_setting(c, &key, &scope, Some(&value), None).await?;
        tokio::time::sleep(settle).await;
        let e: SettingExplanation = c
            .get(&format!(
                "/admin/v1/settings/{}{}",
                escape(&key),
                get_query(&scope)
            ))
            .await?;
        let m1 = metric_sum(&metrics().await?, "sprig_settings_apply_failures_total", "");
        let resolved = match &e.value {
            sprig_settings::Value::Str(s) => s.clone(),
            sprig_settings::Value::Int(i) => i.to_string(),
            sprig_settings::Value::Float(f) => f.to_string(),
            sprig_settings::Value::Bool(b) => b.to_string(),
        };
        let previous = w
            .previous
            .as_ref()
            .and_then(|p| p.value.as_ref())
            .and_then(|v| serde_json::to_value(v).ok());
        let restored = write_setting(c, &key, &scope, None, previous.as_ref())
            .await
            .is_ok();
        tokio::time::sleep(settle).await;
        let id = identity().await?;
        let (f1, r1) = counts(&load);
        #[allow(clippy::float_cmp)]
        let applied_ok = resolved == value && m1 == m0;
        steps.push(Step {
            change: format!("set {key}={value} at {scope}"),
            applied: w.changed,
            observed: format!("resolves to {resolved}; apply failures {m0} -> {m1}"),
            restored,
            failures: f1 - f0,
            reconnects: r1 - r0,
            ok: applied_ok && restored && f1 == f0 && alive(id),
        });
    }

    let lscope: ScopeRef = sub
        .get_one::<String>("listener-scope")
        .map_or("dc", String::as_str)
        .parse()
        .map_err(Failed)?;
    for l in sub.get_many::<String>("listener").into_iter().flatten() {
        let (name, addr) = l
            .split_once('=')
            .ok_or_else(|| Failed(format!("--listener {l:?}: expected name=host:port")))?;
        let key = format!("listener.{name}.bind");
        let (f0, r0) = counts(&load);
        let g0 = metric_sum(
            &metrics().await?,
            "sprig_gateway_listener_generation",
            &format!("listener=\"{name}\""),
        );
        let w = write_setting(c, &key, &lscope, Some(addr), None).await?;
        tokio::time::sleep(settle).await;
        let reachable =
            tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
                .await
                .is_ok_and(|r| r.is_ok());
        let g1 = metric_sum(
            &metrics().await?,
            "sprig_gateway_listener_generation",
            &format!("listener=\"{name}\""),
        );
        let previous = w
            .previous
            .as_ref()
            .and_then(|p| p.value.as_ref())
            .and_then(|v| serde_json::to_value(v).ok());
        let restored = write_setting(c, &key, &lscope, None, previous.as_ref())
            .await
            .is_ok();
        tokio::time::sleep(settle).await;
        let id = identity().await?;
        let (f1, r1) = counts(&load);
        steps.push(Step {
            change: format!("rebind {name} to {addr} at {lscope}"),
            applied: w.changed,
            observed: format!(
                "{addr} {}; generation {g0} -> {g1}",
                if reachable { "accepts" } else { "refuses" }
            ),
            restored,
            failures: f1 - f0,
            reconnects: r1 - r0,
            ok: reachable && g1 > g0 && restored && f1 == f0 && alive(id),
        });
    }

    if let Some(spec) = sub.get_one::<String>("rotate-cert") {
        let (live, new) = spec
            .split_once('=')
            .ok_or_else(|| Failed(format!("--rotate-cert {spec:?}: expected live.pem=new.pem")))?;
        let key = sub
            .get_one::<String>("rotate-key")
            .map(|k| {
                k.split_once('=')
                    .map(|(a, b)| (a.to_owned(), b.to_owned()))
                    .ok_or_else(|| Failed(format!("--rotate-key {k:?}: expected live=new")))
            })
            .transpose()?;
        let (f0, r0) = counts(&load);
        let e0 = samples(&metrics().await?, "sprig_gateway_cert_expiry_seconds");
        let backup = |p: &str| format!("{p}.sprigctl-check");
        let mut copy = vec![(live.to_owned(), new.to_owned())];
        if let Some((a, b)) = &key {
            copy.insert(0, (a.clone(), b.clone()));
        }
        for (dst, src) in &copy {
            std::fs::copy(dst, backup(dst)).map_err(|e| Failed(format!("{dst}: {e}")))?;
            std::fs::copy(src, dst).map_err(|e| Failed(format!("{src} -> {dst}: {e}")))?;
        }
        tokio::time::sleep(settle).await;
        let e1 = samples(&metrics().await?, "sprig_gateway_cert_expiry_seconds");
        // Expiry gauges count down: a changed certificate jumps by more
        // than the time that passed.
        let changed = e1.iter().any(|(l, v)| {
            e0.iter()
                .find(|(l0, _)| l0 == l)
                .is_some_and(|(_, v0)| (v - v0).abs() > settle.as_secs_f64() + 60.0)
        });
        let mut restored = true;
        for (dst, _) in copy.iter().rev() {
            restored &= std::fs::copy(backup(dst), dst).is_ok();
            let _ = std::fs::remove_file(backup(dst));
        }
        tokio::time::sleep(settle).await;
        let id = identity().await?;
        let (f1, r1) = counts(&load);
        steps.push(Step {
            change: format!("rotate certificate {live}"),
            applied: true,
            observed: if changed {
                "a certificate's expiry gauge changed".into()
            } else {
                "no certificate changed".into()
            },
            restored,
            failures: f1 - f0,
            reconnects: r1 - r0,
            ok: changed && restored && f1 == f0 && alive(id),
        });
    }

    load.stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }
    let after = identity().await?;
    let failures = load.failures.load(Ordering::Relaxed);
    let restarted = after.pid != before.pid || after.started_unix_ms != before.started_unix_ms;
    let report = CheckReport {
        pid: before.pid,
        started_unix_ms: before.started_unix_ms,
        restarted,
        config_drift: after.drift,
        statements: load.ok.load(Ordering::Relaxed),
        failures,
        reconnects: load.reconnects.load(Ordering::Relaxed),
        passed: !restarted && failures == 0 && after.ready && steps.iter().all(|s| s.ok),
        steps,
    };
    let last = load
        .last_error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    print(out, &report, |r| {
        let mut s = String::new();
        for st in &r.steps {
            let _ = writeln!(
                s,
                "{} {}: {}; restored {}; {} failures, {} reconnects",
                if st.ok { "PASS" } else { "FAIL" },
                st.change,
                st.observed,
                st.restored,
                st.failures,
                st.reconnects
            );
        }
        let _ = write!(
            s,
            "process pid {} started {}: {}; {} statements, {} failed, {} reconnects{}\n{}\n",
            r.pid,
            r.started_unix_ms,
            if r.restarted {
                "RESTARTED"
            } else {
                "no restart"
            },
            r.statements,
            r.failures,
            r.reconnects,
            if r.config_drift {
                "; sprigdb.toml differs from the running configuration"
            } else {
                ""
            },
            if r.passed { "PASS" } else { "FAIL" }
        );
        if let Some(e) = &last {
            let _ = writeln!(s, "last error: {e}");
        }
        s
    });
    Ok(report.passed)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn identity_comes_from_readyz_verbose() {
        let body = "ready\n[+]process pid=42 started_unix_ms=1794156588875 version=0.0.1\n[+]watchdog ticking\n[-]config drift: /x differs\n";
        let id = parse_identity(body).unwrap();
        assert_eq!(id.pid, 42);
        assert_eq!(id.started_unix_ms, 1_794_156_588_875);
        assert!(id.ready && id.drift);
        assert!(
            !parse_identity("starting\n[+]process pid=1 started_unix_ms=2 version=x\n")
                .unwrap()
                .ready
        );
        assert!(parse_identity("ready\n").is_none());
    }

    #[test]
    fn metrics_are_summed_by_name_and_label() {
        let text = "# HELP x\nsprig_gateway_listener_generation{listener=\"mysql\"} 3\nsprig_gateway_listener_generation{listener=\"http\"} 5\nsprig_gateway_listener_generation_other 9\n";
        assert!(
            (metric_sum(
                text,
                "sprig_gateway_listener_generation",
                "listener=\"mysql\""
            ) - 3.0)
                .abs()
                < 1e-9
        );
        assert!((metric_sum(text, "sprig_gateway_listener_generation", "") - 8.0).abs() < 1e-9);
        assert_eq!(samples(text, "sprig_gateway_listener_generation").len(), 2);
    }

    #[test]
    fn changes_parse() {
        assert_eq!(
            split_change("telemetry.trace_slow_ms=50@dc:dc-a").unwrap(),
            (
                "telemetry.trace_slow_ms".into(),
                "50".into(),
                "dc:dc-a".into()
            )
        );
        assert!(split_change("nokey").is_err());
    }
}
