//! Data centers in the gateway: the
//! client's DC id, and the traffic counts the home DC weighs.
//!
//! **DC ids.** A MySQL or MariaDB client names its DC with the connection
//! attribute `sprig_dc` or `SET @sprig_dc = 'fra1'` (consumed here, never
//! forwarded); REST and gRPC with the header `Sprig-DC` (gRPC metadata
//! `sprig-dc`). [`parse_hint`] validates it against the DC registry
//! (`/sprig/g/dcs/`, mirrored by [`DcRegistry`]): `Known`, `Unknown` (well
//! formed but not registered, or retired) or `Missing`. Only a known id is
//! kept in the session ([`crate::session::Session::client_dc`]); `SET
//! @sprig_dc` to an unknown id answers OK with warning 39503
//! `ER_SPRIG_UNKNOWN_DC`. Every hint counts in
//! `sprig_gateway_dc_hints_total{protocol, result}`.
//!
//! **Traffic.** Every statement of a tenant session counts as a read or a
//! write (anything that can change data or schema) per (tenant, client DC).
//! Every `routing.traffic_report_seconds` the gateway sends the counts to
//! the leader controller of each tenant's home DC
//! (`POST /admin/v1/traffic/reports` on its Admin port), which keeps them
//! in one-minute buckets over `migration.window_minutes`
//! (`sprig-controller::traffic`). Counts that can't be sent are kept and
//! summed into the next report.
//!
//! **Report path.** There is no internal gRPC port for traffic reports
//! yet (one that would let the gateway's own DC leader merge rows and
//! forward one batch per home DC), so the gateway reports straight to each
//! home DC's leader through the Admin port (found as the Admin API's
//! forwarding finds it, in that DC's tier of the one etcd). Unknown client DCs report as `"?"`, missing ones as `""`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sprig_common::ids::{DcId, TenantId};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::{MetaStore, Mirror, Record, Tier, TierView};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

/// `routing.traffic_report_seconds`.
pub static TRAFFIC_REPORT_SECONDS: Setting<u64> = Setting {
    key: "routing.traffic_report_seconds",
    default: 10,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 3600),
    apply: Apply::Live,
    operator_only: true,
    owner: 24,
    doc: "How often gateways report traffic counts to the home DCs",
};
sprig_settings::register!(TRAFFIC_REPORT_SECONDS);

/// A client's DC id, as given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DcHint {
    /// No id, or an empty one.
    Missing,
    /// Malformed, not registered, or retired.
    Unknown(String),
    /// A registered DC.
    Known(DcId),
}

impl DcHint {
    /// The metric label.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            DcHint::Missing => "missing",
            DcHint::Unknown(_) => "unknown",
            DcHint::Known(_) => "known",
        }
    }

    /// The known DC, if any.
    #[must_use]
    pub fn known(self) -> Option<DcId> {
        match self {
            DcHint::Known(d) => Some(d),
            _ => None,
        }
    }
}

/// The DC registry as the gateway sees it: a mirror of `/sprig/g/dcs/`.
#[derive(Debug, Clone)]
pub struct DcRegistry {
    mirror: Mirror,
}

impl DcRegistry {
    /// Mirrors `dcs/` of `global`.
    ///
    /// # Errors
    /// Internal never in practice (the prefix is valid).
    pub fn start(global: &Arc<dyn MetaStore>) -> sprig_common::Result<Self> {
        let prefix = Tier::Global.key("dcs/")?;
        Ok(Self {
            mirror: Mirror::spawn(TierView::over(global, Tier::Global), prefix),
        })
    }

    /// Whether `dc` is registered and not retired.
    #[must_use]
    pub fn is_known(&self, dc: &DcId) -> bool {
        let Ok(key) = Tier::Global.key(&format!("dcs/{dc}")) else {
            return false;
        };
        self.mirror
            .get(&key)
            .and_then(|kv| Record::<DcRecord>::decode(&kv.value).ok())
            .is_some_and(|r| r.body.state != "retired")
    }
}

/// The process's registry, installed when tenant services start (the
/// front ends validate hints at login, before any tenant service is at
/// hand).
static REGISTRY: OnceLock<DcRegistry> = OnceLock::new();

/// Installs the process's registry (once; later calls are ignored).
pub fn install_registry(r: DcRegistry) {
    let _ = REGISTRY.set(r);
}

/// The process's registry, if installed.
#[must_use]
pub fn registry() -> Option<&'static DcRegistry> {
    REGISTRY.get()
}

/// `hint(raw)`: absent or empty is `Missing`; text that isn't
/// a DC id (`[a-z][a-z0-9-]{1,31}` after lowercasing and trimming) is
/// `Unknown`; a registered, not retired DC is `Known`. Without a registry
/// (development without etcd) a well-formed id is taken as known.
#[must_use]
pub fn parse_hint(raw: Option<&str>, reg: Option<&DcRegistry>) -> DcHint {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return DcHint::Missing;
    };
    let s = raw.to_ascii_lowercase();
    let well_formed = s.len() >= 2
        && s.len() <= 32
        && s.starts_with(|c: char| c.is_ascii_lowercase())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if !well_formed {
        return DcHint::Unknown(raw.to_owned());
    }
    let Ok(dc) = DcId::new(s.as_str()) else {
        return DcHint::Unknown(raw.to_owned());
    };
    match reg {
        Some(r) if !r.is_known(&dc) => DcHint::Unknown(s),
        _ => DcHint::Known(dc),
    }
}

/// Validates a hint with the process's registry and counts it.
#[must_use]
pub fn hint(protocol: &'static str, raw: Option<&str>) -> DcHint {
    let h = parse_hint(raw, registry());
    metrics::counter!("sprig_gateway_dc_hints_total", "protocol" => protocol, "result" => h.label())
        .increment(1);
    h
}

/// The value of `SET @sprig_dc = '<id>'` (also `:=`, double quotes, a
/// trailing `;`), when that is the whole statement. Any other statement,
/// including one that assigns more variables, is `None` and goes to
/// MariaDB as usual.
#[must_use]
pub fn parse_set(sql: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(sql).ok()?.trim();
    let s = s.strip_suffix(';').unwrap_or(s).trim_end();
    let rest = strip_word(s, "set")?;
    let rest = rest.strip_prefix('@')?;
    let rest = strip_word_exact(rest, "sprig_dc")?.trim_start();
    let rest = rest
        .strip_prefix(":=")
        .or_else(|| rest.strip_prefix('='))?
        .trim_start();
    let q = rest.chars().next().filter(|c| *c == '\'' || *c == '"')?;
    let body = rest.strip_prefix(q)?;
    let end = body.find(q)?;
    if !body[end + 1..].trim().is_empty() || body[..end].contains('\\') {
        return None;
    }
    Some(body[..end].to_owned())
}

/// `s` without a leading case-insensitive `word` and the whitespace after it.
fn strip_word<'a>(s: &'a str, word: &str) -> Option<&'a str> {
    let head = s.get(..word.len())?;
    let rest = &s[word.len()..];
    (head.eq_ignore_ascii_case(word) && rest.starts_with(char::is_whitespace))
        .then(|| rest.trim_start())
}

/// `s` without a leading case-insensitive `word` not followed by an
/// identifier character.
fn strip_word_exact<'a>(s: &'a str, word: &str) -> Option<&'a str> {
    let head = s.get(..word.len())?;
    let rest = &s[word.len()..];
    let ident = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '$';
    (head.eq_ignore_ascii_case(word) && !rest.starts_with(ident)).then_some(rest)
}

/// Whether a statement text may change data or schema: anything but a
/// plain read (`SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`, `WITH`, `SET`,
/// `USE`, `HELP`, `DO`, transaction control). Counts only, so a cheap look
/// at the first keyword.
#[must_use]
pub fn is_write(sql: &[u8]) -> bool {
    let mut s = sql;
    loop {
        while let [b, rest @ ..] = s
            && (b.is_ascii_whitespace() || *b == b'(')
        {
            s = rest;
        }
        // Skip leading comments (`/* */`, `-- `, `#`).
        if s.starts_with(b"/*") {
            match s.windows(2).position(|w| w == b"*/") {
                Some(p) => s = &s[p + 2..],
                None => return false,
            }
        } else if s.starts_with(b"--") || s.starts_with(b"#") {
            match s.iter().position(|b| *b == b'\n') {
                Some(p) => s = &s[p + 1..],
                None => return false,
            }
        } else {
            break;
        }
    }
    let word: Vec<u8> = s
        .iter()
        .take_while(|b| b.is_ascii_alphabetic())
        .map(u8::to_ascii_lowercase)
        .collect();
    !matches!(
        word.as_slice(),
        b"select"
            | b"show"
            | b"describe"
            | b"desc"
            | b"explain"
            | b"with"
            | b"set"
            | b"use"
            | b"help"
            | b"do"
            | b"begin"
            | b"start"
            | b"commit"
            | b"rollback"
            | b"savepoint"
            | b"release"
            | b""
    )
}

/// One row of a traffic report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficRow {
    /// The tenant.
    pub tenant_id: u64,
    /// The client DC; `""` for none, `"?"` for an unknown one.
    pub client_dc: String,
    /// Reads.
    pub reads: u64,
    /// Writes.
    pub writes: u64,
}

/// A traffic report (a `TrafficBatch`, as JSON).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficBatch {
    /// Unix minute the counts end in.
    pub minute: i64,
    /// The rows.
    pub rows: Vec<TrafficRow>,
}

type Counts = HashMap<(TenantId, String), (u64, u64)>;

/// The gateway's traffic counts and where its tenants live.
pub struct DcRouting {
    dc: DcId,
    counts: Mutex<Counts>,
    /// Each followed tenant's home DC, as the locator last saw it.
    homes: Mutex<HashMap<TenantId, DcId>>,
}

impl std::fmt::Debug for DcRouting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DcRouting")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl DcRouting {
    /// Routing state of a gateway in `dc`.
    #[must_use]
    pub fn new(dc: DcId) -> Arc<Self> {
        Arc::new(Self {
            dc,
            counts: Mutex::new(HashMap::new()),
            homes: Mutex::new(HashMap::new()),
        })
    }

    /// This gateway's DC.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc
    }

    /// Counts one statement of `tenant` from `client` (the session's known
    /// DC, or `None`).
    pub fn record(&self, tenant: TenantId, client: Option<&DcId>, write: bool) {
        let key = (tenant, client.map_or_else(String::new, ToString::to_string));
        let mut c = lock(&self.counts);
        let e = c.entry(key).or_insert((0, 0));
        if write {
            e.1 += 1;
        } else {
            e.0 += 1;
        }
    }

    /// Notes `tenant`'s home DC (the locator calls it).
    pub fn set_home(&self, tenant: TenantId, home: DcId) {
        lock(&self.homes).insert(tenant, home);
    }

    /// `tenant`'s home DC as last seen; this DC if unknown.
    #[must_use]
    pub fn home(&self, tenant: TenantId) -> DcId {
        lock(&self.homes)
            .get(&tenant)
            .cloned()
            .unwrap_or_else(|| self.dc.clone())
    }

    /// Takes the counts, grouped by home DC.
    fn take(&self, minute: i64) -> HashMap<DcId, TrafficBatch> {
        let counts = std::mem::take(&mut *lock(&self.counts));
        let mut out: HashMap<DcId, TrafficBatch> = HashMap::new();
        for ((tenant, client_dc), (reads, writes)) in counts {
            out.entry(self.home(tenant))
                .or_insert_with(|| TrafficBatch {
                    minute,
                    rows: Vec::new(),
                })
                .rows
                .push(TrafficRow {
                    tenant_id: tenant.0,
                    client_dc,
                    reads,
                    writes,
                });
        }
        out
    }

    /// Puts counts that couldn't be sent back, summed into the next report.
    fn restore(&self, batch: TrafficBatch) {
        let mut c = lock(&self.counts);
        for r in batch.rows {
            let e = c
                .entry((TenantId(r.tenant_id), r.client_dc))
                .or_insert((0, 0));
            e.0 += r.reads;
            e.1 += r.writes;
        }
    }

    /// Sends the counts every `routing.traffic_report_seconds` until the
    /// last strong reference to `this` drops. `dc_store` is this DC's
    /// tier; other DCs' leaders are found through it (single-etcd mode).
    pub fn spawn_reporter(
        this: &Arc<Self>,
        dc_store: Arc<dyn MetaStore>,
        live: Option<LiveSettings>,
    ) -> tokio::task::JoinHandle<()> {
        let weak: Weak<Self> = Arc::downgrade(this);
        let my_dc = this.dc.clone();
        tokio::spawn(async move {
            let mut apis: HashMap<DcId, crate::http::admin::AdminApi> = HashMap::new();
            loop {
                let secs = live.as_ref().map_or(TRAFFIC_REPORT_SECONDS.default, |l| {
                    l.get(&TRAFFIC_REPORT_SECONDS, &SettingCtx::process(&my_dc))
                });
                tokio::time::sleep(Duration::from_secs(secs)).await;
                let Some(this) = weak.upgrade() else {
                    return;
                };
                let minute = unix_minute();
                for (home, batch) in this.take(minute) {
                    let api = apis.entry(home.clone()).or_insert_with(|| {
                        crate::http::admin::AdminApi::new(
                            TierView::over(&dc_store, Tier::Dc(home.clone())),
                            format!("traffic@{my_dc}"),
                            None,
                            Arc::new(|_: &str| false),
                        )
                    });
                    let body = match serde_json::to_vec(&batch) {
                        Ok(b) => Bytes::from(b),
                        Err(_) => continue,
                    };
                    let sent = api
                        .forward(
                            &hyper::Method::POST,
                            "/admin/v1/traffic/reports",
                            None,
                            body,
                        )
                        .await;
                    match sent {
                        Ok(r) if r.status < 300 => {
                            metrics::counter!("sprig_gateway_traffic_reports_total", "result" => "ok")
                                .increment(1);
                        }
                        other => {
                            let why = match other {
                                Ok(r) => format!("status {}", r.status),
                                Err(e) => e.to_string(),
                            };
                            tracing::debug!(%home, why, "traffic report not sent; kept for the next one");
                            metrics::counter!("sprig_gateway_traffic_reports_total", "result" => "failed")
                                .increment(1);
                            this.restore(batch);
                        }
                    }
                }
            }
        })
    }
}

fn unix_minute() -> i64 {
    let s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    i64::try_from(s / 60).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hints() {
        assert_eq!(parse_hint(None, None), DcHint::Missing);
        assert_eq!(parse_hint(Some("  "), None), DcHint::Missing);
        assert_eq!(
            parse_hint(Some(" FRA1 "), None),
            DcHint::Known(DcId::new("fra1").unwrap())
        );
        assert!(matches!(parse_hint(Some("1fra"), None), DcHint::Unknown(_)));
        assert!(matches!(parse_hint(Some("f"), None), DcHint::Unknown(_)));
        assert!(matches!(
            parse_hint(Some("fra_1"), None),
            DcHint::Unknown(_)
        ));
        assert!(matches!(
            parse_hint(Some(&"a".repeat(33)), None),
            DcHint::Unknown(_)
        ));
        assert_eq!(DcHint::Missing.label(), "missing");
    }

    #[test]
    fn set_statements() {
        assert_eq!(
            parse_set(b"SET @sprig_dc = 'fra1'").as_deref(),
            Some("fra1")
        );
        assert_eq!(
            parse_set(b"set @SPRIG_DC:=\"mum1\";").as_deref(),
            Some("mum1")
        );
        assert_eq!(parse_set(b"  SET   @sprig_dc='' ").as_deref(), Some(""));
        assert_eq!(parse_set(b"SET @sprig_dcx = 'a'"), None);
        assert_eq!(parse_set(b"SET @sprig_dc = 'a', @b = 1"), None);
        assert_eq!(parse_set(b"SET @other = 'a'"), None);
        assert_eq!(parse_set(b"SELECT 1"), None);
        assert_eq!(parse_set(b"SET @sprig_dc = 'a\\'b'"), None);
    }

    #[test]
    fn writes() {
        assert!(!is_write(b"SELECT 1"));
        assert!(!is_write(b"  /* hi */ (select 1)"));
        assert!(!is_write(b"-- c\nshow tables"));
        assert!(!is_write(b"BEGIN"));
        assert!(is_write(b"INSERT INTO t VALUES (1)"));
        assert!(is_write(b"/*+ x */ update t set a=1"));
        assert!(is_write(b"CREATE TABLE t (a int)"));
        assert!(!is_write(b""));
    }

    #[test]
    fn counts_group_by_home_and_survive_failures() {
        let r = DcRouting::new(DcId::new("mum1").unwrap());
        let fra1 = DcId::new("fra1").unwrap();
        r.record(TenantId(1), Some(&fra1), true);
        r.record(TenantId(1), Some(&fra1), false);
        r.record(TenantId(2), None, false);
        r.set_home(TenantId(1), fra1.clone());
        let mut by = r.take(5);
        assert_eq!(by.len(), 2);
        let b = by.remove(&fra1).unwrap();
        assert_eq!(b.rows.len(), 1);
        assert_eq!((b.rows[0].reads, b.rows[0].writes), (1, 1));
        r.restore(b);
        r.record(TenantId(1), Some(&fra1), true);
        let b = r.take(6).remove(&fra1).unwrap();
        assert_eq!((b.rows[0].reads, b.rows[0].writes), (1, 2));
    }
}
