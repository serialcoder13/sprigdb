//! The `studio.*` keys (chapter 44, "Configuration") and the values
//! serving reads from them.
//!
//! Every key is a live setting. Lists (`studio.listeners`,
//! `studio.public_origins`) are comma-separated text until the settings
//! registry has a list type. Keys the chapter also accepts at tenant,
//! database or object scope are registered with those scopes; the gateway
//! resolves them at its own scopes (dc, global) in R0, as it does the
//! `api.*` keys, because a request's tenant is known only after it signs
//! in.

use std::borrow::Cow;

use serde_json::{Value, json};
use sprig_common::ids::DcId;
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Global only.
const GLOBAL: Scopes = Scopes(0b10_0000);

macro_rules! def {
    ($NAME:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $doc:literal) => {
        #[doc = concat!("`", $key, "`: ", $doc, ".")]
        pub static $NAME: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 44,
            doc: $doc,
        };
        sprig_settings::register!($NAME);
    };
}

def!(ENABLED: bool = "studio.enabled", true, Scopes::PROCESS, HardBounds::NONE, "Serve Studio");
def!(PATH: Cow<'static, str> = "studio.path", Cow::Borrowed("/studio"), Scopes::PROCESS, HardBounds::NONE,
    "Mount path; not under /v1, /admin or /.well-known");
def!(LISTENERS: Cow<'static, str> = "studio.listeners", Cow::Borrowed("http,https"), Scopes::PROCESS,
    HardBounds::NONE, "Listeners serving Studio, comma-separated");
def!(ALLOW_INSECURE_HTTP: bool = "studio.allow_insecure_http", false, Scopes::PROCESS, HardBounds::NONE,
    "Allow login on plain HTTP from non-loopback clients");
def!(PUBLIC_ORIGINS: Cow<'static, str> = "studio.public_origins", Cow::Borrowed(""), Scopes::PROCESS,
    HardBounds::NONE, "Extra origins for the Origin check (reverse proxies), comma-separated");
def!(SESSION_IDLE_MINUTES: u64 = "studio.session_idle_minutes", 30, Scopes::TREE, HardBounds::range(1, 10_080),
    "Idle time before the session ends");
def!(SESSION_MAX_HOURS: u64 = "studio.session_max_hours", 12, Scopes::TREE, HardBounds::range(1, 720),
    "Longest session");
def!(PERSIST_DRAFTS: bool = "studio.persist_drafts", true, Scopes::TREE, HardBounds::NONE,
    "Keep drafts in the browser until logout");
def!(MAX_RESULT_ROWS: u64 = "studio.max_result_rows", 10_000, Scopes::DATA, HardBounds::range(1, 10_000_000),
    "Rows held per result");
def!(MAX_RESULT_MB: u64 = "studio.max_result_mb", 64, Scopes::PROCESS, HardBounds::range(1, 4096),
    "Browser memory per result");
def!(STATEMENT_TIMEOUT_MS: u64 = "studio.statement_timeout_ms", 30_000, Scopes::DATA,
    HardBounds::range(100, 86_400_000), "Statement timeout");
def!(CONFIRM_WRITES: Cow<'static, str> = "studio.confirm_writes", Cow::Borrowed("ddl"), Scopes::DATA,
    HardBounds::one_of(&["off", "ddl", "all"]), "off, ddl or all");
def!(EXPORT_TICKET_SECONDS: u64 = "studio.export_ticket_seconds", 60, Scopes::PROCESS, HardBounds::range(5, 3600),
    "Life of an export ticket");
def!(IMPORT_BATCH_ROWS: u64 = "studio.import_batch_rows", 1000, TENANT_GLOBAL, HardBounds::range(1, 100_000),
    "Rows per import transaction");
def!(IMPORT_MAX_MB: u64 = "studio.import_max_mb", 1024, TENANT_GLOBAL, HardBounds::range(1, 65_536),
    "Largest import file");
def!(FEED_VIEW_MAX_EVENTS: u64 = "studio.feed_view_max_events", 5000, TENANT_GLOBAL,
    HardBounds::range(10, 1_000_000), "Events kept in the feed view");
def!(GRAPH_MAX_ELEMENTS: u64 = "studio.graph_max_elements", 2000, TENANT_GLOBAL, HardBounds::range(10, 1_000_000),
    "Nodes plus relationships drawn");
def!(MAP_TILE_URL: Cow<'static, str> = "studio.map_tile_url", Cow::Borrowed(""), Scopes::TREE, HardBounds::NONE,
    "Base-map tiles; origin added to the CSP");
def!(VIEW_AS: Cow<'static, str> = "studio.view_as", Cow::Borrowed("on"), Scopes::TREE,
    HardBounds::one_of(&["on", "off"]), "Allow the row-level security tester");
def!(OPERATOR_MODE: Cow<'static, str> = "studio.operator_mode", Cow::Borrowed("on"), Scopes::PROCESS,
    HardBounds::one_of(&["on", "off"]), "Offer operator mode");
def!(OPERATOR_MTLS: Cow<'static, str> = "studio.operator_mtls", Cow::Borrowed("off"), Scopes::PROCESS,
    HardBounds::one_of(&["off", "required"]), "off or required: client certificate for operator mode");
def!(OPERATOR_REQUIRE_MFA: bool = "studio.operator_require_mfa", false, GLOBAL, HardBounds::NONE,
    "Second factor for operator mode");

/// What serving and the session endpoints read, resolved at this
/// gateway's scopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StudioConfig {
    /// `studio.enabled`.
    pub enabled: bool,
    /// `studio.path`, normalized: a leading `/`, no trailing `/`.
    pub path: String,
    /// `studio.listeners`.
    pub listeners: Vec<String>,
    /// The Content-Security-Policy, built from `studio.map_tile_url`.
    pub csp: String,
    /// `studio.public_origins`.
    pub public_origins: Vec<String>,
    /// `studio.allow_insecure_http`.
    pub allow_insecure_http: bool,
    /// `studio.session_idle_minutes`.
    pub session_idle_minutes: u64,
    /// `studio.session_max_hours`.
    pub session_max_hours: u64,
    /// `studio.export_ticket_seconds`.
    pub export_ticket_seconds: u64,
    /// The values the browser needs (`WhoAmI`'s `studio`).
    pub client: Value,
}

impl Default for StudioConfig {
    fn default() -> Self {
        Self::from_lookup(&Defaults)
    }
}

/// Where values come from: the live mirror, or the defaults.
trait Lookup {
    fn b(&self, s: &'static Setting<bool>) -> bool;
    fn n(&self, s: &'static Setting<u64>) -> u64;
    fn t(&self, s: &'static Setting<Cow<'static, str>>) -> String;
}

struct Defaults;

impl Lookup for Defaults {
    fn b(&self, s: &'static Setting<bool>) -> bool {
        s.default
    }
    fn n(&self, s: &'static Setting<u64>) -> u64 {
        s.default
    }
    fn t(&self, s: &'static Setting<Cow<'static, str>>) -> String {
        s.default.to_string()
    }
}

struct Live<'a> {
    snap: sprig_settings::Snapshot,
    ctx: SettingCtx<'a>,
}

impl Lookup for Live<'_> {
    fn b(&self, s: &'static Setting<bool>) -> bool {
        self.snap.get(s, &self.ctx)
    }
    fn n(&self, s: &'static Setting<u64>) -> u64 {
        self.snap.get(s, &self.ctx)
    }
    fn t(&self, s: &'static Setting<Cow<'static, str>>) -> String {
        self.snap.get(s, &self.ctx).to_string()
    }
}

/// `"a, b,,c"` → `["a", "b", "c"]`.
#[must_use]
pub fn split_list(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// `studio.path` as served: a leading `/`, no trailing `/`; a value under
/// `/v1`, `/admin` or `/.well-known`, or empty, falls back to `/studio`.
#[must_use]
pub fn normalize_path(p: &str) -> String {
    let t = p.trim().trim_end_matches('/');
    let t = if t.starts_with('/') {
        t.to_owned()
    } else {
        format!("/{t}")
    };
    let reserved = ["/v1", "/admin", "/.well-known"]
        .iter()
        .any(|r| t == *r || t.starts_with(&format!("{r}/")));
    if t == "/" || reserved || t.contains("..") || t.contains('?') || t.contains('#') {
        "/studio".to_owned()
    } else {
        t
    }
}

/// The origin (`scheme://host[:port]`) of a URL, or `None`.
#[must_use]
pub fn origin_of(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    if !matches!(scheme, "https" | "http") {
        return None;
    }
    let host = rest.split(['/', '?', '#']).next()?;
    if host.is_empty()
        || host
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, ';' | '\'' | '"' | ','))
    {
        return None;
    }
    Some(format!("{scheme}://{host}"))
}

/// The Content-Security-Policy of chapter 44, with the tile server's
/// origin when `studio.map_tile_url` names one.
#[must_use]
pub fn csp(map_tile_url: &str) -> String {
    let tiles = origin_of(map_tile_url)
        .map(|o| format!(" {o}"))
        .unwrap_or_default();
    format!(
        "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data: blob:{tiles}; font-src 'self'; connect-src 'self'{tiles}; \
         worker-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; \
         object-src 'none'"
    )
}

impl StudioConfig {
    /// The values at `dc`'s process scopes in `live`.
    #[must_use]
    pub fn resolve(live: &LiveSettings, dc: &DcId) -> Self {
        Self::from_lookup(&Live {
            snap: live.snapshot(),
            ctx: SettingCtx::process(dc),
        })
    }

    fn from_lookup(l: &dyn Lookup) -> Self {
        let tiles = l.t(&MAP_TILE_URL);
        Self {
            enabled: l.b(&ENABLED),
            path: normalize_path(&l.t(&PATH)),
            listeners: split_list(&l.t(&LISTENERS)),
            csp: csp(&tiles),
            public_origins: split_list(&l.t(&PUBLIC_ORIGINS)),
            allow_insecure_http: l.b(&ALLOW_INSECURE_HTTP),
            session_idle_minutes: l.n(&SESSION_IDLE_MINUTES),
            session_max_hours: l.n(&SESSION_MAX_HOURS),
            export_ticket_seconds: l.n(&EXPORT_TICKET_SECONDS),
            client: json!({
                "session_idle_minutes": l.n(&SESSION_IDLE_MINUTES),
                "session_max_hours": l.n(&SESSION_MAX_HOURS),
                "persist_drafts": l.b(&PERSIST_DRAFTS),
                "max_result_rows": l.n(&MAX_RESULT_ROWS),
                "max_result_mb": l.n(&MAX_RESULT_MB),
                "statement_timeout_ms": l.n(&STATEMENT_TIMEOUT_MS),
                "confirm_writes": l.t(&CONFIRM_WRITES),
                "export_ticket_seconds": l.n(&EXPORT_TICKET_SECONDS),
                "import_batch_rows": l.n(&IMPORT_BATCH_ROWS),
                "import_max_mb": l.n(&IMPORT_MAX_MB),
                "feed_view_max_events": l.n(&FEED_VIEW_MAX_EVENTS),
                "graph_max_elements": l.n(&GRAPH_MAX_ELEMENTS),
                "map_tile_url": tiles,
                "view_as": l.t(&VIEW_AS),
                "operator_mode": l.t(&OPERATOR_MODE),
            }),
        }
    }

    /// Whether Studio is served on `listener`.
    #[must_use]
    pub fn on_listener(&self, listener: &str) -> bool {
        self.listeners.iter().any(|l| l == listener)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_are_normalized_and_reserved_prefixes_refused() {
        assert_eq!(normalize_path("/studio/"), "/studio");
        assert_eq!(normalize_path("ui"), "/ui");
        assert_eq!(normalize_path("/v1"), "/studio");
        assert_eq!(normalize_path("/v1/x"), "/studio");
        assert_eq!(normalize_path("/admin/ui"), "/studio");
        assert_eq!(normalize_path("/.well-known"), "/studio");
        assert_eq!(normalize_path("/"), "/studio");
        assert_eq!(normalize_path("/a/../v1"), "/studio");
        assert_eq!(normalize_path("/v1studio"), "/v1studio");
    }

    #[test]
    fn csp_adds_only_a_clean_tile_origin() {
        let base = csp("");
        assert!(base.contains("script-src 'self';"));
        assert!(base.contains("connect-src 'self';"));
        assert!(!base.contains("unsafe-eval"));
        let t = csp("https://tiles.example.com/{z}/{x}/{y}.png");
        assert!(t.contains("img-src 'self' data: blob: https://tiles.example.com;"));
        assert!(t.contains("connect-src 'self' https://tiles.example.com;"));
        // Anything that could smuggle a directive is dropped.
        assert_eq!(csp("https://a.com; script-src *"), base);
        assert_eq!(csp("javascript:alert(1)"), base);
    }

    #[test]
    fn defaults_match_the_chapter() {
        let c = StudioConfig::default();
        assert!(c.enabled);
        assert_eq!(c.path, "/studio");
        assert_eq!(c.listeners, ["http", "https"]);
        assert!(!c.allow_insecure_http);
        assert_eq!(c.session_idle_minutes, 30);
        assert_eq!(c.session_max_hours, 12);
        assert_eq!(c.client["max_result_rows"], 10_000);
        assert_eq!(c.client["confirm_writes"], "ddl");
        assert_eq!(split_list(" a, ,b "), ["a", "b"]);
    }

    #[test]
    fn keys_are_registered() {
        let reg = sprig_settings::Registry::global().expect("registry");
        for k in [
            "studio.enabled",
            "studio.path",
            "studio.max_result_rows",
            "studio.operator_require_mfa",
        ] {
            assert!(reg.get(k).is_some(), "{k}");
        }
    }
}
