//! The listener keys and their resolution into [`ListenerSpec`]s (chapter
//! 04, "Configuration").
//!
//! The registry holds fixed keys, so each listener's four keys and the
//! `default` bundle's two are declared one by one; other bundle names can't
//! be named until the registry supports key patterns.
//! `gateway.proxy_trusted_cidrs` is a comma-separated string because setting
//! values have no list type yet.

use std::borrow::Cow;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx};

use super::proxy::{ProxyMode, ProxyPolicy};
use super::{ListenerRejected, ListenerSpec, TlsMode, TlsVersion};
use crate::frontend::Protocol;
use crate::tls::CertStore;

type Text = Cow<'static, str>;

const TLS_MODES: &[&str] = &["required", "optional", "off"];

/// The keys of one listener.
#[derive(Debug)]
pub struct ListenerKeys {
    /// The listener name in `listener.<name>.*`.
    pub name: &'static str,
    /// The protocol it serves.
    pub protocol: Protocol,
    /// `listener.<name>.bind`.
    pub bind: &'static Setting<Text>,
    /// `listener.<name>.enabled`.
    pub enabled: &'static Setting<bool>,
    /// `listener.<name>.tls`.
    pub tls: &'static Setting<Text>,
    /// `listener.<name>.cert`.
    pub cert: &'static Setting<Text>,
}

macro_rules! listener {
    ($name:literal, $bind:literal, $enabled:literal, $tls:literal,
     $BIND:ident, $ENABLED:ident, $TLS:ident, $CERT:ident) => {
        #[doc = concat!("`listener.", $name, ".bind`.")]
        pub static $BIND: Setting<Text> = Setting {
            key: concat!("listener.", $name, ".bind"),
            default: Cow::Borrowed($bind),
            scopes: Scopes::PROCESS,
            hard: HardBounds::NONE,
            apply: Apply::Rebind,
            operator_only: false,
            owner: 4,
            doc: concat!("Address and port of the ", $name, " listener"),
        };
        sprig_settings::register!($BIND);
        #[doc = concat!("`listener.", $name, ".enabled`.")]
        pub static $ENABLED: Setting<bool> = Setting {
            key: concat!("listener.", $name, ".enabled"),
            default: $enabled,
            scopes: Scopes::PROCESS,
            hard: HardBounds::NONE,
            apply: Apply::Rebind,
            operator_only: false,
            owner: 4,
            doc: concat!("Serve the ", $name, " listener"),
        };
        sprig_settings::register!($ENABLED);
        #[doc = concat!("`listener.", $name, ".tls`.")]
        pub static $TLS: Setting<Text> = Setting {
            key: concat!("listener.", $name, ".tls"),
            default: Cow::Borrowed($tls),
            scopes: Scopes::PROCESS,
            hard: HardBounds::one_of(TLS_MODES),
            apply: Apply::Rebind,
            operator_only: false,
            owner: 4,
            doc: concat!(
                "TLS mode of the ",
                $name,
                " listener: required, optional or off"
            ),
        };
        sprig_settings::register!($TLS);
        #[doc = concat!("`listener.", $name, ".cert`.")]
        pub static $CERT: Setting<Text> = Setting {
            key: concat!("listener.", $name, ".cert"),
            default: Cow::Borrowed("default"),
            scopes: Scopes::PROCESS,
            hard: HardBounds::NONE,
            apply: Apply::Rebind,
            operator_only: false,
            owner: 4,
            doc: concat!("Certificate bundle of the ", $name, " listener"),
        };
        sprig_settings::register!($CERT);
    };
}

listener!(
    "mysql",
    "0.0.0.0:3306",
    true,
    "required",
    MYSQL_BIND,
    MYSQL_ENABLED,
    MYSQL_TLS,
    MYSQL_CERT
);
listener!(
    "mariadb",
    "0.0.0.0:3307",
    true,
    "required",
    MARIADB_BIND,
    MARIADB_ENABLED,
    MARIADB_TLS,
    MARIADB_CERT
);
listener!(
    "http",
    "0.0.0.0:8080",
    true,
    "off",
    HTTP_BIND,
    HTTP_ENABLED,
    HTTP_TLS,
    HTTP_CERT
);
listener!(
    "https",
    "0.0.0.0:8443",
    true,
    "required",
    HTTPS_BIND,
    HTTPS_ENABLED,
    HTTPS_TLS,
    HTTPS_CERT
);
listener!(
    "mongo",
    "0.0.0.0:27017",
    false,
    "required",
    MONGO_BIND,
    MONGO_ENABLED,
    MONGO_TLS,
    MONGO_CERT
);
listener!(
    "redis",
    "0.0.0.0:6379",
    false,
    "required",
    REDIS_BIND,
    REDIS_ENABLED,
    REDIS_TLS,
    REDIS_CERT
);
listener!(
    "bolt",
    "0.0.0.0:7687",
    false,
    "required",
    BOLT_BIND,
    BOLT_ENABLED,
    BOLT_TLS,
    BOLT_CERT
);
listener!(
    "kafka",
    "0.0.0.0:9092",
    false,
    "required",
    KAFKA_BIND,
    KAFKA_ENABLED,
    KAFKA_TLS,
    KAFKA_CERT
);

/// Every client listener the manager runs. Telemetry (9180) is bound by the
/// `sprigdb` binary before the role starts, and the peer listener (7403,
/// fixed mutual TLS) is build step 10.
pub static LISTENERS: [ListenerKeys; 8] = [
    ListenerKeys {
        name: "mysql",
        protocol: Protocol::MySql84,
        bind: &MYSQL_BIND,
        enabled: &MYSQL_ENABLED,
        tls: &MYSQL_TLS,
        cert: &MYSQL_CERT,
    },
    ListenerKeys {
        name: "mariadb",
        protocol: Protocol::MariaDb,
        bind: &MARIADB_BIND,
        enabled: &MARIADB_ENABLED,
        tls: &MARIADB_TLS,
        cert: &MARIADB_CERT,
    },
    ListenerKeys {
        name: "http",
        protocol: Protocol::Http,
        bind: &HTTP_BIND,
        enabled: &HTTP_ENABLED,
        tls: &HTTP_TLS,
        cert: &HTTP_CERT,
    },
    ListenerKeys {
        name: "https",
        protocol: Protocol::Http,
        bind: &HTTPS_BIND,
        enabled: &HTTPS_ENABLED,
        tls: &HTTPS_TLS,
        cert: &HTTPS_CERT,
    },
    ListenerKeys {
        name: "mongo",
        protocol: Protocol::Mongo,
        bind: &MONGO_BIND,
        enabled: &MONGO_ENABLED,
        tls: &MONGO_TLS,
        cert: &MONGO_CERT,
    },
    ListenerKeys {
        name: "redis",
        protocol: Protocol::Redis,
        bind: &REDIS_BIND,
        enabled: &REDIS_ENABLED,
        tls: &REDIS_TLS,
        cert: &REDIS_CERT,
    },
    ListenerKeys {
        name: "bolt",
        protocol: Protocol::Bolt,
        bind: &BOLT_BIND,
        enabled: &BOLT_ENABLED,
        tls: &BOLT_TLS,
        cert: &BOLT_CERT,
    },
    ListenerKeys {
        name: "kafka",
        protocol: Protocol::Kafka,
        bind: &KAFKA_BIND,
        enabled: &KAFKA_ENABLED,
        tls: &KAFKA_TLS,
        cert: &KAFKA_CERT,
    },
];

/// `tls.bundles.default.cert_path`.
pub static DEFAULT_BUNDLE_CERT: Setting<Text> = Setting {
    key: "tls.bundles.default.cert_path",
    default: Cow::Borrowed(""),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 4,
    doc: "PEM certificate chain of the default bundle, leaf first; watched for changes",
};
sprig_settings::register!(DEFAULT_BUNDLE_CERT);

/// `tls.bundles.default.key_path`.
pub static DEFAULT_BUNDLE_KEY: Setting<Text> = Setting {
    key: "tls.bundles.default.key_path",
    default: Cow::Borrowed(""),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 4,
    doc: "PEM private key of the default bundle; watched for changes",
};
sprig_settings::register!(DEFAULT_BUNDLE_KEY);

/// `tls.min_version`.
pub static TLS_MIN_VERSION: Setting<Text> = Setting {
    key: "tls.min_version",
    default: Cow::Borrowed("1.2"),
    scopes: Scopes::PROCESS,
    hard: HardBounds::one_of(&["1.2", "1.3"]),
    apply: Apply::Rebind,
    operator_only: false,
    owner: 4,
    doc: "Lowest TLS version client listeners accept",
};
sprig_settings::register!(TLS_MIN_VERSION);

/// `listener.drain_seconds`.
pub static DRAIN_SECONDS: Setting<u64> = Setting {
    key: "listener.drain_seconds",
    default: 600,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 86_400),
    apply: Apply::Live,
    operator_only: false,
    owner: 4,
    doc: "Grace before sessions of a stricter or retired listener close",
};
sprig_settings::register!(DRAIN_SECONDS);

/// `gateway.proxy_protocol`.
pub static PROXY_PROTOCOL: Setting<Text> = Setting {
    key: "gateway.proxy_protocol",
    default: Cow::Borrowed("off"),
    scopes: Scopes::PROCESS,
    hard: HardBounds::one_of(&["off", "optional", "required"]),
    apply: Apply::Rebind,
    operator_only: false,
    owner: 4,
    doc: "PROXY protocol v1/v2 from trusted peers: off, optional or required",
};
sprig_settings::register!(PROXY_PROTOCOL);

/// `gateway.proxy_trusted_cidrs`.
pub static PROXY_TRUSTED_CIDRS: Setting<Text> = Setting {
    key: "gateway.proxy_trusted_cidrs",
    default: Cow::Borrowed(""),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Rebind,
    operator_only: false,
    owner: 4,
    doc: "Comma-separated CIDRs allowed to send a PROXY header",
};
sprig_settings::register!(PROXY_TRUSTED_CIDRS);

/// `gateway.handshake_timeout_ms`.
pub static HANDSHAKE_TIMEOUT_MS: Setting<u64> = Setting {
    key: "gateway.handshake_timeout_ms",
    default: 10_000,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(100, 600_000),
    apply: Apply::Rebind,
    operator_only: false,
    owner: 4,
    doc: "Longest connection phase: PROXY header, TLS handshake and login",
};
sprig_settings::register!(HANDSHAKE_TIMEOUT_MS);

/// `gateway.drain_timeout_seconds`.
pub static DRAIN_TIMEOUT_SECONDS: Setting<u64> = Setting {
    key: "gateway.drain_timeout_seconds",
    default: 30,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 86_400),
    apply: Apply::Live,
    operator_only: false,
    owner: 4,
    doc: "Longest wait for busy sessions when the gateway drains",
};
sprig_settings::register!(DRAIN_TIMEOUT_SECONDS);

/// Listener specs resolved from settings, plus what couldn't be resolved.
#[derive(Debug, Default)]
pub struct Resolution {
    /// One spec per listener that resolved.
    pub specs: Vec<ListenerSpec>,
    /// Listeners whose settings are invalid; the running listener is kept.
    pub rejected: Vec<ListenerRejected>,
    /// `listener.drain_seconds`.
    pub drain: Duration,
}

/// Resolves every listener in [`LISTENERS`] from `settings` in `ctx`,
/// loading the certificate bundles they name through `certs`.
pub fn resolve(settings: &MemSettings<'_>, ctx: &SettingCtx<'_>, certs: &CertStore) -> Resolution {
    let mut res = Resolution {
        drain: Duration::from_secs(settings.get(&DRAIN_SECONDS, ctx)),
        ..Resolution::default()
    };
    let min_tls = match settings.get(&TLS_MIN_VERSION, ctx).as_ref() {
        "1.3" => TlsVersion::V1_3,
        _ => TlsVersion::V1_2,
    };
    let handshake_timeout = Duration::from_millis(settings.get(&HANDSHAKE_TIMEOUT_MS, ctx));
    let proxy = settings
        .get(&PROXY_PROTOCOL, ctx)
        .parse::<ProxyMode>()
        .and_then(|mode| {
            Ok(ProxyPolicy {
                mode,
                trusted: ProxyPolicy::parse_cidrs(&settings.get(&PROXY_TRUSTED_CIDRS, ctx))?,
            })
        });
    for keys in &LISTENERS {
        let reject = |reason: String| ListenerRejected {
            name: keys.name,
            reason,
        };
        let proxy = match &proxy {
            Ok(p) => p.clone(),
            Err(e) => {
                res.rejected.push(reject(e.clone()));
                continue;
            }
        };
        let bind = settings.get(keys.bind, ctx);
        let Ok(bind) = bind.parse::<SocketAddr>() else {
            res.rejected.push(reject(format!(
                "{} = {bind:?} is not an address and port",
                keys.bind.key
            )));
            continue;
        };
        let enabled = settings.get(keys.enabled, ctx);
        let Ok(tls) = settings.get(keys.tls, ctx).parse::<TlsMode>() else {
            res.rejected
                .push(reject(format!("{} is not a TLS mode", keys.tls.key)));
            continue;
        };
        let cert = if enabled && tls != TlsMode::Off {
            match bundle(settings, ctx, certs, &settings.get(keys.cert, ctx)) {
                Ok(b) => Some(b),
                Err(e) => {
                    res.rejected.push(reject(e));
                    continue;
                }
            }
        } else {
            None
        };
        res.specs.push(ListenerSpec {
            name: keys.name,
            protocol: keys.protocol,
            bind,
            enabled,
            tls,
            cert,
            min_tls,
            proxy: proxy.clone(),
            handshake_timeout,
        });
    }
    res
}

fn bundle(
    settings: &MemSettings<'_>,
    ctx: &SettingCtx<'_>,
    certs: &CertStore,
    name: &str,
) -> Result<Arc<crate::tls::CertBundle>, String> {
    if name != "default" {
        return Err(format!(
            "certificate bundle {name:?}: only the default bundle can be configured yet"
        ));
    }
    let cert = settings.get(&DEFAULT_BUNDLE_CERT, ctx);
    let key = settings.get(&DEFAULT_BUNDLE_KEY, ctx);
    if cert.is_empty() || key.is_empty() {
        return Err(format!(
            "TLS needs a certificate: set {} and {}",
            DEFAULT_BUNDLE_CERT.key, DEFAULT_BUNDLE_KEY.key
        ));
    }
    certs
        .bundle(name, Path::new(cert.as_ref()), Path::new(key.as_ref()))
        .map_err(|e| e.message)
}
