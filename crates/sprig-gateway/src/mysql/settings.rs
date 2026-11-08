//! The MySQL and MariaDB front end's live settings (chapter 05,
//! "Configuration"; Appendix B). Each is read when a connection's greeting
//! is built, so a change reaches new connections only.
//!
//! `tls.min_version` is a listener key ([`crate::listener::settings`]):
//! raising it re-applies the listeners and retires older sessions through
//! [`super::listener_hooks`].
//!
//! `gateway.compression` is a comma-separated string, as list values don't
//! exist in the registry yet (like `gateway.proxy_trusted_cidrs`). The
//! tenant scope of the per-tenant keys can't apply before the tenant is
//! known (M0.3); they resolve at the process scopes for now.

use std::borrow::Cow;

use sprig_common::ids::DcId;
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::handshake::CompressionOffer;
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

type Text = Cow<'static, str>;

/// Tenant (and every ancestor), dc and global.
const TENANT_DC_GLOBAL: Scopes = Scopes::TREE;

/// `gateway.mysql_server_version`.
pub static MYSQL_SERVER_VERSION: Setting<Text> = Setting {
    key: "gateway.mysql_server_version",
    default: Cow::Borrowed("8.4.8-SprigDB"),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 5,
    doc: "Greeting on the mysql listener",
};
sprig_settings::register!(MYSQL_SERVER_VERSION);

/// `gateway.mariadb_server_version`.
pub static MARIADB_SERVER_VERSION: Setting<Text> = Setting {
    key: "gateway.mariadb_server_version",
    default: Cow::Borrowed("11.8.3-MariaDB-SprigDB"),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 5,
    doc: "Greeting on mariadb; no newer than the oldest backend",
};
sprig_settings::register!(MARIADB_SERVER_VERSION);

/// `gateway.max_allowed_packet_bytes`.
pub static MAX_ALLOWED_PACKET_BYTES: Setting<u64> = Setting {
    key: "gateway.max_allowed_packet_bytes",
    default: 67_108_864,
    scopes: TENANT_DC_GLOBAL,
    hard: HardBounds::range(1024, 1 << 30),
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 5,
    doc: "Largest logical packet (MySQL 8.4's default)",
};
sprig_settings::register!(MAX_ALLOWED_PACKET_BYTES);

/// `gateway.max_connect_attrs_bytes`.
pub static MAX_CONNECT_ATTRS_BYTES: Setting<u64> = Setting {
    key: "gateway.max_connect_attrs_bytes",
    default: 65_536,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 1 << 20),
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 5,
    doc: "Connection attributes kept",
};
sprig_settings::register!(MAX_CONNECT_ATTRS_BYTES);

/// `gateway.max_prepared_per_session`.
pub static MAX_PREPARED_PER_SESSION: Setting<u64> = Setting {
    key: "gateway.max_prepared_per_session",
    default: 1024,
    scopes: TENANT_DC_GLOBAL,
    hard: HardBounds::range(0, 1_048_576),
    apply: Apply::Live,
    operator_only: false,
    owner: 5,
    doc: "Prepared statements per session; next prepare",
};
sprig_settings::register!(MAX_PREPARED_PER_SESSION);

/// `gateway.compression`.
pub static COMPRESSION: Setting<Text> = Setting {
    key: "gateway.compression",
    default: Cow::Borrowed("zlib,zstd"),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 5,
    doc: "Compression algorithms offered, comma-separated: zlib, zstd",
};
sprig_settings::register!(COMPRESSION);

/// `gateway.relay_buffer_kb` (chapter 04).
pub static RELAY_BUFFER_KB: Setting<u64> = Setting {
    key: "gateway.relay_buffer_kb",
    default: 256,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(8, 65_536),
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 4,
    doc: "Results waiting for a slow client",
};
sprig_settings::register!(RELAY_BUFFER_KB);

/// What one connection reads from the settings when its greeting is built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendSettings {
    /// `gateway.mysql_server_version`.
    pub mysql_version: String,
    /// `gateway.mariadb_server_version`.
    pub mariadb_version: String,
    /// `gateway.max_allowed_packet_bytes`.
    pub max_allowed_packet: usize,
    /// `gateway.max_connect_attrs_bytes`.
    pub max_connect_attrs: usize,
    /// `gateway.max_prepared_per_session`.
    pub max_prepared: usize,
    /// `gateway.compression`.
    pub compression: CompressionOffer,
    /// `gateway.relay_buffer_kb`, in bytes.
    pub relay_buffer: usize,
}

impl Default for FrontendSettings {
    fn default() -> Self {
        Self {
            mysql_version: MYSQL_SERVER_VERSION.default.clone().into_owned(),
            mariadb_version: MARIADB_SERVER_VERSION.default.clone().into_owned(),
            max_allowed_packet: to_usize(MAX_ALLOWED_PACKET_BYTES.default),
            max_connect_attrs: to_usize(MAX_CONNECT_ATTRS_BYTES.default),
            max_prepared: to_usize(MAX_PREPARED_PER_SESSION.default),
            compression: CompressionOffer::parse(&COMPRESSION.default),
            relay_buffer: to_usize(RELAY_BUFFER_KB.default) * 1024,
        }
    }
}

fn to_usize(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

impl FrontendSettings {
    /// The greeting's version string for `persona`.
    #[must_use]
    pub fn version(&self, persona: Persona) -> &str {
        match persona {
            Persona::MySql84 => &self.mysql_version,
            Persona::MariaDb => &self.mariadb_version,
        }
    }

    /// Reads the current values.
    #[must_use]
    pub fn read(live: &LiveSettings, dc: &DcId) -> Self {
        let snap = live.snapshot();
        let ctx = SettingCtx::process(dc);
        Self {
            mysql_version: snap.get(&MYSQL_SERVER_VERSION, &ctx).into_owned(),
            mariadb_version: snap.get(&MARIADB_SERVER_VERSION, &ctx).into_owned(),
            max_allowed_packet: to_usize(snap.get(&MAX_ALLOWED_PACKET_BYTES, &ctx)),
            max_connect_attrs: to_usize(snap.get(&MAX_CONNECT_ATTRS_BYTES, &ctx)),
            max_prepared: to_usize(snap.get(&MAX_PREPARED_PER_SESSION, &ctx)),
            compression: CompressionOffer::parse(&snap.get(&COMPRESSION, &ctx)),
            relay_buffer: to_usize(snap.get(&RELAY_BUFFER_KB, &ctx)).saturating_mul(1024),
        }
    }
}

/// Where a front end reads its settings.
#[derive(Clone)]
pub enum SettingsSource {
    /// Fixed values (tests, development without etcd).
    Fixed(FrontendSettings),
    /// The live mirror, resolved for this gateway's DC.
    Live {
        /// The settings mirror.
        live: LiveSettings,
        /// This gateway's DC.
        dc: DcId,
    },
}

impl std::fmt::Debug for SettingsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SettingsSource::Fixed(s) => f.debug_tuple("Fixed").field(s).finish(),
            SettingsSource::Live { dc, .. } => f.debug_struct("Live").field("dc", dc).finish(),
        }
    }
}

impl SettingsSource {
    /// The values now.
    #[must_use]
    pub fn current(&self) -> FrontendSettings {
        match self {
            SettingsSource::Fixed(s) => s.clone(),
            SettingsSource::Live { live, dc } => FrontendSettings::read(live, dc),
        }
    }
}
