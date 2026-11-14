//! The pass-through executor's live settings: read per statement, so a
//! change applies to the next one. The tenant scopes can't apply before the
//! tenant is known;
//! they resolve at the process scopes for now.

use sprig_common::ids::DcId;
use sprig_proto_mysql::Persona;
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

/// `gateway.long_data_max_bytes`.
pub static LONG_DATA_MAX_BYTES: Setting<u64> = Setting {
    key: "gateway.long_data_max_bytes",
    default: 67_108_864,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 1 << 30),
    apply: Apply::Live,
    operator_only: false,
    owner: 5,
    doc: "Buffered long data per prepared statement; more fails the execute with 39011",
};
sprig_settings::register!(LONG_DATA_MAX_BYTES);

/// `sql.local_infile`.
pub static LOCAL_INFILE: Setting<bool> = Setting {
    key: "sql.local_infile",
    default: true,
    scopes: Scopes::DATA,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 5,
    doc: "Allow LOAD DATA LOCAL INFILE; per statement",
};
sprig_settings::register!(LOCAL_INFILE);

/// The values one statement reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecSettings {
    /// `gateway.long_data_max_bytes`.
    pub long_data_max: usize,
    /// `sql.local_infile`.
    pub local_infile: bool,
}

impl Default for ExecSettings {
    fn default() -> Self {
        Self {
            long_data_max: usize::try_from(LONG_DATA_MAX_BYTES.default).unwrap_or(usize::MAX),
            local_infile: LOCAL_INFILE.default,
        }
    }
}

/// Where the executor reads its settings.
#[derive(Clone)]
pub enum ExecSettingsSource {
    /// Fixed values (tests, development without etcd).
    Fixed(ExecSettings),
    /// The live mirror, resolved for this gateway's DC.
    Live {
        /// The settings mirror.
        live: LiveSettings,
        /// This gateway's DC.
        dc: DcId,
    },
}

impl std::fmt::Debug for ExecSettingsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecSettingsSource::Fixed(s) => f.debug_tuple("Fixed").field(s).finish(),
            ExecSettingsSource::Live { dc, .. } => f.debug_struct("Live").field("dc", dc).finish(),
        }
    }
}

impl ExecSettingsSource {
    /// The values now.
    #[must_use]
    pub fn current(&self) -> ExecSettings {
        match self {
            ExecSettingsSource::Fixed(s) => *s,
            ExecSettingsSource::Live { live, dc } => {
                let snap = live.snapshot();
                let ctx = SettingCtx::process(dc);
                ExecSettings {
                    long_data_max: usize::try_from(snap.get(&LONG_DATA_MAX_BYTES, &ctx))
                        .unwrap_or(usize::MAX),
                    local_infile: snap.get(&LOCAL_INFILE, &ctx),
                }
            }
        }
    }

    /// The persona's version string, as the greeting reports it
    /// (`gateway.mysql_server_version`, `gateway.mariadb_server_version`):
    /// what `@@version` and `VERSION()` answer.
    #[must_use]
    pub fn persona_version(&self, persona: Persona) -> String {
        use crate::mysql::settings::{MARIADB_SERVER_VERSION, MYSQL_SERVER_VERSION};
        let setting = match persona {
            Persona::MySql84 => &MYSQL_SERVER_VERSION,
            Persona::MariaDb => &MARIADB_SERVER_VERSION,
        };
        match self {
            ExecSettingsSource::Fixed(_) => setting.default.clone().into_owned(),
            ExecSettingsSource::Live { live, dc } => {
                // Asked several times per statement (the
                // lexer's options); resolved once per settings snapshot.
                type Memo = Option<(sprig_settings::Snapshot, DcId, [String; 2])>;
                static MEMO: std::sync::Mutex<Memo> = std::sync::Mutex::new(None);
                let snap = live.snapshot();
                let i = usize::from(persona == Persona::MariaDb);
                let mut memo = MEMO
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some((s, d, v)) = memo.as_ref()
                    && std::sync::Arc::ptr_eq(s, &snap)
                    && d == dc
                {
                    return v[i].clone();
                }
                let ctx = SettingCtx::process(dc);
                let v = [
                    snap.get(&MYSQL_SERVER_VERSION, &ctx).into_owned(),
                    snap.get(&MARIADB_SERVER_VERSION, &ctx).into_owned(),
                ];
                let out = v[i].clone();
                *memo = Some((snap, dc.clone(), v));
                out
            }
        }
    }
}
