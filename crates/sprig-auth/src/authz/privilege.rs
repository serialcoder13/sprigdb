//! The privilege model (chapter 29, "The model"; build step 1): actions as
//! stable bits, the levels a grant sits at, and the MySQL and MariaDB
//! privilege names.
//!
//! An [`Action`]'s bit never changes and is never reused: [`ActionSet`]s
//! are stored in `privileges.actions`. Levels follow MySQL's: `*.*` is the
//! tenant (every logical database, now and later), `db.*` one database,
//! `db.t` one object (a table, view or routine), `db.t (c)` one column.
//!
//! **Tenant scoping.** Server-wide privileges (`SUPER`, `FILE`, `RELOAD`,
//! replication, ...) mean nothing inside a tenant; [`parse_privilege`]
//! reports them as [`PrivName::Dropped`] and the statement drops them with
//! warning 39350 (chapter 29, "Tenant scoping of `*.*`").

use std::fmt;

/// One action (chapter 29's action table). The discriminant is the bit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum Action {
    /// `SELECT`: read.
    Select = 0,
    /// `INSERT`.
    Insert = 1,
    /// `UPDATE`.
    Update = 2,
    /// `DELETE`.
    Delete = 3,
    /// `CREATE`: tables and databases.
    Create = 4,
    /// `DROP`.
    Drop = 5,
    /// `ALTER`.
    Alter = 6,
    /// `INDEX`.
    Index = 7,
    /// `EXECUTE`: routines and logic functions.
    Execute = 8,
    /// `CREATE ROUTINE`.
    CreateRoutine = 9,
    /// `ALTER ROUTINE`.
    AlterRoutine = 10,
    /// `TRIGGER`.
    Trigger = 11,
    /// `EVENT`.
    Event = 12,
    /// `GRANT OPTION`.
    GrantOption = 13,
    /// `CREATE USER`: manage users (tenant-scoped).
    CreateUser = 14,
    /// `CREATE ROLE` (MySQL).
    CreateRole = 15,
    /// `DROP ROLE` (MySQL).
    DropRole = 16,
    /// `ROLE_ADMIN`: grant and revoke any role.
    RoleAdmin = 17,
    /// The extension `WATCH` (change streams, R5).
    Watch = 18,
    /// `SHOW DATABASES`, tenant-scoped.
    ShowDatabases = 19,
    /// `PROCESS`, tenant-scoped.
    Process = 20,
    /// `REFERENCES`.
    References = 21,
    /// `CREATE VIEW`.
    CreateView = 22,
    /// `SHOW VIEW`.
    ShowView = 23,
    /// `LOCK TABLES`.
    LockTables = 24,
    /// `CREATE TEMPORARY TABLES`.
    CreateTemporaryTables = 25,
    /// `SPRIG_SETTINGS_ADMIN`: tenant-scope settings and bounds.
    SettingsAdmin = 26,
    /// `SPRIG_TENANT_ADMIN`: manage this tenant and its subtree.
    TenantAdmin = 27,
    /// The extension `BYPASS RLS` (R4).
    BypassRls = 28,
    /// `CONNECTION_ADMIN` (MariaDB `CONNECTION ADMIN`), tenant-scoped.
    ConnectionAdmin = 29,
    /// `SYSTEM_VARIABLES_ADMIN`, over chapter 28's allow-list.
    SystemVariablesAdmin = 30,
    /// `SET_ANY_DEFINER` (MariaDB `SET USER`): any tenant account as
    /// `DEFINER`.
    SetAnyDefiner = 31,
    /// `SPRIG_SINK_ADMIN`: create, alter and drop sinks and connections
    /// (chapter 51); granted by name only.
    SinkAdmin = 32,
    /// `SPRIG_FAN_IN`: read every member's instance of an inherited
    /// definition, as fan-in readers and sinks do (chapters 42, 51);
    /// granted by name only.
    FanIn = 33,
}

impl Action {
    /// Every action, in bit order.
    pub const ALL: [Action; 34] = [
        Action::Select,
        Action::Insert,
        Action::Update,
        Action::Delete,
        Action::Create,
        Action::Drop,
        Action::Alter,
        Action::Index,
        Action::Execute,
        Action::CreateRoutine,
        Action::AlterRoutine,
        Action::Trigger,
        Action::Event,
        Action::GrantOption,
        Action::CreateUser,
        Action::CreateRole,
        Action::DropRole,
        Action::RoleAdmin,
        Action::Watch,
        Action::ShowDatabases,
        Action::Process,
        Action::References,
        Action::CreateView,
        Action::ShowView,
        Action::LockTables,
        Action::CreateTemporaryTables,
        Action::SettingsAdmin,
        Action::TenantAdmin,
        Action::BypassRls,
        Action::ConnectionAdmin,
        Action::SystemVariablesAdmin,
        Action::SetAnyDefiner,
        Action::SinkAdmin,
        Action::FanIn,
    ];

    /// The bit.
    #[must_use]
    pub const fn bit(self) -> u64 {
        1 << (self as u8)
    }

    /// The command name MySQL and MariaDB print in 1142 (`SELECT command
    /// denied ...`).
    #[must_use]
    pub const fn command(self) -> &'static str {
        match self {
            Action::Select => "SELECT",
            Action::Insert => "INSERT",
            Action::Update => "UPDATE",
            Action::Delete => "DELETE",
            Action::Create => "CREATE",
            Action::Drop => "DROP",
            Action::Alter => "ALTER",
            Action::Index => "INDEX",
            Action::Execute => "EXECUTE",
            Action::CreateRoutine => "CREATE ROUTINE",
            Action::AlterRoutine => "ALTER ROUTINE",
            Action::Trigger => "TRIGGER",
            Action::Event => "EVENT",
            Action::GrantOption => "GRANT",
            Action::CreateUser => "CREATE USER",
            Action::CreateRole => "CREATE ROLE",
            Action::DropRole => "DROP ROLE",
            Action::RoleAdmin => "ROLE_ADMIN",
            Action::Watch => "WATCH",
            Action::ShowDatabases => "SHOW DATABASES",
            Action::Process => "PROCESS",
            Action::References => "REFERENCES",
            Action::CreateView => "CREATE VIEW",
            Action::ShowView => "SHOW VIEW",
            Action::LockTables => "LOCK TABLES",
            Action::CreateTemporaryTables => "CREATE TEMPORARY TABLES",
            Action::SettingsAdmin => "SPRIG_SETTINGS_ADMIN",
            Action::TenantAdmin => "SPRIG_TENANT_ADMIN",
            Action::BypassRls => "BYPASS RLS",
            Action::ConnectionAdmin => "CONNECTION_ADMIN",
            Action::SystemVariablesAdmin => "SYSTEM_VARIABLES_ADMIN",
            Action::SetAnyDefiner => "SET_ANY_DEFINER",
            Action::SinkAdmin => "SPRIG_SINK_ADMIN",
            Action::FanIn => "SPRIG_FAN_IN",
        }
    }

    /// Whether `SHOW GRANTS` prints it on the dynamic-privilege line
    /// (MySQL), after the static ones.
    #[must_use]
    pub const fn is_dynamic(self) -> bool {
        matches!(
            self,
            Action::RoleAdmin
                | Action::SettingsAdmin
                | Action::TenantAdmin
                | Action::ConnectionAdmin
                | Action::SystemVariablesAdmin
                | Action::SetAnyDefiner
                | Action::SinkAdmin
                | Action::FanIn
                | Action::Watch
                | Action::BypassRls
        )
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.command())
    }
}

/// A set of actions: the `privileges.actions` bits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ActionSet(pub u64);

impl fmt::Debug for ActionSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

impl ActionSet {
    /// No action.
    pub const EMPTY: ActionSet = ActionSet(0);

    /// The set of `actions`.
    #[must_use]
    pub const fn of(actions: &[Action]) -> Self {
        let mut bits = 0;
        let mut i = 0;
        while i < actions.len() {
            bits |= actions[i].bit();
            i += 1;
        }
        ActionSet(bits)
    }

    /// Whether `a` is in the set.
    #[must_use]
    pub const fn has(self, a: Action) -> bool {
        self.0 & a.bit() != 0
    }

    /// Whether every action of `other` is in the set.
    #[must_use]
    pub const fn contains(self, other: ActionSet) -> bool {
        self.0 & other.0 == other.0
    }

    /// Both sets.
    #[must_use]
    pub const fn union(self, other: ActionSet) -> Self {
        ActionSet(self.0 | other.0)
    }

    /// The actions of `self` not in `other`.
    #[must_use]
    pub const fn minus(self, other: ActionSet) -> Self {
        ActionSet(self.0 & !other.0)
    }

    /// The actions in both.
    #[must_use]
    pub const fn intersect(self, other: ActionSet) -> Self {
        ActionSet(self.0 & other.0)
    }

    /// Whether the set is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The set with `a`.
    #[must_use]
    pub const fn with(self, a: Action) -> Self {
        ActionSet(self.0 | a.bit())
    }

    /// The actions, in bit order. Unknown bits (from a newer binary) are
    /// skipped.
    pub fn iter(self) -> impl Iterator<Item = Action> {
        Action::ALL.into_iter().filter(move |a| self.has(*a))
    }
}

/// The level of a grant (`privileges.level`, the R0 subset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Level {
    /// `*.*`: the tenant.
    Tenant,
    /// `db.*`: one logical database, by name.
    Database,
    /// `db.obj`: one table, view or routine.
    Object,
    /// `db.t (c)`: one column.
    Column,
    /// One label or relationship type of a graph (chapter 43, R8):
    /// `GRANT READ ON GRAPH db.g NODES L`.
    GraphElement,
}

impl Level {
    /// The `privileges.level` text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Level::Tenant => "tenant",
            Level::Database => "database",
            Level::Object => "object",
            Level::Column => "column",
            Level::GraphElement => "graph_element",
        }
    }

    /// Parses the `privileges.level` text; `None` for levels of later
    /// releases (key-value prefixes, channels, graph elements), which this
    /// binary ignores.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "tenant" => Some(Level::Tenant),
            "database" => Some(Level::Database),
            "object" => Some(Level::Object),
            "column" => Some(Level::Column),
            "graph_element" => Some(Level::GraphElement),
            _ => None,
        }
    }
}

/// What kind of object a grant names (`ON TABLE`, `ON FUNCTION`, `ON
/// PROCEDURE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ObjectType {
    /// A table or view (the default).
    Table,
    /// A stored function.
    Function,
    /// A stored procedure.
    Procedure,
}

/// Actions grantable at the tenant level: every tenant-safe static and
/// dynamic privilege.
pub const TENANT_ACTIONS: ActionSet = ActionSet(u64::MAX >> 30);

/// Actions of `ALL [PRIVILEGES] ON *.*`: everything tenant-scoped but
/// `GRANT OPTION`, `SPRIG_TENANT_ADMIN`, `BYPASS RLS`, `SPRIG_SINK_ADMIN` and
/// `SPRIG_FAN_IN`, which are always
/// granted by name (chapter 29: the bootstrap admin holds `ALL` and
/// `SPRIG_TENANT_ADMIN`).
pub const ALL_TENANT: ActionSet = ActionSet(
    TENANT_ACTIONS.0
        & !Action::GrantOption.bit()
        & !Action::TenantAdmin.bit()
        & !Action::BypassRls.bit()
        & !Action::SinkAdmin.bit()
        & !Action::FanIn.bit(),
);

/// Actions grantable at the database level (MySQL's `db` table).
pub const DATABASE_ACTIONS: ActionSet = ActionSet::of(&[
    Action::Select,
    Action::Insert,
    Action::Update,
    Action::Delete,
    Action::Create,
    Action::Drop,
    Action::GrantOption,
    Action::References,
    Action::Index,
    Action::Alter,
    Action::CreateTemporaryTables,
    Action::LockTables,
    Action::CreateView,
    Action::ShowView,
    Action::CreateRoutine,
    Action::AlterRoutine,
    Action::Execute,
    Action::Event,
    Action::Trigger,
]);

/// Actions grantable on a table or view (MySQL's `tables_priv`).
pub const TABLE_ACTIONS: ActionSet = ActionSet::of(&[
    Action::Select,
    Action::Insert,
    Action::Update,
    Action::Delete,
    Action::Create,
    Action::Drop,
    Action::GrantOption,
    Action::References,
    Action::Index,
    Action::Alter,
    Action::CreateView,
    Action::ShowView,
    Action::Trigger,
]);

/// Actions grantable on a routine (MySQL's `procs_priv`).
pub const ROUTINE_ACTIONS: ActionSet =
    ActionSet::of(&[Action::Execute, Action::AlterRoutine, Action::GrantOption]);

/// Actions grantable on a column (MySQL's `columns_priv`).
pub const COLUMN_ACTIONS: ActionSet = ActionSet::of(&[
    Action::Select,
    Action::Insert,
    Action::Update,
    Action::References,
]);

/// The actions of `ALL [PRIVILEGES]` at a level, without `GRANT OPTION`.
#[must_use]
pub const fn all_at(level: Level, object: ObjectType) -> ActionSet {
    let set = match (level, object) {
        (Level::Tenant, _) => ALL_TENANT,
        (Level::Database, _) => DATABASE_ACTIONS,
        (Level::Object, ObjectType::Table) => TABLE_ACTIONS,
        (Level::Object, _) => ROUTINE_ACTIONS,
        (Level::Column, _) => COLUMN_ACTIONS,
        (Level::GraphElement, _) => ActionSet::of(&[Action::Select]),
    };
    set.minus(ActionSet::of(&[Action::GrantOption]))
}

/// The actions a level accepts.
#[must_use]
pub const fn grantable_at(level: Level, object: ObjectType) -> ActionSet {
    match (level, object) {
        (Level::Tenant, _) => TENANT_ACTIONS,
        (Level::Database, _) => DATABASE_ACTIONS,
        (Level::Object, ObjectType::Table) => TABLE_ACTIONS,
        (Level::Object, _) => ROUTINE_ACTIONS,
        (Level::Column, _) => COLUMN_ACTIONS,
        (Level::GraphElement, _) => ActionSet::of(&[Action::Select]),
    }
}

/// A privilege name in `GRANT` or `REVOKE`, recognized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivName {
    /// A tenant-safe privilege.
    Action(Action),
    /// `ALL [PRIVILEGES]`.
    All,
    /// `USAGE`: no privilege.
    Usage,
    /// A server-wide privilege, dropped with warning 39350.
    Dropped,
    /// Not a privilege either server knows.
    Unknown,
}

/// Static privilege names of MySQL 8.4 and MariaDB 11.8 (upper case,
/// single spaces) and their actions.
const NAMES: &[(&str, Action)] = &[
    ("SELECT", Action::Select),
    ("INSERT", Action::Insert),
    ("UPDATE", Action::Update),
    ("DELETE", Action::Delete),
    ("CREATE", Action::Create),
    ("DROP", Action::Drop),
    ("ALTER", Action::Alter),
    ("INDEX", Action::Index),
    ("EXECUTE", Action::Execute),
    ("CREATE ROUTINE", Action::CreateRoutine),
    ("ALTER ROUTINE", Action::AlterRoutine),
    ("TRIGGER", Action::Trigger),
    ("EVENT", Action::Event),
    ("GRANT OPTION", Action::GrantOption),
    ("CREATE USER", Action::CreateUser),
    ("CREATE ROLE", Action::CreateRole),
    ("DROP ROLE", Action::DropRole),
    ("ROLE_ADMIN", Action::RoleAdmin),
    ("WATCH", Action::Watch),
    ("SHOW DATABASES", Action::ShowDatabases),
    ("PROCESS", Action::Process),
    ("REFERENCES", Action::References),
    ("CREATE VIEW", Action::CreateView),
    ("SHOW VIEW", Action::ShowView),
    ("LOCK TABLES", Action::LockTables),
    ("CREATE TEMPORARY TABLES", Action::CreateTemporaryTables),
    ("SPRIG_SETTINGS_ADMIN", Action::SettingsAdmin),
    ("SPRIG_TENANT_ADMIN", Action::TenantAdmin),
    ("BYPASS RLS", Action::BypassRls),
    ("CONNECTION_ADMIN", Action::ConnectionAdmin),
    ("CONNECTION ADMIN", Action::ConnectionAdmin),
    ("SYSTEM_VARIABLES_ADMIN", Action::SystemVariablesAdmin),
    ("SET_ANY_DEFINER", Action::SetAnyDefiner),
    ("SET USER", Action::SetAnyDefiner),
    ("SPRIG_SINK_ADMIN", Action::SinkAdmin),
    ("SPRIG_FAN_IN", Action::FanIn),
];

/// Server-wide privileges of either server: dropped in a tenant.
const DROPPED: &[&str] = &[
    "SUPER",
    "FILE",
    "RELOAD",
    "SHUTDOWN",
    "REPLICATION SLAVE",
    "REPLICATION CLIENT",
    "REPLICATION SLAVE ADMIN",
    "REPLICATION MASTER ADMIN",
    "REPLICA MONITOR",
    "SLAVE MONITOR",
    "BINLOG MONITOR",
    "BINLOG ADMIN",
    "BINLOG REPLAY",
    "BINLOG_ADMIN",
    "BINLOG_ENCRYPTION_ADMIN",
    "CREATE TABLESPACE",
    "READ_ONLY ADMIN",
    "READ_ONLY_ADMIN",
    "FEDERATED ADMIN",
    "DELETE HISTORY",
    "SHOW CREATE ROUTINE",
    "BACKUP_ADMIN",
    "CLONE_ADMIN",
    "ENCRYPTION_KEY_ADMIN",
    "GROUP_REPLICATION_ADMIN",
    "INNODB_REDO_LOG_ARCHIVE",
    "PERSIST_RO_VARIABLES_ADMIN",
    "REPLICATION_APPLIER",
    "REPLICATION_SLAVE_ADMIN",
    "RESOURCE_GROUP_ADMIN",
    "RESOURCE_GROUP_USER",
    "SERVICE_CONNECTION_ADMIN",
    "SESSION_VARIABLES_ADMIN",
    "SYSTEM_USER",
    "TABLE_ENCRYPTION_ADMIN",
    "XA_RECOVER_ADMIN",
    "AUDIT_ADMIN",
    "FLUSH_OPTIMIZER_COSTS",
    "FLUSH_STATUS",
    "FLUSH_TABLES",
    "FLUSH_USER_RESOURCES",
    "APPLICATION_PASSWORD_ADMIN",
    "AUTHENTICATION_POLICY_ADMIN",
    "PASSWORDLESS_USER_ADMIN",
    "SENSITIVE_VARIABLES_OBSERVER",
    "SHOW_ROUTINE",
    "TELEMETRY_LOG_ADMIN",
    "TRANSACTION_GTID_TAG",
];

/// Recognizes a privilege name, as the parser normalized it (upper case,
/// single spaces).
#[must_use]
pub fn parse_privilege(name: &str) -> PrivName {
    match name {
        "ALL" | "ALL PRIVILEGES" => return PrivName::All,
        "USAGE" => return PrivName::Usage,
        _ => {}
    }
    if let Some((_, a)) = NAMES.iter().find(|(n, _)| *n == name) {
        return PrivName::Action(*a);
    }
    if DROPPED.contains(&name) {
        return PrivName::Dropped;
    }
    PrivName::Unknown
}

/// The privilege names `SHOW GRANTS` prints for `set` at `level`, in
/// MySQL's canonical order (static ones only; see [`Action::is_dynamic`]).
/// `ALL PRIVILEGES` when the set holds every action of the level.
#[must_use]
pub fn render_static(set: ActionSet, level: Level, object: ObjectType) -> Vec<&'static str> {
    let set = set.minus(ActionSet::of(&[Action::GrantOption]));
    let all = all_at(level, object);
    let statics: ActionSet = ActionSet(
        all.0
            & !Action::ALL
                .iter()
                .filter(|a| a.is_dynamic())
                .fold(0, |b, a| b | a.bit()),
    );
    if !statics.is_empty() && set.contains(statics) {
        return vec!["ALL PRIVILEGES"];
    }
    ORDER
        .iter()
        .filter(|a| set.has(**a) && !a.is_dynamic())
        .map(|a| a.command())
        .map(|c| if c == "GRANT" { "GRANT OPTION" } else { c })
        .collect()
}

/// MySQL's order of static privileges in `SHOW GRANTS`.
const ORDER: &[Action] = &[
    Action::Select,
    Action::Insert,
    Action::Update,
    Action::Delete,
    Action::Create,
    Action::Drop,
    Action::Process,
    Action::References,
    Action::Index,
    Action::Alter,
    Action::ShowDatabases,
    Action::CreateTemporaryTables,
    Action::LockTables,
    Action::Execute,
    Action::CreateView,
    Action::ShowView,
    Action::CreateRoutine,
    Action::AlterRoutine,
    Action::CreateUser,
    Action::Event,
    Action::Trigger,
    Action::CreateRole,
    Action::DropRole,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits_are_stable_and_unique() {
        let mut seen = 0u64;
        for (i, a) in Action::ALL.iter().enumerate() {
            assert_eq!(*a as usize, i);
            assert_eq!(seen & a.bit(), 0);
            seen |= a.bit();
        }
        // Fixed values: these are stored in catalogs.
        assert_eq!(Action::Select.bit(), 1);
        assert_eq!(Action::GrantOption.bit(), 1 << 13);
        assert_eq!(Action::TenantAdmin.bit(), 1 << 27);
    }

    #[test]
    fn names_parse() {
        assert_eq!(parse_privilege("SELECT"), PrivName::Action(Action::Select));
        assert_eq!(parse_privilege("ALL PRIVILEGES"), PrivName::All);
        assert_eq!(parse_privilege("SUPER"), PrivName::Dropped);
        assert_eq!(
            parse_privilege("SET USER"),
            PrivName::Action(Action::SetAnyDefiner)
        );
        assert_eq!(parse_privilege("FOO_BAR"), PrivName::Unknown);
    }

    #[test]
    fn all_sets() {
        assert!(!ALL_TENANT.has(Action::GrantOption));
        assert!(!ALL_TENANT.has(Action::TenantAdmin));
        assert!(ALL_TENANT.has(Action::SettingsAdmin));
        assert!(ALL_TENANT.has(Action::CreateUser));
        assert!(!all_at(Level::Database, ObjectType::Table).has(Action::CreateUser));
        assert_eq!(
            render_static(
                all_at(Level::Database, ObjectType::Table),
                Level::Database,
                ObjectType::Table
            ),
            ["ALL PRIVILEGES"]
        );
        assert_eq!(
            render_static(
                ActionSet::of(&[Action::Insert, Action::Select, Action::GrantOption]),
                Level::Object,
                ObjectType::Table
            ),
            ["SELECT", "INSERT"]
        );
    }
}
