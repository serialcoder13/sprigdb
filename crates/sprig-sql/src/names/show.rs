//! The `SHOW` family.
//!
//! [`classify`] reads a rewritten `SHOW` statement and says what it is;
//! [`super::plan()`] then forwards it with a [`super::ResultMapper`] or answers it
//! here:
//!
//! | Statement | Handling |
//! |---|---|
//! | `SHOW DATABASES`, `SCHEMAS` | local from the name map ([`show_databases`]); `WHERE` runs in MariaDB over the inlined rows |
//! | `SHOW [FULL] TABLES` | forward; `Tables_in_<logical>` header (and in `WHERE`); `__sprig_` rows dropped |
//! | `SHOW TABLE STATUS`, `OPEN TABLES`, `TRIGGERS`, `EVENTS`, `PROCEDURE`/`FUNCTION`/`PACKAGE STATUS` | forward; `Db`, `Database` mapped; rows of other schemas and internal objects dropped; bodies mapped |
//! | `SHOW COLUMNS`, `FIELDS` | forward; `_sprig_` rows dropped; `my_row_id` per the GIPK variable |
//! | `SHOW INDEX`, `KEYS` | forward; automatic indexes and indexes on hidden columns dropped |
//! | `SHOW CREATE TABLE`, `SEQUENCE` | forward; the line filter; schemas in `REFERENCES` mapped |
//! | `SHOW CREATE VIEW`, `TRIGGER`, `EVENT`, `DATABASE` | forward; physical schemas in the text mapped; `DEFINER` mapped |
//! | `SHOW CREATE PROCEDURE`, `FUNCTION` | forward and mapped; the gateway answers from the catalog's original text with [`routine_answer`] when it has it |
//! | `SHOW PROCESSLIST` | local from the gateway's sessions ([`show_processlist`]) |
//! | `SHOW WARNINGS`, `ERRORS` | forward; messages mapped |
//! | `SHOW ENGINE`, `BINARY LOGS`, `MASTER STATUS`, `REPLICA STATUS`, `SLAVE HOSTS`, ... | ERR 1227 |
//! | anything else (`VARIABLES`, `STATUS`, `GRANTS`, `COLLATION`, ...) | forward; every value scrubbed |

use std::ops::Range;

use bytes::Bytes;
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::packets::Persona;
use sprig_proto_mysql::resultset::{ColumnFlags, column_type};

use super::map::{LogicalDb, NameMap};
use super::output::{OutputCtx, map_bytes};
use super::result::{LocalAnswer, TextRow};
use crate::lex::{Token, TokenKind, ident_value, string_value};

/// Which `SHOW` statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShowKind {
    /// `SHOW DATABASES`, `SHOW SCHEMAS`.
    Databases,
    /// `SHOW [FULL] TABLES`.
    Tables,
    /// `SHOW TABLE STATUS`.
    TableStatus,
    /// `SHOW [FULL] COLUMNS`, `FIELDS`.
    Columns,
    /// `SHOW INDEX`, `INDEXES`, `KEYS`.
    Index,
    /// `SHOW CREATE TABLE`.
    CreateTable,
    /// `SHOW CREATE VIEW`.
    CreateView,
    /// `SHOW CREATE PROCEDURE`.
    CreateProcedure,
    /// `SHOW CREATE FUNCTION`.
    CreateFunction,
    /// `SHOW CREATE TRIGGER`.
    CreateTrigger,
    /// `SHOW CREATE EVENT`.
    CreateEvent,
    /// `SHOW CREATE DATABASE`, `SCHEMA`.
    CreateDatabase,
    /// `SHOW CREATE SEQUENCE` (MariaDB).
    CreateSequence,
    /// `SHOW CREATE PACKAGE [BODY]` (MariaDB).
    CreatePackage,
    /// `SHOW TRIGGERS`.
    Triggers,
    /// `SHOW EVENTS`.
    Events,
    /// `SHOW PROCEDURE STATUS`.
    ProcedureStatus,
    /// `SHOW FUNCTION STATUS`.
    FunctionStatus,
    /// `SHOW PACKAGE [BODY] STATUS` (MariaDB).
    PackageStatus,
    /// `SHOW OPEN TABLES`.
    OpenTables,
    /// `SHOW [FULL] PROCESSLIST`.
    Processlist,
    /// `SHOW WARNINGS`.
    Warnings,
    /// `SHOW ERRORS`.
    Errors,
    /// `SHOW GRANTS` (forwarded, its output scrubbed).
    Grants,
    /// A statement that fails with 1227.
    Denied,
    /// Anything else: forwarded and scrubbed.
    Other,
}

/// What [`classify`] learned about a `SHOW` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowInfo {
    /// Which statement.
    pub kind: ShowKind,
    /// `FULL` (`TABLES`, `COLUMNS`, `PROCESSLIST`).
    pub full: bool,
    /// The physical schema named by `FROM db` or a qualified object.
    pub schema: Option<String>,
    /// The object of `SHOW CREATE` and `SHOW COLUMNS`/`INDEX FROM`.
    pub object: Option<String>,
    /// The value of `LIKE 'pattern'`.
    pub like: Option<String>,
    /// The byte range of `WHERE ...` to the end of the statement.
    pub filter: Option<Range<usize>>,
}

struct Toks<'a> {
    src: &'a [u8],
    t: Vec<Token>,
    nbe: bool,
}

impl Toks<'_> {
    fn word(&self, j: usize, w: &str) -> bool {
        self.t.get(j).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(self.src).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    fn any(&self, j: usize, ws: &[&str]) -> bool {
        ws.iter().any(|w| self.word(j, w))
    }

    fn ident(&self, j: usize) -> Option<String> {
        let t = self.t.get(j)?;
        t.kind
            .is_ident_like()
            .then(|| String::from_utf8_lossy(&ident_value(t.bytes(self.src))).into_owned())
    }

    fn dot(&self, j: usize) -> bool {
        self.t
            .get(j)
            .is_some_and(|t| t.kind == TokenKind::Punct && self.src[t.start as usize] == b'.')
    }

    fn string(&self, j: usize) -> Option<String> {
        let t = self.t.get(j)?;
        if !matches!(t.kind, TokenKind::String | TokenKind::NationalString) {
            return None;
        }
        let b = t.bytes(self.src);
        let b = if t.kind == TokenKind::NationalString {
            &b[1..]
        } else {
            b
        };
        string_value(b, self.nbe).map(|v| String::from_utf8_lossy(&v).into_owned())
    }

    /// A possibly qualified name at `j`: (schema, object, next index).
    fn name(&self, j: usize) -> Option<(Option<String>, String, usize)> {
        let first = self.ident(j)?;
        if self.dot(j + 1)
            && let Some(second) = self.ident(j + 2)
        {
            return Some((Some(first), second, j + 3));
        }
        Some((None, first, j + 1))
    }
}

/// Classifies the `SHOW` statement `sql` (one statement, rewritten, so
/// schemas in it are physical). `None` if it is not a `SHOW`.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn classify(sql: &[u8], tokens: &[Token], no_backslash_escapes: bool) -> Option<ShowInfo> {
    let tk = Toks {
        src: sql,
        t: tokens
            .iter()
            .copied()
            .filter(|t| !t.kind.is_trivia())
            .collect(),
        nbe: no_backslash_escapes,
    };
    if !tk.word(0, "SHOW") {
        return None;
    }
    let mut info = ShowInfo {
        kind: ShowKind::Other,
        full: false,
        schema: None,
        object: None,
        like: None,
        filter: None,
    };
    let mut j = 1;
    while tk.any(
        j,
        &["FULL", "EXTENDED", "GLOBAL", "SESSION", "LOCAL", "ALL"],
    ) {
        info.full |= tk.word(j, "FULL");
        j += 1;
    }
    let mut lists = false; // [FROM db] [LIKE | WHERE] follow
    if tk.word(j, "CREATE") {
        j += 1;
        let kind = [
            ("TABLE", ShowKind::CreateTable),
            ("VIEW", ShowKind::CreateView),
            ("PROCEDURE", ShowKind::CreateProcedure),
            ("FUNCTION", ShowKind::CreateFunction),
            ("TRIGGER", ShowKind::CreateTrigger),
            ("EVENT", ShowKind::CreateEvent),
            ("DATABASE", ShowKind::CreateDatabase),
            ("SCHEMA", ShowKind::CreateDatabase),
            ("SEQUENCE", ShowKind::CreateSequence),
            ("PACKAGE", ShowKind::CreatePackage),
        ]
        .into_iter()
        .find(|(w, _)| tk.word(j, w));
        if let Some((_, k)) = kind {
            info.kind = k;
            j += 1;
            if k == ShowKind::CreatePackage && tk.word(j, "BODY") {
                j += 1;
            }
            if k == ShowKind::CreateDatabase {
                if tk.word(j, "IF") {
                    j += 3;
                }
                info.schema = tk.ident(j);
            } else if let Some((s, o, _)) = tk.name(j) {
                info.schema = s;
                info.object = Some(o);
            }
        }
        return Some(info);
    }
    let w = |j: usize, s: &str| tk.word(j, s);
    if tk.any(j, &["DATABASES", "SCHEMAS"]) {
        info.kind = ShowKind::Databases;
        lists = true;
        j += 1;
    } else if w(j, "TABLES") {
        info.kind = ShowKind::Tables;
        lists = true;
        j += 1;
    } else if w(j, "TABLE") && w(j + 1, "STATUS") {
        info.kind = ShowKind::TableStatus;
        lists = true;
        j += 2;
    } else if w(j, "OPEN") && w(j + 1, "TABLES") {
        info.kind = ShowKind::OpenTables;
        lists = true;
        j += 2;
    } else if tk.any(j, &["COLUMNS", "FIELDS", "INDEX", "INDEXES", "KEYS"]) {
        info.kind = if tk.any(j, &["COLUMNS", "FIELDS"]) {
            ShowKind::Columns
        } else {
            ShowKind::Index
        };
        lists = true;
        j += 1;
        if tk.any(j, &["FROM", "IN"])
            && let Some((s, o, next)) = tk.name(j + 1)
        {
            info.schema = s;
            info.object = Some(o);
            j = next;
        }
    } else if w(j, "TRIGGERS") {
        info.kind = ShowKind::Triggers;
        lists = true;
        j += 1;
    } else if w(j, "EVENTS") {
        info.kind = ShowKind::Events;
        lists = true;
        j += 1;
    } else if w(j, "PROCEDURE") && w(j + 1, "STATUS") {
        info.kind = ShowKind::ProcedureStatus;
        lists = true;
        j += 2;
    } else if w(j, "FUNCTION") && w(j + 1, "STATUS") {
        info.kind = ShowKind::FunctionStatus;
        lists = true;
        j += 2;
    } else if w(j, "PACKAGE") && (w(j + 1, "STATUS") || (w(j + 1, "BODY") && w(j + 2, "STATUS"))) {
        info.kind = ShowKind::PackageStatus;
        lists = true;
        j += if w(j + 1, "BODY") { 3 } else { 2 };
    } else if w(j, "PROCESSLIST") {
        info.kind = ShowKind::Processlist;
    } else if w(j, "WARNINGS") || (w(j, "COUNT") && tk.any(j + 4, &["WARNINGS"])) {
        info.kind = ShowKind::Warnings;
    } else if w(j, "ERRORS") || (w(j, "COUNT") && tk.any(j + 4, &["ERRORS"])) {
        info.kind = ShowKind::Errors;
    } else if w(j, "GRANTS") {
        info.kind = ShowKind::Grants;
    } else if denied(&tk, j) {
        info.kind = ShowKind::Denied;
    }
    if lists {
        while j < tk.t.len() {
            if tk.any(j, &["FROM", "IN"]) {
                info.schema = tk.ident(j + 1);
                j += 2;
            } else if w(j, "LIKE") {
                info.like = tk.string(j + 1);
                j += 2;
            } else if w(j, "WHERE") {
                info.filter = Some(tk.t[j].start as usize..sql.len());
                break;
            } else {
                j += 1;
            }
        }
    }
    Some(info)
}

/// `SHOW` statements the name layer refuses with 1227.
fn denied(tk: &Toks<'_>, j: usize) -> bool {
    let w = |k: usize, s: &str| tk.word(k, s);
    w(j, "ENGINE")
        || w(j, "MUTEX")
        || ((w(j, "BINARY") || w(j, "MASTER")) && w(j + 1, "LOGS"))
        || ((w(j, "MASTER") || w(j, "BINLOG")) && w(j + 1, "STATUS"))
        || ((w(j, "REPLICA") || w(j, "SLAVE")) && (w(j + 1, "STATUS") || w(j + 1, "HOSTS")))
        || ((w(j, "SLAVES") || w(j, "REPLICAS")) && w(j + 1, "STATUS"))
        || w(j, "REPLICAS")
        || ((w(j, "BINLOG") || w(j, "RELAYLOG")) && w(j + 1, "EVENTS"))
}

/// The result character set of the text columns of local answers, as
/// MariaDB reports them to a `utf8mb4` session (`utf8mb4_unicode_ci`); the
/// gateway sets the session's own with [`LocalAnswer::set_text_charset`].
pub const TEXT_CHARSET: u16 = 224;

/// `NO_DEFAULT_VALUE`, as MariaDB flags `information_schema` columns.
const NO_DEFAULT: u16 = 1 << 12;

/// A column definition shaped like MariaDB's for the same `SHOW` (captured
/// in `tests/fixtures/names/show/`): text columns carry decimals 39 and
/// four bytes per character.
fn col(name: &str, col_type: u8, length: u32, flags: u16) -> ColumnDef {
    let text = !matches!(
        col_type,
        column_type::LONGLONG | column_type::LONG | column_type::DOUBLE
    );
    ColumnDef {
        schema: Bytes::new(),
        table: Bytes::new(),
        org_table: Bytes::new(),
        name: Bytes::from(name.to_owned()),
        org_name: Bytes::new(),
        ext_type: None,
        charset: if text { TEXT_CHARSET } else { 63 },
        length,
        col_type,
        flags: if text {
            flags
        } else {
            flags | ColumnFlags::BINARY.bits()
        },
        decimals: if text { 39 } else { 0 },
    }
}

const NOT_NULL: u16 = ColumnFlags::NOT_NULL.bits();
const VARCHAR: u8 = column_type::VAR_STRING;

/// The column definition of `SHOW DATABASES` (`Database`, or
/// `Database (pattern)` with `LIKE`).
#[must_use]
pub fn databases_column(like: Option<&str>) -> ColumnDef {
    let name = match like {
        Some(p) => format!("Database ({p})"),
        None => "Database".to_owned(),
    };
    ColumnDef {
        schema: Bytes::from_static(b"information_schema"),
        table: Bytes::from_static(b"SCHEMATA"),
        org_table: Bytes::from_static(b"SCHEMATA"),
        org_name: Bytes::from_static(b"SCHEMA_NAME"),
        decimals: 0,
        ..col(&name, VARCHAR, 256, NOT_NULL | NO_DEFAULT)
    }
}

/// The system schemas `SHOW DATABASES` lists for a persona: always
/// `information_schema`, and `performance_schema` on the MySQL persona.
#[must_use]
pub fn system_databases(persona: Persona) -> &'static [&'static str] {
    match persona {
        Persona::MySql84 => &["information_schema", "performance_schema"],
        Persona::MariaDb => &["information_schema"],
    }
}

/// The names `SHOW DATABASES` and `SCHEMATA` list, sorted: the persona's
/// system schemas and the tenant's databases that `visible` keeps
/// (the caller decides which the user may see).
#[must_use]
pub fn database_names(
    map: &dyn NameMap,
    persona: Persona,
    visible: Option<&(dyn Fn(&LogicalDb) -> bool + Sync)>,
) -> Vec<String> {
    let mut names: Vec<String> = system_databases(persona)
        .iter()
        .map(|s| (*s).to_owned())
        .chain(
            map.list()
                .into_iter()
                .filter(|d| visible.is_none_or(|f| f(d)))
                .map(|d| d.name.clone()),
        )
        .collect();
    names.sort();
    names
}

/// `SHOW DATABASES [LIKE 'pattern']`, answered from the name map
/// (databases are never listed from MariaDB, whose scan reads every
/// schema on the server). `case_insensitive` follows
/// `names.lower_case_table_names`.
#[must_use]
pub fn show_databases(names: &[String], like: Option<&str>, case_insensitive: bool) -> LocalAnswer {
    let rows = names
        .iter()
        .filter(|n| like.is_none_or(|p| like_match(p, n, case_insensitive)))
        .map(|n| vec![Some(Bytes::from(n.clone()))])
        .collect();
    LocalAnswer {
        columns: vec![databases_column(like)],
        rows,
    }
}

/// SQL `LIKE` with `%`, `_` and `\` as the escape.
#[must_use]
pub fn like_match(pattern: &str, value: &str, case_insensitive: bool) -> bool {
    let fold = |s: &str| -> Vec<char> {
        if case_insensitive {
            s.chars().flat_map(char::to_lowercase).collect()
        } else {
            s.chars().collect()
        }
    };
    let p = fold(pattern);
    let v = fold(value);
    like_at(&p, &v)
}

fn like_at(p: &[char], v: &[char]) -> bool {
    // Iterative matching with backtracking to the last `%`.
    let (mut pi, mut vi) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while vi < v.len() {
        if pi < p.len() {
            match p[pi] {
                '%' => {
                    star = Some((pi, vi));
                    pi += 1;
                    continue;
                }
                '_' => {
                    pi += 1;
                    vi += 1;
                    continue;
                }
                '\\' if pi + 1 < p.len() => {
                    if p[pi + 1] == v[vi] {
                        pi += 2;
                        vi += 1;
                        continue;
                    }
                }
                c if c == v[vi] => {
                    pi += 1;
                    vi += 1;
                    continue;
                }
                _ => {}
            }
        }
        match star {
            Some((sp, sv)) => {
                pi = sp + 1;
                vi = sv + 1;
                star = Some((sp, sv + 1));
            }
            None => return false,
        }
    }
    p[pi..].iter().all(|&c| c == '%')
}

/// One session of the tenant, as the gateway knows it, for
/// `SHOW PROCESSLIST` and `information_schema.PROCESSLIST`. Names are the
/// client's: logical database, user name, client address.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessRow {
    /// The connection id the client sees.
    pub id: u64,
    /// The user name.
    pub user: String,
    /// `host:port` of the client.
    pub host: String,
    /// The session's logical database.
    pub db: Option<String>,
    /// `Query`, `Sleep`, ...
    pub command: String,
    /// Seconds in the current state.
    pub time: u64,
    /// Milliseconds in the current state.
    pub time_ms: u64,
    /// The state, if any.
    pub state: Option<String>,
    /// The statement running, if any, as the client sent it.
    pub info: Option<String>,
}

/// The tenant's sessions on the data center's gateways.
pub trait SessionSource: Send + Sync {
    /// Every session of the tenant the user may see (all of them with
    /// `PROCESS`, otherwise the user's own).
    fn processes(&self) -> Vec<ProcessRow>;
}

/// `SHOW [FULL] PROCESSLIST`, from the gateway's sessions. Without `FULL`,
/// `Info` is cut to 100 characters, as MariaDB does.
#[must_use]
pub fn show_processlist(rows: &[ProcessRow], full: bool, persona: Persona) -> LocalAnswer {
    let mut columns = vec![
        col("Id", column_type::LONGLONG, 11, NOT_NULL),
        col("User", VARCHAR, 512, NOT_NULL),
        col("Host", VARCHAR, 256, NOT_NULL),
        col("db", VARCHAR, 256, 0),
        col("Command", VARCHAR, 64, NOT_NULL),
        col("Time", column_type::LONG, 7, NOT_NULL),
        col("State", VARCHAR, 120, 0),
        col("Info", VARCHAR, if full { 262_140 } else { 400 }, 0),
    ];
    if persona == Persona::MariaDb {
        columns.push(ColumnDef {
            decimals: 3,
            ..col("Progress", column_type::DOUBLE, 7, NOT_NULL)
        });
    }
    let rows = rows
        .iter()
        .map(|p| {
            let info = p.info.as_ref().map(|i| {
                if full {
                    i.clone()
                } else {
                    i.chars().take(100).collect()
                }
            });
            let mut r: TextRow = vec![
                Some(Bytes::from(p.id.to_string())),
                Some(Bytes::from(p.user.clone())),
                Some(Bytes::from(p.host.clone())),
                p.db.clone().map(Bytes::from),
                Some(Bytes::from(p.command.clone())),
                Some(Bytes::from(p.time.to_string())),
                p.state.clone().map(Bytes::from),
                info.map(Bytes::from),
            ];
            if persona == Persona::MariaDb {
                r.push(Some(Bytes::from_static(b"0.000")));
            }
            r
        })
        .collect();
    LocalAnswer { columns, rows }
}

/// A stored routine's original text, kept by the catalog
/// (`schema_versions`) when the routine was created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineSource {
    /// The routine's name.
    pub name: String,
    /// `PROCEDURE` or `FUNCTION`.
    pub function: bool,
    /// `sql_mode` at creation.
    pub sql_mode: String,
    /// The client's `CREATE` statement, with logical names (the `DEFINER`
    /// as the client wrote it or as account management records it).
    pub create_text: String,
    /// `character_set_client` at creation.
    pub character_set_client: String,
    /// `collation_connection` at creation.
    pub collation_connection: String,
    /// The database's collation.
    pub database_collation: String,
}

/// `SHOW CREATE PROCEDURE` or `FUNCTION` answered from the original text
/// (the catalog's copy, so comments and layout survive and no
/// physical name can appear). Any physical name in the stored text is
/// still mapped, as a safety net.
#[must_use]
pub fn routine_answer(src: &RoutineSource, ctx: &OutputCtx<'_>) -> LocalAnswer {
    let (head, create) = if src.function {
        ("Function", "Create Function")
    } else {
        ("Procedure", "Create Procedure")
    };
    let columns = vec![
        col(head, VARCHAR, 256, NOT_NULL),
        col("sql_mode", VARCHAR, 356, NOT_NULL),
        col(create, VARCHAR, 4096, 0),
        col("character_set_client", VARCHAR, 128, NOT_NULL),
        col("collation_connection", VARCHAR, 256, NOT_NULL),
        col("Database Collation", VARCHAR, 128, NOT_NULL),
    ];
    let text = map_bytes(src.create_text.as_bytes(), ctx);
    let rows = vec![vec![
        Some(Bytes::from(src.name.clone())),
        Some(Bytes::from(src.sql_mode.clone())),
        Some(Bytes::from(text.text.into_owned())),
        Some(Bytes::from(src.character_set_client.clone())),
        Some(Bytes::from(src.collation_connection.clone())),
        Some(Bytes::from(src.database_collation.clone())),
    ]];
    LocalAnswer { columns, rows }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lex::{LexOptions, lex};

    fn info(sql: &str) -> ShowInfo {
        let t = lex(sql.as_bytes(), &LexOptions::mariadb()).unwrap();
        classify(sql.as_bytes(), &t, false).unwrap()
    }

    #[test]
    fn classifies_the_family() {
        let i = info("SHOW FULL TABLES FROM `t1_d1_g1_s1` LIKE 'a%'");
        assert_eq!(i.kind, ShowKind::Tables);
        assert!(i.full);
        assert_eq!(i.schema.as_deref(), Some("t1_d1_g1_s1"));
        assert_eq!(i.like.as_deref(), Some("a%"));
        let i = info("show columns from `t1_d1_g1_s1`.orders where Field = 'x'");
        assert_eq!(i.kind, ShowKind::Columns);
        assert_eq!(i.object.as_deref(), Some("orders"));
        assert_eq!(i.schema.as_deref(), Some("t1_d1_g1_s1"));
        assert!(i.filter.is_some());
        assert_eq!(info("SHOW KEYS IN o").kind, ShowKind::Index);
        assert_eq!(
            info("SHOW CREATE SCHEMA IF NOT EXISTS x").schema.as_deref(),
            Some("x")
        );
        assert_eq!(
            info("SHOW CREATE PACKAGE BODY p").kind,
            ShowKind::CreatePackage
        );
        assert_eq!(
            info("SHOW PACKAGE BODY STATUS").kind,
            ShowKind::PackageStatus
        );
        assert_eq!(info("SHOW COUNT(*) WARNINGS").kind, ShowKind::Warnings);
        for d in [
            "SHOW ENGINE INNODB STATUS",
            "SHOW BINARY LOGS",
            "SHOW MASTER STATUS",
            "SHOW BINLOG STATUS",
            "SHOW REPLICA STATUS",
            "SHOW ALL SLAVES STATUS",
            "SHOW SLAVE HOSTS",
            "SHOW REPLICAS",
            "SHOW BINLOG EVENTS",
        ] {
            assert_eq!(info(d).kind, ShowKind::Denied, "{d}");
        }
        assert_eq!(info("SHOW ENGINES").kind, ShowKind::Other);
        assert_eq!(info("SHOW GLOBAL VARIABLES LIKE 'x'").kind, ShowKind::Other);
        assert_eq!(info("SHOW FULL PROCESSLIST").kind, ShowKind::Processlist);
    }

    #[test]
    fn like_patterns() {
        assert!(like_match("b%", "billing", false));
        assert!(!like_match("B%", "billing", false));
        assert!(like_match("B%", "billing", true));
        assert!(like_match("b_lling", "billing", false));
        assert!(like_match("%ll%", "billing", false));
        assert!(like_match("my\\_db", "my_db", false));
        assert!(!like_match("my\\_db", "myxdb", false));
        assert!(like_match("%", "", false));
        assert!(!like_match("a", "", false));
        assert!(like_match("a%b%c", "aXbYc", false));
        assert!(!like_match("a%b%c", "aXbY", false));
    }
}
