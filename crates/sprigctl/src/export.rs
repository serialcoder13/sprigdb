//! `sprigctl tenant export` and `sprigctl tenant import` (chapter 47,
//! "Logical export"; build step 8).
//!
//! An export is a directory:
//!
//! ```text
//! manifest.json       format, tenant, time; per database its CREATE DATABASE,
//!                     per table its CREATE statement, row count and checksum
//! accounts.json       users with verifier strings and SQL roles (Admin API),
//!                     and each account's SHOW GRANTS lines (gateway)
//! settings.json       the tenant's own setting rows (tenant, database, object)
//! data/<db>/<t>.sql   one INSERT per line, at most 512 KB each
//! ```
//!
//! Schemas, rows and grants travel through the gateway's normal SQL path
//! under logical names, so an export loads into any tenant on any cluster
//! whatever its topology; the Admin API carries what SQL can't (verifier
//! hashes, setting rows with their bounds). Rows are read in one
//! `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
//!
//! **Checksums.** Each table's checksum is order-independent: the sum,
//! modulo 2^128, of the first 16 bytes of each row's SHA-256 over its
//! values as the text protocol returns them (NULL marked apart from the
//! empty string), with the row count. `import` recomputes it on the
//! target and compares every table and every account's grants with the
//! manifest; a difference fails with 39971 `ER_SPRIG_IMPORT_VERIFY_FAILED`.
//!
//! **Order of an import.** Create the tenant (`--create`); create the
//! accounts with their verifiers (so the source's passwords log in to the
//! target), making the ones that held `SPRIG_TENANT_ADMIN` tenant
//! administrators; then, logged in through the gateway: databases, tables,
//! rows, views, roles, grants; then the setting rows; then verify.
//!
//! Not carried yet: triggers, stored routines and events (listed in the
//! manifest's `skipped`), automatic indexes (left out by design: the
//! target's own loop decides), and other data models (R5 on).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{BufRead, BufWriter};
use std::path::{Path, PathBuf};

use clap::{Arg, ArgAction, ArgMatches, Command};
use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{Column, Conn, OptsBuilder, SslOpts, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sprig_auth::portable::PortableAccounts;
use sprig_controller::admin::types::{
    CreateTenantRequest, CreateTenantResponse, LockSettingRequest, ScopeRef, SetBoundsRequest,
    SetSettingRequest, SettingWriteResponse, TenantSettingRowView, TenantSettingsResponse,
    TenantView,
};
use sprig_controller::users::ImportAccountsResponse;

use crate::client::AdminClient;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// The export format version.
pub(crate) const FORMAT: u32 = 1;
/// The largest INSERT statement written.
const MAX_STATEMENT: usize = 512 * 1024;
/// Schemas a tenant never owns.
const SYSTEM_DBS: &[&str] = &["information_schema", "performance_schema", "mysql", "sys"];

/// One table or view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    /// The name.
    pub name: String,
    /// `table` or `view`.
    pub kind: String,
    /// `SHOW CREATE TABLE` (or `VIEW`, without its definer).
    pub create: String,
    /// Rows (tables).
    #[serde(default)]
    pub rows: u64,
    /// The checksum (tables).
    #[serde(default)]
    pub checksum: String,
}

/// One database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DbEntry {
    /// The logical name.
    pub name: String,
    /// `SHOW CREATE DATABASE`.
    pub create: String,
    /// Tables, then views.
    pub tables: Vec<TableEntry>,
}

/// `manifest.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    /// [`FORMAT`].
    pub format: u32,
    /// The source tenant.
    pub tenant: String,
    /// Its id.
    pub tenant_id: u64,
    /// Its catalog cluster.
    pub catalog_cluster: String,
    /// When, RFC 3339 UTC.
    pub exported_at: String,
    /// Databases.
    pub databases: Vec<DbEntry>,
    /// What wasn't carried, per database.
    #[serde(default)]
    pub skipped: Vec<String>,
}

/// `accounts.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct AccountsFile {
    /// Users and roles from the Admin API.
    pub accounts: PortableAccounts,
    /// `SHOW GRANTS` lines per account `'name'@'host'`.
    pub grants: BTreeMap<String, Vec<String>>,
}

/// `sprigctl tenant export`.
pub(crate) fn export_command() -> Command {
    gateway_args(
        Command::new("export")
            .about("Export a tenant: schemas, rows, users and grants, settings (chapter 47)")
            .arg(Arg::new("name").required(true).help("The tenant"))
            .arg(
                Arg::new("to")
                    .long("to")
                    .required(true)
                    .help("A new or empty directory"),
            ),
    )
}

/// `sprigctl tenant import`.
pub(crate) fn import_command() -> Command {
    gateway_args(
        Command::new("import")
            .about("Load a tenant export into a tenant and verify checksums and grants")
            .arg(Arg::new("from").required(true).help("The export directory"))
            .arg(
                Arg::new("into")
                    .long("into")
                    .required(true)
                    .help("The target tenant"),
            )
            .arg(
                Arg::new("create")
                    .long("create")
                    .action(ArgAction::SetTrue)
                    .help("Create the target tenant first"),
            )
            .arg(
                Arg::new("cluster")
                    .long("cluster")
                    .help("With --create: the target's cluster"),
            )
            .arg(
                Arg::new("verify-only")
                    .long("verify-only")
                    .action(ArgAction::SetTrue)
                    .help("Only compare the target with the export"),
            ),
    )
}

fn gateway_args(c: Command) -> Command {
    c.arg(
        Arg::new("gateway")
            .long("gateway")
            .env("SPRIGCTL_GATEWAY")
            .required(true)
            .help("A gateway's MySQL or MariaDB listener, host:port"),
    )
    .arg(
        Arg::new("user")
            .long("user")
            .required(true)
            .help("A tenant administrator's login, <tenant>.<user>"),
    )
    .arg(
        Arg::new("password")
            .long("password")
            .env("SPRIGCTL_GATEWAY_PASSWORD")
            .required(true)
            .hide_env_values(true)
            .help("Its password"),
    )
    .arg(
        Arg::new("tls-ca")
            .long("tls-ca")
            .help("Verify the gateway's certificate against this CA (PEM)"),
    )
    .arg(
        Arg::new("tls-insecure")
            .long("tls-insecure")
            .action(ArgAction::SetTrue)
            .help("Accept any gateway certificate (development)"),
    )
}

fn arg(sub: &ArgMatches, name: &str) -> String {
    sub.get_one::<String>(name).cloned().unwrap_or_default()
}

async fn connect(sub: &ArgMatches, user: &str) -> Result<Conn, Failed> {
    let gw = arg(sub, "gateway");
    let (host, port) = gw
        .rsplit_once(':')
        .and_then(|(h, p)| Some((h.to_owned(), p.parse::<u16>().ok()?)))
        .ok_or_else(|| Failed(format!("--gateway {gw:?}: expected host:port")))?;
    let mut ssl = SslOpts::default();
    if let Some(ca) = sub.get_one::<String>("tls-ca") {
        ssl = ssl
            .with_root_certs(vec![PathBuf::from(ca).into()])
            .with_danger_skip_domain_validation(true);
    }
    if sub.get_flag("tls-insecure") {
        ssl = ssl
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true);
    }
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(arg(sub, "password")))
        .prefer_socket(false)
        .ssl_opts(ssl);
    Conn::new(opts)
        .await
        .map_err(|e| Failed(format!("gateway {gw} as {user}: {e}")))
}

fn sql_err(what: &str) -> impl Fn(mysql_async::Error) -> Failed + '_ {
    move |e| Failed(format!("{what}: {e}"))
}

/// `` `name` `` with backticks doubled.
pub(crate) fn ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `'name'@'host'`, as SHOW GRANTS FOR takes it.
fn account(name: &str, host: &str) -> String {
    format!("'{}'@'{}'", name.replace('\'', "''"), host.replace('\'', "''"))
}

/// The order-independent checksum of a table (see the module docs).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Checksum {
    /// Rows.
    pub rows: u64,
    sum: u128,
}

impl Checksum {
    /// Adds one row.
    pub(crate) fn add(&mut self, row: &[Value]) {
        let mut h = Sha256::new();
        for v in row {
            match v {
                Value::NULL => h.update([0xff]),
                Value::Bytes(b) => {
                    h.update([0x00]);
                    h.update((b.len() as u64).to_le_bytes());
                    h.update(b);
                }
                // The text protocol only returns bytes; other forms are
                // hashed by their SQL text.
                other => {
                    let s = other.as_sql(false);
                    h.update([0x01]);
                    h.update((s.len() as u64).to_le_bytes());
                    h.update(s.as_bytes());
                }
            }
        }
        let d = h.finalize();
        let mut first = [0u8; 16];
        first.copy_from_slice(&d[..16]);
        self.sum = self.sum.wrapping_add(u128::from_le_bytes(first));
        self.rows += 1;
    }

    /// `<rows>:<32 hex digits>`.
    pub(crate) fn text(&self) -> String {
        format!("{}:{:032x}", self.rows, self.sum)
    }
}

/// How a value of `col` is written in an INSERT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Literal {
    /// Digits as they are.
    Number,
    /// `X'..'`: binary strings, BIT, geometry.
    Binary,
    /// `_utf8mb4 X'..'`: text (the connection's character set), converted
    /// to the column's on insert, so no escaping is needed.
    Text,
}

pub(crate) fn literal_kind(col: &Column) -> Literal {
    use ColumnType::{
        MYSQL_TYPE_BIT, MYSQL_TYPE_BLOB, MYSQL_TYPE_DECIMAL, MYSQL_TYPE_DOUBLE, MYSQL_TYPE_FLOAT,
        MYSQL_TYPE_GEOMETRY, MYSQL_TYPE_INT24, MYSQL_TYPE_LONG, MYSQL_TYPE_LONG_BLOB,
        MYSQL_TYPE_LONGLONG, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_SHORT,
        MYSQL_TYPE_STRING, MYSQL_TYPE_TINY, MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_VAR_STRING,
        MYSQL_TYPE_VARCHAR, MYSQL_TYPE_YEAR,
    };
    match col.column_type() {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_INT24
        | MYSQL_TYPE_LONGLONG | MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_FLOAT
        | MYSQL_TYPE_DOUBLE | MYSQL_TYPE_YEAR => Literal::Number,
        MYSQL_TYPE_BIT | MYSQL_TYPE_GEOMETRY => Literal::Binary,
        MYSQL_TYPE_STRING | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR | MYSQL_TYPE_BLOB
        | MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB
            if col.character_set() == 63 =>
        {
            Literal::Binary
        }
        _ => Literal::Text,
    }
}

/// Appends `v` as a SQL literal.
pub(crate) fn push_literal(out: &mut String, v: &Value, kind: Literal) {
    let hex = |out: &mut String, b: &[u8]| {
        out.push_str("X'");
        for x in b {
            let _ = write!(out, "{x:02x}");
        }
        out.push('\'');
    };
    match (v, kind) {
        (Value::NULL, _) => out.push_str("NULL"),
        (Value::Bytes(b), Literal::Number) if b.iter().all(|c| c.is_ascii_graphic()) => {
            out.push_str(&String::from_utf8_lossy(b));
        }
        (Value::Bytes(b), Literal::Binary) => hex(out, b),
        (Value::Bytes(b), _) => {
            out.push_str("_utf8mb4 ");
            hex(out, b);
        }
        (other, _) => out.push_str(&other.as_sql(false)),
    }
}

fn now_rfc3339() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let days = secs / 86_400;
    let rem = secs % 86_400;
    // Civil date from days (Howard Hinnant).
    let z = i64::try_from(days).unwrap_or(0) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem / 60) % 60,
        rem % 60
    )
}

/// Drops `DEFINER=...` and `SQL SECURITY ...` from a CREATE VIEW, so the
/// view is created as the importing user.
pub(crate) fn strip_definer(create: &str) -> String {
    let mut s = create.to_owned();
    if let Some(i) = s.find(" DEFINER=") {
        let rest = &s[i + 9..];
        // The definer is `user`@`host`: skip to the space after it.
        let end = rest.find(" SQL SECURITY").or_else(|| rest.find(" VIEW"));
        if let Some(e) = end {
            s = format!("{}{}", &s[..i], &rest[e..]);
        }
    }
    if let Some(i) = s.find(" SQL SECURITY ") {
        let rest = &s[i + 14..];
        if let Some(e) = rest.find(' ') {
            s = format!("{}{}", &s[..i], &rest[e..]);
        }
    }
    s
}

/// Lists the tenant's databases, tables and views (no rows).
async fn schema(conn: &mut Conn, skipped: &mut Vec<String>) -> Result<Vec<DbEntry>, Failed> {
    let dbs: Vec<String> = conn
        .query("SHOW DATABASES")
        .await
        .map_err(sql_err("SHOW DATABASES"))?;
    let mut out = Vec::new();
    for db in dbs
        .into_iter()
        .filter(|d| !SYSTEM_DBS.contains(&d.to_ascii_lowercase().as_str()))
    {
        let create: Option<(String, String)> = conn
            .query_first(format!("SHOW CREATE DATABASE {}", ident(&db)))
            .await
            .map_err(sql_err("SHOW CREATE DATABASE"))?;
        let mut entry = DbEntry {
            create: create.map(|(_, c)| c).unwrap_or_default(),
            name: db.clone(),
            tables: Vec::new(),
        };
        let tables: Vec<(String, String)> = conn
            .query(format!("SHOW FULL TABLES FROM {}", ident(&db)))
            .await
            .map_err(sql_err("SHOW FULL TABLES"))?;
        let mut views = Vec::new();
        for (name, kind) in tables {
            if kind.eq_ignore_ascii_case("VIEW") {
                let row: Option<mysql_async::Row> = conn
                    .query_first(format!("SHOW CREATE VIEW {}.{}", ident(&db), ident(&name)))
                    .await
                    .map_err(sql_err("SHOW CREATE VIEW"))?;
                let create: String = row
                    .and_then(|r| r.get::<String, usize>(1))
                    .unwrap_or_default();
                views.push(TableEntry {
                    name,
                    kind: "view".into(),
                    create: strip_definer(&create),
                    rows: 0,
                    checksum: String::new(),
                });
            } else {
                let row: Option<(String, String)> = conn
                    .query_first(format!("SHOW CREATE TABLE {}.{}", ident(&db), ident(&name)))
                    .await
                    .map_err(sql_err("SHOW CREATE TABLE"))?;
                entry.tables.push(TableEntry {
                    name,
                    kind: "table".into(),
                    create: row.map(|(_, c)| c).unwrap_or_default(),
                    rows: 0,
                    checksum: String::new(),
                });
            }
        }
        entry.tables.extend(views);
        // Triggers and routines aren't carried yet: say so.
        if let Ok(trg) = conn
            .query::<mysql_async::Row, _>(format!("SHOW TRIGGERS FROM {}", ident(&db)))
            .await
            && !trg.is_empty()
        {
            skipped.push(format!("{db}: {} triggers", trg.len()));
        }
        out.push(entry);
    }
    Ok(out)
}

/// Reads every row of `db`.`t`: its checksum, and its INSERTs to `sink`
/// when given.
async fn scan(
    conn: &mut Conn,
    db: &str,
    t: &str,
    mut sink: Option<&mut dyn std::io::Write>,
) -> Result<Checksum, Failed> {
    let mut sum = Checksum::default();
    let mut result = conn
        .query_iter(format!("SELECT * FROM {}.{}", ident(db), ident(t)))
        .await
        .map_err(sql_err("SELECT"))?;
    let cols = result.columns().unwrap_or_else(|| Vec::new().into());
    let kinds: Vec<Literal> = cols.iter().map(literal_kind).collect();
    let head = format!(
        "INSERT INTO {} ({}) VALUES ",
        ident(t),
        cols.iter()
            .map(|c| ident(&c.name_str()))
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut stmt = String::new();
    let mut io_err: Option<std::io::Error> = None;
    result
        .for_each(|row| {
            let values = row.unwrap();
            sum.add(&values);
            if let Some(w) = sink.as_deref_mut() {
                if stmt.is_empty() {
                    stmt.push_str(&head);
                } else {
                    stmt.push(',');
                }
                stmt.push('(');
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        stmt.push(',');
                    }
                    push_literal(&mut stmt, v, kinds.get(i).copied().unwrap_or(Literal::Text));
                }
                stmt.push(')');
                if stmt.len() >= MAX_STATEMENT {
                    stmt.push('\n');
                    if let Err(e) = w.write_all(stmt.as_bytes()) {
                        io_err.get_or_insert(e);
                    }
                    stmt.clear();
                }
            }
        })
        .await
        .map_err(sql_err("reading rows"))?;
    if let Some(w) = sink {
        if !stmt.is_empty() {
            stmt.push('\n');
            w.write_all(stmt.as_bytes())
                .map_err(|e| Failed(format!("writing {db}.{t}: {e}")))?;
        }
        w.flush()
            .map_err(|e| Failed(format!("writing {db}.{t}: {e}")))?;
    }
    if let Some(e) = io_err {
        return Err(Failed(format!("writing {db}.{t}: {e}")));
    }
    Ok(sum)
}

async fn grants_of(conn: &mut Conn, name: &str, host: &str) -> Result<Vec<String>, Failed> {
    let mut lines: Vec<String> = conn
        .query(format!("SHOW GRANTS FOR {}", account(name, host)))
        .await
        .map_err(sql_err("SHOW GRANTS"))?;
    lines.sort();
    Ok(lines)
}

fn data_file(dir: &Path, db: &str, t: &str) -> PathBuf {
    // Logical names may hold any character: hex keeps file names safe.
    let enc = |s: &str| s.bytes().fold(String::new(), |mut o, b| {
        if b.is_ascii_alphanumeric() || b == b'_' {
            o.push(char::from(b));
        } else {
            let _ = write!(o, "%{b:02x}");
        }
        o
    });
    dir.join("data").join(enc(db)).join(format!("{}.sql", enc(t)))
}

fn write_json<T: Serialize>(path: &Path, v: &T) -> Result<(), Failed> {
    let text = serde_json::to_vec_pretty(v).map_err(|e| Failed(e.to_string()))?;
    std::fs::write(path, text).map_err(|e| Failed(format!("{}: {e}", path.display())))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Failed> {
    let text = std::fs::read(path).map_err(|e| Failed(format!("{}: {e}", path.display())))?;
    serde_json::from_slice(&text).map_err(|e| Failed(format!("{}: {e}", path.display())))
}

/// What an export or import reports.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct Report {
    tenant: String,
    dir: String,
    databases: usize,
    tables: usize,
    rows: u64,
    accounts: usize,
    settings: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mismatches: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    notes: Vec<String>,
}

fn render(r: &Report, verb: &str) -> String {
    let mut s = format!(
        "{verb} tenant {} ({}): {} databases, {} tables, {} rows, {} accounts, {} setting rows\n",
        r.tenant, r.dir, r.databases, r.tables, r.rows, r.accounts, r.settings
    );
    for n in &r.notes {
        let _ = writeln!(s, "note: {n}");
    }
    for m in &r.mismatches {
        let _ = writeln!(s, "MISMATCH: {m}");
    }
    if verb == "imported" && r.mismatches.is_empty() {
        s.push_str("verified: every table's checksum and every account's grants match the export\n");
    }
    s
}

/// `sprigctl tenant export <name> --to <dir> --gateway ... --user ... --password ...`.
pub(crate) async fn export(c: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let tenant = arg(sub, "name");
    let dir = PathBuf::from(arg(sub, "to"));
    if dir.exists()
        && std::fs::read_dir(&dir)
            .map_err(|e| Failed(format!("{}: {e}", dir.display())))?
            .next()
            .is_some()
    {
        return Err(Failed(format!("{} is not empty", dir.display())));
    }
    std::fs::create_dir_all(dir.join("data")).map_err(|e| Failed(format!("{}: {e}", dir.display())))?;
    let view: TenantView = c
        .get(&format!("/admin/v1/tenants/{}", escape(&tenant)))
        .await?;
    let accounts: PortableAccounts = c
        .get(&format!("/admin/v1/tenants/{}/accounts", escape(&tenant)))
        .await?;
    let settings: TenantSettingsResponse = c
        .get(&format!("/admin/v1/tenants/{}/settings", escape(&tenant)))
        .await?;
    let own: Vec<TenantSettingRowView> = settings
        .rows
        .into_iter()
        .filter(|r| r.scope != "ancestor")
        .collect();

    let mut conn = connect(sub, &arg(sub, "user")).await?;
    let mut skipped = Vec::new();
    // One snapshot for every table (R0: a tenant's schemas live on one
    // cluster, behind one pinned backend connection).
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(sql_err("START TRANSACTION"))?;
    let mut dbs = schema(&mut conn, &mut skipped).await?;
    let mut report = Report {
        tenant: tenant.clone(),
        dir: dir.display().to_string(),
        databases: dbs.len(),
        ..Report::default()
    };
    for db in &mut dbs {
        for t in db.tables.iter_mut().filter(|t| t.kind == "table") {
            let path = data_file(&dir, &db.name, &t.name);
            if let Some(p) = path.parent() {
                std::fs::create_dir_all(p).map_err(|e| Failed(format!("{}: {e}", p.display())))?;
            }
            let file = std::fs::File::create(&path)
                .map_err(|e| Failed(format!("{}: {e}", path.display())))?;
            let mut w = BufWriter::new(file);
            let sum = scan(&mut conn, &db.name, &t.name, Some(&mut w)).await?;
            t.rows = sum.rows;
            t.checksum = sum.text();
            report.tables += 1;
            report.rows += sum.rows;
            eprintln!("exported {}.{}: {} rows", db.name, t.name, sum.rows);
        }
    }
    conn.query_drop("COMMIT").await.map_err(sql_err("COMMIT"))?;
    let mut grants = BTreeMap::new();
    for u in &accounts.users {
        grants.insert(account(&u.name, &u.host), grants_of(&mut conn, &u.name, &u.host).await?);
    }
    for (name, host) in &accounts.roles {
        grants.insert(account(name, host), grants_of(&mut conn, name, host).await?);
    }
    let _ = conn.disconnect().await;
    report.accounts = accounts.users.len() + accounts.roles.len();
    report.settings = own.len();
    if !skipped.is_empty() {
        report.notes.push(format!("not exported: {}", skipped.join("; ")));
    }
    let manifest = Manifest {
        format: FORMAT,
        tenant: tenant.clone(),
        tenant_id: view.tenant_id.0,
        catalog_cluster: view
            .cluster
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        exported_at: now_rfc3339(),
        databases: dbs,
        skipped,
    };
    write_json(&dir.join("accounts.json"), &AccountsFile { accounts, grants })?;
    write_json(&dir.join("settings.json"), &own)?;
    write_json(&dir.join("manifest.json"), &manifest)?;
    print(out, &report, |r| render(r, "exported"));
    Ok(())
}

/// The scope of a source setting row in the target tenant.
pub(crate) fn target_scope(row: &TenantSettingRowView, source: &str, target: &str) -> Option<ScopeRef> {
    let rest = row.target.strip_prefix(source)?;
    let text = match row.scope.as_str() {
        "tenant" => format!("tenant:{target}"),
        "database" => format!("db:{target}{rest}"),
        "object" => format!("object:{target}{rest}"),
        _ => return None,
    };
    text.parse().ok()
}

/// `sprigctl tenant import <dir> --into <tenant> [--create [--cluster c]]`.
#[allow(clippy::too_many_lines)] // the import's steps, in order
pub(crate) async fn import(c: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let dir = PathBuf::from(arg(sub, "from"));
    let target = arg(sub, "into");
    let manifest: Manifest = read_json(&dir.join("manifest.json"))?;
    if manifest.format != FORMAT {
        return Err(Failed(format!(
            "export format {} isn't supported (expected {FORMAT})",
            manifest.format
        )));
    }
    let accounts: AccountsFile = read_json(&dir.join("accounts.json"))?;
    let settings: Vec<TenantSettingRowView> = read_json(&dir.join("settings.json"))?;
    let mut report = Report {
        tenant: target.clone(),
        dir: dir.display().to_string(),
        databases: manifest.databases.len(),
        accounts: accounts.grants.len(),
        settings: settings.len(),
        ..Report::default()
    };
    let verify_only = sub.get_flag("verify-only");
    if !verify_only {
        if sub.get_flag("create") {
            let cluster = sub
                .get_one::<String>("cluster")
                .map(|c| sprig_common::ids::ClusterId::new(c.as_str()))
                .transpose()
                .map_err(|e| format!("--cluster: {e}"))?;
            let r: CreateTenantResponse = c
                .post(
                    "/admin/v1/tenants",
                    &CreateTenantRequest {
                        name: target.clone(),
                        parent: None,
                        cluster,
                    },
                )
                .await?;
            eprintln!("created tenant {} ({})", r.tenant.name, r.tenant.tenant_id.0);
        }
        // Accounts first, so the source's administrators log in to the
        // target with their own passwords.
        let mut req = accounts.accounts.clone();
        req.roles.clear();
        req.tenant_admins = req
            .users
            .iter()
            .filter(|u| {
                accounts
                    .grants
                    .get(&account(&u.name, &u.host))
                    .is_some_and(|g| g.iter().any(|l| l.contains("SPRIG_TENANT_ADMIN")))
            })
            .map(|u| (u.name.clone(), u.host.clone()))
            .collect();
        let r: ImportAccountsResponse = c
            .post(
                &format!("/admin/v1/tenants/{}/accounts", escape(&target)),
                &req,
            )
            .await?;
        if !r.existing.is_empty() {
            report
                .notes
                .push(format!("accounts already in the target, left alone: {}", r.existing.join(", ")));
        }
        eprintln!("accounts: {} created, {} existing", r.created.len(), r.existing.len());
    }
    let mut conn = connect(sub, &arg(sub, "user")).await?;
    if !verify_only {
        for s in [
            "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'",
            "SET SESSION foreign_key_checks = 0",
        ] {
            if let Err(e) = conn.query_drop(s).await {
                report.notes.push(format!("{s}: {e}"));
            }
        }
        for db in &manifest.databases {
            let create = if db.create.is_empty() {
                format!("CREATE DATABASE {}", ident(&db.name))
            } else {
                db.create.clone()
            };
            conn.query_drop(create)
                .await
                .map_err(sql_err(&format!("CREATE DATABASE {}", db.name)))?;
            conn.query_drop(format!("USE {}", ident(&db.name)))
                .await
                .map_err(sql_err("USE"))?;
            for t in db.tables.iter().filter(|t| t.kind == "table") {
                conn.query_drop(&t.create)
                    .await
                    .map_err(sql_err(&format!("CREATE TABLE {}.{}", db.name, t.name)))?;
                let path = data_file(&dir, &db.name, &t.name);
                let f = std::fs::File::open(&path)
                    .map_err(|e| Failed(format!("{}: {e}", path.display())))?;
                for line in std::io::BufReader::new(f).lines() {
                    let line = line.map_err(|e| Failed(format!("{}: {e}", path.display())))?;
                    if line.is_empty() {
                        continue;
                    }
                    conn.query_drop(line)
                        .await
                        .map_err(sql_err(&format!("loading {}.{}", db.name, t.name)))?;
                }
                eprintln!("imported {}.{}: {} rows", db.name, t.name, t.rows);
            }
        }
        // Views after every table; a view on another view may need a
        // second pass.
        let mut views: Vec<(&str, &TableEntry)> = manifest
            .databases
            .iter()
            .flat_map(|d| d.tables.iter().filter(|t| t.kind == "view").map(move |t| (d.name.as_str(), t)))
            .collect();
        for _ in 0..4 {
            let mut left = Vec::new();
            for (db, v) in views {
                conn.query_drop(format!("USE {}", ident(db)))
                    .await
                    .map_err(sql_err("USE"))?;
                if conn.query_drop(&v.create).await.is_err() {
                    left.push((db, v));
                }
            }
            views = left;
            if views.is_empty() {
                break;
            }
        }
        for (db, v) in &views {
            report.mismatches.push(format!("view {db}.{} could not be created", v.name));
        }
        // Roles, then every grant line.
        for (name, host) in &accounts.accounts.roles {
            if let Err(e) = conn
                .query_drop(format!("CREATE ROLE IF NOT EXISTS {}", account(name, host)))
                .await
            {
                report.notes.push(format!("CREATE ROLE {name}: {e}"));
            }
        }
        let ordered: Vec<(&String, &Vec<String>)> = accounts.grants.iter().collect();
        for (who, lines) in ordered {
            for l in lines {
                if l.starts_with("GRANT USAGE ON *.* TO ") {
                    continue;
                }
                if let Err(e) = conn.query_drop(l.as_str()).await {
                    report.notes.push(format!("{who}: {l}: {e}"));
                }
            }
        }
        // Setting rows, value then bounds then lock.
        let actor = std::env::var("SPRIGCTL_ACTOR")
            .or_else(|_| std::env::var("USER"))
            .ok();
        for row in &settings {
            let Some(scope) = target_scope(row, &manifest.tenant, &target) else {
                report.notes.push(format!("setting {} at {} {}: no target scope", row.key, row.scope, row.target));
                continue;
            };
            let path = format!("/admin/v1/settings/{}", escape(&row.key));
            let reason = format!("import of tenant {}", manifest.tenant);
            if let Some(v) = &row.value {
                let _: SettingWriteResponse = c
                    .put(
                        &path,
                        &SetSettingRequest {
                            scope: Some(scope.clone()),
                            value: Some(v.clone()),
                            value_text: None,
                            reason: reason.clone(),
                            actor: actor.clone(),
                        },
                    )
                    .await?;
            }
            if let Some(b) = &row.bounds {
                if b.min.is_some() || b.max.is_some() || b.allowed.is_some() {
                    let _: SettingWriteResponse = c
                        .post(
                            &format!("{path}:bounds"),
                            &SetBoundsRequest {
                                scope: Some(scope.clone()),
                                min: b.min,
                                max: b.max,
                                allowed: b.allowed.clone(),
                                allowed_text: None,
                                clear: Vec::new(),
                                reason: reason.clone(),
                                actor: actor.clone(),
                            },
                        )
                        .await?;
                }
                if b.locked {
                    let _: SettingWriteResponse = c
                        .post(
                            &format!("{path}:lock"),
                            &LockSettingRequest {
                                scope: Some(scope.clone()),
                                reason: reason.clone(),
                                actor: actor.clone(),
                            },
                        )
                        .await?;
                }
            }
        }
    }
    // Verify.
    for db in &manifest.databases {
        for t in db.tables.iter().filter(|t| t.kind == "table") {
            let got = scan(&mut conn, &db.name, &t.name, None).await?;
            report.tables += 1;
            report.rows += got.rows;
            if got.text() != t.checksum {
                report.mismatches.push(format!(
                    "{}.{}: checksum {} in the target, {} in the export",
                    db.name,
                    t.name,
                    got.text(),
                    t.checksum
                ));
            }
        }
    }
    for (who, want) in &accounts.grants {
        let (name, host) = who
            .strip_prefix('\'')
            .and_then(|s| s.strip_suffix('\''))
            .and_then(|s| s.split_once("'@'"))
            .map(|(n, h)| (n.replace("''", "'"), h.replace("''", "'")))
            .unwrap_or_default();
        match grants_of(&mut conn, &name, &host).await {
            Ok(got) if &got == want => {}
            Ok(got) => report.mismatches.push(format!(
                "{who}: grants differ: target {got:?}, export {want:?}"
            )),
            Err(e) => report.mismatches.push(format!("{who}: {}", e.0)),
        }
    }
    let target_rows: TenantSettingsResponse = c
        .get(&format!("/admin/v1/tenants/{}/settings", escape(&target)))
        .await?;
    for row in &settings {
        let want_target = row.target.replacen(&manifest.tenant, &target, 1);
        let found = target_rows.rows.iter().any(|r| {
            r.scope == row.scope && r.target == want_target && r.key == row.key && r.value == row.value
        });
        if !found {
            report
                .mismatches
                .push(format!("setting {} at {} {} missing or different", row.key, row.scope, want_target));
        }
    }
    let _ = conn.disconnect().await;
    let failed = !report.mismatches.is_empty();
    print(out, &report, |r| render(r, "imported"));
    if failed {
        return Err(Failed(format!(
            "39971 ER_SPRIG_IMPORT_VERIFY_FAILED: {} differences from the export",
            report.mismatches.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn checksums_ignore_row_order_and_tell_null_from_empty() {
        let a = vec![Value::Bytes(b"1".to_vec()), Value::NULL];
        let b = vec![Value::Bytes(b"2".to_vec()), Value::Bytes(Vec::new())];
        let mut x = Checksum::default();
        x.add(&a);
        x.add(&b);
        let mut y = Checksum::default();
        y.add(&b);
        y.add(&a);
        assert_eq!(x, y);
        assert!(x.text().starts_with("2:"));
        let mut z = Checksum::default();
        z.add(&[Value::Bytes(b"1".to_vec()), Value::Bytes(Vec::new())]);
        z.add(&b);
        assert_ne!(x, z);
    }

    #[test]
    fn literals_need_no_escaping() {
        let mut s = String::new();
        push_literal(&mut s, &Value::Bytes(b"it's \\ \n".to_vec()), Literal::Text);
        assert_eq!(s, "_utf8mb4 X'69742773205c200a'");
        s.clear();
        push_literal(&mut s, &Value::Bytes(b"-12.50".to_vec()), Literal::Number);
        assert_eq!(s, "-12.50");
        s.clear();
        push_literal(&mut s, &Value::Bytes(vec![0, 255]), Literal::Binary);
        assert_eq!(s, "X'00ff'");
        s.clear();
        push_literal(&mut s, &Value::NULL, Literal::Text);
        assert_eq!(s, "NULL");
    }

    #[test]
    fn view_definers_are_dropped() {
        let v = "CREATE ALGORITHM=UNDEFINED DEFINER=`admin`@`%` SQL SECURITY DEFINER VIEW `v` AS select 1 AS `x`";
        assert_eq!(
            strip_definer(v),
            "CREATE ALGORITHM=UNDEFINED VIEW `v` AS select 1 AS `x`"
        );
    }

    #[test]
    fn setting_rows_move_to_the_target_tenant() {
        let row = |scope: &str, target: &str| TenantSettingRowView {
            scope: scope.into(),
            target: target.into(),
            key: "k".into(),
            value: None,
            bounds: None,
            set_by: String::new(),
            set_at: String::new(),
            version: 0,
        };
        assert_eq!(
            target_scope(&row("tenant", "src"), "src", "dst").unwrap().to_string(),
            "tenant:dst"
        );
        assert_eq!(
            target_scope(&row("database", "src.shop"), "src", "dst")
                .unwrap()
                .to_string(),
            "db:dst.shop"
        );
        assert_eq!(
            target_scope(&row("object", "src.shop.t"), "src", "dst")
                .unwrap()
                .to_string(),
            "object:dst.shop.t"
        );
        assert!(target_scope(&row("ancestor", "x"), "src", "dst").is_none());
    }

    #[test]
    fn rfc3339_has_the_right_shape() {
        let t = now_rfc3339();
        assert_eq!(t.len(), 20, "{t}");
        assert!(t.ends_with('Z'));
    }
}
