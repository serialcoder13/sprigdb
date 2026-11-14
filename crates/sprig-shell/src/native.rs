//! Native protocols (`--protocol`): what drivers see.
//!
//! - **mysql**: `mysql_async`, text protocol, multi-statements; cancel by
//!   `KILL QUERY` on a second connection.
//! - **redis**, **mongo**, **bolt**: see [`redis`], [`mongo`] and [`bolt`].

pub mod bolt;
pub mod mongo;
pub mod redis;

use base64::Engine as _;
use mysql_async::prelude::Queryable;

use crate::error::{Result, ShellCode, ShellError};
use crate::lang::Lang;
use crate::model::{Column, EndInfo, ResultEvent, Sink};

/// A native protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// The gateway's REST API (the default).
    Http,
    /// MySQL wire protocol.
    Mysql,
    /// MongoDB wire protocol.
    Mongo,
    /// RESP.
    Redis,
    /// Bolt.
    Bolt,
}

impl Protocol {
    /// Parses a protocol name.
    ///
    /// # Errors
    /// 39983 for another name.
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "http" | "https" | "rest" => Ok(Protocol::Http),
            "mysql" => Ok(Protocol::Mysql),
            "mongo" | "mongodb" => Ok(Protocol::Mongo),
            "redis" => Ok(Protocol::Redis),
            "bolt" | "neo4j" => Ok(Protocol::Bolt),
            _ => Err(ShellError::shell(
                ShellCode::LanguageUnsupported,
                format!("protocol {s:?}: use http, mysql, mongo, redis or bolt"),
            )),
        }
    }

    /// The language a protocol speaks.
    #[must_use]
    pub fn lang(self) -> Option<Lang> {
        match self {
            Protocol::Http => None,
            Protocol::Mysql => Some(Lang::Sql),
            Protocol::Mongo => Some(Lang::Mongo),
            Protocol::Redis => Some(Lang::Redis),
            Protocol::Bolt => Some(Lang::Cypher),
        }
    }

    /// The default port.
    #[must_use]
    pub fn default_port(self) -> u16 {
        match self {
            Protocol::Http => 8080,
            Protocol::Mysql => 3306,
            Protocol::Mongo => 27017,
            Protocol::Redis => 6379,
            Protocol::Bolt => 7687,
        }
    }
}

/// Where and as whom to connect natively.
#[derive(Clone)]
pub struct NativeTarget {
    /// `host:port`.
    pub addr: String,
    /// The user (`tenant.user` or the MySQL form the profile uses).
    pub user: String,
    /// The password.
    pub password: String,
    /// The database.
    pub database: Option<String>,
}

impl std::fmt::Debug for NativeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeTarget")
            .field("addr", &self.addr)
            .field("user", &self.user)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

fn unreachable(e: impl std::fmt::Display) -> ShellError {
    ShellError::shell(ShellCode::Unreachable, e.to_string())
}

/// A native connection.
pub enum Native {
    /// MySQL.
    Mysql(MysqlBackend),
    /// Redis.
    Redis(redis::RedisBackend),
    /// MongoDB.
    Mongo(mongo::MongoBackend),
    /// Bolt.
    Bolt(bolt::BoltBackend),
}

impl Native {
    /// Connects with `protocol`.
    ///
    /// # Errors
    /// 39987, or the server's login error.
    pub async fn connect(protocol: Protocol, t: NativeTarget) -> Result<Self> {
        match protocol {
            Protocol::Mysql => Ok(Native::Mysql(MysqlBackend::connect(t).await?)),
            Protocol::Redis => Ok(Native::Redis(redis::RedisBackend::connect(t).await?)),
            Protocol::Mongo => Ok(Native::Mongo(mongo::MongoBackend::connect(t).await?)),
            Protocol::Bolt => Ok(Native::Bolt(bolt::BoltBackend::connect(t).await?)),
            Protocol::Http => Err(ShellError::shell(
                ShellCode::LanguageUnsupported,
                "http is not a native protocol",
            )),
        }
    }

    /// For the status bar.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Native::Mysql(m) => format!("mysql://{}", m.target.addr),
            Native::Redis(r) => format!("redis://{}", r.addr()),
            Native::Mongo(m) => format!("mongodb://{}", m.addr()),
            Native::Bolt(b) => format!("bolt://{}", b.addr()),
        }
    }

    /// Whether a transaction is open.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        match self {
            Native::Mysql(m) => m.in_txn,
            Native::Bolt(b) => b.in_transaction(),
            _ => false,
        }
    }

    /// Runs one statement.
    ///
    /// # Errors
    /// 39983 for a language this protocol doesn't speak; the server's error.
    pub async fn run(&mut self, stmt: &str, lang: Lang, sink: &mut dyn Sink) -> Result<()> {
        let want = match self {
            Native::Mysql(_) => Lang::Sql,
            Native::Redis(_) => Lang::Redis,
            Native::Mongo(_) => Lang::Mongo,
            Native::Bolt(_) => Lang::Cypher,
        };
        if lang != want {
            return Err(ShellError::shell(
                ShellCode::LanguageUnsupported,
                format!(
                    "this connection speaks {}, not {}",
                    want.name(),
                    lang.name()
                ),
            ));
        }
        match self {
            Native::Mysql(m) => m.run(stmt, sink).await,
            Native::Redis(r) => r.run(stmt, sink).await,
            Native::Mongo(m) => m.run(stmt, sink).await,
            Native::Bolt(b) => b.run(stmt, sink).await,
        }
    }

    /// What cancels the running statement from another task.
    #[must_use]
    pub fn canceller(&self) -> Option<Canceller> {
        match self {
            Native::Mysql(m) => m.conn_id.map(|id| Canceller::MysqlKill {
                target: m.target.clone(),
                id,
            }),
            _ => None,
        }
    }

    /// After a cancelled statement: the connection's state is unknown, so
    /// reconnect.
    ///
    /// # Errors
    /// 39987.
    pub async fn reset(&mut self) -> Result<()> {
        match self {
            Native::Mysql(m) => m.reconnect().await,
            Native::Redis(r) => r.reconnect().await,
            Native::Mongo(m) => m.reconnect().await,
            Native::Bolt(b) => b.reconnect().await,
        }
    }
}

/// Cancels a statement from outside its task.
#[derive(Debug, Clone)]
pub enum Canceller {
    /// `KILL QUERY id` on a second MySQL connection.
    MysqlKill {
        /// Where.
        target: NativeTarget,
        /// The connection id.
        id: u32,
    },
    /// A REST statement: the gateway notices a dropped `Sql.Query` stream
    /// only when it next writes to it, so a statement that sends nothing
    /// for a while (a long aggregate, `SLEEP`) would run on. This finds the
    /// caller's own session running exactly this text in `SHOW FULL
    /// PROCESSLIST` and sends `KILL QUERY` for it, through the same API and
    /// credential; with two such sessions it kills neither.
    RestKill {
        /// The REST client, as the user.
        client: crate::api::DataClient,
        /// The statement's text as sent.
        sql: String,
    },
}

/// The session ids in `SHOW FULL PROCESSLIST` events running `sql`.
#[must_use]
pub fn sessions_running(events: &[ResultEvent], sql: &str) -> Vec<String> {
    let want = sql.trim();
    crate::model::ResultSet::from_events(events)
        .into_iter()
        .flat_map(|s| {
            let col = |n: &str| {
                s.columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(n))
            };
            let (id, cmd, info) = (col("Id"), col("Command"), col("Info"));
            s.rows.into_iter().filter_map(move |r| {
                let get = |i: Option<usize>| i.and_then(|i| r.get(i).cloned().flatten());
                let running = get(cmd).is_none_or(|c| !c.eq_ignore_ascii_case("Sleep"));
                (running && get(info).is_some_and(|t| t.trim() == want))
                    .then(|| get(id))
                    .flatten()
            })
        })
        .collect()
}

impl Canceller {
    /// Sends the cancel.
    ///
    /// # Errors
    /// 39987, or the server's error.
    pub async fn cancel(&self) -> Result<()> {
        match self {
            Canceller::MysqlKill { target, id } => {
                let mut c = MysqlBackend::open(target).await?;
                let r = c.query_drop(format!("KILL QUERY {id}")).await;
                let _ = c.disconnect().await;
                r.map_err(mysql_error)
            }
            Canceller::RestKill { client, sql } => {
                let mut c = crate::model::Collect::default();
                let caps = crate::api::Caps::default();
                client
                    .sql_query(None, "SHOW FULL PROCESSLIST", None, caps, &mut c)
                    .await?;
                let ids = sessions_running(&c.0, sql);
                if let [id] = ids.as_slice()
                    && id.chars().all(|ch| ch.is_ascii_digit())
                {
                    let mut k = crate::model::Collect::default();
                    client
                        .sql_query(None, &format!("KILL QUERY {id}"), None, caps, &mut k)
                        .await?;
                }
                Ok(())
            }
        }
    }
}

/// A MySQL wire connection.
pub struct MysqlBackend {
    target: NativeTarget,
    conn: Option<mysql_async::Conn>,
    conn_id: Option<u32>,
    in_txn: bool,
}

fn mysql_error(e: mysql_async::Error) -> ShellError {
    match e {
        mysql_async::Error::Server(s) => ShellError::server(u32::from(s.code), "", s.message),
        other => unreachable(other),
    }
}

fn cell(v: mysql_async::Value, binary: bool) -> Option<String> {
    use mysql_async::Value as V;
    match v {
        V::NULL => None,
        V::Bytes(b) if binary => Some(base64::engine::general_purpose::STANDARD.encode(b)),
        V::Bytes(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        V::Int(i) => Some(i.to_string()),
        V::UInt(u) => Some(u.to_string()),
        V::Float(f) => Some(f.to_string()),
        V::Double(f) => Some(f.to_string()),
        other => Some(other.as_sql(true).trim_matches('\'').to_owned()),
    }
}

impl MysqlBackend {
    async fn open(t: &NativeTarget) -> Result<mysql_async::Conn> {
        let (host, port) = split_addr(&t.addr, 3306)?;
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some(t.user.clone()))
            .pass(Some(t.password.clone()))
            .db_name(t.database.clone())
            .prefer_socket(false);
        mysql_async::Conn::new(opts).await.map_err(mysql_error)
    }

    /// Connects.
    ///
    /// # Errors
    /// 39987 or the server's login error.
    pub async fn connect(target: NativeTarget) -> Result<Self> {
        let conn = Self::open(&target).await?;
        Ok(Self {
            conn_id: Some(conn.id()),
            conn: Some(conn),
            target,
            in_txn: false,
        })
    }

    async fn reconnect(&mut self) -> Result<()> {
        self.conn = None;
        let c = Self::open(&self.target).await?;
        self.conn_id = Some(c.id());
        self.conn = Some(c);
        self.in_txn = false;
        Ok(())
    }

    async fn run(&mut self, stmt: &str, sink: &mut dyn Sink) -> Result<()> {
        if self.conn.is_none() {
            self.reconnect().await?;
        }
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| unreachable("not connected"))?;
        let mut qr = conn.query_iter(stmt).await.map_err(mysql_error)?;
        let io = |e: std::io::Error| ShellError::shell(ShellCode::Cancelled, e.to_string());
        loop {
            let cols = qr
                .columns()
                .unwrap_or_else(|| std::sync::Arc::from(Vec::new()));
            if cols.is_empty() {
                let end = EndInfo {
                    affected: qr.affected_rows(),
                    last_insert_id: qr.last_insert_id().unwrap_or(0),
                    warnings: u32::from(qr.warnings()),
                    info: qr.info().into_owned(),
                    truncated: false,
                };
                // Consume the OK packet and move on.
                let _ = qr.next().await.map_err(mysql_error)?;
                sink.event(ResultEvent::End(end)).map_err(io)?;
            } else {
                let binary: Vec<bool> = cols
                    .iter()
                    .map(|c| {
                        c.character_set() == 63
                            && !matches!(
                                c.column_type(),
                                mysql_async::consts::ColumnType::MYSQL_TYPE_LONG
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_LONGLONG
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_TINY
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_SHORT
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_INT24
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_DECIMAL
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_DOUBLE
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_FLOAT
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_DATE
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_DATETIME
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_TIMESTAMP
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_TIME
                                    | mysql_async::consts::ColumnType::MYSQL_TYPE_YEAR
                            )
                    })
                    .collect();
                let header: Vec<Column> = cols
                    .iter()
                    .zip(&binary)
                    .map(|(c, &b)| Column {
                        name: c.name_str().into_owned(),
                        type_name: type_name(c.column_type()).to_owned(),
                        binary: b,
                    })
                    .collect();
                sink.event(ResultEvent::Header(header)).map_err(io)?;
                let mut batch = Vec::new();
                let mut warnings = 0;
                while let Some(row) = qr.next().await.map_err(mysql_error)? {
                    let vals: Vec<Option<String>> = row
                        .unwrap()
                        .into_iter()
                        .zip(&binary)
                        .map(|(v, &b)| cell(v, b))
                        .collect();
                    batch.push(vals);
                    if batch.len() >= 500 {
                        sink.event(ResultEvent::Rows(std::mem::take(&mut batch)))
                            .map_err(io)?;
                    }
                    warnings = u32::from(qr.warnings());
                }
                if !batch.is_empty() {
                    sink.event(ResultEvent::Rows(batch)).map_err(io)?;
                }
                sink.event(ResultEvent::End(EndInfo {
                    warnings,
                    ..EndInfo::default()
                }))
                .map_err(io)?;
            }
            if qr.is_empty() {
                break;
            }
        }
        drop(qr);
        let w = crate::lang::top_words(stmt, false);
        match w.first().map(String::as_str) {
            Some("BEGIN") => self.in_txn = true,
            Some("START") if w.get(1).is_some_and(|x| x == "TRANSACTION") => self.in_txn = true,
            Some("COMMIT" | "ROLLBACK") if w.get(1).is_none_or(|x| x != "TO") => {
                self.in_txn = false;
            }
            _ => {}
        }
        Ok(())
    }
}

fn type_name(t: mysql_async::consts::ColumnType) -> &'static str {
    use mysql_async::consts::ColumnType as T;
    match t {
        T::MYSQL_TYPE_TINY => "TINYINT",
        T::MYSQL_TYPE_SHORT => "SMALLINT",
        T::MYSQL_TYPE_INT24 => "MEDIUMINT",
        T::MYSQL_TYPE_LONG => "INT",
        T::MYSQL_TYPE_LONGLONG => "BIGINT",
        T::MYSQL_TYPE_DECIMAL | T::MYSQL_TYPE_NEWDECIMAL => "DECIMAL",
        T::MYSQL_TYPE_FLOAT => "FLOAT",
        T::MYSQL_TYPE_DOUBLE => "DOUBLE",
        T::MYSQL_TYPE_YEAR => "YEAR",
        T::MYSQL_TYPE_DATE => "DATE",
        T::MYSQL_TYPE_DATETIME => "DATETIME",
        T::MYSQL_TYPE_TIMESTAMP => "TIMESTAMP",
        T::MYSQL_TYPE_TIME => "TIME",
        T::MYSQL_TYPE_JSON => "JSON",
        T::MYSQL_TYPE_BLOB
        | T::MYSQL_TYPE_TINY_BLOB
        | T::MYSQL_TYPE_MEDIUM_BLOB
        | T::MYSQL_TYPE_LONG_BLOB => "BLOB",
        T::MYSQL_TYPE_BIT => "BIT",
        T::MYSQL_TYPE_GEOMETRY => "GEOMETRY",
        _ => "VARCHAR",
    }
}

/// Splits `host:port`, with a default port.
///
/// # Errors
/// 39980 for a bad port.
pub fn split_addr(addr: &str, default: u16) -> Result<(String, u16)> {
    match addr.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') || h.starts_with('[') => {
            let port = p.parse().map_err(|_| {
                ShellError::shell(
                    ShellCode::ProfileInvalid,
                    format!("address {addr:?}: bad port"),
                )
            })?;
            Ok((h.trim_matches(['[', ']']).to_owned(), port))
        }
        _ => Ok((addr.to_owned(), default)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_running_session() {
        let col = |n: &str| Column::named(n);
        let s = |x: &str| Some(x.to_owned());
        let evs = vec![
            ResultEvent::Header(vec![col("Id"), col("User"), col("Command"), col("Info")]),
            ResultEvent::Rows(vec![
                vec![s("7"), s("acme.app"), s("Query"), s("SELECT SLEEP(30)")],
                vec![
                    s("8"),
                    s("acme.app"),
                    s("Query"),
                    s("SHOW FULL PROCESSLIST"),
                ],
                vec![s("9"), s("acme.app"), s("Sleep"), s("SELECT SLEEP(30)")],
            ]),
            ResultEvent::End(EndInfo::default()),
        ];
        assert_eq!(sessions_running(&evs, " SELECT SLEEP(30) "), ["7"]);
        assert_eq!(
            split_addr("db:3307", 3306).expect("addr"),
            ("db".to_owned(), 3307)
        );
        assert_eq!(
            split_addr("db", 3306).expect("addr"),
            ("db".to_owned(), 3306)
        );
    }
}
