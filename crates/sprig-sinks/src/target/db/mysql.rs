//! MySQL and MariaDB destinations through `mysql_async`.

use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn as MyConn, Opts, OptsBuilder, Params, Pool, PoolConstraints, PoolOpts};
use sprig_common::Result;

use super::{Backend, Col, DbErr, DbResult, Dialect, Param, TableInfo, Tx};
use crate::model::ConnKind;
use crate::target::{Conn, TargetCtx};

/// A MySQL or MariaDB server.
pub(super) struct MyBackend {
    pool: Pool,
    /// The connection's database (`DATABASE()`), for tables named without
    /// one.
    database: Option<String>,
}

fn tls_mode(conn: &Conn) -> Result<String> {
    let m = conn
        .opt("tls")
        .unwrap_or_else(|| "verify_full".into())
        .to_ascii_lowercase();
    match m.as_str() {
        "off" | "required" | "verify_full" => Ok(m),
        _ => Err(crate::model::bad_option(format!(
            "tls must be off, required or verify_full, not '{m}'"
        ))),
    }
}

impl MyBackend {
    /// Opens a pool to the connection's checked address.
    pub(super) async fn connect(conn: &Conn, ctx: &TargetCtx) -> Result<Self> {
        let host = conn.need("host")?;
        let port = u16::try_from(conn.opt_u64("port")?.unwrap_or(3306))
            .map_err(|_| crate::model::bad_option("port must be below 65536"))?;
        let addr = conn
            .dest
            .resolve(&host, port, Some(ConnKind::Mysql), Some(&conn.options))
            .await?;
        let user = conn.secret("username")?;
        let pass = conn.secret("password")?;
        let database = conn.opt("database");
        let lanes = usize::try_from(ctx.opts.u64("lanes", 8)?).unwrap_or(8);
        let constraints = PoolConstraints::new(0, lanes.clamp(1, 64) + 2).unwrap_or_default();
        let pool_opts = PoolOpts::default()
            .with_constraints(constraints)
            .with_inactive_connection_ttl(Duration::from_secs(60))
            .with_ttl_check_interval(Duration::from_secs(30));
        let mut b = OptsBuilder::default()
            .ip_or_hostname(addr.ip().to_string())
            .tcp_port(addr.port())
            .user(Some(user.as_str()))
            .pass(Some(pass.as_str()))
            .db_name(database.clone())
            .client_found_rows(true)
            .pool_opts(pool_opts)
            .conn_ttl(Some(Duration::from_secs(300)))
            .init(vec!["SET SESSION time_zone = '+00:00'"]);
        match tls_mode(conn)?.as_str() {
            "off" => {}
            "required" => {
                b = b.ssl_opts(Some(
                    mysql_async::SslOpts::default()
                        .with_danger_accept_invalid_certs(true)
                        .with_danger_skip_domain_validation(true),
                ));
            }
            _ => {
                let mut s = mysql_async::SslOpts::default()
                    .with_danger_tls_hostname_override(Some(host.clone()));
                if let Some(pem) = conn.opt("ca_pem") {
                    s = s.with_root_certs(vec![pem.into_bytes().into()]);
                }
                b = b.ssl_opts(Some(s));
            }
        }
        let opts: Opts = b.into();
        Ok(Self {
            pool: Pool::new(opts),
            database,
        })
    }

    fn split<'a>(&'a self, name: &'a str) -> (Option<&'a str>, &'a str) {
        match name.split_once('.') {
            Some((db, t)) => (Some(db), t),
            None => (self.database.as_deref(), name),
        }
    }
}

impl Drop for MyBackend {
    fn drop(&mut self) {
        let pool = self.pool.clone();
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            h.spawn(async move {
                let _ = pool.disconnect().await;
            });
        }
    }
}

/// Classifies a `mysql_async` error.
pub(super) fn classify(e: &mysql_async::Error) -> DbErr {
    match e {
        mysql_async::Error::Server(s) => {
            let msg = format!("MySQL error {} ({}): {}", s.code, s.state, s.message);
            match s.code {
                // A column or table that doesn't fit the records.
                1146 | 1054 => DbErr::Schema(msg),
                // Rows the destination refuses.
                1048 | 1062 | 1264 | 1265 | 1292 | 1366 | 1367 | 1406 | 1411 | 1416 | 1451
                | 1452 | 1690 | 3140 | 3819 | 4025 | 4078 | 1918 => DbErr::Row(msg),
                _ => DbErr::Retry(msg),
            }
        }
        other => DbErr::Retry(format!("MySQL connection: {other}")),
    }
}

fn params(p: Vec<Param>) -> Params {
    if p.is_empty() {
        return Params::Empty;
    }
    Params::Positional(
        p.into_iter()
            .map(|v| match v {
                Param::Null => mysql_async::Value::NULL,
                Param::Text(s) => mysql_async::Value::Bytes(s.into_bytes()),
                Param::Bytes(b) => mysql_async::Value::Bytes(b),
                Param::Int(n) => mysql_async::Value::Int(n),
            })
            .collect(),
    )
}

struct MyTx {
    conn: Option<MyConn>,
}

impl MyTx {
    fn c(&mut self) -> DbResult<&mut MyConn> {
        self.conn
            .as_mut()
            .ok_or_else(|| DbErr::Retry("transaction already ended".into()))
    }
}

#[async_trait]
impl Tx for MyTx {
    async fn exec(&mut self, sql: &str, p: Vec<Param>) -> DbResult<u64> {
        let c = self.c()?;
        c.exec_drop(sql, params(p))
            .await
            .map_err(|e| classify(&e))?;
        Ok(c.affected_rows())
    }

    async fn offsets(&mut self, sql: &str, p: Vec<Param>) -> DbResult<Vec<(String, Vec<u8>, u64)>> {
        self.c()?
            .exec(sql, params(p))
            .await
            .map_err(|e| classify(&e))
    }

    async fn strings(&mut self, sql: &str, p: Vec<Param>) -> DbResult<Vec<String>> {
        self.c()?
            .exec(sql, params(p))
            .await
            .map_err(|e| classify(&e))
    }

    async fn commit(mut self: Box<Self>) -> DbResult<()> {
        let mut c = self
            .conn
            .take()
            .ok_or_else(|| DbErr::Retry("transaction already ended".into()))?;
        match c.query_drop("COMMIT").await {
            Ok(()) => Ok(()),
            Err(e) => {
                // The outcome is unknown: never reuse the connection.
                let _ = c.disconnect().await;
                Err(DbErr::Retry(format!("commit: {e}")))
            }
        }
    }

    async fn rollback(mut self: Box<Self>) {
        if let Some(mut c) = self.conn.take()
            && c.query_drop("ROLLBACK").await.is_err()
        {
            let _ = c.disconnect().await;
        }
    }
}

impl Drop for MyTx {
    fn drop(&mut self) {
        // A transaction dropped mid-way (the task was stopped): roll back
        // before the connection goes back to the pool.
        if let Some(mut c) = self.conn.take()
            && let Ok(h) = tokio::runtime::Handle::try_current()
        {
            h.spawn(async move {
                if c.query_drop("ROLLBACK").await.is_err() {
                    let _ = c.disconnect().await;
                }
            });
        }
    }
}

#[async_trait]
impl Backend for MyBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Mysql
    }

    async fn begin(&self) -> DbResult<Box<dyn Tx>> {
        let mut c = self.pool.get_conn().await.map_err(|e| classify(&e))?;
        c.query_drop("START TRANSACTION")
            .await
            .map_err(|e| classify(&e))?;
        Ok(Box::new(MyTx { conn: Some(c) }))
    }

    async fn table(&self, name: &str) -> DbResult<Option<TableInfo>> {
        let (db, t) = self.split(name);
        let mut c = self.pool.get_conn().await.map_err(|e| classify(&e))?;
        let schema = match db {
            Some(d) => d.to_owned(),
            None => c
                .query_first::<Option<String>, _>("SELECT DATABASE()")
                .await
                .map_err(|e| classify(&e))?
                .flatten()
                .ok_or_else(|| {
                    DbErr::Schema(format!(
                        "table {name} names no database and the connection has none"
                    ))
                })?,
        };
        let cols: Vec<(String, String)> = c
            .exec(
                "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                (schema.as_str(), t),
            )
            .await
            .map_err(|e| classify(&e))?;
        if cols.is_empty() {
            return Ok(None);
        }
        let pk: Vec<String> = c
            .exec(
                "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' \
                 ORDER BY SEQ_IN_INDEX",
                (schema.as_str(), t),
            )
            .await
            .map_err(|e| classify(&e))?;
        Ok(Some(TableInfo {
            name: name.to_owned(),
            cols: cols
                .into_iter()
                .map(|(n, ty)| {
                    let ty = ty.to_ascii_lowercase();
                    Col {
                        name: n,
                        json: ty == "json",
                        binary: ty.contains("binary") || ty.contains("blob"),
                        cast: ty,
                    }
                })
                .collect(),
            pk,
        }))
    }

    async fn exec(&self, sql: &str) -> DbResult<()> {
        let mut c = self.pool.get_conn().await.map_err(|e| classify(&e))?;
        c.query_drop(sql).await.map_err(|e| classify(&e))
    }
}
