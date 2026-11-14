//! PostgreSQL destinations through `sqlx` (`sqlx-postgres` 0.8 builds
//! with the workspace's rustls and ring).

use std::time::Duration;

use async_trait::async_trait;
use sprig_common::Result;
use sqlx::Row as _;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};

use super::{Backend, Col, DbErr, DbResult, Dialect, Param, TableInfo, Tx};
use crate::model::ConnKind;
use crate::target::{Conn, TargetCtx};

/// A PostgreSQL server.
pub(super) struct PgBackend {
    pool: PgPool,
}

impl PgBackend {
    /// Opens a pool. With `tls = off | required` it connects to the
    /// checked address itself; `verify_full` needs the host name for the
    /// certificate check, and `sqlx` has no override for it, so it
    /// connects by name after the name and its addresses were checked.
    pub(super) async fn connect(conn: &Conn, ctx: &TargetCtx) -> Result<Self> {
        let host = conn.need("host")?;
        let port = u16::try_from(conn.opt_u64("port")?.unwrap_or(5432))
            .map_err(|_| crate::model::bad_option("port must be below 65536"))?;
        let addr = conn
            .dest
            .resolve(&host, port, Some(ConnKind::Postgresql), Some(&conn.options))
            .await?;
        let user = conn.secret("username")?;
        let pass = conn.secret("password")?;
        let tls = conn
            .opt("tls")
            .unwrap_or_else(|| "verify_full".into())
            .to_ascii_lowercase();
        let (mode, target_host) = match tls.as_str() {
            "off" => (PgSslMode::Disable, addr.ip().to_string()),
            "required" => (PgSslMode::Require, addr.ip().to_string()),
            "verify_full" => (PgSslMode::VerifyFull, host.clone()),
            o => {
                return Err(crate::model::bad_option(format!(
                    "tls must be off, required or verify_full, not '{o}'"
                )));
            }
        };
        let mut o = PgConnectOptions::new_without_pgpass()
            .host(&target_host)
            .port(addr.port())
            .username(&user)
            .password(&pass)
            .ssl_mode(mode)
            .application_name("sprigdb-sink")
            .options([("timezone", "UTC")]);
        if let Some(db) = conn.opt("database") {
            o = o.database(&db);
        }
        if let Some(pem) = conn.opt("ca_pem") {
            o = o.ssl_root_cert_from_pem(pem.into_bytes());
        }
        let lanes = u32::try_from(ctx.opts.u64("lanes", 8)?).unwrap_or(8);
        let pool = PgPoolOptions::new()
            .min_connections(0)
            .max_connections(lanes.clamp(1, 64) + 2)
            .idle_timeout(Some(Duration::from_secs(60)))
            .max_lifetime(Some(Duration::from_secs(300)))
            .acquire_timeout(Duration::from_secs(10))
            .connect_lazy_with(o);
        Ok(Self { pool })
    }
}

/// Classifies a `sqlx` error by SQLSTATE.
pub(super) fn classify(e: &sqlx::Error) -> DbErr {
    match e {
        sqlx::Error::Database(db) => {
            let code = db
                .code()
                .map(std::borrow::Cow::into_owned)
                .unwrap_or_default();
            let msg = format!("PostgreSQL error {code}: {}", db.message());
            match code.as_str() {
                "40001" | "40P01" | "53300" | "57P01" | "57P02" | "57P03" | "55P03" | "57014" => {
                    DbErr::Retry(msg)
                }
                "42P01" | "42703" | "3F000" => DbErr::Schema(msg),
                c if c.starts_with("22") || c.starts_with("23") => DbErr::Row(msg),
                c if c.starts_with("08") => DbErr::Retry(msg),
                _ => DbErr::Retry(msg),
            }
        }
        other => DbErr::Retry(format!("PostgreSQL connection: {other}")),
    }
}

fn bind(
    mut q: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
    p: Vec<Param>,
) -> sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for v in p {
        q = match v {
            Param::Null => q.bind(None::<String>),
            Param::Text(s) => q.bind(s),
            Param::Bytes(b) => q.bind(b),
            Param::Int(n) => q.bind(n),
        };
    }
    q
}

struct PgTx {
    tx: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
}

impl PgTx {
    fn t(&mut self) -> DbResult<&mut sqlx::Transaction<'static, sqlx::Postgres>> {
        self.tx
            .as_mut()
            .ok_or_else(|| DbErr::Retry("transaction already ended".into()))
    }
}

#[async_trait]
impl Tx for PgTx {
    async fn exec(&mut self, sql: &str, p: Vec<Param>) -> DbResult<u64> {
        let t = self.t()?;
        bind(sqlx::query(sql), p)
            .execute(&mut **t)
            .await
            .map(|r| r.rows_affected())
            .map_err(|e| classify(&e))
    }

    async fn offsets(&mut self, sql: &str, p: Vec<Param>) -> DbResult<Vec<(String, Vec<u8>, u64)>> {
        let t = self.t()?;
        let rows = bind(sqlx::query(sql), p)
            .fetch_all(&mut **t)
            .await
            .map_err(|e| classify(&e))?;
        rows.iter()
            .map(|r| {
                let k: String = r.try_get(0).map_err(|e| classify(&e))?;
                let pos: Vec<u8> = r.try_get(1).map_err(|e| classify(&e))?;
                let e: i64 = r.try_get(2).map_err(|e| classify(&e))?;
                Ok((k, pos, u64::try_from(e).unwrap_or(0)))
            })
            .collect()
    }

    async fn strings(&mut self, sql: &str, p: Vec<Param>) -> DbResult<Vec<String>> {
        let t = self.t()?;
        let rows = bind(sqlx::query(sql), p)
            .fetch_all(&mut **t)
            .await
            .map_err(|e| classify(&e))?;
        rows.iter()
            .map(|r| r.try_get::<String, _>(0).map_err(|e| classify(&e)))
            .collect()
    }

    async fn commit(mut self: Box<Self>) -> DbResult<()> {
        let t = self
            .tx
            .take()
            .ok_or_else(|| DbErr::Retry("transaction already ended".into()))?;
        t.commit()
            .await
            .map_err(|e| DbErr::Retry(format!("commit: {e}")))
    }

    async fn rollback(mut self: Box<Self>) {
        if let Some(t) = self.tx.take() {
            let _ = t.rollback().await;
        }
    }
}

#[async_trait]
impl Backend for PgBackend {
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    async fn begin(&self) -> DbResult<Box<dyn Tx>> {
        let tx = self.pool.begin().await.map_err(|e| classify(&e))?;
        Ok(Box::new(PgTx { tx: Some(tx) }))
    }

    async fn table(&self, name: &str) -> DbResult<Option<TableInfo>> {
        let reg = super::plan::table_ident(Dialect::Postgres, name);
        let cols = sqlx::query(
            "SELECT a.attname::text, format_type(a.atttypid, NULL), t.typname::text \
             FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(&reg)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| classify(&e))?;
        if cols.is_empty() {
            return Ok(None);
        }
        let pk = sqlx::query(
            "SELECT a.attname::text FROM pg_index i \
             JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, n) ON true \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
             WHERE i.indrelid = to_regclass($1) AND i.indisprimary ORDER BY k.n",
        )
        .bind(&reg)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| classify(&e))?;
        let mut out = Vec::new();
        for r in &cols {
            let name: String = r.try_get(0).map_err(|e| classify(&e))?;
            let cast: String = r.try_get(1).map_err(|e| classify(&e))?;
            let typ: String = r.try_get(2).map_err(|e| classify(&e))?;
            out.push(Col {
                name,
                json: typ == "json" || typ == "jsonb",
                binary: typ == "bytea",
                cast,
            });
        }
        Ok(Some(TableInfo {
            name: name.to_owned(),
            cols: out,
            pk: pk
                .iter()
                .map(|r| r.try_get::<String, _>(0).map_err(|e| classify(&e)))
                .collect::<DbResult<_>>()?,
        }))
    }

    async fn exec(&self, sql: &str) -> DbResult<()> {
        sqlx::raw_sql(sql)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| classify(&e))
    }
}
