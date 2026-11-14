//! `SET GLOBAL` and `SET PERSIST` for tenants (server-wide assignments
//! under `SYSTEM_VARIABLES_ADMIN`).
//!
//! ```text
//! SET GLOBAL v = x  (also PERSIST, PERSIST_ONLY, @@global.v, SET GLOBAL TRANSACTION ...)
//!   no SYSTEM_VARIABLES_ADMIN                     → 1227 naming the privilege
//!   v not in the allow-list                       → 1227: server-wide, not the tenant's
//!   v in sql_mode, time_zone, transaction_isolation, max_execution_time, wait_timeout:
//!     x evaluated (a literal, DEFAULT, or SELECT <expr> on the session) and checked
//!     as the persona checks it (1231, 1232, 1298, 1193)
//!     → the tenant setting compat.global.<v> (DEFAULT resets it), never MariaDB
//!   session assignments in the same statement run after it, as the session's
//! session start (the first request, and after a connection reset):
//!   SET SESSION <each compat.global.* the tenant has> through the session's own
//!   path; @@global.<v> reads answer the tenant's value
//! ```
//!
//! As on MySQL, a global assignment changes the sessions that start
//! afterwards, not the session that made it. `PERSIST` and `PERSIST_ONLY`
//! do the same: a tenant default is stored in the catalog, so it persists
//! anyway.

use std::borrow::Cow;
use std::collections::BTreeMap;

use bytes::Bytes;
use sprig_catalog::{Principal, SetRequest, Target};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::Persona;
use sprig_settings::{Apply, HardBounds, Scopes, Setting, SettingChange, Value};
use sprig_sql::compat::setvar::{self, GlobalValue};
use sprig_sql::lex::LexOptions;

use super::PassSession;
use crate::pipeline::ExecCtx;

type Text = Cow<'static, str>;

macro_rules! global_key {
    ($NAME:ident, $key:literal, $doc:literal) => {
        #[doc = concat!("`", $key, "`.")]
        pub(crate) static $NAME: Setting<Text> = Setting {
            key: $key,
            default: Cow::Borrowed(""),
            scopes: Scopes::TREE,
            hard: HardBounds::NONE,
            apply: Apply::NewSessions,
            operator_only: false,
            owner: 28,
            doc: $doc,
        };
        sprig_settings::register!($NAME);
    };
}

global_key!(
    GLOBAL_SQL_MODE,
    "compat.global.sql_mode",
    "The tenant's default sql_mode for new sessions (SET GLOBAL sql_mode); empty: the persona's"
);
global_key!(
    GLOBAL_TIME_ZONE,
    "compat.global.time_zone",
    "The tenant's default time_zone for new sessions (SET GLOBAL time_zone); empty: the server's"
);
global_key!(
    GLOBAL_TRANSACTION_ISOLATION,
    "compat.global.transaction_isolation",
    "The tenant's default transaction_isolation for new sessions (SET GLOBAL); empty: the server's"
);
global_key!(
    GLOBAL_MAX_EXECUTION_TIME,
    "compat.global.max_execution_time",
    "The tenant's default max_execution_time in ms for new 3306 sessions (SET GLOBAL); empty: 0"
);
global_key!(
    GLOBAL_WAIT_TIMEOUT,
    "compat.global.wait_timeout",
    "The tenant's default wait_timeout in seconds for new sessions (SET GLOBAL); empty: the gateway's"
);

/// The allow-list's keys, by variable.
fn key_of(var: &str) -> Option<&'static Setting<Text>> {
    Some(match var {
        "sql_mode" => &GLOBAL_SQL_MODE,
        "time_zone" => &GLOBAL_TIME_ZONE,
        "transaction_isolation" => &GLOBAL_TRANSACTION_ISOLATION,
        "max_execution_time" => &GLOBAL_MAX_EXECUTION_TIME,
        "wait_timeout" => &GLOBAL_WAIT_TIMEOUT,
        _ => return None,
    })
}

fn bad_value(var: &str, v: &str) -> SprigError {
    SprigError::standard(
        1231,
        *b"42000",
        format!("Variable '{var}' can't be set to the value of '{v}'"),
    )
}

fn denied_var(var: &str) -> SprigError {
    SprigError::standard(
        1227,
        *b"42000",
        format!(
            "Access denied; GLOBAL and PERSIST assignments of '{var}' would change the server for \
             every tenant. A tenant can set only {} globally",
            setvar::TENANT_GLOBALS.join(", ")
        ),
    )
}

/// MySQL's integer checks: the value as a number (1232 otherwise), within
/// `max`.
fn integer(var: &str, v: &str, min: u64, max: u64) -> Result<String> {
    let n: u64 = v.trim().parse().map_err(|_| {
        SprigError::standard(
            1232,
            *b"42000",
            format!("Incorrect argument type to variable '{var}'"),
        )
    })?;
    if n < min || n > max {
        return Err(bad_value(var, v));
    }
    Ok(n.to_string())
}

/// What a global assignment does to its key.
enum Change {
    Set(String),
    Reset,
}

/// What [`PassSession::tenant_set_global`] leaves for the caller.
pub(super) enum GlobalOutcome {
    /// Done: answer OK.
    Done,
    /// The statement's session assignments still run: this `SET`.
    Rest(Bytes),
}

impl PassSession {
    /// The tenant's defaults of the allow-list (variable to value), as
    /// the session's catalog snapshot resolves them.
    pub(super) fn tenant_globals(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        let Some(ts) = self.tenant.as_deref() else {
            return out;
        };
        let Some(cat) = ts.catalog() else {
            return out;
        };
        let svc = ts.services();
        let ctx = cat.setting_ctx(&svc.dc);
        for var in setvar::TENANT_GLOBALS {
            if *var == "max_execution_time" && self.persona != Persona::MySql84 {
                continue;
            }
            if let Some(k) = key_of(var) {
                let v = svc.settings.get(k, &ctx, Some(&cat.settings));
                if !v.is_empty() {
                    out.insert((*var).to_owned(), v.into_owned());
                }
            }
        }
        out
    }

    /// Runs a server-wide `SET` (see the module docs). The caller has
    /// checked that it is one ([`setvar::sets_global`]).
    ///
    /// # Errors
    /// 1227 without `SYSTEM_VARIABLES_ADMIN` or for a variable outside the
    /// allow-list; the value's 1193, 1231, 1232 or 1298; the catalog's.
    pub(super) async fn tenant_set_global(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &[u8],
        lex: &LexOptions,
    ) -> Result<GlobalOutcome> {
        let admin = crate::pipeline::SessionExec::holds_tenant_privilege(
            &*self,
            sprig_auth::authz::Action::SystemVariablesAdmin,
        )
        .unwrap_or(false);
        if !admin {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN \
                 privilege(s) for this operation",
            ));
        }
        let Some(split) = setvar::split_global(text, lex) else {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; this GLOBAL assignment can't be read as the tenant's default: \
                 write it as SET GLOBAL <variable> = <value>",
            ));
        };
        // Check every assignment before writing any.
        let mut changes: Vec<(&'static Setting<Text>, Change)> = Vec::new();
        for a in &split.globals {
            if a.name == "max_execution_time" && self.persona != Persona::MySql84 {
                return Err(SprigError::standard(
                    1193,
                    *b"HY000",
                    "Unknown system variable 'max_execution_time'",
                ));
            }
            let Some(key) = key_of(&a.name) else {
                return Err(denied_var(&a.name));
            };
            let raw = match &a.value {
                GlobalValue::Default => {
                    changes.push((key, Change::Reset));
                    continue;
                }
                GlobalValue::Literal(v) => v.clone(),
                GlobalValue::Expr(e) => Box::pin(self.global_expr(cx, &a.name, e)).await?,
            };
            let v = Box::pin(self.check_global(cx, &a.name, &raw)).await?;
            changes.push((key, Change::Set(v)));
        }
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("SET GLOBAL outside a tenant session"));
        };
        let Some(uid) = ts.principal.user_id else {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; GLOBAL assignments need a catalog login",
            ));
        };
        let svc = std::sync::Arc::clone(ts.services());
        for (key, change) in changes {
            let change = match change {
                Change::Set(v) => SettingChange::Set(Value::Str(v)),
                Change::Reset => SettingChange::Reset,
            };
            let out = svc
                .writer
                .set(&SetRequest {
                    tenant: ts.tenant,
                    target: Target::Tenant,
                    key: key.key.to_owned(),
                    change,
                    principal: Principal::User(uid),
                    reason: "SET GLOBAL".to_owned(),
                })
                .await?;
            ts.changed(out.version);
        }
        Ok(match split.rest {
            Some(r) => GlobalOutcome::Rest(Bytes::from(r)),
            None => GlobalOutcome::Done,
        })
    }

    /// An expression's value, read with `SELECT` on the session (the
    /// common `SET GLOBAL sql_mode = (SELECT REPLACE(@@sql_mode, ...))`).
    async fn global_expr(&mut self, cx: &mut ExecCtx<'_>, var: &str, expr: &str) -> Result<String> {
        let c = self
            .run_checked(cx, &format!("SELECT {expr}"), false)
            .await?;
        match c.rows.first().and_then(|r| r.first()) {
            Some(Some(v)) => Ok(String::from_utf8_lossy(v).into_owned()),
            _ => Err(bad_value(var, "NULL")),
        }
    }

    /// The value to store for `var`, checked as the persona checks it.
    async fn check_global(&mut self, cx: &mut ExecCtx<'_>, var: &str, v: &str) -> Result<String> {
        match var {
            "sql_mode" if self.persona == Persona::MySql84 => {
                let m = match v.trim().parse::<u64>() {
                    Ok(n) => sprig_sql::compat::sqlmode::from_number(n),
                    Err(_) => sprig_sql::compat::sqlmode::parse_mysql(v),
                };
                m.map(sprig_sql::compat::sqlmode::Mode::render)
                    .map_err(|e| SprigError::standard(1231, *b"42000", e.message()))
            }
            // MariaDB's own names: the server checks them, on the
            // session's connection, without changing the session.
            "sql_mode" | "time_zone" => {
                let lit = sprig_sql::names::rewrite::sql_string(v, false);
                self.run_checked(cx, &format!("SET STATEMENT {var} = {lit} FOR DO 0"), false)
                    .await?;
                Ok(if var == "sql_mode" {
                    v.trim().to_ascii_uppercase()
                } else {
                    v.trim().to_owned()
                })
            }
            "transaction_isolation" => setvar::isolation_level(v)
                .map(str::to_owned)
                .ok_or_else(|| bad_value("transaction_isolation", v)),
            // MySQL 8.4's ranges.
            "max_execution_time" => integer(var, v, 0, u64::from(u32::MAX)),
            "wait_timeout" => integer(var, v, 1, 31_536_000),
            _ => Err(denied_var(var)),
        }
    }

    /// Gives a new session the tenant's defaults (see the module docs):
    /// once, at its first request. A default its persona refuses (a
    /// MariaDB-only `sql_mode` name on 3306) is logged and left out.
    pub(super) async fn apply_tenant_globals(&mut self, cx: &mut ExecCtx<'_>) {
        // A new session's catalog snapshot loads at its first statement.
        if let Some(ts) = self.tenant.as_deref_mut()
            && ts.catalog().is_none()
            && let Err(e) = ts.refresh().await
        {
            tracing::warn!(error = %e, "the tenant's compat.global defaults: no catalog");
            return;
        }
        let globals = self.tenant_globals();
        if globals.is_empty() {
            return;
        }
        for (var, v) in globals {
            let value = match var.as_str() {
                "max_execution_time" | "wait_timeout" => v,
                // MariaDB truncates fractional seconds by default, which is
                // what MySQL's TIME_TRUNCATE_FRACTIONAL asks for.
                "sql_mode" if self.persona == Persona::MariaDb => {
                    sprig_sql::names::rewrite::sql_string(
                        &v.split(',')
                            .filter(|m| !m.eq_ignore_ascii_case("TIME_TRUNCATE_FRACTIONAL"))
                            .collect::<Vec<_>>()
                            .join(","),
                        false,
                    )
                }
                _ => sprig_sql::names::rewrite::sql_string(&v, false),
            };
            let sql = format!("SET SESSION {var} = {value}");
            match self.run_internal(cx, &sql, false).await {
                Ok(c) if c.error.is_none() => {}
                Ok(c) => tracing::warn!(
                    variable = %var,
                    error = %c.error.map(|e| e.message).unwrap_or_default(),
                    "the tenant's compat.global default doesn't apply to this session"
                ),
                Err(e) => tracing::warn!(
                    variable = %var,
                    error = %e,
                    "the tenant's compat.global default doesn't apply to this session"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_allowed_variable_has_a_key() {
        for v in setvar::TENANT_GLOBALS {
            let k = key_of(v).unwrap();
            assert_eq!(k.key, format!("compat.global.{v}"));
        }
        assert!(key_of("max_connections").is_none());
    }

    fn errno(e: &SprigError) -> Option<u16> {
        match e.kind {
            sprig_common::ErrorKind::Standard { errno, .. } => Some(errno),
            _ => None,
        }
    }

    #[test]
    fn integers_are_checked_as_mysql_does() {
        assert_eq!(integer("wait_timeout", "60", 1, 31_536_000).unwrap(), "60");
        assert_eq!(
            errno(&integer("wait_timeout", "0", 1, 31_536_000).unwrap_err()),
            Some(1231)
        );
        assert_eq!(
            errno(&integer("wait_timeout", "x", 1, 31_536_000).unwrap_err()),
            Some(1232)
        );
        assert_eq!(errno(&denied_var("max_connections")), Some(1227));
    }
}
