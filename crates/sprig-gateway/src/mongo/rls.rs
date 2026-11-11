//! Row-level security on collections from MongoDB drivers (chapter 29,
//! "Policies", R5; decision RL33):
//!
//! ```text
//! {sprigCreatePolicy: "<coll>", name: "own", mode: "permissive" | "restrictive",
//!  commands: "all" | ["find", "insert", "update", "remove"],
//!  to: ["user@host", "role", ...],          (none: PUBLIC)
//!  using: {<filter>}, check: {<filter>}}
//! {sprigDropPolicy: "<coll>", name: "own", ifExists: true}
//! {sprigRowSecurity: "<coll>", enable: true | false, force: true | false}
//! ```
//!
//! Each becomes chapter 29's SQL statement on the collection's table, run
//! on the command's own executor session, so privileges (`ALTER` on the
//! table), the catalog, the write triggers and the policy views are SQL's.
//! A filter becomes SQL over `doc` and `_types` (`sprig_doc::policy`); the
//! filters themselves are kept with the policy (`using_mongo`,
//! `check_mongo`) for change streams.

use bson::{Bson, Document, doc};
use sprig_doc::engine::{Engine, SqlRunner};
use sprig_doc::sql::{ident, str_lit};
use sprig_proto_mongo::errors::MongoError;

use super::sql::SqlSession;

/// Whether `name` is one of this module's commands.
#[must_use]
pub fn handles(name: &str) -> bool {
    matches!(
        name,
        "sprigCreatePolicy" | "sprigDropPolicy" | "sprigRowSecurity"
    )
}

fn bad(why: impl Into<String>) -> MongoError {
    MongoError::new(sprig_proto_mongo::errors::code::FAILED_TO_PARSE, why.into())
}

fn commands_of(v: Option<&Bson>) -> Result<String, MongoError> {
    let list: Vec<&str> = match v {
        None => return Ok("ALL".into()),
        Some(Bson::String(s)) => vec![s.as_str()],
        Some(Bson::Array(a)) => a
            .iter()
            .map(|x| x.as_str().ok_or_else(|| bad("commands must be strings")))
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(bad("commands must be a string or an array")),
    };
    let mut out: Vec<&str> = Vec::new();
    for c in list {
        let sql = match c {
            "all" => "ALL",
            "find" | "select" => "SELECT",
            "insert" => "INSERT",
            "update" => "UPDATE",
            "remove" | "delete" => "DELETE",
            other => return Err(bad(format!("unknown policy command '{other}'"))),
        };
        if !out.contains(&sql) {
            out.push(sql);
        }
    }
    if out.contains(&"ALL") {
        return Ok("ALL".into());
    }
    Ok(out.join(", "))
}

fn grantees_of(v: Option<&Bson>) -> Result<String, MongoError> {
    let Some(v) = v else {
        return Ok("PUBLIC".into());
    };
    let Bson::Array(a) = v else {
        return Err(bad("to must be an array of user and role names"));
    };
    if a.is_empty() {
        return Ok("PUBLIC".into());
    }
    let mut out = Vec::new();
    for g in a {
        let s = g.as_str().ok_or_else(|| bad("to must hold strings"))?;
        out.push(match s.split_once('@') {
            Some((u, h)) => format!("{}@{}", str_lit(u), str_lit(h)),
            None if s.eq_ignore_ascii_case("public") => "PUBLIC".into(),
            None => ident(s),
        });
    }
    Ok(out.join(", "))
}

fn filter_of<'a>(body: &'a Document, key: &str) -> Result<Option<&'a Document>, MongoError> {
    match body.get(key) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Document(d)) => Ok(Some(d)),
        Some(_) => Err(bad(format!("{key} must be a filter document"))),
    }
}

/// The relaxed extended JSON of `d`, as a SQL string literal.
fn json_lit(d: &Document) -> String {
    str_lit(&Bson::Document(d.clone()).into_relaxed_extjson().to_string())
}

/// Runs one of this module's commands in `db`.
///
/// # Errors
/// 26 for a missing collection, 9 for a malformed command, 39364 for a
/// filter a policy can't hold, and the SQL statement's own errors (1142
/// without `ALTER` on the collection, 1304, 1305).
pub async fn command(
    engine: &Engine,
    sql: &mut SqlSession,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    let coll = body
        .get_str(name)
        .map_err(|_| bad(format!("{name} needs a collection name")))?;
    let ns = sprig_doc::engine::Ns {
        db: db.to_owned(),
        coll: coll.to_owned(),
    };
    let Some(meta) = engine.meta(sql, &ns).await? else {
        return Err(MongoError::new(
            sprig_proto_mongo::errors::code::NAMESPACE_NOT_FOUND,
            format!("ns does not exist: {db}.{coll}"),
        ));
    };
    let table = format!("{}.{}", ident(db), ident(&meta.table));
    let stmt = match name {
        "sprigCreatePolicy" => {
            let pname = body
                .get_str("name")
                .map_err(|_| bad("sprigCreatePolicy needs a name"))?;
            let mode = match body.get_str("mode").unwrap_or("permissive") {
                "permissive" => "PERMISSIVE",
                "restrictive" => "RESTRICTIVE",
                other => return Err(bad(format!("unknown policy mode '{other}'"))),
            };
            let using = filter_of(body, "using")?;
            let check = filter_of(body, "check")?;
            let mut s = format!(
                "CREATE POLICY {} ON {table} AS {mode} FOR {} TO {}",
                ident(pname),
                commands_of(body.get("commands"))?,
                grantees_of(body.get("to"))?
            );
            if let Some(u) = using {
                s.push_str(&format!(" USING ({})", sprig_doc::policy::to_sql(u)?));
            }
            if let Some(c) = check {
                s.push_str(&format!(" WITH CHECK ({})", sprig_doc::policy::to_sql(c)?));
            }
            if using.is_some() || check.is_some() {
                s.push_str(" SPRIG MONGO");
                if let Some(u) = using {
                    s.push_str(&format!(" USING {}", json_lit(u)));
                }
                if let Some(c) = check {
                    s.push_str(&format!(" CHECK {}", json_lit(c)));
                }
            }
            s
        }
        "sprigDropPolicy" => {
            let pname = body
                .get_str("name")
                .map_err(|_| bad("sprigDropPolicy needs a name"))?;
            let if_exists = body.get_bool("ifExists").unwrap_or(false);
            format!(
                "DROP POLICY {}{} ON {table}",
                if if_exists { "IF EXISTS " } else { "" },
                ident(pname)
            )
        }
        _ => {
            let mut out = Vec::new();
            if let Ok(e) = body.get_bool("enable") {
                out.push(format!(
                    "ALTER TABLE {table} {} ROW LEVEL SECURITY",
                    if e { "ENABLE" } else { "DISABLE" }
                ));
            }
            if let Ok(f) = body.get_bool("force") {
                out.push(format!(
                    "ALTER TABLE {table} {} ROW LEVEL SECURITY",
                    if f { "FORCE" } else { "NO FORCE" }
                ));
            }
            if out.is_empty() {
                return Err(bad("sprigRowSecurity needs enable or force"));
            }
            for s in &out {
                sql.exec(s)
                    .await
                    .map_err(|e| sprig_doc::engine::mongo_error(&e))?;
            }
            return Ok(doc! {"ok": 1.0});
        }
    };
    sql.exec(&stmt)
        .await
        .map_err(|e| sprig_doc::engine::mongo_error(&e))?;
    Ok(doc! {"ok": 1.0})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_parts() {
        assert_eq!(commands_of(None).unwrap(), "ALL");
        assert_eq!(
            commands_of(Some(&Bson::Array(vec!["find".into(), "remove".into()]))).unwrap(),
            "SELECT, DELETE"
        );
        assert!(commands_of(Some(&Bson::String("drop".into()))).is_err());
        assert_eq!(grantees_of(None).unwrap(), "PUBLIC");
        assert_eq!(
            grantees_of(Some(&Bson::Array(vec!["bob@%".into(), "readers".into()]))).unwrap(),
            "'bob'@'%', `readers`"
        );
        assert_eq!(
            json_lit(&doc! {"owner": "$$USER"}),
            "'{\"owner\":\"$$USER\"}'"
        );
    }
}
