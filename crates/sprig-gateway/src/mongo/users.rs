//! MongoDB user and role commands (chapter 29, "MongoDB user and role
//! commands (R5)"; build step 10).
//!
//! Commands that change accounts become chapter 29's SQL account
//! statements, run through the caller's own executor session, so the
//! caller's privileges (`CREATE USER`, `GRANT OPTION`, ...) are checked
//! exactly as for SQL and the rows land in the same `users`, `roles`,
//! `role_edges` and `privileges` tables. Reads (`usersInfo`, `rolesInfo`)
//! come from the gateway's catalog snapshot and never return credentials.
//!
//! - **Users** are the tenant's accounts `'name'@'%'`: one set of users
//!   for every protocol, so a user made here logs in over SQL too, and the
//!   other way round. Their `db` is reported as `admin`, the database
//!   drivers authenticate against.
//! - **Roles.** A MongoDB role `(role, db)` is the SQL role
//!   `mongo.<db>.<role>`. Built-in roles are created on first use with
//!   their privileges (`read` is `SELECT, WATCH` on `db.*`, `readWrite` adds
//!   `INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX`, ...);
//!   `createRole` makes a custom one from `privileges` and `roles`. Every
//!   granted role is a default role, since MongoDB has no `SET ROLE`.
//!
//! Not built: `customData`, `authenticationRestrictions`, `mechanisms`
//! other than SCRAM-SHA-256, `updateRole`, `grantPrivilegesToRole` and
//! `revokePrivilegesFromRole`, `dropAllUsersFromDatabase`.

use bson::{Bson, Document, doc};
use sprig_auth::authz::state::AuthzState;
use sprig_auth::authz::store::Grantee;
use sprig_catalog::Freshness;
use sprig_common::ids::TenantId;
use sprig_doc::engine::SqlRunner;
use sprig_proto_mongo::errors::{MongoError, code};

use super::conn::Conn;
use super::sql::SqlSession;

/// The prefix of SQL roles that are MongoDB roles.
const PREFIX: &str = "mongo.";

const USER_NOT_FOUND: i32 = 11;
const ROLE_NOT_FOUND: i32 = 31;
const DUPLICATE_ACCOUNT: i32 = 51003;
const DUPLICATE_ROLE: i32 = 51002;

/// A built-in role: its SQL privileges, and whether it is tenant-wide
/// (`*AnyDatabase`, `root`, cluster roles: only on `admin`).
fn builtin(role: &str) -> Option<(&'static str, bool)> {
    // MongoDB's read roles carry `changeStream`, which is chapter 29's
    // `WATCH` (decision bugfix-interim).
    const RW: &str = "SELECT, WATCH, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX";
    Some(match role {
        "read" => ("SELECT, WATCH", false),
        "readWrite" => (RW, false),
        "dbAdmin" => ("CREATE, DROP, ALTER, INDEX", false),
        "userAdmin" => ("GRANT OPTION", false),
        "dbOwner" => ("ALL PRIVILEGES", false),
        "enableSharding" => ("", false),
        "readAnyDatabase" | "backup" => ("SELECT, WATCH", true),
        "readWriteAnyDatabase" => (RW, true),
        "dbAdminAnyDatabase" => ("CREATE, DROP, ALTER, INDEX", true),
        "userAdminAnyDatabase" => ("CREATE USER, GRANT OPTION", true),
        "restore" => ("INSERT, CREATE, DROP, ALTER, INDEX", true),
        "clusterMonitor" | "clusterManager" | "hostManager" | "clusterAdmin" => ("PROCESS", true),
        "root" => ("ALL PRIVILEGES", true),
        _ => return None,
    })
}

fn q(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

fn qi(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn account(user: &str) -> String {
    format!("{}@'%'", q(user))
}

fn sql_role(role: &str, db: &str) -> String {
    format!("{PREFIX}{db}.{role}")
}

/// `(role, db)` of an SQL role name, when it is a MongoDB role.
fn mongo_role(name: &str) -> Option<(String, String)> {
    let rest = name.strip_prefix(PREFIX)?;
    let (db, role) = rest.split_once('.')?;
    Some((role.to_owned(), db.to_owned()))
}

fn bad(m: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, m)
}

/// The roles of a `roles` array: strings name roles on `db`.
fn roles_of(v: Option<&Bson>, db: &str) -> Result<Vec<(String, String)>, MongoError> {
    let Some(v) = v else { return Ok(Vec::new()) };
    let Bson::Array(a) = v else {
        return Err(MongoError::new(
            code::TYPE_MISMATCH,
            "roles must be an array",
        ));
    };
    a.iter()
        .map(|r| match r {
            Bson::String(s) => Ok((s.clone(), db.to_owned())),
            Bson::Document(d) => Ok((
                d.get_str("role")
                    .map_err(|_| bad("a role needs 'role'"))?
                    .to_owned(),
                d.get_str("db")
                    .map_err(|_| bad("a role needs 'db'"))?
                    .to_owned(),
            )),
            _ => Err(bad("roles are names or {role, db}")),
        })
        .collect()
}

async fn run(s: &mut SqlSession, stmt: &str) -> Result<(), MongoError> {
    s.exec(stmt).await.map(|_| ()).map_err(|e| {
        let m = sprig_doc::engine::mongo_error(&e);
        match e.errno {
            Some(1396) => MongoError::new(code::BAD_VALUE, e.message.clone()),
            _ => m,
        }
    })
}

/// Makes sure the SQL role of `(role, db)` exists; built-in roles are
/// created with their privileges.
#[allow(clippy::single_match_else)]
async fn ensure_role(s: &mut SqlSession, role: &str, db: &str) -> Result<String, MongoError> {
    let name = sql_role(role, db);
    if matches!(role, "__system" | "__queryableBackup") {
        return Err(MongoError::new(
            code::UNAUTHORIZED,
            format!("the role {role} is not available"),
        ));
    }
    match builtin(role) {
        Some((privs, any)) => {
            if any && db != "admin" {
                return Err(MongoError::new(
                    ROLE_NOT_FOUND,
                    format!("Could not find role: {role}@{db}"),
                ));
            }
            run(s, &format!("CREATE ROLE IF NOT EXISTS {}", q(&name))).await?;
            if !privs.is_empty() {
                let on = if any {
                    "*.*".to_owned()
                } else {
                    format!("{}.*", qi(db))
                };
                // GRANT OPTION is granted with WITH GRANT OPTION.
                let (list, with) = match privs {
                    "GRANT OPTION" => ("USAGE", " WITH GRANT OPTION"),
                    "CREATE USER, GRANT OPTION" => ("CREATE USER", " WITH GRANT OPTION"),
                    p if role == "root" => (p, " WITH GRANT OPTION"),
                    p => (p, ""),
                };
                run(s, &format!("GRANT {list} ON {on} TO {}{with}", q(&name))).await?;
            }
        }
        None => {
            // A custom role must exist (createRole).
            let probe = format!("SHOW GRANTS FOR {}", q(&name));
            if s.query(&probe).await.is_err() {
                return Err(MongoError::new(
                    ROLE_NOT_FOUND,
                    format!("Could not find role: {role}@{db}"),
                ));
            }
        }
    }
    Ok(name)
}

async fn grant_roles(
    s: &mut SqlSession,
    to: &str,
    roles: &[(String, String)],
) -> Result<(), MongoError> {
    for (r, d) in roles {
        let name = ensure_role(s, r, d).await?;
        run(s, &format!("GRANT {} TO {to}", q(&name))).await?;
    }
    Ok(())
}

/// What the read commands need from the connection.
struct Who {
    tenants: std::sync::Arc<crate::tenant::TenantServices>,
    tenant: TenantId,
}

fn who(c: &Conn) -> Result<Who, MongoError> {
    let (Some(t), Some(p)) = (c.shared.d.tenants.as_ref(), c.principal.as_ref()) else {
        return Err(MongoError::new(
            code::COMMAND_NOT_SUPPORTED,
            "user commands need the tenant catalog (catalog logins)",
        ));
    };
    Ok(Who {
        tenants: std::sync::Arc::clone(t),
        tenant: p
            .tenant
            .ok_or_else(|| MongoError::new(code::UNAUTHORIZED, "no tenant"))?,
    })
}

/// The tenant's auth state now.
async fn auth_state(w: &Who) -> Result<std::sync::Arc<sprig_catalog::TenantCatalog>, MongoError> {
    w.tenants
        .cache
        .get(w.tenant, Freshness::Latest)
        .await
        .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.message))
}

/// MongoDB 9.0's `usersInfo.authenticatedMechanism` picks the
/// authorization backend to ask; SprigDB has one, so a known mechanism
/// changes nothing (M4, decision S116).
fn check_mechanism(body: &Document) -> Result<(), MongoError> {
    match body.get("authenticatedMechanism") {
        None | Some(Bson::Null) => Ok(()),
        Some(Bson::String(m))
            if matches!(
                m.as_str(),
                "SCRAM-SHA-256"
                    | "SCRAM-SHA-1"
                    | "PLAIN"
                    | "MONGODB-X509"
                    | "MONGODB-OIDC"
                    | "GSSAPI"
                    | "MONGODB-AWS"
            ) =>
        {
            Ok(())
        }
        Some(Bson::String(m)) => Err(MongoError::new(
            code::BAD_VALUE,
            format!("Unknown authentication mechanism '{m}'"),
        )),
        Some(v) => Err(MongoError::new(
            code::TYPE_MISMATCH,
            format!(
                "BSON field 'usersInfo.authenticatedMechanism' is the wrong type '{}', expected type 'string'",
                sprig_doc::update::type_name(v)
            ),
        )),
    }
}

/// `usersInfo` entries: every user, or those named.
async fn users_info(w: &Who, body: &Document, db: &str) -> Result<Document, MongoError> {
    let cat = auth_state(w).await?;
    let auth = sprig_auth::cache::TenantAuth::of(&cat);
    let authz = AuthzState::of(&cat);
    let wanted: Option<Vec<String>> = match body.get("usersInfo") {
        Some(Bson::String(s)) => Some(vec![s.clone()]),
        Some(Bson::Document(d)) => d.get_str("user").ok().map(|u| vec![u.to_owned()]),
        Some(Bson::Array(a)) => Some(
            a.iter()
                .filter_map(|x| match x {
                    Bson::String(s) => Some(s.clone()),
                    Bson::Document(d) => d.get_str("user").ok().map(str::to_owned),
                    _ => None,
                })
                .collect(),
        ),
        _ => None,
    };
    let show_privs = body.get_bool("showPrivileges").unwrap_or(false);
    check_mechanism(body)?;
    let mut out = Vec::new();
    if let Some(a) = auth {
        let mut users: Vec<_> = a.users().filter(|u| u.row.host == "%").collect();
        users.sort_by(|x, y| x.row.name.cmp(&y.row.name));
        for u in users {
            if let Some(w) = &wanted
                && !w.contains(&u.row.name)
            {
                continue;
            }
            let roles: Vec<Bson> = authz
                .map(|z| {
                    z.granted_roles(u.row.user_id)
                        .into_iter()
                        .filter_map(|r| z.role(r))
                        .map(|r| match mongo_role(&r.name) {
                            Some((role, d)) => Bson::Document(doc! {"role": role, "db": d}),
                            None => Bson::Document(doc! {"role": r.name.clone(), "db": "admin"}),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let mut d = doc! {
                "_id": format!("admin.{}", u.row.name),
                "userId": bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: {
                    let mut b = [0u8; 16];
                    let id = u.row.user_id.0.to_be_bytes();
                    b[16 - id.len()..].copy_from_slice(&id);
                    b.to_vec()
                }},
                "user": u.row.name.clone(),
                "db": "admin",
                "roles": roles.clone(),
                "mechanisms": ["SCRAM-SHA-256"],
            };
            if show_privs {
                d.insert("inheritedRoles", roles);
                let privs: Vec<Bson> = authz
                    .map(|z| {
                        z.closure(&z.granted_roles(u.row.user_id))
                            .into_iter()
                            .flat_map(|r| {
                                z.privileges_of(Grantee::Role(r))
                                    .cloned()
                                    .collect::<Vec<_>>()
                            })
                            .chain(z.privileges_of(Grantee::User(u.row.user_id)).cloned())
                            .map(|p| Bson::String(format!("{:?} {:?}", p.target, p.actions)))
                            .collect()
                    })
                    .unwrap_or_default();
                d.insert("sprigPrivileges", privs);
            }
            out.push(Bson::Document(d));
        }
    }
    let _ = db;
    Ok(doc! {"users": out, "ok": 1.0})
}

/// `rolesInfo`: MongoDB roles of `db` (custom ones; `showBuiltinRoles`
/// adds the built-in ones).
async fn roles_info(w: &Who, body: &Document, db: &str) -> Result<Document, MongoError> {
    let cat = auth_state(w).await?;
    let authz = AuthzState::of(&cat);
    let wanted: Option<Vec<(String, String)>> = match body.get("rolesInfo") {
        Some(Bson::String(s)) => Some(vec![(s.clone(), db.to_owned())]),
        Some(Bson::Document(d)) => d
            .get_str("role")
            .ok()
            .map(|r| vec![(r.to_owned(), d.get_str("db").unwrap_or(db).to_owned())]),
        Some(Bson::Array(_)) => Some(roles_of(body.get("rolesInfo"), db)?),
        _ => None,
    };
    let mut out = Vec::new();
    let mut seen = Vec::new();
    if let Some(z) = authz {
        for r in &z.rows().roles {
            let Some((role, d)) = mongo_role(&r.name) else {
                continue;
            };
            let keep = match &wanted {
                Some(w) => w.contains(&(role.clone(), d.clone())),
                None => d == db && builtin(&role).is_none(),
            };
            if keep {
                seen.push((role.clone(), d.clone()));
                out.push(Bson::Document(doc! {
                    "_id": format!("{d}.{role}"), "role": role.clone(), "db": d.clone(),
                    "isBuiltin": builtin(&role).is_some(), "roles": [], "inheritedRoles": [],
                }));
            }
        }
    }
    // Built-in roles exist on every database whether or not used yet.
    let builtins = body.get_bool("showBuiltinRoles").unwrap_or(false);
    let names: Vec<(String, String)> = match &wanted {
        Some(w) => w.clone(),
        None if builtins => [
            "read",
            "readWrite",
            "dbAdmin",
            "userAdmin",
            "dbOwner",
            "enableSharding",
        ]
        .iter()
        .map(|r| ((*r).to_owned(), db.to_owned()))
        .collect(),
        None => Vec::new(),
    };
    for (role, d) in names {
        if seen.contains(&(role.clone(), d.clone())) {
            continue;
        }
        if let Some((_, any)) = builtin(&role)
            && (!any || d == "admin")
        {
            out.push(Bson::Document(doc! {
                "_id": format!("{d}.{role}"), "role": role, "db": d,
                "isBuiltin": true, "roles": [], "inheritedRoles": [],
            }));
        }
    }
    Ok(doc! {"roles": out, "ok": 1.0})
}

/// Whether `name` is a user or role command this module answers.
#[must_use]
pub fn handles(name: &str) -> bool {
    matches!(
        name,
        "createUser"
            | "updateUser"
            | "dropUser"
            | "usersInfo"
            | "grantRolesToUser"
            | "revokeRolesFromUser"
            | "createRole"
            | "dropRole"
            | "rolesInfo"
            | "grantRolesToRole"
            | "revokeRolesFromRole"
    )
}

/// Runs a user or role command.
///
/// # Errors
/// 11 `UserNotFound`, 31 `RoleNotFound`, 51003 for an existing user,
/// 51002 for an existing role, 13 without the privilege.
pub async fn command(
    c: &mut Conn,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    match name {
        "usersInfo" => return users_info(&who(c)?, body, db).await,
        "rolesInfo" => return roles_info(&who(c)?, body, db).await,
        _ => {}
    }
    let target = body
        .get_str(name)
        .map_err(|_| bad(format!("{name} needs a name")))?
        .to_owned();
    let roles = roles_of(body.get("roles"), db)?;
    let mut s = c
        .sql
        .take()
        .ok_or_else(|| MongoError::new(code::INTERNAL_ERROR, "no session"))?;
    let r = account_command(&mut s, name, body, db, &target, &roles).await;
    c.sql = Some(s);
    r?;
    Ok(doc! {"ok": 1.0})
}

#[allow(clippy::too_many_lines)]
async fn account_command(
    s: &mut SqlSession,
    name: &str,
    body: &Document,
    db: &str,
    target: &str,
    roles: &[(String, String)],
) -> Result<(), MongoError> {
    let acc = account(target);
    match name {
        "createUser" => {
            let pwd = body
                .get_str("pwd")
                .map_err(|_| bad("createUser needs 'pwd' (external users are not supported)"))?;
            if let Err(e) = run(s, &format!("CREATE USER {acc} IDENTIFIED BY {}", q(pwd))).await {
                return Err(if e.code == code::BAD_VALUE {
                    MongoError::new(
                        DUPLICATE_ACCOUNT,
                        format!("User \"{target}@{db}\" already exists"),
                    )
                } else {
                    e
                });
            }
            grant_roles(s, &acc, roles).await?;
            if !roles.is_empty() {
                run(s, &format!("SET DEFAULT ROLE ALL TO {acc}")).await?;
            }
        }
        "updateUser" => {
            if let Ok(pwd) = body.get_str("pwd") {
                run(s, &format!("ALTER USER {acc} IDENTIFIED BY {}", q(pwd)))
                    .await
                    .map_err(|e| not_found_user(e, target, db))?;
            }
            if body.contains_key("roles") {
                revoke_all(s, &acc)
                    .await
                    .map_err(|e| not_found_user(e, target, db))?;
                grant_roles(s, &acc, roles).await?;
                run(s, &format!("SET DEFAULT ROLE ALL TO {acc}")).await?;
            }
        }
        "dropUser" => run(s, &format!("DROP USER {acc}"))
            .await
            .map_err(|e| not_found_user(e, target, db))?,
        "grantRolesToUser" => {
            grant_roles(s, &acc, roles)
                .await
                .map_err(|e| not_found_user(e, target, db))?;
            run(s, &format!("SET DEFAULT ROLE ALL TO {acc}")).await?;
        }
        "revokeRolesFromUser" => {
            for (r, d) in roles {
                run(s, &format!("REVOKE {} FROM {acc}", q(&sql_role(r, d))))
                    .await
                    .map_err(|e| not_found_user(e, target, db))?;
            }
        }
        "createRole" => {
            let me = q(&sql_role(target, db));
            if builtin(target).is_some() {
                return Err(MongoError::new(
                    DUPLICATE_ROLE,
                    format!("Role \"{target}@{db}\" already exists"),
                ));
            }
            if run(s, &format!("CREATE ROLE {me}")).await.is_err() {
                return Err(MongoError::new(
                    DUPLICATE_ROLE,
                    format!("Role \"{target}@{db}\" already exists"),
                ));
            }
            for p in body
                .get_array("privileges")
                .map(Vec::as_slice)
                .unwrap_or(&[])
            {
                let Some(p) = p.as_document() else { continue };
                let res = p
                    .get_document("resource")
                    .map_err(|_| bad("a privilege needs 'resource'"))?;
                let on = match (res.get_str("db"), res.get_str("collection")) {
                    (Ok("") | Err(_), _) => "*.*".to_owned(),
                    (Ok(d), Ok("") | Err(_)) => format!("{}.*", qi(d)),
                    (Ok(d), Ok(c)) => format!("{}.{}", qi(d), qi(&sprig_doc::meta::table_name(c))),
                };
                let privs =
                    sql_privileges(p.get_array("actions").map(Vec::as_slice).unwrap_or(&[]));
                if !privs.is_empty() {
                    run(s, &format!("GRANT {} ON {on} TO {me}", privs.join(", "))).await?;
                }
            }
            grant_roles(s, &me, roles).await?;
        }
        "dropRole" => run(s, &format!("DROP ROLE {}", q(&sql_role(target, db))))
            .await
            .map_err(|_| {
                MongoError::new(ROLE_NOT_FOUND, format!("Role {target}@{db} not found"))
            })?,
        "grantRolesToRole" => grant_roles(s, &q(&sql_role(target, db)), roles).await?,
        "revokeRolesFromRole" => {
            for (r, d) in roles {
                run(
                    s,
                    &format!(
                        "REVOKE {} FROM {}",
                        q(&sql_role(r, d)),
                        q(&sql_role(target, db))
                    ),
                )
                .await?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn not_found_user(e: MongoError, user: &str, db: &str) -> MongoError {
    if e.code == code::BAD_VALUE {
        MongoError::new(
            USER_NOT_FOUND,
            format!("Could not find user \"{user}\" for db \"{db}\""),
        )
    } else {
        e
    }
}

/// Revokes every MongoDB role `acc` holds.
async fn revoke_all(s: &mut SqlSession, acc: &str) -> Result<(), MongoError> {
    let rows = s
        .query(&format!("SHOW GRANTS FOR {acc}"))
        .await
        .map_err(|e| {
            if e.errno == Some(1141) {
                MongoError::new(code::BAD_VALUE, e.message.clone())
            } else {
                sprig_doc::engine::mongo_error(&e)
            }
        })?;
    for r in rows {
        let line = r
            .first()
            .and_then(|c| c.as_ref())
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default();
        // GRANT `mongo.shop.read`@`%`,`x`@`%` TO `alice`@`%`
        if !line.starts_with("GRANT `") {
            continue;
        }
        let list = line
            .trim_start_matches("GRANT ")
            .split(" TO ")
            .next()
            .unwrap_or_default();
        for part in list.split(',') {
            let n = part.trim().trim_start_matches('`');
            let n = n.split('`').next().unwrap_or_default();
            if n.starts_with(PREFIX) {
                run(s, &format!("REVOKE {} FROM {acc}", q(n))).await?;
            }
        }
    }
    Ok(())
}

/// SQL privileges for MongoDB actions (chapter 29's table).
fn sql_privileges(actions: &[Bson]) -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    for a in actions.iter().filter_map(Bson::as_str) {
        let p: &[&'static str] = match a {
            "find" | "listCollections" | "listIndexes" => &["SELECT"],
            // Chapter 29: `changeStream` is `WATCH`; a stream needs `find`
            // too, as in MongoDB.
            "changeStream" => &["WATCH"],
            "insert" => &["INSERT"],
            "update" => &["UPDATE"],
            "remove" => &["DELETE"],
            "createCollection" => &["CREATE"],
            "dropCollection" | "dropDatabase" => &["DROP"],
            "collMod" => &["ALTER"],
            "createIndex" | "dropIndex" => &["INDEX"],
            "createUser" | "dropUser" | "createRole" | "dropRole" => &["CREATE USER"],
            "listDatabases" | "inprog" => &["PROCESS"],
            _ => &[],
        };
        for x in p {
            if !out.contains(x) {
                out.push(x);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(sql_role("readWrite", "shop"), "mongo.shop.readWrite");
        assert_eq!(
            mongo_role("mongo.shop.readWrite"),
            Some(("readWrite".into(), "shop".into()))
        );
        assert_eq!(mongo_role("app_role"), None);
        assert_eq!(builtin("readAnyDatabase"), Some(("SELECT, WATCH", true)));
        assert!(builtin("nope").is_none());
        assert_eq!(
            roles_of(
                Some(&bson::bson!(["read", {"role": "dbOwner", "db": "x"}])),
                "shop"
            )
            .unwrap(),
            vec![
                ("read".into(), "shop".into()),
                ("dbOwner".into(), "x".into())
            ]
        );
        assert_eq!(
            sql_privileges(&[Bson::from("find"), Bson::from("insert"), Bson::from("find")]),
            vec!["SELECT", "INSERT"]
        );
        // Chapter 29: changeStream is WATCH (a stream needs find too).
        assert_eq!(
            sql_privileges(&[Bson::from("find"), Bson::from("changeStream")]),
            vec!["SELECT", "WATCH"]
        );
        assert_eq!(builtin("read"), Some(("SELECT, WATCH", false)));
        assert_eq!(account("o'k"), "'o''k'@'%'");
    }
}
