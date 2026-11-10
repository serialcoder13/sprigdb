//! MongoDB user and role commands (chapter 29, "MongoDB user and role
//! commands (R5)") through the Rust driver: users made over MongoDB log in
//! with SCRAM, and their roles decide what they may do.
//!
//! - `SPRIG_TEST_MONGO_URI`: the gateway, logged in as a tenant user who
//!   may manage users (as in `driver_live.rs`).
//! - `SPRIG_TEST_MONGO_REF_URI`: the reference MongoDB, for the
//!   differential part.
//!
//! The users `r5u_reader` and `r5u_diff` and the role `r5u_inserter` are
//! dropped at the end, as is the database `r5mongo2_users`.

#![allow(
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown
)]

use mongodb::bson::{Bson, Document, doc};
use mongodb::options::ClientOptions;
use mongodb::{Client, Database};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

async fn client(u: &str) -> Client {
    let mut o = ClientOptions::parse(u).await.unwrap();
    o.app_name = Some("r5mongo2-users~dc-a".into());
    Client::with_options(o).unwrap()
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) => w.code,
        mongodb::error::ErrorKind::Authentication { .. } => 18,
        _ => -1,
    }
}

/// The gateway URI with another login (`acme.<user>:<pwd>`).
fn as_user(u: &str, user: &str, pwd: &str) -> String {
    let (scheme, rest) = u.split_once("://").unwrap();
    let (cred, host) = rest.split_once('@').unwrap();
    let tenant = cred.split('.').next().unwrap();
    format!("{scheme}://{tenant}.{user}:{pwd}@{host}")
}

async fn can_find(u: &str) -> Result<(), i32> {
    client(u)
        .await
        .database("r5mongo2_users")
        .collection::<Document>("c")
        .find_one(doc! {})
        .await
        .map(|_| ())
        .map_err(|e| code_of(&e))
}

async fn can_insert(u: &str, id: i32) -> Result<(), i32> {
    client(u)
        .await
        .database("r5mongo2_users")
        .collection::<Document>("c")
        .insert_one(doc! {"_id": id})
        .await
        .map(|_| ())
        .map_err(|e| code_of(&e))
}

async fn cmd(db: &Database, c: Document) -> Result<Document, i32> {
    db.run_command(c).await.map_err(|e| code_of(&e))
}

#[tokio::test]
async fn users_and_roles() {
    let Some(u) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        return;
    };
    let admin_client = client(&u).await;
    let data = admin_client.database("r5mongo2_users");
    let admin = admin_client.database("admin");
    let _ = data.drop().await;
    let _ = cmd(&admin, doc! {"dropUser": "r5u_reader"}).await;
    let _ = cmd(&data, doc! {"dropRole": "r5u_inserter"}).await;
    data.collection::<Document>("c")
        .insert_one(doc! {"_id": 0})
        .await
        .unwrap();

    // createUser with a built-in role on one database.
    cmd(
        &admin,
        doc! {"createUser": "r5u_reader", "pwd": "pw-one", "roles": [{"role": "read", "db": "r5mongo2_users"}]},
    )
    .await
    .unwrap();
    assert_eq!(
        cmd(
            &admin,
            doc! {"createUser": "r5u_reader", "pwd": "x", "roles": []}
        )
        .await
        .unwrap_err(),
        51003
    );
    let info = cmd(&admin, doc! {"usersInfo": "r5u_reader"}).await.unwrap();
    let users = info.get_array("users").unwrap();
    assert_eq!(users.len(), 1, "{info}");
    let user = users[0].as_document().unwrap();
    assert_eq!(user.get_str("user").unwrap(), "r5u_reader");
    assert_eq!(
        user.get_array("roles").unwrap(),
        &vec![Bson::Document(
            doc! {"role": "read", "db": "r5mongo2_users"}
        )]
    );
    assert!(!user.contains_key("credentials"));

    // The new user logs in with SCRAM; read is allowed, write isn't.
    let reader = as_user(&u, "r5u_reader", "pw-one");
    can_find(&reader).await.unwrap();
    assert_eq!(can_insert(&reader, 1).await, Err(13));

    // grantRolesToUser readWrite: writes work; revoke: refused again.
    cmd(
        &admin,
        doc! {"grantRolesToUser": "r5u_reader", "roles": [{"role": "readWrite", "db": "r5mongo2_users"}]},
    )
    .await
    .unwrap();
    can_insert(&reader, 2).await.unwrap();
    cmd(
        &admin,
        doc! {"revokeRolesFromUser": "r5u_reader", "roles": [{"role": "readWrite", "db": "r5mongo2_users"}]},
    )
    .await
    .unwrap();
    assert_eq!(can_insert(&reader, 3).await, Err(13));

    // A custom role: insert and find on one collection.
    cmd(
        &data,
        doc! {"createRole": "r5u_inserter", "privileges": [{"resource": {"db": "r5mongo2_users", "collection": "c"}, "actions": ["insert", "find"]}], "roles": []},
    )
    .await
    .unwrap();
    let roles = cmd(&data, doc! {"rolesInfo": "r5u_inserter"})
        .await
        .unwrap();
    assert_eq!(roles.get_array("roles").unwrap().len(), 1, "{roles}");
    cmd(
        &admin,
        doc! {"grantRolesToUser": "r5u_reader", "roles": [{"role": "r5u_inserter", "db": "r5mongo2_users"}]},
    )
    .await
    .unwrap();
    can_insert(&reader, 4).await.unwrap();

    // updateUser: the new password works, the old one doesn't.
    cmd(&admin, doc! {"updateUser": "r5u_reader", "pwd": "pw-two"})
        .await
        .unwrap();
    assert_eq!(can_find(&reader).await, Err(18));
    can_find(&as_user(&u, "r5u_reader", "pw-two"))
        .await
        .unwrap();

    // Unknown names.
    assert_eq!(
        cmd(&admin, doc! {"dropUser": "r5u_nobody"})
            .await
            .unwrap_err(),
        11
    );
    assert_eq!(
        cmd(
            &admin,
            doc! {"grantRolesToUser": "r5u_reader", "roles": [{"role": "nope", "db": "x"}]}
        )
        .await
        .unwrap_err(),
        31
    );

    // dropUser: the login is refused.
    cmd(&admin, doc! {"dropUser": "r5u_reader"}).await.unwrap();
    assert_eq!(
        can_find(&as_user(&u, "r5u_reader", "pw-two")).await,
        Err(18)
    );
    cmd(&data, doc! {"dropRole": "r5u_inserter"}).await.unwrap();
    data.drop().await.unwrap();
}

/// The replies of user commands, compared with MongoDB 8.0.
fn sorted_roles(u: &Document) -> Vec<Bson> {
    let mut r: Vec<Bson> = u.get_array("roles").cloned().unwrap_or_default();
    r.sort_by_key(ToString::to_string);
    r
}

async fn user_corpus(admin: &Database) -> Vec<(String, Bson)> {
    let mut out = Vec::new();
    let view = |r: Result<Document, i32>| -> Bson {
        match r {
            Err(c) => Bson::String(format!("error {c}")),
            Ok(d) => {
                let users: Vec<Bson> = d
                    .get_array("users")
                    .map(|a| {
                        a.iter()
                            .filter_map(Bson::as_document)
                            .map(|u| {
                                Bson::Document(doc! {
                                    "user": u.get("user").cloned().unwrap_or(Bson::Null),
                                    "db": u.get("db").cloned().unwrap_or(Bson::Null),
                                    // A role set: MongoDB keeps it in hash order
                                    // after a grant, so compare it sorted.
                                    "roles": sorted_roles(u),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Bson::Document(
                    doc! {"ok": d.get("ok").cloned().unwrap_or(Bson::Null), "users": users},
                )
            }
        }
    };
    let _ = cmd(admin, doc! {"dropUser": "r5u_diff"}).await;
    let steps: Vec<(&str, Document)> = vec![
        (
            "createUser",
            doc! {"createUser": "r5u_diff", "pwd": "pw", "roles": [{"role": "readWrite", "db": "r5mongo2_users"}]},
        ),
        (
            "createUser again",
            doc! {"createUser": "r5u_diff", "pwd": "pw", "roles": []},
        ),
        ("usersInfo", doc! {"usersInfo": "r5u_diff"}),
        (
            "grantRolesToUser",
            doc! {"grantRolesToUser": "r5u_diff", "roles": [{"role": "read", "db": "other"}]},
        ),
        (
            "usersInfo after grant",
            doc! {"usersInfo": {"user": "r5u_diff", "db": "admin"}},
        ),
        (
            "revokeRolesFromUser",
            doc! {"revokeRolesFromUser": "r5u_diff", "roles": [{"role": "readWrite", "db": "r5mongo2_users"}]},
        ),
        ("usersInfo after revoke", doc! {"usersInfo": "r5u_diff"}),
        ("dropUser", doc! {"dropUser": "r5u_diff"}),
        ("dropUser again", doc! {"dropUser": "r5u_diff"}),
        ("usersInfo after drop", doc! {"usersInfo": "r5u_diff"}),
    ];
    for (name, c) in steps {
        out.push((name.to_owned(), view(cmd(admin, c).await)));
    }
    out
}

#[tokio::test]
async fn user_commands_against_mongodb() {
    let (Some(g), Some(r)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MONGO_REF_URI")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return;
    };
    let a = user_corpus(&client(&g).await.database("admin")).await;
    let b = user_corpus(&client(&r).await.database("admin")).await;
    let mut fails = Vec::new();
    for ((name, x), (_, y)) in a.iter().zip(&b) {
        if x != y {
            fails.push(format!("{name}\n  sprig: {x}\n  mongo: {y}"));
        }
    }
    eprintln!(
        "user commands: {} of {} steps equal",
        a.len() - fails.len(),
        a.len()
    );
    assert!(fails.is_empty(), "{}", fails.join("\n"));
}
