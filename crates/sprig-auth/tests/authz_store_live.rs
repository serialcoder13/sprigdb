//! Every statement of the authorization catalog store
//! (`sprig_auth::authz::store::AuthzStore`) against a real MariaDB: a grant
//! passing 11 values for 10 placeholders broke every `GRANT` for an hour and no
//! unit test saw it, because only MariaDB checks placeholders against values
//! and columns against the table.
//!
//! ```sh
//! SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301 \
//!   cargo test -p sprig-auth --test authz_store_live -- --nocapture
//! ```
//!
//! The test migrates the `auth` and `authz` catalog modules
//! into its own scratch schema `__sprig_test_authz_store_<pid>_<n>` and
//! drops it afterwards, also when it fails. Without the variable it is
//! skipped with a message.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::panic::AssertUnwindSafe;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Pool};
use sprig_auth::authz::privilege::{Action, ActionSet};
use sprig_auth::authz::store::{AUTHZ_MODULE, AuthzRows, AuthzStore, EdgeRow, Grantee, PrivTarget};
use sprig_auth::store::AUTH_MODULE;
use sprig_catalog::Migrator;
use sprig_catalog::migrate::MigrateOpts;
use sprig_common::ids::{DbId, ObjectId, RoleId, UserId};
use sprig_common::names::quote;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

/// The targets and actions `who` holds, in `rows`' order.
fn held(rows: &AuthzRows, who: Grantee) -> Vec<(PrivTarget, ActionSet)> {
    rows.privileges
        .iter()
        .filter(|p| p.grantee == who)
        .map(|p| (p.target.clone(), p.actions))
        .collect()
}

async fn count(c: &mut Conn, schema: &str, table: &str) -> u64 {
    c.query_first(format!("SELECT COUNT(*) FROM {}.{table}", quote(schema)))
        .await
        .unwrap()
        .unwrap()
}

async fn run(c: &mut Conn, schema: &str) {
    let s = AuthzStore::new(schema);
    assert!(s.migrated(c).await.unwrap(), "the authz module's tables");
    let alice = Grantee::User(UserId(7));
    let by = Some(UserId(1));
    let sel = ActionSet::of(&[Action::Select]);
    let ins = ActionSet::of(&[Action::Insert]);
    let si = sel.union(ins);
    let (db, obj) = (DbId(3), ObjectId(41));
    let targets = [
        PrivTarget::Tenant,
        PrivTarget::Database("shop".into()),
        PrivTarget::Object { db, object: obj },
        PrivTarget::Column {
            db,
            object: obj,
            column: "price".into(),
        },
        PrivTarget::Restriction("hr".into()),
        PrivTarget::Named {
            db_key: "shop".into(),
            name_key: "gone".into(),
            column: None,
        },
        PrivTarget::Named {
            db_key: "shop".into(),
            name_key: "gone".into(),
            column: Some("c1".into()),
        },
        PrivTarget::GraphElement {
            db,
            graph: ObjectId(42),
            rels: false,
            name: "Person".into(),
        },
        PrivTarget::GraphElement {
            db,
            graph: ObjectId(42),
            rels: true,
            name: "KNOWS".into(),
        },
    ];
    // grant: one INSERT per target, then again (ON DUPLICATE KEY UPDATE
    // adds actions).
    for t in &targets {
        s.grant(c, alice, t, sel, by).await.unwrap();
    }
    for t in &targets {
        s.grant(c, alice, t, ins, None).await.unwrap();
    }
    let rows = s.load(c).await.unwrap();
    assert_eq!(
        held(&rows, alice),
        targets.iter().map(|t| (t.clone(), si)).collect::<Vec<_>>(),
        "every target loads back as granted"
    );
    // revoke: a part (UPDATE) and the rest (DELETE); a missing row.
    for t in &targets {
        assert_eq!(s.revoke(c, alice, t, ins).await.unwrap(), si, "{t:?}");
    }
    assert!(
        held(&s.load(c).await.unwrap(), alice)
            .iter()
            .all(|(_, a)| *a == sel)
    );
    for t in &targets {
        assert_eq!(s.revoke(c, alice, t, sel).await.unwrap(), sel, "{t:?}");
        assert_eq!(s.revoke(c, alice, t, sel).await.unwrap(), ActionSet::EMPTY);
    }
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    // Roles: create (and again: 1062 is None), find, edges, defaults.
    let r1 = s.create_role(c, "reader", "%", by).await.unwrap().unwrap();
    let r2 = s
        .create_role(c, "writer", "%", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.create_role(c, "reader", "%", by).await.unwrap(), None);
    assert_eq!(s.role(c, "reader", "%").await.unwrap(), Some(r1));
    assert_eq!(s.role(c, "nobody", "%").await.unwrap(), None);
    s.grant_role(c, alice, r1, false, by).await.unwrap();
    s.grant_role(c, alice, r1, true, by).await.unwrap();
    s.grant_role(c, alice, r2, false, None).await.unwrap();
    s.grant_role(c, Grantee::Role(r2), r1, false, by)
        .await
        .unwrap();
    s.set_default_roles(c, UserId(7), &[r2]).await.unwrap();
    let rows = s.load(c).await.unwrap();
    assert_eq!(rows.roles.len(), 2);
    let edge = |g: Grantee, r: RoleId| {
        rows.edges
            .iter()
            .find(|e| e.grantee == g && e.role == r)
            .copied()
    };
    assert_eq!(
        edge(alice, r1),
        Some(EdgeRow {
            grantee: alice,
            role: r1,
            admin_option: true,
            is_default: false
        })
    );
    assert!(edge(alice, r2).is_some_and(|e| e.is_default && !e.admin_option));
    assert!(edge(Grantee::Role(r2), r1).is_some());
    assert!(s.revoke_role(c, alice, r2).await.unwrap());
    assert!(!s.revoke_role(c, alice, r2).await.unwrap());
    // forget_privileges and forget: a role's grants and the edges to it.
    let role2 = Grantee::Role(r2);
    s.grant(c, role2, &PrivTarget::Tenant, sel, by)
        .await
        .unwrap();
    s.forget_privileges(c, role2).await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), role2).is_empty());
    s.grant(c, role2, &PrivTarget::Tenant, sel, by)
        .await
        .unwrap();
    s.forget(c, Grantee::Role(r1)).await.unwrap();
    s.forget(c, role2).await.unwrap();
    s.delete_role(c, r1).await.unwrap();
    s.delete_role(c, r2).await.unwrap();
    let rows = s.load(c).await.unwrap();
    assert!(rows.roles.is_empty() && rows.edges.is_empty(), "{rows:?}");
    // keep_by_name: an object's grants become named ones; forget_named.
    let col = PrivTarget::Column {
        db,
        object: obj,
        column: "price".into(),
    };
    let tbl = PrivTarget::Object { db, object: obj };
    s.grant(c, alice, &tbl, si, by).await.unwrap();
    s.grant(c, alice, &col, sel, by).await.unwrap();
    assert!(s.keep_by_name(c, db, obj, "shop", "items").await.unwrap());
    assert!(!s.keep_by_name(c, db, obj, "shop", "items").await.unwrap());
    let named = |column: Option<&str>| PrivTarget::Named {
        db_key: "shop".into(),
        name_key: "items".into(),
        column: column.map(str::to_owned),
    };
    let mut got = held(&s.load(c).await.unwrap(), alice);
    got.sort();
    let mut want = vec![(named(None), si), (named(Some("price")), sel)];
    want.sort();
    assert_eq!(got, want);
    s.forget_named(c, "shop", Some("items")).await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    s.grant(c, alice, &named(None), sel, by).await.unwrap();
    s.forget_named(c, "shop", None).await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    // forget_object and forget_database.
    s.grant(c, alice, &tbl, sel, by).await.unwrap();
    s.forget_object(c, db, obj).await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    s.grant(c, alice, &PrivTarget::Database("shop".into()), sel, by)
        .await
        .unwrap();
    s.grant(c, alice, &col, sel, by).await.unwrap();
    s.forget_database(c, db, "shop").await.unwrap();
    assert!(held(&s.load(c).await.unwrap(), alice).is_empty());
    assert_eq!(count(c, schema, "privileges").await, 0);
    assert_eq!(count(c, schema, "role_edges").await, 0);
    assert_eq!(count(c, schema, "roles").await, 0);
}

#[tokio::test]
async fn every_authz_store_statement_runs_on_mariadb() {
    let Some(url) = std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped: set {MARIADB_ENV} to run it");
        return;
    };
    let pool = Pool::new(Opts::from_url(&url).unwrap());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let schema = format!("__sprig_test_authz_store_{}_{nanos:x}", std::process::id());
    let mut c = pool.get_conn().await.unwrap();
    let r = AssertUnwindSafe(async {
        Migrator::new(&[AUTH_MODULE, AUTHZ_MODULE])
            .unwrap()
            .run(&mut c, &schema, &MigrateOpts::default())
            .await
            .unwrap();
        run(&mut c, &schema).await;
    })
    .catch_unwind()
    .await;
    // A fresh connection: a failed case may have broken `c`.
    drop(c);
    let mut d = Conn::new(Opts::from_url(&url).unwrap()).await.unwrap();
    d.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
        .await
        .unwrap();
    d.disconnect().await.unwrap();
    pool.disconnect().await.ok();
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
    eprintln!("every authz store statement ran on MariaDB ({schema}, dropped)");
}
