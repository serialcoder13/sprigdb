//! bugfix-26 (decisions/bugfix-interim.md): `SET PASSWORD FOR` another
//! account follows MySQL 8.4's and MariaDB's rule, on both personas.
//!
//! Both servers ask for `UPDATE` on the `mysql` schema
//! (`check_change_password`), which a tenant-wide `UPDATE` gives here
//! (`GRANT UPDATE ON *.*`, `ALL ON *.*`), and refuse others with 1044 on
//! database `mysql`; an unknown account is 1133. Before, every `SET
//! PASSWORD FOR` another account was refused with 1227 "you need the
//! CREATE USER privilege", even for a user holding `ALL ON *.*`.
//!
//! ```sh
//! SPRIG_TEST_SET_PASSWORD=<mysql persona port>,<mariadb persona port> \
//! SPRIG_TEST_SET_PASSWORD_LOGIN=acme.app:sprig-dev-app \
//!   cargo test -p sprig-gateway --test set_password_live -- --nocapture
//! ```
//!
//! The login must be able to create users; the test drops the users it
//! makes (`bf7sp_*`).

#![allow(clippy::doc_markdown, clippy::unwrap_used, missing_docs)]

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

fn opts(port: u16, user: &str, pw: &str) -> OptsBuilder {
    OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(pw))
        .ssl_opts(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        )
}

fn code(r: &Result<(), mysql_async::Error>) -> u16 {
    match r {
        Ok(()) => 0,
        Err(mysql_async::Error::Server(s)) => s.code,
        Err(_) => u16::MAX,
    }
}

#[tokio::test]
async fn set_password_for_another_account_follows_mysql() {
    let Ok(v) = std::env::var("SPRIG_TEST_SET_PASSWORD") else {
        eprintln!("skipped: set SPRIG_TEST_SET_PASSWORD=<mysql port>,<mariadb port>");
        return;
    };
    let ports: Vec<u16> = v.split(',').filter_map(|p| p.trim().parse().ok()).collect();
    let login = std::env::var("SPRIG_TEST_SET_PASSWORD_LOGIN")
        .unwrap_or_else(|_| "acme.app:sprig-dev-app".to_owned());
    let (admin, admin_pw) = login.split_once(':').unwrap();
    let tenant = admin.split_once('.').unwrap().0.to_owned();
    let mut failed = Vec::new();
    for (i, &port) in ports.iter().enumerate() {
        let persona = if i == 0 { "MySQL" } else { "MariaDB" };
        let mut a = Conn::new(opts(port, admin, admin_pw)).await.unwrap();
        // (user, its grant, SET PASSWORD FOR the target: wanted code)
        let cases = [
            ("bf7sp_all", Some("ALL ON *.*"), 0u16),
            ("bf7sp_upd", Some("UPDATE ON *.*"), 0),
            ("bf7sp_cu", Some("CREATE USER ON *.*"), 1044),
            ("bf7sp_sel", Some("SELECT, INSERT, DELETE ON *.*"), 1044),
            ("bf7sp_none", None, 1044),
        ];
        for q in [
            "DROP USER IF EXISTS bf7sp_all, bf7sp_upd, bf7sp_cu, bf7sp_sel, bf7sp_none, bf7sp_tgt",
            "CREATE USER bf7sp_tgt IDENTIFIED BY 'Bf7-tgt-pw0'",
        ] {
            a.query_drop(q).await.unwrap();
        }
        for (u, g, _) in &cases {
            a.query_drop(format!("CREATE USER {u} IDENTIFIED BY 'Bf7-sp-pw1'"))
                .await
                .unwrap();
            if let Some(g) = g {
                a.query_drop(format!("GRANT {g} TO {u}")).await.unwrap();
            }
        }
        for (n, (u, g, want)) in cases.iter().enumerate() {
            let mut c = Conn::new(opts(port, &format!("{tenant}.{u}"), "Bf7-sp-pw1"))
                .await
                .unwrap();
            let new_pw = format!("Bf7-tgt-pw{}{n}", i + 1);
            let r = c
                .query_drop(format!("SET PASSWORD FOR 'bf7sp_tgt'@'%' = '{new_pw}'"))
                .await;
            eprintln!("{persona} {u} ({g:?}): {r:?}");
            if code(&r) != *want {
                failed.push(format!("{persona} {u}: {r:?} (wanted {want})"));
            }
            if *want == 0 {
                // The target logs in with the new password.
                match Conn::new(opts(port, &format!("{tenant}.bf7sp_tgt"), &new_pw)).await {
                    Ok(t) => {
                        t.disconnect().await.ok();
                    }
                    Err(x) => failed.push(format!("{persona} after {u}: target login {x}")),
                }
                // Bare name, and an unknown account (1133).
                let r = c
                    .query_drop(format!("SET PASSWORD FOR bf7sp_tgt = '{new_pw}x'"))
                    .await;
                if code(&r) != 0 {
                    failed.push(format!("{persona} {u} bare name: {r:?}"));
                }
                let r = c
                    .query_drop("SET PASSWORD FOR bf7sp_nobody = 'Bf7-x-pw1'")
                    .await;
                if code(&r) != 1133 {
                    failed.push(format!(
                        "{persona} {u} unknown account: {r:?} (wanted 1133)"
                    ));
                }
            }
            // Its own password, by name, needs nothing.
            let r = c
                .query_drop(format!("SET PASSWORD FOR {u} = 'Bf7-sp-pw2'"))
                .await;
            if code(&r) != 0 {
                failed.push(format!("{persona} {u} own password by name: {r:?}"));
            }
            c.disconnect().await.ok();
        }
        a.query_drop(
            "DROP USER IF EXISTS bf7sp_all, bf7sp_upd, bf7sp_cu, bf7sp_sel, bf7sp_none, bf7sp_tgt",
        )
        .await
        .unwrap();
        a.disconnect().await.ok();
    }
    assert!(failed.is_empty(), "{failed:#?}");
}
