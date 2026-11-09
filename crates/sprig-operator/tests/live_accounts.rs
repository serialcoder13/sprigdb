//! The operator's system-account step against a real MariaDB started with
//! the rendered baseline (`SPRIG_OPERATOR_LIVE_ADDR=host:port`,
//! `SPRIG_OPERATOR_LIVE_ROOT=<root password>`); skipped otherwise.

#![allow(clippy::unwrap_used)]

use sprig_operator::accounts::{BootstrapTarget, MysqlAccounts, SystemAccounts};

#[tokio::test]
async fn accounts_and_the_gtid_domain_on_a_real_server() {
    let (Ok(addr), Ok(root)) = (
        std::env::var("SPRIG_OPERATOR_LIVE_ADDR"),
        std::env::var("SPRIG_OPERATOR_LIVE_ROOT"),
    ) else {
        eprintln!("skipped: SPRIG_OPERATOR_LIVE_ADDR is not set");
        return;
    };
    let acc = MysqlAccounts::new(None);
    let t = BootstrapTarget {
        cluster: "r2prov-live".into(),
        writer: addr.clone(),
        nodes: vec![addr.clone()],
        root_password: root,
        ctl_password: "ctl-live-r2prov".into(),
        repl_password: "repl-live-r2prov".into(),
    };
    acc.ensure(&t).await.unwrap();
    acc.ensure(&t).await.unwrap();
    // From the file's value (42; a rerun may find another).
    acc.ensure_domain(&t, 42).await.unwrap();
    assert_eq!(acc.ensure_domain(&t, 77).await.unwrap(), 1);
    assert_eq!(acc.ensure_domain(&t, 77).await.unwrap(), 0, "already set");
    assert_eq!(
        acc.ensure_domain(&t, 42).await.unwrap(),
        1,
        "back to the file's value"
    );
}
