//! `sprig_gw`, the gateways' monitoring account, on a live cluster:
//! created as `sprig_ctl` on the writer,
//! replicated to every node, able to read the replication status and
//! nothing else.
//!
//! ```sh
//! export SPRIG_TEST_GW_CREDENTIALS=file:$PWD/deploy/compose/.gateway-dev/cluster-credentials.toml
//! export SPRIG_TEST_GW_NODES=127.0.0.1:13311,127.0.0.1:13312,127.0.0.1:13313   # writer first
//! cargo test -p sprig-provision --test gw_account_live -- --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::Duration;

use sprig_cluster::record::NodeSpec;
use sprig_cluster::sql::ProbeSession;
use sprig_common::ids::NodeId;
use sprig_provision::credentials::CredentialConnector;
use sprig_provision::gw_account::{self, Ensured};

#[tokio::test]
async fn sprig_gw_is_created_and_can_only_monitor() {
    let (Ok(creds), Ok(nodes)) = (
        std::env::var("SPRIG_TEST_GW_CREDENTIALS"),
        std::env::var("SPRIG_TEST_GW_NODES"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_GW_CREDENTIALS and SPRIG_TEST_GW_NODES not set");
        return;
    };
    let refs = sprig_provision::credentials::refs_for(&creds).unwrap();
    let nodes: Vec<NodeSpec> = nodes
        .split(',')
        .enumerate()
        .map(|(i, a)| NodeSpec {
            node: NodeId::new(format!("n{}", i + 1)).unwrap(),
            addr: a.trim().to_owned(),
            zone: None,
        })
        .collect();
    let timeout = Duration::from_secs(10);
    let gw = gw_account::gw_credential(&refs.ctl).unwrap();
    let ctl =
        sprig_cluster::driver::NodeConnector::endpoint(&CredentialConnector::ctl(&refs), &nodes[0])
            .unwrap();
    let first = gw_account::ensure(&nodes[0], &ctl, &gw, timeout)
        .await
        .unwrap();
    println!("first ensure: {first:?}");
    // Idempotent: the second finds it and writes nothing.
    let second = gw_account::ensure(&nodes[0], &ctl, &gw, timeout)
        .await
        .unwrap();
    assert_eq!(second, Ensured::Present);
    // Every node: logs in, reads what the gateway's checks read, and may
    // not read or write data.
    tokio::time::sleep(Duration::from_secs(1)).await;
    for n in &nodes {
        let ep = gw.endpoint(n).unwrap();
        let mut s = ProbeSession::connect(&ep, timeout).await.unwrap();
        let row = s
            .rows(
                "SELECT @@read_only, @@gtid_slave_pos, @@gtid_binlog_pos, \
                 (SELECT COUNT(*) FROM information_schema.SLAVE_STATUS), \
                 @@global.max_allowed_packet",
            )
            .await
            .unwrap();
        println!("{}: {:?}", n.addr, row.first());
        let grants = s.rows("SHOW GRANTS").await.unwrap();
        println!("{}: grants {:?}", n.addr, grants);
        assert!(s.rows("SELECT COUNT(*) FROM mysql.user").await.is_err());
        assert!(s.exec("CREATE DATABASE t0_sprig_gw_probe").await.is_err());
        s.close().await;
    }
}
