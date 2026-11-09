//! `sprig_gw`, the gateways' monitoring account (chapter 09, "System
//! accounts"; chapter 06, "Health checks"; decision gwhard-3).
//!
//! Gateways check every node's health, role and replication position. As
//! a tenant's own account those checks can't read the replication status,
//! so a replica's role stays unknown and replicas get no reads. `sprig_gw`
//! holds exactly what the checks need: `SLAVE MONITOR` (the replication
//! status; `REPLICA MONITOR` is MariaDB's other name for it), nothing on
//! data. Variables such as `@@gtid_binlog_pos` and `@@max_allowed_packet`
//! need no privilege.
//!
//! **Its credential** ([`gw_credential`]) lives next to `sprig_ctl`'s: the
//! `[sprig_gw]` table of the cluster's credentials file when there is one
//! (the operator writes it), else a password derived from `sprig_ctl`'s
//! (HMAC-SHA256 of `sprig_gw` keyed by it, in hex) with `sprig_ctl`'s TLS
//! settings. Whoever can resolve `sprig_ctl` (controllers, workers,
//! gateways) can resolve `sprig_gw`, and no new secret has to be stored.
//!
//! **Creating it** ([`ensure`]): the controller, as `sprig_ctl`, when it
//! registers a cluster and once per cluster after it starts; the
//! Kubernetes operator as root. If `sprig_gw` already logs in and reads
//! the replication status nothing is written; otherwise `CREATE USER IF
//! NOT EXISTS`, `ALTER USER` (the password) and `GRANT SLAVE MONITOR`. On
//! primary + replicas and Galera the account replicates from the writer.

use std::time::Duration;

use sha2::{Digest, Sha256};
use sprig_cluster::record::NodeSpec;
use sprig_cluster::sql::{NodeEndpoint, ProbeSession, literal};
use sprig_common::{Result, SprigCode, SprigError};

use crate::credentials::{Credential, SecretRef, SecretSource};

/// The account's name.
pub const GW_USER: &str = "sprig_gw";

/// What the gateway's checks read; it needs `SLAVE MONITOR`.
const CHECK_SQL: &str = "SELECT COUNT(*) FROM information_schema.SLAVE_STATUS";

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const B: usize = 64;
    let mut k = [0u8; B];
    if key.len() > B {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; B];
    let mut opad = [0x5cu8; B];
    for i in 0..B {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let inner = Sha256::new().chain_update(ipad).chain_update(msg).finalize();
    Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize()
        .into()
}

/// `sprig_gw`'s password derived from `sprig_ctl`'s.
fn derived_password(ctl_password: &str) -> String {
    hmac_sha256(ctl_password.as_bytes(), GW_USER.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// `sprig_gw`'s credential for a cluster whose `sprig_ctl` reference is
/// `ctl_ref` (see the module notes).
///
/// # Errors
/// 39998 when `sprig_ctl`'s reference doesn't resolve, or the file's
/// `[sprig_gw]` table doesn't parse.
pub fn gw_credential(ctl_ref: &str) -> Result<Credential> {
    let ctl: SecretRef = ctl_ref.parse()?;
    if ctl.section.is_some() && !matches!(ctl.source, SecretSource::Resolved(_)) {
        let own = SecretRef {
            source: ctl.source.clone(),
            section: Some(GW_USER.to_owned()),
        };
        match own.resolve(GW_USER) {
            Ok(c) => return Ok(c),
            // No `[sprig_gw]` table: derive.
            Err(e) if e.message.contains("no [sprig_gw] table") => {}
            Err(e) => return Err(e),
        }
    }
    let c = ctl.resolve(crate::credentials::CTL_USER)?;
    let mut gw = Credential::new(GW_USER, derived_password(c.password()), c.tls);
    gw.ca_file = c.ca_file;
    Ok(gw)
}

/// The statements that create (or re-assert) `sprig_gw` with `password`.
#[must_use]
pub fn account_sql(password: &str) -> Vec<String> {
    let pw = literal(password);
    vec![
        format!("CREATE USER IF NOT EXISTS 'sprig_gw'@'%' IDENTIFIED BY {pw} REQUIRE SSL"),
        format!("ALTER USER 'sprig_gw'@'%' IDENTIFIED BY {pw} REQUIRE SSL"),
        "GRANT SLAVE MONITOR ON *.* TO 'sprig_gw'@'%'".to_owned(),
    ]
}

/// What [`ensure`] found or did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ensured {
    /// `sprig_gw` already logged in and read the replication status.
    Present,
    /// It was created or repaired.
    Created,
}

/// Makes sure `sprig_gw` logs in on `node` with `gw`'s password and can
/// read the replication status; creates or repairs it as `ctl` (an
/// endpoint on the same node) when not.
///
/// # Errors
/// 39511 `ER_SPRIG_CLUSTER_PRECHECK_FAILED` naming what `sprig_ctl`
/// couldn't do and what an operator runs instead (the password is never
/// in the message).
pub async fn ensure(
    node: &NodeSpec,
    ctl: &NodeEndpoint,
    gw: &Credential,
    timeout: Duration,
) -> Result<Ensured> {
    if check(node, gw, timeout).await.is_ok() {
        return Ok(Ensured::Present);
    }
    let mut s = ProbeSession::connect(ctl, timeout)
        .await
        .map_err(|e| failed(node, &format!("sprig_ctl can't log in: {e}")))?;
    let mut r = Ok(());
    for sql in account_sql(gw.password()) {
        if let Err(e) = s.exec(&sql).await {
            let verb = sql.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
            r = Err(failed(node, &format!("sprig_ctl can't run {verb} ({e})")));
            break;
        }
    }
    s.close().await;
    r?;
    check(node, gw, timeout)
        .await
        .map_err(|e| failed(node, &format!("sprig_gw was created but still fails: {e}")))?;
    Ok(Ensured::Created)
}

/// Logs in as `sprig_gw` and reads the replication status.
async fn check(node: &NodeSpec, gw: &Credential, timeout: Duration) -> Result<()> {
    let ep = gw.endpoint(node)?;
    let mut s = ProbeSession::connect(&ep, timeout).await?;
    let r = s.scalar(CHECK_SQL).await;
    s.close().await;
    r.map(|_| ()).map_err(SprigError::from)
}

fn failed(node: &NodeSpec, why: &str) -> SprigError {
    SprigError::new(
        SprigCode::ClusterPrecheckFailed,
        format!(
            "node {}: the monitoring account sprig_gw: {why}; until it exists, gateways \
             can't read replication and send no reads to replicas. Create it as an \
             administrator: CREATE USER 'sprig_gw'@'%' IDENTIFIED BY '<its password>' \
             REQUIRE SSL; GRANT SLAVE MONITOR ON *.* TO 'sprig_gw'@'%' (the password is \
             the [sprig_gw] table's in the cluster's credentials file)",
            node.node
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::TlsMode;

    #[test]
    fn hmac_matches_rfc_4231_case_2() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn the_credential_is_the_files_table_or_derived_from_sprig_ctl() {
        let dir = std::env::temp_dir().join(format!("sprig-gw-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.toml");
        std::fs::write(
            &path,
            "[sprig_ctl]\npassword = \"ctl-pw\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\npassword = \"r\"\n",
        )
        .unwrap();
        let ctl_ref = format!("file:{}#sprig_ctl", path.display());
        let derived = gw_credential(&ctl_ref).unwrap();
        assert_eq!(derived.user, "sprig_gw");
        assert_eq!(derived.tls, TlsMode::SkipVerify);
        assert_eq!(derived.password(), derived_password("ctl-pw"));
        assert_eq!(derived.password().len(), 64);
        assert_ne!(derived.password(), "ctl-pw");
        // The same on every resolver: a pure function of sprig_ctl's.
        assert_eq!(gw_credential(&ctl_ref).unwrap(), derived);
        // A [sprig_gw] table wins.
        std::fs::write(
            &path,
            "[sprig_ctl]\npassword = \"ctl-pw\"\n[sprig_gw]\npassword = \"own\"\n",
        )
        .unwrap();
        let own = gw_credential(&ctl_ref).unwrap();
        assert_eq!((own.user.as_str(), own.password()), ("sprig_gw", "own"));
        // A broken table is an error, not a silent derivation.
        std::fs::write(
            &path,
            "[sprig_ctl]\npassword = \"ctl-pw\"\n[sprig_gw]\npasswd = \"typo\"\n",
        )
        .unwrap();
        assert!(gw_credential(&ctl_ref).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_statements_grant_only_slave_monitor() {
        let sql = account_sql("p'w");
        assert_eq!(sql.len(), 3);
        assert!(sql[0].contains("IDENTIFIED BY 'p''w' REQUIRE SSL"), "{}", sql[0]);
        assert_eq!(sql[2], "GRANT SLAVE MONITOR ON *.* TO 'sprig_gw'@'%'");
    }
}
