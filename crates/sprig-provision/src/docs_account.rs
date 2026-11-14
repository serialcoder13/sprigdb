//! `sprig_docs_r`, the docs-reader account: the gateways read the product
//! documentation in `__sprig_docs` with it, and with nothing else.
//!
//! It holds `SELECT` on `__sprig_docs`.* only: no data, no other schema, no
//! writes. Tenants' backend accounts get nothing on `__sprig_docs`, and the
//! controller's `sprig_ctl` owns the schema and loads it.
//!
//! **Its credential** ([`docs_credential`]) works like `sprig_gw`'s: the
//! `[sprig_docs_r]` table of the cluster's credentials file when there is
//! one, else a password derived from `sprig_ctl`'s (HMAC-SHA256 of
//! `sprig_docs_r` keyed by it, in hex) with `sprig_ctl`'s TLS settings.
//! Whoever can resolve `sprig_ctl` can resolve it, and no new secret has
//! to be stored.
//!
//! **Creating it** ([`account_sql`]): the controller's docs loader, as
//! `sprig_ctl`, once per cluster after it starts and before it loads a
//! pack. `sprig_ctl` needs `CREATE USER` (which it has) and `GRANT OPTION`
//! on `__sprig_docs` ([`CTL_GRANT`]).

use sprig_cluster::sql::literal;
use sprig_common::Result;

use crate::credentials::{Credential, SecretRef, SecretSource};
use crate::gw_account::hmac_sha256;

/// The account's name.
pub const DOCS_USER: &str = "sprig_docs_r";

/// The schema it reads.
pub const DOCS_SCHEMA: &str = "__sprig_docs";

/// `sprig_docs_r`'s password derived from `sprig_ctl`'s (when the
/// credentials file has no `[sprig_docs_r]` table).
#[must_use]
pub fn derived_password(ctl_password: &str) -> String {
    hmac_sha256(ctl_password.as_bytes(), DOCS_USER.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// `sprig_docs_r`'s credential for a cluster whose `sprig_ctl` reference
/// is `ctl_ref` (see the module notes).
///
/// # Errors
/// 39998 when `sprig_ctl`'s reference doesn't resolve, or the file's
/// `[sprig_docs_r]` table doesn't parse.
pub fn docs_credential(ctl_ref: &str) -> Result<Credential> {
    let ctl: SecretRef = ctl_ref.parse()?;
    if ctl.section.is_some() && !matches!(ctl.source, SecretSource::Resolved(_)) {
        let own = SecretRef {
            source: ctl.source.clone(),
            section: Some(DOCS_USER.to_owned()),
        };
        match own.resolve(DOCS_USER) {
            Ok(c) => return Ok(c),
            // No `[sprig_docs_r]` table: derive.
            Err(e) if e.message.contains("no [sprig_docs_r] table") => {}
            Err(e) => return Err(e),
        }
    }
    let c = ctl.resolve(crate::credentials::CTL_USER)?;
    let mut docs = Credential::new(DOCS_USER, derived_password(c.password()), c.tls);
    docs.ca_file = c.ca_file;
    Ok(docs)
}

/// The statements that create (or re-assert) `sprig_docs_r` with
/// `password` and its one grant. Run after `__sprig_docs` exists.
#[must_use]
pub fn account_sql(password: &str) -> Vec<String> {
    let pw = literal(password);
    vec![
        format!("CREATE USER IF NOT EXISTS '{DOCS_USER}'@'%' IDENTIFIED BY {pw} REQUIRE SSL"),
        format!("ALTER USER '{DOCS_USER}'@'%' IDENTIFIED BY {pw} REQUIRE SSL"),
        format!("GRANT SELECT ON `{DOCS_SCHEMA}`.* TO '{DOCS_USER}'@'%'"),
    ]
}

/// The grant `sprig_ctl` needs to own `__sprig_docs`, for messages.
pub const CTL_GRANT: &str =
    "GRANT ALL PRIVILEGES ON `__sprig_docs`.* TO 'sprig_ctl'@'%' WITH GRANT OPTION";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::TlsMode;

    #[test]
    fn the_credential_is_the_files_table_or_derived_from_sprig_ctl() {
        let dir = std::env::temp_dir().join(format!("sprig-docs-r-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.toml");
        std::fs::write(
            &path,
            "[sprig_ctl]\npassword = \"ctl-pw\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\npassword = \"r\"\n",
        )
        .unwrap();
        let ctl_ref = format!("file:{}#sprig_ctl", path.display());
        let derived = docs_credential(&ctl_ref).unwrap();
        assert_eq!(derived.user, DOCS_USER);
        assert_eq!(derived.tls, TlsMode::SkipVerify);
        assert_eq!(derived.password(), derived_password("ctl-pw"));
        assert_eq!(derived.password().len(), 64);
        // Not sprig_gw's: each account has its own password.
        assert_ne!(
            derived.password(),
            crate::gw_account::derived_password("ctl-pw")
        );
        std::fs::write(
            &path,
            "[sprig_ctl]\npassword = \"ctl-pw\"\n[sprig_docs_r]\npassword = \"own\"\n",
        )
        .unwrap();
        let own = docs_credential(&ctl_ref).unwrap();
        assert_eq!((own.user.as_str(), own.password()), (DOCS_USER, "own"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_statements_grant_only_select_on_the_docs() {
        let sql = account_sql("p'w");
        assert_eq!(sql.len(), 3);
        assert!(
            sql[0].contains("IDENTIFIED BY 'p''w' REQUIRE SSL"),
            "{}",
            sql[0]
        );
        assert_eq!(
            sql[2],
            "GRANT SELECT ON `__sprig_docs`.* TO 'sprig_docs_r'@'%'"
        );
    }
}
