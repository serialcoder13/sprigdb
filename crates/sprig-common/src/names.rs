//! Physical names inside MariaDB (chapter 03, "Identifiers"; chapter 07,
//! "Physical names" and "Reserved names"; writing guide 5.4).
//!
//! This module is the only place that knows the patterns. Anything that
//! needs a physical schema, account or internal object name asks it, and
//! anything that meets such a name in backend output parses it here.
//!
//! ```text
//! schema  := "t" tenant "_" ( "d" db "_" ( "g" group "_s" shard | "ref" ) | "sprig" )
//! account := "t" tenant "_" ( "u" user | "r" role | "rc" 8 lowercase hex )
//! ```
//!
//! Ids are written in their canonical decimal form, so parsing rejects
//! leading zeros (`t042_sprig`) and anything else that would make two
//! spellings name one object.

use std::fmt;
use std::str::FromStr;

use crate::ids::{DbId, GroupId, RoleId, ShardId, TenantId, UserId};

/// The per-cluster internal schema: probe tables, heartbeats, mover state
/// (chapter 09). Never granted to tenants.
pub const SYS_SCHEMA: &str = "__sprig_sys";

/// Prefix of internal tables, views, routines, triggers and events.
pub const INTERNAL_OBJECT_PREFIX: &str = "__sprig_";

/// Prefix of internal columns, always `INVISIBLE`.
pub const INTERNAL_COLUMN_PREFIX: &str = "_sprig_";

/// Prefix of promoted document field columns (`_sprig_p_<slug>`).
pub const PROMOTED_FIELD_PREFIX: &str = "_sprig_p_";

/// Prefix of automatic index names (`sprig_auto_<8 hex>`).
pub const AUTO_INDEX_PREFIX: &str = "sprig_auto_";

/// Prefix of user variables owned by the gateway (`@sprig_...`), without
/// the `@`.
pub const USER_VARIABLE_PREFIX: &str = "sprig_";

/// The invisible primary key, named like MySQL's GIPK.
pub const GIPK_COLUMN: &str = "my_row_id";

/// Database names no one may create or drop.
pub const SYSTEM_SCHEMAS: [&str; 4] = ["information_schema", "performance_schema", "sys", "mysql"];

/// Grant pattern covering every tenant schema, for `sprig_ctl` (chapter 09).
pub const TENANT_SCHEMAS_GRANT_PATTERN: &str = "t%";

/// Grant pattern covering every tenant catalog schema, for `sprig_gw`.
pub const CATALOG_SCHEMAS_GRANT_PATTERN: &str = "t%\\_sprig";

/// Error returned when a name doesn't parse as a SprigDB pattern.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{name:?} is not a SprigDB {kind} name")]
pub struct NameParseError {
    kind: &'static str,
    name: String,
}

impl NameParseError {
    fn new(kind: &'static str, name: &str) -> Self {
        Self {
            kind,
            name: name.to_owned(),
        }
    }
}

/// A physical schema name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SchemaName {
    /// One shard of a table group: `t<tenant>_d<db>_g<group>_s<shard>`.
    Shard {
        /// The tenant.
        tenant: TenantId,
        /// The logical database.
        db: DbId,
        /// The table group.
        group: GroupId,
        /// The shard.
        shard: ShardId,
    },
    /// The reference tables of a database: `t<tenant>_d<db>_ref` (R1).
    Reference {
        /// The tenant.
        tenant: TenantId,
        /// The logical database.
        db: DbId,
    },
    /// The tenant catalog: `t<tenant>_sprig`. Never visible to clients.
    Catalog {
        /// The tenant.
        tenant: TenantId,
    },
}

impl SchemaName {
    /// A shard schema.
    #[must_use]
    pub const fn shard(tenant: TenantId, db: DbId, group: GroupId, shard: ShardId) -> Self {
        Self::Shard {
            tenant,
            db,
            group,
            shard,
        }
    }

    /// A reference-table schema.
    #[must_use]
    pub const fn reference(tenant: TenantId, db: DbId) -> Self {
        Self::Reference { tenant, db }
    }

    /// A tenant catalog schema.
    #[must_use]
    pub const fn catalog(tenant: TenantId) -> Self {
        Self::Catalog { tenant }
    }

    /// The owning tenant.
    #[must_use]
    pub const fn tenant(&self) -> TenantId {
        match *self {
            Self::Shard { tenant, .. }
            | Self::Reference { tenant, .. }
            | Self::Catalog { tenant } => tenant,
        }
    }

    /// The logical database, unless this is the catalog.
    #[must_use]
    pub const fn db(&self) -> Option<DbId> {
        match *self {
            Self::Shard { db, .. } | Self::Reference { db, .. } => Some(db),
            Self::Catalog { .. } => None,
        }
    }
}

impl fmt::Display for SchemaName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shard {
                tenant,
                db,
                group,
                shard,
            } => write!(f, "t{tenant}_d{db}_g{group}_s{shard}"),
            Self::Reference { tenant, db } => write!(f, "t{tenant}_d{db}_ref"),
            Self::Catalog { tenant } => write!(f, "t{tenant}_sprig"),
        }
    }
}

impl FromStr for SchemaName {
    type Err = NameParseError;

    /// Parses the exact canonical form, case-sensitively, as MariaDB
    /// returns it.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || NameParseError::new("schema", s);
        let (tenant, rest) = tenant_prefix(s).ok_or_else(err)?;
        if rest == "sprig" {
            return Ok(Self::Catalog { tenant });
        }
        let rest = rest.strip_prefix('d').ok_or_else(err)?;
        let (db, rest) = rest.split_once('_').ok_or_else(err)?;
        let db = DbId(canonical(db, 10).ok_or_else(err)?);
        if rest == "ref" {
            return Ok(Self::Reference { tenant, db });
        }
        let rest = rest.strip_prefix('g').ok_or_else(err)?;
        let (group, shard) = rest.split_once("_s").ok_or_else(err)?;
        Ok(Self::Shard {
            tenant,
            db,
            group: GroupId(canonical(group, 10).ok_or_else(err)?),
            shard: ShardId(canonical(shard, 10).ok_or_else(err)?),
        })
    }
}

/// A tenant's backend account (chapter 29), without the host part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AccountName {
    /// A user: `t<tenant>_u<user>`.
    User {
        /// The tenant.
        tenant: TenantId,
        /// The user.
        user: UserId,
    },
    /// A role: `t<tenant>_r<role>`.
    Role {
        /// The tenant.
        tenant: TenantId,
        /// The role.
        role: RoleId,
    },
    /// A combined role: `t<tenant>_rc<8 hex>`, the hex a hash of the set.
    CombinedRole {
        /// The tenant.
        tenant: TenantId,
        /// The 32-bit hash, written as 8 lowercase hex digits.
        hash: u32,
    },
}

impl AccountName {
    /// The owning tenant.
    #[must_use]
    pub const fn tenant(&self) -> TenantId {
        match *self {
            Self::User { tenant, .. }
            | Self::Role { tenant, .. }
            | Self::CombinedRole { tenant, .. } => tenant,
        }
    }
}

impl fmt::Display for AccountName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::User { tenant, user } => write!(f, "t{tenant}_u{user}"),
            Self::Role { tenant, role } => write!(f, "t{tenant}_r{role}"),
            Self::CombinedRole { tenant, hash } => write!(f, "t{tenant}_rc{hash:08x}"),
        }
    }
}

impl FromStr for AccountName {
    type Err = NameParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || NameParseError::new("account", s);
        let (tenant, rest) = tenant_prefix(s).ok_or_else(err)?;
        if let Some(hex) = rest.strip_prefix("rc") {
            let ok = hex.len() == 8 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
            let hash = ok
                .then(|| u32::from_str_radix(hex, 16).ok())
                .flatten()
                .ok_or_else(err)?;
            return Ok(Self::CombinedRole { tenant, hash });
        }
        if let Some(role) = rest.strip_prefix('r') {
            let role = RoleId(canonical(role, 10).ok_or_else(err)?);
            return Ok(Self::Role { tenant, role });
        }
        let user = rest.strip_prefix('u').ok_or_else(err)?;
        let user = UserId(canonical(user, 10).ok_or_else(err)?);
        Ok(Self::User { tenant, user })
    }
}

/// The `LIKE` pattern matching every backend account of `tenant`, with
/// `_` escaped: `t42\_%`. Used by fences to find the tenant's grants.
#[must_use]
pub fn tenant_account_like(tenant: TenantId) -> String {
    format!("t{tenant}\\_%")
}

/// SprigDB's system accounts (chapter 09, "System accounts").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemAccount {
    /// `sprig_ctl`: controllers and workers.
    Ctl,
    /// `sprig_repl`: binlog reading.
    Repl,
    /// `sprig_gw`: the gateway's catalogs, health and positions.
    Gw,
}

impl SystemAccount {
    /// Every system account.
    pub const ALL: [SystemAccount; 3] = [Self::Ctl, Self::Repl, Self::Gw];

    /// The account's user name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ctl => "sprig_ctl",
            Self::Repl => "sprig_repl",
            Self::Gw => "sprig_gw",
        }
    }
}

impl fmt::Display for SystemAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SystemAccount {
    type Err = NameParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|a| a.as_str() == s)
            .ok_or_else(|| NameParseError::new("system account", s))
    }
}

/// An internal table or other object: `__sprig_<name>`.
#[must_use]
pub fn internal_object(name: &str) -> String {
    format!("{INTERNAL_OBJECT_PREFIX}{name}")
}

/// An internal column: `_sprig_<name>`.
#[must_use]
pub fn internal_column(name: &str) -> String {
    format!("{INTERNAL_COLUMN_PREFIX}{name}")
}

/// A promoted document field column: `_sprig_p_<slug>`.
#[must_use]
pub fn promoted_field_column(slug: &str) -> String {
    format!("{PROMOTED_FIELD_PREFIX}{slug}")
}

/// An automatic index name: `sprig_auto_<8 hex>`.
#[must_use]
pub fn auto_index(hash: u32) -> String {
    format!("{AUTO_INDEX_PREFIX}{hash:08x}")
}

/// Parses an automatic index name back to its hash.
#[must_use]
pub fn parse_auto_index(name: &str) -> Option<u32> {
    let hex = name.strip_prefix(AUTO_INDEX_PREFIX)?;
    (hex.len() == 8 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
        .then(|| u32::from_str_radix(hex, 16).ok())
        .flatten()
}

/// A document array-index side table: `__sprig_ix_<collection>__<index id>`.
#[must_use]
pub fn doc_array_index_table(collection: &str, index: u32) -> String {
    format!("{INTERNAL_OBJECT_PREFIX}ix_{collection}__{index}")
}

/// A document wildcard-index side table: `__sprig_wx_<collection>`.
#[must_use]
pub fn doc_wildcard_index_table(collection: &str) -> String {
    format!("{INTERNAL_OBJECT_PREFIX}wx_{collection}")
}

/// Whether `name` has the `prefix`, ignoring ASCII case (chapter 07:
/// reserved prefixes match case-insensitively).
fn has_prefix_ci(name: &str, prefix: &str) -> bool {
    name.len() >= prefix.len()
        && name.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}

/// Whether a table, view, routine, trigger or event name is reserved.
#[must_use]
pub fn is_internal_object(name: &str) -> bool {
    has_prefix_ci(name, INTERNAL_OBJECT_PREFIX)
}

/// Whether a column name is reserved.
#[must_use]
pub fn is_internal_column(name: &str) -> bool {
    has_prefix_ci(name, INTERNAL_COLUMN_PREFIX)
}

/// Whether an index name is reserved for automatic indexes.
#[must_use]
pub fn is_auto_index(name: &str) -> bool {
    has_prefix_ci(name, AUTO_INDEX_PREFIX)
}

/// Whether a user variable (without the `@`) belongs to the gateway.
#[must_use]
pub fn is_gateway_variable(name: &str) -> bool {
    has_prefix_ci(name, USER_VARIABLE_PREFIX)
}

/// Whether a logical database name is reserved: `t` followed by digits and
/// `_`, or a system schema. Reserving `t<digits>_` guarantees that such a
/// token in backend output is a physical name.
#[must_use]
pub fn is_reserved_database(name: &str) -> bool {
    if SYSTEM_SCHEMAS.iter().any(|s| s.eq_ignore_ascii_case(name)) {
        return true;
    }
    let b = name.as_bytes();
    if b.len() < 3 || !b[0].eq_ignore_ascii_case(&b't') {
        return false;
    }
    let digits = b[1..].iter().take_while(|c| c.is_ascii_digit()).count();
    digits > 0 && b.get(1 + digits) == Some(&b'_')
}

/// Quotes an identifier with backquotes, doubling any inside it.
#[must_use]
pub fn quote(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('`');
    for c in ident.chars() {
        if c == '`' {
            out.push('`');
        }
        out.push(c);
    }
    out.push('`');
    out
}

/// Splits `t<tenant>_<rest>`.
fn tenant_prefix(s: &str) -> Option<(TenantId, &str)> {
    let (t, rest) = s.strip_prefix('t')?.split_once('_')?;
    Some((TenantId(canonical(t, 20)?), rest))
}

/// Parses a canonical decimal number of at most `max_digits` digits: no
/// sign, no leading zero (except `0` itself), and in range for `T`.
fn canonical<T: FromStr>(s: &str, max_digits: usize) -> Option<T> {
    let ok = !s.is_empty()
        && s.len() <= max_digits
        && s.bytes().all(|b| b.is_ascii_digit())
        && (s == "0" || !s.starts_with('0'));
    ok.then(|| s.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_format_as_the_guide_says() {
        let s = SchemaName::shard(TenantId(42), DbId(1), GroupId(3), ShardId(17));
        assert_eq!(s.to_string(), "t42_d1_g3_s17");
        assert_eq!(
            SchemaName::reference(TenantId(42), DbId(1)).to_string(),
            "t42_d1_ref"
        );
        assert_eq!(SchemaName::catalog(TenantId(42)).to_string(), "t42_sprig");
        assert_eq!(s.tenant(), TenantId(42));
        assert_eq!(s.db(), Some(DbId(1)));
        assert_eq!(SchemaName::catalog(TenantId(9)).db(), None);
    }

    #[test]
    fn schemas_round_trip_at_the_extremes() {
        let names = [
            SchemaName::shard(TenantId(0), DbId(0), GroupId(0), ShardId(0)),
            SchemaName::shard(
                TenantId(u64::MAX),
                DbId(u32::MAX),
                GroupId(u32::MAX),
                ShardId(u32::MAX),
            ),
            SchemaName::reference(TenantId(u64::MAX), DbId(7)),
            SchemaName::catalog(TenantId(1)),
        ];
        for n in names {
            assert_eq!(n.to_string().parse::<SchemaName>().unwrap(), n, "{n}");
        }
        // The longest name fits MariaDB's 64-character identifier limit.
        assert!(names[1].to_string().len() <= 64);
    }

    #[test]
    fn schema_parse_rejects_near_misses() {
        for bad in [
            "",
            "t42",
            "t42_",
            "t_sprig",
            "T42_sprig",
            "t042_sprig",
            "t42_d01_g1_s1",
            "t42_d1_g1_s",
            "t42_d1_g1_s1x",
            "t42_d1_g1_s+1",
            "t42_d1_g1",
            "t42_d1_REF",
            "t42_d1_g1_s4294967296",
            "t18446744073709551616_sprig",
            "t42_sprig_x",
            "x42_sprig",
            "t-1_sprig",
            "t42_u7",
            "__sprig_sys",
        ] {
            assert!(bad.parse::<SchemaName>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn accounts_round_trip() {
        let cases = [
            (
                "t42_u7",
                AccountName::User {
                    tenant: TenantId(42),
                    user: UserId(7),
                },
            ),
            (
                "t42_r3",
                AccountName::Role {
                    tenant: TenantId(42),
                    role: RoleId(3),
                },
            ),
            (
                "t42_rc3fa91c0e",
                AccountName::CombinedRole {
                    tenant: TenantId(42),
                    hash: 0x3fa9_1c0e,
                },
            ),
            (
                "t1_rc00000000",
                AccountName::CombinedRole {
                    tenant: TenantId(1),
                    hash: 0,
                },
            ),
        ];
        for (text, name) in cases {
            assert_eq!(name.to_string(), text);
            assert_eq!(text.parse::<AccountName>().unwrap(), name);
            assert_eq!(name.tenant(), name.tenant());
        }
        for bad in [
            "t42_u",
            "t42_u07",
            "t42_rc3FA91C0E",
            "t42_rc3fa91c0",
            "t42_rc3fa91c0e0",
            "t42_x7",
            "t42_d1_g1_s1",
            "sprig_ctl",
        ] {
            assert!(bad.parse::<AccountName>().is_err(), "{bad:?}");
        }
        assert_eq!(tenant_account_like(TenantId(42)), "t42\\_%");
    }

    #[test]
    fn system_accounts() {
        for a in SystemAccount::ALL {
            assert_eq!(a.to_string().parse::<SystemAccount>().unwrap(), a);
        }
        assert_eq!(SystemAccount::Ctl.as_str(), "sprig_ctl");
        assert!("sprig_admin".parse::<SystemAccount>().is_err());
    }

    #[test]
    fn internal_names() {
        assert_eq!(internal_object("probe"), "__sprig_probe");
        assert_eq!(internal_column("rk"), "_sprig_rk");
        assert_eq!(promoted_field_column("customer_id"), "_sprig_p_customer_id");
        assert_eq!(auto_index(0x3fa9_1c0e), "sprig_auto_3fa91c0e");
        assert_eq!(parse_auto_index("sprig_auto_3fa91c0e"), Some(0x3fa9_1c0e));
        assert_eq!(parse_auto_index("sprig_auto_3fa91c0"), None);
        assert_eq!(doc_array_index_table("orders", 2), "__sprig_ix_orders__2");
        assert_eq!(doc_wildcard_index_table("events"), "__sprig_wx_events");
        assert!(SYS_SCHEMA.starts_with(INTERNAL_OBJECT_PREFIX));
    }

    #[test]
    fn reserved_prefixes_match_case_insensitively() {
        assert!(is_internal_object("__SPRIG_dtx"));
        assert!(is_internal_object("__sprig_lk_orders__email"));
        assert!(!is_internal_object("_sprig_rk"));
        assert!(is_internal_column("_Sprig_rk"));
        assert!(!is_internal_column("sprig_rk"));
        assert!(is_auto_index("SPRIG_AUTO_x"));
        assert!(!is_auto_index("sprig_aut"));
        assert!(is_gateway_variable("sprig_show_internal"));
        assert!(!is_gateway_variable("spri"));
    }

    #[test]
    fn reserved_databases() {
        for r in [
            "t42_x",
            "T1_",
            "t0_billing",
            "mysql",
            "INFORMATION_SCHEMA",
            "sys",
        ] {
            assert!(is_reserved_database(r), "{r:?}");
        }
        for ok in ["t", "t_", "tx_1", "t42", "billing", "tenants", "systems"] {
            assert!(!is_reserved_database(ok), "{ok:?}");
        }
    }

    #[test]
    fn quoting_doubles_backquotes() {
        assert_eq!(quote("t42_sprig"), "`t42_sprig`");
        assert_eq!(quote("a`b"), "`a``b`");
    }
}
