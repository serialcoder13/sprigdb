//! Who may act on a recommendation (chapter 52, "Who may act").
//!
//! Pure, so the gateway (which knows a session's privileges) and the
//! controller (which checks the gateway's claim) give the same answer.

use serde::{Deserialize, Serialize};
use sprig_common::ids::{DcId, TenantId};

use crate::model::{Level, Recommendation};

/// The caller of an action, as the controller sees it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)] // one flag per privilege the gateway checks
pub struct Caller {
    /// For the history and the decision log: `op:alice`,
    /// `studio:acme.ana`, `sql:acme.ana`.
    pub name: String,
    /// A platform operator.
    #[serde(default)]
    pub platform: bool,
    /// An operator of this DC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dc_operator: Option<DcId>,
    /// The caller's tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<TenantId>,
    /// Holds `SPRIG_TENANT_ADMIN` in that tenant.
    #[serde(default)]
    pub tenant_admin: bool,
    /// Holds `ALTER` (or `INDEX`, for index kinds) on the item's database.
    #[serde(default)]
    pub db_privilege: bool,
    /// Holds `ALTER` or `INDEX` on the item's table.
    #[serde(default)]
    pub table_privilege: bool,
    /// The caller's `ALTER` and `INDEX` grants, for gateways that pass a
    /// claim covering many items (a list); [`Caller::for_item`] turns them
    /// into the two flags above.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<GrantClaim>,
}

/// One `ALTER` or `INDEX` grant: `db` and `table` are names or `*`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantClaim {
    /// A database name or `*`.
    pub db: String,
    /// A table name or `*`.
    #[serde(default = "star")]
    pub table: String,
    /// Holds `ALTER`.
    #[serde(default)]
    pub alter: bool,
    /// Holds `INDEX`.
    #[serde(default)]
    pub index: bool,
}

fn star() -> String {
    "*".into()
}

impl Caller {
    /// The caller with `db_privilege` and `table_privilege` set from its
    /// grant claims for `rec`'s database and table (`INDEX` counts only
    /// for index kinds).
    #[must_use]
    pub fn for_item(&self, rec: &Recommendation) -> Caller {
        let mut c = self.clone();
        let fits = |g: &GrantClaim| g.alter || (g.index && rec.kind.is_index());
        let same =
            |a: &str, b: Option<&String>| a == "*" || b.is_some_and(|b| a.eq_ignore_ascii_case(b));
        for g in &self.grants {
            if !fits(g) || !same(&g.db, rec.subject.db_name.as_ref()) {
                continue;
            }
            if g.table == "*" {
                c.db_privilege = true;
                c.table_privilege = true;
            } else if same(&g.table, rec.subject.object_name.as_ref()) {
                c.table_privilege = true;
            }
        }
        c
    }

    /// A platform operator.
    #[must_use]
    pub fn platform(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            platform: true,
            ..Self::default()
        }
    }

    /// A DC operator of `dc`.
    #[must_use]
    pub fn dc_operator(name: impl Into<String>, dc: DcId) -> Self {
        Self {
            name: name.into(),
            dc_operator: Some(dc),
            ..Self::default()
        }
    }
}

/// Whether `caller` may act on `rec`. `ancestry` is the tenant's current
/// ancestors, root first (the directory's, not the copy in the item).
#[must_use]
pub fn may_act(rec: &Recommendation, caller: &Caller, ancestry: &[TenantId]) -> bool {
    let caller = &caller.for_item(rec);
    if caller.platform {
        return true;
    }
    if let Some(dc) = &caller.dc_operator
        && rec.level == Level::Dc
        && rec.subject.dc.as_ref() == Some(dc)
    {
        return true;
    }
    let Some(me) = caller.tenant else {
        return false;
    };
    let Some(owner) = rec.subject.tenant else {
        return false;
    };
    if me == owner {
        if caller.tenant_admin && rec.level <= Level::Tenant {
            return true;
        }
        if caller.db_privilege && rec.level <= Level::Database && rec.subject.db.is_some() {
            return true;
        }
        if caller.table_privilege && rec.level == Level::Table && rec.subject.object.is_some() {
            return true;
        }
        return false;
    }
    // An ancestor's administrators act for every descendant (Kiran, 10
    // November 2026).
    caller.tenant_admin && rec.level <= Level::Tenant && ancestry.contains(&me)
}

/// Whether `caller` may see `rec` in a list: anyone who may act, plus a
/// tenant's users with any privilege the gateway found on its target.
#[must_use]
pub fn may_see(rec: &Recommendation, caller: &Caller, ancestry: &[TenantId]) -> bool {
    let caller = &caller.for_item(rec);
    may_act(rec, caller, ancestry)
        || (caller.tenant.is_some()
            && caller.tenant == rec.subject.tenant
            && (caller.db_privilege || caller.table_privilege || caller.tenant_admin))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::new_item;
    use crate::model::{Kind, Mode, Proposal, RecId, Subject};
    use crate::policy::Policy;
    use sprig_common::ids::{DbId, ObjectId};

    fn rec(kind: Kind, level: Level) -> Recommendation {
        let p = Proposal {
            kind,
            subject: Subject {
                tenant: Some(TenantId(42)),
                db: Some(DbId(1)),
                object: Some(ObjectId(7)),
                dc: Some(DcId::new("mum1").unwrap()),
                ..Subject::default()
            },
            target: "t42/o7/x".into(),
            action: crate::model::Action::default(),
            evidence: serde_json::json!({}),
            score: 1.0,
            summary: String::new(),
            emergency: false,
        };
        new_item(
            &p,
            &Policy::fixed(Mode::Approve, level),
            RecId::Tenant(TenantId(42), 1),
            &[TenantId(1), TenantId(9)],
            0,
        )
    }

    fn user(tenant: u64) -> Caller {
        Caller {
            name: "u".into(),
            tenant: Some(TenantId(tenant)),
            ..Caller::default()
        }
    }

    #[test]
    fn the_table_of_who_may_act() {
        let anc = [TenantId(1), TenantId(9)];
        let table = rec(Kind::IndexCreate, Level::Table);
        let db = rec(Kind::IndexCreate, Level::Database);
        let tenant = rec(Kind::DcMove, Level::Tenant);
        let dc = rec(Kind::DcMove, Level::Dc);
        let platform = rec(Kind::IndexCreate, Level::Platform);

        let op = Caller::platform("op:root");
        for r in [&table, &db, &tenant, &dc, &platform] {
            assert!(may_act(r, &op, &anc));
        }
        let dco = Caller::dc_operator("dc:mum1", DcId::new("mum1").unwrap());
        assert!(may_act(&dc, &dco, &anc));
        assert!(!may_act(&tenant, &dco, &anc));
        assert!(!may_act(&platform, &dco, &anc));
        let other_dc = Caller::dc_operator("dc:fra1", DcId::new("fra1").unwrap());
        assert!(!may_act(&dc, &other_dc, &anc));

        let admin = Caller {
            tenant_admin: true,
            ..user(42)
        };
        assert!(may_act(&table, &admin, &anc));
        assert!(may_act(&db, &admin, &anc));
        assert!(may_act(&tenant, &admin, &anc));
        assert!(!may_act(&dc, &admin, &anc));
        assert!(!may_act(&platform, &admin, &anc));

        let owner = Caller {
            table_privilege: true,
            ..user(42)
        };
        assert!(may_act(&table, &owner, &anc));
        assert!(!may_act(&db, &owner, &anc));
        let dbo = Caller {
            db_privilege: true,
            ..user(42)
        };
        assert!(may_act(&table, &dbo, &anc));
        assert!(may_act(&db, &dbo, &anc));
        assert!(!may_act(&tenant, &dbo, &anc));

        // Ancestors' administrators: yes; their plain users: no.
        let parent_admin = Caller {
            tenant_admin: true,
            ..user(9)
        };
        let root_admin = Caller {
            tenant_admin: true,
            ..user(1)
        };
        assert!(may_act(&tenant, &parent_admin, &anc));
        assert!(may_act(&table, &root_admin, &anc));
        assert!(!may_act(&platform, &root_admin, &anc));
        let parent_user = Caller {
            db_privilege: true,
            table_privilege: true,
            ..user(9)
        };
        assert!(!may_act(&table, &parent_user, &anc));
        let stranger_admin = Caller {
            tenant_admin: true,
            ..user(77)
        };
        assert!(!may_act(&table, &stranger_admin, &anc));
        assert!(!may_act(&table, &Caller::default(), &anc));

        // Grant claims: INDEX on the table's database covers index items
        // at table and database level; ALTER on another table doesn't.
        let mut t = table.clone();
        t.subject.db_name = Some("shop".into());
        t.subject.object_name = Some("orders".into());
        let claim = |db: &str, table: &str, alter: bool, index: bool| Caller {
            grants: vec![GrantClaim {
                db: db.into(),
                table: table.into(),
                alter,
                index,
            }],
            ..user(42)
        };
        assert!(may_act(&t, &claim("shop", "*", false, true), &anc));
        assert!(may_act(&t, &claim("SHOP", "orders", true, false), &anc));
        assert!(!may_act(&t, &claim("shop", "items", true, true), &anc));
        assert!(!may_act(&t, &claim("other", "*", true, true), &anc));
        let mut mv = tenant.clone();
        mv.subject.db_name = Some("shop".into());
        assert!(
            !may_act(&mv, &claim("*", "*", false, true), &anc),
            "INDEX doesn't cover DC moves"
        );
    }
}
