//! The assistants' roles.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Who an assistant works for, and so which model, tools and egress rules
/// apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// The administration agent (operators): error advice, health checks,
    /// write-ups. Sees no tenant data, only the problem.
    Admin,
    /// The cheap model that screens administration intake for noise. No
    /// tools.
    Triage,
    /// The tenant's DBA assistant.
    Dba,
    /// The tenant's data specialist.
    Data,
}

impl Role {
    /// Every role.
    pub const ALL: [Role; 4] = [Role::Admin, Role::Triage, Role::Dba, Role::Data];

    /// `admin`, `triage`, `dba` or `data`: the name in settings, metrics
    /// and the catalog.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Triage => "triage",
            Role::Dba => "dba",
            Role::Data => "data",
        }
    }

    /// The role named `s`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Role> {
        Role::ALL.into_iter().find(|r| r.as_str() == s)
    }

    /// A tenant role: it works on one tenant's data, as one of its users.
    #[must_use]
    pub const fn is_tenant(self) -> bool {
        matches!(self, Role::Dba | Role::Data)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
