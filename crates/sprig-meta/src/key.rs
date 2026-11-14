//! Tiers and keys: the etcd key layout.
//!
//! Global keys live under `/sprig/g/`, a data center's under
//! `/sprig/dc/<dc_id>/`. Only this crate builds etcd keys.

use std::fmt;

use sprig_common::ids::DcId;
use sprig_common::{Result, SprigError};

/// Which etcd tier a store talks to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Tier {
    /// The global tier: `/sprig/g/`.
    Global,
    /// One data center's tier: `/sprig/dc/<dc_id>/`.
    Dc(DcId),
}

impl Tier {
    /// The tier's root prefix, ending in `/`.
    #[must_use]
    pub fn root(&self) -> Key {
        match self {
            Tier::Global => Key("/sprig/g/".to_owned()),
            Tier::Dc(dc) => Key(format!("/sprig/dc/{dc}/")),
        }
    }

    /// A key under this tier, from a relative path such as
    /// `clusters/mum1-c7` (a key) or `clusters/` (a prefix).
    ///
    /// # Errors
    /// `Internal` if `rel` is empty, absolute, or has an empty or invalid
    /// segment: keys are built by code, so a bad one is a bug.
    pub fn key(&self, rel: &str) -> Result<Key> {
        let body = rel.strip_suffix('/').unwrap_or(rel);
        let valid = !body.is_empty()
            && body.split('/').all(|seg| {
                !seg.is_empty()
                    && seg
                        .bytes()
                        .all(|b| b.is_ascii_graphic() && b != b'/' && b != b'*')
            });
        if !valid {
            return Err(SprigError::internal(format!(
                "invalid etcd key path {rel:?}"
            )));
        }
        Ok(Key(format!("{}{rel}", self.root().0)))
    }
}

/// An absolute etcd key or prefix. A prefix ends in `/`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(String);

impl Key {
    /// A key exactly as a store returned it, unchecked. Only stores call
    /// this: every key they return was built through [`Tier::key`].
    pub(crate) fn from_stored(raw: String) -> Self {
        Key(raw)
    }

    /// The key as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether `self` lies under `prefix`.
    #[must_use]
    pub fn starts_with(&self, prefix: &Key) -> bool {
        self.0.starts_with(&prefix.0)
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_follow_the_layout() {
        let dc = Tier::Dc(DcId::new("mum1").unwrap());
        assert_eq!(
            dc.key("clusters/mum1-c7").unwrap().as_str(),
            "/sprig/dc/mum1/clusters/mum1-c7"
        );
        assert_eq!(Tier::Global.key("dcs/").unwrap().as_str(), "/sprig/g/dcs/");
        let k = dc.key("settings/auto_index.min_runs").unwrap();
        assert!(k.starts_with(&dc.key("settings/").unwrap()));
        assert!(!k.starts_with(&Tier::Global.root()));
        for bad in ["", "/abs", "a//b", "a b", "a/*"] {
            assert!(Tier::Global.key(bad).is_err(), "{bad:?}");
        }
    }
}
