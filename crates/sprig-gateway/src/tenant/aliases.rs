//! Backend accounts in output, shown as the session's user.
//!
//! A tenant's statements run as a backend account (a per-user
//! `t<tenant>_u<id>`, which [`super::names::Accounts`] maps, or the one
//! `[gateway] backends_file` names), and stored programs as the cluster's
//! provisioning account (see [`super::ddl`]). Neither may reach a tenant: a
//! `DEFINER` in `SHOW CREATE` output, in `information_schema` and in dumps,
//! a grantee in `SHOW GRANTS`, a user in an error message. Each
//! `'account'@'host'` naming one of them, in any of MariaDB's spellings,
//! becomes the session's user at host `%`:
//!
//! | Written | Becomes |
//! |---|---|
//! | `` `root`@`%` `` | `` `app`@`%` `` |
//! | `'root'@'localhost'` | `'app'@'%'` |
//! | `root@%` (a `DEFINER` column) | `app@%` |
//!
//! A dump restored through the gateway therefore names the tenant's own
//! user, which is also what the restore's `DEFINER` becomes.

/// The accounts to hide and the name to show instead.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountAliases {
    /// Backend account names (user part).
    pub backend: Vec<String>,
    /// The session's user.
    pub user: String,
}

fn host_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'.' | b'%' | b'_' | b':' | b'-')
}

fn ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

impl AccountAliases {
    /// `text` with every backend account replaced; `None` when there is
    /// none.
    #[must_use]
    pub fn apply(&self, text: &[u8]) -> Option<Vec<u8>> {
        if self.backend.is_empty() {
            return None;
        }
        let mut out: Option<Vec<u8>> = None;
        let mut copied = 0;
        let mut i = 0;
        while i < text.len() {
            let mut hit = None;
            for b in &self.backend {
                if let Some(end) = matches(text, i, b.as_bytes()) {
                    hit = Some(end);
                    break;
                }
            }
            match hit {
                Some((end, quote)) => {
                    let o = out.get_or_insert_with(Vec::new);
                    o.extend_from_slice(&text[copied..i]);
                    match quote {
                        Some(q) => {
                            let q = q as char;
                            let user = if q == '`' {
                                self.user.replace('`', "``")
                            } else {
                                self.user.replace('\'', "''")
                            };
                            o.extend_from_slice(format!("{q}{user}{q}@{q}%{q}").as_bytes());
                        }
                        None => o.extend_from_slice(format!("{}@%", self.user).as_bytes()),
                    }
                    copied = end;
                    i = end;
                }
                None => i += 1,
            }
        }
        let mut o = out?;
        o.extend_from_slice(&text[copied..]);
        Some(o)
    }
}

/// Whether `user@host` in one of the spellings starts at `i`: the end
/// offset and the quote used.
fn matches(t: &[u8], i: usize, user: &[u8]) -> Option<(usize, Option<u8>)> {
    let c = *t.get(i)?;
    if matches!(c, b'`' | b'\'' | b'"') {
        let rest = t.get(i + 1..)?;
        if !rest.starts_with(user) || rest.get(user.len()) != Some(&c) {
            return None;
        }
        let at = i + 1 + user.len() + 1;
        if t.get(at) != Some(&b'@') || t.get(at + 1) != Some(&c) {
            return None;
        }
        let hs = at + 2;
        let he = t[hs..].iter().position(|&x| x == c)? + hs;
        return Some((he + 1, Some(c)));
    }
    if i > 0 && (ident_byte(t[i - 1]) || t[i - 1] == b'@') {
        return None;
    }
    if !t[i..].starts_with(user) || t.get(i + user.len()) != Some(&b'@') {
        return None;
    }
    let hs = i + user.len() + 1;
    let he = t[hs..]
        .iter()
        .position(|&x| !host_byte(x))
        .map_or(t.len(), |o| hs + o);
    if he == hs {
        return None;
    }
    Some((he, None))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn backend_accounts_become_the_user() {
        let a = AccountAliases {
            backend: vec!["root".into(), "sprig_ctl".into()],
            user: "app".into(),
        };
        let s = |t: &str| String::from_utf8(a.apply(t.as_bytes()).unwrap()).unwrap();
        assert_eq!(
            s("CREATE DEFINER=`root`@`%` PROCEDURE p()"),
            "CREATE DEFINER=`app`@`%` PROCEDURE p()"
        );
        assert_eq!(s("DEFINER='sprig_ctl'@'localhost'"), "DEFINER='app'@'%'");
        assert_eq!(s("root@%|root@localhost"), "app@%|app@%");
        assert!(a.apply(b"rooted@x groot@% @root@%").is_none());
        assert!(a.apply(b"`rootx`@`%`").is_none());
    }
}
