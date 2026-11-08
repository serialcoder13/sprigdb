//! Names on the way out (chapter 07, "Result metadata and error messages";
//! build step 7): column definitions, error and warning messages, and any
//! text MariaDB sends back.
//!
//! Every function here works on the same scan: identifier runs (bytes in
//! `[A-Za-z0-9_$]` or non-ASCII) that parse as a SprigDB physical name.
//! Reserving `t<digits>_` for database names (chapter 07, "Reserved names")
//! makes such a run in backend output a physical name.
//!
//! | Run | Becomes |
//! |---|---|
//! | the tenant's shard or reference schema | the logical database name |
//! | `Tables_in_<schema>` (the `SHOW TABLES` header) | `Tables_in_<logical>` |
//! | the tenant's account `t<tenant>_u<id>`, `_r<id>`, `_rc<hex>` | the user or role name, from the [`AccountResolver`] |
//! | anything else physical: another tenant's names, the tenant catalog, an unknown database id, an unresolved account | `?`, counted as a foreign name |
//! | `__sprig_lk_<table>__<cols>`, `__sprig_ix_<collection>__<id>`, `__sprig_wx_<collection>` | the owning table or collection |
//!
//! Inside backquotes the replacement doubles any backquote, so the quoting
//! stays valid. A foreign name is a security event (`names.foreign_name`)
//! and pages through `sprig_names_foreign_names_total`; the counted
//! variants return how many were replaced so the caller can raise them.

use std::borrow::Cow;

use sprig_common::names::{AccountName, SchemaName};
use sprig_proto_mysql::ColumnDef;

use super::map::NameMap;
use crate::lex::LexOptions;

/// Resolves the tenant's backend accounts to the names clients know
/// (chapter 29). Without one, account names map to `?`.
pub trait AccountResolver: Send + Sync {
    /// The user or role name for `account`, which belongs to the session's
    /// tenant: `alice` for `t42_u7`.
    fn account_name(&self, account: &AccountName) -> Option<String>;
}

/// What the output mapping needs: the tenant's names and accounts.
#[derive(Clone, Copy)]
pub struct OutputCtx<'a> {
    /// The tenant's names.
    pub map: &'a dyn NameMap,
    /// The tenant's account names, if known.
    pub accounts: Option<&'a dyn AccountResolver>,
    /// Lexer options for definitions MariaDB returns (view bodies,
    /// `SHOW CREATE` text).
    pub lex: LexOptions,
}

impl std::fmt::Debug for OutputCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputCtx")
            .field("tenant", &self.map.tenant())
            .field("accounts", &self.accounts.is_some())
            .field("lex", &self.lex)
            .finish()
    }
}

impl<'a> OutputCtx<'a> {
    /// A context without account names, lexing definitions as MariaDB
    /// writes them.
    #[must_use]
    pub fn new(map: &'a dyn NameMap) -> Self {
        Self {
            map,
            accounts: None,
            lex: LexOptions::mariadb(),
        }
    }

    /// With an account resolver.
    #[must_use]
    pub fn accounts(mut self, accounts: &'a dyn AccountResolver) -> Self {
        self.accounts = Some(accounts);
        self
    }
}

/// Text after mapping, with the number of foreign names replaced by `?`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapped<'a> {
    /// The mapped text.
    pub text: Cow<'a, [u8]>,
    /// Physical names that were not the tenant's (or could not be
    /// resolved), replaced by `?`.
    pub foreign: usize,
}

const UNKNOWN: &str = "?";
const TABLES_IN: &[u8] = b"Tables_in_";

fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// What one identifier run maps to.
enum Repl {
    Keep,
    Logical(String),
    Foreign,
}

fn map_run(run: &str, ctx: &OutputCtx<'_>, owners: bool) -> Repl {
    let b = run.as_bytes();
    if b.len() >= 3 && b[0] == b't' && b[1].is_ascii_digit() {
        if let Ok(schema) = run.parse::<SchemaName>() {
            if schema.tenant() == ctx.map.tenant()
                && let Some(db) = ctx.map.logical(run)
            {
                return Repl::Logical(db.name.clone());
            }
            return Repl::Foreign;
        }
        if let Ok(account) = run.parse::<AccountName>() {
            if account.tenant() == ctx.map.tenant()
                && let Some(name) = ctx.accounts.and_then(|a| a.account_name(&account))
            {
                return Repl::Logical(name);
            }
            return Repl::Foreign;
        }
        return Repl::Keep;
    }
    if let Some(rest) = run.strip_prefix("Tables_in_")
        && rest.starts_with('t')
    {
        return match map_run(rest, ctx, false) {
            Repl::Logical(l) => Repl::Logical(format!("Tables_in_{l}")),
            Repl::Foreign => Repl::Logical(format!("Tables_in_{UNKNOWN}")),
            Repl::Keep => Repl::Keep,
        };
    }
    if owners && let Some(owner) = internal_owner(run) {
        return Repl::Logical(owner.to_owned());
    }
    Repl::Keep
}

/// The table or collection an internal table belongs to, for the patterns
/// whose name says so (writing guide 5.4).
fn internal_owner(name: &str) -> Option<&str> {
    for prefix in ["__sprig_lk_", "__sprig_ix_"] {
        if let Some(rest) = name.strip_prefix(prefix) {
            return rest
                .find("__")
                .map(|i| &rest[..i])
                .filter(|s| !s.is_empty());
        }
    }
    name.strip_prefix("__sprig_wx_").filter(|s| !s.is_empty())
}

/// Maps every physical name in `text` (see the module table), except
/// internal table names, which stay as they are: result values may list
/// them under `@sprig_show_internal`.
#[must_use]
pub fn map_bytes<'t>(text: &'t [u8], ctx: &OutputCtx<'_>) -> Mapped<'t> {
    scan(text, ctx, false)
}

fn scan<'t>(text: &'t [u8], ctx: &OutputCtx<'_>, owners: bool) -> Mapped<'t> {
    let mut out: Option<Vec<u8>> = None;
    let mut foreign = 0;
    let mut copied = 0;
    let mut i = 0;
    while i < text.len() {
        if !is_ident_byte(text[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < text.len() && is_ident_byte(text[i]) {
            i += 1;
        }
        let run = &text[start..i];
        let candidate = (run.len() >= 3 && run[0] == b't' && run[1].is_ascii_digit())
            || run.starts_with(TABLES_IN)
            || (owners && run.starts_with(b"__sprig_"));
        if !candidate {
            continue;
        }
        let Ok(run) = std::str::from_utf8(run) else {
            continue;
        };
        let repl = match map_run(run, ctx, owners) {
            Repl::Keep => continue,
            Repl::Logical(l) => l,
            Repl::Foreign => {
                foreign += 1;
                UNKNOWN.to_owned()
            }
        };
        let backquoted = start > 0 && text[start - 1] == b'`' && text.get(i) == Some(&b'`');
        let o = out.get_or_insert_with(|| Vec::with_capacity(text.len() + 16));
        o.extend_from_slice(&text[copied..start]);
        if backquoted {
            o.extend_from_slice(repl.replace('`', "``").as_bytes());
        } else {
            o.extend_from_slice(repl.as_bytes());
        }
        copied = i;
    }
    match out {
        Some(mut o) => {
            o.extend_from_slice(&text[copied..]);
            Mapped {
                text: Cow::Owned(o),
                foreign,
            }
        }
        None => Mapped {
            text: Cow::Borrowed(text),
            foreign,
        },
    }
}

/// [`map_message`] with the foreign-name count.
#[must_use]
pub fn map_text<'t>(msg: &'t str, ctx: &OutputCtx<'_>) -> (Cow<'t, str>, usize) {
    let m = scan(msg.as_bytes(), ctx, true);
    let text = match m.text {
        Cow::Borrowed(_) => Cow::Borrowed(msg),
        Cow::Owned(o) => Cow::Owned(
            String::from_utf8(o)
                .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()),
        ),
    };
    (text, m.foreign)
}

/// Maps the names in an error or warning message (chapter 07,
/// `map_message`): the tenant's schemas become logical names, accounts
/// become user names, internal tables their owning object, anything
/// foreign becomes `?`. Use [`map_text`] to also learn how many foreign
/// names were replaced.
#[must_use]
pub fn map_message<'t>(msg: &'t str, ctx: &OutputCtx<'_>) -> Cow<'t, str> {
    map_text(msg, ctx).0
}

/// Maps a column definition: `schema` to the logical database (empty for
/// a schema that is not the tenant's), and any physical name in `table`,
/// `org_table`, `name` and `org_name` (the `Tables_in_<schema>` header of
/// `SHOW TABLES`, for one). Returns the number of foreign names replaced.
pub fn map_column_def(cd: &mut ColumnDef, ctx: &OutputCtx<'_>) -> usize {
    let mut foreign = 0;
    if !cd.schema.is_empty()
        && let Ok(s) = std::str::from_utf8(&cd.schema)
        && s.parse::<SchemaName>().is_ok()
    {
        if let Some(db) = ctx.map.logical(s) {
            cd.schema = bytes::Bytes::from(db.name.clone());
        } else {
            foreign += 1;
            cd.schema = bytes::Bytes::new();
        }
    }
    for field in [
        &mut cd.table,
        &mut cd.org_table,
        &mut cd.name,
        &mut cd.org_name,
    ] {
        let m = map_bytes(field, ctx);
        if let Cow::Owned(o) = m.text {
            foreign += m.foreign;
            *field = bytes::Bytes::from(o);
        }
    }
    foreign
}

/// Every `t<digits>_` in `bytes` that starts an identifier or follows `_`
/// (`Tables_in_t42_...`): what a physical name looks like. The leak suite
/// (chapter 07 build step 10) asserts this is empty for every byte a
/// client can see.
#[must_use]
pub fn physical_name_leaks(bytes: &[u8]) -> Vec<String> {
    let mut found = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let starts = bytes[i] == b't'
            && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
            && bytes.get(i + 1).is_some_and(u8::is_ascii_digit);
        if starts {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if bytes.get(j) == Some(&b'_') {
                let mut k = j;
                while k < bytes.len() && is_ident_byte(bytes[k]) {
                    k += 1;
                }
                found.push(String::from_utf8_lossy(&bytes[i..k]).into_owned());
                i = k;
                continue;
            }
        }
        i += 1;
    }
    found
}

#[cfg(test)]
mod tests;
