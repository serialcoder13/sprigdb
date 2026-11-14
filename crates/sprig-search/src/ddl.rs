//! Search DDL in SQL.
//!
//! ```sql
//! CREATE SEARCH INDEX [IF NOT EXISTS] name ON [db.]object (
//!   column KIND [WITH (analyzer = 'english', boost = 2.0, filter = true, ...)],
//!   '$.path' KIND [WITH (...)],                 -- a collection's document
//!   VALUE ['$.path'] KIND [WITH (...)]          -- a key-value namespace's value
//! ) [WITH (analyzer = 'standard', partitions = 2)];
//! ALTER SEARCH INDEX name [ON [db.]object] ADD FIELD spec | DROP FIELD name | REBUILD;
//! DROP SEARCH INDEX [IF EXISTS] name [ON [db.]object];
//! SHOW SEARCH INDEXES [FROM | IN [db.]object];
//! CREATE SEARCH ANALYZER [IF NOT EXISTS] name WITH (tokenizer = 'standard',
//!   filters = ('lowercase', 'stem:english'), synonyms = '[["tv","television"]]');
//! DROP SEARCH ANALYZER [IF EXISTS] name;
//! CREATE FULLTEXT INDEX name ON [db.]table (columns);           -- MySQL dumps
//! ALTER TABLE [db.]table ADD FULLTEXT [INDEX | KEY] [name] (columns);
//! ```
//!
//! [`parse`] returns `None` for any other statement, so the gateway can
//! try it on every statement that mentions `SEARCH` or `FULLTEXT`.

use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};

use crate::analyze::CustomAnalyzer;
use crate::def::{FieldDef, FieldKind, Flavor, IndexDef, Model, Source};

/// A parsed search statement.
#[derive(Debug, Clone, PartialEq)]
pub enum SearchDdl {
    /// `CREATE SEARCH INDEX`; `def.db` is empty for the current database,
    /// `def.model` is `Table` until the object is known.
    Create {
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// The definition.
        def: IndexDef,
    },
    /// `ALTER SEARCH INDEX ... ADD FIELD`.
    AddField {
        /// Index name.
        name: String,
        /// Database, when given.
        db: Option<String>,
        /// The field.
        field: FieldDef,
    },
    /// `ALTER SEARCH INDEX ... DROP FIELD`.
    DropField {
        /// Index name.
        name: String,
        /// Database, when given.
        db: Option<String>,
        /// The field's name.
        field: String,
    },
    /// `ALTER SEARCH INDEX ... REBUILD`.
    Rebuild {
        /// Index name.
        name: String,
        /// Database, when given.
        db: Option<String>,
    },
    /// `DROP SEARCH INDEX`.
    Drop {
        /// `IF EXISTS`.
        if_exists: bool,
        /// Index name.
        name: String,
        /// Database, when given.
        db: Option<String>,
    },
    /// `SHOW SEARCH INDEXES`.
    Show {
        /// Database, when given.
        db: Option<String>,
        /// Object, when given.
        object: Option<String>,
    },
    /// `CREATE SEARCH ANALYZER`.
    CreateAnalyzer {
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// The analyzer (version 1).
        analyzer: CustomAnalyzer,
    },
    /// `DROP SEARCH ANALYZER`.
    DropAnalyzer {
        /// `IF EXISTS`.
        if_exists: bool,
        /// Its name.
        name: String,
    },
    /// `CREATE FULLTEXT INDEX` or `ALTER TABLE ... ADD FULLTEXT`.
    Fulltext {
        /// The index name (generated when absent).
        name: Option<String>,
        /// Database, when given.
        db: Option<String>,
        /// The table.
        table: String,
        /// Its columns.
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum T {
    Id(String),
    Quoted(String),
    Str(String),
    Num(String),
    P(char),
}

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, msg)
}

fn lex(s: &str) -> Result<Vec<T>> {
    let mut out = Vec::new();
    let b: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && b.get(i + 1) == Some(&'-') {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && b.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == '*' && b[i + 1] == '/') {
                i += 1;
            }
            i += 2;
        } else if c == '`' || c == '\'' || c == '"' {
            let q = c;
            let mut v = String::new();
            i += 1;
            loop {
                let Some(&x) = b.get(i) else {
                    return Err(syntax("unterminated quote"));
                };
                if x == '\\' && q != '`' {
                    if let Some(&n) = b.get(i + 1) {
                        v.push(match n {
                            'n' => '\n',
                            't' => '\t',
                            '0' => '\0',
                            o => o,
                        });
                    }
                    i += 2;
                    continue;
                }
                if x == q {
                    if b.get(i + 1) == Some(&q) {
                        v.push(q);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(x);
                i += 1;
            }
            out.push(if q == '`' { T::Quoted(v) } else { T::Str(v) });
        } else if c.is_ascii_digit() {
            let mut v = String::new();
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == '.') {
                v.push(b[i]);
                i += 1;
            }
            out.push(T::Num(v));
        } else if c.is_alphanumeric() || c == '_' || c == '$' {
            let mut v = String::new();
            while i < b.len() && (b[i].is_alphanumeric() || b[i] == '_' || b[i] == '$') {
                v.push(b[i]);
                i += 1;
            }
            out.push(T::Id(v));
        } else {
            out.push(T::P(c));
            i += 1;
        }
    }
    while out.last() == Some(&T::P(';')) {
        out.pop();
    }
    Ok(out)
}

struct P {
    t: Vec<T>,
    i: usize,
}

impl P {
    fn kw(&self, w: &str) -> bool {
        matches!(self.t.get(self.i), Some(T::Id(x)) if x.eq_ignore_ascii_case(w))
    }

    fn eat(&mut self, w: &str) -> bool {
        if self.kw(w) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, w: &str) -> Result<()> {
        if self.eat(w) {
            Ok(())
        } else {
            Err(syntax(format!("expected {w}")))
        }
    }

    fn punct(&mut self, c: char) -> bool {
        if self.t.get(self.i) == Some(&T::P(c)) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn need(&mut self, c: char) -> Result<()> {
        if self.punct(c) {
            Ok(())
        } else {
            Err(syntax(format!("expected '{c}'")))
        }
    }

    fn ident(&mut self) -> Result<String> {
        match self.t.get(self.i).cloned() {
            Some(T::Id(x) | T::Quoted(x)) => {
                self.i += 1;
                Ok(x)
            }
            _ => Err(syntax("expected a name")),
        }
    }

    /// `[db.]name`.
    fn qualified(&mut self) -> Result<(Option<String>, String)> {
        let a = self.ident()?;
        if self.punct('.') {
            Ok((Some(a), self.ident()?))
        } else {
            Ok((None, a))
        }
    }

    fn done(&self) -> Result<()> {
        if self.i < self.t.len() {
            Err(syntax(format!("unexpected {:?}", self.t[self.i])))
        } else {
            Ok(())
        }
    }

    /// `WITH (k = v, ...)`: values are strings, numbers, true/false or a
    /// parenthesized list of strings.
    fn options(&mut self) -> Result<Vec<(String, Json)>> {
        let mut out = Vec::new();
        if !self.eat("WITH") {
            return Ok(out);
        }
        self.need('(')?;
        loop {
            let k = self.ident()?.to_ascii_lowercase();
            self.need('=')?;
            let v = match self.t.get(self.i).cloned() {
                Some(T::Str(s)) => {
                    self.i += 1;
                    Json::String(s)
                }
                Some(T::Num(n)) => {
                    self.i += 1;
                    serde_json::from_str(&n).map_err(|_| syntax(format!("bad number {n}")))?
                }
                Some(T::Id(w)) if w.eq_ignore_ascii_case("true") => {
                    self.i += 1;
                    Json::Bool(true)
                }
                Some(T::Id(w)) if w.eq_ignore_ascii_case("false") => {
                    self.i += 1;
                    Json::Bool(false)
                }
                Some(T::Id(w)) => {
                    self.i += 1;
                    Json::String(w)
                }
                Some(T::P('(')) => {
                    self.i += 1;
                    let mut l = Vec::new();
                    loop {
                        match self.t.get(self.i).cloned() {
                            Some(T::Str(s) | T::Id(s)) => {
                                self.i += 1;
                                l.push(Json::String(s));
                            }
                            _ => return Err(syntax("expected a string in the list")),
                        }
                        if !self.punct(',') {
                            break;
                        }
                    }
                    self.need(')')?;
                    Json::Array(l)
                }
                _ => return Err(syntax(format!("bad value for option {k}"))),
            };
            out.push((k, v));
            if !self.punct(',') {
                break;
            }
        }
        self.need(')')?;
        Ok(out)
    }

    fn field(&mut self) -> Result<FieldDef> {
        let (source, name) = if self.eat("VALUE") {
            match self.t.get(self.i).cloned() {
                Some(T::Str(p)) => {
                    self.i += 1;
                    (Source::KvValue, path_of(&p))
                }
                _ => (Source::KvValue, String::new()),
            }
        } else {
            match self.t.get(self.i).cloned() {
                Some(T::Str(p)) => {
                    self.i += 1;
                    (Source::Path, path_of(&p))
                }
                _ => (Source::Column, self.ident()?),
            }
        };
        let kw = self.ident()?;
        let kind = FieldKind::parse(&kw)
            .ok_or_else(|| syntax(format!("unknown search field kind {kw}")))?;
        let mut f = FieldDef::new(&name, source, kind);
        for (k, v) in self.options()? {
            match k.as_str() {
                "analyzer" => f.analyzer = v.as_str().map(crate::analyze::from_atlas),
                "query_analyzer" | "search_analyzer" => {
                    f.query_analyzer = v.as_str().map(crate::analyze::from_atlas);
                }
                "boost" => {
                    f.boost = v
                        .as_f64()
                        .filter(|b| *b > 0.0)
                        .ok_or_else(|| syntax("boost must be a positive number"))?;
                }
                "filter" => f.filter = v.as_bool().unwrap_or(false),
                "sort" => f.sort = v.as_bool().unwrap_or(false),
                "facet" => f.facet = v.as_bool().unwrap_or(false),
                other => return Err(syntax(format!("unknown field option {other}"))),
            }
        }
        if f.kind == FieldKind::Facet {
            f.facet = true;
        }
        Ok(f)
    }
}

/// `$.a.b` or `a.b` as a dotted path.
fn path_of(p: &str) -> String {
    p.strip_prefix("$.")
        .or_else(|| p.strip_prefix('$'))
        .unwrap_or(p)
        .to_owned()
}

/// Parses `sql` when it is a search statement.
///
/// # Errors
/// 39822 for a malformed search statement.
pub fn parse(sql: &str) -> Result<Option<SearchDdl>> {
    let t = lex(sql)?;
    let mut p = P { t, i: 0 };
    let r = if p.eat("CREATE") {
        if p.eat("SEARCH") {
            if p.eat("INDEX") {
                create_index(&mut p)?
            } else if p.eat("ANALYZER") {
                create_analyzer(&mut p)?
            } else {
                return Err(syntax("expected INDEX or ANALYZER after CREATE SEARCH"));
            }
        } else if p.eat("FULLTEXT") {
            p.expect("INDEX")?;
            let name = p.ident()?;
            p.expect("ON")?;
            let (db, table) = p.qualified()?;
            let columns = columns(&mut p)?;
            if p.kw("WITH") {
                return Err(SprigError::new(
                    SprigCode::SearchUnsupported,
                    "FULLTEXT parsers are not supported",
                ));
            }
            SearchDdl::Fulltext {
                name: Some(name),
                db,
                table,
                columns,
            }
        } else {
            return Ok(None);
        }
    } else if p.eat("ALTER") {
        if p.eat("SEARCH") {
            p.expect("INDEX")?;
            let (db, name) = p.qualified()?;
            if p.eat("ON") {
                p.qualified()?;
            }
            if p.eat("ADD") {
                p.expect("FIELD")?;
                SearchDdl::AddField {
                    name,
                    db,
                    field: p.field()?,
                }
            } else if p.eat("DROP") {
                p.expect("FIELD")?;
                SearchDdl::DropField {
                    name,
                    db,
                    field: p.ident()?,
                }
            } else if p.eat("REBUILD") {
                SearchDdl::Rebuild { name, db }
            } else {
                return Err(syntax("expected ADD FIELD, DROP FIELD or REBUILD"));
            }
        } else if p.eat("TABLE") {
            let (db, table) = p.qualified()?;
            if !p.eat("ADD") || !p.eat("FULLTEXT") {
                return Ok(None);
            }
            let _ = p.eat("INDEX") || p.eat("KEY");
            let name = if p.t.get(p.i) == Some(&T::P('(')) {
                None
            } else {
                Some(p.ident()?)
            };
            let columns = columns(&mut p)?;
            if p.i < p.t.len() {
                // Other clauses after it: MariaDB runs the statement whole.
                return Ok(None);
            }
            SearchDdl::Fulltext {
                name,
                db,
                table,
                columns,
            }
        } else {
            return Ok(None);
        }
    } else if p.eat("DROP") {
        if !p.eat("SEARCH") {
            return Ok(None);
        }
        if p.eat("ANALYZER") {
            let if_exists = if p.eat("IF") {
                p.expect("EXISTS")?;
                true
            } else {
                false
            };
            SearchDdl::DropAnalyzer {
                if_exists,
                name: p.ident()?,
            }
        } else {
            p.expect("INDEX")?;
            let if_exists = if p.eat("IF") {
                p.expect("EXISTS")?;
                true
            } else {
                false
            };
            let (mut db, name) = p.qualified()?;
            if p.eat("ON") {
                let (d, _) = p.qualified()?;
                db = db.or(d);
            }
            SearchDdl::Drop {
                if_exists,
                name,
                db,
            }
        }
    } else if p.eat("SHOW") {
        if !p.eat("SEARCH") {
            return Ok(None);
        }
        if !(p.eat("INDEXES") || p.eat("INDEX")) {
            return Err(syntax("expected INDEXES"));
        }
        let (db, object) = if p.eat("FROM") || p.eat("IN") {
            let (d, o) = p.qualified()?;
            (d, Some(o))
        } else {
            (None, None)
        };
        SearchDdl::Show { db, object }
    } else {
        return Ok(None);
    };
    p.done()?;
    Ok(Some(r))
}

fn columns(p: &mut P) -> Result<Vec<String>> {
    p.need('(')?;
    let mut v = Vec::new();
    loop {
        v.push(p.ident()?);
        // A prefix length `(col(10))` is ignored.
        if p.punct('(') {
            while !p.punct(')') {
                if p.i >= p.t.len() {
                    return Err(syntax("unbalanced '('"));
                }
                p.i += 1;
            }
        }
        if !p.punct(',') {
            break;
        }
    }
    p.need(')')?;
    Ok(v)
}

fn create_index(p: &mut P) -> Result<SearchDdl> {
    let if_not_exists = if p.eat("IF") {
        p.expect("NOT")?;
        p.expect("EXISTS")?;
        true
    } else {
        false
    };
    let name = p.ident()?;
    p.expect("ON")?;
    let (db, object) = p.qualified()?;
    p.need('(')?;
    let mut def = IndexDef::new(
        &name,
        db.as_deref().unwrap_or(""),
        &object,
        Model::Table,
        Flavor::Sql,
    );
    loop {
        let f = p.field()?;
        def.push(f)?;
        if !p.punct(',') {
            break;
        }
    }
    p.need(')')?;
    for (k, v) in p.options()? {
        match k.as_str() {
            "analyzer" => {
                def.default_analyzer = v
                    .as_str()
                    .map(crate::analyze::from_atlas)
                    .ok_or_else(|| syntax("analyzer must be a string"))?;
            }
            "partitions" => {
                def.partitions = v
                    .as_u64()
                    .and_then(|n| u32::try_from(n).ok())
                    .filter(|n| (1..=64).contains(n))
                    .ok_or_else(|| syntax("partitions must be 1 to 64"))?;
            }
            "dynamic" => def.dynamic = v.as_bool().unwrap_or(false),
            other => return Err(syntax(format!("unknown search index option {other}"))),
        }
    }
    if def.fields.iter().any(|f| f.source != Source::Column) {
        def.model = if def.fields.iter().any(|f| f.source == Source::KvValue) {
            Model::Kv
        } else {
            Model::Collection
        };
        def.key_columns = match def.model {
            Model::Kv => vec!["k".to_owned()],
            _ => vec!["_id".to_owned()],
        };
    }
    Ok(SearchDdl::Create { if_not_exists, def })
}

fn create_analyzer(p: &mut P) -> Result<SearchDdl> {
    let if_not_exists = if p.eat("IF") {
        p.expect("NOT")?;
        p.expect("EXISTS")?;
        true
    } else {
        false
    };
    let name = p.ident()?;
    if crate::analyze::is_builtin(&name) {
        return Err(syntax(format!("'{name}' is a built-in analyzer")));
    }
    let mut a = CustomAnalyzer {
        name,
        tokenizer: "standard".to_owned(),
        tokenizer_opts: Json::Object(serde_json::Map::new()),
        filters: Vec::new(),
        synonyms: Vec::new(),
        version: 1,
    };
    for (k, v) in p.options()? {
        match k.as_str() {
            "tokenizer" => v
                .as_str()
                .unwrap_or("standard")
                .clone_into(&mut a.tokenizer),
            "min_gram" | "max_gram" => {
                if let Json::Object(m) = &mut a.tokenizer_opts {
                    m.insert(k, v);
                }
            }
            "filters" => {
                a.filters = match v {
                    Json::Array(l) => l
                        .into_iter()
                        .filter_map(|x| x.as_str().map(str::to_owned))
                        .collect(),
                    Json::String(s) => vec![s],
                    _ => return Err(syntax("filters must be a list of strings")),
                };
            }
            "synonyms" => {
                let s = v
                    .as_str()
                    .ok_or_else(|| syntax("synonyms must be a JSON string"))?;
                a.synonyms = serde_json::from_str(s)
                    .map_err(|e| syntax(format!("synonyms must be a JSON array of arrays: {e}")))?;
            }
            other => return Err(syntax(format!("unknown analyzer option {other}"))),
        }
    }
    crate::analyze::custom(&a)?;
    Ok(SearchDdl::CreateAnalyzer {
        if_not_exists,
        analyzer: a,
    })
}

/// `FULLTEXT` keys inside a `CREATE TABLE`, taken out of it:
/// the statement without them, the table, and each key's name and columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableFulltext {
    /// The `CREATE TABLE` without its `FULLTEXT` keys.
    pub stripped: String,
    /// Database, when given.
    pub db: Option<String>,
    /// The table.
    pub table: String,
    /// Each key: its name (when given) and columns.
    pub keys: Vec<(Option<String>, Vec<String>)>,
}

/// Byte offsets of a SQL text's top-level structure, skipping strings,
/// quoted names and comments.
fn skip_quoted(b: &[u8], mut i: usize) -> usize {
    let q = b[i];
    i += 1;
    while i < b.len() {
        if b[i] == b'\\' && q != b'`' {
            i += 2;
            continue;
        }
        if b[i] == q {
            if b.get(i + 1) == Some(&q) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    b.len()
}

fn skip_comment(b: &[u8], i: usize) -> Option<usize> {
    if b[i] == b'#' || (b[i] == b'-' && b.get(i + 1) == Some(&b'-')) {
        let mut j = i;
        while j < b.len() && b[j] != b'\n' {
            j += 1;
        }
        return Some(j);
    }
    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
        let mut j = i + 2;
        while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
            j += 1;
        }
        return Some((j + 2).min(b.len()));
    }
    None
}

/// `CREATE TABLE ... ( ..., FULLTEXT [INDEX | KEY] [name] (cols), ... )`:
/// `None` for any other statement, or one without `FULLTEXT` keys.
///
/// # Errors
/// 39823 for `WITH PARSER`; 39822 for a malformed key.
pub fn create_table_fulltext(sql: &str) -> Result<Option<CreateTableFulltext>> {
    let b = sql.as_bytes();
    // The head up to the element list: CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name.
    let mut open = None;
    let mut i = 0;
    while i < b.len() {
        if let Some(j) = skip_comment(b, i) {
            i = j;
            continue;
        }
        match b[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            b'(' => {
                open = Some(i);
                break;
            }
            _ => i += 1,
        }
    }
    let Some(open) = open else { return Ok(None) };
    let head = lex(&sql[..open])?;
    let mut p = P { t: head, i: 0 };
    if !p.eat("CREATE") {
        return Ok(None);
    }
    if p.eat("OR") && !p.eat("REPLACE") {
        return Ok(None);
    }
    if !p.eat("TABLE") {
        return Ok(None);
    }
    if p.eat("IF") && !(p.eat("NOT") && p.eat("EXISTS")) {
        return Ok(None);
    }
    let Ok((db, table)) = p.qualified() else {
        return Ok(None);
    };
    if p.i < p.t.len() {
        return Ok(None);
    }
    // The elements, split at top-level commas.
    let mut depth = 0usize;
    let mut start = open + 1;
    let mut elems: Vec<(usize, usize)> = Vec::new();
    let mut close = None;
    let mut i = open + 1;
    while i < b.len() {
        if let Some(j) = skip_comment(b, i) {
            i = j;
            continue;
        }
        match b[i] {
            b'\'' | b'"' | b'`' => {
                i = skip_quoted(b, i);
                continue;
            }
            b'(' => depth += 1,
            b')' if depth == 0 => {
                elems.push((start, i));
                close = Some(i);
                break;
            }
            b')' => depth -= 1,
            b',' if depth == 0 => {
                elems.push((start, i));
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let Some(close) = close else { return Ok(None) };
    let mut keys = Vec::new();
    let mut kept: Vec<&str> = Vec::new();
    for (a, z) in &elems {
        let e = &sql[*a..*z];
        let w = e.trim_start();
        let fulltext = w.len() > 8
            && w.as_bytes()[..8].eq_ignore_ascii_case(b"FULLTEXT")
            && !w.as_bytes()[8].is_ascii_alphanumeric()
            && w.as_bytes()[8] != b'_';
        if !fulltext {
            kept.push(e);
            continue;
        }
        let mut q = P { t: lex(e)?, i: 0 };
        q.eat("FULLTEXT");
        let _ = q.eat("INDEX") || q.eat("KEY");
        let name = if q.t.get(q.i) == Some(&T::P('(')) {
            None
        } else {
            Some(q.ident()?)
        };
        let cols = columns(&mut q)?;
        if q.kw("WITH") {
            return Err(SprigError::new(
                SprigCode::SearchUnsupported,
                "FULLTEXT parsers are not supported",
            ));
        }
        keys.push((name, cols));
    }
    if keys.is_empty() {
        return Ok(None);
    }
    if kept.is_empty() {
        return Err(syntax("a CREATE TABLE of FULLTEXT keys only"));
    }
    let stripped = format!("{}({}){}", &sql[..open], kept.join(","), &sql[close + 1..]);
    Ok(Some(CreateTableFulltext {
        stripped,
        db,
        table,
        keys,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_index() {
        let Some(SearchDdl::Create { if_not_exists, def }) = parse(
            "CREATE SEARCH INDEX IF NOT EXISTS product_text ON shop.products (
               name TEXT WITH (analyzer = 'english', boost = 2.0),
               description TEXT,
               brand KEYWORD WITH (facet = true),
               price NUMBER WITH (filter = true, sort = true)
             ) WITH (partitions = 3);",
        )
        .unwrap() else {
            panic!()
        };
        assert!(if_not_exists);
        assert_eq!(def.db, "shop");
        assert_eq!(def.model, Model::Table);
        assert_eq!(def.partitions, 3);
        assert_eq!(def.fields.len(), 4);
        assert_eq!(def.fields[0].analyzer.as_deref(), Some("english"));
        assert!(def.fields[3].sort);
    }

    #[test]
    fn collection_and_kv_fields() {
        let Some(SearchDdl::Create { def, .. }) = parse(
            "CREATE SEARCH INDEX order_notes ON orders ('$.notes' TEXT, '$.customer.name' AUTOCOMPLETE, '$.tags' KEYWORD WITH (facet = true))",
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(def.model, Model::Collection);
        assert_eq!(def.fields[1].name, "customer.name");
        let Some(SearchDdl::Create { def, .. }) =
            parse("CREATE SEARCH INDEX note_text ON sessions (VALUE '$.note' TEXT)").unwrap()
        else {
            panic!()
        };
        assert_eq!(def.model, Model::Kv);
    }

    #[test]
    fn other_statements() {
        assert_eq!(
            parse("DROP SEARCH INDEX IF EXISTS product_text ON products").unwrap(),
            Some(SearchDdl::Drop {
                if_exists: true,
                name: "product_text".into(),
                db: None
            })
        );
        assert_eq!(
            parse("SHOW SEARCH INDEXES FROM shop.products").unwrap(),
            Some(SearchDdl::Show {
                db: Some("shop".into()),
                object: Some("products".into())
            })
        );
        assert!(matches!(
            parse("ALTER SEARCH INDEX product_text ADD FIELD color KEYWORD").unwrap(),
            Some(SearchDdl::AddField { .. })
        ));
        assert_eq!(
            parse("ALTER TABLE t ADD FULLTEXT KEY ft (title, body)").unwrap(),
            Some(SearchDdl::Fulltext {
                name: Some("ft".into()),
                db: None,
                table: "t".into(),
                columns: vec!["title".into(), "body".into()]
            })
        );
        assert_eq!(parse("ALTER TABLE t ADD COLUMN x INT").unwrap(), None);
        assert_eq!(parse("SELECT 1").unwrap(), None);
        assert!(parse("CREATE SEARCH INDEX x ON t (a BLAH)").is_err());
    }

    #[test]
    fn analyzers() {
        let Some(SearchDdl::CreateAnalyzer { analyzer, .. }) = parse(
            r#"CREATE SEARCH ANALYZER en_syn WITH (tokenizer = 'standard',
               filters = ('lowercase', 'ascii_folding', 'stop:english', 'stem:english'),
               synonyms = '[["tv","television"],["laptop","notebook"]]')"#,
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(analyzer.filters.len(), 4);
        assert_eq!(analyzer.synonyms.len(), 2);
        assert!(parse("CREATE SEARCH ANALYZER english WITH (tokenizer = 'standard')").is_err());
    }

    #[test]
    fn fulltext_keys_leave_create_table() {
        let sql = "CREATE TABLE IF NOT EXISTS shop.articles (\n  id INT PRIMARY KEY,\n  title VARCHAR(200) COMMENT 'a, (b)',\n  body TEXT,\n  FULLTEXT KEY ft_tb (title, body),\n  KEY by_title (title),\n  FULLTEXT (body)\n) ENGINE=InnoDB /* , FULLTEXT (x) */";
        let c = create_table_fulltext(sql).unwrap().unwrap();
        assert_eq!(c.db.as_deref(), Some("shop"));
        assert_eq!(c.table, "articles");
        assert_eq!(
            c.keys,
            vec![
                (Some("ft_tb".into()), vec!["title".into(), "body".into()]),
                (None, vec!["body".into()])
            ]
        );
        assert!(!c.stripped.to_ascii_uppercase().contains("FULLTEXT KEY"));
        assert!(c.stripped.contains("KEY by_title (title)"));
        assert!(c.stripped.contains("COMMENT 'a, (b)'"));
        assert!(c.stripped.ends_with(") ENGINE=InnoDB /* , FULLTEXT (x) */"));
        assert_eq!(
            create_table_fulltext("CREATE TABLE t (id INT, KEY k (id))").unwrap(),
            None
        );
        assert_eq!(
            create_table_fulltext("CREATE TABLE t LIKE u").unwrap(),
            None
        );
        assert_eq!(
            create_table_fulltext("SELECT 'FULLTEXT (a)'").unwrap(),
            None
        );
        assert!(
            create_table_fulltext("CREATE TABLE t (a TEXT, FULLTEXT (a) WITH PARSER ngram)")
                .is_err()
        );
    }
}
