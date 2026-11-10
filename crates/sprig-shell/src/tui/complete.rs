//! Completion from the catalog (chapter 53, "Completion and
//! highlighting").

use std::collections::BTreeMap;

use serde_json::Value;

use crate::lang::{CYPHER_KEYWORDS, Lang, REDIS_COMMANDS, SQL_KEYWORDS, Tok, lex};

/// What the shell knows of the catalog.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    /// Databases.
    pub databases: Vec<String>,
    /// Objects per database.
    pub objects: BTreeMap<String, Vec<String>>,
    /// Columns per (database, table).
    pub columns: BTreeMap<(String, String), Vec<String>>,
}

impl Catalog {
    /// Records a database's snapshot.
    pub fn set_snapshot(&mut self, db: &str, v: &Value, extra: &[String]) {
        let mut objs: Vec<String> = v
            .get("objects")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|o| o.get("name").and_then(Value::as_str).map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        objs.extend(extra.iter().cloned());
        self.objects.insert(db.to_owned(), objs);
        if let Some(m) = v.get("columns").and_then(Value::as_object) {
            for (t, cols) in m {
                let names = cols
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| {
                                c.get("name").and_then(Value::as_str).map(str::to_owned)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                self.columns.insert((db.to_owned(), t.clone()), names);
            }
        }
    }
}

fn matches(cands: impl IntoIterator<Item = String>, prefix: &str) -> Vec<String> {
    let p = prefix.to_ascii_lowercase();
    let mut v: Vec<String> = cands
        .into_iter()
        .filter(|c| c.to_ascii_lowercase().starts_with(&p) && !c.eq_ignore_ascii_case(prefix))
        .collect();
    v.sort_by_key(|c| c.to_ascii_lowercase());
    v.dedup();
    v.truncate(50);
    v
}

fn keyword_case(k: &str, typed: &str) -> String {
    if typed.chars().any(char::is_lowercase) {
        k.to_ascii_lowercase()
    } else {
        k.to_owned()
    }
}

/// Candidates for the word before the cursor: what replaces `word`.
#[must_use]
pub fn complete(
    cat: &Catalog,
    lang: Lang,
    db: Option<&str>,
    statement: &str,
    word: &str,
) -> Vec<String> {
    match lang {
        Lang::Redis => {
            if statement.trim_start().len() > word.len() {
                return Vec::new();
            }
            matches(
                REDIS_COMMANDS.iter().map(|(c, _)| keyword_case(c, word)),
                word,
            )
        }
        Lang::Cypher => matches(CYPHER_KEYWORDS.iter().map(|k| keyword_case(k, word)), word),
        Lang::Mongo => complete_mongo(cat, db, word),
        Lang::Sql => complete_sql(cat, db, statement, word),
    }
}

fn complete_mongo(cat: &Catalog, db: Option<&str>, word: &str) -> Vec<String> {
    let parts: Vec<&str> = word.split('.').collect();
    let objects = db
        .and_then(|d| cat.objects.get(d))
        .cloned()
        .unwrap_or_default();
    match parts.as_slice() {
        ["db", c] => matches(
            objects.into_iter().map(|o| format!("db.{o}")),
            &format!("db.{c}"),
        ),
        ["db", coll, m] => {
            let methods = [
                "find",
                "findOne",
                "aggregate",
                "countDocuments",
                "insertOne",
                "insertMany",
                "updateOne",
                "updateMany",
                "replaceOne",
                "deleteOne",
                "deleteMany",
                "distinct",
                "createIndex",
                "getIndexes",
                "drop",
            ];
            matches(
                methods.iter().map(|x| format!("db.{coll}.{x}")),
                &format!("db.{coll}.{m}"),
            )
        }
        _ => matches(["db".to_owned()], word),
    }
}

fn complete_sql(cat: &Catalog, db: Option<&str>, statement: &str, word: &str) -> Vec<String> {
    // The keyword before the word being typed.
    let before = &statement[..statement.len().saturating_sub(word.len())];
    let toks = lex(before, false);
    let prev = toks
        .iter()
        .rev()
        .find(|t| t.kind != Tok::Space && t.kind != Tok::Comment)
        .map(|t| t.text(before).to_ascii_uppercase())
        .unwrap_or_default();
    if let Some((left, right)) = word.rsplit_once('.') {
        let left_bare = left.trim_matches('`');
        let mut out = Vec::new();
        if let Some(objs) = cat.objects.get(left_bare) {
            out.extend(objs.iter().map(|o| format!("{left}.{o}")));
        }
        if let Some(d) = db
            && let Some(cols) = cat.columns.get(&(d.to_owned(), left_bare.to_owned()))
        {
            out.extend(cols.iter().map(|c| format!("{left}.{c}")));
        }
        let _ = right;
        return matches(out, word);
    }
    let objects: Vec<String> = db
        .and_then(|d| cat.objects.get(d))
        .cloned()
        .unwrap_or_default();
    match prev.as_str() {
        "USE" => matches(cat.databases.clone(), word),
        "FROM" | "JOIN" | "INTO" | "UPDATE" | "TABLE" | "DESCRIBE" | "DESC" | "TRUNCATE" => {
            let mut c = objects;
            c.extend(cat.databases.iter().cloned());
            matches(c, word)
        }
        _ => {
            // Columns of the tables named in the statement, then keywords
            // and objects.
            let mut c: Vec<String> = Vec::new();
            if let Some(d) = db {
                let words: Vec<String> = lex(statement, false)
                    .iter()
                    .filter(|t| matches!(t.kind, Tok::Word | Tok::Quoted))
                    .map(|t| t.text(statement).trim_matches('`').to_owned())
                    .collect();
                for w in &words {
                    if let Some(cols) = cat.columns.get(&(d.to_owned(), w.clone())) {
                        c.extend(cols.iter().cloned());
                    }
                }
            }
            if word.is_empty() {
                return Vec::new();
            }
            // Columns first, then keywords and objects.
            let mut out = matches(c, word);
            let mut rest: Vec<String> =
                SQL_KEYWORDS.iter().map(|k| keyword_case(k, word)).collect();
            rest.extend(objects);
            for x in matches(rest, word) {
                if !out.contains(&x) {
                    out.push(x);
                }
            }
            out.truncate(50);
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cat() -> Catalog {
        let mut c = Catalog {
            databases: vec!["shop".into(), "hr".into()],
            ..Catalog::default()
        };
        c.set_snapshot(
            "shop",
            &json!({"objects": [{"name": "orders"}, {"name": "order_items"}],
                    "columns": {"orders": [{"name": "order_id"}, {"name": "status"}]}}),
            &["order_events".to_owned()],
        );
        c
    }

    #[test]
    fn sql() {
        let c = cat();
        assert_eq!(
            complete(&c, Lang::Sql, Some("shop"), "SELECT * FROM ord", "ord"),
            ["order_events", "order_items", "orders"]
        );
        assert_eq!(
            complete(&c, Lang::Sql, Some("shop"), "USE s", "s"),
            ["shop"]
        );
        assert_eq!(
            complete(&c, Lang::Sql, Some("shop"), "SELECT st FROM orders", "st")[0],
            "status"
        );
        assert_eq!(
            complete(
                &c,
                Lang::Sql,
                Some("shop"),
                "SELECT * FROM shop.orders_",
                "shop.orders_"
            ),
            Vec::<String>::new()
        );
        assert_eq!(
            complete(&c, Lang::Sql, Some("shop"), "SELECT orders.s", "orders.s"),
            ["orders.status"]
        );
        assert_eq!(complete(&c, Lang::Sql, None, "sel", "sel"), ["select"]);
    }

    #[test]
    fn other_languages() {
        let c = cat();
        assert_eq!(
            complete(&c, Lang::Redis, None, "hge", "hge"),
            ["hget", "hgetall"]
        );
        assert!(complete(&c, Lang::Redis, None, "GET hge", "hge").is_empty());
        assert_eq!(
            complete(&c, Lang::Mongo, Some("shop"), "db.or", "db.or"),
            ["db.order_events", "db.order_items", "db.orders"]
        );
        assert_eq!(
            complete(
                &c,
                Lang::Mongo,
                Some("shop"),
                "db.orders.fi",
                "db.orders.fi"
            ),
            ["db.orders.find", "db.orders.findOne"]
        );
        assert_eq!(complete(&c, Lang::Cypher, None, "MAT", "MAT"), ["MATCH"]);
    }
}
