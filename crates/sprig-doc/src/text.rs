//! `$text`, `$search` and `$searchMeta` (chapter 39, "Translating
//! queries", MongoDB; R5 round 5).
//!
//! Search indexes live outside the engine: the gateway's search service
//! keeps them ([`TextSearch`]) and answers a call with keys and scores.
//! The engine then reads those documents through the user's session (so
//! privileges, and row-level security once it covers collections, apply
//! to what is returned), rechecks the rest of the filter, and carries
//! each score in the hidden field [`crate::engine::models::SCORE_FIELD`]
//! for `{$meta: "textScore"}` and `{$meta: "searchScore"}`.
//!
//! ```text
//! find({$text: {$search: s}, ...rest}):
//!   hits := text index's search for s (27 without a text index)
//!   docs := documents whose _id is a hit and that match rest (pages of 500 keys)
//!   score each; sort (a $meta sort key sorts by score); skip, limit, project
//! aggregate([{$match: {$text: ...}}, ...]): the same for the first stage
//! aggregate([{$search: {...}}, ...]): hits in score order, documents in that order
//! aggregate([{$searchMeta: {...}}]): one document: count and facets
//! ```

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::engine::models::SCORE_FIELD;
use crate::engine::{Engine, Ns, SqlRunner, mongo_error};
use crate::meta::CollMeta;
use crate::sql::{bin_lit, ident};
use crate::translate::translate;
use crate::{filter, json};

/// The hidden field carrying `{$meta: "searchHighlights"}`.
pub const HIGHLIGHT_FIELD: &str = "__sprig_search_highlights";

/// Keys per `IN (...)` page when reading hits.
const PAGE: usize = 500;

/// What kind of index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextKind {
    /// A `text` index (`createIndexes`).
    Text,
    /// A search index (`createSearchIndexes`, `type: "search"`).
    Search,
}

/// A text or search index as `listIndexes` / `$listSearchIndexes` show it.
#[derive(Debug, Clone, PartialEq)]
pub struct TextIndexInfo {
    /// Its name.
    pub name: String,
    /// Its id (search indexes).
    pub id: String,
    /// Which kind.
    pub kind: TextKind,
    /// `listIndexes`' document (text) or the definition (search).
    pub spec: Document,
    /// The index is seeded and searchable here.
    pub ready: bool,
}

/// An index to create.
#[derive(Debug, Clone, PartialEq)]
pub enum TextCreate {
    /// `createIndexes` with a `"text"` key: the whole index spec.
    Text(Document),
    /// `createSearchIndexes` with `type: "search"`: name and definition.
    Search {
        /// The name.
        name: String,
        /// The definition.
        definition: Document,
    },
}

/// A search to run.
#[derive(Debug, Clone, PartialEq)]
pub enum TextCall {
    /// `$text`'s argument.
    Text(Document),
    /// A `$search` stage's argument.
    Search(Document),
    /// A `$searchMeta` stage's argument.
    Meta(Document),
}

/// A search's answer.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TextHits {
    /// Encoded `_id`s and scores, best first.
    pub hits: Vec<(Vec<u8>, f64)>,
    /// `$searchMeta`'s document.
    pub meta: Option<Document>,
    /// Highlights by encoded `_id` (`highlight` option).
    pub highlights: HashMap<Vec<u8>, Bson>,
}

/// The gateway's search indexes on collections.
#[async_trait]
pub trait TextSearch: Send + Sync + Debug {
    /// Creates an index; the existing one when an equal one exists.
    async fn create(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        req: TextCreate,
    ) -> Result<TextIndexInfo, MongoError>;

    /// Replaces a search index's definition; `false` when it doesn't exist.
    async fn update(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: &str,
        definition: &Document,
    ) -> Result<bool, MongoError>;

    /// Drops an index by name; `false` when it doesn't exist.
    async fn drop_index(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: &str,
    ) -> Result<bool, MongoError>;

    /// The collection's text and search indexes.
    async fn list(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<Vec<TextIndexInfo>, MongoError>;

    /// Runs a search.
    async fn hits(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        call: TextCall,
    ) -> Result<TextHits, MongoError>;
}

/// Whether an index key pattern is a text index's.
#[must_use]
pub fn is_text_key(key: &Document) -> bool {
    key.values().any(|v| v.as_str() == Some("text"))
}

/// Splits a filter into its top-level `$text` (alone, or in a top-level
/// `$and`) and the rest.
///
/// # Errors
/// 2 for two `$text` or one nested elsewhere.
pub fn split_text(f: &Document) -> Result<Option<(Document, Document)>, MongoError> {
    let mut text: Option<Document> = None;
    let mut rest = Document::new();
    for (k, v) in f {
        if k == "$text" {
            let Bson::Document(d) = v else {
                return Err(MongoError::new(code::BAD_VALUE, "$text expects an object"));
            };
            text = Some(d.clone());
        } else if k == "$and" {
            let Bson::Array(a) = v else {
                rest.insert(k, v.clone());
                continue;
            };
            let mut keep = Vec::new();
            for x in a {
                match x {
                    Bson::Document(d) if d.len() == 1 && d.contains_key("$text") => {
                        if text.is_some() {
                            return Err(MongoError::new(
                                code::BAD_VALUE,
                                "Too many text expressions",
                            ));
                        }
                        if let Ok(t) = d.get_document("$text") {
                            text = Some(t.clone());
                        }
                    }
                    other => keep.push(other.clone()),
                }
            }
            if !keep.is_empty() {
                rest.insert("$and", keep);
            }
        } else {
            rest.insert(k, v.clone());
        }
    }
    Ok(text.map(|t| (t, rest)))
}

fn no_search() -> MongoError {
    MongoError::new(
        code::SPRIG_DOC_FEATURE_UNSUPPORTED,
        "full-text search isn't available on this connection",
    )
}

impl Engine {
    fn text_hook(&self) -> Result<&std::sync::Arc<dyn TextSearch>, MongoError> {
        self.text.as_ref().ok_or_else(no_search)
    }

    /// The documents of `hits` that match `rest`, in hit order, each with
    /// its score (and highlights).
    async fn hit_docs(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        h: &TextHits,
        rest: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        let pred = filter::parse(rest)?;
        let sqlp = translate(&pred, m);
        let cx = crate::agg::AggExpr {
            now: crate::engine::now_date(),
        };
        let mut found: HashMap<Vec<u8>, Document> = HashMap::new();
        for chunk in h.hits.chunks(PAGE) {
            let keys: Vec<String> = chunk.iter().map(|(k, _)| bin_lit(k)).collect();
            let q = format!(
                "SELECT _id, doc, _types FROM {}.{} WHERE _id IN ({}) AND ({})",
                ident(&ns.db),
                ident(&m.table),
                keys.join(", "),
                sqlp.sql
            );
            let rows = match sql.query(&q).await {
                Ok(r) => r,
                Err(e) if e.is(1146) => return Ok(Vec::new()),
                Err(e) => return Err(mongo_error(&e)),
            };
            for mut r in rows {
                let key = r.first_mut().and_then(Option::take).unwrap_or_default();
                let doc = r.get(1).and_then(Option::as_deref).unwrap_or_default();
                let d = json::to_bson(
                    &String::from_utf8_lossy(doc),
                    r.get(2)
                        .and_then(Option::as_ref)
                        .map(|b| String::from_utf8_lossy(b))
                        .as_deref(),
                )?;
                if pred.matches_with(&d, &cx) {
                    found.insert(key, d);
                }
            }
        }
        let mut out = Vec::with_capacity(found.len());
        for (k, s) in &h.hits {
            if let Some(mut d) = found.remove(k) {
                d.insert(SCORE_FIELD, Bson::Double(*s));
                if let Some(hl) = h.highlights.get(k) {
                    d.insert(HIGHLIGHT_FIELD, hl.clone());
                }
                out.push(d);
            }
        }
        Ok(out)
    }

    /// The documents a `$text` filter (with the rest of its filter)
    /// selects, in score order, scored.
    ///
    /// # Errors
    /// 27 without a text index; the search's and SQL errors.
    pub async fn text_docs(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        text: Document,
        rest: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        let h = self
            .text_hook()?
            .hits(sql, ns, TextCall::Text(text))
            .await?;
        self.hit_docs(sql, ns, m, &h, rest).await
    }

    /// The `$search` stage.
    pub(crate) async fn search_stage(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        spec: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(Vec::new());
        };
        let h = self
            .text_hook()?
            .hits(sql, ns, TextCall::Search(spec.clone()))
            .await?;
        self.hit_docs(sql, ns, &m, &h, &Document::new()).await
    }

    /// The `$searchMeta` stage.
    pub(crate) async fn search_meta_stage(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        spec: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        if self.meta(sql, ns).await?.is_none() {
            return Ok(Vec::new());
        }
        let h = self
            .text_hook()?
            .hits(sql, ns, TextCall::Meta(spec.clone()))
            .await?;
        Ok(vec![
            h.meta
                .unwrap_or_else(|| doc! {"count": {"lowerBound": 0_i64}}),
        ])
    }

    /// Text and search indexes as `listIndexes` shows them (text only).
    pub(crate) async fn text_index_specs(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<Vec<Document>, MongoError> {
        let Some(t) = &self.text else {
            return Ok(Vec::new());
        };
        Ok(t.list(sql, ns)
            .await?
            .into_iter()
            .filter(|i| i.kind == TextKind::Text)
            .map(|i| i.spec)
            .collect())
    }
}

/// Removes the hidden search fields from results.
pub fn strip(docs: &mut [Document]) {
    for d in docs {
        d.remove(HIGHLIGHT_FIELD);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_text_out_of_filters() {
        let (t, rest) = split_text(&doc! {"$text": {"$search": "a"}, "x": 1})
            .unwrap()
            .unwrap();
        assert_eq!(t, doc! {"$search": "a"});
        assert_eq!(rest, doc! {"x": 1});
        let (_, rest) = split_text(&doc! {"$and": [{"$text": {"$search": "a"}}, {"y": 2}]})
            .unwrap()
            .unwrap();
        assert_eq!(rest, doc! {"$and": [{"y": 2}]});
        assert!(split_text(&doc! {"x": 1}).unwrap().is_none());
        assert!(is_text_key(&doc! {"a": "text", "b": "text"}));
        assert!(!is_text_key(&doc! {"a": 1}));
    }
}
