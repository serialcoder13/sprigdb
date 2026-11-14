//! Text and search indexes on collections for the document engine (the
//! MongoDB front end routes the commands here): the engine's
//! [`TextSearch`] over the gateway's search service.
//!
//! - `createIndexes` with `"text"` keys makes a `mongo_text` index (one
//!   per collection, as in MongoDB); `createSearchIndexes` with
//!   `type: "search"` a `mongo_search` index. Both are full-text search
//!   indexes in the tenant catalog, built here as the creating user.
//! - `$text` waits for `latest` by default: MongoDB's text
//!   indexes are updated with the write, so a driver reading its own
//!   write expects to find it. `$search` and `$searchMeta` use
//!   `search.default_consistency`, as Atlas Search is eventually
//!   consistent; a stage may say `consistency: "latest"`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bson::{Bson, Document, doc};
use serde_json::Value as Json;
use sprig_common::ids::TenantId;
use sprig_common::{SprigCode, SprigError};
use sprig_doc::engine::{Ns, SqlRunner};
use sprig_doc::meta::CollMeta;
use sprig_doc::text::{TextCall, TextCreate, TextHits, TextIndexInfo, TextKind, TextSearch};
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_search::def::{Flavor, IndexDef, Model};
use sprig_search::query::{SearchQuery, parse_atlas, parse_mongo_text};
use sprig_search::{Hit, highlight};

use super::catalog as scat;
use super::{Consistency, SearchService};

/// The engine's hook (looks the service up at each call).
#[derive(Debug, Default)]
pub struct DocSearch {
    /// The newest catalog version this gateway committed, per tenant.
    seen: Mutex<HashMap<u64, u64>>,
}

fn mongo(e: &SprigError) -> MongoError {
    let c = match e.kind {
        sprig_common::ErrorKind::Sprig(c) => i32::from(c.number()),
        sprig_common::ErrorKind::Standard { errno: 1061, .. } => code::INDEX_OPTIONS_CONFLICT,
        _ => code::INTERNAL_ERROR,
    };
    MongoError::new(c, e.message.clone())
}

fn svc() -> Result<&'static Arc<SearchService>, MongoError> {
    super::service().ok_or_else(|| {
        MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            "full-text search isn't running in this gateway",
        )
    })
}

fn tenant_of(sql: &dyn SqlRunner) -> Result<TenantId, MongoError> {
    match sql.tenant() {
        0 => Err(MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            "search indexes need a tenant login",
        )),
        t => Ok(TenantId(t)),
    }
}

fn to_json(d: &Document) -> Json {
    Bson::Document(d.clone()).into_relaxed_extjson()
}

fn to_doc(j: &Json) -> Document {
    Bson::try_from(j.clone())
        .ok()
        .and_then(|b| b.as_document().cloned())
        .unwrap_or_default()
}

/// `mongo_english` → `english`.
fn language_of(def: &IndexDef) -> String {
    def.default_analyzer
        .strip_prefix("mongo_")
        .unwrap_or(&def.default_analyzer)
        .to_owned()
}

/// A `text` index as `listIndexes` shows it.
fn text_spec(def: &IndexDef) -> Document {
    let mut weights = Document::new();
    if def.dynamic {
        weights.insert("$**", 1);
    }
    for f in &def.fields {
        #[allow(clippy::cast_possible_truncation)]
        let w = f.boost.round() as i32;
        weights.insert(f.name.clone(), w);
    }
    doc! {
        "v": 2,
        "key": {"_fts": "text", "_ftsx": 1},
        "name": def.name.clone(),
        "weights": weights,
        "default_language": language_of(def),
        "language_override": "language",
        "textIndexVersion": 3,
    }
}

impl DocSearch {
    fn saw(&self, t: TenantId, v: u64) {
        let mut m = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let e = m.entry(t.0).or_insert(0);
        *e = (*e).max(v);
    }

    fn min(&self, t: TenantId) -> u64 {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&t.0)
            .copied()
            .unwrap_or(0)
    }

    async fn indexes(
        &self,
        t: TenantId,
        ns: &Ns,
    ) -> Result<
        (
            TenantId,
            Arc<sprig_catalog::TenantCatalog>,
            u32,
            Vec<Arc<IndexDef>>,
        ),
        MongoError,
    > {
        let s = svc()?;
        let cat = s
            .catalog()
            .snapshot(t, self.min(t))
            .await
            .map_err(|e| mongo(&e))?;
        let Some(db) = s.catalog().db_id(&cat, &ns.db) else {
            return Ok((t, cat, 0, Vec::new()));
        };
        let lctn = s.catalog().lctn(&cat);
        let v = scat::state(&cat).on_object(db.0, &ns.coll, lctn);
        Ok((t, cat, db.0, v))
    }

    #[allow(clippy::unused_self)]
    async fn info(&self, t: TenantId, d: &IndexDef) -> TextIndexInfo {
        let ready = match svc() {
            Ok(s) => s.state(t, d).await.0,
            Err(_) => false,
        };
        match d.flavor {
            Flavor::MongoText => TextIndexInfo {
                name: d.name.clone(),
                id: d.index_id.to_string(),
                kind: TextKind::Text,
                spec: text_spec(d),
                ready,
            },
            _ => TextIndexInfo {
                name: d.name.clone(),
                id: format!("{:024x}", d.index_id),
                kind: TextKind::Search,
                spec: to_doc(&d.given),
                ready,
            },
        }
    }

    /// Commits `def` (new, or `replace`) and starts its copy here.
    async fn commit(
        &self,
        t: TenantId,
        hint: Option<(String, u32)>,
        ns: &Ns,
        table: &str,
        mut def: IndexDef,
        replace: Option<&IndexDef>,
    ) -> Result<IndexDef, MongoError> {
        let s = svc()?;
        let cat = s
            .catalog()
            .snapshot(t, self.min(t))
            .await
            .map_err(|e| mongo(&e))?;
        let db_id = s.catalog().db_id(&cat, &ns.db).ok_or_else(|| {
            MongoError::new(code::NAMESPACE_NOT_FOUND, format!("ns not found {ns}"))
        })?;
        let (tenant_name, user) = hint.ok_or_else(|| {
            MongoError::new(code::UNAUTHORIZED, "search indexes need a logged-in user")
        })?;
        def.model = Model::Collection;
        table.clone_into(&mut def.table);
        def.key_columns = vec!["_id".to_owned()];
        def.tenant_name = tenant_name;
        def.definer_user_id = user;
        def.object_id = s.catalog().table_id(&cat, db_id, table).unwrap_or(0);
        def.check(&scat::state(&cat).analyzers)
            .map_err(|e| mongo(&e))?;
        let v = if let Some(old) = replace {
            def.index_id = old.index_id;
            def.generation = old.generation + 1;
            s.catalog()
                .replace(t, db_id, &def)
                .await
                .map_err(|e| mongo(&e))?
        } else {
            {
                let max = s.knobs().max_per_object;
                match s
                    .catalog()
                    .create(t, db_id, &mut def, false, max, "user")
                    .await
                    .map_err(|e| mongo(&e))?
                {
                    Some(v) => v,
                    None => return Ok(def),
                }
            }
        };
        self.saw(t, v);
        let cat = s.catalog().snapshot(t, v).await.map_err(|e| mongo(&e))?;
        if let Some(d) = scat::state(&cat).by_name(db_id.0, &def.name) {
            s.prepare(
                t,
                &d,
                &scat::state(&cat).analyzers,
                s.knobs().wait_timeout * 10,
            )
            .await
            .map_err(|e| mongo(&e))?;
            return Ok((*d).clone());
        }
        Ok(def)
    }

    /// The hits of `q` on `def`, with the wait `consistency` asks for.
    async fn search(
        &self,
        t: TenantId,
        cat: &sprig_catalog::TenantCatalog,
        def: &Arc<IndexDef>,
        q: SearchQuery,
        limit: Option<usize>,
        consistency: Consistency,
    ) -> Result<Vec<Hit>, MongoError> {
        let s = svc()?;
        crate::exec::search::run_search(s, t, cat, def, q, limit, consistency)
            .await
            .map_err(|e| mongo(&e))
    }

    /// Atlas-style highlights of `paths` for the hits, from the documents
    /// the user can read.
    async fn highlights(
        &self,
        sql: &mut dyn SqlRunner,
        def: &IndexDef,
        q: &SearchQuery,
        hits: &[Hit],
        paths: &[String],
    ) -> Result<HashMap<Vec<u8>, Bson>, MongoError> {
        let s = svc()?;
        let schema = sprig_search::IndexSchema::build(def, &[]).map_err(|e| mongo(&e))?;
        let mut terms: HashSet<String> = HashSet::new();
        collect_terms(q, &schema, &mut terms);
        let chars = s.knobs().highlight_chars;
        let mut out = HashMap::new();
        for chunk in hits.chunks(500) {
            let keys: Vec<String> = chunk
                .iter()
                .map(|h| sprig_doc::sql::bin_lit(&h.key.0))
                .collect();
            let rows = sql
                .query(&format!(
                    "SELECT _id, doc FROM {}.{} WHERE _id IN ({})",
                    sprig_doc::sql::ident(&def.db),
                    sprig_doc::sql::ident(&def.table),
                    keys.join(", ")
                ))
                .await
                .map_err(|e| sprig_doc::engine::mongo_error(&e))?;
            for r in rows {
                let key = r.first().cloned().flatten().unwrap_or_default();
                let Some(doc) = r
                    .get(1)
                    .cloned()
                    .flatten()
                    .and_then(|b| serde_json::from_slice::<Json>(&b).ok())
                else {
                    continue;
                };
                let mut list = Vec::new();
                for p in paths {
                    let Some(f) = def.fields.iter().find(|f| &f.name == p && f.kind.is_text())
                    else {
                        continue;
                    };
                    let Ok(an) = schema
                        .analyzers
                        .get(f.index_analyzer(&def.default_analyzer))
                    else {
                        continue;
                    };
                    let mut v = &doc;
                    for part in p.split('.') {
                        v = v.get(part).unwrap_or(&Json::Null);
                    }
                    let Some(text) = v.as_str() else { continue };
                    if let Some(fr) = highlight::fragment(an, text, &terms, chars) {
                        let mut texts = Vec::new();
                        let mut last = 0;
                        for (a, b) in &fr.hits {
                            if *a > last {
                                texts.push(doc! {"value": &fr.text[last..*a], "type": "text"});
                            }
                            texts.push(doc! {"value": &fr.text[*a..*b], "type": "hit"});
                            last = *b;
                        }
                        if last < fr.text.len() {
                            texts.push(doc! {"value": &fr.text[last..], "type": "text"});
                        }
                        #[allow(clippy::cast_precision_loss)]
                        let score = fr.hits.len() as f64;
                        list.push(Bson::Document(
                            doc! {"score": score, "path": p.clone(), "texts": texts},
                        ));
                    }
                }
                out.insert(key, Bson::Array(list));
            }
        }
        Ok(out)
    }
}

/// The keys of `hits` whose documents the session reads.
async fn visible_ids(
    sql: &mut dyn SqlRunner,
    def: &IndexDef,
    hits: &[Hit],
) -> Result<Vec<sprig_search::schema::Key>, MongoError> {
    let mut out = Vec::with_capacity(hits.len());
    for chunk in hits.chunks(500) {
        let keys: Vec<String> = chunk
            .iter()
            .map(|h| sprig_doc::sql::bin_lit(&h.key.0))
            .collect();
        let rows = sql
            .query(&format!(
                "SELECT _id FROM {}.{} WHERE _id IN ({})",
                sprig_doc::sql::ident(&def.db),
                sprig_doc::sql::ident(&def.table),
                keys.join(", ")
            ))
            .await
            .map_err(|e| sprig_doc::engine::mongo_error(&e))?;
        out.extend(
            rows.into_iter()
                .filter_map(|r| r.into_iter().next().flatten())
                .map(sprig_search::schema::Key),
        );
    }
    Ok(out)
}

fn collect_terms(q: &SearchQuery, schema: &sprig_search::IndexSchema, out: &mut HashSet<String>) {
    match q {
        SearchQuery::Match { text, .. }
        | SearchQuery::Phrase { text, .. }
        | SearchQuery::Prefix { text, .. } => {
            for a in schema.analyzers.iter() {
                out.extend(a.query_tokens(text).into_iter().map(|t| t.text));
            }
        }
        SearchQuery::Bool {
            must,
            should,
            filter,
            ..
        } => {
            for x in must.iter().chain(should).chain(filter) {
                collect_terms(x, schema, out);
            }
        }
        SearchQuery::Boost(x, _) => collect_terms(x, schema, out),
        _ => {}
    }
}

/// A `$searchMeta` spec's query (its `facet.operator`, or its one
/// operator, else every document) and its facets.
fn meta_query(spec: &Document) -> Result<(SearchQuery, Option<Document>), MongoError> {
    let (op, facets) = if let Ok(f) = spec.get_document("facet") {
        (
            f.get_document("operator").ok().map(to_json),
            f.get_document("facets").ok().cloned(),
        )
    } else {
        {
            let rest: Document = spec
                .iter()
                .filter(|(k, _)| !matches!(k.as_str(), "index" | "count" | "consistency"))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            ((!rest.is_empty()).then(|| to_json(&rest)), None)
        }
    };
    let q = match op {
        Some(o) if o.as_object().is_some_and(|m| m.len() == 1) => {
            parse_atlas(&o).map_err(|e| mongo(&e))?
        }
        _ => SearchQuery::All,
    };
    Ok((q, facets))
}

fn index_name(spec: &Document) -> String {
    spec.get_str("index").unwrap_or("default").to_owned()
}

fn consistency(spec: &Document) -> Result<Consistency, MongoError> {
    let s = svc()?;
    match spec.get_str("consistency") {
        Ok(c) => Consistency::parse(c).map_err(|e| mongo(&e)),
        Err(_) => Ok(s.knobs().consistency),
    }
}

#[async_trait]
impl TextSearch for DocSearch {
    async fn create(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        req: TextCreate,
    ) -> Result<TextIndexInfo, MongoError> {
        let (t, _, _, existing) = self.indexes(tenant_of(sql)?, ns).await?;
        let def = match req {
            TextCreate::Text(spec) => {
                let key = spec.get_document("key").map_err(|_| {
                    MongoError::new(code::FAILED_TO_PARSE, "index spec needs 'key'")
                })?;
                let pairs: Vec<(String, Json)> = key
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone().into_relaxed_extjson()))
                    .collect();
                let weights: Vec<(String, f64)> = spec
                    .get_document("weights")
                    .map(|w| {
                        w.iter()
                            .filter_map(|(k, v)| {
                                let n = match v {
                                    Bson::Int32(i) => f64::from(*i),
                                    Bson::Int64(i) => *i as f64,
                                    Bson::Double(f) => *f,
                                    _ => return None,
                                };
                                Some((k.clone(), n))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let name = spec.get_str("name").map_or_else(
                    |_| {
                        key.keys()
                            .map(|k| format!("{k}_text"))
                            .collect::<Vec<_>>()
                            .join("_")
                    },
                    str::to_owned,
                );
                let def = IndexDef::from_mongo_text(
                    &name,
                    &ns.db,
                    &ns.coll,
                    &pairs,
                    &weights,
                    spec.get_str("default_language").ok(),
                    spec.get_str("language_override").ok(),
                )
                .map_err(|e| mongo(&e))?;
                if let Some(old) = existing.iter().find(|d| d.flavor == Flavor::MongoText) {
                    if old.name == def.name
                        && old.fields == def.fields
                        && old.dynamic == def.dynamic
                    {
                        return Ok(self.info(t, old).await);
                    }
                    return Err(MongoError::new(
                        code::INDEX_OPTIONS_CONFLICT,
                        format!(
                            "An equivalent index already exists with a different name and options. Requested index: {name}, existing index: {}",
                            old.name
                        ),
                    ));
                }
                def
            }
            TextCreate::Search { name, definition } => {
                let def = IndexDef::from_atlas(&name, &ns.db, &ns.coll, &to_json(&definition))
                    .map_err(|e| mongo(&e))?;
                if let Some(old) = existing.iter().find(|d| d.name == name) {
                    if old.given == def.given {
                        return Ok(self.info(t, old).await);
                    }
                    return Err(MongoError::new(
                        code::INDEX_KEY_SPECS_CONFLICT,
                        format!("Index {name} already exists with a different definition"),
                    ));
                }
                def
            }
        };
        let def = self
            .commit(
                tenant_of(sql)?,
                sql.principal_hint(),
                ns,
                &m.table,
                def,
                None,
            )
            .await?;
        Ok(self.info(t, &def).await)
    }

    async fn update(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: &str,
        definition: &Document,
    ) -> Result<bool, MongoError> {
        let (_, _, _, existing) = self.indexes(tenant_of(sql)?, ns).await?;
        let Some(old) = existing
            .iter()
            .find(|d| d.name == name && d.flavor == Flavor::MongoSearch)
            .cloned()
        else {
            return Ok(false);
        };
        let def = IndexDef::from_atlas(name, &ns.db, &ns.coll, &to_json(definition))
            .map_err(|e| mongo(&e))?;
        self.commit(
            tenant_of(sql)?,
            sql.principal_hint(),
            ns,
            &old.table,
            def,
            Some(&old),
        )
        .await?;
        Ok(true)
    }

    async fn drop_index(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: &str,
    ) -> Result<bool, MongoError> {
        let (t, _, _, existing) = self.indexes(tenant_of(sql)?, ns).await?;
        let Some(old) = existing.iter().find(|d| d.name == name) else {
            return Ok(false);
        };
        let s = svc()?;
        let v = s
            .catalog()
            .drop_index(t, old.index_id)
            .await
            .map_err(|e| mongo(&e))?;
        self.saw(t, v);
        s.forget(t, old.index_id);
        Ok(true)
    }

    async fn list(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> Result<Vec<TextIndexInfo>, MongoError> {
        let (t, _, _, existing) = self.indexes(tenant_of(sql)?, ns).await?;
        let mut out = Vec::new();
        for d in existing
            .iter()
            .filter(|d| matches!(d.flavor, Flavor::MongoText | Flavor::MongoSearch))
        {
            out.push(self.info(t, d).await);
        }
        Ok(out)
    }

    async fn plan(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        call: &TextCall,
        execute: bool,
    ) -> Result<Document, MongoError> {
        let (t, cat, _, existing) = self.indexes(tenant_of(sql)?, ns).await?;
        let s = svc()?;
        let (def, q, cons) = match call {
            TextCall::Text(arg) => {
                let Some(def) = existing.iter().find(|d| d.flavor == Flavor::MongoText) else {
                    return Err(MongoError::new(
                        code::INDEX_NOT_FOUND,
                        "text index required for $text query",
                    ));
                };
                let text = arg
                    .get_str("$search")
                    .map_err(|_| MongoError::new(code::BAD_VALUE, "$search required for $text"))?;
                (def, parse_mongo_text(text), Consistency::Latest)
            }
            TextCall::Search(spec) => {
                let name = index_name(spec);
                let Some(def) = existing.iter().find(|d| d.name == name) else {
                    return Ok(doc! {"index": name, "exists": false});
                };
                let q = parse_atlas(&to_json(spec)).map_err(|e| mongo(&e))?;
                (def, q, consistency(spec)?)
            }
            TextCall::Meta(spec) => {
                let name = index_name(spec);
                let Some(def) = existing.iter().find(|d| d.name == name) else {
                    return Ok(doc! {"index": name, "exists": false});
                };
                (def, meta_query(spec)?.0, consistency(spec)?)
            }
        };
        // The copy's count includes documents hidden from a caller a row
        // policy binds: shown to others only.
        let shown =
            execute && !super::visible::needs_visible(def, sql.row_policy_binds(def.object_id));
        let (hits, took) = if shown {
            let started = std::time::Instant::now();
            let (a, _) = s
                .query(
                    t,
                    def,
                    &scat::state(&cat).analyzers,
                    q.clone(),
                    vec![sprig_search::rpc::Op::Count],
                    cons,
                )
                .await
                .map_err(|e| mongo(&e))?;
            let n = match a.first() {
                Some(sprig_search::rpc::Answer::Count(n)) => usize::try_from(*n).ok(),
                _ => None,
            };
            (n, Some(started.elapsed()))
        } else {
            (None, None)
        };
        let v = super::explain::describe(s, t, def, &q, cons, hits, took).await;
        Ok(to_doc(&v))
    }

    async fn hits(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        call: TextCall,
    ) -> Result<TextHits, MongoError> {
        let (t, cat, _, existing) = self.indexes(tenant_of(sql)?, ns).await?;
        let s = svc()?;
        match call {
            TextCall::Text(arg) => {
                let Some(def) = existing.iter().find(|d| d.flavor == Flavor::MongoText) else {
                    return Err(MongoError::new(
                        code::INDEX_NOT_FOUND,
                        "text index required for $text query",
                    ));
                };
                let text = arg
                    .get_str("$search")
                    .map_err(|_| MongoError::new(code::BAD_VALUE, "$search required for $text"))?;
                if let Ok(l) = arg.get_str("$language")
                    && !l.eq_ignore_ascii_case(&language_of(def))
                {
                    return Err(mongo(&SprigError::new(
                        SprigCode::SearchAnalyzerUnknown,
                        format!(
                            "$language '{l}' differs from the text index's default_language; per-query languages are not supported"
                        ),
                    )));
                }
                // `$caseSensitive` and `$diacriticSensitive` are applied by
                // the engine after the search.
                let hits = self
                    .search(
                        t,
                        &cat,
                        def,
                        parse_mongo_text(text),
                        None,
                        Consistency::Latest,
                    )
                    .await?;
                Ok(TextHits {
                    hits: hits
                        .into_iter()
                        .map(|h| (h.key.0, f64::from(h.score)))
                        .collect(),
                    ..TextHits::default()
                })
            }
            TextCall::Search(spec) => {
                let name = index_name(&spec);
                let Some(def) = existing.iter().find(|d| d.name == name) else {
                    return Ok(TextHits::default());
                };
                let q = parse_atlas(&to_json(&spec)).map_err(|e| mongo(&e))?;
                let max = s.knobs().max_window;
                let hits = self
                    .search(t, &cat, def, q.clone(), Some(max), consistency(&spec)?)
                    .await?;
                let paths: Vec<String> = match spec
                    .get_document("highlight")
                    .ok()
                    .and_then(|h| h.get("path"))
                {
                    Some(Bson::String(p)) => vec![p.clone()],
                    Some(Bson::Array(a)) => a
                        .iter()
                        .filter_map(|x| x.as_str().map(str::to_owned))
                        .collect(),
                    _ => Vec::new(),
                };
                let highlights = if paths.is_empty() {
                    HashMap::new()
                } else {
                    self.highlights(sql, def, &q, &hits, &paths).await?
                };
                Ok(TextHits {
                    hits: hits
                        .into_iter()
                        .map(|h| (h.key.0, f64::from(h.score)))
                        .collect(),
                    meta: None,
                    highlights,
                })
            }
            TextCall::Meta(spec) => {
                let name = index_name(&spec);
                let Some(def) = existing.iter().find(|d| d.name == name) else {
                    return Ok(TextHits {
                        meta: Some(doc! {"count": {"lowerBound": 0_i64}}),
                        ..TextHits::default()
                    });
                };
                let cons = consistency(&spec)?;
                let (q, facets) = meta_query(&spec)?;
                let total = spec
                    .get_document("count")
                    .ok()
                    .and_then(|c| c.get_str("type").ok())
                    == Some("total");
                // One query for the count and every facet.
                let mut ops = vec![sprig_search::rpc::Op::Count];
                let mut names = Vec::new();
                for (fname, f) in facets.iter().flatten() {
                    let Bson::Document(f) = f else { continue };
                    let path = f.get_str("path").unwrap_or_default().to_owned();
                    #[allow(clippy::cast_precision_loss)]
                    let bounds: Option<Vec<f64>> = f.get_array("boundaries").ok().map(|a| {
                        a.iter()
                            .filter_map(|x| match x {
                                Bson::Int32(i) => Some(f64::from(*i)),
                                Bson::Int64(i) => Some(*i as f64),
                                Bson::Double(d) => Some(*d),
                                _ => None,
                            })
                            .collect()
                    });
                    let n = f
                        .get("numBuckets")
                        .and_then(|x| match x {
                            Bson::Int32(i) => usize::try_from(*i).ok(),
                            Bson::Int64(i) => usize::try_from(*i).ok(),
                            _ => None,
                        })
                        .unwrap_or(10);
                    names.push(fname.clone());
                    ops.push(sprig_search::rpc::Op::Facet { path, n, bounds });
                }
                // A caller a row policy on the collection
                // binds gets the count and facets of the documents it may
                // see (the copy's own include hidden ones): the same ops
                // over the query restricted to the visible keys.
                let q = if super::visible::needs_visible(def, sql.row_policy_binds(def.object_id)) {
                    let all = super::visible::all_hits(s, t, &cat, def, q.clone(), cons)
                        .await
                        .map_err(|e| mongo(&e))?;
                    super::visible::within(q, visible_ids(sql, def, &all).await?)
                } else {
                    q
                };
                let (answers, _) = s
                    .query(t, def, &scat::state(&cat).analyzers, q, ops, cons)
                    .await
                    .map_err(|e| mongo(&e))?;
                let mut it = answers.into_iter();
                let n = match it.next() {
                    Some(sprig_search::rpc::Answer::Count(n)) => n,
                    _ => 0,
                };
                #[allow(clippy::cast_possible_wrap)]
                let mut r = doc! {"count": if total { doc!{"total": n as i64} } else { doc!{"lowerBound": n as i64} }};
                if facets.is_some() {
                    let mut fd = Document::new();
                    for (fname, a) in names.into_iter().zip(it) {
                        let sprig_search::rpc::Answer::Buckets(b) = a else {
                            continue;
                        };
                        let buckets: Vec<Bson> = b
                            .into_iter()
                            .map(|x| {
                                let id = Bson::try_from(x.value).unwrap_or(Bson::Null);
                                #[allow(clippy::cast_possible_wrap)]
                                Bson::Document(doc! {"_id": id, "count": x.count as i64})
                            })
                            .collect();
                        fd.insert(fname, doc! {"buckets": buckets});
                    }
                    r.insert("facet", fd);
                }
                Ok(TextHits {
                    meta: Some(r),
                    ..TextHits::default()
                })
            }
        }
    }
}
