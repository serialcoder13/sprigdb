//! `$redact`, `$documents` and `$graphLookup`'s search (M2, 12 Nov 2026).
//!
//! `$graphLookup` reads another collection, so the engine drives it
//! ([`crate::engine`]); this module parses it and keeps the breadth-first
//! search, which asks a caller-supplied lookup for each level's matches.

use std::collections::HashSet;

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::{AggLimits, Vars, eval, missing};
use crate::update::type_name;

fn err(c: i32, msg: impl Into<String>) -> MongoError {
    MongoError::new(c, msg)
}

// --- $redact ---------------------------------------------------------------------------

fn redact_vars(root: &Document, cur: &Document, lim: &AggLimits) -> Vars {
    Vars::new(root, lim.now)
        .with("CURRENT", Bson::Document(cur.clone()))
        .with("DESCEND", Bson::String("descend".into()))
        .with("PRUNE", Bson::String("prune".into()))
        .with("KEEP", Bson::String("keep".into()))
}

fn redact_doc(
    root: &Document,
    cur: &Document,
    e: &Bson,
    lim: &AggLimits,
) -> Result<Option<Document>, MongoError> {
    match eval(e, &redact_vars(root, cur, lim))? {
        Bson::String(s) if s == "keep" => Ok(Some(cur.clone())),
        Bson::String(s) if s == "prune" => Ok(None),
        Bson::String(s) if s == "descend" => {
            let mut out = Document::new();
            for (k, v) in cur {
                if let Some(x) = redact_val(root, v, e, lim)? {
                    out.insert(k.clone(), x);
                }
            }
            Ok(Some(out))
        }
        other => Err(err(
            17053,
            format!(
                "$redact's expression should not return anything aside from the variables $$KEEP, $$DESCEND, and $$PRUNE, but returned {}",
                if missing(&other) {
                    "MISSING".to_owned()
                } else {
                    other.to_string()
                }
            ),
        )),
    }
}

fn redact_val(
    root: &Document,
    v: &Bson,
    e: &Bson,
    lim: &AggLimits,
) -> Result<Option<Bson>, MongoError> {
    Ok(match v {
        Bson::Document(d) => redact_doc(root, d, e, lim)?.map(Bson::Document),
        Bson::Array(a) => {
            let mut out = Vec::with_capacity(a.len());
            for x in a {
                if let Some(y) = redact_val(root, x, e, lim)? {
                    out.push(y);
                }
            }
            Some(Bson::Array(out))
        }
        o => Some(o.clone()),
    })
}

/// `$redact`.
///
/// # Errors
/// 17053 for a result other than `$$KEEP`, `$$DESCEND` or `$$PRUNE`.
pub(crate) fn redact(
    docs: Vec<Document>,
    e: &Bson,
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let mut out = Vec::with_capacity(docs.len());
    for d in docs {
        if let Some(x) = redact_doc(&d, &d, e, lim)? {
            out.push(x);
        }
    }
    Ok(out)
}

// --- $documents ----------------------------------------------------------------------------

/// `$documents` as the first stage: its expression's documents.
///
/// # Errors
/// 5858203 when it isn't an array; 40228 for an element that isn't a
/// document.
pub fn documents(arg: &Bson, lim: &AggLimits) -> Result<Vec<Document>, MongoError> {
    let empty = Document::new();
    let v = eval(arg, &Vars::new(&empty, lim.now))?;
    let Bson::Array(a) = v else {
        return Err(err(5_858_203, "an array is expected"));
    };
    a.into_iter()
        .map(|x| match x {
            Bson::Document(d) => Ok(d),
            o => Err(err(
                40228,
                format!(
                    "elements within the array passed to $documents must be objects, but found {}",
                    type_name(&o)
                ),
            )),
        })
        .collect()
}

/// The error for a stage that needs a collection in `aggregate: 1`
/// (73), or `None` for the stages that run without one.
#[must_use]
pub fn db_level_error(first: &str) -> Option<MongoError> {
    if matches!(
        first,
        "$documents"
            | "$currentOp"
            | "$listLocalSessions"
            | "$listSessions"
            | "$changeStream"
            | "$listClusterCatalog"
            | "$querySettings"
            | "$queryStats"
    ) {
        return None;
    }
    Some(err(
        code::INVALID_NAMESPACE,
        format!("{{aggregate: 1}} is not valid for '{first}'; a collection is required."),
    ))
}

// --- $graphLookup --------------------------------------------------------------------------

/// `doc.graph_lookup_max_bytes`: the documents one input document's
/// search may hold (M5, decision S77).
static GRAPH_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(104_857_600);

/// Sets `doc.graph_lookup_max_bytes`.
pub fn set_graph_lookup_max_bytes(n: usize) {
    GRAPH_MAX.store(n.max(1), std::sync::atomic::Ordering::Relaxed);
}

/// A parsed `$graphLookup`.
#[derive(Debug, Clone)]
pub struct GraphSpec {
    /// The collection searched (same database).
    pub from: String,
    start: Bson,
    from_field: String,
    to_field: String,
    as_field: String,
    max_depth: Option<i64>,
    depth_field: Option<String>,
    restrict: Option<Document>,
}

fn path_ok(name: &str, v: &str) -> Result<(), MongoError> {
    if v.is_empty() || v.starts_with('$') || v.split('.').any(str::is_empty) {
        return Err(err(
            16410,
            format!(
                "FieldPath field names may not start with '$'. Consider using $getField or $setField for a field path with '.' or '$'. :: caused by :: {name}: {v}"
            ),
        ));
    }
    Ok(())
}

impl GraphSpec {
    /// Parses `$graphLookup`'s argument.
    ///
    /// # Errors
    /// MongoDB 8.0's codes (9, 40101–40105, 40185, 16410).
    pub fn parse(arg: &Bson) -> Result<Self, MongoError> {
        let Bson::Document(d) = arg else {
            return Err(err(
                code::FAILED_TO_PARSE,
                "the $graphLookup stage specification must be an object",
            ));
        };
        let mut from = None;
        let mut start = None;
        let mut strs: [Option<String>; 4] = [None, None, None, None];
        let mut max_depth = None;
        let mut restrict = None;
        for (k, v) in d {
            match k.as_str() {
                "from" => match v {
                    Bson::String(s) => from = Some(s.clone()),
                    o => {
                        return Err(err(
                            code::FAILED_TO_PARSE,
                            format!(
                                "$graphLookup 'from' field must be a string, but found {}",
                                type_name(o)
                            ),
                        ));
                    }
                },
                "startWith" => start = Some(v.clone()),
                "connectFromField" | "connectToField" | "as" | "depthField" => {
                    let Bson::String(s) = v else {
                        return Err(err(
                            40103,
                            format!(
                                "expected string as argument for {k}, found: {}",
                                type_name(v)
                            ),
                        ));
                    };
                    path_ok(k, s)?;
                    let i = ["connectFromField", "connectToField", "as", "depthField"]
                        .iter()
                        .position(|x| x == k)
                        .unwrap_or(0);
                    strs[i] = Some(s.clone());
                }
                "maxDepth" => {
                    let Some(n) = super::accum_num::integral(v) else {
                        return Err(err(
                            40102,
                            format!("maxDepth could not be represented as a long long: {v}"),
                        ));
                    };
                    if n < 0 {
                        return Err(err(
                            40101,
                            format!("maxDepth requires a nonnegative argument, found: {n}"),
                        ));
                    }
                    max_depth = Some(n);
                }
                "restrictSearchWithMatch" => match v {
                    Bson::Document(r) => restrict = Some(r.clone()),
                    o => {
                        return Err(err(
                            40185,
                            format!(
                                "restrictSearchWithMatch must be an object, found {}",
                                type_name(o)
                            ),
                        ));
                    }
                },
                _ => return Err(err(40104, format!("Unknown argument to $graphLookup: {k}"))),
            }
        }
        let Some(from) = from else {
            return Err(err(
                code::FAILED_TO_PARSE,
                format!("missing 'from' option to $graphLookup stage specification: {d}"),
            ));
        };
        let [cf, ct, asf, df] = strs;
        let (Some(start), Some(cf), Some(ct), Some(asf)) = (start, cf, ct, asf) else {
            return Err(err(
                40105,
                "$graphLookup requires 'from', 'as', 'startWith', 'connectFromField', and 'connectToField' to be specified.",
            ));
        };
        if let Some(r) = &restrict {
            crate::filter::parse(r)?;
        }
        Ok(Self {
            from,
            start,
            from_field: cf,
            to_field: ct,
            as_field: asf,
            max_depth,
            depth_field: df,
            restrict,
        })
    }

    /// The start values of `d`: `startWith`, arrays flattened.
    ///
    /// # Errors
    /// The expression's.
    pub fn start_values(&self, d: &Document, lim: &AggLimits) -> Result<Vec<Bson>, MongoError> {
        let v = eval(&self.start, &Vars::new(d, lim.now))?;
        Ok(match v {
            Bson::Array(a) => a,
            x if missing(&x) => Vec::new(),
            x => vec![x],
        })
    }

    /// The filter of one level: `connectToField` among `values`, and the
    /// restriction.
    #[must_use]
    pub fn level_filter(&self, values: &[Bson]) -> Document {
        let mut conds = vec![Bson::Document(
            doc! {self.to_field.clone(): {"$in": values.to_vec()}},
        )];
        if let Some(r) = &self.restrict {
            conds.push(Bson::Document(r.clone()));
        }
        doc! {"$and": conds}
    }

    /// Starts the breadth-first search from `d`.
    ///
    /// # Errors
    /// `startWith`'s.
    pub fn start(&self, d: Document, lim: &AggLimits) -> Result<GraphSearch<'_>, MongoError> {
        let mut frontier: Vec<Bson> = Vec::new();
        let mut seen_vals: HashSet<Vec<u8>> = HashSet::new();
        for v in self.start_values(&d, lim)? {
            if seen_vals.insert(crate::keyenc::eq_key(&v)) {
                frontier.push(v);
            }
        }
        Ok(GraphSearch {
            spec: self,
            doc: d,
            frontier,
            seen_vals,
            seen_ids: HashSet::new(),
            found: Vec::new(),
            bytes: 0,
            depth: 0,
            limit: GRAPH_MAX.load(std::sync::atomic::Ordering::Relaxed),
            spilled: None,
        })
    }

    /// The breadth-first search from `d`; `fetch` answers one level.
    ///
    /// # Errors
    /// `fetch`'s; 292 past `doc.agg_stage_memory_bytes`.
    pub fn search(
        &self,
        d: Document,
        lim: &AggLimits,
        mut fetch: impl FnMut(&Document) -> Result<Vec<Document>, MongoError>,
    ) -> Result<Document, MongoError> {
        let mut s = self.start(d, lim)?;
        while let Some(f) = s.next_filter() {
            let level = fetch(&f)?;
            s.absorb(level)?;
        }
        s.finish()
    }
}

/// One document's `$graphLookup` search, a level at a time (the engine
/// answers each level from the database).
#[derive(Debug)]
pub struct GraphSearch<'a> {
    spec: &'a GraphSpec,
    doc: Document,
    frontier: Vec<Bson>,
    seen_vals: HashSet<Vec<u8>>,
    seen_ids: HashSet<Vec<u8>>,
    found: Vec<Bson>,
    bytes: usize,
    depth: i64,
    limit: usize,
    /// Past the limit, with `allowDiskUse` (M7, decision S88): the
    /// matches so far and later ones, on disk until `finish`.
    spilled: Option<Spilled>,
}

/// A search's matches on disk.
struct Spilled(crate::spill::SpillWriter);

impl std::fmt::Debug for Spilled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Spilled")
    }
}

impl GraphSearch<'_> {
    /// The values the next level looks for.
    #[must_use]
    pub fn frontier(&self) -> &[Bson] {
        &self.frontier
    }

    /// The next level's filter, or `None` when the search is over.
    #[must_use]
    pub fn next_filter(&self) -> Option<Document> {
        if self.frontier.is_empty() || self.spec.max_depth.is_some_and(|m| self.depth > m) {
            return None;
        }
        Some(self.spec.level_filter(&self.frontier))
    }

    /// Takes one level's matches.
    ///
    /// # Errors
    /// 292 past `doc.agg_stage_memory_bytes`.
    pub fn absorb(&mut self, level: Vec<Document>) -> Result<(), MongoError> {
        let mut next = Vec::new();
        for mut f in level {
            let id = crate::keyenc::eq_key(f.get("_id").unwrap_or(&Bson::Null));
            if !self.seen_ids.insert(id) {
                continue;
            }
            for v in crate::filter::values_at(&f, &self.spec.from_field).vals {
                let vs = match v {
                    Bson::Array(a) => a.clone(),
                    o => vec![o.clone()],
                };
                for x in vs {
                    if self.seen_vals.insert(crate::keyenc::eq_key(&x)) {
                        next.push(x);
                    }
                }
            }
            if let Some(df) = &self.spec.depth_field {
                crate::update::set_path(&mut f, df, Bson::Int64(self.depth))?;
            }
            self.bytes += bson::to_vec(&f).map_or(0, |b| b.len());
            if let Some(w) = self.spilled.as_mut() {
                w.0.push(&f)?;
                continue;
            }
            if self.bytes > self.limit {
                // M7 (S88): with `allowDiskUse` the search's matches go to
                // disk and the search goes on; 292 without (S77).
                if !crate::spill::allowed() {
                    return Err(MongoError::new(
                        code::QUERY_EXCEEDED_MEMORY_LIMIT,
                        format!(
                            "$graphLookup reached maximum memory consumption: {} bytes (doc.graph_lookup_max_bytes)",
                            self.limit
                        ),
                    ));
                }
                let mut w = crate::spill::SpillWriter::create()?;
                for x in self.found.drain(..) {
                    if let Bson::Document(x) = x {
                        w.push(&x)?;
                    }
                }
                w.push(&f)?;
                self.spilled = Some(Spilled(w));
                continue;
            }
            self.found.push(Bson::Document(f));
        }
        self.frontier = next;
        self.depth += 1;
        Ok(())
    }

    /// The input document with the matches under `as`.
    ///
    /// # Errors
    /// 28 for an `as` path through a non-object.
    pub fn finish(self) -> Result<Document, MongoError> {
        let mut out = self.doc;
        let found = match self.spilled {
            None => self.found,
            // The matches come back from disk; the output document must
            // still fit BSON's 16 MiB.
            Some(w) => {
                if self.bytes > 16 * 1024 * 1024 {
                    return Err(MongoError::new(
                        code::BSON_OBJECT_TOO_LARGE,
                        format!(
                            "$graphLookup's result of {} bytes is larger than the 16 MiB a document may hold",
                            self.bytes
                        ),
                    ));
                }
                let f = w.0.finish()?;
                crate::spill::record_file(&f);
                f.reader()?
                    .map(|d| d.map(Bson::Document))
                    .collect::<Result<Vec<_>, _>>()?
            }
        };
        crate::update::set_path(&mut out, &self.spec.as_field, Bson::Array(found))?;
        Ok(out)
    }
}

/// `$graphLookup` over a collection held in memory (tests and
/// in-gateway sources).
///
/// # Errors
/// The search's and the filter's.
pub fn graph_lookup_in_memory(
    docs: Vec<Document>,
    spec: &GraphSpec,
    coll: &[Document],
    lim: &AggLimits,
) -> Result<Vec<Document>, MongoError> {
    let mut out = Vec::with_capacity(docs.len());
    for d in docs {
        out.push(spec.search(d, lim, |f| {
            let p = crate::filter::parse(f)?;
            Ok(coll.iter().filter(|x| p.matches(x)).cloned().collect())
        })?);
    }
    Ok(out)
}
