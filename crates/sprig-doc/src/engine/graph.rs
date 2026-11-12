//! `$graphLookup` against the database (M2, 12 Nov 2026): each level of
//! the breadth-first search is one read of the `from` collection through
//! the caller's session (privileges and row policies apply), with
//! `connectToField` among the level's values and `restrictSearchWithMatch`.
//! Since M5 the searches of a batch of input documents share each level's
//! read.

use std::collections::HashSet;

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::MongoError;

use super::{Engine, Ns, SqlRunner};
use crate::agg::{AggLimits, GraphSpec};

impl Engine {
    /// `$graphLookup` over `docs`: the searches of up to [`BATCH`] input
    /// documents run in step, each level one read of the `from`
    /// collection for all of them (M5, decision S77); each search takes
    /// the documents its own level filter matches.
    pub(super) async fn graph_lookup(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        docs: Vec<Document>,
        arg: &Bson,
        lim: &AggLimits,
    ) -> Result<Vec<Document>, MongoError> {
        let spec = GraphSpec::parse(arg)?;
        let fns = Ns {
            db: ns.db.clone(),
            coll: spec.from.clone(),
        };
        let mut out = Vec::with_capacity(docs.len());
        let mut docs = docs.into_iter();
        loop {
            let batch: Vec<Document> = (&mut docs).take(BATCH).collect();
            if batch.is_empty() {
                break;
            }
            let mut searches = batch
                .into_iter()
                .map(|d| spec.start(d, lim))
                .collect::<Result<Vec<_>, _>>()?;
            loop {
                let active: Vec<(usize, Document)> = searches
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| s.next_filter().map(|f| (i, f)))
                    .collect();
                if active.is_empty() {
                    break;
                }
                if let [(i, f)] = active.as_slice() {
                    let level = Box::pin(self.aggregate(sql, &fns, &[doc! {"$match": f.clone()}])).await?;
                    searches[*i].absorb(level)?;
                    continue;
                }
                // One read for the batch: every frontier value once.
                let mut seen = HashSet::new();
                let mut values = Vec::new();
                for (i, _) in &active {
                    for v in searches[*i].frontier() {
                        if seen.insert(crate::keyenc::eq_key(v)) {
                            values.push(v.clone());
                        }
                    }
                }
                let level = Box::pin(self.aggregate(
                    sql,
                    &fns,
                    &[doc! {"$match": spec.level_filter(&values)}],
                ))
                .await?;
                for (i, f) in active {
                    let p = crate::filter::parse(&f)?;
                    let mine: Vec<Document> = crate::heavy::run(level.len(), || {
                        level.iter().filter(|d| p.matches(d)).cloned().collect()
                    });
                    searches[i].absorb(mine)?;
                }
            }
            for s in searches {
                out.push(s.finish()?);
            }
        }
        Ok(out)
    }
}

/// Input documents whose searches share each level's read.
const BATCH: usize = 100;
