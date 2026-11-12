//! `$graphLookup` against the database (M2, 12 Nov 2026): each level of
//! the breadth-first search is one read of the `from` collection through
//! the caller's session (privileges and row policies apply), with
//! `connectToField` among the level's values and `restrictSearchWithMatch`.

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::MongoError;

use super::{Engine, Ns, SqlRunner};
use crate::agg::{AggLimits, GraphSpec};

impl Engine {
    /// `$graphLookup` over `docs`.
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
        for d in docs {
            let mut s = spec.start(d, lim)?;
            while let Some(f) = s.next_filter() {
                let level = Box::pin(self.aggregate(sql, &fns, &[doc! {"$match": f}])).await?;
                s.absorb(level)?;
            }
            out.push(s.finish()?);
        }
        Ok(out)
    }
}
