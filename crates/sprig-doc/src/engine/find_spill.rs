//! `find` with `allowDiskUse`: a sort past `doc.max_sort_bytes` spills
//! as `$sort` does (the external sort of [`crate::spill`]), and a
//! `limit` keeps only `skip + limit` documents while sorting.

use std::sync::Arc;

use sprig_proto_mongo::errors::MongoError;

use super::{CollMeta, CursorPlan, Engine, FindSpec, Ns, SqlRunner, capped, now_date};
use crate::agg::{self, Projection, SortSpec};
use crate::filter::Pred;
use crate::spill::{self, Cmp, DocIter, DocSeq};
use crate::translate::SqlPred;

impl Engine {
    /// A sorted `find` that may spill; the result is read as the cursor
    /// goes, its files deleted with the cursor.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn find_sorted_spill(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
        pred: &Pred,
        sqlp: &SqlPred,
        sort: SortSpec,
        spec: &FindSpec,
        projection: Option<Projection>,
    ) -> Result<CursorPlan, MongoError> {
        spill::set_kind(spill::Kind::Sort);
        let seq = self
            .scan_seq(sql, ns, &m.table, pred, sqlp, capped::KeyOrder::of(m))
            .await?;
        let skip = usize::try_from(spec.skip).unwrap_or(usize::MAX);
        let limit = (spec.limit > 0).then(|| usize::try_from(spec.limit).unwrap_or(usize::MAX));
        let keep = limit.map(|l| l.saturating_add(skip));
        let cmp: Cmp = Arc::new(move |a, b| agg::cmp_docs(a, b, &sort));
        let budget = self.max_sort_bytes();
        let sorted = crate::heavy::run(crate::heavy::HEAVY_DOCS, || {
            spill::sort_seq(seq.into_iter_docs()?, cmp, keep, budget, "Sort")
        })?;
        let now = now_date();
        let it = sorted
            .into_iter_docs()?
            .skip(skip)
            .take(limit.unwrap_or(usize::MAX));
        let it: DocIter = match projection {
            Some(p) => Box::new(it.map(move |d| d.and_then(|d| p.apply(&d, now)))),
            None => Box::new(it),
        };
        let mut plan = CursorPlan::from_seq(ns.clone(), DocSeq::Stream(it))?;
        plan.table.clone_from(&m.table);
        Ok(plan)
    }
}
