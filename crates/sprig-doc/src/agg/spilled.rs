//! The in-gateway stages over sequences that may live on disk (M5,
//! decisions S72–S76).
//!
//! With `allowDiskUse` (the gateway's [`crate::spill::scope`]):
//! - streaming stages run a chunk at a time over a spilled input;
//! - `$sort` (with a following `$limit` kept as a top-k) sorts in runs on
//!   disk and merges them;
//! - `$group`, `$bucket` and `$sortByCount` group in memory first; past
//!   `doc.agg_stage_memory_bytes` they start again sorted by group key
//!   on disk and accumulate one group at a time;
//! - `$bucketAuto` sorts on disk and fills one bucket at a time;
//! - `$setWindowFields` sorts by partition and `sortBy` on disk and
//!   computes one partition at a time (a single partition must fit);
//! - `$facet` reads its input once per facet, and its output document is
//!   held to MongoDB's 100 MB (4031700).
//!
//! Without it, every stage runs in memory as before ([`super::run_stage`]),
//! except that `$sort` followed by `$limit` keeps only the top documents,
//! as MongoDB does.

use std::cmp::Ordering;
use std::sync::Arc;

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::{
    AggLimits, accum, cmp_docs, loc, parse_sort, run_stage, size_of, stages_bucket, window,
};
use crate::filter::as_i64;
use crate::keyenc::compare;
use crate::spill::{self, Cmp, DocSeq, SeqBuilder, doc_bytes};

/// Documents per chunk of a streaming stage over a spilled input.
const CHUNK: usize = 1024;

/// MongoDB's `$facet` output limit code.
const FACET_TOO_LARGE: i32 = 4_031_700;

/// Runs `stage` over `seq`; `next` is the stage after it (a `$limit`
/// after a `$sort`).
///
/// # Errors
/// The stage's; 292 past the stage's memory without `allowDiskUse`;
/// 14031 past a spill cap.
pub fn run_stage_seq(
    seq: DocSeq,
    stage: &Document,
    next: Option<&Document>,
    lim: &AggLimits,
) -> Result<DocSeq, MongoError> {
    let (name, arg) = match stage.iter().next() {
        Some((n, a)) if stage.len() == 1 => (n.as_str(), a),
        _ => return Ok(DocSeq::Mem(run_stage(seq.into_vec()?, stage, lim)?)),
    };
    let top_k = name == "$sort"
        && next
            .and_then(|n| n.get("$limit"))
            .and_then(as_i64)
            .is_some_and(|n| n > 0);
    if !spill::allowed() && !top_k && seq.is_mem() {
        return Ok(DocSeq::Mem(run_stage(seq.into_vec()?, stage, lim)?));
    }
    let doc_arg = || {
        arg.as_document()
            .ok_or_else(|| loc(15969, format!("{name} specification must be an object")))
    };
    match name {
        "$match" | "$project" | "$addFields" | "$set" | "$unset" | "$replaceRoot"
        | "$replaceWith" | "$unwind" | "$redact" => chunked(seq, stage, lim),
        "$skip" | "$limit" | "$sample" => {
            // The arguments' errors, as in memory.
            run_stage(Vec::new(), stage, lim)?;
            if let DocSeq::Mem(v) = seq {
                return Ok(DocSeq::Mem(run_stage(v, stage, lim)?));
            }
            let n = match name {
                "$sample" => doc_arg()?.get("size").and_then(as_i64).unwrap_or(0),
                _ => as_i64(arg).unwrap_or(0),
            };
            let n = usize::try_from(n.max(0)).unwrap_or(usize::MAX);
            let it = seq.into_iter_docs()?;
            Ok(DocSeq::Stream(if name == "$skip" {
                Box::new(it.skip(n))
            } else {
                Box::new(it.take(n))
            }))
        }
        "$count" => {
            run_stage(Vec::new(), stage, lim)?;
            if let DocSeq::Mem(v) = seq {
                return Ok(DocSeq::Mem(run_stage(v, stage, lim)?));
            }
            let mut n = 0usize;
            for d in seq.into_iter_docs()? {
                d?;
                n += 1;
            }
            let Bson::String(f) = arg else {
                return Ok(DocSeq::Mem(Vec::new()));
            };
            Ok(DocSeq::Mem(if n == 0 {
                Vec::new()
            } else {
                vec![
                    doc! {f.clone(): i32::try_from(n).map_or(Bson::Int64(i64::try_from(n).unwrap_or(i64::MAX)), Bson::Int32)},
                ]
            }))
        }
        "$sort" => {
            let spec = parse_sort(doc_arg()?)?;
            let limit = next
                .and_then(|n| n.get("$limit"))
                .and_then(as_i64)
                .filter(|n| *n > 0)
                .map(|n| usize::try_from(n).unwrap_or(usize::MAX));
            if let DocSeq::Mem(v) = &seq
                && size_of(v) <= lim.stage_bytes
            {
                let DocSeq::Mem(v) = seq else { unreachable!() };
                return Ok(DocSeq::Mem(run_stage(v, stage, lim)?));
            }
            let cmp: Cmp = Arc::new(move |a: &Document, b: &Document| cmp_docs(a, b, &spec));
            spill::sort_seq(seq.into_iter_docs()?, cmp, limit, lim.stage_bytes, "Sort")
        }
        "$group" => {
            let spec = doc_arg()?;
            let id_expr = spec
                .get("_id")
                .ok_or_else(|| loc(15955, "a group specification must include an _id"))?
                .clone();
            let accs = accum::parse_fields(spec, true)?;
            group_seq(
                seq,
                &|d: &Document| accum::group_key(&id_expr, d, lim),
                &accs,
                spec,
                lim,
                "$group",
            )
        }
        "$sortByCount" => {
            let spec = doc! {"_id": arg.clone(), "count": {"$sum": 1}};
            let accs = accum::parse_fields(&spec, true)?;
            let id_expr = arg.clone();
            let g = group_seq(
                seq,
                &|d: &Document| accum::group_key(&id_expr, d, lim),
                &accs,
                &spec,
                lim,
                "$sortByCount",
            )?;
            let by = vec![("count".to_owned(), -1)];
            let cmp: Cmp = Arc::new(move |a: &Document, b: &Document| cmp_docs(a, b, &by));
            spill::sort_seq(g.into_iter_docs()?, cmp, None, lim.stage_bytes, "Sort")
        }
        "$bucket" => {
            let sp = stages_bucket::parse_bucket(arg)?;
            let g = group_seq(
                seq,
                &|d: &Document| sp.key(d, lim),
                &sp.accs,
                &sp.js_spec,
                lim,
                "$bucket",
            )?;
            // One document per boundary: always small.
            let mut out = g.into_vec()?;
            stages_bucket::sort_by_id(&mut out);
            Ok(DocSeq::Mem(out))
        }
        "$bucketAuto" => bucket_auto_seq(seq, stage, arg, lim),
        "$setWindowFields" => window_seq(seq, stage, arg, lim),
        "$facet" => facet_seq(seq, arg, lim),
        _ => {
            let v =
                seq.into_vec_within(lim.stage_bytes, || accum::over_error(name, lim.stage_bytes))?;
            Ok(DocSeq::Mem(run_stage(v, stage, lim)?))
        }
    }
}

/// Runs `stages` over `seq` (a `$facet` branch).
///
/// # Errors
/// The stages'.
pub fn run_stages_seq(
    mut seq: DocSeq,
    stages: &[Document],
    lim: &AggLimits,
) -> Result<DocSeq, MongoError> {
    for (i, s) in stages.iter().enumerate() {
        seq = run_stage_seq(seq, s, stages.get(i + 1), lim)?;
    }
    Ok(seq)
}

/// A streaming stage: in memory as before; over a spilled input, a chunk
/// at a time into a builder.
fn chunked(seq: DocSeq, stage: &Document, lim: &AggLimits) -> Result<DocSeq, MongoError> {
    if let DocSeq::Mem(v) = seq {
        return Ok(DocSeq::Mem(run_stage(v, stage, lim)?));
    }
    let mut out = SeqBuilder::new(lim.stage_bytes);
    let mut chunk = Vec::with_capacity(CHUNK);
    let flush = |chunk: Vec<Document>, out: &mut SeqBuilder| -> Result<(), MongoError> {
        for d in run_stage(chunk, stage, lim)? {
            out.push(d)?;
        }
        Ok(())
    };
    for d in seq.into_iter_docs()? {
        chunk.push(d?);
        if chunk.len() >= CHUNK {
            flush(std::mem::take(&mut chunk), &mut out)?;
        }
    }
    if !chunk.is_empty() {
        flush(chunk, &mut out)?;
    }
    out.finish()
}

fn key_bytes(d: &Document) -> &[u8] {
    match d.get("k") {
        Some(Bson::Binary(b)) => &b.bytes,
        _ => &[],
    }
}

/// A `$group`-like stage: in memory while its groups fit, else sorted by
/// group key on disk and accumulated one group at a time (S72).
fn group_seq(
    seq: DocSeq,
    key: &dyn Fn(&Document) -> Result<Bson, MongoError>,
    accs: &[(String, accum::AccSpec)],
    js_spec: &Document,
    lim: &AggLimits,
    stage: &str,
) -> Result<DocSeq, MongoError> {
    let input = seq.rereadable(lim.stage_bytes)?;
    // In memory first.
    {
        let mut js = crate::serverjs::GroupJs::parse(js_spec)?;
        let mut g = accum::Grouper::new(accs, lim, stage, (!js.is_empty()).then_some(&mut js));
        let mut overflow = false;
        let mut push = |d: &Document| -> Result<bool, MongoError> {
            match g.push(d, key(d)?) {
                Ok(()) => Ok(true),
                Err(e) if e.code == code::QUERY_EXCEEDED_MEMORY_LIMIT && spill::allowed() => {
                    Ok(false)
                }
                Err(e) => Err(e),
            }
        };
        match &input {
            DocSeq::Mem(v) => {
                for d in v {
                    if !push(d)? {
                        overflow = true;
                        break;
                    }
                }
            }
            other => {
                for d in other.reread()? {
                    if !push(&d?)? {
                        overflow = true;
                        break;
                    }
                }
            }
        }
        if !overflow {
            return Ok(DocSeq::Mem(g.finish()?));
        }
    }
    // Sorted by group key (stable: each group keeps its input order).
    let wrapped = input.into_iter_docs()?.map(|d| {
        let d = d?;
        let id = key(&d)?;
        Ok(doc! {
            "k": Binary { subtype: BinarySubtype::Generic, bytes: crate::keyenc::key(&id) },
            "i": id,
            "d": d,
        })
    });
    let cmp: Cmp = Arc::new(|a: &Document, b: &Document| key_bytes(a).cmp(key_bytes(b)));
    let sorted = spill::sort_seq(wrapped, cmp, None, lim.stage_bytes, stage)?;
    let mut out = SeqBuilder::new(lim.stage_bytes);
    let mut js = crate::serverjs::GroupJs::parse(js_spec)?;
    let has_js = !js.is_empty();
    let mut cur: Option<Vec<u8>> = None;
    let mut g: Option<accum::Grouper<'_, '_>> = None;
    for w in sorted.into_iter_docs()? {
        let mut w = w?;
        let k = key_bytes(&w).to_vec();
        if cur.as_ref() != Some(&k) {
            if let Some(done) = g.take() {
                for d in done.finish()? {
                    out.push(d)?;
                }
            }
            let mut n = accum::Grouper::new(accs, lim, stage, has_js.then_some(&mut js));
            n.single = true;
            g = Some(n);
            cur = Some(k);
        }
        let id = w.remove("i").unwrap_or(Bson::Null);
        let d = match w.remove("d") {
            Some(Bson::Document(d)) => d,
            _ => Document::new(),
        };
        if let Some(g) = g.as_mut() {
            g.push(&d, id)?;
        }
    }
    if let Some(done) = g.take() {
        for d in done.finish()? {
            out.push(d)?;
        }
    }
    out.finish()
}

/// `$bucketAuto` past its memory: the documents sorted on disk, the
/// values in memory to place the buckets, then one bucket at a time.
fn bucket_auto_seq(
    seq: DocSeq,
    stage: &Document,
    arg: &Bson,
    lim: &AggLimits,
) -> Result<DocSeq, MongoError> {
    if let DocSeq::Mem(v) = &seq
        && size_of(v) <= lim.stage_bytes
    {
        return Ok(DocSeq::Mem(run_stage(seq.into_vec()?, stage, lim)?));
    }
    let sp = stages_bucket::parse_auto(arg)?;
    let mut vals: Vec<Bson> = Vec::new();
    // Pass 1: by value (stable), the values kept to place the buckets.
    let sorted = {
        let mut i = 0i64;
        let wrapped = seq.into_iter_docs()?.map(|d| {
            let d = d?;
            let v = sp.value(&d, lim)?;
            vals.push(v.clone());
            i += 1;
            Ok(doc! {"v": v, "i": i, "d": d})
        });
        let cmp: Cmp = Arc::new(|a: &Document, b: &Document| {
            compare(
                a.get("v").unwrap_or(&Bson::Null),
                b.get("v").unwrap_or(&Bson::Null),
            )
        });
        spill::sort_seq(wrapped, cmp, None, lim.stage_bytes, "$bucketAuto")?
    };
    vals.sort_by(compare);
    let plan = sp.plan(&vals);
    drop(vals);
    // Pass 2: each document tagged with its bucket, then sorted by bucket
    // and input order, as MongoDB 8.0 accumulates a bucket.
    let mut pos = 0usize;
    let mut bucket = 0usize;
    let ends: Vec<usize> = plan.iter().map(|(_, b, _, _)| *b).collect();
    let tagged = sorted.into_iter_docs()?.map(|w| {
        let mut w = w?;
        while bucket + 1 < ends.len() && pos >= ends[bucket] {
            bucket += 1;
        }
        pos += 1;
        w.remove("v");
        w.insert("b", i64::try_from(bucket).unwrap_or(i64::MAX));
        Ok(w)
    });
    let by_bucket: Cmp = Arc::new(|a: &Document, b: &Document| {
        (a.get_i64("b").unwrap_or(0), a.get_i64("i").unwrap_or(0))
            .cmp(&(b.get_i64("b").unwrap_or(0), b.get_i64("i").unwrap_or(0)))
    });
    let grouped = spill::sort_seq(tagged, by_bucket, None, lim.stage_bytes, "$bucketAuto")?;
    let mut it = grouped.into_iter_docs()?;
    let mut out = Vec::new();
    for (a, b, min, max) in plan {
        let ds = (&mut it).take(b - a).map(|w| {
            w.map(|mut w| match w.remove("d") {
                Some(Bson::Document(d)) => d,
                _ => Document::new(),
            })
        });
        if let Some(d) = sp.bucket(ds, min, max, lim)? {
            out.push(d);
        }
    }
    Ok(DocSeq::Mem(out))
}

/// `$setWindowFields` past its memory: sorted by partition and `sortBy`
/// on disk, then one partition at a time.
fn window_seq(
    seq: DocSeq,
    stage: &Document,
    arg: &Bson,
    lim: &AggLimits,
) -> Result<DocSeq, MongoError> {
    if let DocSeq::Mem(v) = &seq
        && size_of(v) <= lim.stage_bytes
    {
        return Ok(DocSeq::Mem(run_stage(seq.into_vec()?, stage, lim)?));
    }
    let ord = Arc::new(window::Order::parse(arg)?);
    let o2 = Arc::clone(&ord);
    let wrapped = seq.into_iter_docs()?.map(|d| {
        let d = d?;
        let p = ord.key(&d, lim)?;
        Ok(doc! {"p": p, "d": d})
    });
    let cmp: Cmp = Arc::new(move |a: &Document, b: &Document| {
        let o = compare(
            a.get("p").unwrap_or(&Bson::Null),
            b.get("p").unwrap_or(&Bson::Null),
        );
        if o != Ordering::Equal {
            return o;
        }
        match (a.get_document("d"), b.get_document("d")) {
            (Ok(x), Ok(y)) => cmp_docs(x, y, &o2.sort),
            _ => Ordering::Equal,
        }
    });
    let sorted = spill::sort_seq(wrapped, cmp, None, lim.stage_bytes, "$setWindowFields")?;
    let mut out = SeqBuilder::new(lim.stage_bytes);
    let mut part: Vec<Document> = Vec::new();
    let mut bytes = 0usize;
    let mut cur: Option<Bson> = None;
    let flush = |part: Vec<Document>, out: &mut SeqBuilder| -> Result<(), MongoError> {
        if part.is_empty() {
            return Ok(());
        }
        for d in window::set_window_fields(part, arg, lim)? {
            out.push(d)?;
        }
        Ok(())
    };
    for w in sorted.into_iter_docs()? {
        let mut w = w?;
        let p = w.remove("p").unwrap_or(Bson::Null);
        let d = match w.remove("d") {
            Some(Bson::Document(d)) => d,
            _ => Document::new(),
        };
        if cur
            .as_ref()
            .is_none_or(|c| compare(c, &p) != Ordering::Equal)
        {
            flush(std::mem::take(&mut part), &mut out)?;
            bytes = 0;
            cur = Some(p);
        }
        bytes += doc_bytes(&d);
        if bytes > lim.stage_bytes {
            return Err(MongoError::new(
                code::EXCEEDED_MEMORY_LIMIT,
                format!(
                    "$setWindowFields: one partition exceeded the memory limit of {} bytes and a partition can't be spilled (doc.agg_stage_memory_bytes)",
                    lim.stage_bytes
                ),
            ));
        }
        part.push(d);
    }
    flush(part, &mut out)?;
    out.finish()
}

/// `$facet`: each branch over the input read again; the output document
/// within `doc.agg_stage_memory_bytes` (MongoDB's 100 MB, 4031700).
fn facet_seq(seq: DocSeq, arg: &Bson, lim: &AggLimits) -> Result<DocSeq, MongoError> {
    let spec = arg
        .as_document()
        .ok_or_else(|| loc(15969, "$facet specification must be an object"))?;
    let mut branches = Vec::new();
    for (k, p) in spec {
        let Bson::Array(stages) = p else {
            return Err(loc(40170, "arguments to $facet must be arrays"));
        };
        let mut ss = Vec::with_capacity(stages.len());
        for s in stages {
            ss.push(
                s.as_document()
                    .ok_or_else(|| loc(40171, "each stage must be an object"))?
                    .clone(),
            );
        }
        branches.push((k.clone(), ss));
    }
    let input = seq.rereadable(lim.stage_bytes)?;
    let limit = lim.stage_bytes;
    let mut out = Document::new();
    let mut total = 0usize;
    let too_large = |n: usize| {
        MongoError::new(
            FACET_TOO_LARGE,
            format!(
                "document constructed by $facet is {n} bytes, which exceeds the limit of {limit} bytes"
            ),
        )
    };
    for (k, stages) in branches {
        let src = match &input {
            DocSeq::Mem(v) => DocSeq::Mem(v.clone()),
            other => DocSeq::Stream(other.reread()?),
        };
        let res = run_stages_seq(src, &stages, lim)?;
        let docs = res.into_vec_within(limit.saturating_sub(total), || too_large(limit + 1))?;
        total += size_of(&docs);
        if total > limit {
            return Err(too_large(total));
        }
        out.insert(k, docs.into_iter().map(Bson::Document).collect::<Vec<_>>());
    }
    Ok(DocSeq::Mem(vec![out]))
}

#[cfg(test)]
mod tests {
    use bson::{DateTime, doc};

    use super::*;
    use crate::spill::SpillCtx;

    fn lim(bytes: usize) -> AggLimits {
        AggLimits {
            stage_bytes: bytes,
            now: DateTime::now(),
        }
    }

    fn docs(n: i64) -> Vec<Document> {
        (0..n)
            .map(|i| doc! {"_id": i, "g": (i * 7919) % 300, "v": i % 17, "s": "x".repeat(40)})
            .collect()
    }

    /// Runs `pipeline` over `docs` in memory (no limit) and spilled
    /// (`bytes` per stage): the same documents.
    async fn same(
        pipeline: Vec<Document>,
        n: i64,
        bytes: usize,
        sort_out: bool,
    ) -> (Vec<Document>, Vec<spill::StageSpill>) {
        let big = lim(usize::MAX);
        let mut want = DocSeq::Mem(docs(n));
        for (i, s) in pipeline.iter().enumerate() {
            want = run_stage_seq(want, s, pipeline.get(i + 1), &big).unwrap();
        }
        let mut want = want.into_vec().unwrap();
        let ctx = SpillCtx::new(true, 42);
        let c2 = ctx.clone();
        let p2 = pipeline.clone();
        let mut got = spill::scope(ctx, async move {
            let small = lim(bytes);
            let mut seq = DocSeq::Mem(docs(n));
            for (i, s) in p2.iter().enumerate() {
                spill::set_stage(i);
                seq = run_stage_seq(seq, s, p2.get(i + 1), &small).unwrap();
            }
            seq.into_vec().unwrap()
        })
        .await;
        if sort_out {
            let by = vec![("_id".to_owned(), 1)];
            super::super::sort_docs(&mut want, &by);
            super::super::sort_docs(&mut got, &by);
        }
        assert_eq!(got, want, "{pipeline:?}");
        (got, c2.stats())
    }

    #[tokio::test]
    async fn spilled_stages_match_memory() {
        let _g = spill::serial().await;
        let (_, st) = same(vec![doc! {"$sort": {"v": -1, "g": 1}}], 3000, 20_000, false).await;
        assert!(st[0].spills > 1);
        same(
            vec![doc! {"$sort": {"v": 1}}, doc! {"$limit": 25}],
            3000,
            2_000,
            false,
        )
        .await;
        let (_, st) = same(
            vec![doc! {"$group": {"_id": "$g", "n": {"$sum": 1}, "all": {"$push": "$_id"}, "f": {"$first": "$v"}, "a": {"$avg": "$v"}, "top": {"$topN": {"n": 2, "sortBy": {"v": -1}, "output": "$_id"}}}}],
            3000,
            20_000,
            true,
        )
        .await;
        assert!(st[0].spills > 0, "{st:?}");
        same(vec![doc! {"$sortByCount": "$v"}], 3000, 5_000, false).await;
        same(
            vec![doc! {"$bucket": {"groupBy": "$v", "boundaries": [0, 5, 10, 20], "output": {"ids": {"$push": "$_id"}}}}],
            3000,
            25_000,
            false,
        )
        .await;
        same(
            vec![doc! {"$bucketAuto": {"groupBy": "$g", "buckets": 7, "output": {"n": {"$sum": 1}, "m": {"$max": "$_id"}}}}],
            3000,
            20_000,
            false,
        )
        .await;
        same(
            vec![doc! {"$setWindowFields": {"partitionBy": "$v", "sortBy": {"g": 1}, "output": {"r": {"$rank": {}}, "c": {"$sum": "$g", "window": {"documents": ["unbounded", "current"]}}}}}],
            3000,
            20_000,
            false,
        )
        .await;
        same(
            vec![
                doc! {"$sort": {"g": 1}},
                doc! {"$project": {"g": 1, "v": 1}},
                doc! {"$skip": 10},
                doc! {"$facet": {"a": [{"$count": "n"}], "b": [{"$group": {"_id": "$v", "s": {"$sum": "$g"}}}, {"$sort": {"_id": 1}}]}},
            ],
            3000,
            20_000,
            false,
        )
        .await;
        assert_eq!(spill::usage(Some(42)).2, 0, "every file is gone");
    }

    #[tokio::test]
    async fn without_allow_disk_use_group_and_sort_give_292() {
        let _g = spill::serial().await;
        for p in [
            doc! {"$group": {"_id": "$_id", "a": {"$push": "$s"}}},
            doc! {"$sort": {"g": 1}},
        ] {
            let r = spill::scope(SpillCtx::new(false, 43), async {
                run_stage_seq(DocSeq::Mem(docs(3000)), &p, None, &lim(20_000))
            })
            .await;
            assert_eq!(r.unwrap_err().code, 292, "{p:?}");
        }
        // A top-k fits without spilling, as in MongoDB.
        let r = spill::scope(SpillCtx::new(false, 43), async {
            run_stage_seq(
                DocSeq::Mem(docs(3000)),
                &doc! {"$sort": {"g": 1}},
                Some(&doc! {"$limit": 3}),
                &lim(20_000),
            )
        })
        .await;
        assert_eq!(r.unwrap().into_vec().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn facet_output_is_limited() {
        let _g = spill::serial().await;
        let r = spill::scope(SpillCtx::new(true, 44), async {
            run_stage_seq(
                DocSeq::Mem(docs(3000)),
                &doc! {"$facet": {"all": []}},
                None,
                &lim(20_000),
            )
        })
        .await;
        assert_eq!(r.unwrap_err().code, FACET_TOO_LARGE);
    }
}
