//! Chapter 33's key-value shapes in automatic indexing (chapter 13, "Other
//! models"; chapter 33 build step 9; R6 round 5).
//!
//! Redis keys and key prefixes are primary-key ranges, and sorted sets
//! have their score index, so most key-value shapes need nothing. What
//! does need an index is SQL that looks for a hash field's value or a set
//! member across keys, through chapter 33's read-only views:
//!
//! ```sql
//! SELECT k FROM cache__hash WHERE field = 'email' AND value = 'asha@x.io';
//! SELECT k FROM cache__set  WHERE member = 'admins';
//! ```
//!
//! MariaDB merges the view into a read of the element table
//! `__sprig_ke_<ns>` (columns `e`, the field or member, and `v`, the
//! value) joined to the key row, so the index belongs on the element
//! table. [`translate`] rewrites such a shape onto the element table
//! (`field` and `member` become `e`, `value` becomes `v`) before
//! qualification, and [`candidates`] proposes `(e(255), v(64))` when the
//! value is fixed too, else `(e(255))`, or `(v(64))` for a value alone.
//! `InnoDB` appends the primary key `(k, g, o, e)`, so the key comes with
//! every entry. A shape that fixes `k` needs nothing: the primary key
//! serves it. The prefixes keep the key at 319 bytes, inside
//! `auto_index.max_key_bytes`; such an index can't give an order or
//! cover, so its benefit is estimated from rows returned, as for spatial
//! and vector indexes. An element-table index costs every element write
//! of the namespace (hash fields, list items, set members alike).
//!
//! **Value fields of string keys** (R6 round 6, decision K3).
//! `JSON_VALUE(v, '$.email') = ...` on a key table gets a field column
//! `_sprig_p_<slug>_<8 hex>` ([`field_column`]) defined as
//! `LONGBLOB AS (JSON_VALUE(v, '<path>')) VIRTUAL INVISIBLE`, with a
//! prefix index ([`FIELD_PREFIX`] bytes) on it ([`field_candidates`]).
//! The column has exactly the type MariaDB gives the expression (`v` is a
//! `LONGBLOB`, so `JSON_VALUE` returns a binary `LONGBLOB`), and then
//! MariaDB 11.8 puts the column in place of the expression by itself
//! (its virtual-column substitution) for `=`, `IN`, `BETWEEN`, `<`, `>`
//! and `IS NULL`, in `SELECT`, `UPDATE` and `DELETE`, with literals or
//! `?`. Decision K2 had found no substitution because it tried
//! `VARCHAR`, `TEXT` and `CAST` columns, whose types differ from the
//! expression's. So no statement is rewritten: the gateway, the router,
//! prepared statements and the replay send the client's text as it is.
//! `LIKE` and comparisons with numbers aren't substituted and get no
//! candidate. The prefix keeps every value (a `VARCHAR` would cut long
//! values or fail writes); MariaDB checks the whole value after the
//! index lookup, so results are exact. The column costs no storage; the
//! index costs every write that changes `v`.

use sprig_common::ids::ObjectId;
use sprig_workload::shape::{ColumnRef, PredOp, Shape};

use super::candidates::{FpInput, MODEL_READ_FACTOR};
use super::model::{Candidate, Evidence, IndexDef, IndexKind, KeyPart, PartSource, Role};
use super::stats::TableStats;

/// Which view of chapter 33 a shape reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvView {
    /// `<ns>__hash (k, field, value)`.
    Hash,
    /// `<ns>__set (k, member)`.
    Set,
}

/// Where a view's rows live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvTarget {
    /// The element table, `__sprig_ke_<ns>`.
    pub elements: ObjectId,
    /// The view.
    pub view: KvView,
}

/// The namespace and view of a view name `<ns>__hash` or `<ns>__set`.
#[must_use]
pub fn view_of(name: &str) -> Option<(&str, KvView)> {
    for (suffix, v) in [("__hash", KvView::Hash), ("__set", KvView::Set)] {
        let n = name.len();
        if n > suffix.len()
            && name.as_bytes()[n - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
        {
            return Some((&name[..n - suffix.len()], v));
        }
    }
    None
}

/// The element table name of namespace table `ns`.
#[must_use]
pub fn elements_of(ns: &str) -> String {
    format!("__sprig_ke_{ns}")
}

/// Whether `stats` describe a key-value element table.
#[must_use]
pub fn is_element_table(stats: &TableStats) -> bool {
    stats.table.len() > "__sprig_ke_".len()
        && stats.table.as_bytes()[.."__sprig_ke_".len()].eq_ignore_ascii_case(b"__sprig_ke_")
}

fn column_of(view: KvView, col: &str) -> Option<&'static str> {
    match (view, col.to_ascii_lowercase().as_str()) {
        (_, "k") => Some("k"),
        (KvView::Hash, "field") | (KvView::Set, "member") => Some("e"),
        (KvView::Hash, "value") => Some("v"),
        _ => None,
    }
}

/// `shape` with every reference to a key-value view moved to its element
/// table and the view's columns renamed (`field`, `member` to `e`,
/// `value` to `v`); `None` when it reads no such view. `target` names the
/// element table of a view object.
#[must_use]
pub fn translate(shape: &Shape, target: &dyn Fn(ObjectId) -> Option<KvTarget>) -> Option<Shape> {
    let mut out = shape.clone();
    let mut changed = false;
    for i in 0..out.tables.len() {
        let Some(o) = out.tables[i].object_id else {
            continue;
        };
        let Some(t) = target(o) else { continue };
        out.tables[i].object_id = Some(t.elements);
        changed = true;
        let alias = out.tables[i].alias.clone();
        let fix = |c: &mut ColumnRef| {
            if c.table.as_deref() == Some(alias.as_str())
                && let Some(n) = column_of(t.view, &c.col)
            {
                n.clone_into(&mut c.col);
            }
        };
        for p in &mut out.predicates {
            fix(&mut p.column);
        }
        for j in &mut out.joins {
            fix(&mut j.l);
            fix(&mut j.r);
        }
        for o in &mut out.order_by {
            fix(&mut o.column);
        }
        for g in &mut out.group_by {
            fix(g);
        }
        if let Some(cols) = out.select.get_mut(&alias) {
            for c in cols.iter_mut() {
                if let Some(n) = column_of(t.view, c) {
                    n.clone_into(c);
                }
            }
        }
    }
    changed.then_some(out)
}

/// The prefix of `e` in an element-table index.
pub const E_PREFIX: u16 = 255;
/// The prefix of `v` in an element-table index.
pub const V_PREFIX: u16 = 64;

/// The note that marks an element-table candidate.
pub const NOTE: &str = "variant:kv_elements";

/// Whether `c` is a key-value candidate, on an element table or a field
/// column of a key table (its benefit is estimated from rows returned).
#[must_use]
pub fn is_kv(c: &Candidate) -> bool {
    c.evidence
        .notes
        .iter()
        .any(|n| n == NOTE || n == FIELD_NOTE)
}

/// The element-table candidates of `f` on table `alias` (module docs).
#[must_use]
pub fn candidates(
    f: &FpInput,
    alias: &str,
    object: ObjectId,
    stats: &TableStats,
) -> Vec<Candidate> {
    if !is_element_table(stats) {
        return Vec::new();
    }
    let on = |c: &str| {
        f.shape
            .predicates
            .iter()
            .filter(|p| p.column.table.as_deref() == Some(alias) && p.column.col == c)
            .map(|p| &p.op)
            .collect::<Vec<_>>()
    };
    let fixes = |ops: &[&PredOp]| {
        ops.iter()
            .any(|o| matches!(o, PredOp::Eq | PredOp::In { .. } | PredOp::IsNull))
    };
    let seeks = |ops: &[&PredOp]| {
        ops.iter().any(|o| {
            matches!(
                o,
                PredOp::Eq | PredOp::In { .. } | PredOp::Range { .. } | PredOp::Prefix
            )
        })
    };
    if fixes(&on("k")) {
        return Vec::new(); // the primary key serves it
    }
    let (e, v) = (on("e"), on("v"));
    let part = |c: &str, n: u16| KeyPart {
        prefix: Some(n),
        ..KeyPart::col(c)
    };
    let (parts, sources) = match (fixes(&e), seeks(&v)) {
        (true, true) => (
            vec![part("e", E_PREFIX), part("v", V_PREFIX)],
            vec![PartSource::Eq, PartSource::Range],
        ),
        (true, false) => (vec![part("e", E_PREFIX)], vec![PartSource::Eq]),
        (false, true) => (vec![part("v", V_PREFIX)], vec![PartSource::Range]),
        (false, false) => return Vec::new(),
    };
    vec![Candidate {
        def: IndexDef {
            object,
            kind: IndexKind::BTree,
            parts,
            options: super::model::IndexOptions::default(),
        },
        covering: false,
        serves: vec![(f.id, Role::Primary)],
        evidence: Evidence {
            parts: sources,
            entries_per_run: Some(f.returned_per_run.max(1.0) * MODEL_READ_FACTOR),
            notes: vec![NOTE.to_owned()],
            ..Evidence::default()
        },
    }]
}

/// `sql` with every reference to the view of element table `table` (in
/// `schema`) replaced by the view's own query, its element table read
/// with `IGNORE INDEX (index)`: the replay's form without the candidate,
/// since a view can't take an index hint. The queries mirror
/// `sprig_kv::api::view_ddl`. `None` when no such view is referenced.
#[must_use]
pub fn ignore_through_view(
    sql: &str,
    schema: &str,
    table: &str,
    index: &str,
    default_db: Option<&str>,
) -> Option<String> {
    use sprig_common::names::quote;
    let p = "__sprig_ke_";
    if table.len() <= p.len() || !table.as_bytes()[..p.len()].eq_ignore_ascii_case(p.as_bytes()) {
        return None;
    }
    let ns = &table[p.len()..];
    let live = "(t.expires_at IS NULL OR t.expires_at > UTC_TIMESTAMP(3))";
    let from = format!(
        "FROM {s}.{k} AS t JOIN {s}.{e} AS e IGNORE INDEX ({i}) ON e.k = t.k AND e.g = t.g",
        s = quote(schema),
        k = quote(ns),
        e = quote(table),
        i = quote(index)
    );
    let mut out = sql.to_owned();
    let mut found = false;
    for (suffix, cols, kind) in [
        ("__hash", "t.k AS k, e.e AS field, e.v AS value", "hash"),
        ("__set", "t.k AS k, e.e AS member", "set"),
    ] {
        let view = format!("{ns}{suffix}");
        let refs = super::replay::sqltext::table_refs(&out, schema, &view, default_db).ok()?;
        for r in refs.iter().rev() {
            found = true;
            let mut with = format!("(SELECT {cols} {from} WHERE t.type = '{kind}' AND {live})");
            if !r.aliased {
                with.push_str(" AS ");
                with.push_str(&quote(&view));
            }
            out.replace_range(r.start..r.name_end, &with);
        }
    }
    found.then_some(out)
}

/// Whether `f` filters a key-value key table's value by a JSON path
/// (a value field, decision K3).
#[must_use]
pub fn value_field(f: &FpInput, alias: &str, stats: &TableStats) -> bool {
    is_key_table(stats)
        && f.shape.predicates.iter().any(|p| {
            p.column.table.as_deref() == Some(alias)
                && p.column.col == "v"
                && matches!(p.op, PredOp::JsonPath { .. })
        })
}

/// Whether `stats` describe a key table (chapter 33: `k`, `type`, a
/// `LONGBLOB` `v` and `_sprig_dk`, and not an element or sorted-set table).
#[must_use]
pub fn is_key_table(stats: &TableStats) -> bool {
    let has = |c: &str| stats.columns.contains_key(c);
    let lower = stats.table.to_ascii_lowercase();
    has("_sprig_dk")
        && has("k")
        && has("type")
        && stats
            .columns
            .get("v")
            .is_some_and(|c| c.data_type.ends_with("blob"))
        && !lower.starts_with("__sprig_ke_")
        && !lower.starts_with("__sprig_kz_")
}

/// The index prefix of a field column, bytes.
pub const FIELD_PREFIX: u16 = 64;
/// The note that marks a field-column candidate.
pub const FIELD_NOTE: &str = "variant:kv_field";
/// The option key prefix naming a field column's JSON path:
/// `field.<column>` = path.
pub const FIELD_OPTION: &str = "field.";
/// The column a key table's field columns are computed from.
pub const VALUE_COLUMN: &str = "v";
/// The most field columns one candidate has (equalities, then a range).
pub const MAX_FIELDS: usize = 3;

/// The field column of JSON path `path`: `_sprig_p_`, the path's letters,
/// digits and `_` in lower case (others as `_`, at most 40), `_` and the
/// first 32 bits of `xxh3_64(path)` in hex. At most 58 characters; two
/// paths that look alike get different columns.
#[must_use]
pub fn field_column(path: &str) -> String {
    let mut slug = String::new();
    for c in path.chars() {
        if slug.len() >= 40 {
            break;
        }
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.is_empty() && !slug.ends_with('_') {
            slug.push('_');
        }
    }
    let slug = slug.trim_end_matches('_');
    let h = twox_hash::XxHash3_64::oneshot(path.as_bytes()) >> 32;
    if slug.is_empty() {
        format!("_sprig_p_{h:08x}")
    } else {
        format!("_sprig_p_{slug}_{h:08x}")
    }
}

/// Whether a JSON path can go into a field column's definition: a `$`
/// path of at most 200 bytes without quotes `'`, backslashes or control
/// characters.
#[must_use]
pub fn safe_path(path: &str) -> bool {
    path.starts_with('$')
        && path.len() <= 200
        && !path
            .chars()
            .any(|c| c == '\'' || c == '\\' || c.is_control())
}

/// The `ADD COLUMN` clause of field column `column` over `path`:
/// `ADD COLUMN IF NOT EXISTS <column> LONGBLOB AS (JSON_VALUE(v, '<path>'))
/// VIRTUAL INVISIBLE`. MariaDB refuses to add a virtual column online in
/// the same `ALTER TABLE` as an index, so builds run it first, on its own
/// (`ALGORITHM=INSTANT`).
#[must_use]
pub fn add_field_clause(column: &str, path: &str) -> String {
    format!(
        "ADD COLUMN IF NOT EXISTS {} LONGBLOB AS (JSON_VALUE({}, '{path}')) VIRTUAL INVISIBLE",
        sprig_common::names::quote(column),
        sprig_common::names::quote(VALUE_COLUMN),
    )
}

/// The field columns of a definition, `(column, path)`, in part order.
#[must_use]
pub fn fields_of(def: &super::model::IndexDef) -> Vec<(String, String)> {
    if def.kind != IndexKind::FieldColumn {
        return Vec::new();
    }
    // A document index (S131): its pair columns, each with its path.
    if super::doc::is_doc(def) {
        return super::doc::paths_of(def)
            .into_iter()
            .flat_map(|(p, _)| {
                let (c, n, s) = sprig_doc::meta::columns(&p);
                [c, n, s].map(|x| (x.to_ascii_lowercase(), p.clone()))
            })
            .collect();
    }
    def.parts
        .iter()
        .filter_map(|p| {
            let path = def.options.0.get(&format!("{FIELD_OPTION}{}", p.column))?;
            Some((p.column.clone(), path.clone()))
        })
        .collect()
}

/// The `ADD COLUMN` clauses of every field column of `def`, joined by
/// `, `; `None` for any other kind. Run as `ALTER TABLE s.t <clauses>,
/// ALGORITHM=INSTANT` before the index (see [`add_field_clause`]).
#[must_use]
pub fn add_fields_clause(def: &super::model::IndexDef) -> Option<String> {
    if super::doc::is_doc(def) {
        return super::doc::add_fields_clause(def);
    }
    let f = fields_of(def);
    (!f.is_empty()).then(|| {
        f.iter()
            .map(|(c, p)| add_field_clause(c, p))
            .collect::<Vec<_>>()
            .join(", ")
    })
}

/// `ALTER TABLE <schema>.<table> <add_fields_clause>, ALGORITHM=INSTANT`.
#[must_use]
pub fn add_fields_sql(schema: &str, table: &str, def: &super::model::IndexDef) -> Option<String> {
    use sprig_common::names::quote;
    add_fields_clause(def).map(|c| {
        format!(
            "ALTER TABLE {}.{} {c}, ALGORITHM=INSTANT",
            quote(schema),
            quote(table)
        )
    })
}

/// `ALTER TABLE <schema>.<table> DROP COLUMN IF EXISTS <c>, ...,
/// ALGORITHM=INSTANT`; `None` for no columns.
#[must_use]
pub fn drop_fields_sql(schema: &str, table: &str, columns: &[String]) -> Option<String> {
    use sprig_common::names::quote;
    (!columns.is_empty()).then(|| {
        let drops = columns
            .iter()
            .map(|c| format!("DROP COLUMN IF EXISTS {}", quote(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "ALTER TABLE {}.{} {drops}, ALGORITHM=INSTANT",
            quote(schema),
            quote(table)
        )
    })
}

/// The columns whose writes change a definition's index entries: its
/// parts, and `v` for field columns.
#[must_use]
pub fn maintained_columns(def: &super::model::IndexDef) -> Vec<String> {
    let mut cols: Vec<String> = def.columns().map(str::to_owned).collect();
    if super::doc::is_doc(def) {
        // A document index changes with the document (S131).
        cols.push(sprig_workload::fingerprint::mongo::DOC_COLUMN.to_owned());
    } else if def.kind == IndexKind::FieldColumn {
        cols.push(VALUE_COLUMN.to_owned());
    }
    cols
}

/// The field-column candidate of `f` on key table `alias` (module docs):
/// up to [`MAX_FIELDS`] `JSON_VALUE` paths of `v`, the equalities (`=`,
/// `IN`, `IS NULL`) by path, then one range. Nothing when the shape fixes
/// `k` (the primary key serves it) or no path qualifies.
#[must_use]
pub fn field_candidates(
    f: &FpInput,
    alias: &str,
    object: ObjectId,
    stats: &TableStats,
) -> Vec<Candidate> {
    if !is_key_table(stats) {
        return Vec::new();
    }
    let mine = |p: &&sprig_workload::shape::Predicate| p.column.table.as_deref() == Some(alias);
    let fixes_k = f.shape.predicates.iter().filter(mine).any(|p| {
        p.column.col == "k" && matches!(p.op, PredOp::Eq | PredOp::In { .. } | PredOp::IsNull)
    });
    if fixes_k {
        return Vec::new();
    }
    let mut eq: Vec<&str> = Vec::new();
    let mut range: Vec<&str> = Vec::new();
    for p in f.shape.predicates.iter().filter(mine) {
        let PredOp::JsonPath {
            path,
            func: Some(func),
            cmp: Some(cmp),
        } = &p.op
        else {
            continue;
        };
        if p.column.col != VALUE_COLUMN || func != "JSON_VALUE" || !safe_path(path) {
            continue;
        }
        match **cmp {
            PredOp::Eq | PredOp::In { .. } | PredOp::IsNull => eq.push(path),
            PredOp::Range { .. } => range.push(path),
            _ => {}
        }
    }
    eq.sort_unstable();
    eq.dedup();
    range.sort_unstable();
    let mut paths: Vec<(&str, PartSource)> = eq
        .into_iter()
        .take(MAX_FIELDS)
        .map(|p| (p, PartSource::Eq))
        .collect();
    if paths.len() < MAX_FIELDS
        && let Some(r) = range
            .into_iter()
            .find(|r| !paths.iter().any(|(p, _)| p == r))
    {
        paths.push((r, PartSource::Range));
    }
    if paths.is_empty() {
        return Vec::new();
    }
    let mut options = std::collections::BTreeMap::new();
    let mut parts = Vec::new();
    let mut sources = Vec::new();
    for (path, src) in paths {
        let col = field_column(path);
        options.insert(format!("{FIELD_OPTION}{col}"), path.to_owned());
        parts.push(KeyPart {
            prefix: Some(FIELD_PREFIX),
            ..KeyPart::col(&col)
        });
        sources.push(src);
    }
    vec![Candidate {
        def: IndexDef {
            object,
            kind: IndexKind::FieldColumn,
            parts,
            options: super::model::IndexOptions(options),
        },
        covering: false,
        serves: vec![(f.id, Role::Primary)],
        evidence: Evidence {
            parts: sources,
            entries_per_run: Some(f.returned_per_run.max(1.0) * MODEL_READ_FACTOR),
            notes: vec![FIELD_NOTE.to_owned()],
            ..Evidence::default()
        },
    }]
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::many_single_char_names)]

    use std::sync::Arc;

    use super::*;

    const VIEW: ObjectId = ObjectId(70);
    const ELEMS: ObjectId = ObjectId(71);

    fn shape(json: &str) -> Shape {
        Shape::from_json(json).unwrap()
    }

    fn target(o: ObjectId) -> Option<KvTarget> {
        (o == VIEW).then_some(KvTarget {
            elements: ELEMS,
            view: KvView::Hash,
        })
    }

    fn input(s: Shape) -> FpInput {
        FpInput {
            id: sprig_common::ids::Fingerprint(7),
            shape: Arc::new(s),
            runs: 100,
            mean_ms: 40.0,
            p95_ms: 80.0,
            examined_per_run: 50_000.0,
            returned_per_run: 1.0,
            affected_per_run: 0.0,
            limit: None,
            in_list_max: None,
            samples: Vec::new(),
            fast_only: false,
        }
    }

    fn stats(table: &str) -> TableStats {
        TableStats {
            table: table.into(),
            ..TableStats::default()
        }
    }

    const BY_VALUE: &str = r#"{"v":1,"type":"select","tables":[{"alias":"h","object_id":70,"db":"app","table":"cache__hash","db_id":1}],"predicates":[{"t":"h","col":"field","op":"eq","args":[0]},{"t":"h","col":"value","op":"eq","args":[1]}],"joins":[],"order_by":[],"group_by":[],"select":{"h":["k"]},"limit":false,"writes":null,"or_groups":0}"#;

    #[test]
    fn a_hash_lookup_by_value_gets_e_then_v_on_the_element_table() {
        let t = translate(&shape(BY_VALUE), &target).unwrap();
        assert_eq!(t.tables[0].object_id, Some(ELEMS));
        let cols: Vec<&str> = t.predicates.iter().map(|p| p.column.col.as_str()).collect();
        assert_eq!(cols, ["e", "v"]);
        assert_eq!(t.select["h"], ["k"]);
        let c = candidates(&input(t), "h", ELEMS, &stats("__sprig_ke_cache"));
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].def.ddl_columns(), "`e`(255), `v`(64)");
        assert!(is_kv(&c[0]));
        assert!((c[0].evidence.entries_per_run.unwrap() - 1.3).abs() < 1e-9);
        // Not an element table: nothing.
        assert!(candidates(&input(shape(BY_VALUE)), "h", VIEW, &stats("cache__hash")).is_empty());
    }

    #[test]
    fn fixed_keys_need_nothing_and_members_get_e() {
        let by_key = BY_VALUE.replace(r#""col":"field""#, r#""col":"k""#);
        let t = translate(&shape(&by_key), &target).unwrap();
        assert!(candidates(&input(t), "h", ELEMS, &stats("__sprig_ke_cache")).is_empty());
        let set = BY_VALUE
            .replace("cache__hash", "cache__set")
            .replace(r#",{"t":"h","col":"value","op":"eq","args":[1]}"#, "")
            .replace(r#""col":"field""#, r#""col":"member""#);
        let st = |o: ObjectId| {
            (o == VIEW).then_some(KvTarget {
                elements: ELEMS,
                view: KvView::Set,
            })
        };
        let t = translate(&shape(&set), &st).unwrap();
        let c = candidates(&input(t), "h", ELEMS, &stats("__sprig_ke_cache"));
        assert_eq!(c[0].def.ddl_columns(), "`e`(255)");
        // A shape on no view is left alone.
        assert!(translate(&shape(BY_VALUE), &|_| None).is_none());
        assert_eq!(view_of("cache__HASH"), Some(("cache", KvView::Hash)));
        assert_eq!(view_of("__hash"), None);
        assert_eq!(elements_of("db0"), "__sprig_ke_db0");
    }

    const KEYS: ObjectId = ObjectId(80);

    fn key_stats() -> TableStats {
        let mut t = stats("db0");
        for (c, ty) in [
            ("k", "varbinary"),
            ("type", "enum"),
            ("v", "longblob"),
            ("_sprig_dk", "varbinary"),
        ] {
            let (class, lob, fixed) = super::super::stats::reader::classify(ty);
            t.columns.insert(
                c.into(),
                super::super::stats::ColumnInfo {
                    name: c.into(),
                    data_type: ty.into(),
                    class,
                    charset: None,
                    collation: None,
                    nullable: c == "v",
                    max_bytes: fixed,
                    lob,
                    generated: false,
                },
            );
        }
        t.primary_key = vec!["k".into()];
        t
    }

    /// `SELECT k FROM db0 WHERE JSON_VALUE(v, '$.email') <op> ?`, with
    /// the extractor's JSON form of the predicate.
    fn by_field(pred: &str) -> Shape {
        shape(&format!(
            r#"{{"v":1,"type":"select","tables":[{{"alias":"db0","object_id":80,"db":"redis","table":"db0","db_id":1}}],"predicates":[{pred}],"joins":[],"order_by":[],"group_by":[],"select":{{"db0":["k"]}},"limit":false,"writes":null,"or_groups":0}}"#
        ))
    }

    const EMAIL_EQ: &str = r#"{"t":"db0","col":"v","op":"json_path","path":"$.email","func":"JSON_VALUE","cmp":{"op":"eq"},"args":[0]}"#;

    #[test]
    fn a_value_field_gets_a_field_column_and_its_index() {
        let f = input(by_field(EMAIL_EQ));
        let c = field_candidates(&f, "db0", KEYS, &key_stats());
        assert_eq!(c.len(), 1);
        let col = field_column("$.email");
        assert!(col.starts_with("_sprig_p_email_") && col.len() == "_sprig_p_email_".len() + 8);
        assert_eq!(c[0].def.kind, IndexKind::FieldColumn);
        assert_eq!(c[0].def.ddl_columns(), format!("`{col}`(64)"));
        assert_eq!(
            fields_of(&c[0].def),
            vec![(col.clone(), "$.email".to_owned())]
        );
        assert!(is_kv(&c[0]));
        assert_eq!(
            add_fields_sql("t1_redis", "db0", &c[0].def).unwrap(),
            format!(
                "ALTER TABLE `t1_redis`.`db0` ADD COLUMN IF NOT EXISTS `{col}` LONGBLOB AS \
                 (JSON_VALUE(`v`, '$.email')) VIRTUAL INVISIBLE, ALGORITHM=INSTANT"
            )
        );
        assert_eq!(
            drop_fields_sql("t1_redis", "db0", std::slice::from_ref(&col)).unwrap(),
            format!(
                "ALTER TABLE `t1_redis`.`db0` DROP COLUMN IF EXISTS `{col}`, ALGORITHM=INSTANT"
            )
        );
        assert_eq!(maintained_columns(&c[0].def), vec![col, "v".to_owned()]);
        // Writes of `v` maintain the index.
        let mut w = super::super::cost::WriteLoad::default();
        w.updates_h
            .insert(std::iter::once("v".to_owned()).collect(), 100.0);
        assert!((w.touching(c[0].def.columns()) - 0.0).abs() < 1e-9);
        let m = maintained_columns(&c[0].def);
        assert!((w.touching(m.iter().map(String::as_str)) - 100.0).abs() < 1e-9);
        // The generator proposes it instead of skipping the shape (K2).
        let g = super::super::candidates::Generator::default().generate_all(&f, &|o| {
            (o == KEYS).then(|| Box::leak(Box::new(key_stats())) as &TableStats)
        });
        assert_eq!(g.candidates.len(), 1, "{g:?}");
        assert!(g.skipped.is_empty(), "{:?}", g.skipped);
    }

    #[test]
    fn equalities_come_before_one_range_and_unsubstituted_forms_get_nothing() {
        let range = r#"{"t":"db0","col":"v","op":"json_path","path":"$.age","func":"JSON_VALUE","cmp":{"op":"range","lo":true,"hi":false},"args":[1]}"#;
        let inl = r#"{"t":"db0","col":"v","op":"json_path","path":"$.city","func":"JSON_VALUE","cmp":{"op":"in","list":false},"args":[2,3]}"#;
        let f = input(by_field(&format!("{range},{EMAIL_EQ},{inl}")));
        let c = field_candidates(&f, "db0", KEYS, &key_stats());
        let cols: Vec<String> = c[0].def.columns().map(str::to_owned).collect();
        assert_eq!(
            cols,
            [
                field_column("$.city"),
                field_column("$.email"),
                field_column("$.age")
            ]
        );
        assert_eq!(
            c[0].evidence.parts,
            [PartSource::Eq, PartSource::Eq, PartSource::Range]
        );
        // LIKE, `->`, a key lookup, a non-key table: nothing.
        let like = EMAIL_EQ.replace(r#"{"op":"eq"}"#, r#"{"op":"like"}"#);
        let arrow = EMAIL_EQ.replace("JSON_VALUE", "->");
        let old = r#"{"t":"db0","col":"v","op":"json_path","path":"$.email","args":[0]}"#;
        for p in [like.as_str(), arrow.as_str(), old] {
            let f = input(by_field(p));
            assert!(
                field_candidates(&f, "db0", KEYS, &key_stats()).is_empty(),
                "{p}"
            );
            assert!(value_field(&f, "db0", &key_stats()));
        }
        let by_key = format!(r#"{EMAIL_EQ},{{"t":"db0","col":"k","op":"eq","args":[1]}}"#);
        assert!(field_candidates(&input(by_field(&by_key)), "db0", KEYS, &key_stats()).is_empty());
        assert!(
            field_candidates(&input(by_field(EMAIL_EQ)), "db0", KEYS, &stats("db0")).is_empty()
        );
        // Unsafe paths never reach DDL.
        assert!(safe_path("$.\"a b\"[0]"));
        assert!(!safe_path("$.a' OR 1"));
        assert!(!safe_path("email"));
        assert_ne!(field_column("$.a.b"), field_column("$.\"a.b\""));
    }

    /// Decision A4: a Redis write command's guarded batch (one compound
    /// statement) has no shape; the gateway now fingerprints each write
    /// inside it, so the element table's write load, and the write cost of
    /// an element-table candidate, count them.
    #[test]
    fn a_batch_s_writes_reach_the_element_table_write_load() {
        use super::super::cost::CostModel as _;
        use sprig_workload::shape::{ExtractCtx, ResolvedTable, TableResolver, extract};
        struct R;
        impl TableResolver for R {
            fn resolve(&self, _db: Option<&str>, table: &str) -> Option<ResolvedTable> {
                let id = match table {
                    "db0" => KEYS,
                    "__sprig_ke_db0" => ELEMS,
                    _ => return None,
                };
                Some(ResolvedTable {
                    object_id: id,
                    db_id: sprig_common::ids::DbId(1),
                    physical: None,
                })
            }
        }
        let batch = "BEGIN NOT ATOMIC DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; \
            RESIGNAL; END; START TRANSACTION; SELECT 1 INTO @now FROM (SELECT 1 AS one) AS d \
            LEFT JOIN `redis`.`db0` AS t ON t.k = X'61' FOR UPDATE; IF @t IS NULL THEN \
            INSERT INTO `redis`.`db0` (k, type, g, _v, _sprig_dk) VALUES (X'61', 'hash', 7, 9, \
            X'61'); END IF; INSERT INTO `redis`.`__sprig_ke_db0` (k, g, o, e, v, _sprig_dk) \
            VALUES (X'61', 7, 0, 'f', 'v', X'61') ON DUPLICATE KEY UPDATE v = VALUES(v); \
            COMMIT; END";
        let lex = sprig_sql::lex::LexOptions::mariadb();
        let ctx = ExtractCtx {
            lex,
            current_db: Some("redis"),
            lower_case_table_names: false,
        };
        // The whole batch: no writes for the cost model.
        let whole =
            sprig_workload::fingerprint::normalize(batch.as_bytes(), &lex, Some("redis"), false);
        assert!(extract(batch.as_bytes(), &whole, &ctx, &R).writes.is_none());
        // Its writes, each as a fingerprint run 3,600 times in a 1-hour window.
        let fps: Vec<FpInput> =
            sprig_workload::fingerprint::compound::writes(batch.as_bytes(), &lex)
                .into_iter()
                .map(|w| {
                    let text = &batch.as_bytes()[w.range];
                    let norm =
                        sprig_workload::fingerprint::normalize(text, &lex, Some("redis"), false);
                    FpInput {
                        runs: 3_600,
                        mean_ms: 0.0,
                        affected_per_run: 1.0,
                        ..input(extract(text, &norm, &ctx, &R))
                    }
                })
                .collect();
        assert_eq!(fps.len(), 2);
        let params = super::super::cost::CostParams {
            window_hours: 1.0,
            ..super::super::cost::CostParams::default()
        };
        let mut st = stats("__sprig_ke_db0");
        st.n_rows = Some(20_000);
        let t = super::super::cost::TableContext::new(ELEMS, &st, &fps, params);
        // An upsert counts half an insert and half an update of its columns.
        assert!(
            (t.writes.inserts_h - 1_800.0).abs() < 1e-6,
            "{:?}",
            t.writes
        );
        let c = candidates(
            &input(translate(&shape(BY_VALUE), &target).unwrap()),
            "h",
            ELEMS,
            &st,
        );
        let e = super::super::cost::Model.estimate(&c[0], &t);
        // 1,800 inserts + 1,800 updates touching `e` and `v`, at 0.05 ms.
        assert!((e.write_ms_h - 180.0).abs() < 1e-6, "{e:?}");
    }

    #[test]
    fn the_replay_reads_a_view_without_the_candidate() {
        let sql = "SELECT k FROM db0__hash WHERE field = 'email' AND value = 'u7@x.io'";
        let a =
            ignore_through_view(sql, "s1", "__sprig_ke_db0", "sprig_auto_1", Some("s1")).unwrap();
        assert_eq!(
            a,
            "SELECT k FROM (SELECT t.k AS k, e.e AS field, e.v AS value FROM `s1`.`db0` AS t \
             JOIN `s1`.`__sprig_ke_db0` AS e IGNORE INDEX (`sprig_auto_1`) ON e.k = t.k AND \
             e.g = t.g WHERE t.type = 'hash' AND (t.expires_at IS NULL OR t.expires_at > \
             UTC_TIMESTAMP(3))) AS `db0__hash` WHERE field = 'email' AND value = 'u7@x.io'"
        );
        let aliased = "SELECT h.k FROM s1.db0__set h WHERE h.member = 'x'";
        let b = ignore_through_view(aliased, "s1", "__sprig_ke_db0", "i", None).unwrap();
        assert!(
            b.starts_with("SELECT h.k FROM (SELECT t.k AS k, e.e AS member FROM"),
            "{b}"
        );
        assert!(
            b.ends_with("UTC_TIMESTAMP(3))) h WHERE h.member = 'x'"),
            "{b}"
        );
        assert!(ignore_through_view(sql, "s1", "orders", "i", Some("s1")).is_none());
        assert!(ignore_through_view(sql, "s2", "__sprig_ke_db0", "i", Some("s1")).is_none());
    }

    /// Decision X34: a fingerprint stored before R6 round 6 (shape version
    /// 1, `json_path` without `func` and `cmp`) gets no field column; once
    /// a gateway sees the statement again, the store rewrites the row, and
    /// the shape read back gets the candidate. The store part runs against
    /// `SPRIG_TEST_MARIADB_SINGLE` (skipped without it).
    #[tokio::test]
    async fn an_old_fingerprint_row_gets_a_field_column_once_rewritten() {
        use mysql_async::prelude::Queryable;
        use sprig_common::ids::{DbId, TenantId};
        use sprig_workload::fingerprint::{FpFlags, FpKind, FrontEnd};
        use sprig_workload::recorder::{FpMeta, NewFingerprint};
        use sprig_workload::store::WorkloadTables;

        const OLD_EMAIL_EQ: &str =
            r#"{"t":"db0","col":"v","op":"json_path","path":"$.email","args":[0]}"#;
        let old = by_field(OLD_EMAIL_EQ);
        assert_eq!(old.version, 1);
        assert!(old.all_unsargable());
        assert!(field_candidates(&input(old), "db0", KEYS, &key_stats()).is_empty());

        let Some(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE")
            .ok()
            .filter(|v| !v.trim().is_empty())
        else {
            eprintln!("skipped the store part: set SPRIG_TEST_MARIADB_SINGLE to run it");
            return;
        };
        let opts = mysql_async::Opts::from_url(&url).unwrap();
        let mut conn = mysql_async::Conn::new(opts).await.unwrap();
        let schema = format!("__sprig_test_x34_{}", std::process::id());
        let q = sprig_common::names::quote(&schema);
        conn.query_drop(format!("CREATE DATABASE {q}"))
            .await
            .unwrap();
        sprig_workload::flush::migrate(&mut conn, &schema, "test")
            .await
            .unwrap();
        let t = WorkloadTables::new(&schema);
        let id = sprig_common::ids::Fingerprint(0x0034);
        let n = NewFingerprint {
            tenant: TenantId(1),
            meta: Arc::new(FpMeta {
                id,
                kind: FpKind::Sql,
                text: Arc::from("SELECT k FROM db0 WHERE JSON_VALUE(v, ?) = ?"),
                shape: Arc::new(Shape {
                    version: sprig_workload::shape::SHAPE_VERSION,
                    ..by_field(EMAIL_EQ)
                }),
                front_end: FrontEnd::Mysql,
                caller: None,
                db_id: DbId(1),
                flags: FpFlags::default(),
            }),
            first_seen: std::time::SystemTime::now(),
        };
        t.insert_fingerprints(&mut conn, std::slice::from_ref(&n))
            .await
            .unwrap();
        // The row as a build before R6 round 6 left it.
        let stored_old = by_field(OLD_EMAIL_EQ).to_json();
        conn.exec_drop(
            format!("UPDATE {q}.workload_fingerprints SET shape = ?, shape_version = 1"),
            (stored_old,),
        )
        .await
        .unwrap();
        let before = t.fingerprints(&mut conn, &[id]).await.unwrap();
        let none = field_candidates(
            &input((*before[0].shape).clone()),
            "db0",
            KEYS,
            &key_stats(),
        );
        // A gateway records the statement again (its first sighting since
        // it started on this build).
        let rewritten = t.insert_fingerprints(&mut conn, &[n]).await.unwrap();
        let after = t.fingerprints(&mut conn, &[id]).await.unwrap();
        conn.query_drop(format!("DROP DATABASE {q}")).await.unwrap();
        assert!(none.is_empty(), "{none:?}");
        assert_eq!(rewritten, 2);
        let c = field_candidates(&input((*after[0].shape).clone()), "db0", KEYS, &key_stats());
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].def.kind, IndexKind::FieldColumn);
        assert_eq!(
            fields_of(&c[0].def),
            vec![(field_column("$.email"), "$.email".to_owned())]
        );
    }
}
