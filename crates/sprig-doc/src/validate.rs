//! Schema validation:
//! a collection's `validator` (query operators and `$jsonSchema`), its
//! `validationLevel` (`off`, `moderate`, `strict`, and MongoDB 9.0's
//! `constraint`) and `validationAction` (`error`, `warn`, and 8.1's
//! `errorAndLog`), checked on every write the engine makes: inserts,
//! updates and replacements, upserts, `findAndModify`, `bulkWrite`,
//! `$merge` and `$out`.
//!
//! A failing write gets 121 `DocumentValidationFailure` with MongoDB 5.0+'s
//! `errInfo`: `{failingDocumentId, details}`, where `details` explains the
//! failure clause by clause ([`explain`]) and `$jsonSchema` keyword by
//! keyword ([`schema::Schema::explain`]).
//!
//! `bypassDocumentValidation: true` on a command runs its writes inside
//! [`bypass`]; the engine then skips the rules, except on a collection at
//! level `constraint`.

pub mod bits;
pub mod schema;

use std::future::Future;

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::agg::{AggExpr, Vars};
use crate::filter::{self, Pred, values_at};

/// 121 `DocumentValidationFailure`.
pub const DOCUMENT_VALIDATION_FAILURE: i32 = 121;
/// 72 `InvalidOptions`.
pub const INVALID_OPTIONS: i32 = 72;

tokio::task_local! {
    static BYPASS: bool;
}

/// Runs `f` with document validation bypassed when `on`
/// (`bypassDocumentValidation: true`).
pub async fn bypass<F: Future>(on: bool, f: F) -> F::Output {
    if on {
        BYPASS.scope(true, f).await
    } else {
        f.await
    }
}

fn bypassed() -> bool {
    BYPASS.try_with(|b| *b).unwrap_or(false)
}

/// `validationLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// No validation.
    Off,
    /// Inserts, and updates of documents that were valid.
    Moderate,
    /// Every insert and update (the default).
    Strict,
    /// MongoDB 9.0: every insert and update, every document in the
    /// collection valid, and no bypass.
    Constraint,
}

impl Level {
    /// Parses a level name.
    ///
    /// # Errors
    /// 2 `BadValue` for anything else.
    pub fn parse(v: &Bson) -> Result<Self, MongoError> {
        match v.as_str() {
            Some("off") => Ok(Self::Off),
            Some("moderate") => Ok(Self::Moderate),
            Some("strict") => Ok(Self::Strict),
            Some("constraint") => Ok(Self::Constraint),
            Some(s) => Err(MongoError::new(
                code::BAD_VALUE,
                format!(
                    "Enumeration value '{s}' for field 'validationLevel' is not a valid value."
                ),
            )),
            None => Err(MongoError::new(
                code::TYPE_MISMATCH,
                "BSON field 'validationLevel' is the wrong type, expected type 'string'",
            )),
        }
    }

    /// The level's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Moderate => "moderate",
            Self::Strict => "strict",
            Self::Constraint => "constraint",
        }
    }
}

/// `validationAction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Refuse the write (the default).
    Error,
    /// Log and accept.
    Warn,
    /// MongoDB 8.1: log and refuse.
    ErrorAndLog,
}

impl Action {
    /// Parses an action name.
    ///
    /// # Errors
    /// 2 `BadValue` for anything else.
    pub fn parse(v: &Bson) -> Result<Self, MongoError> {
        match v.as_str() {
            Some("error") => Ok(Self::Error),
            Some("warn") => Ok(Self::Warn),
            Some("errorAndLog") => Ok(Self::ErrorAndLog),
            Some(s) => Err(MongoError::new(
                code::BAD_VALUE,
                format!(
                    "Enumeration value '{s}' for field 'validationAction' is not a valid value."
                ),
            )),
            None => Err(MongoError::new(
                code::TYPE_MISMATCH,
                "BSON field 'validationAction' is the wrong type, expected type 'string'",
            )),
        }
    }
}

/// A collection's validation rules.
#[derive(Debug, Clone)]
pub struct Rules {
    /// The validator as written.
    pub validator: Document,
    /// The validator, parsed.
    pub pred: Pred,
    /// `validationLevel`.
    pub level: Level,
    /// `validationAction`.
    pub action: Action,
    /// `collMod`'s `prepareConstraintValidationLevel` was run: no bypass.
    pub prepared: bool,
}

/// Checks a validator document as `create` and `collMod` accept it.
///
/// # Errors
/// The filter's parse errors; 2 for operators MongoDB refuses in
/// validators (`$text`, `$where`, `$near`, `$nearSphere`, `$geoNear`).
pub fn parse_validator(v: &Document) -> Result<Pred, MongoError> {
    fn walk(v: &Bson) -> Result<(), MongoError> {
        match v {
            Bson::Document(d) => {
                for (k, x) in d {
                    if matches!(
                        k.as_str(),
                        "$text" | "$where" | "$near" | "$nearSphere" | "$geoNear"
                    ) {
                        return Err(MongoError::new(
                            code::BAD_VALUE,
                            format!("{k} is not allowed in this context"),
                        ));
                    }
                    if k == "$jsonSchema" || k == "$expr" {
                        continue;
                    }
                    walk(x)?;
                }
                Ok(())
            }
            Bson::Array(a) => a.iter().try_for_each(walk),
            _ => Ok(()),
        }
    }
    walk(&Bson::Document(v.clone()))?;
    filter::parse(v)
}

impl Rules {
    /// The rules in a collection's options; `None` without a validator.
    ///
    /// # Errors
    /// A stored validator that no longer parses.
    pub fn of(options: &Document) -> Result<Option<Self>, MongoError> {
        let Ok(v) = options.get_document("validator") else {
            return Ok(None);
        };
        if v.is_empty() {
            return Ok(None);
        }
        let level = options
            .get("validationLevel")
            .map_or(Ok(Level::Strict), Level::parse)?;
        let action = options
            .get("validationAction")
            .map_or(Ok(Action::Error), Action::parse)?;
        if level == Level::Off {
            return Ok(None);
        }
        Ok(Some(Self {
            validator: v.clone(),
            pred: parse_validator(v)?,
            level,
            action,
            prepared: options.get_bool(PREPARED).unwrap_or(false),
        }))
    }

    /// Whether `d` satisfies the validator.
    #[must_use]
    pub fn valid(&self, d: &Document) -> bool {
        self.pred.matches_with(
            d,
            &AggExpr {
                now: bson::DateTime::now(),
            },
        )
    }

    /// The 121 error for `d`, which fails the validator.
    #[must_use]
    pub fn failure(&self, d: &Document) -> MongoError {
        let details = explain(&self.validator, d);
        let mut info = Document::new();
        if let Some(id) = d.get("_id") {
            info.insert("failingDocumentId", id.clone());
        }
        info.insert("details", details);
        MongoError::new(DOCUMENT_VALIDATION_FAILURE, "Document failed validation")
            .with_extra(doc! {"errInfo": info})
    }

    fn decide(&self, ns: &str, d: &Document) -> Result<(), MongoError> {
        if self.valid(d) {
            return Ok(());
        }
        let e = self.failure(d);
        match self.action {
            Action::Warn | Action::ErrorAndLog => {
                let info = e
                    .extra
                    .as_ref()
                    .and_then(|x| x.get("errInfo"))
                    .map(ToString::to_string)
                    .unwrap_or_default();
                tracing::warn!(
                    target: "sprig_doc::validate",
                    namespace = ns,
                    error_info = %info,
                    action = if self.action == Action::Warn { "warn" } else { "errorAndLog" },
                    "Document would fail validation"
                );
                if self.action == Action::Warn {
                    Ok(())
                } else {
                    Err(e)
                }
            }
            Action::Error => Err(e),
        }
    }

    fn skip(&self) -> bool {
        self.level != Level::Constraint && !self.prepared && bypassed()
    }

    /// Checks a document about to be inserted.
    ///
    /// # Errors
    /// 121 when it fails and the action refuses it.
    pub fn check_insert(&self, ns: &str, d: &Document) -> Result<(), MongoError> {
        if self.skip() {
            return Ok(());
        }
        self.decide(ns, d)
    }

    /// Checks an update of `old` to `new`.
    ///
    /// # Errors
    /// 121 when `new` fails and the action refuses it (at `moderate`,
    /// only when `old` was valid).
    pub fn check_update(&self, ns: &str, old: &Document, new: &Document) -> Result<(), MongoError> {
        if self.skip() {
            return Ok(());
        }
        if self.level == Level::Moderate && !self.valid(old) {
            return Ok(());
        }
        self.decide(ns, new)
    }
}

/// The options `create` and `collMod` take for validation, checked:
/// `validator`, `validationLevel`, `validationAction`.
///
/// # Errors
/// The validator's parse errors; 2 for a bad level or action; 72 for
/// `constraint` with `warn`.
pub fn check_options(options: &Document) -> Result<(), MongoError> {
    if let Some(v) = options.get("validator") {
        let Bson::Document(v) = v else {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "BSON field 'validator' is the wrong type, expected type 'object'",
            ));
        };
        parse_validator(v)?;
    }
    let level = options
        .get("validationLevel")
        .map(Level::parse)
        .transpose()?;
    let action = options
        .get("validationAction")
        .map(Action::parse)
        .transpose()?;
    if level == Some(Level::Constraint) && action == Some(Action::Warn) {
        return Err(MongoError::new(
            INVALID_OPTIONS,
            "validationLevel 'constraint' requires validationAction 'error' or 'errorAndLog'",
        ));
    }
    Ok(())
}

/// The option `collMod`'s `prepareConstraintValidationLevel` leaves on a
/// collection.
pub const PREPARED: &str = "prepareConstraintValidationLevel";

#[allow(clippy::needless_pass_by_value)]
fn prefixed(cmd: &str, e: MongoError) -> MongoError {
    if e.code == code::TYPE_MISMATCH && e.message.starts_with("BSON field") {
        return MongoError::new(
            e.code,
            e.message
                .replacen("BSON field '", &format!("BSON field '{cmd}."), 1),
        );
    }
    MongoError::new(
        e.code,
        format!(
            "Parsing of collection validator failed :: caused by :: {}",
            e.message
        ),
    )
}

/// Checks `create`'s validation options.
///
/// # Errors
/// As [`check_options`], with MongoDB's prefixes.
pub fn check_create(options: &Document) -> Result<(), MongoError> {
    if let Some(v) = options.get("validator")
        && !matches!(v, Bson::Document(_))
    {
        return Err(MongoError::new(
            code::TYPE_MISMATCH,
            format!(
                "BSON field 'create.validator' is the wrong type '{}', expected type 'object'",
                schema::type_alias(v)
            ),
        ));
    }
    check_options(options).map_err(|e| {
        if e.code == code::BAD_VALUE && e.message.starts_with("Enumeration") {
            MongoError::new(e.code, e.message.replacen("field '", "field 'create.", 1))
        } else if e.code == INVALID_OPTIONS || e.message.starts_with("BSON field") {
            e
        } else {
            prefixed("create", e)
        }
    })
}

/// `collMod`'s validation options (`validator`, `validationLevel`,
/// `validationAction`, and 9.0's `prepareConstraintValidationLevel`):
/// the collection's new options, or `None` when the command has none of
/// them. Raising the level to `constraint` first checks every document
/// ([`crate::engine::Engine::find`] with the validator negated).
///
/// # Errors
/// The options' errors; 72 for a change MongoDB 9.0 refuses at level
/// `constraint`; 121 when a document fails the validator.
pub async fn coll_mod(
    engine: &crate::engine::Engine,
    sql: &mut dyn crate::engine::SqlRunner,
    ns: &crate::engine::Ns,
    current: &Document,
    body: &Document,
) -> Result<Option<Document>, MongoError> {
    let keys = ["validator", "validationLevel", "validationAction", PREPARED];
    if !keys.iter().any(|k| body.contains_key(k)) {
        return Ok(None);
    }
    if let Some(v) = body.get("validator")
        && !matches!(v, Bson::Document(_))
    {
        return Err(MongoError::new(
            code::TYPE_MISMATCH,
            format!(
                "BSON field 'collMod.validator' is the wrong type '{}', expected type 'object'",
                schema::type_alias(v)
            ),
        ));
    }
    let mut opts = current.clone();
    let old_level = current
        .get("validationLevel")
        .map_or(Ok(Level::Strict), Level::parse)?;
    let fix = |e: MongoError| {
        if e.code == code::BAD_VALUE && e.message.starts_with("Enumeration") {
            MongoError::new(e.code, e.message.replacen("field '", "field 'collMod.", 1))
        } else if e.code == INVALID_OPTIONS || e.message.starts_with("BSON field") {
            e
        } else {
            prefixed("collMod", e)
        }
    };
    check_options(body).map_err(fix)?;
    let level = body
        .get("validationLevel")
        .map_or(Ok(old_level), Level::parse)?;
    let action = body
        .get("validationAction")
        .or_else(|| current.get("validationAction"))
        .map_or(Ok(Action::Error), Action::parse)?;
    if let Some(p) = body.get(PREPARED) {
        if filter::truthy(p) {
            opts.insert(PREPARED, true);
        } else {
            opts.remove(PREPARED);
        }
    }
    let prepared = opts.get_bool(PREPARED).unwrap_or(false);
    let changes_rules = body
        .get("validator")
        .is_some_and(|v| current.get("validator") != Some(v));
    if changes_rules && (prepared || old_level == Level::Constraint || level == Level::Constraint) {
        return Err(MongoError::new(
            INVALID_OPTIONS,
            "the validator can't change while validationLevel is 'constraint' or being prepared \
             for it: lower validationLevel to 'strict' first",
        ));
    }
    if level == Level::Constraint {
        if action == Action::Warn {
            return Err(MongoError::new(
                INVALID_OPTIONS,
                "validationLevel 'constraint' requires validationAction 'error' or 'errorAndLog'",
            ));
        }
        if old_level != Level::Constraint && !prepared {
            return Err(MongoError::new(
                INVALID_OPTIONS,
                "run collMod with prepareConstraintValidationLevel: true before raising \
                 validationLevel to 'constraint'",
            ));
        }
    }
    if let Some(v) = body.get("validator") {
        opts.insert("validator", v.clone());
    }
    opts.insert("validationLevel", level.name());
    opts.insert(
        "validationAction",
        match action {
            Action::Error => "error",
            Action::Warn => "warn",
            Action::ErrorAndLog => "errorAndLog",
        },
    );
    if level != Level::Constraint
        && body.contains_key("validationLevel")
        && !body.contains_key(PREPARED)
    {
        opts.remove(PREPARED);
    }
    // Raising the level to `constraint`: every document must pass.
    if level == Level::Constraint
        && old_level != Level::Constraint
        && let Ok(v) = opts.get_document("validator")
        && !v.is_empty()
    {
        let spec = crate::engine::FindSpec {
            filter: doc! {"$nor": [v.clone()]},
            sort: None,
            projection: None,
            skip: 0,
            limit: 1,
        };
        let mut plan = engine.find(sql, ns, spec).await?;
        let found = engine.next_batch(sql, &mut plan, 1, 16 << 20).await?;
        if let Some(d) = found.first() {
            let rules = Rules::of(&opts)?;
            let e = rules.map_or_else(
                || MongoError::new(DOCUMENT_VALIDATION_FAILURE, "Document failed validation"),
                |r| r.failure(d),
            );
            return Err(MongoError {
                message: "collMod failed: an existing document does not satisfy the validator \
                          (validationLevel 'constraint')"
                    .into(),
                ..e
            });
        }
    }
    Ok(Some(opts))
}

// --- errInfo for query-operator validators ---------------------------------

/// One clause of a filter, as MongoDB's parser splits it.
#[allow(clippy::large_enum_variant)]
enum Clause<'a> {
    /// `$and`, `$or`, `$nor` with their sub-filters.
    Logic(&'a str, &'a [Bson]),
    /// `$expr`.
    Expr(&'a Bson),
    /// `$jsonSchema`.
    Schema(&'a Bson),
    /// One operator on a path, as written (`{path: {op: arg}}` or
    /// `{path: literal}`).
    Leaf {
        path: &'a str,
        op: String,
        arg: Bson,
        spec: Document,
    },
}

fn clauses(f: &Document) -> Vec<Clause<'_>> {
    let mut out = Vec::new();
    for (k, v) in f {
        match k.as_str() {
            "$and" | "$or" | "$nor" => {
                if let Bson::Array(a) = v {
                    out.push(Clause::Logic(k.as_str(), a.as_slice()));
                }
            }
            "$expr" => out.push(Clause::Expr(v)),
            "$jsonSchema" => out.push(Clause::Schema(v)),
            "$comment" => {}
            _ => match v {
                Bson::Document(d) if d.keys().next().is_some_and(|x| x.starts_with('$')) => {
                    for (op, arg) in d {
                        match op.as_str() {
                            "$options" | "$comment" | "$maxDistance" | "$minDistance" => {}
                            "$regex" => {
                                let mut inner = doc! {"$regex": arg.clone()};
                                if let Some(o) = d.get("$options") {
                                    inner.insert("$options", o.clone());
                                }
                                out.push(Clause::Leaf {
                                    path: k.as_str(),
                                    op: op.clone(),
                                    arg: arg.clone(),
                                    spec: doc! {k.clone(): inner},
                                });
                            }
                            _ => out.push(Clause::Leaf {
                                path: k.as_str(),
                                op: op.clone(),
                                arg: arg.clone(),
                                spec: doc! {k.clone(): {op.clone(): arg.clone()}},
                            }),
                        }
                    }
                }
                Bson::RegularExpression(_) => out.push(Clause::Leaf {
                    path: k.as_str(),
                    op: "$regex".into(),
                    arg: v.clone(),
                    spec: doc! {k.clone(): v.clone()},
                }),
                _ => out.push(Clause::Leaf {
                    path: k.as_str(),
                    op: "$eq".into(),
                    arg: v.clone(),
                    spec: doc! {k.clone(): v.clone()},
                }),
            },
        }
    }
    out
}

fn clause_filter(c: &Clause<'_>) -> Document {
    match c {
        Clause::Logic(op, a) => doc! {*op: a.to_vec()},
        Clause::Expr(e) => doc! {"$expr": (*e).clone()},
        Clause::Schema(s) => doc! {"$jsonSchema": (*s).clone()},
        Clause::Leaf { spec, .. } => spec.clone(),
    }
}

fn now_cx() -> AggExpr {
    AggExpr {
        now: bson::DateTime::now(),
    }
}

fn matches(f: &Document, d: &Document) -> bool {
    filter::parse(f).is_ok_and(|p| p.matches_with(d, &now_cx()))
}

/// MongoDB's `errInfo.details` for document `d` and filter `f` (the
/// validator): why `d` doesn't match.
#[must_use]
pub fn explain(f: &Document, d: &Document) -> Document {
    explain_doc(f, d, false)
}

fn idx(i: usize) -> i32 {
    i32::try_from(i).unwrap_or(i32::MAX)
}

/// `inv`: explaining why the filter matched (under `$not` or `$nor`).
fn explain_doc(f: &Document, d: &Document, inv: bool) -> Document {
    let cs = clauses(f);
    if cs.len() == 1 {
        return explain_clause(&cs[0], d, inv);
    }
    let kids: Vec<Document> = cs.iter().map(clause_filter).collect();
    explain_logic("$and", &kids, d, inv, |i, inv| {
        explain_clause(&cs[i], d, inv)
    })
}

fn explain_logic(
    op: &str,
    kids: &[Document],
    d: &Document,
    inv: bool,
    detail: impl Fn(usize, bool) -> Document,
) -> Document {
    // Which children to list: the failing ones of a failing `$and`/`$or`,
    // the matching ones of a matching `$and`/`$or` (inverted) or of a
    // failing `$nor`.
    let want_matching = match op {
        "$nor" => !inv,
        _ => inv,
    };
    let mut list = Vec::new();
    for (i, k) in kids.iter().enumerate() {
        let m = matches(k, d);
        if m == want_matching {
            list.push(doc! {"index": idx(i), "details": detail(i, want_matching)});
        }
    }
    let key = if want_matching {
        "clausesSatisfied"
    } else {
        "clausesNotSatisfied"
    };
    doc! {"operatorName": op, key: list}
}

fn explain_clause(c: &Clause<'_>, d: &Document, inv: bool) -> Document {
    match c {
        Clause::Logic(op, a) => {
            let kids: Vec<Document> = a
                .iter()
                .map(|x| x.as_document().cloned().unwrap_or_default())
                .collect();
            let kids2 = kids.clone();
            explain_logic(op, &kids, d, inv, move |i, inv| {
                explain_doc(&kids2[i], d, inv)
            })
        }
        Clause::Expr(e) => {
            let r = crate::agg::eval(e, &Vars::new(d, bson::DateTime::now()));
            let mut out = doc! {
                "operatorName": "$expr",
                "specifiedAs": {"$expr": (*e).clone()},
                "reason": if inv { "expression did match" } else { "expression did not match" },
            };
            if let Ok(v) = r {
                out.insert("expressionResult", v);
            }
            out
        }
        Clause::Schema(s) => {
            let mut out = doc! {"operatorName": "$jsonSchema"};
            if let Some(sd) = s.as_document()
                && let Ok(sch) = schema::Schema::parse_root(sd)
            {
                let (t, desc) = sch.annotations();
                if let Some(t) = t {
                    out.insert("title", t);
                }
                if let Some(t) = desc {
                    out.insert("description", t);
                }
                if inv {
                    out.insert("reason", "schema matched");
                } else {
                    out.insert(
                        "schemaRulesNotSatisfied",
                        sch.explain(&Bson::Document(d.clone())),
                    );
                }
            }
            out
        }
        Clause::Leaf {
            path,
            op,
            arg,
            spec,
        } => explain_leaf(path, op, arg, spec, d, inv),
    }
}

fn reason(op: &str, inv: bool) -> &'static str {
    let (no, yes) = match op {
        "$eq" | "$gt" | "$gte" | "$lt" | "$lte" => ("comparison failed", "comparison succeeded"),
        "$ne" => ("comparison succeeded", "comparison failed"),
        "$in" => (
            "no matching value found in array",
            "matching value found in array",
        ),
        "$nin" => (
            "matching value found in array",
            "no matching value found in array",
        ),
        "$type" => ("type did not match", "type did match"),
        "$size" => (
            "array length was not equal to given size",
            "array length was equal to given size",
        ),
        "$regex" => (
            "regular expression did not match",
            "regular expression did match",
        ),
        "$all" => (
            "array did not contain all specified values",
            "array did contain all specified values",
        ),
        "$elemMatch" => (
            "array did not satisfy the child predicate",
            "array did satisfy the child predicate",
        ),
        "$mod" => (
            "$mod did not evaluate to expected remainder",
            "$mod did evaluate to expected remainder",
        ),
        "$bitsAllSet" | "$bitsAnySet" | "$bitsAllClear" | "$bitsAnyClear" => (
            "bitwise operator failed to match",
            "bitwise operator matched successfully",
        ),
        "$geoWithin" | "$within" => (
            "none of considered geometries was contained within the expression's geometry",
            "at least one of considered geometries was contained within the expression's geometry",
        ),
        "$geoIntersects" => (
            "none of considered geometries intersected the expression's geometry",
            "at least one of considered geometries intersected the expression's geometry",
        ),
        _ => ("expression did not match", "expression did match"),
    };
    if inv { yes } else { no }
}

fn explain_leaf(
    path: &str,
    op: &str,
    arg: &Bson,
    spec: &Document,
    d: &Document,
    inv: bool,
) -> Document {
    if op == "$not" {
        let inner = match arg {
            Bson::Document(n) => doc! {path: n.clone()},
            other => doc! {path: other.clone()},
        };
        return doc! {"operatorName": "$not", "details": explain_doc(&inner, d, !inv)};
    }
    let mut out = doc! {"operatorName": op, "specifiedAs": spec.clone()};
    let vs = values_at(d, path);
    if op == "$exists" {
        let want = filter::truthy(arg);
        // Fails when presence differs from what is wanted (or, inverted,
        // matches when it is the same).
        let present = !vs.leaves.is_empty();
        out.insert(
            "reason",
            if present {
                "path does exist"
            } else {
                "path does not exist"
            },
        );
        let _ = want;
        return out;
    }
    if vs.leaves.is_empty() {
        out.insert("reason", "field was missing");
        return out;
    }
    let whole = matches!(op, "$size" | "$all" | "$elemMatch") || vs.positional;
    if matches!(op, "$size" | "$elemMatch") && !vs.leaves.iter().any(|v| v.as_array().is_some()) {
        let v = vs.leaves[0];
        out.insert("reason", "type did not match");
        out.insert("consideredType", schema::type_alias(v));
        out.insert("expectedType", "array");
        out.insert("consideredValue", v.clone());
        return out;
    }
    let vals: Vec<Bson> = if whole {
        vs.leaves.iter().map(|v| (*v).clone()).collect()
    } else {
        let mut out = Vec::new();
        for v in &vs.leaves {
            match v {
                Bson::Array(a) => out.extend(a.iter().cloned()),
                other => out.push((*other).clone()),
            }
        }
        out
    };
    let mut tys: Vec<&str> = vals.iter().map(schema::type_alias).collect();
    tys.sort_unstable();
    tys.dedup();
    let put_types = |out: &mut Document| match tys.len() {
        0 => {}
        1 => {
            out.insert("consideredType", tys[0]);
        }
        _ => {
            out.insert("consideredTypes", tys.clone());
        }
    };
    let put_values = |out: &mut Document| {
        if vals.len() == 1 {
            out.insert("consideredValue", vals[0].clone());
        } else {
            out.insert("consideredValues", vals.clone());
        }
    };
    // Operators that apply to some types only: when no value has one of
    // them, MongoDB says so instead.
    let expected: &[&str] = match op {
        "$regex" => &["regex", "string", "symbol"],
        "$mod" => &["decimal", "double", "int", "long"],
        "$bitsAllSet" | "$bitsAnySet" | "$bitsAllClear" | "$bitsAnyClear" => {
            &["binData", "decimal", "double", "int", "long"]
        }
        _ => &[],
    };
    if !inv
        && !expected.is_empty()
        && !vals
            .iter()
            .any(|v| expected.contains(&schema::type_alias(v)))
    {
        out.insert("reason", "type did not match");
        put_types(&mut out);
        out.insert("expectedTypes", expected.to_vec());
        put_values(&mut out);
        return out;
    }
    out.insert("reason", reason(op, inv));
    put_values(&mut out);
    if op == "$type" {
        put_types(&mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_validator_details() {
        let v = doc! {"a": {"$gt": 1, "$lt": 5}};
        let e = explain(&v, &doc! {"_id": 1, "a": 7});
        assert_eq!(e.get_str("operatorName").unwrap(), "$and");
        let l = e.get_array("clausesNotSatisfied").unwrap();
        assert_eq!(l.len(), 1);
        let e = explain(&doc! {"a": 1}, &doc! {"_id": 1, "a": [2, 3]});
        assert!(e.contains_key("consideredValues"));
    }

    /// The cost of a check (run with `--ignored --nocapture`).
    #[test]
    #[ignore = "timing, not a check"]
    fn cost_of_a_check() {
        let r = Rules::of(&doc! {"validator": {"$jsonSchema": {
        "bsonType": "object", "required": ["name", "qty", "tags"],
        "properties": {
            "name": {"bsonType": "string", "maxLength": 64},
            "qty": {"bsonType": "int", "minimum": 0},
            "price": {"bsonType": ["double", "decimal"]},
            "tags": {"bsonType": "array", "items": {"bsonType": "string"}},
            "status": {"enum": ["new", "paid", "sent"]},
        }}}})
        .unwrap()
        .unwrap();
        let d = doc! {"_id": 1, "name": "widget", "qty": 5, "price": 2.5, "tags": ["a", "b", "c"],
        "status": "paid", "x1": 1, "x2": "two", "x3": [1, 2], "x4": {"y": 1}};
        let n = 20_000u32;
        let t = std::time::Instant::now();
        for _ in 0..n {
            assert!(r.check_insert("d.c", &d).is_ok());
        }
        let ok = t.elapsed() / n;
        let bad = doc! {"_id": 2, "name": 5, "qty": -1, "tags": [1]};
        let t = std::time::Instant::now();
        for _ in 0..n {
            assert!(r.check_insert("d.c", &bad).is_err());
        }
        eprintln!(
            "check: {ok:?} per valid document, {:?} per refused one (with errInfo)",
            t.elapsed() / n
        );
    }

    #[test]
    fn levels_and_actions() {
        let r = Rules::of(&doc! {"validator": {"a": 1}, "validationLevel": "moderate"})
            .unwrap()
            .unwrap();
        assert!(r.check_insert("d.c", &doc! {"a": 2}).is_err());
        assert!(
            r.check_update("d.c", &doc! {"a": 3}, &doc! {"a": 2})
                .is_ok()
        );
        assert!(
            r.check_update("d.c", &doc! {"a": 1}, &doc! {"a": 2})
                .is_err()
        );
        let w = Rules::of(&doc! {"validator": {"a": 1}, "validationAction": "warn"})
            .unwrap()
            .unwrap();
        assert!(w.check_insert("d.c", &doc! {"a": 2}).is_ok());
        assert!(
            Rules::of(&doc! {"validator": {"a": 1}, "validationLevel": "off"})
                .unwrap()
                .is_none()
        );
    }
}
