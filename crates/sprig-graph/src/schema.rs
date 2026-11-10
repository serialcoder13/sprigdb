//! Indexes and uniqueness constraints (chapter 43, "Indexes and
//! constraints"; build step 15): the rules every store follows, the
//! values an index column holds, and `SHOW INDEXES` / `SHOW CONSTRAINTS`.
//!
//! - **Names.** An index and a constraint share one namespace per graph;
//!   a nameless one gets `index_<hash>` or `constraint_<hash>` of its
//!   schema. A name in use, an equivalent index or constraint, or an index
//!   where a constraint covers the same schema (or the reverse) fail with
//!   39908 and Neo4j's status code.
//! - **Columns.** On MariaDB each property is a virtual column of the node
//!   or edge table holding a typed text of the value: `s` + a string, `n` +
//!   a number, `b` + a boolean (`TEXT` indexes: strings only), and `NULL`
//!   for other types or other labels. Texts over 250 characters become
//!   `h` + their `SHA-256`, so the index stays within `InnoDB`'s key size and
//!   uniqueness stays exact. [`index_value`] computes the same text, so an
//!   equality pushes down as `col IN (...)`.
//! - **Built-in lookups.** Neo4j's two token lookup indexes show as the
//!   label and edge tables, which serve the same scans.

use std::fmt::Write as _;

use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};

use crate::ast::{Entity, IndexKind, Schema};
use crate::store::IndexDef;
use crate::value::Value;

/// Longest typed text kept as is in an index column.
pub const MAX_INLINE: usize = 250;

fn neo(code: &str, msg: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::GraphDefinitionInvalid,
        format!("[Neo.ClientError.Schema.{code}] {msg}"),
    )
}

/// A schema's description, as Neo4j words it: `(:Person {email})`.
#[must_use]
pub fn describe(entity: Entity, token: &str, props: &[String]) -> String {
    match entity {
        Entity::Node => format!("(:{token} {{{}}})", props.join(", ")),
        Entity::Relationship => format!("()-[:{token} {{{}}}]-()", props.join(", ")),
    }
}

/// The generated name of a nameless index or constraint.
#[must_use]
pub fn default_name(unique: bool, entity: Entity, token: &str, props: &[String]) -> String {
    let text = format!("{unique}|{entity:?}|{token}|{}", props.join("|"));
    let d = ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    let h = d.as_ref()[..4].iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    });
    format!("{}_{h}", if unique { "constraint" } else { "index" })
}

/// Checks a `CREATE INDEX` / `CREATE CONSTRAINT` against the graph's
/// definitions: the definition to make (with `id` 0 and `token_id` 0), or
/// `None` when `IF NOT EXISTS` finds it there.
///
/// # Errors
/// 39908 with Neo4j's status code, as the module docs say.
pub fn plan_create(existing: &[IndexDef], cmd: &Schema) -> Result<Option<IndexDef>> {
    let Schema::Create {
        name,
        if_not_exists,
        unique,
        kind,
        entity,
        token,
        props,
    } = cmd
    else {
        return Err(SprigError::internal("plan_create of a drop"));
    };
    let name = name
        .clone()
        .unwrap_or_else(|| default_name(*unique, *entity, token, props));
    let def = IndexDef {
        id: 0,
        name: name.clone(),
        unique: *unique,
        kind: *kind,
        entity: *entity,
        token: token.clone(),
        token_id: 0,
        props: props.clone(),
        state: "building".into(),
    };
    let what = if *unique { "constraint" } else { "index" };
    if let Some(e) = existing.iter().find(|e| e.name == name) {
        if *if_not_exists {
            return Ok(None);
        }
        let equivalent = e.same_schema(&def) && e.unique == def.unique && e.kind == def.kind;
        return Err(if equivalent {
            neo(
                "EquivalentSchemaRuleAlreadyExists",
                format!("An equivalent {what} already exists, '{name}'."),
            )
        } else if *unique {
            neo(
                "ConstraintWithNameAlreadyExists",
                format!("There already exists a constraint called '{name}'."),
            )
        } else {
            neo(
                "IndexWithNameAlreadyExists",
                format!("There already exists an index called '{name}'."),
            )
        });
    }
    if let Some(e) = existing.iter().find(|e| e.same_schema(&def)) {
        if *if_not_exists && e.unique == def.unique {
            return Ok(None);
        }
        let d = describe(*entity, token, props);
        return Err(match (e.unique, def.unique) {
            (true, false) => neo(
                "ConstraintAlreadyExists",
                format!(
                    "There is a uniqueness constraint on {d}, so an index is already created that matches this."
                ),
            ),
            (false, true) => neo(
                "IndexAlreadyExists",
                format!(
                    "There already exists an index {d}. A constraint cannot be created until the index has been dropped."
                ),
            ),
            _ if e.kind == def.kind => neo(
                "EquivalentSchemaRuleAlreadyExists",
                format!("An equivalent {what} already exists, '{}'.", e.name),
            ),
            _ => return Ok(Some(def)),
        });
    }
    Ok(Some(def))
}

/// Checks a `DROP INDEX` / `DROP CONSTRAINT`: the definition to drop, or
/// `None` when `IF EXISTS` finds nothing.
///
/// # Errors
/// 39908 for a name that isn't there, or an index a constraint owns.
pub fn plan_drop(existing: &[IndexDef], cmd: &Schema) -> Result<Option<IndexDef>> {
    let Schema::Drop {
        constraint,
        name,
        if_exists,
    } = cmd
    else {
        return Err(SprigError::internal("plan_drop of a create"));
    };
    let found = existing.iter().find(|e| &e.name == name);
    match found {
        Some(e) if e.unique == *constraint => Ok(Some(e.clone())),
        Some(_) if *constraint => Err(neo(
            "ConstraintDropFailed",
            format!("Unable to drop constraint `{name}`: No such constraint {name}."),
        )),
        Some(_) => Err(neo(
            "IndexDropFailed",
            format!("Unable to drop index: Index belongs to constraint: `{name}`"),
        )),
        None if *if_exists => Ok(None),
        None if *constraint => Err(neo(
            "ConstraintDropFailed",
            format!("Unable to drop constraint `{name}`: No such constraint {name}."),
        )),
        None => Err(neo(
            "IndexDropFailed",
            format!("Unable to drop index called `{name}`. There is no such index."),
        )),
    }
}

/// The typed text an index column holds for a stored JSON value; `None`
/// when the value isn't indexed (wrong type).
#[must_use]
pub fn index_value(j: &Json, kind: IndexKind) -> Option<String> {
    let t = match (j, kind) {
        (Json::String(s), _) => format!("s{s}"),
        (Json::Number(n), IndexKind::Range) => format!("n{}", numeric_key(n)),
        (Json::Bool(b), IndexKind::Range) => format!("b{b}"),
        _ => return None,
    };
    Some(fit(t))
}

/// Exclusive bound of the floats [`numeric_key`] writes as integers:
/// 2^63, which `9223372036854775807e0` also rounds to in MariaDB.
const INT_FLOAT_BOUND: f64 = 9_223_372_036_854_775_808.0;

/// The normalized key of a number (decision G19, round 3): integers as
/// their decimal digits, floats with an integral value inside the 64-bit
/// range as that integer (so `1` and `1.0` are one key, as Neo4j's
/// equality says), other floats as their shortest text (what the gateway
/// stored). The equality is exact: `9007199254740993` and the float
/// `9007199254740992.0` stay two keys, as in Neo4j.
/// [`crate::catalog::Tables::index_ddl`] computes the same text in
/// MariaDB.
#[must_use]
pub fn numeric_key(n: &serde_json::Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    match n.as_f64() {
        Some(f) if f.fract() == 0.0 && f.abs() < INT_FLOAT_BOUND => {
            // In range and integral: the cast is exact.
            #[allow(clippy::cast_possible_truncation)]
            let i = f as i64;
            i.to_string()
        }
        _ => n.to_string(),
    }
}

/// Long texts become `h` + their SHA-256 (as MariaDB's `SHA2(t, 256)`).
fn fit(t: String) -> String {
    if t.chars().count() <= MAX_INLINE {
        return t;
    }
    let d = ring::digest::digest(&ring::digest::SHA256, t.as_bytes());
    let mut s = String::with_capacity(65);
    s.push('h');
    for b in d.as_ref() {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn list(v: &[String]) -> Value {
    Value::List(v.iter().cloned().map(Value::String).collect())
}

fn entity_name(e: Entity) -> &'static str {
    match e {
        Entity::Node => "NODE",
        Entity::Relationship => "RELATIONSHIP",
    }
}

/// `SHOW INDEXES`: the columns and rows, `filter` being `ALL`, `RANGE`,
/// `TEXT`, `LOOKUP`, ...
#[must_use]
pub fn show_indexes(defs: &[IndexDef], filter: &str) -> (Vec<&'static str>, Vec<Vec<Value>>) {
    let cols = vec![
        "id",
        "name",
        "state",
        "populationPercent",
        "type",
        "entityType",
        "labelsOrTypes",
        "properties",
        "indexProvider",
        "owningConstraint",
        "lastRead",
        "readCount",
    ];
    let mut rows = Vec::new();
    for (id, name, entity) in [
        (1, "index_343aff4e", Entity::Node),
        (2, "index_f7700477", Entity::Relationship),
    ] {
        if matches!(filter, "ALL" | "LOOKUP") {
            rows.push(vec![
                Value::Int(id),
                Value::str(name),
                Value::str("ONLINE"),
                Value::Float(100.0),
                Value::str("LOOKUP"),
                Value::str(entity_name(entity)),
                Value::Null,
                Value::Null,
                Value::str("token-lookup-1.0"),
                Value::Null,
                Value::Null,
                Value::Int(0),
            ]);
        }
    }
    for d in defs {
        let kind = match d.kind {
            IndexKind::Range => "RANGE",
            IndexKind::Text => "TEXT",
        };
        if !(filter == "ALL" || filter == kind) {
            continue;
        }
        rows.push(vec![
            Value::Int(2 + i64::from(d.id)),
            Value::str(d.name.clone()),
            Value::str(if d.state == "online" {
                "ONLINE"
            } else {
                "POPULATING"
            }),
            Value::Float(if d.state == "online" { 100.0 } else { 0.0 }),
            Value::str(kind),
            Value::str(entity_name(d.entity)),
            list(std::slice::from_ref(&d.token)),
            list(&d.props),
            Value::str(if d.kind == IndexKind::Text {
                "text-2.0"
            } else {
                "range-1.0"
            }),
            if d.unique {
                Value::str(d.name.clone())
            } else {
                Value::Null
            },
            Value::Null,
            Value::Null,
        ]);
    }
    rows.sort_by(|a, b| a[1].order(&b[1]));
    (cols, rows)
}

/// `SHOW CONSTRAINTS`.
#[must_use]
pub fn show_constraints(defs: &[IndexDef], filter: &str) -> (Vec<&'static str>, Vec<Vec<Value>>) {
    let cols = vec![
        "id",
        "name",
        "type",
        "entityType",
        "labelsOrTypes",
        "properties",
        "ownedIndex",
        "propertyType",
    ];
    let mut rows = Vec::new();
    for d in defs.iter().filter(|d| d.unique) {
        let ty = match d.entity {
            Entity::Node => "UNIQUENESS",
            Entity::Relationship => "RELATIONSHIP_UNIQUENESS",
        };
        let shown = matches!(filter, "ALL" | "UNIQUE" | "UNIQUENESS")
            || (filter == "NODE" && d.entity == Entity::Node)
            || (filter == "RELATIONSHIP" && d.entity == Entity::Relationship);
        if !shown {
            continue;
        }
        rows.push(vec![
            Value::Int(2 + i64::from(d.id)),
            Value::str(d.name.clone()),
            Value::str(ty),
            Value::str(entity_name(d.entity)),
            list(std::slice::from_ref(&d.token)),
            list(&d.props),
            Value::str(d.name.clone()),
            Value::Null,
        ]);
    }
    rows.sort_by(|a, b| a[1].order(&b[1]));
    (cols, rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(name: Option<&str>, unique: bool, props: &[&str]) -> Schema {
        Schema::Create {
            name: name.map(str::to_owned),
            if_not_exists: false,
            unique,
            kind: IndexKind::Range,
            entity: Entity::Node,
            token: "P".into(),
            props: props.iter().map(|p| (*p).to_owned()).collect(),
        }
    }

    #[test]
    fn creates_and_drops_follow_neo4j_rules() {
        let a = plan_create(&[], &create(Some("a"), false, &["x"]))
            .expect("ok")
            .expect("new");
        let have = vec![a];
        let e = plan_create(&have, &create(Some("a"), false, &["y"])).expect_err("name");
        assert!(e.message.contains("IndexWithNameAlreadyExists"), "{e}");
        let e = plan_create(&have, &create(None, false, &["x"])).expect_err("equivalent");
        assert!(
            e.message.contains("EquivalentSchemaRuleAlreadyExists"),
            "{e}"
        );
        let e = plan_create(&have, &create(None, true, &["x"])).expect_err("index first");
        assert!(e.message.contains("IndexAlreadyExists"), "{e}");
        assert!(plan_create(&have, &create(None, true, &["y"])).is_ok());
        let drop = |constraint, name: &str, if_exists| Schema::Drop {
            constraint,
            name: name.into(),
            if_exists,
        };
        assert!(
            plan_drop(&have, &drop(false, "a", false))
                .expect("ok")
                .is_some()
        );
        assert!(
            plan_drop(&have, &drop(false, "b", true))
                .expect("ok")
                .is_none()
        );
        assert!(plan_drop(&have, &drop(false, "b", false)).is_err());
        assert!(plan_drop(&have, &drop(true, "a", false)).is_err());
        assert!(default_name(true, Entity::Node, "P", &["x".into()]).starts_with("constraint_"));
    }

    #[test]
    fn index_values_are_typed_and_bounded() {
        assert_eq!(
            index_value(&Json::from("a"), IndexKind::Range).as_deref(),
            Some("sa")
        );
        assert_eq!(
            index_value(&Json::from(42), IndexKind::Range).as_deref(),
            Some("n42")
        );
        assert_eq!(
            index_value(&Json::from(true), IndexKind::Range).as_deref(),
            Some("btrue")
        );
        assert_eq!(index_value(&Json::from(1), IndexKind::Text), None);
        // G19: integral floats are their integer; other floats keep their
        // shortest text; equality stays exact past 2^53.
        let key = |j: Json| index_value(&j, IndexKind::Range).expect("number");
        assert_eq!(key(Json::from(1.0)), "n1");
        assert_eq!(key(Json::from(-0.0)), "n0");
        assert_eq!(key(Json::from(1.5)), "n1.5");
        assert_eq!(key(Json::from(1e16)), "n10000000000000000");
        assert_eq!(key(Json::from(1e16)), key(Json::from(10_000_000_000_000_000_i64)));
        assert_ne!(
            key(Json::from(9_007_199_254_740_992.0)),
            key(Json::from(9_007_199_254_740_993_i64))
        );
        assert_eq!(key(Json::from(1e300)), "n1e+300");
        assert_eq!(key(Json::from(9.223_372_036_854_776e18)), "n9.223372036854776e+18");
        assert_ne!(key(Json::from(1)), key(Json::from("1")));
        let long = "x".repeat(300);
        let v = index_value(&Json::from(long), IndexKind::Range).expect("long");
        assert!(v.starts_with('h') && v.len() == 65, "{v}");
    }
}
