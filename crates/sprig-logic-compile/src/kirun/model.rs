//! KIRun's definition format (KIRun's `json-format.md`): the subset of
//! fields the compiler reads. Unknown fields (positions, comments, step
//! groups) are ignored.

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

/// A JSON object's members in source order: parameter order is the call's
/// argument order.
#[derive(Debug, Clone)]
pub struct Ordered<V>(pub Vec<(String, V)>);

impl<V> Default for Ordered<V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for Ordered<V> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V2<V>(PhantomData<V>);
        impl<'de, V: Deserialize<'de>> Visitor<'de> for V2<V> {
            type Value = Ordered<V>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut m: A) -> Result<Self::Value, A::Error> {
                let mut v = Vec::new();
                while let Some((k, x)) = m.next_entry::<String, V>()? {
                    v.push((k, x));
                }
                Ok(Ordered(v))
            }
        }
        d.deserialize_map(V2(PhantomData))
    }
}

/// A function definition.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Definition {
    /// The function's name.
    pub name: String,
    /// Its namespace, such as `Shop`.
    #[serde(default)]
    pub namespace: String,
    /// Parameters, in source order.
    #[serde(default)]
    pub parameters: Ordered<ParamDef>,
    /// Events, in source order.
    #[serde(default)]
    pub events: Ordered<EventDef>,
    /// Steps by statement name.
    #[serde(default)]
    pub steps: BTreeMap<String, Step>,
    /// Sub-function definitions; not compiled.
    #[serde(default)]
    pub parts: Vec<serde_json::Value>,
}

/// A parameter.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParamDef {
    /// The name (equals the key).
    #[serde(default)]
    pub parameter_name: String,
    /// Its schema.
    #[serde(default)]
    pub schema: SchemaDef,
    /// Whether it takes several values.
    #[serde(default)]
    pub variable_argument: bool,
}

/// `type`: one name or a list of names.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TypeSpec {
    /// `"DOUBLE"`.
    One(String),
    /// `["INTEGER", "NULL"]`.
    Many(Vec<String>),
}

/// A schema: the fields that decide a type and its bounds.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaDef {
    /// The type names.
    #[serde(rename = "type")]
    pub ty: Option<TypeSpec>,
    /// The smallest value.
    pub minimum: Option<f64>,
    /// The largest value.
    pub maximum: Option<f64>,
    /// The default value.
    pub default_value: Option<serde_json::Value>,
    /// A reference to a named schema; not resolved.
    #[serde(rename = "ref")]
    pub reference: Option<String>,
}

impl SchemaDef {
    /// The type names, upper case, without `NULL`.
    #[must_use]
    pub fn types(&self) -> Vec<String> {
        let v = match &self.ty {
            None => Vec::new(),
            Some(TypeSpec::One(s)) => vec![s.clone()],
            Some(TypeSpec::Many(v)) => v.clone(),
        };
        v.into_iter()
            .map(|s| s.to_ascii_uppercase())
            .filter(|s| s != "NULL")
            .collect()
    }
}

/// An event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventDef {
    /// The event's name.
    #[serde(default)]
    pub name: String,
    /// Its fields' schemas, in source order.
    #[serde(default)]
    pub parameters: Ordered<SchemaDef>,
}

/// A step (statement).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Step {
    /// The statement's name (equals the key).
    #[serde(default)]
    pub statement_name: String,
    /// The called function's namespace.
    #[serde(default)]
    pub namespace: String,
    /// The called function's name.
    #[serde(default)]
    pub name: String,
    /// Parameter bindings: parameter name, then reference key.
    #[serde(default)]
    pub parameter_map: BTreeMap<String, BTreeMap<String, ParamRef>>,
    /// `Steps.<s>.<event>` keys this step waits for.
    #[serde(default)]
    pub dependent_statements: BTreeMap<String, bool>,
    /// Expressions that must be present and not false for the step to
    /// run.
    #[serde(default)]
    pub execute_iftrue: BTreeMap<String, bool>,
}

impl Step {
    /// The called function, `Namespace.Name`.
    #[must_use]
    pub fn function(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }

    /// The references bound to `param`, ordered as KIRun orders variable
    /// arguments (by `order`, then by key).
    #[must_use]
    pub fn refs(&self, param: &str) -> Vec<&ParamRef> {
        let Some(m) = self.parameter_map.get(param) else {
            return Vec::new();
        };
        let mut v: Vec<(&String, &ParamRef)> = m.iter().collect();
        v.sort_by(|a, b| {
            a.1.order
                .unwrap_or(0)
                .cmp(&b.1.order.unwrap_or(0))
                .then(a.0.cmp(b.0))
        });
        v.into_iter().map(|(_, r)| r).collect()
    }
}

/// How a reference supplies its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RefKind {
    /// A constant.
    Value,
    /// An expression.
    Expression,
}

/// One parameter reference.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParamRef {
    /// `VALUE` or `EXPRESSION`.
    #[serde(rename = "type")]
    pub kind: RefKind,
    /// The constant.
    #[serde(default)]
    pub value: Option<serde_json::Value>,
    /// The expression.
    #[serde(default)]
    pub expression: Option<String>,
    /// Order among variable arguments.
    #[serde(default)]
    pub order: Option<i64>,
}
