//! Setting values and the Rust types a key can have.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// A stored setting value: what a scope sets, and what bounds list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    /// `true` or `false`.
    Bool(bool),
    /// A whole number.
    Int(i64),
    /// A number with a fraction.
    Float(f64),
    /// Text, typically one of a fixed set (`on`, `recommend`).
    Str(String),
}

impl Value {
    /// The number, for bounds checks and clamping.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // bounds are f64 by design
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(_) | Value::Str(_) => None,
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Str(s.to_owned())
    }
}

/// The type of a key, as declared in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    /// `bool`.
    Bool,
    /// An integer type.
    Int,
    /// `f64`.
    Float,
    /// Text.
    Str,
}

/// A Rust type a setting can have.
pub trait SettingValue: Clone + Send + Sync + 'static {
    /// The registry type.
    const KIND: ValueKind;
    /// Converts to the stored form.
    fn to_value(&self) -> Value;
    /// Converts from the stored form; `None` if the value has the wrong type
    /// or is out of the type's range.
    fn from_value(v: &Value) -> Option<Self>;
}

impl SettingValue for bool {
    const KIND: ValueKind = ValueKind::Bool;
    fn to_value(&self) -> Value {
        Value::Bool(*self)
    }
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

macro_rules! int_setting {
    ($($t:ty),*) => {$(
        impl SettingValue for $t {
            const KIND: ValueKind = ValueKind::Int;
            fn to_value(&self) -> Value {
                Value::Int(i64::try_from(*self).unwrap_or(i64::MAX))
            }
            fn from_value(v: &Value) -> Option<Self> {
                match v {
                    Value::Int(i) => <$t>::try_from(*i).ok(),
                    _ => None,
                }
            }
        }
    )*};
}
int_setting!(i32, i64, u32, u64);

impl SettingValue for f64 {
    const KIND: ValueKind = ValueKind::Float;
    fn to_value(&self) -> Value {
        Value::Float(*self)
    }
    #[allow(clippy::cast_precision_loss)]
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }
}

impl SettingValue for Cow<'static, str> {
    const KIND: ValueKind = ValueKind::Str;
    fn to_value(&self) -> Value {
        Value::Str(self.to_string())
    }
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Str(s) => Some(Cow::Owned(s.clone())),
            _ => None,
        }
    }
}
