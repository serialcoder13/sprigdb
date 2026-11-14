//! Tools: what the assistants may call, and how a call is checked and run.
//!
//! - [`ToolSpec`]: a tool's name, description, JSON schema, [`Effect`],
//!   roles and what its result reveals ([`Shares`]).
//! - [`ToolRegistry`]: the built-in tools ([`builtin`]); the tools of a
//!   role; a call's checks (the role may call it, the input fits the
//!   schema, SQL inputs are reads).
//! - [`ToolHost`]: runs **read** tools, built for one signed-in caller so
//!   every call has exactly that caller's rights. The gateway implements
//!   it over its SQL engine and routes.
//! - **Proposals.** A [`Effect::Propose`] tool never reaches the host: the
//!   registry turns the call into a [`Proposal`] for the user to review
//!   and run. Nothing an assistant does changes state by itself.

pub mod builtin;
pub mod schema;

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};

use crate::provider::ToolDef;
use crate::role::Role;

/// Whether a tool reads or proposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effect {
    /// It reads, as the caller, through the [`ToolHost`].
    Read,
    /// It returns a proposal for the user; it never runs anything.
    Propose,
}

/// What a tool's result reveals, so the egress gate knows what to reduce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Shares {
    /// Names, types, definitions, documentation, settings: no values.
    Schema,
    /// Counts, plans and statistics; values only with `ai.share_data`.
    Stats,
    /// Rows; sent only with `ai.share_data`, else reduced to their shape.
    Rows,
}

/// One tool.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolSpec {
    /// `snake_case`, unique.
    pub name: &'static str,
    /// When and why to call it, for the model.
    pub description: &'static str,
    /// The input's JSON schema: an object, `additionalProperties: false`
    /// on every object, no numeric or length constraints (they are
    /// checked by [`schema::validate`] instead, which providers' strict
    /// modes don't accept).
    pub input: Value,
    /// Reads or proposes.
    pub effect: Effect,
    /// The roles that may call it.
    pub roles: &'static [Role],
    /// What its result reveals.
    pub shares: Shares,
}

impl ToolSpec {
    /// The tool as a provider sees it.
    #[must_use]
    pub fn def(&self) -> ToolDef {
        ToolDef {
            name: self.name.to_owned(),
            description: self.description.to_owned(),
            input_schema: self.input.clone(),
            strict: true,
        }
    }

    /// Whether `role` may call it.
    #[must_use]
    pub fn allows(&self, role: Role) -> bool {
        self.roles.contains(&role)
    }
}

/// Where an answer came from, for the answer's sources list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    /// `docs`, `table`, `query`, `setting`, `plan`, `approval`, ...
    pub kind: String,
    /// What to show.
    pub title: String,
    /// Where it is: a docs page and heading, a table name, a setting key.
    pub reference: String,
}

/// One column of a result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Its name.
    pub name: String,
    /// Its type, as the database names it.
    #[serde(rename = "type")]
    pub ty: String,
}

/// A query result.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ResultSet {
    /// The columns.
    pub columns: Vec<Column>,
    /// The rows held, cells in column order.
    pub rows: Vec<Vec<Value>>,
    /// The rows the query returned (more than `rows` when capped).
    pub row_count: u64,
    /// `rows` was cut at a cap.
    pub truncated: bool,
}

/// Statistics of one column.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ColumnStats {
    /// The column.
    pub column: String,
    /// Its type.
    #[serde(rename = "type")]
    pub ty: String,
    /// Rows counted.
    pub count: u64,
    /// Nulls among them.
    pub nulls: u64,
    /// Distinct values (an estimate).
    pub distinct: Option<u64>,
    /// The smallest value: a value, so sent only with sharing.
    pub min: Option<Value>,
    /// The largest value: a value, so sent only with sharing.
    pub max: Option<Value>,
    /// The commonest values and their counts: values, so sent only with
    /// sharing.
    #[serde(default)]
    pub top: Vec<(Value, u64)>,
}

/// What a read tool returned, before the egress gate.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolBody {
    /// Anything without row values: schemas, documentation, plans,
    /// settings, approvals.
    Json(Value),
    /// Rows.
    Rows(ResultSet),
    /// Column statistics.
    Stats(Vec<ColumnStats>),
}

/// A read tool's result.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolOutput {
    /// The result.
    pub body: ToolBody,
    /// What it was read from.
    pub sources: Vec<Source>,
}

impl ToolOutput {
    /// A JSON result without sources.
    #[must_use]
    pub fn json(v: Value) -> Self {
        Self {
            body: ToolBody::Json(v),
            sources: Vec::new(),
        }
    }
}

/// Runs read tools as one caller. The gateway builds one per request from
/// the signed-in principal; it has no rights of its own.
#[async_trait]
pub trait ToolHost: Send + Sync {
    /// Runs read tool `spec` with `input`, already checked against the
    /// schema (and, for SQL inputs, checked to be a read).
    ///
    /// # Errors
    /// Whatever the call failed with (no privilege, no such table, a
    /// timeout); the model sees the message and may try another way.
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput>;
}

/// A host that runs nothing: every read tool fails. For surfaces with no
/// caller, such as the administration agent's intake.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoHost;

#[async_trait]
impl ToolHost for NoHost {
    async fn call(&self, spec: &ToolSpec, _input: &Value) -> Result<ToolOutput> {
        Err(SprigError::new(
            SprigCode::AiToolDenied,
            format!("{} isn't available here", spec.name),
        ))
    }
}

/// The kinds of proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProposalKind {
    /// A statement for the user to review and run.
    Sql,
    /// A recommendation for the approvals inbox.
    Recommendation,
    /// A dashboard tile.
    Tile,
    /// A glossary entry for an admin to accept.
    Glossary,
    /// Test rows generated from a table's schema.
    GenerateRows,
}

impl ProposalKind {
    /// The metric and audit label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ProposalKind::Sql => "sql",
            ProposalKind::Recommendation => "recommendation",
            ProposalKind::Tile => "tile",
            ProposalKind::Glossary => "glossary",
            ProposalKind::GenerateRows => "generate_rows",
        }
    }
}

/// Something an assistant proposes. It is shown to the user and never run
/// by the assistant: SQL runs through the user's own confirm and query,
/// the rest when the user accepts it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Proposal {
    /// A new id (UUID v7).
    pub id: String,
    /// The kind.
    pub kind: ProposalKind,
    /// The tool call it came from.
    pub tool_use_id: String,
    /// The tool's input, as the model gave it.
    pub body: Value,
}

/// The tools, by name.
#[derive(Debug, Clone)]
pub struct ToolRegistry {
    tools: BTreeMap<&'static str, Arc<ToolSpec>>,
}

static BUILTIN: LazyLock<Arc<ToolRegistry>> = LazyLock::new(|| {
    Arc::new(
        // The built-in specs are checked by the tests; a bad one is a bug.
        ToolRegistry::new(builtin::specs()).unwrap_or_else(|e| {
            tracing::error!(error = %e, "the built-in AI tools are invalid");
            ToolRegistry {
                tools: BTreeMap::new(),
            }
        }),
    )
});

/// A refused or failed call, for the model (`is_error`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallError {
    /// What to tell the model.
    pub message: String,
    /// The call was refused (role, input) rather than failed: these count
    /// towards 39996.
    pub denied: bool,
}

impl CallError {
    fn denied(m: impl Into<String>) -> Self {
        Self {
            message: m.into(),
            denied: true,
        }
    }
}

impl ToolRegistry {
    /// A registry of `specs`.
    ///
    /// # Errors
    /// Internal for a duplicate name or a schema that isn't an object
    /// with `additionalProperties: false` everywhere.
    pub fn new(specs: impl IntoIterator<Item = ToolSpec>) -> Result<Self> {
        let mut tools = BTreeMap::new();
        for s in specs {
            schema::check_spec_schema(s.name, &s.input)?;
            if tools.insert(s.name, Arc::new(s)).is_some() {
                return Err(SprigError::internal("two AI tools share a name"));
            }
        }
        Ok(Self { tools })
    }

    /// The built-in tools.
    #[must_use]
    pub fn builtin() -> Arc<ToolRegistry> {
        Arc::clone(&BUILTIN)
    }

    /// The tool named `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<ToolSpec>> {
        self.tools.get(name)
    }

    /// Every tool, by name.
    pub fn all(&self) -> impl Iterator<Item = &Arc<ToolSpec>> {
        self.tools.values()
    }

    /// The tools `role` may call, by name: what its requests offer.
    #[must_use]
    pub fn for_role(&self, role: Role) -> Vec<Arc<ToolSpec>> {
        self.tools
            .values()
            .filter(|s| s.allows(role))
            .cloned()
            .collect()
    }

    /// Checks a call: the tool exists, `role` may call it, the input fits
    /// its schema, and SQL inputs are what the tool allows.
    ///
    /// # Errors
    /// A [`CallError`] for the model.
    pub fn check(
        &self,
        role: Role,
        name: &str,
        input: &Value,
    ) -> std::result::Result<Arc<ToolSpec>, CallError> {
        let spec = self
            .get(name)
            .ok_or_else(|| CallError::denied(format!("there is no tool {name}")))?;
        if !spec.allows(role) {
            return Err(CallError::denied(format!(
                "the {role} assistant may not call {name}"
            )));
        }
        schema::validate(&spec.input, input)
            .map_err(|m| CallError::denied(format!("invalid input for {name}: {m}")))?;
        builtin::check_input(name, input).map_err(CallError::denied)?;
        Ok(Arc::clone(spec))
    }

    /// A proposal from a checked call of a [`Effect::Propose`] tool.
    ///
    /// # Errors
    /// Internal if `spec` isn't a proposal tool (a bug).
    pub fn propose(spec: &ToolSpec, tool_use_id: &str, input: &Value) -> Result<Proposal> {
        let kind = builtin::proposal_kind(spec.name)
            .filter(|_| spec.effect == Effect::Propose)
            .ok_or_else(|| SprigError::internal(format!("{} isn't a proposal tool", spec.name)))?;
        Ok(Proposal {
            id: uuid::Uuid::now_v7().to_string(),
            kind,
            tool_use_id: tool_use_id.to_owned(),
            body: input.clone(),
        })
    }

    /// What the model is told about a proposal: it was shown, not run.
    #[must_use]
    pub fn proposal_note(p: &Proposal) -> Value {
        json!({
            "proposal_id": p.id,
            "status": "shown to the user; it has not run",
            "next": "The user reviews it and decides. Don't claim it was applied.",
        })
    }
}

#[cfg(test)]
mod tests;
