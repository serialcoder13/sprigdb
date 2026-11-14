//! The `sprig_ai_*` metrics this crate records.
//!
//! - `sprig_ai_requests_total{role,provider,model,result}`: model calls;
//!   `result` is `ok`, `refused` or `error`.
//! - `sprig_ai_tokens_total{role,direction}`: `input`, `output`,
//!   `cache_read` or `cache_write`.
//! - `sprig_ai_turn_seconds{role}`: one turn, model calls and tools.
//! - `sprig_ai_tool_calls_total{tool,result}`: `ok`, `error`, `denied`
//!   or `proposed`.
//! - `sprig_ai_egress_reduced_total{role}`: tool results the egress gate
//!   reduced (rows or values withheld, or cut).
//! - `sprig_ai_proposals_total{kind,outcome}`: proposals; this crate
//!   records `shown`.
//! - `sprig_ai_budget_stops_total`: turns stopped by a daily budget.

use std::time::Duration;

use crate::provider::Usage;
use crate::role::Role;

/// One model call.
pub fn request(role: Role, provider: &str, model: &str, result: &'static str) {
    metrics::counter!(
        "sprig_ai_requests_total",
        "role" => role.as_str(),
        "provider" => provider.to_owned(),
        "model" => model.to_owned(),
        "result" => result
    )
    .increment(1);
}

/// One call's tokens.
pub fn tokens(role: Role, u: &Usage) {
    for (direction, n) in [
        ("input", u.input_tokens),
        ("output", u.output_tokens),
        ("cache_read", u.cache_read_tokens),
        ("cache_write", u.cache_write_tokens),
    ] {
        if n > 0 {
            metrics::counter!(
                "sprig_ai_tokens_total",
                "role" => role.as_str(),
                "direction" => direction
            )
            .increment(n);
        }
    }
}

/// One turn's duration.
pub fn turn(role: Role, took: Duration) {
    metrics::histogram!("sprig_ai_turn_seconds", "role" => role.as_str())
        .record(took.as_secs_f64());
}

/// One tool call.
pub fn tool_call(tool: &str, result: &'static str) {
    metrics::counter!(
        "sprig_ai_tool_calls_total",
        "tool" => tool.to_owned(),
        "result" => result
    )
    .increment(1);
}

/// A tool result the gate reduced.
pub fn egress_reduced(role: Role) {
    metrics::counter!("sprig_ai_egress_reduced_total", "role" => role.as_str()).increment(1);
}

/// A proposal and what became of it.
pub fn proposal(kind: &'static str, outcome: &'static str) {
    metrics::counter!(
        "sprig_ai_proposals_total",
        "kind" => kind,
        "outcome" => outcome
    )
    .increment(1);
}

/// A turn stopped by a budget.
pub fn budget_stop() {
    metrics::counter!("sprig_ai_budget_stops_total").increment(1);
}
