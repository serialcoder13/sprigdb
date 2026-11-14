//! The errors the assistants raise, 39990-39996.

use sprig_common::{SprigCode, SprigError};

use crate::role::Role;

/// 39990: no provider or model for `role`.
#[must_use]
pub fn not_configured(role: Role, why: impl Into<String>) -> SprigError {
    SprigError::new(
        SprigCode::AiNotConfigured,
        format!(
            "no AI model is configured for the {role} assistant: {}",
            why.into()
        ),
    )
}

/// 39991: `ai.enabled` is off.
#[must_use]
pub fn disabled() -> SprigError {
    SprigError::new(
        SprigCode::AiDisabled,
        "the AI assistants are turned off for this tenant (ai.enabled)",
    )
}

/// 39992: a daily budget is used up; `which` is `tenant` or `user`.
#[must_use]
pub fn budget_exceeded(which: &str, limit: u64) -> SprigError {
    SprigError::new(
        SprigCode::AiBudgetExceeded,
        format!(
            "the {which}'s daily AI token budget ({limit} tokens, ai.budget.{which}_tokens_day) is used up; it resets at 00:00 UTC"
        ),
    )
}

/// 39993: the provider failed; `retryable` when trying again later may
/// help (5xx, network, overload, timeout), not for a refused key or a bad
/// request.
#[must_use]
pub fn provider_error(provider: &str, what: impl Into<String>, retryable: bool) -> SprigError {
    let mut e = SprigError::new(
        SprigCode::AiProviderError,
        format!("AI provider {provider}: {}", what.into()),
    );
    e.retryable = retryable;
    e
}

/// 39994: the model declined; `text` is what it said.
#[must_use]
pub fn refused(text: &str) -> SprigError {
    SprigError::new(
        SprigCode::AiRefused,
        format!("the model declined the request: {text}"),
    )
}

/// 39995: the request would send row data and `ai.share_data` is off.
#[must_use]
pub fn egress_denied(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::AiEgressDenied,
        format!("{what} would send row data to the AI provider, and ai.share_data is off"),
    )
}

/// 39996: the model kept calling tools it may not call, or with invalid
/// input.
#[must_use]
pub fn tool_denied(detail: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AiToolDenied, detail.into())
}
