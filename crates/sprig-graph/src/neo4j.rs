//! Neo4j status codes for SprigDB errors (chapter 43, "Errors"): Neo4j's
//! code where one fits, else `Neo.ClientError.Statement.Sprig<CamelCase>`
//! (guide 5.9's MongoDB rule), so drivers retry by classification.

use sprig_common::{ErrorKind, SprigCode, SprigError};

/// The Neo4j status code for `e`.
#[must_use]
pub fn status_code(e: &SprigError) -> String {
    // A code the evaluator put in front of the message: `[Neo...] text`.
    if let Some(rest) = e.message.strip_prefix('[')
        && let Some((code, _)) = rest.split_once(']')
        && code.starts_with("Neo.")
    {
        return code.to_owned();
    }
    match e.kind {
        ErrorKind::Sprig(c) => match c {
            SprigCode::GraphSyntax => "Neo.ClientError.Statement.SyntaxError".into(),
            SprigCode::GraphNotFound => "Neo.ClientError.Database.DatabaseNotFound".into(),
            SprigCode::GraphNodeHasRelationships => {
                "Neo.ClientError.Schema.ConstraintValidationFailed".into()
            }
            SprigCode::GraphPropertyType => "Neo.ClientError.Statement.TypeError".into(),
            SprigCode::BoltProtocol => "Neo.ClientError.Request.Invalid".into(),
            SprigCode::BoltAuthScheme => "Neo.ClientError.Security.Unauthorized".into(),
            SprigCode::BoltImpersonation => "Neo.ClientError.Security.Forbidden".into(),
            SprigCode::GatewayDraining | SprigCode::TenantNotReady => {
                "Neo.TransientError.General.DatabaseUnavailable".into()
            }
            other => {
                let camel = other.mongo_code_name();
                format!("Neo.ClientError.Statement.{camel}")
            }
        },
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => match errno {
            1213 | 1205 => "Neo.TransientError.Transaction.DeadlockDetected".into(),
            1062 => "Neo.ClientError.Schema.ConstraintValidationFailed".into(),
            1142 | 1143 | 1044 => "Neo.ClientError.Security.Forbidden".into(),
            1045 => "Neo.ClientError.Security.Unauthorized".into(),
            1317 => "Neo.TransientError.Transaction.Terminated".into(),
            39424 => "Neo.TransientError.General.DatabaseUnavailable".into(),
            _ => "Neo.DatabaseError.General.UnknownError".into(),
        },
    }
}

/// The message without a leading `[Neo...]` tag.
#[must_use]
pub fn message(e: &SprigError) -> String {
    if let Some(rest) = e.message.strip_prefix('[')
        && let Some((code, msg)) = rest.split_once("] ")
        && code.starts_with("Neo.")
    {
        return msg.to_owned();
    }
    e.message.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_follow_the_chapter() {
        let e = SprigError::new(SprigCode::GraphBudgetExceeded, "x");
        assert_eq!(status_code(&e), "Neo.ClientError.Statement.SprigGraphBudgetExceeded");
        let e = SprigError::new(SprigCode::GraphSyntax, "x");
        assert_eq!(status_code(&e), "Neo.ClientError.Statement.SyntaxError");
        let e = SprigError::backend(1213, *b"40001", "deadlock");
        assert_eq!(status_code(&e), "Neo.TransientError.Transaction.DeadlockDetected");
        let e = SprigError::new(SprigCode::GraphSyntax, "[Neo.ClientError.Statement.ParameterMissing] Expected parameter(s): x");
        assert_eq!(status_code(&e), "Neo.ClientError.Statement.ParameterMissing");
        assert_eq!(message(&e), "Expected parameter(s): x");
    }
}
