//! Token sessions for browser logins (chapter 14, "SprigDB tokens and
//! sessions"; chapter 44, "Login and the session").
//!
//! Since Studio round 2 (decision ST1's alternative, built) the sessions
//! are chapter 14's: `sprig_auth::token`. Access tokens are `EdDSA` JWS
//! signed with this gateway's key, whose public half is published in the
//! global tier, so any gateway in any DC verifies them; sessions live in
//! the tenant catalog's `token_sessions`, so any gateway refreshes them and
//! a refresh token used twice is caught wherever the two uses land. A
//! gateway restart signs nobody out. Logins that don't come from a tenant
//! catalog (the accounts file, tests) keep their sessions in memory
//! ([`MemorySessions`]).

pub use sprig_auth::token::{ACCESS_PREFIX, Issued, Limits, MemorySessions, Verified};
use sprig_common::SprigError;

/// Why a session ended (the `reason` label of
/// `sprig_studio_session_ends_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    /// The user signed out.
    Logout,
    /// Idle past `studio.session_idle_minutes`.
    Idle,
    /// Older than `studio.session_max_hours`.
    MaxAge,
    /// The account changed, or a refresh token was used twice.
    Revoked,
}

impl EndReason {
    /// The metric label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Logout => "logout",
            Self::Idle => "idle",
            Self::MaxAge => "max_age",
            Self::Revoked => "revoked",
        }
    }

    /// Counts one session end.
    pub fn count(self) {
        metrics::counter!("sprig_studio_session_ends_total", "reason" => self.label()).increment(1);
    }

    /// The end a refused refresh means, if it ended the session.
    #[must_use]
    pub fn of_refusal(e: &SprigError) -> Option<Self> {
        match e.protocol().mysql_errno {
            39924 => Some(Self::Idle),
            39925 => Some(Self::MaxAge),
            1045 if e.message.contains("already used") => Some(Self::Revoked),
            _ => None,
        }
    }
}

/// A refused token: 1045, as a wrong password is.
#[must_use]
pub fn denied(why: &str) -> SprigError {
    SprigError::standard(1045, *b"28000", format!("Access denied: {why}"))
}
