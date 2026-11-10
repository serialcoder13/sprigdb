//! The shell's errors and exit codes (chapter 53, "Exit codes" and
//! "Errors").
//!
//! Server errors keep their number and name; the shell's own are 39980–39988.
//! Each error falls in one exit class, and a run exits with the class of
//! its first error.

use std::fmt;

/// The shell's own error numbers (chapter 53, "Errors").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellCode {
    /// 39980: profiles file unreadable or invalid, or no such profile.
    ProfileInvalid,
    /// 39981: keychain unusable, or `secrets.toml` readable by others.
    SecretUnavailable,
    /// 39982: a destructive statement in the CLI without `--yes`.
    ConfirmationRequired,
    /// 39983: the language can't go over the chosen transport.
    LanguageUnsupported,
    /// 39984: `sprig admin` found no `sprigctl`.
    SprigctlMissing,
    /// 39985: input the shell can't split or parse.
    Parse,
    /// 39986: the user cancelled the statement.
    Cancelled,
    /// 39987: the gateway or native endpoint can't be reached.
    Unreachable,
    /// 39988: a password login over plain HTTP to another host.
    InsecureTransport,
}

impl ShellCode {
    /// The error number.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            ShellCode::ProfileInvalid => 39980,
            ShellCode::SecretUnavailable => 39981,
            ShellCode::ConfirmationRequired => 39982,
            ShellCode::LanguageUnsupported => 39983,
            ShellCode::SprigctlMissing => 39984,
            ShellCode::Parse => 39985,
            ShellCode::Cancelled => 39986,
            ShellCode::Unreachable => 39987,
            ShellCode::InsecureTransport => 39988,
        }
    }

    /// The `ER_SPRIG_SHELL_*` name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            ShellCode::ProfileInvalid => "ER_SPRIG_SHELL_PROFILE_INVALID",
            ShellCode::SecretUnavailable => "ER_SPRIG_SHELL_SECRET_UNAVAILABLE",
            ShellCode::ConfirmationRequired => "ER_SPRIG_SHELL_CONFIRMATION_REQUIRED",
            ShellCode::LanguageUnsupported => "ER_SPRIG_SHELL_LANGUAGE_UNSUPPORTED",
            ShellCode::SprigctlMissing => "ER_SPRIG_SHELL_SPRIGCTL_MISSING",
            ShellCode::Parse => "ER_SPRIG_SHELL_PARSE",
            ShellCode::Cancelled => "ER_SPRIG_SHELL_CANCELLED",
            ShellCode::Unreachable => "ER_SPRIG_SHELL_UNREACHABLE",
            ShellCode::InsecureTransport => "ER_SPRIG_SHELL_INSECURE_TRANSPORT",
        }
    }
}

/// Exit classes (chapter 53, "Exit codes").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExitClass {
    /// 0.
    Ok = 0,
    /// 1: the server refused a statement.
    Statement = 1,
    /// 2: bad options or input.
    Usage = 2,
    /// 3: no connection.
    Connection = 3,
    /// 4: authentication.
    Auth = 4,
    /// 5: privileges.
    Permission = 5,
    /// 6: a destructive statement without `--yes`.
    Confirm = 6,
    /// 7: cancelled or timed out.
    Cancelled = 7,
    /// 8: retry later.
    Retry = 8,
}

impl ExitClass {
    /// The process exit code.
    #[must_use]
    pub fn code(self) -> u8 {
        self as u8
    }
}

/// An error, from a server or from the shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellError {
    /// The MySQL, SprigDB or shell error number; 0 when unknown.
    pub code: u32,
    /// `ER_SPRIG_*` or empty.
    pub name: String,
    /// The text.
    pub message: String,
    /// The HTTP status it came with; 0 when none.
    pub http_status: u16,
}

impl ShellError {
    /// One of the shell's own errors.
    #[must_use]
    pub fn shell(code: ShellCode, message: impl Into<String>) -> Self {
        Self {
            code: code.code(),
            name: code.name().to_owned(),
            message: message.into(),
            http_status: 0,
        }
    }

    /// A server error with its number.
    #[must_use]
    pub fn server(code: u32, name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            name: name.into(),
            message: message.into(),
            http_status: 0,
        }
    }

    /// Whether this is `code`.
    #[must_use]
    pub fn is(&self, code: ShellCode) -> bool {
        self.code == code.code()
    }

    /// The exit class.
    #[must_use]
    pub fn class(&self) -> ExitClass {
        match self.code {
            39980 | 39983 | 39985 => ExitClass::Usage,
            39984 | 39987 => ExitClass::Connection,
            39981 | 39988 | 1045 | 39921 | 39924 | 39925 | 39793 => ExitClass::Auth,
            39982 => ExitClass::Confirm,
            39986 | 3024 | 1317 => ExitClass::Cancelled,
            1044 | 1142 | 1143 | 1227 | 1370 => ExitClass::Permission,
            1205 | 1213 | 39780 | 39001 | 39002 | 39030 | 39031 => ExitClass::Retry,
            _ => match self.http_status {
                401 => ExitClass::Auth,
                403 => ExitClass::Permission,
                429 | 503 => ExitClass::Retry,
                _ => ExitClass::Statement,
            },
        }
    }
}

impl fmt::Display for ShellError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.code, self.name.is_empty()) {
            (0, _) => f.write_str(&self.message),
            (c, true) => write!(f, "{c}: {}", self.message),
            (c, false) => write!(f, "{c} {}: {}", self.name, self.message),
        }
    }
}

impl std::error::Error for ShellError {}

/// The shell's result type.
pub type Result<T> = std::result::Result<T, ShellError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes() {
        let c = |code| ShellError::server(code, "", "x").class();
        assert_eq!(c(1064), ExitClass::Statement);
        assert_eq!(c(1045), ExitClass::Auth);
        assert_eq!(c(1142), ExitClass::Permission);
        assert_eq!(c(1213), ExitClass::Retry);
        assert_eq!(c(3024), ExitClass::Cancelled);
        assert_eq!(
            ShellError::shell(ShellCode::ConfirmationRequired, "x").class(),
            ExitClass::Confirm
        );
        let mut e = ShellError::server(0, "", "x");
        e.http_status = 403;
        assert_eq!(e.class(), ExitClass::Permission);
        assert_eq!(
            ShellError::shell(ShellCode::Parse, "open quote").to_string(),
            "39985 ER_SPRIG_SHELL_PARSE: open quote"
        );
    }
}
