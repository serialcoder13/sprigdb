//! MongoDB error codes and their `codeName`s, and the error reply document.
//!
//! SprigDB's own errors (39000-39999) keep their number and get the
//! `codeName` `Sprig<Name>`.

use bson::{Document, doc};
use sprig_common::SprigCode;

/// A command error: code, message, and labels.
#[derive(Debug, Clone, PartialEq)]
pub struct MongoError {
    /// The numeric code.
    pub code: i32,
    /// The message (`errmsg`).
    pub message: String,
    /// `errorLabels`, such as `RetryableWriteError`.
    pub labels: Vec<&'static str>,
    /// Extra fields for the reply (`keyPattern`, `keyValue`, `errInfo`).
    pub extra: Option<Box<Document>>,
}

impl MongoError {
    /// An error with `code` and `message`.
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            labels: Vec::new(),
            extra: None,
        }
    }

    /// The same error with a label added.
    #[must_use]
    pub fn label(mut self, l: &'static str) -> Self {
        if !self.labels.contains(&l) {
            self.labels.push(l);
        }
        self
    }

    /// The same error with extra reply fields.
    #[must_use]
    pub fn with_extra(mut self, d: Document) -> Self {
        self.extra = Some(Box::new(d));
        self
    }

    /// The `codeName`.
    #[must_use]
    pub fn code_name(&self) -> &'static str {
        code_name(self.code)
    }

    /// The reply document: `{ok: 0, errmsg, code, codeName[, errorLabels]}`.
    #[must_use]
    pub fn to_doc(&self) -> Document {
        let mut d = doc! {
            "ok": 0.0,
            "errmsg": self.message.clone(),
            "code": self.code,
            "codeName": self.code_name(),
        };
        if let Some(x) = &self.extra {
            for (k, v) in x.iter() {
                d.insert(k.clone(), v.clone());
            }
        }
        if !self.labels.is_empty() {
            d.insert("errorLabels", self.labels.clone());
        }
        d
    }

    /// A write error entry for `writeErrors` at `index`.
    #[must_use]
    pub fn write_error(&self, index: usize) -> Document {
        let mut d = doc! {
            "index": i32::try_from(index).unwrap_or(i32::MAX),
            "code": self.code,
            "codeName": self.code_name(),
            "errmsg": self.message.clone(),
        };
        if let Some(x) = &self.extra {
            for (k, v) in x.iter() {
                d.insert(k.clone(), v.clone());
            }
        }
        d
    }
}

impl std::fmt::Display for MongoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}): {}", self.code_name(), self.code, self.message)
    }
}

impl std::error::Error for MongoError {}

/// Codes used by the front end and the document engine.
pub mod code {
    #![allow(missing_docs)]
    use sprig_common::SprigCode;

    pub const INTERNAL_ERROR: i32 = 1;
    pub const BAD_VALUE: i32 = 2;
    pub const NO_SUCH_KEY: i32 = 4;
    pub const HOST_UNREACHABLE: i32 = 6;
    pub const FAILED_TO_PARSE: i32 = 9;
    pub const UNAUTHORIZED: i32 = 13;
    pub const TYPE_MISMATCH: i32 = 14;
    pub const AUTHENTICATION_FAILED: i32 = 18;
    pub const ILLEGAL_OPERATION: i32 = 20;
    pub const NAMESPACE_NOT_FOUND: i32 = 26;
    pub const INDEX_NOT_FOUND: i32 = 27;
    pub const CONFLICTING_UPDATE_OPERATORS: i32 = 40;
    pub const CURSOR_NOT_FOUND: i32 = 43;
    pub const NAMESPACE_EXISTS: i32 = 48;
    pub const MAX_TIME_MS_EXPIRED: i32 = 50;
    pub const DOLLAR_PREFIXED_FIELD_NAME: i32 = 52;
    pub const INVALID_ID_FIELD: i32 = 53;
    pub const NOT_SINGLE_VALUE_FIELD: i32 = 54;
    pub const COMMAND_NOT_FOUND: i32 = 59;
    pub const IMMUTABLE_FIELD: i32 = 66;
    pub const CANNOT_CREATE_INDEX: i32 = 67;
    pub const INDEX_ALREADY_EXISTS: i32 = 68;
    pub const INVALID_OPTIONS: i32 = 72;
    pub const INVALID_NAMESPACE: i32 = 73;
    pub const INDEX_OPTIONS_CONFLICT: i32 = 85;
    pub const INDEX_KEY_SPECS_CONFLICT: i32 = 86;
    pub const SHUTDOWN_IN_PROGRESS: i32 = 91;
    pub const WRITE_CONFLICT: i32 = 112;
    pub const COMMAND_NOT_SUPPORTED: i32 = 115;
    pub const DOCUMENT_VALIDATION_FAILURE: i32 = 121;
    pub const NOT_IMPLEMENTED: i32 = 238;
    pub const NO_SUCH_TRANSACTION: i32 = 251;
    pub const TRANSACTION_TOO_OLD: i32 = 225;
    pub const OPERATION_NOT_SUPPORTED_IN_TRANSACTION: i32 = 263;
    pub const QUERY_EXCEEDED_MEMORY_LIMIT: i32 = 292;
    pub const EXCEEDED_MEMORY_LIMIT: i32 = 146;
    pub const QUERY_REJECTED_BY_SETTINGS: i32 = 411;
    pub const TOO_MANY_OPEN_TRANSACTIONS: i32 = 509;
    pub const API_STRICT_ERROR: i32 = 323;
    pub const NOT_WRITABLE_PRIMARY: i32 = 10107;
    pub const BSON_OBJECT_TOO_LARGE: i32 = 10334;
    pub const DUPLICATE_KEY: i32 = 11000;
    pub const CLIENT_METADATA_CANNOT_BE_MUTATED: i32 = 186;
    // SprigDB's own codes come from the registry (`SprigCode`).
    pub const SPRIG_CURSOR_LIMIT: i32 = sprig(SprigCode::CursorLimit);
    pub const SPRIG_DOC_ID_TOO_LONG: i32 = sprig(SprigCode::DocIdTooLong);
    pub const SPRIG_DOC_FEATURE_UNSUPPORTED: i32 = sprig(SprigCode::DocFeatureUnsupported);
    pub const SPRIG_PATH_PUBLISH_TIMEOUT: i32 = sprig(SprigCode::PathPublishTimeout);
    pub const SPRIG_DOC_TOO_DEEP: i32 = sprig(SprigCode::DocTooDeep);

    /// A registry code as a MongoDB code.
    #[must_use]
    pub const fn sprig(c: SprigCode) -> i32 {
        c.number() as i32
    }
}

/// The `codeName` of `code`; `Location<code>` for codes without a name,
/// as MongoDB does.
#[must_use]
pub fn code_name(code: i32) -> &'static str {
    match code {
        0 => "OK",
        1 => "InternalError",
        2 => "BadValue",
        4 => "NoSuchKey",
        6 => "HostUnreachable",
        7 => "HostNotFound",
        8 => "UnknownError",
        9 => "FailedToParse",
        11 => "UserNotFound",
        13 => "Unauthorized",
        14 => "TypeMismatch",
        18 => "AuthenticationFailed",
        20 => "IllegalOperation",
        26 => "NamespaceNotFound",
        27 => "IndexNotFound",
        31 => "RoleNotFound",
        40 => "ConflictingUpdateOperators",
        43 => "CursorNotFound",
        48 => "NamespaceExists",
        50 => "MaxTimeMSExpired",
        52 => "DollarPrefixedFieldName",
        53 => "InvalidIdField",
        54 => "NotSingleValueField",
        57 => "InvalidDBRef",
        59 => "CommandNotFound",
        64 => "WriteConcernTimeout",
        66 => "ImmutableField",
        67 => "CannotCreateIndex",
        68 => "IndexAlreadyExists",
        72 => "InvalidOptions",
        73 => "InvalidNamespace",
        79 => "UnknownReplWriteConcern",
        85 => "IndexOptionsConflict",
        86 => "IndexKeySpecsConflict",
        89 => "NetworkTimeout",
        91 => "ShutdownInProgress",
        100 => "UnsatisfiableWriteConcern",
        112 => "WriteConflict",
        115 => "CommandNotSupported",
        121 => "DocumentValidationFailure",
        133 => "FailedToSatisfyReadPreference",
        136 => "CappedPositionLost",
        139 => "JSInterpreterFailure",
        146 => "ExceededMemoryLimit",
        186 => "ClientMetadataCannotBeMutated",
        189 => "PrimarySteppedDown",
        225 => "TransactionTooOld",
        238 => "NotImplemented",
        251 => "NoSuchTransaction",
        256 => "TransactionCommitted",
        262 => "ExceededTimeLimit",
        263 => "OperationNotSupportedInTransaction",
        292 => "QueryExceededMemoryLimitNoDiskUseAllowed",
        323 => "APIStrictError",
        411 => "QueryRejectedBySettings",
        509 => "TooManyOpenTransactions",
        9001 => "SocketException",
        10107 => "NotWritablePrimary",
        10334 => "BSONObjectTooLarge",
        11000 => "DuplicateKey",
        11600 => "InterruptedAtShutdown",
        11601 => "Interrupted",
        11602 => "InterruptedDueToReplStateChange",
        13435 => "NotPrimaryNoSecondaryOk",
        13436 => "NotPrimaryOrSecondary",
        // SprigDB's own codes: their codeName from the registry.
        39000..=39999 => u16::try_from(code)
            .ok()
            .and_then(SprigCode::from_number)
            .map_or("Location", SprigCode::mongo_code_name),
        _ => "Location",
    }
}

/// The codes drivers retry: a retryable write failing with one gets
/// `RetryableWriteError`.
pub const RETRYABLE_CODES: [i32; 12] = [
    6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_shape() {
        let e = MongoError::new(code::DUPLICATE_KEY, "E11000 duplicate key")
            .with_extra(doc! {"keyPattern": {"_id": 1}, "keyValue": {"_id": 1}});
        let d = e.to_doc();
        assert!(d.get_f64("ok").unwrap().abs() < f64::EPSILON);
        assert_eq!(d.get_str("codeName").unwrap(), "DuplicateKey");
        assert!(d.contains_key("keyPattern"));
        let d = MongoError::new(91, "draining")
            .label("RetryableWriteError")
            .to_doc();
        assert_eq!(d.get_array("errorLabels").unwrap().len(), 1);
        assert_eq!(code_name(39630), "SprigCursorLimit");
    }
}
