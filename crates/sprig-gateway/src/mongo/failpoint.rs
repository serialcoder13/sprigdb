//! MongoDB's `failCommand` fail point, for driver test suites (chapter 30,
//! "Testing"; `decisions/r5-interim.md` D18).
//!
//! The drivers' retryable-writes, transactions and change-stream
//! specification tests inject errors through
//! `{configureFailPoint: "failCommand", mode, data}`, a test command
//! MongoDB accepts only with `enableTestCommands`. The gateway accepts it
//! only when the operator turns on `mongo.test_commands` (off by default:
//! 59 `CommandNotFound`, as MongoDB answers without test commands), and
//! scopes it to the caller's tenant, so a tenant can't disturb another.
//!
//! Supported: `mode` `alwaysOn`, `off`, `{times: n}` and `{skip: n}`; for
//! `failCommand`, `data` with `failCommands`, `errorCode`, `errorLabels`,
//! `closeConnection`, `writeConcernError`, `blockConnection` with
//! `blockTimeMS`, and `appName`. Also `onPrimaryTransactionalWrite` (a
//! retryable write commits, then the connection closes without a reply,
//! or `failBeforeCommitExceptionCode` fails it first) and
//! `failGetMoreAfterCursorCheckout` (`getMore` fails with `errorCode`).

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use bson::{Bson, Document, doc};
use sprig_doc::filter::as_i64;
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// `mongo.test_commands`.
pub static TEST_COMMANDS: Setting<bool> = Setting {
    key: "mongo.test_commands",
    default: false,
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: 30,
    doc: "Accept the failCommand fail point (driver test suites only)",
};
sprig_settings::register!(TEST_COMMANDS);

#[derive(Debug, Clone, Copy)]
enum Mode {
    AlwaysOn,
    Times(i64),
    Skip(i64),
}

#[derive(Debug, Clone)]
struct FailPoint {
    mode: Mode,
    commands: Vec<String>,
    error_code: Option<i32>,
    labels: Option<Vec<String>>,
    close: bool,
    write_concern_error: Option<Document>,
    block: Option<Duration>,
    app_name: Option<String>,
}

/// What a command meets.
#[derive(Debug, Clone, Default)]
pub struct Injected {
    /// Close the connection without a reply.
    pub close: bool,
    /// Fail with this code.
    pub error_code: Option<i32>,
    /// With these labels (`None`: the server's usual ones).
    pub labels: Option<Vec<String>>,
    /// Run, then report this write concern error.
    pub write_concern_error: Option<Document>,
    /// Wait this long first.
    pub block: Option<Duration>,
}

/// The fail points, by tenant and name.
#[derive(Debug, Default)]
pub struct FailPoints {
    map: Mutex<HashMap<(u64, String), FailPoint>>,
}

/// The fail points the gateway knows.
const NAMES: [&str; 3] = [
    "failCommand",
    "onPrimaryTransactionalWrite",
    "failGetMoreAfterCursorCheckout",
];

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FailPoints {
    /// `configureFailPoint` for `tenant`.
    ///
    /// # Errors
    /// 2 `BadValue` for anything but `failCommand` and the modes above.
    pub fn configure(&self, tenant: u64, body: &Document) -> Result<Document, MongoError> {
        let bad = |m: &str| MongoError::new(code::BAD_VALUE, m.to_owned());
        let name = body.get_str("configureFailPoint").unwrap_or_default();
        if !NAMES.contains(&name) {
            return Err(bad(&format!(
                "the fail point '{name}' isn't supported (only {})",
                NAMES.join(", ")
            )));
        }
        let key = (tenant, name.to_owned());
        let mode = match body.get("mode") {
            Some(Bson::String(s)) if s == "off" => {
                lock(&self.map).remove(&key);
                return Ok(doc! {"count": 0, "ok": 1.0});
            }
            Some(Bson::String(s)) if s == "alwaysOn" => Mode::AlwaysOn,
            Some(Bson::Document(d)) => {
                if let Some(n) = d.get("times").and_then(as_i64) {
                    Mode::Times(n)
                } else if let Some(n) = d.get("skip").and_then(as_i64) {
                    Mode::Skip(n)
                } else {
                    return Err(bad("mode must be alwaysOn, off, {times} or {skip}"));
                }
            }
            _ => return Err(bad("mode must be alwaysOn, off, {times} or {skip}")),
        };
        let data = body.get_document("data").cloned().unwrap_or_default();
        let fp = FailPoint {
            mode,
            commands: data
                .get_array("failCommands")
                .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_owned)).collect())
                .unwrap_or_default(),
            error_code: data
                .get(if name == "onPrimaryTransactionalWrite" {
                    "failBeforeCommitExceptionCode"
                } else {
                    "errorCode"
                })
                .and_then(as_i64)
                .and_then(|c| i32::try_from(c).ok()),
            labels: data.get_array("errorLabels").ok().map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_owned))
                    .collect()
            }),
            close: data
                .get_bool("closeConnection")
                .unwrap_or(name == "onPrimaryTransactionalWrite"),
            write_concern_error: data.get_document("writeConcernError").ok().cloned(),
            block: data
                .get_bool("blockConnection")
                .unwrap_or(false)
                .then(|| {
                    Duration::from_millis(
                        data.get("blockTimeMS")
                            .and_then(as_i64)
                            .and_then(|n| u64::try_from(n).ok())
                            .unwrap_or(0),
                    )
                }),
            app_name: data.get_str("appName").ok().map(str::to_owned),
        };
        lock(&self.map).insert(key, fp);
        Ok(doc! {"count": 0, "ok": 1.0})
    }

    /// What `command` from `app` of `tenant` meets at `failCommand` now
    /// (counting it).
    #[must_use]
    pub fn check(&self, tenant: u64, command: &str, app: Option<&str>) -> Option<Injected> {
        self.hit(tenant, "failCommand", Some(command), app)
    }

    /// What the fail point `name` (not `failCommand`) does now.
    #[must_use]
    pub fn named(&self, tenant: u64, name: &str) -> Option<Injected> {
        self.hit(tenant, name, None, None)
    }

    fn hit(
        &self,
        tenant: u64,
        name: &str,
        command: Option<&str>,
        app: Option<&str>,
    ) -> Option<Injected> {
        let key = (tenant, name.to_owned());
        let mut m = lock(&self.map);
        let fp = m.get_mut(&key)?;
        if let Some(command) = command
            && !fp.commands.iter().any(|c| c == command)
        {
            return None;
        }
        if let Some(a) = &fp.app_name
            && app != Some(a.as_str())
        {
            return None;
        }
        match &mut fp.mode {
            Mode::AlwaysOn => {}
            Mode::Times(n) => {
                if *n <= 0 {
                    m.remove(&key);
                    return None;
                }
                *n -= 1;
            }
            Mode::Skip(n) => {
                if *n > 0 {
                    *n -= 1;
                    return None;
                }
            }
        }
        let fp = m.get(&key)?;
        Some(Injected {
            close: fp.close,
            error_code: fp.error_code,
            labels: fp.labels.clone(),
            write_concern_error: fp.write_concern_error.clone(),
            block: fp.block,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_counts_down_per_tenant() {
        let f = FailPoints::default();
        f.configure(
            1,
            &doc! {"configureFailPoint": "failCommand", "mode": {"times": 2}, "data": {"failCommands": ["insert"], "errorCode": 91}},
        )
        .unwrap();
        assert!(f.check(2, "insert", None).is_none());
        assert!(f.check(1, "find", None).is_none());
        assert_eq!(f.check(1, "insert", None).unwrap().error_code, Some(91));
        assert!(f.check(1, "insert", None).is_some());
        assert!(f.check(1, "insert", None).is_none());
        f.configure(
            1,
            &doc! {"configureFailPoint": "failCommand", "mode": "alwaysOn", "data": {"failCommands": ["find"], "closeConnection": true, "appName": "x"}},
        )
        .unwrap();
        assert!(f.check(1, "find", Some("y")).is_none());
        assert!(f.check(1, "find", Some("x")).unwrap().close);
        f.configure(1, &doc! {"configureFailPoint": "failCommand", "mode": "off"})
            .unwrap();
        assert!(f.check(1, "find", Some("x")).is_none());
        assert!(f.configure(1, &doc! {"configureFailPoint": "other", "mode": "off"}).is_err());
        f.configure(
            1,
            &doc! {"configureFailPoint": "onPrimaryTransactionalWrite", "mode": {"times": 1}},
        )
        .unwrap();
        assert!(f.check(1, "insert", None).is_none());
        let i = f.named(1, "onPrimaryTransactionalWrite").unwrap();
        assert!(i.close && i.error_code.is_none());
        assert!(f.named(1, "onPrimaryTransactionalWrite").is_none());
    }
}
