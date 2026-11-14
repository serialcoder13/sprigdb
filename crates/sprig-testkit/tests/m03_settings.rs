//! The settings check: a setting changed at object scope takes effect on
//! every gateway within seconds, and a value outside its bounds is refused.
//!
//! Tenant A sets a key on one table with SQL, `ALTER TABLE t SPRIG SET (k =
//! v)`, and every observer (a second session on the same endpoint, plus
//! each URL in `SPRIG_TEST_M03_OBSERVERS`, for example a second gateway
//! process) must read the new value at object scope from
//! `information_schema.SPRIG_SETTINGS` (or `SHOW SPRIG SETTINGS ... FOR
//! TABLE`, if the view isn't there) within `SPRIG_TEST_M03_WITHIN_S`
//! seconds (5). A second table must keep its old value. Then:
//!
//! - a value above the key's hard maximum is refused with 39080
//!   (`ER_SPRIG_SETTING_OUT_OF_BOUNDS`) and the old value stays;
//! - with database-scope bounds (`ALTER DATABASE d SPRIG SET BOUNDS (k MAX
//!   n)`), a table value above `n` is refused with 39080 as well
//!   (`SPRIG_TEST_M03_SCOPE_BOUNDS=0` leaves this out).
//!
//! ```sh
//! SPRIG_TEST_M03_A=mysql://acme.app:sprig-dev-app@127.0.0.1:13307 \
//!   cargo test -p sprig-testkit --test m03_settings -- --ignored --nocapture
//! # with a second gateway on other ports:
//! SPRIG_TEST_M03_A=mysql://acme.app:sprig-dev-app@127.0.0.1:13307 \
//! SPRIG_TEST_M03_OBSERVERS=mysql://acme.app:sprig-dev-app@127.0.0.1:13317 \
//!   cargo test -p sprig-testkit --test m03_settings -- --ignored --nocapture
//! ```
//!
//! The key is `reads.max_replica_lag_ms` (scopes: all; hard bounds 0 to
//! 86,400,000), or `SPRIG_TEST_M03_SETTING_KEY` with its hard maximum in
//! `SPRIG_TEST_M03_SETTING_MAX`. With `SPRIG_TEST_M03_BASELINE=1` (direct
//! MariaDB) the statements must fail with a syntax error and every check
//! reports n/a. `SPRIG_TEST_M03_REPORT` names a file for one JSON line per
//! check.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row, Value};
use sprig_testkit::{require_env, skip};

const A: &str = "SPRIG_TEST_M03_A";
const OBSERVERS: &str = "SPRIG_TEST_M03_OBSERVERS";
const BASELINE: &str = "SPRIG_TEST_M03_BASELINE";
const REPORT: &str = "SPRIG_TEST_M03_REPORT";
const OUT_OF_BOUNDS: u16 = 39080;

fn tag_of(user: &str) -> String {
    user.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

async fn connect(url: &str) -> Conn {
    let opts = Opts::from_url(url).unwrap_or_else(|e| panic!("{url}: {e}"));
    Conn::new(OptsBuilder::from_opts(opts).prefer_socket(false))
        .await
        .unwrap_or_else(|e| panic!("connect {url}: {e}"))
}

/// The server's error number, or `None` on success.
async fn exec(conn: &mut Conn, sql: &str) -> Result<(), (u16, String)> {
    conn.query_drop(sql).await.map_err(|e| match e {
        mysql_async::Error::Server(s) => (s.code, s.message),
        other => (0, other.to_string()),
    })
}

fn cell(v: &Value) -> String {
    match v {
        Value::NULL => String::new(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => other.as_sql(true),
    }
}

fn unquote(s: &str) -> &str {
    s.trim().trim_matches(|c| c == '"' || c == '\'')
}

/// What an observer reads for `key` on one table.
#[derive(Debug, Default)]
struct Reading {
    value: Option<String>,
    source: Option<String>,
    via: &'static str,
    raw: String,
}

/// Reads the effective value from `information_schema.SPRIG_SETTINGS`,
/// falling back to `SHOW SPRIG SETTINGS`. Columns are found by name
/// (`VALUE`, and a column with `SOURCE` in its name), so either form works.
async fn read(conn: &mut Conn, db: &str, table: &str, key: &str) -> Result<Reading, String> {
    let queries = [
        (
            "information_schema.SPRIG_SETTINGS",
            format!(
                "SELECT * FROM information_schema.SPRIG_SETTINGS WHERE TABLE_SCHEMA = '{db}' \
                 AND TABLE_NAME = '{table}' AND SETTING_NAME = '{key}'"
            ),
        ),
        (
            "SHOW SPRIG SETTINGS",
            format!("SHOW SPRIG SETTINGS LIKE '{key}' FOR TABLE {db}.{table}"),
        ),
    ];
    let mut errors = Vec::new();
    for (via, q) in queries {
        match conn.query::<Row, _>(q.as_str()).await {
            Ok(rows) => {
                let mut r = Reading {
                    via,
                    ..Reading::default()
                };
                for row in rows {
                    let cols = row.columns();
                    let cells: Vec<String> = row.unwrap().iter().map(cell).collect();
                    r.raw.push_str(&cells.join("|"));
                    r.raw.push(' ');
                    for (c, v) in cols.iter().zip(&cells) {
                        let name = c.name_str().to_ascii_lowercase();
                        if name == "value" {
                            r.value = Some(unquote(v).to_owned());
                        } else if name.contains("source") {
                            r.source = Some(v.to_ascii_lowercase());
                        }
                    }
                    // Prefer the object-scope row if the view lists several.
                    if r.source.as_deref().is_some_and(|s| s.contains("object")) {
                        break;
                    }
                }
                return Ok(r);
            }
            Err(e) => errors.push(format!("{via}: {e}")),
        }
    }
    Err(errors.join("; "))
}

/// The value to set: from `nanos`, in 1001 to 51001, so never
/// `reads.max_replica_lag_ms`'s default (1000), and never `previous` (the
/// value read before the change), so the change is always visible.
fn fresh_value(nanos: u128, previous: Option<&str>) -> u64 {
    let v = 1001 + u64::try_from(nanos % 50_000).unwrap();
    if previous.is_some_and(|p| p == v.to_string()) {
        v + 1
    } else {
        v
    }
}

#[test]
fn fresh_value_avoids_the_default_and_the_previous_value() {
    // nanos % 50_000 == 0 gave the default before.
    assert_eq!(fresh_value(0, Some("1000")), 1001);
    assert_eq!(fresh_value(50_000, None), 1001);
    assert_eq!(fresh_value(49_999, None), 51_000);
    assert_eq!(fresh_value(49_999, Some("51000")), 51_001);
    assert_eq!(fresh_value(7, Some("1008")), 1009);
    for n in 0..50_000_u128 {
        let v = fresh_value(n, Some("1000"));
        assert_ne!(v, 1000);
        assert_ne!(fresh_value(n, Some(&v.to_string())), v);
    }
}

fn observed(r: &Reading, want: &str) -> bool {
    r.value.as_deref() == Some(want) && r.source.as_deref().is_none_or(|s| s.contains("object"))
}

fn report(file: Option<&str>, tenant: &str, check: &str, status: &str, detail: &str) {
    let line = serde_json::json!({
        "suite": "settings", "tenant": tenant, "check": check, "status": status,
        "detail": detail.chars().take(400).collect::<String>(), "ms": 0,
    });
    println!("{line}");
    if let Some(f) = file {
        use std::io::Write;
        let mut fh = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(f)
            .unwrap_or_else(|e| panic!("{f}: {e}"));
        writeln!(fh, "{line}").unwrap();
    }
}

#[tokio::test]
#[ignore = "needs a tenant endpoint: SPRIG_TEST_M03_A"]
async fn object_setting_live_and_bounded() {
    let url = require_env!(A);
    let baseline = skip::flag(BASELINE);
    let rf_owned = skip::var(REPORT);
    let rf = rf_owned.as_deref();
    let key = skip::var("SPRIG_TEST_M03_SETTING_KEY")
        .unwrap_or_else(|| "reads.max_replica_lag_ms".to_owned());
    let hard_max: u64 = skip::var("SPRIG_TEST_M03_SETTING_MAX").map_or(86_400_000, |v| {
        v.parse().expect("SPRIG_TEST_M03_SETTING_MAX")
    });
    let within = Duration::from_secs(
        skip::var("SPRIG_TEST_M03_WITHIN_S")
            .map_or(5, |v| v.parse().expect("SPRIG_TEST_M03_WITHIN_S")),
    );
    let scope_bounds = skip::var("SPRIG_TEST_M03_SCOPE_BOUNDS").is_none_or(|v| v != "0");

    let user = Opts::from_url(&url).unwrap().user().unwrap().to_owned();
    let tag = tag_of(&user);
    let db = if baseline {
        format!("{tag}_m03_settings")
    } else {
        "m03_settings".to_owned()
    };
    let mut observer_urls = vec![url.clone()];
    observer_urls.extend(
        skip::var(OBSERVERS)
            .map(|v| skip::urls(&v))
            .unwrap_or_default(),
    );

    let mut writer = connect(&url).await;
    exec(&mut writer,
        &format!(
            "DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db}; \
             CREATE TABLE {db}.hot (id INT PRIMARY KEY); CREATE TABLE {db}.cold (id INT PRIMARY KEY)"
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("setup: {e:?}"));
    let mut observers = Vec::new();
    for u in &observer_urls {
        observers.push((u.clone(), connect(u).await));
    }

    // The value before the change: the inherited one, since the database
    // is new (the key's default unless a wider scope sets it).
    let before = if baseline {
        Err("baseline".to_owned())
    } else {
        read(&mut observers[0].1, &db, "hot", &key).await
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let v = fresh_value(nanos, before.as_ref().ok().and_then(|r| r.value.as_deref()));
    let set = format!("ALTER TABLE {db}.hot SPRIG SET ({key} = {v})");
    let mut failed = Vec::new();

    if baseline {
        // MariaDB has no SPRIG SET: the statement must fail as a syntax error,
        // which proves the check reaches the server and reads its errors.
        let r = exec(&mut writer, &set).await;
        let detail = match &r {
            Err((code, _)) => format!(
                "MariaDB answered {code} to `{set}`, as expected; {} observer(s) connected",
                observers.len()
            ),
            Ok(()) => format!("`{set}` succeeded on a plain MariaDB"),
        };
        let status = if r.is_err() { "n/a" } else { "fail" };
        for check in [
            "object_scope",
            "object_only",
            "out_of_bounds",
            "scope_bounds",
        ] {
            report(rf, &tag, check, status, &detail);
        }
        let _ = exec(&mut writer, &format!("DROP DATABASE IF EXISTS {db}")).await;
        assert!(r.is_err(), "{detail}");
        return;
    }

    // 1. The value at object scope, seen by every observer.
    let t0 = Instant::now();
    if let Err(e) = exec(&mut writer, &set).await {
        report(rf, &tag, "object_scope", "fail", &format!("`{set}`: {e:?}"));
        failed.push(format!("object_scope: {set}: {e:?}"));
    } else {
        let mut seen: Vec<Option<Duration>> = vec![None; observers.len()];
        let mut last: Vec<String> = vec![String::new(); observers.len()];
        while t0.elapsed() < within && seen.iter().any(Option::is_none) {
            for (i, (_, c)) in observers.iter_mut().enumerate() {
                if seen[i].is_some() {
                    continue;
                }
                match read(c, &db, "hot", &key).await {
                    Ok(r) if observed(&r, &v.to_string()) => seen[i] = Some(t0.elapsed()),
                    Ok(r) => {
                        last[i] = format!(
                            "{} read {:?} from {:?} ({})",
                            r.via,
                            r.value,
                            r.source,
                            r.raw.trim()
                        );
                    }
                    Err(e) => last[i] = e,
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let per: Vec<String> = observers
            .iter()
            .zip(&seen)
            .zip(&last)
            .map(|(((u, _), s), l)| match s {
                Some(d) => format!("{} after {} ms", redact(u), d.as_millis()),
                None => format!("{} not within {}s: {l}", redact(u), within.as_secs()),
            })
            .collect();
        let ok = seen.iter().all(Option::is_some);
        let detail = format!(
            "{set}; before: {}; {}",
            before.as_ref().map_or_else(Clone::clone, |r| format!(
                "{:?} from {:?}",
                r.value, r.source
            )),
            per.join("; ")
        );
        report(
            rf,
            &tag,
            "object_scope",
            if ok { "pass" } else { "fail" },
            &detail,
        );
        if !ok {
            failed.push(format!("object_scope: {detail}"));
        }
    }

    // 2. Another table of the same database keeps its value.
    match read(&mut observers[0].1, &db, "cold", &key).await {
        Ok(r) if r.value.as_deref() != Some(v.to_string().as_str()) => report(
            rf,
            &tag,
            "object_only",
            "pass",
            &format!("{db}.cold still reads {:?} from {:?}", r.value, r.source),
        ),
        Ok(r) => {
            report(
                rf,
                &tag,
                "object_only",
                "fail",
                &format!("{db}.cold reads {v} too ({})", r.raw),
            );
            failed.push(format!("object_only: {db}.cold reads {v}"));
        }
        Err(e) => {
            report(rf, &tag, "object_only", "fail", &e);
            failed.push(format!("object_only: {e}"));
        }
    }

    // 3. Above the hard maximum: 39080, and the old value stays.
    let too_big = format!("ALTER TABLE {db}.hot SPRIG SET ({key} = {})", hard_max + 1);
    let r = exec(&mut writer, &too_big).await;
    let after = read(&mut observers[0].1, &db, "hot", &key).await;
    let kept = after.as_ref().is_ok_and(|a| observed(a, &v.to_string()));
    let ok = matches!(&r, Err((OUT_OF_BOUNDS, _))) && kept;
    let detail = format!(
        "`{too_big}`: {}; value afterwards {}",
        match &r {
            Ok(()) => "accepted".to_owned(),
            Err((c, m)) => format!("{c} {m}"),
        },
        after.map_or_else(|e| e, |a| format!("{:?}", a.value))
    );
    report(
        rf,
        &tag,
        "out_of_bounds",
        if ok { "pass" } else { "fail" },
        &detail,
    );
    if !ok {
        failed.push(format!("out_of_bounds: {detail}"));
    }

    // 4. Bounds set at database scope limit the table below it.
    if scope_bounds {
        let bound = v + 1000;
        let steps = [
            format!("ALTER DATABASE {db} SPRIG SET BOUNDS ({key} MAX {bound})"),
            format!("ALTER TABLE {db}.hot SPRIG SET ({key} = {})", bound + 1000),
            format!("ALTER DATABASE {db} SPRIG RESET BOUNDS ({key})"),
        ];
        let r0 = exec(&mut writer, &steps[0]).await;
        let r1 = exec(&mut writer, &steps[1]).await;
        let r2 = exec(&mut writer, &steps[2]).await;
        let ok = r0.is_ok() && matches!(&r1, Err((OUT_OF_BOUNDS, _))) && r2.is_ok();
        let detail = format!(
            "`{}`: {r0:?}; `{}`: {r1:?}; `{}`: {r2:?}",
            steps[0], steps[1], steps[2]
        );
        report(
            rf,
            &tag,
            "scope_bounds",
            if ok { "pass" } else { "fail" },
            &detail,
        );
        if !ok {
            failed.push(format!("scope_bounds: {detail}"));
        }
    }

    let _ = exec(
        &mut writer,
        &format!("ALTER TABLE {db}.hot SPRIG RESET ({key})"),
    )
    .await;
    let _ = exec(&mut writer, &format!("DROP DATABASE IF EXISTS {db}")).await;
    assert!(failed.is_empty(), "settings check:\n{}", failed.join("\n"));
}

/// A URL without its password, for messages.
fn redact(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(s), Some(at)) if at > s => {
            let creds = &url[s + 3..at];
            let user = creds.split(':').next().unwrap_or(creds);
            format!("{}{user}@{}", &url[..s + 3], &url[at + 1..])
        }
        _ => url.to_owned(),
    }
}
