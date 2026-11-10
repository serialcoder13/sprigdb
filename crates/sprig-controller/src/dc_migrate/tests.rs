#![allow(clippy::many_single_char_names, clippy::unwrap_used)]

use super::*;

fn d(s: &str) -> DcId {
    DcId::new(s).unwrap()
}

/// Chapter 24's worked example: 0.5 ms inside a DC, 8 ms fra1-ams1,
/// 160 ms fra1-sin1, 165 ms ams1-sin1.
fn rtt(a: &str, b: &str) -> Option<f64> {
    let mut p = [a, b];
    p.sort_unstable();
    match p {
        [x, y] if x == y => Some(0.5),
        ["ams1", "fra1"] => Some(8.0),
        ["fra1", "sin1"] => Some(160.0),
        ["ams1", "sin1"] => Some(165.0),
        _ => None,
    }
}

fn params() -> Params {
    Params {
        min_signal: 1000,
        min_pct: 30,
        min_ms: 5,
        relief_spare_pct: 15,
        margin: 0.15,
        weights: LeastBusyWeights::default(),
    }
}

fn load(dc: &str, spare: f64, now: u64) -> DcLoad {
    DcLoad {
        dc_id: d(dc),
        storage_spare: spare,
        cpu_spare: spare,
        conn_spare: spare,
        clusters: 1,
        dc_epoch: 1,
        stable_since_ms: 1,
        at_ms: now,
    }
}

fn acme() -> Vec<(String, u64)> {
    vec![
        ("fra1".into(), 2300),
        ("sin1".into(), 10_500),
        ("ams1".into(), 1000),
    ]
}

#[test]
fn the_books_worked_example_moves_acme_to_sin1() {
    let all = [d("fra1"), d("ams1"), d("sin1")];
    let ev = evaluate(
        &d("fra1"),
        &acme(),
        &all,
        &all,
        &rtt,
        &BTreeMap::new(),
        1,
        &params(),
    );
    assert_eq!(ev.weighted_total, 13_800);
    assert!((ev.latency_ms["fra1"] - 122.4).abs() < 0.05, "{ev:?}");
    assert!((ev.latency_ms["sin1"] - 39.0).abs() < 0.05);
    assert!((ev.latency_ms["ams1"] - 126.9).abs() < 0.05);
    assert_eq!(ev.decision, "candidate");
    assert_eq!(ev.best, Some(d("sin1")));
    assert!((ev.gain_ms - 83.4).abs() < 0.05);
    assert!(ev.remote_best.is_none());
}

#[test]
fn residency_keeps_acme_in_the_eu_and_raises_the_alert() {
    let all = [d("fra1"), d("ams1"), d("sin1")];
    let eu = [d("fra1"), d("ams1")];
    let ev = evaluate(
        &d("fra1"),
        &acme(),
        &eu,
        &all,
        &rtt,
        &BTreeMap::new(),
        1,
        &params(),
    );
    assert_eq!(ev.decision, "stay");
    assert_eq!(ev.best, Some(d("fra1")));
    assert_eq!(ev.remote_best, Some(d("sin1")));
    assert!(!ev.latency_ms.contains_key("sin1"));
}

#[test]
fn thresholds_signal_and_unmeasured_pairs() {
    let all = [d("fra1"), d("ams1"), d("sin1")];
    // Too little traffic: no signal.
    let few = vec![("sin1".to_owned(), 600)];
    let ev = evaluate(
        &d("fra1"),
        &few,
        &all,
        &all,
        &rtt,
        &BTreeMap::new(),
        1,
        &params(),
    );
    assert_eq!(ev.decision, "no_signal");
    // A gain under the relative threshold: stay. Traffic mostly from ams1,
    // 8 ms away: L(fra1) ~ 7.3 ms, L(ams1) ~ 1.2 ms (84% but 6.1 ms).
    let near = vec![("ams1".to_owned(), 9000), ("fra1".to_owned(), 900)];
    let ev = evaluate(
        &d("fra1"),
        &near,
        &all,
        &all,
        &rtt,
        &BTreeMap::new(),
        1,
        &params(),
    );
    assert_eq!(ev.decision, "candidate", "{ev:?}");
    let mut p = params();
    p.min_ms = 10;
    let ev = evaluate(&d("fra1"), &near, &all, &all, &rtt, &BTreeMap::new(), 1, &p);
    assert_eq!(
        ev.decision, "stay",
        "the absolute gain is too small: {ev:?}"
    );
    // A pair the matrix lacks drops that candidate; one the home lacks
    // keeps the tenant home.
    let partial = |a: &str, b: &str| if b == "sin1" { None } else { rtt(a, b) };
    let ev = evaluate(
        &d("fra1"),
        &acme(),
        &all,
        &all,
        &partial,
        &BTreeMap::new(),
        1,
        &params(),
    );
    assert!(!ev.latency_ms.contains_key("sin1"));
    assert_ne!(ev.best, Some(d("sin1")));
    let ev = evaluate(
        &d("sin1"),
        &acme(),
        &all,
        &all,
        &partial,
        &BTreeMap::new(),
        1,
        &params(),
    );
    assert_eq!(ev.decision, "stay");
    assert!(ev.reason.contains("lacks"));
}

#[test]
fn ties_prefer_the_home_then_the_least_busy() {
    let flat = |_: &str, _: &str| Some(1.0);
    let all = [d("ams1"), d("fra1"), d("sin1")];
    let c = vec![("sin1".to_owned(), 5000)];
    let ev = evaluate(
        &d("fra1"),
        &c,
        &all,
        &all,
        &flat,
        &BTreeMap::new(),
        1,
        &params(),
    );
    assert_eq!(ev.best, Some(d("fra1")));
    assert_eq!(ev.decision, "stay");
}

#[test]
fn relief_needs_a_busy_home_and_a_clear_margin() {
    let now = 10_000;
    let all = [d("fra1"), d("ams1"), d("sin1")];
    let mut loads: BTreeMap<DcId, DcLoad> = [
        (d("fra1"), load("fra1", 0.10, now)),
        (d("ams1"), load("ams1", 0.60, now)),
        (d("sin1"), load("sin1", 0.20, now)),
    ]
    .into_iter()
    .collect();
    let none: Vec<(String, u64)> = Vec::new();
    let ev = evaluate(&d("fra1"), &none, &all, &all, &rtt, &loads, now, &params());
    assert_eq!(ev.decision, "relief", "{ev:?}");
    assert_eq!(ev.best, Some(d("ams1")));
    // Not busy enough: stays.
    loads.insert(d("fra1"), load("fra1", 0.20, now));
    let ev = evaluate(&d("fra1"), &none, &all, &all, &rtt, &loads, now, &params());
    assert_eq!(ev.decision, "no_signal");
    // Busy, but nobody beats it by the margin.
    loads.insert(d("fra1"), load("fra1", 0.14, now));
    loads.insert(d("ams1"), load("ams1", 0.25, now));
    let ev = evaluate(&d("fra1"), &none, &all, &all, &rtt, &loads, now, &params());
    assert_eq!(ev.decision, "no_signal");
}

#[test]
fn cooldown_doubles_per_move_back_and_is_capped() {
    let h = 6.0;
    let lm = |k: u64| json!({"at_ms": 1_000, "moves_back": k});
    assert_eq!(cooldown_until(None, h, 7), None);
    assert_eq!(
        cooldown_until(Some(&lm(0)), h, 7),
        Some(1_000 + 6 * 3_600_000)
    );
    assert_eq!(
        cooldown_until(Some(&lm(1)), h, 7),
        Some(1_000 + 12 * 3_600_000)
    );
    assert_eq!(
        cooldown_until(Some(&lm(2)), h, 7),
        Some(1_000 + 24 * 3_600_000)
    );
    // 6 h * 2^5 = 192 h > 7 days: capped.
    assert_eq!(
        cooldown_until(Some(&lm(5)), h, 7),
        Some(1_000 + 7 * 86_400_000)
    );
    // Fractions of an hour (tests).
    assert_eq!(cooldown_until(Some(&lm(0)), 0.01, 7), Some(1_000 + 36_000));
}

/// Chapter 24's thrash test, on the pure decision: traffic that alternates
/// between two regions every 20 minutes, evaluated every minute with the
/// book's settings, moves the tenant at most once per cooldown.
#[test]
fn alternating_traffic_moves_at_most_once_per_cooldown() {
    let all = [d("fra1"), d("ams1"), d("sin1")];
    let mut home = d("fra1");
    let mut last_move: Option<Value> = None;
    let mut cand: Option<(DcId, u64)> = None;
    let mut moves = Vec::new();
    let minute = 60_000u64;
    for m in 0..(24 * 60u64) {
        let now = m * minute;
        let from = if (m / 20) % 2 == 0 { "sin1" } else { "fra1" };
        let clients = vec![(from.to_owned(), 20_000u64)];
        if cooldown_until(last_move.as_ref(), 6.0, 7).is_some_and(|u| u > now) {
            cand = None;
            continue;
        }
        let ev = evaluate(
            &home,
            &clients,
            &all,
            &all,
            &rtt,
            &BTreeMap::new(),
            now,
            &params(),
        );
        if !ev.qualifies() {
            cand = None;
            continue;
        }
        let best = ev.best.unwrap();
        let since = match &cand {
            Some((d, s)) if *d == best => *s,
            _ => {
                cand = Some((best.clone(), now));
                now
            }
        };
        if now - since >= 15 * minute {
            let back = last_move
                .as_ref()
                .and_then(|l| l.get("from").and_then(Value::as_str).map(str::to_owned))
                == Some(best.to_string());
            let k = if back {
                last_move
                    .as_ref()
                    .and_then(|l| l["moves_back"].as_u64())
                    .unwrap_or(0)
                    + 1
            } else {
                0
            };
            last_move = Some(json!({"at_ms": now, "from": home.as_str(), "moves_back": k}));
            moves.push((now / minute, best.clone()));
            home = best;
            cand = None;
        }
    }
    // The first move after 15 sustained minutes; each later one at least
    // a (doubling) cooldown after the one before.
    assert!(!moves.is_empty());
    for w in moves.windows(2) {
        assert!(w[1].0 - w[0].0 >= 6 * 60, "{moves:?}");
    }
    assert!(moves.len() <= 4, "{moves:?}");
}
