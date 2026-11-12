//! Finds SprigDB's metrics in the source: every `counter!`, `gauge!` and
//! `histogram!` call (the `metrics` crate's macros, with or without a
//! `metrics::` path) whose name is a literal starting with `sprig_`, and the
//! literal label keys passed with it (`"code" => …`).
//!
//! It is a scanner, not a parser: it skips comments and string contents
//! while looking for the macros, and reads the call up to its matching
//! parenthesis. Names built at run time aren't found; there are none today.

use std::collections::{BTreeMap, BTreeSet};

/// The kind of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetricKind {
    /// A monotonically increasing count.
    Counter,
    /// A value that goes up and down.
    Gauge,
    /// A distribution.
    Histogram,
}

impl MetricKind {
    /// The kind as written in docs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

/// One metric, merged over every place it is recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metric {
    /// `sprig_…`.
    pub name: String,
    /// Its kind; a name recorded with two kinds keeps both.
    pub kinds: BTreeSet<MetricKind>,
    /// Label keys seen at any call.
    pub labels: BTreeSet<String>,
    /// Crates that record it (`sprig-gateway`, …).
    pub crates: BTreeSet<String>,
}

/// Collects metrics from source files.
#[derive(Debug, Default)]
pub struct Scanner {
    found: BTreeMap<String, Metric>,
}

impl Scanner {
    /// Scans one file's text, attributing what it finds to `krate`.
    pub fn scan(&mut self, krate: &str, text: &str) {
        let b = text.as_bytes();
        let mut i = 0;
        while i < b.len() {
            // Skip comments and string literals.
            if b[i] == b'/' && b.get(i + 1) == Some(&b'/') {
                i = memchr(b, i, b'\n');
                continue;
            }
            if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                i = text[i + 2..].find("*/").map_or(b.len(), |p| i + 2 + p + 2);
                continue;
            }
            if b[i] == b'"' {
                i = string_end(b, i);
                continue;
            }
            let kind = [
                ("counter!(", MetricKind::Counter),
                ("gauge!(", MetricKind::Gauge),
                ("histogram!(", MetricKind::Histogram),
            ]
            .into_iter()
            .find(|(m, _)| b[i..].starts_with(m.as_bytes()) && (i == 0 || !is_ident(b[i - 1])));
            let Some((mac, kind)) = kind else {
                i += 1;
                continue;
            };
            let open = i + mac.len() - 1;
            let close = matching_paren(b, open);
            self.record(krate, kind, &text[open + 1..close]);
            i = close + 1;
        }
    }

    fn record(&mut self, krate: &str, kind: MetricKind, args: &str) {
        let lits = literals(args);
        let Some((name, _)) = lits.first() else {
            return;
        };
        if !name.starts_with("sprig_") {
            return;
        }
        let m = self.found.entry(name.clone()).or_insert_with(|| Metric {
            name: name.clone(),
            kinds: BTreeSet::new(),
            labels: BTreeSet::new(),
            crates: BTreeSet::new(),
        });
        m.kinds.insert(kind);
        m.crates.insert(krate.to_owned());
        // A label key is a literal followed by `=>`.
        for (lit, after) in lits.iter().skip(1) {
            if args[*after..].trim_start().starts_with("=>") {
                m.labels.insert(lit.clone());
            }
        }
    }

    /// Everything found, by name.
    #[must_use]
    pub fn metrics(self) -> Vec<Metric> {
        self.found.into_values().collect()
    }
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn memchr(b: &[u8], from: usize, c: u8) -> usize {
    b[from..]
        .iter()
        .position(|&x| x == c)
        .map_or(b.len(), |p| from + p)
}

/// The index just past the string literal starting at `start` (a `"`).
fn string_end(b: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    b.len()
}

/// The index of the `)` matching the `(` at `open`, skipping strings.
fn matching_paren(b: &[u8], open: usize) -> usize {
    let mut depth = 0usize;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'"' => {
                i = string_end(b, i);
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
            _ => {}
        }
        i += 1;
    }
    b.len().saturating_sub(1)
}

/// The plain string literals in `args`, each with the offset just past it.
fn literals(args: &str) -> Vec<(String, usize)> {
    let b = args.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'"' {
            let end = string_end(b, i);
            out.push((args[i + 1..end.saturating_sub(1)].to_owned(), end));
            i = end;
        } else {
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_metrics_labels_and_kinds() {
        let src = r#"
            // counter!("sprig_commented_total") is ignored
            metrics::counter!("sprig_mover_errors_total", "code" => c.name()).increment(1);
            let s = "histogram!(\"sprig_in_a_string\")";
            histogram!(
                "sprig_turn_seconds",
                "role" => role.as_str(),
                "model" => m,
            ).record(t);
            gauge!("sprig_backlog", "x" => f("y")).set(1.0);
            counter!("other_total").increment(1);
            my_counter!("sprig_not_a_macro_of_ours");
        "#;
        let mut s = Scanner::default();
        s.scan("sprig-x", src);
        let m = s.metrics();
        let names: Vec<_> = m.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "sprig_backlog",
                "sprig_mover_errors_total",
                "sprig_turn_seconds"
            ]
        );
        assert_eq!(
            m[2].labels.iter().cloned().collect::<Vec<_>>(),
            ["model", "role"]
        );
        assert_eq!(m[0].labels.iter().cloned().collect::<Vec<_>>(), ["x"]);
        assert!(m[1].kinds.contains(&MetricKind::Counter));
        assert!(m[0].crates.contains("sprig-x"));
    }

    #[test]
    fn non_ascii_source_is_fine() {
        let mut s = Scanner::default();
        s.scan(
            "sprig-y",
            "/// p95 4.2 s → ~40 ms · é\nlet x = \"ä\"; counter!(\"sprig_ok_total\", \"kind\" => \"π\");",
        );
        assert_eq!(s.metrics()[0].name, "sprig_ok_total");
    }
}
