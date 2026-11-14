//! Where a process dials a backend address (development only).
//!
//! Cluster records name nodes by the addresses they have inside their data
//! center's network. A process on a developer's machine can't always reach
//! those addresses directly; the simulated data centers of the
//! cross-DC move tests (`deploy/compose/dc/`) also need every connection a process
//! makes to start in that process's own simulated DC, so that the
//! latency between DCs applies. Both are solved by a dial map, read once
//! from the environment:
//!
//! ```text
//! SPRIG_DIAL_MAP="10.10.1.21:3306=127.0.0.1:34411,10.10.2.21:3306=127.0.0.1:34414"
//! ```
//!
//! Every MariaDB connection (the gateway's protocol client, `mysql_async`
//! endpoints, binlog streams) dials [`dial`]`(addr)` instead of `addr`.
//! Without the variable nothing changes. Production never sets it.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::OnceLock;

/// The environment variable holding the map.
pub const DIAL_MAP_ENV: &str = "SPRIG_DIAL_MAP";

fn map() -> &'static HashMap<String, String> {
    static MAP: OnceLock<HashMap<String, String>> = OnceLock::new();
    MAP.get_or_init(|| {
        let m = std::env::var(DIAL_MAP_ENV)
            .map(|v| parse(&v))
            .unwrap_or_default();
        if !m.is_empty() {
            tracing::info!(
                entries = m.len(),
                "{DIAL_MAP_ENV}: backend addresses are redirected"
            );
        }
        m
    })
}

/// Parses `from=to,from=to`; malformed entries are skipped.
#[must_use]
pub fn parse(spec: &str) -> HashMap<String, String> {
    spec.split(',')
        .filter_map(|e| {
            let (from, to) = e.split_once('=')?;
            let (from, to) = (from.trim(), to.trim());
            (!from.is_empty() && !to.is_empty()).then(|| (from.to_owned(), to.to_owned()))
        })
        .collect()
}

/// The address to dial for `addr` (`host:port`): its entry in the dial map,
/// or `addr` itself.
#[must_use]
pub fn dial(addr: &str) -> Cow<'_, str> {
    match map().get(addr) {
        Some(to) => Cow::Owned(to.clone()),
        None => Cow::Borrowed(addr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_entries_and_skips_bad_ones() {
        let m = parse(" a:1=b:2 ,bad, =x, c:3 = d:4,");
        assert_eq!(m.len(), 2);
        assert_eq!(m["a:1"], "b:2");
        assert_eq!(m["c:3"], "d:4");
        // Unset in tests: everything dials itself.
        assert_eq!(dial("e:5"), "e:5");
    }
}
