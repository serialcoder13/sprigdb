//! CLI parsing and exit codes.

use super::*;

#[test]
fn parses_and_durations() {
    command().debug_assert();
    assert_eq!(parse_duration_ms("30s"), Ok(30_000));
    assert_eq!(parse_duration_ms("500ms"), Ok(500));
    assert_eq!(parse_duration_ms("2m"), Ok(120_000));
    assert!(parse_duration_ms("soon").is_err());
}
