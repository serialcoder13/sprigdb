use super::*;

#[test]
fn bearer_tokens() {
    assert_eq!(bearer(Some("Bearer abc")), Some("abc"));
    assert_eq!(bearer(Some("bearer  abc ")), Some("abc"));
    assert_eq!(bearer(Some("Basic abc")), None);
    assert!(is_session_token(Some("Bearer sprigat_x")));
    assert!(!is_session_token(Some("Bearer sprig_acme_k_s")));
}

#[test]
fn literals_are_quoted() {
    assert_eq!(quote_literal("a'b\\c"), "'a''b\\\\c'");
    assert_eq!(quote_literal("x\0"), "'x\\0'");
}

#[test]
fn origin_checks() {
    let mut cfg = StudioConfig::default();
    let host = Some("127.0.0.1:28080");
    // A same-origin fetch, with or without the headers.
    assert!(
        same_origin(
            &cfg,
            false,
            host,
            Some("http://127.0.0.1:28080"),
            Some("same-origin")
        )
        .is_ok()
    );
    assert!(same_origin(&cfg, false, host, None, None).is_ok());
    // Another site.
    let e = same_origin(&cfg, false, host, Some("https://evil.example"), None).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39922);
    let e = same_origin(&cfg, false, host, None, Some("cross-site")).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39922);
    assert!(same_origin(&cfg, false, host, None, Some("same-site")).is_err());
    // The scheme matters: an https page isn't this plain-HTTP origin...
    assert!(same_origin(&cfg, false, host, Some("https://127.0.0.1:28080"), None).is_err());
    // ...unless a TLS proxy in front is listed.
    cfg.public_origins = vec!["https://db.example.com".into()];
    assert!(same_origin(&cfg, false, host, Some("https://db.example.com"), None).is_ok());
    // Studio's export form: Origin null, vouched for by Sec-Fetch-Site.
    assert!(same_origin(&cfg, false, host, Some("null"), Some("same-origin")).is_ok());
    assert!(same_origin(&cfg, false, host, Some("null"), None).is_err());
}
