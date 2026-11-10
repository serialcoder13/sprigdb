use super::*;

static FILES: &[(&str, Asset)] = &[
    (
        "assets/index-abc123.js",
        Asset {
            content_type: "text/javascript; charset=utf-8",
            identity: b"console.log('studio')",
            gzip: Some(b"GZ"),
            brotli: Some(b"BR"),
            etag: "\"js1\"",
            immutable: true,
        },
    ),
    (
        "favicon.svg",
        Asset {
            content_type: "image/svg+xml",
            identity: b"<svg/>",
            gzip: None,
            brotli: None,
            etag: "\"svg1\"",
            immutable: false,
        },
    ),
    (
        "index.html",
        Asset {
            content_type: "text/html; charset=utf-8",
            identity: b"<!doctype html><div id=root></div>",
            gzip: Some(b"GZI"),
            brotli: None,
            etag: "\"idx1\"",
            immutable: false,
        },
    ),
];

static STORE: AssetStore = AssetStore::new(FILES, "v-test");
static EMPTY: AssetStore = AssetStore::new(&[], "none");

fn get(path: &str) -> StudioResponse {
    get_with(&StudioConfig::default(), path, "http", None, None)
}

fn get_with(
    cfg: &StudioConfig,
    path: &str,
    listener: &str,
    ae: Option<&str>,
    inm: Option<&str>,
) -> StudioResponse {
    serve(
        &STORE,
        cfg,
        &StudioRequest {
            method: "GET",
            path,
            listener,
            tls: listener == "https",
            accept_encoding: ae,
            if_none_match: inm,
            body: b"",
        },
    )
}

fn code(r: &StudioResponse) -> u64 {
    serde_json::from_slice::<Value>(&r.body).unwrap()["code"]
        .as_u64()
        .unwrap()
}

#[test]
fn every_response_carries_the_security_headers() {
    for path in [
        "/studio/",
        "/studio/assets/index-abc123.js",
        "/studio/nope.js",
        "/studio/../x",
        "/studio/version.json",
    ] {
        let r = get(path);
        for h in [
            "content-security-policy",
            "x-content-type-options",
            "referrer-policy",
            "cross-origin-opener-policy",
            "cross-origin-resource-policy",
            "x-frame-options",
            "permissions-policy",
        ] {
            assert!(r.header(h).is_some(), "{path}: {h}");
        }
        assert_eq!(r.header("x-frame-options"), Some("DENY"));
        assert!(
            r.header("strict-transport-security").is_none(),
            "no HSTS on 8080"
        );
    }
    let tls = get_with(&StudioConfig::default(), "/studio/", "https", None, None);
    assert!(tls.header("strict-transport-security").is_some());
}

#[test]
fn routes_fall_back_to_index_with_no_store() {
    for path in [
        "/studio/",
        "/studio/login",
        "/studio/t/acme/q/1",
        "/studio/index.html",
    ] {
        let r = get(path);
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(r.header("cache-control"), Some("no-store"));
        assert_eq!(r.header("content-type"), Some("text/html; charset=utf-8"));
    }
    // A missing file with an extension is a 404, not the app.
    assert_eq!(get("/studio/assets/missing-1.js").status, 404);
    // The mount itself redirects to the slash form.
    let r = get("/studio");
    assert_eq!(r.status, 308);
    assert_eq!(r.header("location"), Some("/studio/"));
}

#[test]
fn hashed_assets_are_immutable_and_compressed_by_preference() {
    let cfg = StudioConfig::default();
    let p = "/studio/assets/index-abc123.js";
    let r = get_with(&cfg, p, "http", Some("gzip, deflate, br"), None);
    assert_eq!(r.header("content-encoding"), Some("br"));
    assert_eq!(&r.body[..], b"BR");
    assert_eq!(
        r.header("cache-control"),
        Some("public, max-age=31536000, immutable")
    );
    assert_eq!(r.header("vary"), Some("Accept-Encoding"));
    let r = get_with(&cfg, p, "http", Some("gzip, br;q=0"), None);
    assert_eq!(r.header("content-encoding"), Some("gzip"));
    let r = get_with(&cfg, p, "http", None, None);
    assert_eq!(r.header("content-encoding"), None);
    assert_eq!(&r.body[..], b"console.log('studio')");
    // ETag → 304 with no body.
    let r = get_with(&cfg, p, "http", Some("br"), Some("\"js1\""));
    assert_eq!(r.status, 304);
    assert!(r.body.is_empty());
    // Unhashed files revalidate.
    assert_eq!(
        get("/studio/favicon.svg").header("cache-control"),
        Some("no-cache")
    );
}

#[test]
fn traversal_is_refused() {
    for path in [
        "/studio/../etc/passwd",
        "/studio/%2e%2e/x.js",
        "/studio/assets/%2E%2E/%2E%2E/x.js",
        "/studio/./index.html",
        "/studio/a%5c..%5cb.js",
        "/studio/%00.js",
        "/studio/%zz.js",
    ] {
        let r = get(path);
        assert_eq!(r.status, 404, "{path}");
    }
}

#[test]
fn disabled_or_wrong_listener_is_39920() {
    let cfg = StudioConfig {
        enabled: false,
        ..Default::default()
    };
    let r = get_with(&cfg, "/studio/", "http", None, None);
    assert_eq!(r.status, 404);
    assert_eq!(code(&r), 39920);
    let cfg = StudioConfig {
        listeners: vec!["https".into()],
        ..Default::default()
    };
    assert_eq!(code(&get_with(&cfg, "/studio/", "http", None, None)), 39920);
    assert_eq!(get_with(&cfg, "/studio/", "https", None, None).status, 200);
}

#[test]
fn a_binary_without_a_build_says_so() {
    let cfg = StudioConfig::default();
    let req = StudioRequest {
        method: "GET",
        path: "/studio/",
        listener: "http",
        ..Default::default()
    };
    let r = serve(&EMPTY, &cfg, &req);
    assert_eq!(r.status, 503);
    assert!(r.header("x-sprig-error").unwrap().starts_with("39932"));
    let v = serve(
        &EMPTY,
        &cfg,
        &StudioRequest {
            path: "/studio/version.json",
            ..req
        },
    );
    assert_eq!(v.status, 200);
    let j: Value = serde_json::from_slice(&v.body).unwrap();
    assert_eq!(j["built"], false);
}

#[test]
fn version_and_error_reports() {
    let r = get("/studio/version.json");
    let j: Value = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(j["build_version"], "v-test");
    assert_eq!(j["min_api"], "v1");
    assert_eq!(r.header("cache-control"), Some("no-store"));
    let cfg = StudioConfig::default();
    let post = |body: &[u8]| {
        serve(
            &STORE,
            &cfg,
            &StudioRequest {
                method: "POST",
                path: "/studio/_/errors",
                listener: "http",
                body,
                ..Default::default()
            },
        )
    };
    assert_eq!(post(br#"{"kind":"js","message":"boom"}"#).status, 204);
    assert_eq!(post(b"not json").status, 204);
    assert_eq!(post(&vec![b'x'; MAX_ERROR_REPORT + 1]).status, 413);
    // Other writes are refused.
    let r = serve(
        &STORE,
        &cfg,
        &StudioRequest {
            method: "POST",
            path: "/studio/index.html",
            listener: "http",
            ..Default::default()
        },
    );
    assert_eq!(r.status, 405);
}

#[test]
fn head_has_no_body() {
    let r = serve(
        &STORE,
        &StudioConfig::default(),
        &StudioRequest {
            method: "HEAD",
            path: "/studio/assets/index-abc123.js",
            listener: "http",
            ..Default::default()
        },
    );
    assert_eq!(r.status, 200);
    assert!(r.body.is_empty());
    assert_eq!(r.header("content-length"), Some("21"));
}

#[test]
fn studio_paths() {
    let cfg = StudioConfig::default();
    assert!(is_studio_path(&cfg, "/studio"));
    assert!(is_studio_path(&cfg, "/studio/x"));
    assert!(!is_studio_path(&cfg, "/studiox"));
    assert!(!is_studio_path(&cfg, "/v1/auth/login"));
}
