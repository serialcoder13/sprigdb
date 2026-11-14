use super::*;

/// `(package.Service, Rpc, METHOD, template)` for every `google.api.http`
/// rule in the protos, read from the `.proto` text.
fn annotated() -> Vec<(String, String, String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("proto");
    let mut out = Vec::new();
    for file in [
        "sprig/v1/sql.proto",
        "sprig/v1/functions.proto",
        "sprig/v1/logic.proto",
        "sprig/v1/kv.proto",
        "sprig/v1/vectors.proto",
        "sprig/v1/analytics.proto",
        "sprig/v1/search.proto",
        "sprig/v1/changes.proto",
        "sprig/admin/v1/admin.proto",
    ] {
        let text = std::fs::read_to_string(root.join(file)).unwrap();
        let package = text
            .lines()
            .find_map(|l| l.trim().strip_prefix("package "))
            .unwrap()
            .trim_end_matches(';')
            .to_owned();
        let mut service = String::new();
        let mut rpc = String::new();
        for line in text.lines().map(str::trim) {
            if let Some(s) = line.strip_prefix("service ") {
                service = s.split_whitespace().next().unwrap().to_owned();
            } else if let Some(r) = line.strip_prefix("rpc ") {
                rpc = r.split('(').next().unwrap().trim().to_owned();
            } else {
                for m in ["get", "post", "put", "delete"] {
                    if let Some(rest) = line.strip_prefix(&format!("{m}: \"")) {
                        let template = rest.trim_end_matches('"').to_owned();
                        out.push((
                            format!("{package}.{service}"),
                            rpc.clone(),
                            m.to_ascii_uppercase(),
                            template,
                        ));
                    }
                }
            }
        }
    }
    out
}

#[test]
fn the_table_matches_the_protos() {
    let from_protos = annotated();
    let table: Vec<_> = ROUTES
        .iter()
        .map(|r| {
            (
                r.service.to_owned(),
                r.rpc.to_owned(),
                r.method.to_owned(),
                r.template.to_owned(),
            )
        })
        .collect();
    assert_eq!(table, from_protos);
}

#[test]
fn templates_match_paths() {
    let m = find("POST", "/v1/databases/shop/sql:execute").unwrap();
    assert_eq!(m.route.short_name(), "Sql.Execute");
    assert_eq!(m.vars["database"], "shop");
    let m = find("post", "/v1/transactions/abc_DEF-1:commit").unwrap();
    assert_eq!(m.route.short_name(), "Transactions.Commit");
    assert_eq!(m.vars["transaction"], "transactions/abc_DEF-1");
    assert!(find("POST", "/v1/transactions/abc:frobnicate").is_none());
    assert!(find("POST", "/v1/databases/shop/sql").is_none());
    let m = find("GET", "/admin/v1/settings/gateway.max_connections:explain").unwrap();
    assert_eq!(m.route.short_name(), "Settings.ExplainSetting");
    assert_eq!(m.vars["name"], "gateway.max_connections");
    let m = find("GET", "/admin/v1/settings/gateway.max_connections").unwrap();
    assert_eq!(m.route.short_name(), "Settings.GetSetting");
    let m = find(
        "POST",
        "/admin/v1/tenants/acme/indexes/orders.sprig_auto_1:approve",
    )
    .unwrap();
    assert_eq!(m.route.short_name(), "Indexes.Apply");
    assert_eq!(m.vars["tenant"], "acme");
    assert_eq!(m.vars["name"], "orders.sprig_auto_1");
    let m = find("GET", "/v1/databases/my%20db/sql:query");
    assert!(m.is_none(), "Query is POST only");
    assert!(path_known("/v1/databases/x/sql:query"));
    let m = find("POST", "/v1/databases/my%20db/sql:query").unwrap();
    assert_eq!(m.vars["database"], "my db");
}

/// The operator routes: custom methods on the collection
/// and on one operator don't fall into each other's templates.
#[test]
fn operator_routes_match() {
    let cases = [
        ("POST", "/admin/v1/operators:login", "Operators.Login", None),
        ("POST", "/admin/v1/operators:enrol", "Operators.Enrol", None),
        (
            "POST",
            "/admin/v1/operators:logout",
            "Operators.Logout",
            None,
        ),
        (
            "GET",
            "/admin/v1/operators:whoami",
            "Operators.WhoAmI",
            None,
        ),
        (
            "GET",
            "/admin/v1/operators",
            "Operators.ListOperators",
            None,
        ),
        (
            "POST",
            "/admin/v1/operators",
            "Operators.CreateOperator",
            None,
        ),
        (
            "GET",
            "/admin/v1/operators/alice",
            "Operators.GetOperator",
            Some("alice"),
        ),
        (
            "POST",
            "/admin/v1/operators/alice:disable",
            "Operators.DisableOperator",
            Some("alice"),
        ),
        (
            "POST",
            "/admin/v1/operators/alice:resetFactor",
            "Operators.ResetOperatorFactor",
            Some("alice"),
        ),
        (
            "POST",
            "/admin/v1/operators/alice:setPassword",
            "Operators.SetOperatorPassword",
            Some("alice"),
        ),
        (
            "GET",
            "/admin/v1/operator-tokens",
            "Operators.ListTokens",
            None,
        ),
        (
            "POST",
            "/admin/v1/operator-tokens",
            "Operators.CreateToken",
            None,
        ),
        (
            "POST",
            "/admin/v1/operator-tokens/k1:revoke",
            "Operators.RevokeToken",
            Some("k1"),
        ),
    ];
    for (method, path, rpc, name) in cases {
        let m = find(method, path).unwrap_or_else(|| panic!("{method} {path}"));
        assert_eq!(m.route.short_name(), rpc, "{method} {path}");
        assert_eq!(m.vars.get("name").map(String::as_str), name, "{path}");
        assert!(m.route.is_admin());
    }
    assert!(find("GET", "/admin/v1/operators:login").is_none());
    assert!(find("POST", "/admin/v1/operators/alice").is_none());
    assert!(find("POST", "/admin/v1/operators/alice:frobnicate").is_none());
    assert!(path_known("/admin/v1/operators:login"));
    assert!(by_name("Operators.WhoAmI").is_some());
}

#[test]
fn paths_invert_matching() {
    for r in ROUTES {
        let mut vars = BTreeMap::new();
        for v in template_vars(r.template) {
            let value = if r.template.contains("transactions/*") {
                "transactions/h1".to_owned()
            } else {
                format!("{v}1")
            };
            vars.insert(v, value);
        }
        let path = r.path(&vars);
        let m = find(r.method, &path).unwrap_or_else(|| panic!("{path}"));
        assert_eq!(m.route, r, "{path}");
        assert_eq!(m.vars, vars);
    }
}

#[test]
fn percent_coding_round_trips() {
    for s in ["plain", "a b", "x/y:z", "ünï"] {
        assert_eq!(percent_decode(&percent_encode(s)), s);
    }
    assert_eq!(percent_decode("bad%zz%4"), "bad%zz%4");
}

#[test]
fn openapi_lists_every_route() {
    let data = openapi(false);
    assert!(data["paths"]["/v1/{transaction}:commit"]["post"].is_object());
    assert_eq!(
        data["paths"]["/v1/databases/{database}/sql:query"]["post"]["operationId"],
        "Sql.Query"
    );
    let admin = openapi(true);
    let n: usize = admin["paths"]
        .as_object()
        .unwrap()
        .values()
        .map(|v| v.as_object().unwrap().len())
        .sum();
    assert_eq!(n, ROUTES.iter().filter(|r| r.is_admin()).count());
}
