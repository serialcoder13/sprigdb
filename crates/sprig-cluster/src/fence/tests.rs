use super::*;

fn db_row(user: &str, host: &str, db: &str, yes: &[&str]) -> TextRow {
    let mut r: TextRow = vec![
        ("Host".into(), Some(host.into())),
        ("Db".into(), Some(db.into())),
        ("User".into(), Some(user.into())),
    ];
    for (c, _) in WRITE_PRIVILEGES.iter().chain(READ_PRIVILEGES) {
        let v = if yes.contains(c) { "Y" } else { "N" };
        r.push(((*c).to_owned(), Some(v.to_owned())));
    }
    r.push(("Grant_priv".into(), Some("Y".into())));
    r
}

fn schemas() -> Vec<String> {
    vec!["t42_d1_g3_s17".into()]
}

#[test]
fn revokes_only_held_privileges_of_the_tenants_accounts() {
    let rows = vec![
        db_row(
            "t42_u7",
            "%",
            "t42_d1_g3_s17",
            &["Select_priv", "Insert_priv", "Update_priv", "Execute_priv"],
        ),
        // A role, stored with the escaped schema name.
        db_row("t42_r2", "", "t42\\_d1\\_g3\\_s17", &["Delete_priv"]),
        // Another tenant whose name shares the prefix: never touched.
        db_row("t421_u1", "%", "t42_d1_g3_s17", &["Insert_priv"]),
        // Not a tenant account.
        db_row("app", "%", "t42_d1_g3_s17", &["Insert_priv"]),
        // Another schema of the tenant.
        db_row("t42_u7", "%", "t42_d1_g3_s18", &["Insert_priv"]),
        // Read-only: nothing to revoke for Writes.
        db_row("t42_u8", "%", "t42_d1_g3_s17", &["Select_priv"]),
    ];
    let g = grants_to_revoke(&rows, TenantId(42), &schemas(), FenceScope::Writes);
    assert_eq!(g.len(), 2, "{g:?}");
    let role = &g[0];
    assert_eq!(role.user, "t42_r2");
    assert!(role.is_role());
    assert_eq!(role.privileges, ["DELETE"]);
    assert_eq!(
        role.revoke_sql(),
        "REVOKE DELETE ON `t42\\_d1\\_g3\\_s17`.* FROM 't42_r2'"
    );
    let user = &g[1];
    assert_eq!(user.privileges, ["INSERT", "UPDATE"]);
    assert_eq!(
        user.revoke_sql(),
        "REVOKE INSERT, UPDATE ON `t42_d1_g3_s17`.* FROM 't42_u7'@'%'"
    );
    assert_eq!(
        user.grant_sql(),
        "GRANT INSERT, UPDATE ON `t42_d1_g3_s17`.* TO 't42_u7'@'%'"
    );

    let all = grants_to_revoke(&rows, TenantId(42), &schemas(), FenceScope::All);
    let u7 = all.iter().find(|g| g.user == "t42_u7").unwrap();
    assert_eq!(u7.privileges, ["INSERT", "UPDATE", "SELECT", "EXECUTE"]);
    assert!(all.iter().any(|g| g.user == "t42_u8"));
}

fn session(id: u64, user: &str, db: Option<&str>, info: Option<&str>) -> SessionRow {
    SessionRow {
        id,
        user: user.into(),
        db: db.map(Into::into),
        info: info.map(Into::into),
    }
}

#[test]
fn kills_only_the_tenants_sessions_that_could_write() {
    let rows = vec![
        session(1, "t42_u7", Some("t42_d1_g3_s17"), None), // in the schema
        session(
            2,
            "t42_u7",
            None,
            Some("UPDATE `t42_d1_g3_s17`.x SET a = 1"),
        ), // names it
        session(3, "t42_u7", Some("t42_d1_g3_s18"), None), // open trx (below)
        session(4, "t42_u7", Some("t42_d1_g3_s18"), None), // idle elsewhere
        session(5, "t421_u1", Some("t42_d1_g3_s17"), None), // another tenant
        session(6, "sprig_ctl", Some("t42_d1_g3_s17"), None), // system account
        session(7, "root", None, Some("SELECT * FROM t42_d1_g3_s17.x")),
    ];
    let open: HashSet<u64> = [3, 5, 6, 7].into();
    let v = sessions_to_kill(&rows, TenantId(42), &schemas(), &open, None);
    assert_eq!(v, [1, 2, 3]);
    // With metadata locks known, an open transaction elsewhere is spared.
    let mdl: HashSet<u64> = HashSet::new();
    let v = sessions_to_kill(&rows, TenantId(42), &schemas(), &open, Some(&mdl));
    assert_eq!(v, [1, 2]);
    let mdl: HashSet<u64> = [4].into();
    let v = sessions_to_kill(&rows, TenantId(42), &schemas(), &open, Some(&mdl));
    assert_eq!(v, [1, 2, 4]);
}

#[test]
fn fenced_names() {
    let n = fenced_name("orders");
    assert!(
        n.starts_with("__sprig_f_") && n.len() == "__sprig_f_".len() + 16,
        "{n}"
    );
    assert!(is_fenced_name(&n));
    assert_eq!(n, fenced_name("orders"));
    assert_ne!(n, fenced_name("Orders"));
    assert!(!is_fenced_name("orders"));
    assert!(!is_fenced_name("__sprig_f_xyz"));
    let r = vec![
        RenamedTable {
            schema: "t42_d1_g3_s17".into(),
            from: "orders".into(),
            to: n.clone(),
        },
        RenamedTable {
            schema: "t42_d1_g3_s17".into(),
            from: "we`ird".into(),
            to: fenced_name("we`ird"),
        },
    ];
    let sql = rename_sql(&r, false);
    assert!(sql.starts_with(&format!(
        "RENAME TABLE `t42_d1_g3_s17`.`orders` TO `t42_d1_g3_s17`.`{n}`, "
    )));
    assert!(sql.contains("`we``ird`"), "{sql}");
    assert!(rename_sql(&r, true).starts_with(&format!(
        "RENAME TABLE `t42_d1_g3_s17`.`{n}` TO `t42_d1_g3_s17`.`orders`"
    )));
}

#[test]
fn schemas_must_be_the_tenants() {
    assert!(check_schemas(TenantId(42), &schemas()).is_ok());
    assert!(check_schemas(TenantId(42), &["t42_sprig".into()]).is_ok());
    assert!(check_schemas(TenantId(43), &schemas()).is_err());
    assert!(check_schemas(TenantId(42), &["mysql".into()]).is_err());
    assert!(check_schemas(TenantId(42), &[]).is_err());
}

#[tokio::test]
async fn a_passed_deadline_fails_before_touching_anything() {
    let ep = NodeEndpoint::from_url(NodeId::new("n1").unwrap(), "mysql://u:p@127.0.0.1:1").unwrap();
    let req = FenceRequest {
        tenant: TenantId(42),
        schemas: schemas(),
        tables: None,
        scope: FenceScope::Writes,
        method: FenceMethod::RevokeKill,
        deadline: Instant::now(),
    };
    let e = fence(&req, &ep, &[], &FenceOptions::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ClusterFenceFailed));
    assert!(e.retryable);
}

#[test]
fn receipts_round_trip() {
    let r = FenceReceipt {
        tenant: TenantId(42),
        method: FenceMethod::RevokeKill,
        scope: FenceScope::All,
        writer: NodeId::new("n1").unwrap(),
        revoked: vec![RevokedGrant {
            user: "t42_u7".into(),
            host: "%".into(),
            db: "t42_d1_g3_s17".into(),
            privileges: vec!["INSERT".into()],
        }],
        renamed: Vec::new(),
        killed: vec![(NodeId::new("n2").unwrap(), 812)],
        writer_pos: GtidPos("0-1-42".into()),
    };
    let json = serde_json::to_value(&r).unwrap();
    assert_eq!(json["writer_pos"], "0-1-42");
    assert_eq!(json["method"], "revoke_kill");
    assert_eq!(json["killed"][0][1], 812);
    let back: FenceReceipt = serde_json::from_value(json).unwrap();
    assert_eq!(back, r);
}

#[test]
fn options_follow_settings() {
    let o = FenceOptions::default();
    assert_eq!(o.kill_wait, Duration::from_millis(1000));
    assert_eq!(o.lock_wait_seconds, 1);
    assert!(!o.use_performance_schema);
}
