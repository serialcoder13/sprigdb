//! `docs/reference/settings.md` lists exactly the settings this binary knows.
//! The page is rendered from `sprigdb docs-settings`: the server's own
//! registry, so no key is missing or extra.
//! With `SPRIG_DOCS_REGEN=1` the test rewrites the page instead of checking it.

use std::path::PathBuf;
use std::process::Command;

use sprig_docs::reference::{SettingDoc, settings_page};

#[test]
fn settings_reference_is_current() {
    let out = Command::new(env!("CARGO_BIN_EXE_sprigdb"))
        .arg("docs-settings")
        .output()
        .expect("run sprigdb docs-settings");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let docs: Vec<SettingDoc> = serde_json::from_slice(&out.stdout).expect("settings JSON");
    // Keys from crates all over the workspace, including ones only the
    // controller's code paths reach.
    for k in [
        "auto_index.min_runs",
        "studio.history_max",
        "approvals.start_level",
    ] {
        assert!(docs.iter().any(|d| d.key == k), "{k} missing");
    }
    let page = settings_page(&docs);
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/reference/settings.md");
    if std::env::var_os("SPRIG_DOCS_REGEN").is_some() {
        std::fs::write(&path, &page).expect("write settings.md");
        return;
    }
    let have = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        have == page,
        "docs/reference/settings.md is stale: run `SPRIG_DOCS_REGEN=1 cargo test -p sprigdb --test docs_settings`"
    );
}
