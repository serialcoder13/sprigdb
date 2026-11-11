use std::path::PathBuf;

use super::*;

fn dir() -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "sprig-secret-file-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn regular_files_pass_and_others_are_refused() {
    let d = dir();
    let f = d.join("token");
    std::fs::write(&f, "secret").unwrap();
    check(&f, "[controller] admin_token_file").unwrap();
    let e = check(&d, "[controller] admin_token_file").unwrap_err();
    assert!(e.to_string().contains("isn't a regular file"), "{e}");
    let e = check(&d.join("missing"), "[gateway] controller_token_file").unwrap_err();
    assert!(e.to_string().contains("controller_token_file"), "{e}");
    let _ = std::fs::remove_dir_all(&d);
}

#[cfg(unix)]
#[test]
fn modes() {
    use std::os::unix::fs::PermissionsExt as _;
    let d = dir();
    let f = d.join("token");
    std::fs::write(&f, "secret").unwrap();
    let set = |m: u32| std::fs::set_permissions(&f, std::fs::Permissions::from_mode(m)).unwrap();
    // Readable by others: the bundle's 0644 files.
    for ok in [0o600, 0o640, 0o644, 0o400, 0o664] {
        set(ok);
        check(&f, "x").unwrap_or_else(|e| panic!("{ok:o}: {e}"));
    }
    for bad in [0o666, 0o646, 0o602, 0o777] {
        set(bad);
        let e = check(&f, "[gateway] workload_key_file").unwrap_err();
        assert!(
            e.to_string().contains("writable by everyone"),
            "{bad:o}: {e}"
        );
        assert!(e.to_string().contains(&format!("{bad:04o}")), "{e}");
        assert!(e.to_string().contains("workload_key_file"), "{e}");
    }
    // A symbolic link to a good file is fine (Kubernetes secret mounts).
    set(0o644);
    let l = d.join("link");
    std::os::unix::fs::symlink(&f, &l).unwrap();
    check(&l, "x").unwrap();
    let _ = std::fs::remove_dir_all(&d);
}
