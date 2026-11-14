//! The generated code in `src/gen/` matches the protos.
//!
//! Building the workspace needs no `protoc`: the prost and tonic output is
//! checked in. This test regenerates it into a scratch directory with
//! `protoc` and compares; with `SPRIG_API_REGEN=1` it writes the new files
//! into `src/gen/` instead. Without `protoc` on the path it skips (CI's
//! codegen job installs it).

use std::path::{Path, PathBuf};

const PROTOS: &[&str] = &[
    "sprig/v1/sql.proto",
    "sprig/v1/functions.proto",
    "sprig/v1/kv.proto",
    "sprig/v1/vectors.proto",
    "sprig/v1/analytics.proto",
    "sprig/v1/search.proto",
    "sprig/v1/logic.proto",
    "sprig/v1/changes.proto",
    "sprig/v1/sinks.proto",
    "sprig/admin/v1/admin.proto",
    "sprig/internal/v1/peer.proto",
    "sprig/internal/logic/v1/logic.proto",
    "sprig/internal/v1/queues.proto",
    "sprig/internal/v1/redis.proto",
    "sprig/internal/v1/search.proto",
    "grpc/health/v1/health.proto",
    "google/rpc/status.proto",
    "google/rpc/error_details.proto",
];

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn have_protoc() -> bool {
    let protoc = std::env::var_os("PROTOC").unwrap_or_else(|| "protoc".into());
    std::process::Command::new(protoc)
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn generate(out: &Path) {
    let root = crate_dir().join("proto");
    let protos: Vec<PathBuf> = PROTOS.iter().map(|p| root.join(p)).collect();
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .build_transport(false)
        .emit_rerun_if_changed(false)
        .out_dir(out)
        .file_descriptor_set_path(out.join("descriptor.bin"))
        .compile_protos(&protos, &[root])
        .expect("protoc and tonic-prost-build");
}

/// The descriptor set without protoc's bundled well-known types
/// (`google/protobuf/*.proto`), which come from protoc's own include
/// directory and change with its version (3.21 in CI, newer locally), while
/// every file of ours is the same from both.
fn own_descriptors(bytes: &[u8]) -> Option<prost_types::FileDescriptorSet> {
    let mut set = <prost_types::FileDescriptorSet as prost::Message>::decode(bytes).ok()?;
    set.file
        .retain(|f| !f.name().starts_with("google/protobuf/"));
    Some(set)
}

/// Whether the checked-in file `old` matches the regenerated `new`.
fn current(name: &str, old: Option<&[u8]>, new: &[u8]) -> bool {
    match old {
        Some(old) if name == "descriptor.bin" => {
            own_descriptors(old).is_some_and(|o| Some(o) == own_descriptors(new))
        }
        Some(old) => old == new,
        None => false,
    }
}

#[test]
fn generated_code_is_current() {
    if !have_protoc() {
        eprintln!("skipped: no protoc on the path");
        return;
    }
    let gen_dir = crate_dir().join("src/gen");
    let scratch = std::env::temp_dir().join(format!("sprig-api-gen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch dir");
    generate(&scratch);
    let regen = std::env::var_os("SPRIG_API_REGEN").is_some();
    let mut stale = Vec::new();
    let mut names: Vec<_> = std::fs::read_dir(&scratch)
        .expect("read scratch")
        .map(|e| e.expect("entry").file_name())
        .collect();
    names.sort();
    for name in names {
        let new = std::fs::read(scratch.join(&name)).expect("read generated");
        let path = gen_dir.join(&name);
        if regen {
            std::fs::write(&path, &new).expect("write src/gen");
        } else {
            let name = name.to_string_lossy().into_owned();
            if !current(&name, std::fs::read(&path).ok().as_deref(), &new) {
                stale.push(name);
            }
        }
    }
    let _ = std::fs::remove_dir_all(&scratch);
    assert!(
        stale.is_empty(),
        "src/gen is stale for {stale:?}: run SPRIG_API_REGEN=1 cargo test -p sprig-api --test codegen"
    );
}
