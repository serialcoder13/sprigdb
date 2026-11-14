//! The `docs.*` settings: loading the docs pack into every cluster (the
//! controller) and searching it (the gateway). Every key is an operator's,
//! at dc or global scope, and applies live.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// The chapter that owns the keys.
const OWNER: u8 = 55;

/// `docs.enabled`.
pub static ENABLED: Setting<bool> = Setting {
    key: "docs.enabled",
    default: true,
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "Load the docs pack into every cluster and serve docs search (the REST call and the sprig_docs database)",
};
sprig_settings::register!(ENABLED);

/// `docs.keep_versions`.
pub static KEEP_VERSIONS: Setting<u64> = Setting {
    key: "docs.keep_versions",
    default: 2,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 20),
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "Releases of the docs kept on each cluster, the running one included (2: current and previous, for a rollback)",
};
sprig_settings::register!(KEEP_VERSIONS);

/// `docs.pack_path`.
pub static PACK_PATH: Setting<Cow<'static, str>> = Setting {
    key: "docs.pack_path",
    default: Cow::Borrowed(""),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "The docs pack the controller loads; empty: sprig-docs-<version>.pack next to the sprigdb binary",
};
sprig_settings::register!(PACK_PATH);

/// `docs.search_limit`.
pub static SEARCH_LIMIT: Setting<u64> = Setting {
    key: "docs.search_limit",
    default: 8,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 50),
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "Hits a docs search returns when the request gives no limit (at most 50 either way)",
};
sprig_settings::register!(SEARCH_LIMIT);

/// `docs.embed_queries`.
pub static EMBED_QUERIES: Setting<bool> = Setting {
    key: "docs.embed_queries",
    default: true,
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "Embed docs search queries with the bundled model for hybrid search; off: keyword search only (the model is never loaded)",
};
sprig_settings::register!(EMBED_QUERIES);

/// `docs.embed_threads`.
pub static EMBED_THREADS: Setting<u64> = Setting {
    key: "docs.embed_threads",
    default: 2,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 64),
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "CPU threads the gateway's embedding model may use for one docs query",
};
sprig_settings::register!(EMBED_THREADS);

/// `docs.model_dir`.
pub static MODEL_DIR: Setting<Cow<'static, str>> = Setting {
    key: "docs.model_dir",
    default: Cow::Borrowed(""),
    scopes: Scopes::PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "The embedding model's directory (config.json, tokenizer.json, model.safetensors); empty: models/bge-small-en-v1.5 next to the sprigdb binary",
};
sprig_settings::register!(MODEL_DIR);

/// The default pack file name for `version`: `sprig-docs-<version>.pack`.
#[must_use]
pub fn pack_file_name(version: &str) -> String {
    format!("sprig-docs-{version}.pack")
}

/// The model directory's default name, under the binary's directory.
pub const DEFAULT_MODEL_DIR: &str = "models/bge-small-en-v1.5";

/// `name` in the running binary's directory, when it can be found.
#[must_use]
pub fn next_to_binary(name: &str) -> Option<std::path::PathBuf> {
    let exe = std::env::current_exe().ok()?;
    Some(exe.parent()?.join(name))
}
