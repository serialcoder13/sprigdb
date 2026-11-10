//! The embedded files (chapter 44, "Serving: `sprig-studio`").
//!
//! `build.rs` writes the table from `studio/dist`: each file's identity
//! bytes, its pre-compressed brotli and gzip forms when the build made them
//! (text files over 512 bytes), the first 8 bytes of its SHA-256 as the
//! `ETag`, and whether it is a hashed file under `assets/` (cached for a
//! year). The build version comes from `studio-manifest.json`.

/// One embedded file.
#[derive(Debug, Clone, Copy)]
pub struct Asset {
    /// The `Content-Type`.
    pub content_type: &'static str,
    /// The file as built.
    pub identity: &'static [u8],
    /// gzip, level 9, when smaller.
    pub gzip: Option<&'static [u8]>,
    /// brotli, quality 11, when smaller.
    pub brotli: Option<&'static [u8]>,
    /// The quoted `ETag`: a hash of the identity bytes, hex.
    pub etag: &'static str,
    /// A hashed file name under `assets/`: cached as immutable.
    pub immutable: bool,
}

mod generated {
    use super::Asset;
    include!(concat!(env!("OUT_DIR"), "/assets.rs"));
}

/// A set of files by path relative to `studio.path`.
#[derive(Debug, Clone, Copy)]
pub struct AssetStore {
    assets: &'static [(&'static str, Asset)],
    version: &'static str,
}

/// The files embedded at build time.
static EMBEDDED: AssetStore = AssetStore {
    assets: generated::ASSETS,
    version: generated::BUILD_VERSION,
};

impl AssetStore {
    /// The files this binary was built with; empty when it was built
    /// without `studio/dist`.
    #[must_use]
    pub fn embedded() -> &'static AssetStore {
        &EMBEDDED
    }

    /// A store over `assets` (tests).
    #[must_use]
    pub const fn new(assets: &'static [(&'static str, Asset)], version: &'static str) -> Self {
        Self { assets, version }
    }

    /// The file at `rel_path` (`index.html`, `assets/index-3f2a.js`).
    #[must_use]
    pub fn get(&self, rel_path: &str) -> Option<&Asset> {
        self.assets
            .binary_search_by(|(p, _)| (*p).cmp(rel_path))
            .ok()
            .and_then(|i| self.assets.get(i))
            .map(|(_, a)| a)
            .or_else(|| {
                // build.rs sorts by path; a store built by hand may not be.
                self.assets
                    .iter()
                    .find(|(p, _)| *p == rel_path)
                    .map(|(_, a)| a)
            })
    }

    /// The build version (`studio-manifest.json`), `none` without a build.
    #[must_use]
    pub fn build_version(&self) -> &'static str {
        self.version
    }

    /// Whether the binary holds a Studio build.
    #[must_use]
    pub fn is_built(&self) -> bool {
        self.get("index.html").is_some()
    }

    /// The number of files.
    #[must_use]
    pub fn len(&self) -> usize {
        self.assets.len()
    }

    /// No files.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.assets.is_empty()
    }

    /// Bytes of every form of every file (identity, gzip, brotli).
    #[must_use]
    pub fn total_bytes(&self) -> usize {
        self.assets
            .iter()
            .map(|(_, a)| {
                a.identity.len() + a.gzip.map_or(0, <[u8]>::len) + a.brotli.map_or(0, <[u8]>::len)
            })
            .sum()
    }
}
