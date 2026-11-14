//! The query embedding: the bundled model, loaded on the first docs search
//! that wants it, run on a small thread pool of its own.
//!
//! - **Lazy.** Nothing is read until a search embeds a query; a gateway
//!   that never serves docs search never holds the model (about 300 MB).
//! - **Bounded.** Each query runs inside a rayon pool of
//!   `docs.embed_threads` threads, so the model's matrix products use at
//!   most that many cores, whatever the machine has.
//! - **Fails soft.** A model that doesn't load is tried again after
//!   [`RETRY`]; meanwhile searches are keyword-only.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_embed::Embedder;

/// How long a model that didn't load is left before it is tried again.
pub const RETRY: Duration = Duration::from_secs(60);

enum State {
    Unloaded,
    Loaded { dir: PathBuf, model: Arc<Embedder> },
    Failed { dir: PathBuf, at: Instant, why: String },
}

/// The model and its thread pool.
pub struct Model {
    state: tokio::sync::Mutex<State>,
    pool: Mutex<Option<(usize, Arc<rayon::ThreadPool>)>>,
}

impl std::fmt::Debug for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Model").finish_non_exhaustive()
    }
}

impl Default for Model {
    fn default() -> Self {
        Self {
            state: tokio::sync::Mutex::new(State::Unloaded),
            pool: Mutex::new(None),
        }
    }
}

impl Model {
    /// Whether a model is loaded (for metrics and tests).
    pub async fn loaded(&self) -> bool {
        matches!(&*self.state.lock().await, State::Loaded { .. })
    }

    /// The model in `dir`, loading it the first time (or after `dir`
    /// changed, or [`RETRY`] after a failure).
    ///
    /// # Errors
    /// Why the model isn't available.
    pub async fn get(&self, dir: &Path) -> Result<Arc<Embedder>, String> {
        let mut st = self.state.lock().await;
        match &*st {
            State::Loaded { dir: d, model } if d == dir => return Ok(Arc::clone(model)),
            State::Failed { dir: d, at, why } if d == dir && at.elapsed() < RETRY => {
                return Err(why.clone());
            }
            _ => {}
        }
        let owned = dir.to_path_buf();
        let started = Instant::now();
        let r = tokio::task::spawn_blocking(move || Embedder::load(&owned))
            .await
            .map_err(|e| format!("the model loader stopped: {e}"))
            .and_then(|r| r.map_err(|e| e.to_string()));
        match r {
            Ok(m) => {
                tracing::info!(dir = %dir.display(), model = m.model_id(),
                    ms = started.elapsed().as_millis(), "docs embedding model loaded");
                let m = Arc::new(m);
                *st = State::Loaded {
                    dir: dir.to_path_buf(),
                    model: Arc::clone(&m),
                };
                Ok(m)
            }
            Err(why) => {
                let why = format!("{}: {why}", dir.display());
                *st = State::Failed {
                    dir: dir.to_path_buf(),
                    at: Instant::now(),
                    why: why.clone(),
                };
                Err(why)
            }
        }
    }

    fn pool(&self, threads: usize) -> Result<Arc<rayon::ThreadPool>, String> {
        let threads = threads.max(1);
        let mut p = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((n, pool)) = &*p
            && *n == threads
        {
            return Ok(Arc::clone(pool));
        }
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .thread_name(|i| format!("docs-embed-{i}"))
                .build()
                .map_err(|e| format!("the embedding thread pool: {e}"))?,
        );
        *p = Some((threads, Arc::clone(&pool)));
        Ok(pool)
    }

    /// `query`'s embedding with the model in `dir`, on at most `threads`
    /// threads.
    ///
    /// # Errors
    /// Why the model isn't available or failed.
    pub async fn embed(&self, dir: &Path, threads: usize, query: &str) -> Result<Vec<f32>, String> {
        let model = self.get(dir).await?;
        let pool = self.pool(threads)?;
        let q = query.to_owned();
        tokio::task::spawn_blocking(move || pool.install(|| model.embed_query(&q)))
            .await
            .map_err(|e| format!("the embedding task stopped: {e}"))?
            .map(|v| v.to_vec())
            .map_err(|e| e.to_string())
    }
}
