//! `sprig-embed`: SprigDB's local text embedding model.
//!
//! [`Embedder`] loads [BAAI/bge-small-en-v1.5] (MIT licence, 384 dimensions,
//! a 12-layer BERT) from a directory holding its `config.json`,
//! `tokenizer.json` and `model.safetensors`, and runs it on the CPU with
//! candle. Nothing leaves the process.
//!
//! It follows the model's own conventions:
//! - the vector is the final hidden state of the `[CLS]` token (CLS pooling),
//!   scaled to unit length, so cosine similarity is a dot product;
//! - inputs longer than 512 tokens are truncated;
//! - a search query is prefixed with [`QUERY_PREFIX`]; passages are not.
//!
//! `scripts/fetch-embed-model.sh` downloads the model, pinned and checked.
//!
//! [BAAI/bge-small-en-v1.5]: https://huggingface.co/BAAI/bge-small-en-v1.5

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use sha2::{Digest, Sha256};
use tokenizers::{Tokenizer, TruncationParams};

/// The number of dimensions of every vector.
pub const DIM: usize = 384;

/// The model's name on Hugging Face.
pub const MODEL_ID: &str = "BAAI/bge-small-en-v1.5";

/// The longest input, in model tokens (including `[CLS]` and `[SEP]`).
pub const MAX_TOKENS: usize = 512;

/// The instruction the model expects before a short search query, so the
/// query lands near the passages that answer it.
pub const QUERY_PREFIX: &str = "Represent this sentence for searching relevant passages: ";

/// The files [`Embedder::load`] reads from the model directory, in the
/// order [`Embedder::model_sha256`] digests them.
pub const MODEL_FILES: [&str; 3] = ["config.json", "tokenizer.json", "model.safetensors"];

/// Passages embedded in one forward pass.
const BATCH: usize = 8;

/// An embedding: unit length, [`DIM`] values.
pub type Vector = [f32; DIM];

/// Why the model couldn't be loaded or run.
#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    /// A model file is missing or unreadable.
    #[error("{path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The error.
        source: std::io::Error,
    },
    /// `config.json` isn't a BERT configuration this crate can run.
    #[error("config.json: {0}")]
    Config(String),
    /// The tokenizer failed to load or to encode.
    #[error("tokenizer: {0}")]
    Tokenizer(String),
    /// The model's weights or a forward pass failed.
    #[error("model: {0}")]
    Model(#[from] candle_core::Error),
}

/// The loaded model and tokenizer. Loading takes about a second and some
/// 130 MB; keep one and share it (`embed_*` take `&self` and are `Sync`).
pub struct Embedder {
    model: BertModel,
    tokenizer: Tokenizer,
    /// The same tokenizer without truncation, for [`Embedder::count_tokens`].
    counter: Tokenizer,
    sha256: [u8; 32],
}

impl std::fmt::Debug for Embedder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Embedder")
            .field("model", &MODEL_ID)
            .field("sha256", &hex(&self.sha256))
            .finish_non_exhaustive()
    }
}

impl Embedder {
    /// Loads the model from `dir`, which holds [`MODEL_FILES`].
    ///
    /// # Errors
    ///
    /// A file is missing or unreadable, or isn't the expected model.
    pub fn load(dir: &Path) -> Result<Self, EmbedError> {
        let read = |name: &str| {
            let path = dir.join(name);
            std::fs::read(&path).map_err(|source| EmbedError::Io { path, source })
        };
        let mut digest = Sha256::new();
        let mut file_digest = |name: &str, bytes: &[u8]| {
            digest.update(name.as_bytes());
            digest.update([0]);
            digest.update(Sha256::digest(bytes));
        };

        let config_bytes = read(MODEL_FILES[0])?;
        file_digest(MODEL_FILES[0], &config_bytes);
        let config: Config =
            serde_json::from_slice(&config_bytes).map_err(|e| EmbedError::Config(e.to_string()))?;
        if config.hidden_size != DIM {
            return Err(EmbedError::Config(format!(
                "hidden_size is {}, expected {DIM}",
                config.hidden_size
            )));
        }

        let tokenizer_bytes = read(MODEL_FILES[1])?;
        file_digest(MODEL_FILES[1], &tokenizer_bytes);
        let mut counter = Tokenizer::from_bytes(&tokenizer_bytes)
            .map_err(|e| EmbedError::Tokenizer(e.to_string()))?;
        counter
            .with_truncation(None)
            .map_err(|e| EmbedError::Tokenizer(e.to_string()))?;
        counter.with_padding(None);
        let mut tokenizer = counter.clone();
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: MAX_TOKENS,
                ..TruncationParams::default()
            }))
            .map_err(|e| EmbedError::Tokenizer(e.to_string()))?;

        let weights = read(MODEL_FILES[2])?;
        file_digest(MODEL_FILES[2], &weights);
        let device = Device::Cpu;
        let tensors: HashMap<String, Tensor> =
            candle_core::safetensors::load_buffer(&weights, &device)?;
        drop(weights);
        let vb = VarBuilder::from_tensors(tensors, DType::F32, &device);
        let model = BertModel::load(vb, &config)?;

        Ok(Self {
            model,
            tokenizer,
            counter,
            sha256: digest.finalize().into(),
        })
    }

    /// The model's name, [`MODEL_ID`].
    #[must_use]
    pub fn model_id(&self) -> &'static str {
        MODEL_ID
    }

    /// A digest of the loaded files: SHA-256 over, for each of
    /// [`MODEL_FILES`] in order, its name, a zero byte and its own SHA-256.
    /// Two embedders with the same digest give the same vectors on the same
    /// CPU architecture.
    #[must_use]
    pub fn model_sha256(&self) -> [u8; 32] {
        self.sha256
    }

    /// The number of model tokens `text` encodes to, `[CLS]` and `[SEP]`
    /// included, before truncation to [`MAX_TOKENS`].
    ///
    /// # Errors
    ///
    /// The tokenizer failed.
    pub fn count_tokens(&self, text: &str) -> Result<usize, EmbedError> {
        let enc = self
            .counter
            .encode(text, true)
            .map_err(|e| EmbedError::Tokenizer(e.to_string()))?;
        Ok(enc.get_ids().len())
    }

    /// Embeds documents to be searched, in order. Each text is embedded as
    /// it is, with no prefix.
    ///
    /// # Errors
    ///
    /// The tokenizer or a forward pass failed.
    pub fn embed_passages(&self, texts: &[&str]) -> Result<Vec<Vector>, EmbedError> {
        let mut out = Vec::with_capacity(texts.len());
        for batch in texts.chunks(BATCH) {
            out.extend(self.forward(batch)?);
        }
        Ok(out)
    }

    /// Embeds a search query, with [`QUERY_PREFIX`] in front.
    ///
    /// # Errors
    ///
    /// The tokenizer or the forward pass failed.
    pub fn embed_query(&self, query: &str) -> Result<Vector, EmbedError> {
        let text = format!("{QUERY_PREFIX}{query}");
        let mut v = self.forward(&[text.as_str()])?;
        v.pop()
            .ok_or_else(|| EmbedError::Tokenizer("no output".into()))
    }

    /// One padded batch through the model: CLS pooling, then L2
    /// normalisation.
    fn forward(&self, texts: &[&str]) -> Result<Vec<Vector>, EmbedError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| EmbedError::Tokenizer(e.to_string()))?;
        let len = encodings
            .iter()
            .map(|e| e.get_ids().len())
            .max()
            .unwrap_or(0)
            .max(1);
        let rows = encodings.len();
        let mut ids = vec![0u32; rows * len];
        let mut mask = vec![0u32; rows * len];
        for (r, e) in encodings.iter().enumerate() {
            for (c, (&id, &m)) in e.get_ids().iter().zip(e.get_attention_mask()).enumerate() {
                ids[r * len + c] = id;
                mask[r * len + c] = m;
            }
        }
        let device = &self.model.device;
        let ids = Tensor::from_vec(ids, (rows, len), device)?;
        let mask = Tensor::from_vec(mask, (rows, len), device)?;
        let types = ids.zeros_like()?;
        let hidden = self.model.forward(&ids, &types, Some(&mask))?;
        let cls = hidden.narrow(1, 0, 1)?.squeeze(1)?.to_dtype(DType::F32)?;
        let cls: Vec<Vec<f32>> = cls.to_vec2()?;
        Ok(cls.into_iter().map(|row| normalise(&row)).collect())
    }
}

/// `v` scaled to unit length (left as zeros if it is all zeros).
fn normalise(v: &[f32]) -> Vector {
    let mut out = [0f32; DIM];
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for (o, x) in out.iter_mut().zip(v) {
            *o = x / norm;
        }
    }
    out
}

/// The dot product of two vectors: their cosine similarity when both are
/// unit length, as every [`Embedder`] output is.
#[must_use]
pub fn cosine(a: &Vector, b: &Vector) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Lower-case hexadecimal.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalise_gives_unit_length() {
        let mut v = vec![0f32; DIM];
        v[0] = 3.0;
        v[1] = 4.0;
        let n = normalise(&v);
        assert!((n[0] - 0.6).abs() < 1e-6 && (n[1] - 0.8).abs() < 1e-6);
        assert!((cosine(&n, &n) - 1.0).abs() < 1e-6);
        assert!(normalise(&[0.0; DIM]).iter().all(|&x| x == 0.0));
    }

    #[test]
    fn hex_is_lower_case() {
        assert_eq!(hex(&[0, 0xab, 0x1f]), "00ab1f");
    }
}
