//! The docs pack: every chunk of a release's documentation, with its
//! keyword terms and embedding, in one reproducible file.
//!
//! # File format
//!
//! Little-endian. A string is a `u16` byte length and UTF-8 bytes.
//!
//! ```text
//! magic "SPRIGDOC" | format u16 = 1 | version str | model id str
//! | model sha256 [32] | chunk count u32
//! | chunks: one CBOR map each, ordered by chunk_id
//! | sha256 of everything before [32]
//! ```
//!
//! Each chunk map has, in this order: `chunk_id` (bytes, 16), `version`,
//! `page`, `title`, `heading_path` (text), `audience` (array of text),
//! `text`, `tokens` (unsigned), `terms` (array of text) and `embedding`
//! (bytes: [`DIM`] IEEE half-precision values, little-endian).
//!
//! The same pages, chunking and model give the same bytes: pages are read
//! in path order, chunking is fixed, embeddings are computed in `f32` and
//! rounded to `f16`, and chunks are written in `chunk_id` order.

use std::collections::HashMap;

use half::f16;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::chunk::{self, PATH_SEP};
use crate::page::Page;

/// The first bytes of every pack.
pub const MAGIC: &[u8; 8] = b"SPRIGDOC";
/// The format this crate writes and reads.
pub const FORMAT: u16 = 1;
/// Embedding dimensions.
pub const DIM: usize = 384;

/// One chunk of the documentation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    /// [`chunk_id`] of the version, page and heading path.
    #[serde(with = "bytes16")]
    pub chunk_id: [u8; 16],
    /// The release, such as `0.9.0`.
    pub version: String,
    /// The page's path under `docs/`.
    pub page: String,
    /// The page's title.
    pub title: String,
    /// The headings above the chunk, joined with `" > "`.
    pub heading_path: String,
    /// The page's audiences.
    pub audience: Vec<String>,
    /// The chunk's Markdown.
    pub text: String,
    /// The estimated token count ([`chunk::estimate_tokens`]).
    pub tokens: u16,
    /// The exact identifiers in the text ([`chunk::terms`]).
    pub terms: Vec<String>,
    /// The normalised embedding of [`Chunk::embedding_input`], [`DIM`]
    /// values (empty until embedded).
    #[serde(with = "f16_bytes")]
    pub embedding: Vec<f16>,
}

impl Chunk {
    /// The text the embedding is computed from: the heading path, a blank
    /// line, then the chunk.
    #[must_use]
    pub fn embedding_input(&self) -> String {
        format!("{}\n\n{}", self.heading_path, self.text)
    }

    /// The embedding as `f32` values.
    #[must_use]
    pub fn embedding_f32(&self) -> Vec<f32> {
        self.embedding.iter().map(|x| x.to_f32()).collect()
    }

    /// Stores `v` (computed in `f32`), rounded to `f16`.
    pub fn set_embedding(&mut self, v: &[f32]) {
        self.embedding = v.iter().map(|&x| f16::from_f32(x)).collect();
    }
}

/// The id of a chunk: the first 16 bytes of SHA-256 over the version, the
/// page and the heading path, separated by zero bytes. When several chunks
/// of a page share a heading path (a long section's pieces), the second and
/// later ones also hash their position: a zero byte and the decimal index
/// (`1`, `2`, ...).
#[must_use]
pub fn chunk_id(version: &str, page: &str, heading_path: &str, occurrence: usize) -> [u8; 16] {
    let mut h = Sha256::new();
    h.update(version.as_bytes());
    h.update([0]);
    h.update(page.as_bytes());
    h.update([0]);
    h.update(heading_path.as_bytes());
    if occurrence > 0 {
        h.update([0]);
        h.update(occurrence.to_string().as_bytes());
    }
    let d = h.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&d[..16]);
    id
}

/// Cuts every page into chunks with their ids, terms and token counts, in
/// page order then position (embeddings left empty).
#[must_use]
pub fn chunk_pages(version: &str, pages: &[Page]) -> Vec<Chunk> {
    let mut out = Vec::new();
    for page in pages {
        let mut seen: HashMap<String, usize> = HashMap::new();
        for c in chunk::chunk_page(page) {
            let heading_path = c.heading_path.join(PATH_SEP);
            let n = seen.entry(heading_path.clone()).or_insert(0);
            let id = chunk_id(version, &page.path, &heading_path, *n);
            *n += 1;
            out.push(Chunk {
                chunk_id: id,
                version: version.to_owned(),
                page: page.path.clone(),
                title: page.title.clone(),
                heading_path,
                audience: page.audience.clone(),
                terms: chunk::terms(&c.text),
                tokens: u16::try_from(c.tokens).unwrap_or(u16::MAX),
                text: c.text,
                embedding: Vec::new(),
            });
        }
    }
    out
}

/// A pack, read or about to be written.
#[derive(Debug, Clone, PartialEq)]
pub struct Pack {
    /// The release the docs describe.
    pub version: String,
    /// The embedding model's name.
    pub model_id: String,
    /// A digest of the embedding model's files.
    pub model_sha256: [u8; 32],
    /// The chunks, ordered by `chunk_id`.
    pub chunks: Vec<Chunk>,
}

/// Why a pack can't be written or read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PackError {
    /// The file is shorter than its header and trailer.
    #[error("the pack is truncated")]
    Truncated,
    /// The file doesn't start with [`MAGIC`].
    #[error("not a docs pack")]
    BadMagic,
    /// A format this build doesn't read.
    #[error("pack format {0} is not supported (this build reads format {FORMAT})")]
    Format(u16),
    /// The trailing SHA-256 doesn't match the contents: the file was
    /// changed or damaged.
    #[error("the pack's checksum doesn't match its contents")]
    Checksum,
    /// The contents don't decode, or break a rule of the format.
    #[error("invalid pack: {0}")]
    Invalid(String),
}

impl Pack {
    /// The pack's bytes. Chunks are sorted by `chunk_id` first.
    ///
    /// # Errors
    ///
    /// Two chunks share an id, a chunk's embedding doesn't have [`DIM`]
    /// values, or a string is too long for the header.
    pub fn to_bytes(&self) -> Result<Vec<u8>, PackError> {
        let mut chunks: Vec<&Chunk> = self.chunks.iter().collect();
        chunks.sort_by_key(|c| c.chunk_id);
        if let Some(w) = chunks.windows(2).find(|w| w[0].chunk_id == w[1].chunk_id) {
            return Err(PackError::Invalid(format!(
                "two chunks have the id {} ({} and {})",
                hex(&w[0].chunk_id),
                w[0].heading_path,
                w[1].heading_path
            )));
        }
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&FORMAT.to_le_bytes());
        put_str(&mut out, &self.version)?;
        put_str(&mut out, &self.model_id)?;
        out.extend_from_slice(&self.model_sha256);
        let count = u32::try_from(chunks.len())
            .map_err(|_| PackError::Invalid("too many chunks".into()))?;
        out.extend_from_slice(&count.to_le_bytes());
        for c in chunks {
            if c.embedding.len() != DIM {
                return Err(PackError::Invalid(format!(
                    "{} > {}: embedding has {} values, expected {DIM}",
                    c.page,
                    c.heading_path,
                    c.embedding.len()
                )));
            }
            ciborium::into_writer(c, &mut out).map_err(|e| PackError::Invalid(e.to_string()))?;
        }
        let digest = Sha256::digest(&out);
        out.extend_from_slice(&digest);
        Ok(out)
    }

    /// Reads and verifies a pack: the trailing checksum first, then the
    /// header, every chunk, their order and their embeddings' size.
    ///
    /// # Errors
    ///
    /// See [`PackError`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PackError> {
        if bytes.len() < MAGIC.len() + 2 + 32 {
            return Err(PackError::Truncated);
        }
        if &bytes[..8] != MAGIC {
            return Err(PackError::BadMagic);
        }
        let (body, trailer) = bytes.split_at(bytes.len() - 32);
        if Sha256::digest(body).as_slice() != trailer {
            return Err(PackError::Checksum);
        }
        let mut r = &body[8..];
        let format = u16::from_le_bytes(take::<2>(&mut r)?);
        if format != FORMAT {
            return Err(PackError::Format(format));
        }
        let version = get_str(&mut r)?;
        let model_id = get_str(&mut r)?;
        let model_sha256 = take::<32>(&mut r)?;
        let count = u32::from_le_bytes(take::<4>(&mut r)?);
        let mut chunks = Vec::with_capacity(usize::try_from(count).unwrap_or(0).min(1 << 16));
        for i in 0..count {
            let c: Chunk = ciborium::from_reader(&mut r)
                .map_err(|e| PackError::Invalid(format!("chunk {i}: {e}")))?;
            if c.embedding.len() != DIM {
                return Err(PackError::Invalid(format!(
                    "chunk {i}: embedding has {} values",
                    c.embedding.len()
                )));
            }
            if chunks
                .last()
                .is_some_and(|p: &Chunk| p.chunk_id >= c.chunk_id)
            {
                return Err(PackError::Invalid(format!(
                    "chunk {i} is out of chunk_id order"
                )));
            }
            chunks.push(c);
        }
        if !r.is_empty() {
            return Err(PackError::Invalid(format!(
                "{} bytes after the last chunk",
                r.len()
            )));
        }
        Ok(Self {
            version,
            model_id,
            model_sha256,
            chunks,
        })
    }
}

/// The SHA-256 a pack ends with: the hash of everything before it.
#[must_use]
pub fn pack_sha256(bytes: &[u8]) -> Option<[u8; 32]> {
    let t = bytes.get(bytes.len().checked_sub(32)?..)?;
    t.try_into().ok()
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

fn put_str(out: &mut Vec<u8>, s: &str) -> Result<(), PackError> {
    let n = u16::try_from(s.len())
        .map_err(|_| PackError::Invalid(format!("header string too long: {s:.40}")))?;
    out.extend_from_slice(&n.to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

fn take<const N: usize>(r: &mut &[u8]) -> Result<[u8; N], PackError> {
    if r.len() < N {
        return Err(PackError::Truncated);
    }
    let (a, b) = r.split_at(N);
    *r = b;
    a.try_into().map_err(|_| PackError::Truncated)
}

fn get_str(r: &mut &[u8]) -> Result<String, PackError> {
    let n = usize::from(u16::from_le_bytes(take::<2>(r)?));
    if r.len() < n {
        return Err(PackError::Truncated);
    }
    let (a, b) = r.split_at(n);
    *r = b;
    String::from_utf8(a.to_vec())
        .map_err(|_| PackError::Invalid("header string isn't UTF-8".into()))
}

/// A byte-string visitor shared by the two serde helpers below.
struct BytesVisitor;

impl serde::de::Visitor<'_> for BytesVisitor {
    type Value = Vec<u8>;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a byte string")
    }
    fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
        Ok(v.to_vec())
    }
    fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
        Ok(v)
    }
}

/// `[u8; 16]` as a CBOR byte string.
mod bytes16 {
    use serde::{Deserializer, Serializer, de::Error as _};

    pub(super) fn serialize<S: Serializer>(v: &[u8; 16], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 16], D::Error> {
        let v = d.deserialize_bytes(super::BytesVisitor)?;
        v.try_into()
            .map_err(|v: Vec<u8>| D::Error::invalid_length(v.len(), &"16 bytes"))
    }
}

/// `Vec<f16>` as a CBOR byte string of little-endian halves.
mod f16_bytes {
    use half::f16;
    use serde::{Deserializer, Serializer, de::Error as _};

    pub(super) fn serialize<S: Serializer>(v: &[f16], s: S) -> Result<S::Ok, S::Error> {
        let bytes: Vec<u8> = v.iter().flat_map(|x| x.to_le_bytes()).collect();
        s.serialize_bytes(&bytes)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<f16>, D::Error> {
        let v = d.deserialize_bytes(super::BytesVisitor)?;
        if v.len() % 2 != 0 {
            return Err(D::Error::invalid_length(
                v.len(),
                &"an even number of bytes",
            ));
        }
        Ok(v.chunks_exact(2)
            .map(|b| f16::from_le_bytes([b[0], b[1]]))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Pack {
        let page = crate::page::parse_page(
            "guides/x.md",
            "---\ntitle: X\naudience: [user]\n---\n\n# X\n\nIntro about 39134 and `auto_index.mode`.\n\n## A\n\nText of A.\n",
        )
        .expect("page");
        let mut chunks = chunk_pages("1.2.3", &[page]);
        for (i, c) in chunks.iter_mut().enumerate() {
            #[allow(clippy::cast_precision_loss)]
            let v: Vec<f32> = (0..DIM).map(|j| ((i + j) as f32).sin()).collect();
            c.set_embedding(&v);
        }
        Pack {
            version: "1.2.3".into(),
            model_id: "test/model".into(),
            model_sha256: [7; 32],
            chunks,
        }
    }

    #[test]
    fn round_trips() {
        let mut p = sample();
        let bytes = p.to_bytes().expect("write");
        assert_eq!(&bytes[..8], MAGIC);
        let back = Pack::from_bytes(&bytes).expect("read");
        p.chunks.sort_by_key(|c| c.chunk_id);
        assert_eq!(back, p);
        assert_eq!(back.to_bytes().expect("write"), bytes);
        assert_eq!(
            pack_sha256(&bytes).map(|h| h.to_vec()),
            Some(bytes[bytes.len() - 32..].to_vec())
        );
    }

    #[test]
    fn detects_tampering() {
        let bytes = sample().to_bytes().expect("write");
        // Any changed byte breaks the checksum.
        for i in [0, 9, 40, bytes.len() / 2, bytes.len() - 40, bytes.len() - 1] {
            let mut b = bytes.clone();
            b[i] ^= 0x20;
            let e = Pack::from_bytes(&b).expect_err("tampered");
            assert!(
                matches!(e, PackError::Checksum | PackError::BadMagic),
                "byte {i}: {e}"
            );
        }
        assert_eq!(Pack::from_bytes(&bytes[..20]), Err(PackError::Truncated));
        assert_eq!(
            Pack::from_bytes(&bytes[..bytes.len() - 1]),
            Err(PackError::Checksum)
        );
        // A consistent checksum over a bad body is still refused.
        let mut b = bytes[..bytes.len() - 32].to_vec();
        b[8] = 2;
        let d = Sha256::digest(&b);
        b.extend_from_slice(&d);
        assert_eq!(Pack::from_bytes(&b), Err(PackError::Format(2)));
    }

    #[test]
    fn refuses_bad_embeddings_and_duplicate_ids() {
        let mut p = sample();
        p.chunks[0].embedding.pop();
        assert!(p.to_bytes().is_err());
        let mut p = sample();
        let c = p.chunks[0].clone();
        p.chunks.push(c);
        assert!(p.to_bytes().is_err());
    }

    #[test]
    fn chunk_ids_are_deterministic() {
        let a = chunk_id("0.9.0", "guides/a.md", "A > B", 0);
        assert_eq!(a, chunk_id("0.9.0", "guides/a.md", "A > B", 0));
        assert_eq!(hex(&a).len(), 32);
        assert_ne!(a, chunk_id("0.9.1", "guides/a.md", "A > B", 0));
        assert_ne!(a, chunk_id("0.9.0", "guides/b.md", "A > B", 0));
        assert_ne!(a, chunk_id("0.9.0", "guides/a.md", "A > B", 1));
        // The separators keep fields apart: "a" + "b/c" is not "a/b" + "c".
        assert_ne!(chunk_id("v", "a", "b/c", 0), chunk_id("v", "a/b", "c", 0));
        let ids: Vec<_> = sample().chunks.iter().map(|c| c.chunk_id).collect();
        let again: Vec<_> = sample().chunks.iter().map(|c| c.chunk_id).collect();
        assert_eq!(ids, again);
        // Fixed, so a change to the hashing is noticed.
        assert_eq!(
            hex(&chunk_id("0.9.0", "guides/a.md", "A > B", 0)),
            hex(&{
                let d = Sha256::digest(b"0.9.0\0guides/a.md\0A > B");
                let mut x = [0u8; 16];
                x.copy_from_slice(&d[..16]);
                x
            })
        );
    }
}
