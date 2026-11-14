//! The docs pack as rows: the system schema `__sprig_docs` on every
//! cluster, its tables, the row form of a [`Chunk`], and the digest the
//! controller checks the loaded rows against.
//!
//! ```text
//! __sprig_docs.chunks  one row per chunk and release (PRIMARY KEY (version, chunk_id)),
//!                      FULLTEXT on text; on text and heading_path; and on
//!                      text, heading_path and terms; a cosine VECTOR index on embedding
//! __sprig_docs.packs   one row per release: its pack's SHA-256, chunk count,
//!                      load time and state (loading → ready → retired)
//! ```
//!
//! `terms` holds the chunk's exact identifiers (error codes and names,
//! SQLSTATEs, setting keys), separated by spaces, so a full-text search on
//! an identifier finds the chunks that name it even when the prose
//! doesn't spell it the same way.
//!
//! **Digest.** [`rows_digest`] is SHA-256 over every column of every row,
//! in `chunk_id` order, each field length-prefixed. The loader computes it
//! from the pack and from the rows read back; any edited, missing or extra
//! row changes it. Embeddings are stored as `f32` (the pack's `f16` values
//! widened, which is exact), so the bytes read back are the bytes written.

use sha2::{Digest, Sha256};

use crate::pack::{Chunk, DIM};

/// The schema, reserved by the `__sprig_` prefix.
pub const SCHEMA: &str = "__sprig_docs";
/// The chunk table.
pub const CHUNKS: &str = "chunks";
/// The table of loaded releases.
pub const PACKS: &str = "packs";
/// The logical database tenants read the docs through.
pub const VIRTUAL_DATABASE: &str = "sprig_docs";
/// The members of the `audience` set, in the set's order.
pub const AUDIENCES: [&str; 3] = ["user", "dba", "operator"];

/// The longest `version`, `page`, `title` and `heading_path` the table
/// takes, in characters.
pub const MAX_VERSION: usize = 32;
/// See [`MAX_VERSION`].
pub const MAX_PAGE: usize = 255;
/// See [`MAX_VERSION`].
pub const MAX_TITLE: usize = 255;
/// See [`MAX_VERSION`].
pub const MAX_HEADING_PATH: usize = 1024;

/// `` `__sprig_docs`.`<table>` ``.
#[must_use]
pub fn qualified(table: &str) -> String {
    format!("`{SCHEMA}`.`{table}`")
}

/// The statements that create the schema and its tables; each is
/// idempotent. Text columns compare case-insensitively, so full-text
/// search ignores case.
#[must_use]
pub fn ddl() -> [String; 3] {
    [
        format!(
            "CREATE DATABASE IF NOT EXISTS `{SCHEMA}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             version VARCHAR({MAX_VERSION}) NOT NULL, \
             chunk_id BINARY(16) NOT NULL, \
             page VARCHAR({MAX_PAGE}) NOT NULL, \
             title VARCHAR({MAX_TITLE}) NOT NULL, \
             heading_path VARCHAR({MAX_HEADING_PATH}) NOT NULL, \
             audience SET('user','dba','operator') NOT NULL, \
             text MEDIUMTEXT NOT NULL, \
             tokens SMALLINT UNSIGNED NOT NULL, \
             terms TEXT NOT NULL, \
             embedding VECTOR({DIM}) NOT NULL, \
             PRIMARY KEY (version, chunk_id), \
             FULLTEXT KEY ft_text (text), \
             FULLTEXT KEY ft_text_heading (text, heading_path), \
             FULLTEXT KEY ft_search (text, heading_path, terms), \
             VECTOR INDEX v_emb (embedding) DISTANCE=cosine\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
            qualified(CHUNKS)
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             version VARCHAR({MAX_VERSION}) NOT NULL PRIMARY KEY, \
             sha256 BINARY(32) NOT NULL, \
             chunks INT UNSIGNED NOT NULL, \
             loaded_at DATETIME(3) NOT NULL, \
             state ENUM('loading','ready','retired') NOT NULL\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
            qualified(PACKS)
        ),
    ]
}

/// The columns of `chunks`, in the order [`Row`] binds them.
pub const COLUMNS: &str =
    "version, chunk_id, page, title, heading_path, audience, text, tokens, terms, embedding";

/// A multi-row `INSERT` of `rows` rows, ten placeholders each. The
/// binary columns are cast, so a parameter sent as a string isn't read in
/// the connection's character set.
#[must_use]
pub fn insert_sql(rows: usize) -> String {
    let one = "(?, CAST(? AS BINARY), ?, ?, ?, ?, ?, ?, ?, CAST(? AS BINARY))";
    let mut s = format!("INSERT INTO {} ({COLUMNS}) VALUES ", qualified(CHUNKS));
    for i in 0..rows {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(one);
    }
    s
}

/// One row of `chunks`, as written and as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The chunk's id.
    pub chunk_id: [u8; 16],
    /// The release.
    pub version: String,
    /// The page's path under `docs/`.
    pub page: String,
    /// The page's title.
    pub title: String,
    /// The headings above the chunk.
    pub heading_path: String,
    /// The set's text: members of [`AUDIENCES`] in order, comma-separated.
    pub audience: String,
    /// The chunk's Markdown.
    pub text: String,
    /// The token estimate.
    pub tokens: u16,
    /// The identifiers, separated by spaces.
    pub terms: String,
    /// [`DIM`] little-endian `f32` values.
    pub embedding: Vec<u8>,
}

/// Why a chunk can't be a row.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{page} > {heading_path}: {why}")]
pub struct RowError {
    /// The chunk's page.
    pub page: String,
    /// The chunk's heading path.
    pub heading_path: String,
    /// What doesn't fit.
    pub why: String,
}

/// The `audience` set's text for `audience`: the known members, in the
/// set's order.
#[must_use]
pub fn audience_set(audience: &[String]) -> String {
    AUDIENCES
        .iter()
        .filter(|a| audience.iter().any(|x| x.eq_ignore_ascii_case(a)))
        .copied()
        .collect::<Vec<_>>()
        .join(",")
}

/// `v` as little-endian `f32` bytes, the form a `VECTOR` column stores.
#[must_use]
pub fn vector_bytes(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// `VEC_FromText`'s argument for `v`: `[x,y,...]`.
#[must_use]
pub fn vector_text(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 12 + 2);
    s.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        // `{:?}` prints the shortest text that reads back as the same f32.
        s.push_str(&format!("{x:?}"));
    }
    s.push(']');
    s
}

impl Row {
    /// The row of `c`.
    ///
    /// # Errors
    /// A field is longer than its column, or the embedding doesn't have
    /// [`DIM`] values.
    pub fn of(c: &Chunk) -> Result<Self, RowError> {
        let err = |why: String| RowError {
            page: c.page.clone(),
            heading_path: c.heading_path.clone(),
            why,
        };
        for (name, value, max) in [
            ("version", &c.version, MAX_VERSION),
            ("page", &c.page, MAX_PAGE),
            ("title", &c.title, MAX_TITLE),
            ("heading_path", &c.heading_path, MAX_HEADING_PATH),
        ] {
            let n = value.chars().count();
            if n > max {
                return Err(err(format!("{name} has {n} characters, the column {max}")));
            }
        }
        if c.embedding.len() != DIM {
            return Err(err(format!(
                "the embedding has {} values, expected {DIM}",
                c.embedding.len()
            )));
        }
        Ok(Self {
            chunk_id: c.chunk_id,
            version: c.version.clone(),
            page: c.page.clone(),
            title: c.title.clone(),
            heading_path: c.heading_path.clone(),
            audience: audience_set(&c.audience),
            text: c.text.clone(),
            tokens: c.tokens,
            terms: c.terms.join(" "),
            embedding: vector_bytes(&c.embedding_f32()),
        })
    }
}

/// The rows of a pack's chunks, in `chunk_id` order.
///
/// # Errors
/// See [`Row::of`].
pub fn rows_of(chunks: &[Chunk]) -> Result<Vec<Row>, RowError> {
    let mut rows = chunks.iter().map(Row::of).collect::<Result<Vec<_>, _>>()?;
    rows.sort_by_key(|r| r.chunk_id);
    Ok(rows)
}

/// SHA-256 over `rows` (which must be in `chunk_id` order): per row, every
/// column, each as a `u32` little-endian length and its bytes.
#[must_use]
pub fn rows_digest<'a>(rows: impl IntoIterator<Item = &'a Row>) -> [u8; 32] {
    let mut h = Sha256::new();
    let mut field = |b: &[u8]| {
        let n = u32::try_from(b.len()).unwrap_or(u32::MAX);
        h.update(n.to_le_bytes());
        h.update(b);
    };
    for r in rows {
        field(&r.chunk_id);
        field(r.version.as_bytes());
        field(r.page.as_bytes());
        field(r.title.as_bytes());
        field(r.heading_path.as_bytes());
        field(r.audience.as_bytes());
        field(r.text.as_bytes());
        field(&r.tokens.to_le_bytes());
        field(r.terms.as_bytes());
        field(&r.embedding);
    }
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pack::chunk_pages;

    fn chunks() -> Vec<Chunk> {
        let page = crate::page::parse_page(
            "guides/x.md",
            "---\ntitle: X\naudience: [dba, user]\n---\n\n# X\n\nIntro about 39134 and `auto_index.mode`.\n\n## A\n\nText of A.\n",
        )
        .expect("page");
        let mut c = chunk_pages("1.2.3", &[page]);
        for (i, ch) in c.iter_mut().enumerate() {
            #[allow(clippy::cast_precision_loss)]
            let v: Vec<f32> = (0..DIM).map(|j| ((i * 7 + j) as f32).cos()).collect();
            ch.set_embedding(&v);
        }
        c
    }

    #[test]
    fn rows_keep_every_field_and_order_by_id() {
        let c = chunks();
        let rows = rows_of(&c).expect("rows");
        assert_eq!(rows.len(), c.len());
        assert!(rows.windows(2).all(|w| w[0].chunk_id < w[1].chunk_id));
        let r = rows.iter().find(|r| r.text.contains("39134")).expect("row");
        // Set members in the set's order, whatever the front matter's.
        assert_eq!(r.audience, "user,dba");
        assert!(r.terms.split(' ').any(|t| t == "39134"), "{}", r.terms);
        assert!(r.terms.split(' ').any(|t| t == "auto_index.mode"));
        assert_eq!(r.embedding.len(), DIM * 4);
        // f16 widened to f32 is exact.
        let src = c.iter().find(|x| x.chunk_id == r.chunk_id).expect("chunk");
        let first = f32::from_le_bytes(r.embedding[..4].try_into().expect("4"));
        assert_eq!(first, src.embedding[0].to_f32());
    }

    #[test]
    fn the_digest_sees_any_change() {
        let rows = rows_of(&chunks()).expect("rows");
        let d = rows_digest(&rows);
        assert_eq!(d, rows_digest(&rows.clone()));
        let mut edits: Vec<Box<dyn Fn(&mut Vec<Row>)>> = vec![
            Box::new(|r| r[0].text.push('!')),
            Box::new(|r| r[0].heading_path.push('!')),
            Box::new(|r| r[0].audience = "operator".into()),
            Box::new(|r| r[0].tokens += 1),
            Box::new(|r| r[0].terms.clear()),
            Box::new(|r| r[0].embedding[5] ^= 1),
            Box::new(|r| r[0].title.push(' ')),
            Box::new(|r| {
                r.pop();
            }),
            Box::new(|r| {
                let x = r[0].clone();
                r.push(x);
            }),
        ];
        for (i, e) in edits.iter_mut().enumerate() {
            let mut r = rows.clone();
            e(&mut r);
            assert_ne!(rows_digest(&r), d, "edit {i}");
        }
        // Fields can't run into each other.
        let mut a = rows.clone();
        a[0].title = "ab".into();
        a[0].heading_path = "c".into();
        let mut b = rows.clone();
        b[0].title = "a".into();
        b[0].heading_path = "bc".into();
        assert_ne!(rows_digest(&a), rows_digest(&b));
    }

    #[test]
    fn rows_refuse_what_the_columns_cant_hold() {
        let mut c = chunks();
        c[0].heading_path = "h".repeat(MAX_HEADING_PATH + 1);
        assert!(Row::of(&c[0]).is_err());
        let mut c = chunks();
        c[0].embedding.pop();
        assert!(Row::of(&c[0]).unwrap_err().why.contains("embedding"));
    }

    #[test]
    fn statements() {
        let d = ddl();
        assert!(d[1].contains("VECTOR(384)"), "{}", d[1]);
        assert!(d[1].contains("FULLTEXT KEY ft_text (text)"));
        assert!(d[1].contains("DISTANCE=cosine"));
        assert!(d[2].contains("ENUM('loading','ready','retired')"));
        let s = insert_sql(2);
        assert_eq!(s.matches('?').count(), 20);
        assert!(s.starts_with("INSERT INTO `__sprig_docs`.`chunks` (version, chunk_id"));
        assert_eq!(vector_text(&[1.0, -0.5]), "[1.0,-0.5]");
        assert_eq!(vector_bytes(&[1.0]), vec![0, 0, 0x80, 0x3f]);
    }
}
