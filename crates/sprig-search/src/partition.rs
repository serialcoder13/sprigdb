//! One partition of a search index: a Tantivy index with one writer, its
//! reader, and the feed position committed with it (chapter 39,
//! "Indexing from the feed"; build step 4).
//!
//! Every change is delete-then-add by `_key`: Tantivy applies a delete
//! only to documents added before it, so replaying events after a crash is
//! harmless. Each commit stores a payload (the caller's feed position), so
//! the index and its position are always consistent.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use sprig_common::{Result, SprigError};
use tantivy::directory::MmapDirectory;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TantivyDocument, Term};

use crate::schema::{IndexSchema, Key, Record};

/// Tantivy's smallest writer budget.
pub const MIN_WRITER_BYTES: usize = 15_000_000;

fn terr(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(format!("search index {what}: {e}"))
}

/// One partition.
pub struct Partition {
    /// Its number in the index.
    pub no: u32,
    index: Index,
    writer: Mutex<IndexWriter<TantivyDocument>>,
    reader: IndexReader,
    key: tantivy::schema::Field,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("no", &self.no)
            .finish_non_exhaustive()
    }
}

impl Partition {
    /// Opens the partition in `dir` (created when missing), or in memory
    /// when `dir` is `None`. The writer has one thread and `writer_bytes`.
    ///
    /// # Errors
    /// Tantivy's (a directory holding another schema, I/O).
    pub fn open(
        no: u32,
        schema: &IndexSchema,
        dir: Option<&Path>,
        writer_bytes: usize,
    ) -> Result<Self> {
        let index = match dir {
            None => Index::create_in_ram(schema.schema.clone()),
            Some(d) => {
                std::fs::create_dir_all(d).map_err(|e| terr("directory", e))?;
                let md = MmapDirectory::open(d).map_err(|e| terr("directory", e))?;
                Index::open_or_create(md, schema.schema.clone()).map_err(|e| terr("open", e))?
            }
        };
        for a in schema.analyzers.iter() {
            index
                .tokenizers()
                .register(&a.registered, a.index_analyzer());
        }
        let writer = index
            .writer_with_num_threads(1, writer_bytes.max(MIN_WRITER_BYTES))
            .map_err(|e| terr("writer", e))?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| terr("reader", e))?;
        Ok(Self {
            no,
            index,
            writer: Mutex::new(writer),
            reader,
            key: schema.key,
        })
    }

    fn w(&self) -> MutexGuard<'_, IndexWriter<TantivyDocument>> {
        self.writer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The payload of the last commit (the caller's position).
    ///
    /// # Errors
    /// Tantivy's.
    pub fn payload(&self) -> Result<Option<String>> {
        Ok(self
            .index
            .load_metas()
            .map_err(|e| terr("metas", e))?
            .payload)
    }

    /// Deletes the key's document (pending until the next commit).
    pub fn delete(&self, key: &Key) {
        self.w()
            .delete_term(Term::from_field_bytes(self.key, &key.0));
    }

    /// Replaces the key's document with `doc` (pending until the next
    /// commit); `doc` `None` only deletes.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn upsert(&self, key: &Key, doc: Option<TantivyDocument>) -> Result<()> {
        let w = self.w();
        w.delete_term(Term::from_field_bytes(self.key, &key.0));
        if let Some(d) = doc {
            w.add_document(d).map_err(|e| terr("add", e))?;
        }
        Ok(())
    }

    /// Extracts and upserts `r`.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn index(&self, schema: &IndexSchema, r: &Record) -> Result<()> {
        self.upsert(r.key(), schema.extract(r))
    }

    /// Deletes every document (a `TRUNCATE`).
    ///
    /// # Errors
    /// Tantivy's.
    pub fn delete_all(&self) -> Result<()> {
        self.w()
            .delete_all_documents()
            .map_err(|e| terr("truncate", e))?;
        Ok(())
    }

    /// Commits what is pending with `payload`, and makes it searchable.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn commit(&self, payload: &str) -> Result<()> {
        {
            let mut w = self.w();
            let mut c = w.prepare_commit().map_err(|e| terr("prepare", e))?;
            c.set_payload(payload);
            c.commit().map_err(|e| terr("commit", e))?;
        }
        self.reader.reload().map_err(|e| terr("reload", e))
    }

    /// A searcher over the last commit.
    #[must_use]
    pub fn searcher(&self) -> Searcher {
        self.reader.searcher()
    }

    /// Live documents in the last commit.
    #[must_use]
    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}
