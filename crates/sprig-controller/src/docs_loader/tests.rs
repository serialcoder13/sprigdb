#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use sprig_docs::pack::{DIM, chunk_pages};

use super::*;

/// One cluster's `__sprig_docs`, in memory.
#[derive(Debug, Default)]
struct State {
    /// `version -> (sha256, chunks, state, load order)`.
    packs: BTreeMap<String, (Vec<u8>, u32, String, u64)>,
    /// `(version, chunk_id) -> row`.
    rows: BTreeMap<(String, [u8; 16]), Row>,
    inserts: Vec<usize>,
    schema: usize,
    readers: Vec<String>,
    clock: u64,
    /// Drops this many rows of each insert (a server that loses writes).
    lose: usize,
}

#[derive(Clone, Default)]
struct Fake(Arc<Mutex<State>>);

impl Fake {
    fn st(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl DocsDb for Fake {
    async fn ensure_schema(&mut self) -> Result<()> {
        self.st().schema += 1;
        Ok(())
    }
    async fn ensure_reader(&mut self, password: &str) -> Result<()> {
        self.st().readers.push(password.to_owned());
        Ok(())
    }
    async fn pack_state(&mut self, version: &str) -> Result<Option<PackState>> {
        Ok(self
            .st()
            .packs
            .get(version)
            .map(|(sha256, chunks, state, _)| PackState {
                sha256: sha256.clone(),
                chunks: *chunks,
                state: state.clone(),
            }))
    }
    async fn read_rows(&mut self, version: &str) -> Result<Vec<Row>> {
        Ok(self
            .st()
            .rows
            .iter()
            .filter(|((v, _), _)| v == version)
            .map(|(_, r)| r.clone())
            .collect())
    }
    async fn begin_load(&mut self, version: &str, sha256: &[u8; 32], chunks: u32) -> Result<()> {
        let mut st = self.st();
        st.clock += 1;
        let at = st.clock;
        st.packs.insert(
            version.to_owned(),
            (sha256.to_vec(), chunks, "loading".into(), at),
        );
        st.rows.retain(|(v, _), _| v != version);
        Ok(())
    }
    async fn insert(&mut self, rows: &[Row]) -> Result<()> {
        let mut st = self.st();
        st.inserts.push(rows.len());
        let lose = st.lose;
        for r in rows.iter().skip(lose) {
            st.rows
                .insert((r.version.clone(), r.chunk_id), r.clone());
        }
        Ok(())
    }
    async fn mark_ready(&mut self, version: &str) -> Result<()> {
        let mut st = self.st();
        st.clock += 1;
        let at = st.clock;
        if let Some(p) = st.packs.get_mut(version) {
            p.2 = "ready".into();
            p.3 = at;
        }
        Ok(())
    }
    async fn versions(&mut self) -> Result<Vec<(String, String)>> {
        let mut v: Vec<(u64, String, String)> = self
            .st()
            .packs
            .iter()
            .map(|(k, (_, _, s, at))| (*at, k.clone(), s.clone()))
            .collect();
        v.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
        Ok(v.into_iter().map(|(_, k, s)| (k, s)).collect())
    }
    async fn retire(&mut self, version: &str) -> Result<()> {
        let mut st = self.st();
        st.rows.retain(|(v, _), _| v != version);
        if let Some(p) = st.packs.get_mut(version) {
            p.2 = "retired".into();
        }
        Ok(())
    }
}

/// A pack of `pages` pages for `version`, its bytes and the loader's form.
fn pack(version: &str, pages: usize) -> (Vec<u8>, LoadedPack) {
    let pages: Vec<_> = (0..pages)
        .map(|i| {
            sprig_docs::page::parse_page(
                &format!("guides/p{i}.md"),
                &format!(
                    "---\ntitle: Page {i}\naudience: [user]\n---\n\n# Page {i}\n\nAbout 3913{} and \
                     `auto_index.mode`.\n\n## Part\n\nMore text {i}.\n",
                    i % 10
                ),
            )
            .unwrap()
        })
        .collect();
    let mut chunks = chunk_pages(version, &pages);
    for (i, c) in chunks.iter_mut().enumerate() {
        #[allow(clippy::cast_precision_loss)]
        let v: Vec<f32> = (0..DIM).map(|j| ((i * 31 + j) as f32).sin()).collect();
        c.set_embedding(&v);
    }
    let bytes = Pack {
        version: version.into(),
        model_id: "test/model".into(),
        model_sha256: [1; 32],
        chunks,
    }
    .to_bytes()
    .unwrap();
    let loaded = LoadedPack::from_bytes(&bytes).unwrap();
    (bytes, loaded)
}

fn row(i: usize, text_len: usize) -> Row {
    Row {
        chunk_id: [u8::try_from(i % 256).unwrap(); 16],
        version: "v".into(),
        page: "p".into(),
        title: "t".into(),
        heading_path: "h".into(),
        audience: "user".into(),
        text: "x".repeat(text_len),
        tokens: 1,
        terms: String::new(),
        embedding: vec![0; 4],
    }
}

#[test]
fn batches_cap_rows_and_bytes() {
    let rows: Vec<Row> = (0..1203).map(|i| row(i, 10)).collect();
    let b = batches(&rows, 500, usize::MAX);
    assert_eq!(b, vec![0..500, 500..1000, 1000..1203]);
    // Bytes: each row is about 1 KB, so 3 fit in 3.5 KB.
    let big: Vec<Row> = (0..7).map(|i| row(i, 1000)).collect();
    let b = batches(&big, 500, 3500);
    assert_eq!(b, vec![0..3, 3..6, 6..7]);
    // A row bigger than the limit goes alone.
    let huge = vec![row(0, 10), row(1, 10_000), row(2, 10)];
    assert_eq!(batches(&huge, 500, 1000), vec![0..1, 1..2, 2..3]);
    assert!(batches(&[], 500, 1000).is_empty());
    // Every row is in exactly one batch, in order.
    let n: usize = batches(&rows, 7, 50_000).iter().map(ExactSizeIterator::len).sum();
    assert_eq!(n, rows.len());
}

#[tokio::test]
async fn loads_then_trusts_then_verifies_again_at_start() {
    let (_, p) = pack("1.0.0", 30);
    let fake = Fake::default();
    let mut db = fake.clone();
    let r = sync_cluster(&mut db, &p, 2, false).await.unwrap();
    assert!(r.loaded && r.verified && !r.tampered);
    assert_eq!(fake.st().rows.len(), p.rows.len());
    assert_eq!(fake.st().packs["1.0.0"].2, "ready");
    assert_eq!(fake.st().packs["1.0.0"].0, p.sha256.to_vec());
    // A verified copy, trusted: nothing read, nothing written.
    let r = sync_cluster(&mut db, &p, 2, true).await.unwrap();
    assert!(!r.loaded && !r.verified);
    assert_eq!(fake.st().inserts.len(), 1);
    // A new process verifies the rows once, and finds them good.
    let r = sync_cluster(&mut db, &p, 2, false).await.unwrap();
    assert!(r.verified && !r.loaded && !r.tampered);
}

#[tokio::test]
async fn edited_missing_or_extra_rows_are_reloaded() {
    let (_, p) = pack("1.0.0", 12);
    let edits: Vec<Box<dyn Fn(&mut State)>> = vec![
        Box::new(|st| {
            let r = st.rows.values_mut().next().unwrap();
            r.text = "edited".into();
        }),
        Box::new(|st| {
            let r = st.rows.values_mut().nth(2).unwrap();
            r.embedding[7] ^= 0x40;
        }),
        Box::new(|st| {
            let k = st.rows.keys().next().unwrap().clone();
            st.rows.remove(&k);
        }),
        Box::new(|st| {
            let mut r = st.rows.values().next().unwrap().clone();
            r.chunk_id = [0xff; 16];
            st.rows.insert((r.version.clone(), r.chunk_id), r);
        }),
        Box::new(|st| {
            let r = st.rows.values_mut().last().unwrap();
            r.audience = "operator".into();
        }),
    ];
    for (i, edit) in edits.iter().enumerate() {
        let fake = Fake::default();
        let mut db = fake.clone();
        sync_cluster(&mut db, &p, 2, false).await.unwrap();
        edit(&mut fake.st());
        let r = sync_cluster(&mut db, &p, 2, false).await.unwrap();
        assert!(r.tampered && r.loaded, "edit {i}: {r:?}");
        let rows: Vec<Row> = fake.st().rows.values().cloned().collect();
        assert_eq!(table::rows_digest(&rows), p.digest, "edit {i}");
    }
}

#[tokio::test]
async fn a_different_or_unfinished_pack_is_loaded_again() {
    let (_, p) = pack("1.0.0", 5);
    let fake = Fake::default();
    let mut db = fake.clone();
    sync_cluster(&mut db, &p, 2, false).await.unwrap();
    // Another build's pack of the same version (its sha differs).
    fake.st().packs.get_mut("1.0.0").unwrap().0 = vec![9; 32];
    let r = sync_cluster(&mut db, &p, 2, true).await.unwrap();
    assert!(r.loaded && !r.tampered);
    // A load that stopped half way.
    fake.st().packs.get_mut("1.0.0").unwrap().2 = "loading".into();
    let r = sync_cluster(&mut db, &p, 2, true).await.unwrap();
    assert!(r.loaded);
    assert_eq!(fake.st().packs["1.0.0"].2, "ready");
}

#[tokio::test]
async fn rows_that_dont_read_back_fail_the_load() {
    let (_, p) = pack("1.0.0", 5);
    let fake = Fake::default();
    fake.st().lose = 1;
    let mut db = fake.clone();
    let e = sync_cluster(&mut db, &p, 2, false).await.unwrap_err();
    assert!(e.message.contains("don't match"), "{e}");
    // Never marked ready, so the next pass loads again.
    assert_eq!(fake.st().packs["1.0.0"].2, "loading");
    fake.st().lose = 0;
    assert!(sync_cluster(&mut db, &p, 2, false).await.unwrap().loaded);
}

#[tokio::test]
async fn keeps_the_newest_versions_and_retires_the_rest() {
    let fake = Fake::default();
    let mut db = fake.clone();
    for v in ["1.0.0", "1.1.0", "1.2.0"] {
        let (_, p) = pack(v, 3);
        sync_cluster(&mut db, &p, 2, false).await.unwrap();
    }
    {
        let st = fake.st();
        assert_eq!(st.packs["1.0.0"].2, "retired");
        assert_eq!(st.packs["1.1.0"].2, "ready");
        assert_eq!(st.packs["1.2.0"].2, "ready");
        assert!(st.rows.keys().all(|(v, _)| v != "1.0.0"));
    }
    // A rollback to 1.1.0 keeps 1.1.0 (current) and 1.2.0 (the newest other).
    let (_, p) = pack("1.1.0", 3);
    let r = sync_cluster(&mut db, &p, 2, false).await.unwrap();
    assert!(r.retired.is_empty() && !r.loaded);
    // keep 1: only the running version.
    let r = sync_cluster(&mut db, &p, 1, true).await.unwrap();
    assert_eq!(r.retired, vec!["1.2.0".to_owned()]);
    assert_eq!(fake.st().packs["1.2.0"].2, "retired");
    // Retired versions aren't retired again.
    let r = sync_cluster(&mut db, &p, 1, true).await.unwrap();
    assert!(r.retired.is_empty());
}

#[tokio::test]
async fn inserts_go_in_batches() {
    let (_, p) = pack("2.0.0", 700);
    assert!(p.rows.len() > BATCH_ROWS, "{}", p.rows.len());
    let fake = Fake::default();
    let mut db = fake.clone();
    let r = sync_cluster(&mut db, &p, 2, false).await.unwrap();
    let inserts = fake.st().inserts.clone();
    assert_eq!(r.batches, inserts.len());
    assert!(inserts.iter().all(|n| *n <= BATCH_ROWS));
    assert_eq!(inserts.iter().sum::<usize>(), p.rows.len());
}

#[test]
fn a_tampered_or_foreign_pack_file_is_refused() {
    let (bytes, p) = pack("1.0.0", 3);
    assert_eq!(p.sha256.to_vec(), bytes[bytes.len() - 32..].to_vec());
    let mut b = bytes.clone();
    let mid = b.len() / 2;
    b[mid] ^= 1;
    assert!(LoadedPack::from_bytes(&b).is_err());
    assert!(LoadedPack::from_bytes(b"not a pack").is_err());
}

struct Clusters {
    fake: Fake,
    ids: Vec<ClusterId>,
}

#[async_trait::async_trait]
impl DocsClusters for Clusters {
    async fn clusters(&self) -> Result<Vec<ClusterId>> {
        Ok(self.ids.clone())
    }
    async fn open(&self, _cluster: &ClusterId) -> Result<Box<dyn DocsDb>> {
        Ok(Box::new(self.fake.clone()))
    }
    async fn reader_password(&self, cluster: &ClusterId) -> Result<String> {
        Ok(format!("pw-{cluster}"))
    }
}

#[tokio::test]
async fn the_loader_verifies_once_per_process_and_asserts_the_reader_once() {
    let dir = std::env::temp_dir().join(format!("sprig-docs-loader-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("p.pack");
    let (bytes, p) = pack("3.0.0", 4);
    std::fs::write(&path, &bytes).unwrap();
    let fake = Fake::default();
    let c = ClusterId::new("dc-a-c1").unwrap();
    let clusters = Arc::new(Clusters {
        fake: fake.clone(),
        ids: vec![c.clone()],
    });
    let loader = DocsLoader::new(clusters.clone(), None, PackSource::File(path.clone()));
    let pk = loader.pack().unwrap();
    assert_eq!(pk.digest, p.digest);
    let r = loader.sync(&c, &pk).await.unwrap();
    assert!(r.loaded);
    let r = loader.sync(&c, &pk).await.unwrap();
    assert!(!r.verified && !r.loaded);
    assert_eq!(fake.st().readers, vec!["pw-dc-a-c1".to_owned()]);
    assert_eq!(fake.st().schema, 1);
    // A restarted controller verifies again and catches an edit.
    fake.st().rows.values_mut().next().unwrap().text.push('!');
    let again = DocsLoader::new(clusters, None, PackSource::File(path.clone()));
    let r = again.sync(&c, &again.pack().unwrap()).await.unwrap();
    assert!(r.tampered && r.loaded);
    // A damaged file is refused, and stays refused for a while.
    let mut b = bytes.clone();
    b[20] ^= 1;
    std::fs::write(dir.join("bad.pack"), &b).unwrap();
    let bad = DocsLoader::new(
        Arc::new(Clusters {
            fake: Fake::default(),
            ids: Vec::new(),
        }),
        None,
        PackSource::File(dir.join("bad.pack")),
    );
    assert!(bad.pack().is_err());
    assert!(bad.pack().is_err());
    std::fs::remove_dir_all(&dir).ok();
}
