//! Tests that run the real model. They need it on disk: run
//! `scripts/fetch-embed-model.sh` and set `SPRIG_EMBED_MODEL_DIR` to the
//! directory it prints. Without the variable they pass without checking.

use std::sync::OnceLock;

use sprig_embed::{DIM, Embedder, cosine};

fn model() -> Option<&'static Embedder> {
    static MODEL: OnceLock<Option<Embedder>> = OnceLock::new();
    MODEL
        .get_or_init(|| {
            let dir = std::env::var_os("SPRIG_EMBED_MODEL_DIR")?;
            Some(Embedder::load(std::path::Path::new(&dir)).expect("load the model"))
        })
        .as_ref()
}

macro_rules! need_model {
    () => {
        match model() {
            Some(m) => m,
            None => {
                eprintln!("SPRIG_EMBED_MODEL_DIR is not set; skipped");
                return;
            }
        }
    };
}

fn norm(v: &[f32; DIM]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

fn bits(v: &[[f32; DIM]]) -> Vec<u32> {
    v.iter().flatten().map(|x| x.to_bits()).collect()
}

#[test]
fn vectors_are_unit_length() {
    let m = need_model!();
    let p = m
        .embed_passages(&["Shards move between clusters online.", "", "x"])
        .expect("embed");
    assert_eq!(p.len(), 3);
    for v in &p {
        assert!((norm(v) - 1.0).abs() < 1e-4, "norm {}", norm(v));
    }
    let q = m.embed_query("how do shards move").expect("embed");
    assert!((norm(&q) - 1.0).abs() < 1e-4);
}

#[test]
fn similar_sentences_score_higher() {
    let m = need_model!();
    let p = m
        .embed_passages(&[
            "A shard move copies rows to the target cluster, then cuts over.",
            "Bananas are a good source of potassium.",
        ])
        .expect("embed");
    let q = m
        .embed_query("how do I move a shard to another cluster")
        .expect("embed");
    let (near, far) = (cosine(&q, &p[0]), cosine(&q, &p[1]));
    assert!(near > far + 0.1, "near {near}, far {far}");

    let a = m
        .embed_passages(&[
            "The gateway refused the connection because the tenant has too many sessions.",
            "Too many open sessions for the tenant, so the new connection was rejected.",
            "Vector indexes use cosine distance by default.",
        ])
        .expect("embed");
    assert!(cosine(&a[0], &a[1]) > cosine(&a[0], &a[2]) + 0.1);
}

#[test]
fn embeddings_are_deterministic() {
    let m = need_model!();
    let texts = [
        "Automatic indexing proves each index on a copy before building it.",
        "Error 39134 means the move's source diverged.",
    ];
    let one = m.embed_passages(&texts).expect("embed");
    let two = m.embed_passages(&texts).expect("embed");
    // Bit for bit, not merely close.
    assert_eq!(bits(&one), bits(&two));
    // A batch gives each text the vector it gets alone, to rounding.
    let alone = m.embed_passages(&texts[1..]).expect("embed");
    assert!(cosine(&alone[0], &one[1]) > 0.9999);
    let q1 = m.embed_query("what is 39134").expect("embed");
    let q2 = m.embed_query("what is 39134").expect("embed");
    assert_eq!(bits(&[q1]), bits(&[q2]));
}

#[test]
fn long_inputs_are_truncated() {
    let m = need_model!();
    let long = "replication lag ".repeat(2000);
    assert!(m.count_tokens(&long).expect("count") > 512);
    let v = m.embed_passages(&[long.as_str()]).expect("embed");
    assert!((norm(&v[0]) - 1.0).abs() < 1e-4);
}
