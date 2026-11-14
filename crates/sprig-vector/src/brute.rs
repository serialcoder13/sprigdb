//! Exact distances in Rust, brute force: the reference for exactness and recall.

use crate::index::Metric;

/// MariaDB's distance between two vectors, computed in `f64`.
#[must_use]
pub fn distance(m: Metric, a: &[f32], b: &[f32]) -> f64 {
    match m {
        Metric::Euclidean => a
            .iter()
            .zip(b)
            .map(|(x, y)| {
                let d = f64::from(*x) - f64::from(*y);
                d * d
            })
            .sum::<f64>()
            .sqrt(),
        Metric::Cosine => {
            let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
            for (x, y) in a.iter().zip(b) {
                let (x, y) = (f64::from(*x), f64::from(*y));
                dot += x * y;
                na += x * x;
                nb += y * y;
            }
            if na == 0.0 || nb == 0.0 {
                return 1.0;
            }
            1.0 - dot / (na.sqrt() * nb.sqrt())
        }
    }
}

/// `v` scaled to length 1 (`dotProduct` indexes store these); `None` for
/// a zero vector.
#[must_use]
pub fn normalized(v: &[f32]) -> Option<Vec<f32>> {
    let n = v
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    if n == 0.0 || !n.is_finite() {
        return None;
    }
    Some(v.iter().map(|x| (f64::from(*x) / n) as f32).collect())
}

/// The `k` nearest of `items` to `q`, by distance then id.
#[must_use]
pub fn top_k<'a, I>(m: Metric, q: &[f32], items: I, k: usize) -> Vec<(Vec<u8>, f64)>
where
    I: IntoIterator<Item = (&'a [u8], &'a [f32])>,
{
    let mut all: Vec<(Vec<u8>, f64)> = items
        .into_iter()
        .map(|(id, v)| (id.to_vec(), distance(m, q, v)))
        .collect();
    all.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    all.truncate(k);
    all
}

/// Recall at k: the share of `exact`'s ids found in `got`.
#[must_use]
pub fn recall(exact: &[Vec<u8>], got: &[Vec<u8>]) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    let hit = exact.iter().filter(|id| got.contains(id)).count();
    hit as f64 / exact.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances() {
        assert!((distance(Metric::Euclidean, &[0.0, 3.0], &[4.0, 0.0]) - 5.0).abs() < 1e-12);
        assert!(distance(Metric::Cosine, &[1.0, 0.0], &[2.0, 0.0]).abs() < 1e-12);
        assert!((distance(Metric::Cosine, &[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-12);
        assert!((distance(Metric::Cosine, &[1.0, 0.0], &[-1.0, 0.0]) - 2.0).abs() < 1e-12);
        assert_eq!(normalized(&[0.0, 0.0]), None);
        let n = normalized(&[3.0, 4.0]).unwrap();
        assert!((n[0] - 0.6).abs() < 1e-6 && (n[1] - 0.8).abs() < 1e-6);
        let items = [
            (b"a".as_slice(), [0.0f32, 1.0].as_slice()),
            (b"b", &[0.0, 2.0]),
            (b"c", &[0.0, 0.5]),
        ];
        let top = top_k(Metric::Euclidean, &[0.0, 0.9], items, 2);
        assert_eq!(
            top.iter().map(|x| x.0.clone()).collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"c".to_vec()]
        );
        assert!((recall(&[b"a".to_vec(), b"c".to_vec()], &[b"c".to_vec()]) - 0.5).abs() < 1e-12);
    }
}
