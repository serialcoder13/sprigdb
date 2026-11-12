//! MongoDB 9.0's per-operation memory limit (M3, decision S99): the
//! gateway runs a command inside [`scope`] with
//! `mongo.max_operation_memory_bytes`, and the in-gateway stages report
//! the bytes they hold to [`check`]; past the limit the command fails with
//! 146 `ExceededMemoryLimit`. A stage over its own budget
//! (`doc.agg_stage_memory_bytes`, `doc.max_sort_bytes`) still gets 292
//! `QueryExceededMemoryLimitNoDiskUseAllowed` first, as in MongoDB.

use std::future::Future;

use sprig_proto_mongo::errors::{MongoError, code};

tokio::task_local! {
    static LIMIT: usize;
}

/// Runs `f` with an operation memory limit of `bytes` (0: none).
pub async fn scope<F: Future>(bytes: usize, f: F) -> F::Output {
    if bytes == 0 {
        f.await
    } else {
        LIMIT.scope(bytes, f).await
    }
}

/// Checks that `bytes` held by `stage` fit the operation's limit.
///
/// # Errors
/// 146 `ExceededMemoryLimit` past the limit.
pub fn check(bytes: usize, stage: &str) -> Result<(), MongoError> {
    match LIMIT.try_with(|l| *l) {
        Ok(limit) if bytes > limit => Err(MongoError::new(
            code::EXCEEDED_MEMORY_LIMIT,
            format!(
                "Query exceeded the memory limit of {limit} bytes for one operation at {stage} \
                 (mongo.max_operation_memory_bytes)"
            ),
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limits_only_inside_a_scope() {
        assert!(check(usize::MAX, "$group").is_ok());
        let r = scope(10, async { check(11, "$group") }).await;
        assert_eq!(r.unwrap_err().code, 146);
        assert!(scope(10, async { check(10, "$sort") }).await.is_ok());
    }
}
