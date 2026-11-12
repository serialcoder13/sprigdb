//! Index hints (M6, decision S135): `hint` on `find`, `count`,
//! `distinct` and `aggregate`, and `indexHints` from query settings,
//! applied as MariaDB's `FORCE INDEX` on the collection table's reads.
//!
//! The gateway resolves the hint to a MariaDB index name and runs the
//! command under [`scope`]; the engine's reads of the collection table
//! (the first batch of a `find`, `count`, the `$match` read of an
//! `aggregate`) add [`clause`] after the table name. `getMore` batches
//! run without it: MariaDB chooses again, which changes only speed.

use std::future::Future;

tokio::task_local! {
    static INDEX: Option<String>;
}

/// Runs `f` with reads of the collection table forced onto `index`.
pub async fn scope<F: Future>(index: Option<String>, f: F) -> F::Output {
    INDEX.scope(index, f).await
}

/// ` FORCE INDEX (`name`)` while a hint is in scope, else empty.
#[must_use]
pub fn clause() -> String {
    INDEX
        .try_with(|i| {
            i.as_ref()
                .map(|n| format!(" FORCE INDEX ({})", crate::sql::ident(n)))
        })
        .ok()
        .flatten()
        .unwrap_or_default()
}
