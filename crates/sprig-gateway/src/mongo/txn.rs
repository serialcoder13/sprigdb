//! Transaction outcomes that outlive the gateway (chapter 30, "Sessions
//! and transactions"; build step 8; `decisions/r5-interim.md` D17).
//!
//! - **Commit records.** `commitTransaction` writes a `committed` row for
//!   `(lsid, uid)` with the transaction's number in the `__sprig_doc_sessions`
//!   table of every database the transaction wrote, inside the transaction,
//!   just before `COMMIT`: the outcome commits with the writes.
//! - **Recovery tokens.** Every reply inside a transaction carries
//!   `recoveryToken: {sprig: BinData}` naming the tenant, this gateway and
//!   the databases written so far, as a `mongos` returns its own. Drivers
//!   send it back with `commitTransaction` and `abortTransaction`.
//! - **Recovery.** A `commitTransaction` or `abortTransaction` that finds
//!   no open transaction here (another gateway ran it, this gateway
//!   restarted, or its `COMMIT` failed with an unknown outcome) reads the
//!   record of the token's first database, locked:
//!
//! ```text
//! row (N, committed)        commit: ok          abort: 256 TransactionCommitted
//! anything else             write a fence (N, $aborted), so the transaction,
//!                           still open on its gateway, can never commit;
//!                           commit: 251 NoSuchTransaction   abort: ok
//! ```
//!
//!   The lock serializes recovery with the transaction's own commit, which
//!   locks the same row first: whichever comes first decides, and the
//!   other sees it.

use std::collections::BTreeSet;

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, doc};
use sprig_doc::engine::{SqlRunner, mongo_error};
use sprig_doc::sessions::{self, StmtResults};
use sprig_proto_mongo::errors::{MongoError, code};

use super::sql::SqlSession;

/// MongoDB's `TransactionCommitted`.
const TRANSACTION_COMMITTED: i32 = 256;

/// The recovery token of a transaction of `tenant` on gateway `gateway`
/// that wrote `dbs`.
#[must_use]
pub fn token(tenant: u64, gateway: &str, dbs: &BTreeSet<String>) -> Document {
    let inner = doc! {
        "v": 1,
        "t": i64::try_from(tenant).unwrap_or(i64::MAX),
        "g": gateway,
        "d": dbs.iter().cloned().map(Bson::String).collect::<Vec<_>>(),
    };
    doc! {"sprig": Binary {
        subtype: BinarySubtype::Generic,
        bytes: bson::to_vec(&inner).unwrap_or_default(),
    }}
}

/// The databases of the request's `recoveryToken`, when it is one of
/// `tenant`'s; `None` without a token (or another tenant's).
#[must_use]
pub fn token_dbs(body: &Document, tenant: u64) -> Option<BTreeSet<String>> {
    let b = body
        .get_document("recoveryToken")
        .ok()?
        .get_binary_generic("sprig")
        .ok()?;
    let inner = bson::from_slice::<Document>(b).ok()?;
    if inner.get_i64("t").ok()? != i64::try_from(tenant).ok()? {
        return None;
    }
    Some(
        inner
            .get_array("d")
            .ok()?
            .iter()
            .filter_map(|d| d.as_str().map(str::to_owned))
            .collect(),
    )
}

async fn locked(
    s: &mut SqlSession,
    db: &str,
    lsid: &[u8],
    uid: &[u8; 32],
) -> Result<Option<sessions::Record>, MongoError> {
    let rows = s
        .query(&sessions::select_sql(db, lsid, uid))
        .await
        .map_err(|e| mongo_error(&e))?;
    rows.first().map(|r| sessions::decode(r)).transpose()
}

fn no_txn(n: i64) -> MongoError {
    MongoError::new(
        code::NO_SUCH_TRANSACTION,
        format!("Transaction with txnNumber {n} has been aborted."),
    )
    .label("TransientTransactionError")
}

/// Inside the open transaction `n`, before `COMMIT`: the outcome rows of
/// every database it wrote. A fence or a newer number found there means
/// another gateway settled it as aborted: 251, and the caller rolls back.
///
/// # Errors
/// 251 for a fenced transaction; SQL errors.
pub async fn commit_records(
    s: &mut SqlSession,
    dbs: &BTreeSet<String>,
    lsid: &[u8],
    uid: &[u8; 32],
    n: i64,
) -> Result<(), MongoError> {
    let empty = sessions::encode(&StmtResults::new())?;
    for db in dbs {
        if let Some(r) = locked(s, db, lsid, uid).await?
            && (r.txn_number > n || (r.txn_number == n && r.aborted))
        {
            return Err(no_txn(n));
        }
        s.exec(&sessions::upsert_sql(db, lsid, uid, n, &empty, true))
            .await
            .map_err(|e| mongo_error(&e))?;
    }
    Ok(())
}

/// Settles transaction `n` from its records (see the module docs), on
/// the connection's own session, outside any transaction.
///
/// # Errors
/// 251 for a commit of a transaction that didn't commit; 256 for an
/// abort of one that did; SQL errors.
pub async fn recover(
    s: &mut SqlSession,
    commit: bool,
    dbs: &BTreeSet<String>,
    lsid: &[u8],
    uid: &[u8; 32],
    n: i64,
) -> Result<Document, MongoError> {
    let Some(db) = dbs.iter().next() else {
        // A transaction that wrote nothing has nothing to commit.
        return Ok(doc! {"ok": 1.0});
    };
    s.exec("START TRANSACTION")
        .await
        .map_err(|e| mongo_error(&e))?;
    let r = settle(s, db, lsid, uid, n).await;
    let end = if r.is_ok() { "COMMIT" } else { "ROLLBACK" };
    let ended = s.exec(end).await.map_err(|e| mongo_error(&e));
    let committed = r?;
    ended?;
    metrics::counter!("sprig_mongo_transactions_total", "outcome" => if committed { "recovered_committed" } else { "recovered_aborted" })
        .increment(1);
    match (committed, commit) {
        (true, true) | (false, false) => Ok(doc! {"ok": 1.0}),
        (false, true) => Err(no_txn(n)),
        (true, false) => Err(MongoError::new(
            TRANSACTION_COMMITTED,
            format!("Transaction with txnNumber {n} has been committed."),
        )),
    }
}

/// Whether `n` committed; fences it when it didn't.
async fn settle(
    s: &mut SqlSession,
    db: &str,
    lsid: &[u8],
    uid: &[u8; 32],
    n: i64,
) -> Result<bool, MongoError> {
    let r = match locked(s, db, lsid, uid).await {
        Ok(r) => r,
        // No records table: nothing was ever committed there.
        Err(e) if e.code == code::NAMESPACE_NOT_FOUND => return Ok(false),
        Err(e) => return Err(e),
    };
    match r {
        Some(r) if r.txn_number == n && r.committed => Ok(true),
        Some(r) if r.txn_number > n || (r.txn_number == n && r.aborted) => Ok(false),
        _ => {
            s.exec(&sessions::upsert_sql(
                db,
                lsid,
                uid,
                n,
                &sessions::aborted_results(),
                false,
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_round_trip_per_tenant() {
        let dbs: BTreeSet<String> = ["a".to_owned(), "b".to_owned()].into();
        let t = token(42, "gw-1", &dbs);
        let body = doc! {"commitTransaction": 1, "recoveryToken": t};
        assert_eq!(token_dbs(&body, 42), Some(dbs));
        assert_eq!(token_dbs(&body, 43), None);
        assert_eq!(token_dbs(&doc! {"commitTransaction": 1}, 42), None);
    }
}
