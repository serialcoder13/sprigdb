//! The session's backend connection as a chapter 18 transaction branch:
//! [`sprig_txn::BranchConn`] on [`BackendSession`], so the coordinator
//! sends its control statements (`START TRANSACTION`, `XA START`, `XA END;
//! XA PREPARE`, the decision insert and `COMMIT`, savepoints) through the
//! same pinned connection the router's statements use.
//!
//! Not yet: the session holding one `TxnCoordinator` with a
//! `BackendSession` per cluster (a gateway `TxnBackend`), and the router's
//! executor calling it (chapter 18, "Interfaces", the router's side).

use sprig_common::ErrorKind;
use sprig_proto_mysql::client::PacketKind;
use sprig_txn::{BranchConn, Ctl, DbError, DbResult};

use super::{BackendSession, StmtInfo};

fn db_error(e: &sprig_common::SprigError) -> DbError {
    match e.kind {
        ErrorKind::Backend { errno, .. } => DbError::new(errno, e.message.clone()),
        // Anything else (lost connection, pool, protocol): the outcome of
        // the statement is unknown.
        _ => DbError::lost(e.to_string()),
    }
}

#[async_trait::async_trait]
impl BranchConn for BackendSession {
    fn thread_id(&self) -> u64 {
        BackendSession::thread_id(self).map_or(0, |(_, t)| t)
    }

    async fn control(&mut self, ctl: &[Ctl]) -> DbResult<()> {
        for c in ctl {
            let sql = c.sql();
            let info = StmtInfo::classify(sql.as_bytes());
            self.query(&info, sql.as_bytes())
                .await
                .map_err(|e| db_error(&e))?;
            let mut err = None;
            while let Some(p) = self.next().await.map_err(|e| db_error(&e))? {
                if let PacketKind::Err(e) = p.kind {
                    err = Some(DbError::new(e.code, e.message.clone()));
                }
            }
            self.finish().await.map_err(|e| db_error(&e))?;
            if let Some(e) = err {
                return Err(e);
            }
        }
        Ok(())
    }
}
