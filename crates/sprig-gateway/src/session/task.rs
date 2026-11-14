//! The connection task wrapper: a panic is caught at the task boundary.
//!
//! A panic in one session's task is caught here, logged with the session id
//! and counted; the session leaves the table and its connection closes. No
//! other session notices.

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use sprig_common::ids::ConnId;
use sprig_common::{Result, SprigError};

use super::{Session, SessionTable};
use crate::frontend::{ClientConn, Protocol};

/// Counter of session tasks that panicked, by `protocol`
/// (`sprig_<component>_<what>_<unit>`, writing guide 5.8).
pub const PANICS_METRIC: &str = "sprig_gateway_session_panics_total";

/// A future that turns a panic while polling `F` into `Err(payload)`.
struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::result::Result<F::Output, Box<dyn Any + Send>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.0.as_mut();
        // The future is dropped right after a panic and never polled again,
        // so state it left half-updated is never observed.
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

/// The panic message, when it is a string.
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

fn record_panic(id: ConnId, protocol: Protocol, payload: &(dyn Any + Send)) -> SprigError {
    let message = panic_message(payload);
    tracing::error!(session = %id, %protocol, panic = message, "session task panicked; connection closed");
    metrics::counter!(PANICS_METRIC, "protocol" => protocol.as_str()).increment(1);
    SprigError::internal(format!("session {id} panicked"))
}

impl SessionTable {
    /// Runs one connection's session: opens a [`Session`] for `conn`, runs
    /// `body` with it, and cleans up however `body` ends. Front ends call it
    /// from [`Frontend::serve`](crate::frontend::Frontend::serve).
    ///
    /// - A panic in `body` (while building or polling its future) is caught,
    ///   logged with the session id and counted in
    ///   `sprig_gateway_session_panics_total{protocol}`; `body`'s future,
    ///   and with it the connection, is dropped, and the result is
    ///   `Internal`.
    /// - `KILL` ([`SessionFlags::kill_conn`](super::SessionFlags::kill_conn))
    ///   drops `body`'s future at its next await, which closes the
    ///   connection; the result is `Ok`.
    /// - The session stays in the table until the task ends, even if `body`
    ///   drops it early, and leaves it in every case.
    ///
    /// # Errors
    /// MySQL 1040 if no connection id is free, with the connection dropped
    /// unanswered (to answer it, call [`SessionTable::open`] and
    /// [`SessionTable::run_session`] instead); `body`'s own error;
    /// `Internal` after a panic.
    pub async fn run<F, Fut>(
        self: &Arc<Self>,
        conn: ClientConn,
        protocol: Protocol,
        body: F,
    ) -> Result<()>
    where
        F: FnOnce(Session, ClientConn) -> Fut,
        Fut: Future<Output = Result<()>> + Send,
    {
        let session = self.open(&conn, protocol)?;
        Self::run_session(session, conn, body).await
    }

    /// [`SessionTable::run`] for a session the caller opened, so it can use
    /// the session (or answer 1040) before handing over.
    ///
    /// # Errors
    /// As [`SessionTable::run`], without the 1040.
    pub async fn run_session<C, F, Fut>(session: Session, conn: C, body: F) -> Result<()>
    where
        F: FnOnce(Session, C) -> Fut,
        Fut: Future<Output = Result<()>> + Send,
    {
        let id = session.id;
        let protocol = session.protocol;
        let handle = Arc::clone(session.handle());
        // Holds the table entry until the task ends, whatever `body` does
        // with the session.
        let registration = Arc::clone(&session.registration);
        let fut = match catch_unwind(AssertUnwindSafe(|| body(session, conn))) {
            Ok(fut) => fut,
            Err(payload) => return Err(record_panic(id, protocol, payload.as_ref())),
        };
        let outcome = tokio::select! {
            biased;
            () = handle.flags().killed() => {
                tracing::debug!(session = %id, %protocol, "session killed");
                Ok(())
            }
            r = CatchUnwind(Box::pin(fut)) => match r {
                Ok(r) => r,
                Err(payload) => Err(record_panic(id, protocol, payload.as_ref())),
            },
        };
        drop(registration);
        outcome
    }
}
