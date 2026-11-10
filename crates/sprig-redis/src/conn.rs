//! The connection loop (chapter 40, "Pipelining"): decode every command
//! the client sent, answer them in order, flush once per batch, and
//! deliver pub/sub messages between batches.
//!
//! While a command runs (a blocking pop may wait for minutes), the loop
//! keeps reading into its buffer, so a client that goes away ends the
//! command at once.

use std::future::Future;

use bytes::BytesMut;
use sprig_proto_redis::{Decoder, Frame, Limits, encode};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::session::Session;

/// Serves `sess` on `stream` until the client quits, the stream ends, or
/// `stop` completes.
///
/// # Errors
/// I/O errors on the stream.
pub async fn serve<S, F>(
    mut stream: S,
    mut sess: Session,
    limits: Limits,
    stop: F,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: Future<Output = ()> + Send,
{
    let decoder = Decoder::new(limits);
    let mut inbuf = BytesMut::with_capacity(16 * 1024);
    let mut out = BytesMut::with_capacity(16 * 1024);
    let mut msgs = sess.take_messages();
    tokio::pin!(stop);
    let r = async {
        loop {
            // Answer every whole command already buffered.
            loop {
                let cmd = match decoder.decode(&mut inbuf) {
                    Ok(Some(c)) => c,
                    Ok(None) => break,
                    Err(e) => {
                        encode(&Frame::err(&e.to_string()), sess.proto, &mut out);
                        stream.write_all(&out).await?;
                        return Ok(());
                    }
                };
                let frames = {
                    let fut = sess.handle(cmd);
                    tokio::pin!(fut);
                    loop {
                        tokio::select! {
                            f = &mut fut => break Some(f),
                            n = stream.read_buf(&mut inbuf) => {
                                if n? == 0 {
                                    // The client went away mid-command.
                                    break None;
                                }
                            }
                        }
                    }
                };
                let Some(frames) = frames else {
                    return Ok(());
                };
                for f in &frames {
                    encode(f, sess.proto, &mut out);
                }
                if sess.closing {
                    stream.write_all(&out).await?;
                    return Ok(());
                }
                if out.len() > 64 * 1024 {
                    stream.write_all(&out).await?;
                    out.clear();
                }
            }
            if !out.is_empty() {
                stream.write_all(&out).await?;
                stream.flush().await?;
                out.clear();
            }
            tokio::select! {
                n = stream.read_buf(&mut inbuf) => {
                    if n? == 0 {
                        return Ok(());
                    }
                }
                m = async {
                    match &mut msgs {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(m) = m {
                        let f = sess.message_frame(m);
                        encode(&f, sess.proto, &mut out);
                    }
                }
                () = &mut stop => return Ok(()),
            }
        }
    }
    .await;
    sess.close().await;
    r
}
