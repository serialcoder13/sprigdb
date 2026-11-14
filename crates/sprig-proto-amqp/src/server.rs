//! One AMQP connection on 5672 or 5671, from the protocol header on.

use std::sync::Arc;
use std::time::Duration;

use sprig_common::CancellationToken;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::auth::{ConnInfo, Identity};
use crate::backend::QueueBackend;
use crate::error::{MqCode, MqError};
use crate::reply_to::DirectReplyTo;
use crate::sniff::Detected;

/// Settings read when a connection starts.
#[derive(Debug, Clone)]
pub struct Config {
    /// `product` in server properties.
    pub product: String,
    /// `version` in server properties.
    pub version: String,
    /// This gateway's name (`cluster_name`, 1.0 `container-id`).
    pub node: String,
    /// `amqp.sasl_mechanisms`.
    pub mechanisms: Vec<String>,
    /// `amqp.heartbeat_seconds`.
    pub heartbeat: u16,
    /// `amqp.frame_max`.
    pub frame_max: u32,
    /// `amqp.channel_max`.
    pub channel_max: u16,
    /// `amqp.max_message_bytes`.
    pub max_message_bytes: u64,
    /// `amqp.max_unacked_per_consumer`: the cap for prefetch 0.
    pub max_unacked_per_consumer: u32,
    /// `gateway.handshake_timeout_ms`: header, SASL and open.
    pub handshake_timeout: Duration,
    /// `amqp10.link_credit`.
    pub link_credit: u32,
    /// `amqp10.legacy_addresses`.
    pub legacy_addresses: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            product: "SprigDB".into(),
            version: "4.1.0".into(),
            node: "sprigdb".into(),
            mechanisms: vec!["PLAIN".into(), "AMQPLAIN".into(), "EXTERNAL".into()],
            heartbeat: 60,
            frame_max: 131_072,
            channel_max: 2047,
            max_message_bytes: 16 * 1024 * 1024,
            max_unacked_per_consumer: 65_535,
            handshake_timeout: Duration::from_secs(10),
            link_credit: 256,
            legacy_addresses: false,
        }
    }
}

/// What connections are served with.
#[derive(Clone)]
pub struct Services {
    /// Logins.
    pub identity: Arc<dyn Identity>,
    /// The queue engine.
    pub backend: Arc<dyn QueueBackend>,
    /// Direct reply-to requesters of this gateway.
    pub replies: Arc<DirectReplyTo>,
}

impl std::fmt::Debug for Services {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Services").finish_non_exhaustive()
    }
}

/// Serves one connection: reads the protocol header and runs the front end
/// it names, until the connection ends or `close` asks it to (the gateway
/// drains: 0-9-1 `connection.close` 320, 1.0 `close` with
/// `amqp:connection:forced`).
///
/// # Errors
/// What ended the connection abnormally (logged by the caller).
pub async fn serve<S>(
    mut io: S,
    info: ConnInfo,
    svc: &Services,
    cfg: Config,
    close: CancellationToken,
) -> Result<(), MqError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut header = [0u8; 8];
    match tokio::time::timeout(cfg.handshake_timeout, io.read_exact(&mut header)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(io_err(&e)),
        Err(_) => {
            return Err(MqError::new(
                MqCode::ProtocolHeader,
                "no protocol header in time",
            ));
        }
    }
    let detected = Detected::of(&header);
    if let Some(reply) = detected.refusal() {
        metrics::counter!("sprig_amqp_protocol_header_refused_total", "header" => detected.label())
            .increment(1);
        let _ = io.write_all(&reply).await;
        let _ = io.flush().await;
        let _ = io.shutdown().await;
        return Err(MqError::new(
            MqCode::ProtocolHeader,
            format!("refused protocol header {header:02x?}"),
        ));
    }
    let gauge = metrics::gauge!("sprig_amqp_connections", "protocol" => detected.label());
    gauge.increment(1.0);
    let r = match detected {
        Detected::Amqp091 => crate::v091::server::run(io, info, svc, cfg, close).await,
        _ => crate::v10::server::run(io, info, svc, cfg, close).await,
    };
    gauge.decrement(1.0);
    r
}

pub(crate) fn io_err(e: &std::io::Error) -> MqError {
    MqError::new(MqCode::FrameError, format!("connection error: {e}")).with_reply(320)
}
