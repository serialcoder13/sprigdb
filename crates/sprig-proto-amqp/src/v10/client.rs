//! A minimal AMQP 1.0 client for tests and tools: SASL, `open`, `begin`,
//! links, transfers (split into frames), flow and dispositions. No AMQP
//! 1.0 client library is installed on the build machines.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::performative::{
    DeliveryState, FRAME_AMQP, FRAME_SASL, Frame, Performative, Source, Target, put_frame, role,
    snd_settle,
};
use crate::sniff::{AMQP_10, AMQP_10_SASL};

/// A client error.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// I/O.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Something unexpected.
    #[error("protocol: {0}")]
    Protocol(String),
    /// SASL refused, with the outcome code.
    #[error("sasl outcome {0}")]
    Sasl(u8),
    /// No frame in time.
    #[error("timeout")]
    Timeout,
}

/// One performative from the server, with a whole transfer's payload.
#[derive(Debug, Clone)]
pub struct Incoming {
    /// The channel.
    pub channel: u16,
    /// The performative (for a multi-frame transfer, its first frame's).
    pub perf: Performative,
    /// The message payload of a transfer.
    pub payload: Bytes,
}

/// A connected client.
pub struct Client<S> {
    io: S,
    inb: BytesMut,
    /// The server's `max-frame-size`.
    pub max_frame: u32,
    /// The server's open.
    pub server_open: Option<Performative>,
    pending: VecDeque<Incoming>,
    partial: HashMap<(u16, u32), (Performative, BytesMut)>,
    next_delivery: HashMap<u16, u32>,
    /// How long to wait for a frame.
    pub timeout: Duration,
}

impl<S> std::fmt::Debug for Client<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
    /// SASL with `mechanism`, then the AMQP header and `open` with
    /// `hostname` (`vhost:<name>`).
    ///
    /// # Errors
    /// `Sasl(code)` when refused; protocol and I/O errors.
    pub async fn connect(
        mut io: S,
        mechanism: &str,
        response: Option<Bytes>,
        hostname: Option<&str>,
    ) -> Result<Self, ClientError> {
        io.write_all(&AMQP_10_SASL).await?;
        let mut c = Self {
            io,
            inb: BytesMut::new(),
            max_frame: 512,
            server_open: None,
            pending: VecDeque::new(),
            partial: HashMap::new(),
            next_delivery: HashMap::new(),
            timeout: Duration::from_secs(10),
        };
        let h = c.header().await?;
        if h != AMQP_10_SASL {
            return Err(ClientError::Protocol(format!("unexpected header {h:02x?}")));
        }
        let f = c.frame().await?;
        let Ok(Performative::SaslMechanisms { mechanisms }) = perf(&f) else {
            return Err(ClientError::Protocol("expected sasl-mechanisms".into()));
        };
        if !mechanisms.iter().any(|m| m == mechanism) && mechanism != "ANONYMOUS" {
            return Err(ClientError::Protocol(format!(
                "{mechanism} not offered: {mechanisms:?}"
            )));
        }
        c.send_kind(
            FRAME_SASL,
            0,
            &Performative::SaslInit {
                mechanism: mechanism.into(),
                initial_response: response,
                hostname: None,
            },
            &[],
        )
        .await?;
        let f = c.frame().await?;
        let Ok(Performative::SaslOutcome { code, .. }) = perf(&f) else {
            return Err(ClientError::Protocol("expected sasl-outcome".into()));
        };
        if code != 0 {
            return Err(ClientError::Sasl(code));
        }
        c.io.write_all(&AMQP_10).await?;
        let h = c.header().await?;
        if h != AMQP_10 {
            return Err(ClientError::Protocol(format!("unexpected header {h:02x?}")));
        }
        c.send(
            0,
            &Performative::Open {
                container_id: "sprig-test".into(),
                hostname: hostname.map(str::to_owned),
                max_frame_size: 65_536,
                channel_max: 64,
                idle_timeout: None,
                offered_capabilities: vec![],
                desired_capabilities: vec![],
                properties: None,
            },
        )
        .await?;
        let open = c.expect(0).await?;
        match &open.perf {
            Performative::Open { max_frame_size, .. } => {
                c.max_frame = (*max_frame_size).min(65_536);
            }
            Performative::Close { error } => {
                return Err(ClientError::Protocol(format!("closed at open: {error:?}")));
            }
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected open, got {}",
                    other.name()
                )));
            }
        }
        c.server_open = Some(open.perf);
        Ok(c)
    }

    async fn header(&mut self) -> Result<[u8; 8], ClientError> {
        while self.inb.len() < 8 {
            let n = tokio::time::timeout(self.timeout, self.io.read_buf(&mut self.inb))
                .await
                .map_err(|_| ClientError::Timeout)??;
            if n == 0 {
                return Err(ClientError::Io(std::io::ErrorKind::UnexpectedEof.into()));
            }
        }
        let h = self.inb.split_to(8);
        let mut a = [0u8; 8];
        a.copy_from_slice(&h);
        Ok(a)
    }

    async fn frame(&mut self) -> Result<Frame, ClientError> {
        loop {
            if let Some(f) =
                Frame::parse(&mut self.inb, 0).map_err(|e| ClientError::Protocol(e.0))?
            {
                return Ok(f);
            }
            let n = tokio::time::timeout(self.timeout, self.io.read_buf(&mut self.inb))
                .await
                .map_err(|_| ClientError::Timeout)??;
            if n == 0 {
                return Err(ClientError::Io(std::io::ErrorKind::UnexpectedEof.into()));
            }
        }
    }

    async fn send_kind(
        &mut self,
        kind: u8,
        ch: u16,
        p: &Performative,
        payload: &[u8],
    ) -> Result<(), ClientError> {
        let mut b = BytesMut::new();
        put_frame(&mut b, kind, ch, &p.to_value(), payload);
        self.io.write_all(&b).await?;
        Ok(())
    }

    /// Sends a performative.
    ///
    /// # Errors
    /// I/O.
    pub async fn send(&mut self, ch: u16, p: &Performative) -> Result<(), ClientError> {
        self.send_kind(FRAME_AMQP, ch, p, &[]).await
    }

    /// Sends raw bytes.
    ///
    /// # Errors
    /// I/O.
    pub async fn send_raw(&mut self, b: &[u8]) -> Result<(), ClientError> {
        self.io.write_all(b).await?;
        Ok(())
    }

    /// The next performative from the server, with whole transfers.
    ///
    /// # Errors
    /// I/O, protocol, timeout.
    pub async fn recv(&mut self) -> Result<Incoming, ClientError> {
        if let Some(i) = self.pending.pop_front() {
            return Ok(i);
        }
        self.read_one().await
    }

    async fn read_one(&mut self) -> Result<Incoming, ClientError> {
        loop {
            let f = self.frame().await?;
            if f.body.is_none() {
                continue;
            }
            let p = perf(&f)?;
            if let Performative::Transfer { handle, more, .. } = &p {
                let key = (f.channel, *handle);
                let (first, mut buf) = self
                    .partial
                    .remove(&key)
                    .unwrap_or_else(|| (p.clone(), BytesMut::new()));
                buf.extend_from_slice(&f.payload);
                if *more {
                    self.partial.insert(key, (first, buf));
                    continue;
                }
                return Ok(Incoming {
                    channel: f.channel,
                    perf: first,
                    payload: buf.freeze(),
                });
            }
            return Ok(Incoming {
                channel: f.channel,
                perf: p,
                payload: Bytes::new(),
            });
        }
    }

    /// The next non-transfer, non-flow, non-disposition performative on
    /// `ch`; others wait for [`Client::recv`].
    ///
    /// # Errors
    /// As [`Client::recv`].
    pub async fn expect(&mut self, ch: u16) -> Result<Incoming, ClientError> {
        let is_reply = |i: &Incoming| {
            i.channel == ch
                && !matches!(
                    i.perf,
                    Performative::Transfer { .. }
                        | Performative::Flow { .. }
                        | Performative::Disposition { .. }
                )
        };
        if let Some(i) = self.pending.iter().position(is_reply) {
            return Ok(self.pending.remove(i).unwrap_or_else(|| unreachable!()));
        }
        loop {
            let i = self.read_one().await?;
            if is_reply(&i) {
                return Ok(i);
            }
            self.pending.push_back(i);
        }
    }

    /// The next performative matching `f`; others wait.
    ///
    /// # Errors
    /// As [`Client::recv`].
    pub async fn wait_for(
        &mut self,
        f: impl Fn(&Incoming) -> bool,
    ) -> Result<Incoming, ClientError> {
        if let Some(i) = self.pending.iter().position(&f) {
            return Ok(self.pending.remove(i).unwrap_or_else(|| unreachable!()));
        }
        loop {
            let i = self.read_one().await?;
            if f(&i) {
                return Ok(i);
            }
            self.pending.push_back(i);
        }
    }

    /// Begins a session on `ch`.
    ///
    /// # Errors
    /// As [`Client::recv`].
    pub async fn begin(&mut self, ch: u16) -> Result<Performative, ClientError> {
        self.send(
            ch,
            &Performative::Begin {
                remote_channel: None,
                next_outgoing_id: 0,
                incoming_window: 65_535,
                outgoing_window: 65_535,
                handle_max: 255,
                offered_capabilities: vec![],
                desired_capabilities: vec![],
                properties: None,
            },
        )
        .await?;
        Ok(self.expect(ch).await?.perf)
    }

    /// Attaches a link; `sender` is our role. Returns the server's attach
    /// (or its detach, for a refused link: the refusal's attach is
    /// skipped).
    ///
    /// # Errors
    /// As [`Client::recv`].
    #[allow(clippy::too_many_arguments)]
    pub async fn attach(
        &mut self,
        ch: u16,
        handle: u32,
        name: &str,
        sender: bool,
        address: Option<&str>,
        settled: bool,
        source_caps: Vec<String>,
    ) -> Result<Performative, ClientError> {
        let (source, target) = if sender {
            (
                Some(Source::default()),
                Some(Target {
                    address: address.map(str::to_owned),
                    ..Target::default()
                }),
            )
        } else {
            (
                Some(Source {
                    address: address.map(str::to_owned),
                    capabilities: source_caps,
                    ..Source::default()
                }),
                Some(Target::default()),
            )
        };
        self.send(
            ch,
            &Performative::Attach {
                name: name.into(),
                handle,
                role: if sender { role::SENDER } else { role::RECEIVER },
                snd_settle_mode: if settled {
                    snd_settle::SETTLED
                } else {
                    snd_settle::UNSETTLED
                },
                rcv_settle_mode: 0,
                source,
                target,
                coordinator: false,
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: sender.then_some(0),
                max_message_size: None,
                offered_capabilities: vec![],
                desired_capabilities: vec![],
                properties: None,
            },
        )
        .await?;
        let a = self.expect(ch).await?.perf;
        // A refused link: null terminus, then detach.
        let refused = match &a {
            Performative::Attach { source, target, .. } => {
                if sender {
                    target.is_none()
                } else {
                    source.is_none()
                }
            }
            _ => false,
        };
        if refused {
            return Ok(self.expect(ch).await?.perf);
        }
        Ok(a)
    }

    /// Grants link credit.
    ///
    /// # Errors
    /// I/O.
    pub async fn flow(
        &mut self,
        ch: u16,
        handle: u32,
        delivery_count: u32,
        credit: u32,
        drain: bool,
    ) -> Result<(), ClientError> {
        self.send(
            ch,
            &Performative::Flow {
                next_incoming_id: Some(0),
                incoming_window: 65_535,
                next_outgoing_id: self.next_delivery.get(&ch).copied().unwrap_or(0),
                outgoing_window: 65_535,
                handle: Some(handle),
                delivery_count: Some(delivery_count),
                link_credit: Some(credit),
                available: None,
                drain,
                echo: false,
                properties: None,
            },
        )
        .await
    }

    /// Sends a message on a sending link, split into frames that fit;
    /// returns its delivery id.
    ///
    /// # Errors
    /// I/O.
    pub async fn transfer(
        &mut self,
        ch: u16,
        handle: u32,
        settled: bool,
        payload: &[u8],
    ) -> Result<u32, ClientError> {
        let id = *self.next_delivery.entry(ch).or_insert(0);
        self.next_delivery.insert(ch, id.wrapping_add(1));
        let room = (self.max_frame as usize).saturating_sub(64).max(64);
        let chunks: Vec<&[u8]> = if payload.is_empty() {
            vec![&[]]
        } else {
            payload.chunks(room).collect()
        };
        let n = chunks.len();
        for (i, chunk) in chunks.into_iter().enumerate() {
            let first = i == 0;
            let t = Performative::Transfer {
                handle,
                delivery_id: first.then_some(id),
                delivery_tag: first.then(|| Bytes::copy_from_slice(&id.to_be_bytes())),
                message_format: first.then_some(0),
                settled: first.then_some(settled),
                more: i + 1 < n,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            };
            self.send_kind(FRAME_AMQP, ch, &t, chunk).await?;
        }
        Ok(id)
    }

    /// Settles received deliveries with an outcome.
    ///
    /// # Errors
    /// I/O.
    pub async fn settle(
        &mut self,
        ch: u16,
        first: u32,
        last: u32,
        state: DeliveryState,
    ) -> Result<(), ClientError> {
        self.send(
            ch,
            &Performative::Disposition {
                role: role::RECEIVER,
                first,
                last: Some(last),
                settled: true,
                state: Some(state),
                batchable: false,
            },
        )
        .await
    }

    /// Closes the connection.
    ///
    /// # Errors
    /// I/O.
    pub async fn close(mut self) -> Result<(), ClientError> {
        self.send(0, &Performative::Close { error: None }).await?;
        loop {
            match self.read_one().await {
                Ok(Incoming {
                    perf: Performative::Close { .. },
                    ..
                })
                | Err(_) => return Ok(()),
                Ok(_) => {}
            }
        }
    }
}

fn perf(f: &Frame) -> Result<Performative, ClientError> {
    let b = f
        .body
        .as_ref()
        .ok_or_else(|| ClientError::Protocol("empty frame".into()))?;
    Performative::from_value(b).map_err(|e| ClientError::Protocol(e.0))
}
