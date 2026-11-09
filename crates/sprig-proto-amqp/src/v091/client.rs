//! A minimal AMQP 0-9-1 client for tests and tools: the handshake, method
//! calls, publishing with content, and receiving methods with their
//! content. No RabbitMQ client library is installed on the build machines,
//! so the live tests use this one; the golden-byte tests keep it honest.

use std::collections::VecDeque;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::codec::{FieldTable, FieldValue};
use super::frame::{
    ContentHeader, FRAME_BODY, FRAME_HEADER, FRAME_HEARTBEAT, FRAME_METHOD, Frame, PROTOCOL_HEADER,
    put_content, put_heartbeat, put_method,
};
use super::method::{
    ConnectionCloseOk, ConnectionOpen, ConnectionStartOk, ConnectionTuneOk, Method,
};
use super::props::BasicProperties;

/// A client error.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// I/O.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The server sent something unexpected.
    #[error("protocol: {0}")]
    Protocol(String),
    /// The server closed the connection (`connection.close`).
    #[error("connection closed by server: {0} {1}")]
    Closed(u16, String),
    /// No frame in time.
    #[error("timeout")]
    Timeout,
}

/// One method from the server, with its content.
#[derive(Debug, Clone)]
pub struct Incoming {
    /// The channel.
    pub channel: u16,
    /// The method.
    pub method: Method,
    /// Properties and body, for `basic.deliver`, `get-ok` and `return`.
    pub content: Option<(BasicProperties, Bytes)>,
}

/// A connected client.
pub struct Client<S> {
    io: S,
    inb: BytesMut,
    /// Negotiated frame size.
    pub frame_max: u32,
    /// Server properties from `connection.start`.
    pub server_properties: FieldTable,
    /// Mechanisms the server offered.
    pub mechanisms: String,
    pending: VecDeque<Incoming>,
    /// How long to wait for a frame.
    pub timeout: Duration,
}

impl<S> std::fmt::Debug for Client<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("frame_max", &self.frame_max)
            .finish_non_exhaustive()
    }
}

/// The client capabilities RabbitMQ clients announce.
#[must_use]
pub fn default_capabilities() -> FieldTable {
    let t = FieldValue::Bool(true);
    FieldTable::from([
        ("publisher_confirms", t.clone()),
        ("exchange_exchange_bindings", t.clone()),
        ("basic.nack", t.clone()),
        ("consumer_cancel_notify", t.clone()),
        ("connection.blocked", t.clone()),
        ("authentication_failure_close", t),
    ])
}

/// A SASL PLAIN response.
#[must_use]
pub fn plain(user: &str, password: &str) -> Bytes {
    Bytes::from(format!("\0{user}\0{password}"))
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
    /// Sends the protocol header only; the caller drives the rest (for
    /// tests of the handshake itself).
    ///
    /// # Errors
    /// I/O.
    pub async fn raw(mut io: S) -> Result<Self, ClientError> {
        io.write_all(&PROTOCOL_HEADER).await?;
        Ok(Self {
            io,
            inb: BytesMut::new(),
            frame_max: 131_072,
            server_properties: FieldTable::new(),
            mechanisms: String::new(),
            pending: VecDeque::new(),
            timeout: Duration::from_secs(10),
        })
    }

    /// Connects: header, `start-ok` with `mechanism` and `response`,
    /// `tune-ok` (taking the server's values unless `heartbeat` is given),
    /// `open`.
    ///
    /// # Errors
    /// A refused login (`Closed(403, ..)` with
    /// `authentication_failure_close`), or a protocol error.
    pub async fn connect(
        io: S,
        mechanism: &str,
        response: Bytes,
        vhost: &str,
        capabilities: FieldTable,
        heartbeat: Option<u16>,
    ) -> Result<Self, ClientError> {
        let mut c = Self::raw(io).await?;
        let Method::ConnectionStart(start) = c.next_method(0).await?.method else {
            return Err(ClientError::Protocol("expected connection.start".into()));
        };
        c.server_properties = start.server_properties;
        c.mechanisms = String::from_utf8_lossy(&start.mechanisms).into_owned();
        let props = FieldTable::from([
            ("product", FieldValue::str("sprig-proto-amqp test client")),
            ("capabilities", FieldValue::Table(capabilities)),
        ]);
        c.send(
            0,
            ConnectionStartOk {
                client_properties: props,
                mechanism: mechanism.into(),
                response,
                locale: "en_US".into(),
            },
        )
        .await?;
        let Method::ConnectionTune(tune) = c.next_method(0).await?.method else {
            return Err(ClientError::Protocol("expected connection.tune".into()));
        };
        c.frame_max = tune.frame_max;
        c.send(
            0,
            ConnectionTuneOk {
                channel_max: tune.channel_max,
                frame_max: tune.frame_max,
                heartbeat: heartbeat.unwrap_or(tune.heartbeat),
            },
        )
        .await?;
        c.send(
            0,
            ConnectionOpen {
                virtual_host: vhost.into(),
                ..Default::default()
            },
        )
        .await?;
        match c.next_method(0).await?.method {
            Method::ConnectionOpenOk(_) => Ok(c),
            other => Err(ClientError::Protocol(format!(
                "expected open-ok, got {}",
                other.name()
            ))),
        }
    }

    /// Sends a method.
    ///
    /// # Errors
    /// I/O.
    pub async fn send(&mut self, ch: u16, m: impl Into<Method>) -> Result<(), ClientError> {
        let mut b = BytesMut::new();
        put_method(&mut b, ch, &m.into());
        self.io.write_all(&b).await?;
        Ok(())
    }

    /// Sends raw bytes (malformed-frame tests).
    ///
    /// # Errors
    /// I/O.
    pub async fn send_raw(&mut self, b: &[u8]) -> Result<(), ClientError> {
        self.io.write_all(b).await?;
        Ok(())
    }

    /// Sends a heartbeat.
    ///
    /// # Errors
    /// I/O.
    pub async fn heartbeat(&mut self) -> Result<(), ClientError> {
        let mut b = BytesMut::new();
        put_heartbeat(&mut b);
        self.io.write_all(&b).await?;
        Ok(())
    }

    /// Publishes.
    ///
    /// # Errors
    /// I/O.
    pub async fn publish(
        &mut self,
        ch: u16,
        exchange: &str,
        routing_key: &str,
        mandatory: bool,
        props: &BasicProperties,
        body: &[u8],
    ) -> Result<(), ClientError> {
        let mut b = BytesMut::new();
        let m: Method = super::method::BasicPublish {
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            mandatory,
            ..Default::default()
        }
        .into();
        put_content(&mut b, ch, &m, &props.to_bytes(), body, self.frame_max);
        self.io.write_all(&b).await?;
        Ok(())
    }

    /// Reads one frame.
    async fn frame(&mut self) -> Result<Frame, ClientError> {
        loop {
            if let Some(f) =
                Frame::parse(&mut self.inb, 0).map_err(|e| ClientError::Protocol(e.to_string()))?
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

    /// The next method from the server (any channel), with content.
    ///
    /// # Errors
    /// `Closed` when the server sends `connection.close` (answered with
    /// close-ok), I/O or protocol errors.
    pub async fn recv(&mut self) -> Result<Incoming, ClientError> {
        if let Some(i) = self.pending.pop_front() {
            return Ok(i);
        }
        self.read_one().await
    }

    async fn read_one(&mut self) -> Result<Incoming, ClientError> {
        loop {
            let f = self.frame().await?;
            match f.kind {
                FRAME_HEARTBEAT => {}
                FRAME_METHOD => {
                    let method = Method::decode(&f.payload)
                        .map_err(|e| ClientError::Protocol(e.to_string()))?;
                    if let Method::ConnectionClose(c) = &method {
                        let _ = self.send(0, ConnectionCloseOk {}).await;
                        return Err(ClientError::Closed(c.reply_code, c.reply_text.clone()));
                    }
                    let content = if method.has_content() {
                        let h = self.frame().await?;
                        if h.kind != FRAME_HEADER {
                            return Err(ClientError::Protocol("expected a content header".into()));
                        }
                        let h = ContentHeader::decode(&h.payload)
                            .map_err(|e| ClientError::Protocol(e.to_string()))?;
                        let mut body = BytesMut::new();
                        while (body.len() as u64) < h.body_size {
                            let b = self.frame().await?;
                            if b.kind != FRAME_BODY {
                                return Err(ClientError::Protocol("expected a body frame".into()));
                            }
                            body.extend_from_slice(&b.payload);
                        }
                        Some((h.props, body.freeze()))
                    } else {
                        None
                    };
                    return Ok(Incoming {
                        channel: f.channel,
                        method,
                        content,
                    });
                }
                other => {
                    return Err(ClientError::Protocol(format!(
                        "unexpected frame type {other}"
                    )));
                }
            }
        }
    }

    /// The next method on `ch`; methods on other channels and deliveries
    /// wait in a queue for [`Client::recv`].
    ///
    /// # Errors
    /// As [`Client::recv`].
    pub async fn next_method(&mut self, ch: u16) -> Result<Incoming, ClientError> {
        if let Some(i) = self
            .pending
            .iter()
            .position(|i| i.channel == ch && !is_async(&i.method))
        {
            return Ok(self.pending.remove(i).unwrap_or_else(|| unreachable!()));
        }
        loop {
            let i = self.read_one().await?;
            if i.channel == ch && !is_async(&i.method) {
                return Ok(i);
            }
            self.pending.push_back(i);
        }
    }

    /// Sends a method and returns the reply on its channel.
    ///
    /// # Errors
    /// As [`Client::recv`].
    pub async fn call(&mut self, ch: u16, m: impl Into<Method>) -> Result<Method, ClientError> {
        self.send(ch, m).await?;
        Ok(self.next_method(ch).await?.method)
    }

    /// Closes the connection politely.
    ///
    /// # Errors
    /// I/O.
    pub async fn close(mut self) -> Result<(), ClientError> {
        self.send(
            0,
            super::method::ConnectionClose {
                reply_code: 200,
                reply_text: "bye".into(),
                class_id: 0,
                method_id: 0,
            },
        )
        .await?;
        loop {
            match self.read_one().await {
                Ok(Incoming {
                    method: Method::ConnectionCloseOk(_),
                    ..
                })
                | Err(ClientError::Io(_)) => return Ok(()),
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
    }
}

/// Methods the server sends on its own: deliveries, returns, confirms,
/// cancels and notices.
fn is_async(m: &Method) -> bool {
    matches!(
        m,
        Method::BasicDeliver(_)
            | Method::BasicReturn(_)
            | Method::BasicAck(_)
            | Method::BasicNack(_)
            | Method::BasicCancel(_)
            | Method::ConnectionBlocked(_)
            | Method::ConnectionUnblocked(_)
    )
}
