//! A minimal Kafka client over any byte stream: for this crate's tests, the
//! gateway's live tests and tools. No Kafka client library is available
//! offline (no librdkafka, kcat, Java or Python Kafka client on the build
//! machines), so SprigDB tests its broker with this one, written from the
//! protocol guide, not from the broker's code paths.
//!
//! It sends one request at a time and handles SASL PLAIN and SCRAM
//! (SaslHandshake v1 + SaslAuthenticate) and the consumer protocol's
//! subscription and assignment bytes (`ConsumerProtocolSubscription` and
//! `ConsumerProtocolAssignment`, version 0).

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::messages::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use crate::protocol::{RequestHeader, api, read_response_header, request_frame};
use crate::sasl::{ScramClient, ScramMechanism};
use crate::wire::{Message, Reader, Writer};

/// A client error.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The socket failed or closed.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    /// A malformed response.
    #[error("codec: {0}")]
    Codec(#[from] crate::wire::CodecError),
    /// The server refused (error code and message).
    #[error("error {0}: {1}")]
    Refused(i16, String),
}

/// A connection.
#[derive(Debug)]
pub struct Client<S> {
    stream: S,
    next_id: i32,
    /// The client id sent in headers.
    pub client_id: String,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<S> {
    /// A client over `stream`.
    pub fn new(stream: S, client_id: &str) -> Self {
        Self {
            stream,
            next_id: 1,
            client_id: client_id.to_owned(),
        }
    }

    /// Sends a request and reads its response.
    ///
    /// # Errors
    /// [`ClientError`].
    pub async fn call<Q: Message, R: Message>(
        &mut self,
        version: i16,
        q: &Q,
    ) -> Result<R, ClientError> {
        let id = self.next_id;
        self.next_id += 1;
        let header = RequestHeader {
            api_key: Q::API_KEY,
            api_version: version,
            correlation_id: id,
            client_id: Some(self.client_id.clone()),
            unknown_tagged_fields: Vec::new(),
        };
        let frame = request_frame(&header, |w| q.encode(w, version))?;
        self.stream.write_all(&frame).await?;
        let resp = self.read_frame().await?;
        let (corr, mut r) = read_response_header(&resp, Q::API_KEY, version)?;
        if corr != id {
            return Err(ClientError::Refused(
                -1,
                format!("correlation id {corr}, expected {id}"),
            ));
        }
        Ok(R::decode(&mut r, version)?)
    }

    /// Sends a request without waiting for its response (pipelining);
    /// returns its correlation id for [`Client::recv`].
    ///
    /// # Errors
    /// [`ClientError`].
    pub async fn send<Q: Message>(&mut self, version: i16, q: &Q) -> Result<i32, ClientError> {
        let id = self.next_id;
        self.next_id += 1;
        let header = RequestHeader {
            api_key: Q::API_KEY,
            api_version: version,
            correlation_id: id,
            client_id: Some(self.client_id.clone()),
            unknown_tagged_fields: Vec::new(),
        };
        let frame = request_frame(&header, |w| q.encode(w, version))?;
        self.stream.write_all(&frame).await?;
        Ok(id)
    }

    /// Reads the next response, which must answer correlation id `id` of
    /// API `R` at `version`.
    ///
    /// # Errors
    /// [`ClientError`].
    pub async fn recv<R: Message>(&mut self, version: i16, id: i32) -> Result<R, ClientError> {
        let resp = self.read_frame().await?;
        let (corr, mut r) = read_response_header(&resp, R::API_KEY, version)?;
        if corr != id {
            return Err(ClientError::Refused(
                -1,
                format!("correlation id {corr}, expected {id}"),
            ));
        }
        Ok(R::decode(&mut r, version)?)
    }

    /// Sends raw bytes (a hand-made request) and returns the raw response
    /// frame (after its size).
    ///
    /// # Errors
    /// [`ClientError`].
    pub async fn raw(&mut self, frame: &[u8]) -> Result<Vec<u8>, ClientError> {
        self.stream.write_all(frame).await?;
        self.read_frame().await
    }

    async fn read_frame(&mut self) -> Result<Vec<u8>, ClientError> {
        let mut size = [0u8; 4];
        self.stream.read_exact(&mut size).await?;
        let n = usize::try_from(i32::from_be_bytes(size))
            .map_err(|_| ClientError::Refused(-1, "size".into()))?;
        let mut buf = vec![0u8; n];
        self.stream.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn handshake(&mut self, mechanism: &str) -> Result<(), ClientError> {
        let r: SaslHandshakeResponse = self
            .call(
                1,
                &SaslHandshakeRequest {
                    mechanism: mechanism.into(),
                    ..Default::default()
                },
            )
            .await?;
        if r.error_code != 0 {
            return Err(ClientError::Refused(
                r.error_code,
                format!("mechanisms {:?}", r.mechanisms),
            ));
        }
        Ok(())
    }

    async fn authenticate(&mut self, bytes: Vec<u8>) -> Result<Vec<u8>, ClientError> {
        let r: SaslAuthenticateResponse = self
            .call(
                2,
                &SaslAuthenticateRequest {
                    auth_bytes: bytes,
                    ..Default::default()
                },
            )
            .await?;
        if r.error_code != 0 {
            return Err(ClientError::Refused(
                r.error_code,
                r.error_message.unwrap_or_default(),
            ));
        }
        Ok(r.auth_bytes)
    }

    /// Logs in with SASL PLAIN.
    ///
    /// # Errors
    /// [`ClientError::Refused`] with the server's code and message.
    pub async fn sasl_plain(&mut self, user: &str, password: &str) -> Result<(), ClientError> {
        self.handshake("PLAIN").await?;
        self.authenticate(format!("\0{user}\0{password}").into_bytes())
            .await?;
        Ok(())
    }

    /// Logs in with SCRAM.
    ///
    /// # Errors
    /// [`ClientError::Refused`]; also if the server's signature is wrong.
    pub async fn sasl_scram(
        &mut self,
        mech: ScramMechanism,
        user: &str,
        password: &str,
    ) -> Result<(), ClientError> {
        self.handshake(mech.name()).await?;
        let mut nonce = [0u8; 12];
        let _ = getrandom::fill(&mut nonce);
        let cnonce: String = nonce.iter().fold(String::new(), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        });
        let mut c = ScramClient::new(mech, user, password.as_bytes(), &cnonce);
        let sf = self.authenticate(c.first()).await?;
        let last = c.last(&sf).map_err(|e| ClientError::Refused(-1, e.0))?;
        let fin = self.authenticate(last).await?;
        if !c.verify(&fin) {
            return Err(ClientError::Refused(-1, "bad server signature".into()));
        }
        Ok(())
    }
}

/// `ConsumerProtocolSubscription` v0: the topics a member wants.
#[must_use]
pub fn subscription(topics: &[&str]) -> Vec<u8> {
    let mut w = Writer::new();
    w.i16(0);
    let _ = w.array(topics, false, |w, t| w.string(t, false));
    let _ = w.nullable_bytes(None, false);
    w.into_inner()
}

/// `ConsumerProtocolAssignment` v0: partitions per topic.
#[must_use]
pub fn assignment(parts: &[(&str, Vec<i32>)]) -> Vec<u8> {
    let mut w = Writer::new();
    w.i16(0);
    let _ = w.array(parts, false, |w, (t, ps)| {
        w.string(t, false)?;
        w.array(ps, false, |w, p| {
            w.i32(*p);
            Ok(())
        })
    });
    let _ = w.nullable_bytes(None, false);
    w.into_inner()
}

/// Decodes [`assignment`]'s bytes.
///
/// # Errors
/// A [`crate::wire::CodecError`].
pub fn parse_assignment(b: &[u8]) -> Result<Vec<(String, Vec<i32>)>, crate::wire::CodecError> {
    if b.is_empty() {
        return Ok(Vec::new());
    }
    let mut r = Reader::new(b);
    let _version = r.i16()?;
    r.array(false, |r| {
        Ok((r.string(false)?, r.array(false, Reader::i32)?))
    })
}

/// The API key constant of a request type, for callers building raw
/// frames.
pub const API_VERSIONS: i16 = api::API_VERSIONS;
