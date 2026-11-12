//! A minimal Bolt client, for tests and the differential suite: handshake,
//! `HELLO`/`LOGON`, `RUN`/`PULL`, transactions. Not a driver: no routing,
//! no retries, no pooling.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::message::{BoltVersion, Dechunker, MAGIC, Request, Response, write_chunked};
use crate::packstream::PackValue;

/// A client error: I/O, a malformed reply or a `FAILURE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientError {
    /// The Neo4j code for a `FAILURE`; empty otherwise.
    pub code: String,
    /// The message.
    pub message: String,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ClientError {}

fn io_err(e: impl std::fmt::Display) -> ClientError {
    ClientError {
        code: String::new(),
        message: e.to_string(),
    }
}

/// A result: column names, records and the closing `SUCCESS`.
#[derive(Debug, Clone, PartialEq)]
pub struct Rows {
    /// Column names.
    pub fields: Vec<String>,
    /// The records.
    pub records: Vec<Vec<PackValue>>,
    /// The final `SUCCESS` metadata.
    pub summary: PackValue,
}

/// A Bolt connection.
pub struct BoltClient<S> {
    io: S,
    buf: BytesMut,
    dechunk: Dechunker,
    /// The negotiated version.
    pub version: BoltVersion,
    /// The `HELLO` answer.
    pub server: PackValue,
}

impl<S> std::fmt::Debug for BoltClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoltClient")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> BoltClient<S> {
    /// Handshakes offering `5.4` down to `5.0` (or `proposals`).
    ///
    /// # Errors
    /// I/O, or `00 00 00 00` (no common version).
    pub async fn handshake(
        mut io: S,
        proposals: Option<[[u8; 4]; 4]>,
    ) -> Result<Self, ClientError> {
        let p = proposals.unwrap_or([[0, 4, 4, 5], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]);
        let mut hs = MAGIC.to_vec();
        for x in p {
            hs.extend_from_slice(&x);
        }
        io.write_all(&hs).await.map_err(io_err)?;
        let mut v = [0u8; 4];
        io.read_exact(&mut v).await.map_err(io_err)?;
        if v == [0, 0, 0, 0] {
            return Err(io_err("no Bolt version in common"));
        }
        Ok(Self {
            io,
            buf: BytesMut::new(),
            dechunk: Dechunker::new(64 << 20),
            version: BoltVersion::new(v[3], v[2]),
            server: PackValue::Null,
        })
    }

    /// Sends one request.
    ///
    /// # Errors
    /// I/O.
    pub async fn send(&mut self, r: &Request) -> Result<(), ClientError> {
        let mut out = BytesMut::new();
        write_chunked(&r.encode(), &mut out);
        self.io.write_all(&out).await.map_err(io_err)?;
        self.io.flush().await.map_err(io_err)
    }

    /// Receives one response.
    ///
    /// # Errors
    /// I/O, a closed connection or a malformed reply.
    pub async fn recv(&mut self) -> Result<Response, ClientError> {
        loop {
            if let Some(m) = self.dechunk.next(&mut self.buf).map_err(io_err)? {
                return Response::decode(&m).map_err(io_err);
            }
            let n = self.io.read_buf(&mut self.buf).await.map_err(io_err)?;
            if n == 0 {
                return Err(io_err("connection closed"));
            }
        }
    }

    async fn expect_success(&mut self) -> Result<PackValue, ClientError> {
        match self.recv().await? {
            Response::Success(m) => Ok(m),
            Response::Failure { code, message } => Err(ClientError { code, message }),
            other => Err(io_err(format!("unexpected {other:?}"))),
        }
    }

    /// `HELLO` then (5.1+) `LOGON` with `basic` credentials.
    ///
    /// # Errors
    /// A refused login.
    pub async fn login(&mut self, user: &str, password: &str) -> Result<(), ClientError> {
        let auth = vec![
            ("scheme", PackValue::str("basic")),
            ("principal", PackValue::str(user)),
            ("credentials", PackValue::str(password)),
        ];
        let mut hello = vec![
            ("user_agent", PackValue::str("sprig-bolt-client/0.1")),
            (
                "bolt_agent",
                PackValue::map([("product", PackValue::str("sprig-bolt-client/0.1"))]),
            ),
        ];
        if self.version < BoltVersion::new(5, 1) {
            hello.extend(auth.clone());
        }
        self.send(&Request::Hello(PackValue::map(hello))).await?;
        self.server = self.expect_success().await?;
        if self.version >= BoltVersion::new(5, 1) {
            self.send(&Request::Logon(PackValue::map(auth))).await?;
            self.expect_success().await?;
        }
        Ok(())
    }

    /// `RUN` and `PULL {n: -1}`; on a failure, `RESET` so the connection
    /// can be used again.
    ///
    /// # Errors
    /// The server's `FAILURE`.
    pub async fn query(
        &mut self,
        q: &str,
        params: Vec<(&str, PackValue)>,
        extra: Vec<(&str, PackValue)>,
    ) -> Result<Rows, ClientError> {
        self.send(&Request::Run {
            query: q.to_owned(),
            params: PackValue::map(params),
            extra: PackValue::map(extra),
        })
        .await?;
        self.send(&Request::Pull(PackValue::map([("n", PackValue::Int(-1))])))
            .await?;
        let head = match self.recv().await? {
            Response::Success(m) => m,
            Response::Failure { code, message } => {
                // PULL is IGNORED.
                let _ = self.recv().await?;
                self.reset().await?;
                return Err(ClientError { code, message });
            }
            other => return Err(io_err(format!("unexpected {other:?}"))),
        };
        let fields = head
            .get("fields")
            .and_then(PackValue::as_list)
            .unwrap_or_default()
            .iter()
            .filter_map(|f| f.as_str().map(str::to_owned))
            .collect();
        let mut records = Vec::new();
        loop {
            match self.recv().await? {
                Response::Record(r) => records.push(r),
                Response::Success(summary) => {
                    return Ok(Rows {
                        fields,
                        records,
                        summary,
                    });
                }
                // A 5.7 failure decodes as `Failure`; `GqlFailure` is
                // only ever encoded.
                Response::Failure { code, message }
                | Response::GqlFailure { code, message, .. } => {
                    self.reset().await?;
                    return Err(ClientError { code, message });
                }
                Response::Ignored => return Err(io_err("PULL ignored")),
            }
        }
    }

    /// `BEGIN` with `extra`.
    ///
    /// # Errors
    /// The server's `FAILURE`.
    pub async fn begin(&mut self, extra: Vec<(&str, PackValue)>) -> Result<(), ClientError> {
        self.send(&Request::Begin(PackValue::map(extra))).await?;
        self.expect_success().await.map(drop)
    }

    /// `COMMIT`: the bookmark.
    ///
    /// # Errors
    /// The server's `FAILURE`.
    pub async fn commit(&mut self) -> Result<PackValue, ClientError> {
        self.send(&Request::Commit).await?;
        match self.expect_success().await {
            Ok(m) => Ok(m),
            Err(e) => {
                self.reset().await?;
                Err(e)
            }
        }
    }

    /// `ROLLBACK`.
    ///
    /// # Errors
    /// The server's `FAILURE`.
    pub async fn rollback(&mut self) -> Result<(), ClientError> {
        self.send(&Request::Rollback).await?;
        self.expect_success().await.map(drop)
    }

    /// `RESET`.
    ///
    /// # Errors
    /// The server's `FAILURE`.
    pub async fn reset(&mut self) -> Result<(), ClientError> {
        self.send(&Request::Reset).await?;
        self.expect_success().await.map(drop)
    }

    /// `ROUTE`: the routing table.
    ///
    /// # Errors
    /// The server's `FAILURE`.
    pub async fn route(&mut self, db: Option<&str>) -> Result<PackValue, ClientError> {
        let extra = match db {
            Some(d) => PackValue::map([("db", PackValue::str(d))]),
            None => PackValue::Map(vec![]),
        };
        self.send(&Request::Route {
            routing: PackValue::map([("address", PackValue::str("localhost"))]),
            bookmarks: PackValue::List(vec![]),
            extra,
        })
        .await?;
        self.expect_success().await
    }

    /// `GOODBYE`.
    ///
    /// # Errors
    /// I/O.
    pub async fn goodbye(mut self) -> Result<(), ClientError> {
        self.send(&Request::Goodbye).await?;
        self.io.shutdown().await.map_err(io_err)
    }
}
