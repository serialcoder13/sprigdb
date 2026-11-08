//! Server auth plugins (chapter 05, "Authentication plugins"; build step 4).
//!
//! This module moves bytes; a [`LoginFlow`] (chapter 14's login decisions,
//! implemented by the gateway) decides. The plugins:
//!
//! - [`CachingSha2Password`] (both personas, the default): the empty
//!   password, the fast path (`AuthMoreData 0x03` then the verdict), and the
//!   full path (`AuthMoreData 0x04`, then the clear-text password over TLS).
//!   Without TLS the full path would need the server's RSA key; decision D4
//!   (`decisions/m0.2-interim.md`) refuses it with [`Outcome::NeedsTls`],
//!   whether the client asks for the key (`0x02`) or already has one.
//! - [`NativePassword`]: `mysql_native_password`, when the flow allows it.
//! - [`ClearPassword`]: `mysql_clear_password`, refused with 3159 without
//!   TLS.
//!
//! [`authenticate`] runs the exchange: it asks the flow which plugin the
//! login uses, switches to it with a fresh nonce when the client started
//! with another one (and always for `COM_CHANGE_USER`), then runs it to the
//! end. An unknown account takes exactly the same packets as a real one
//! that misses the fast cache: the flow fails it at the end, after the same
//! hash work (the decoy, chapter 14).
//!
//! The verdict ([`AuthVerdict`]) comes back to the caller, which sends the
//! OK packet (or the ERR) itself, since the OK carries session state.

use async_trait::async_trait;
use bytes::Bytes;
use zeroize::Zeroizing;

use crate::error::{ErrPacket, StdErr};
use crate::handshake::{NONCE_LEN, PacketIo, new_nonce};
use crate::packets::{AuthMoreData, AuthSwitchRequest, Persona};
use crate::proto_error::ProtoResult;

/// `caching_sha2_password`.
pub const CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";
/// `mysql_native_password`.
pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
/// `mysql_clear_password`.
pub const MYSQL_CLEAR_PASSWORD: &str = "mysql_clear_password";

/// `AuthMoreData` byte: the client asks for the server's RSA public key.
pub const REQUEST_PUBLIC_KEY: u8 = 0x02;
/// `AuthMoreData` byte: the fast path succeeded.
pub const FAST_AUTH_SUCCESS: u8 = 0x03;
/// `AuthMoreData` byte: send the password (full authentication).
pub const PERFORM_FULL_AUTHENTICATION: u8 = 0x04;

/// The scramble length of `caching_sha2_password` (SHA-256).
pub const SHA2_SCRAMBLE_LEN: usize = 32;

/// The mechanism a login uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    /// `caching_sha2_password`.
    CachingSha2,
    /// `mysql_native_password`.
    Native,
    /// `mysql_clear_password`.
    ClearText,
}

impl Method {
    /// The plugin name on the wire.
    #[must_use]
    pub fn plugin_name(self) -> &'static str {
        match self {
            Method::CachingSha2 => CACHING_SHA2_PASSWORD,
            Method::Native => MYSQL_NATIVE_PASSWORD,
            Method::ClearText => MYSQL_CLEAR_PASSWORD,
        }
    }

    /// The method a plugin name stands for, if the gateway implements it.
    #[must_use]
    pub fn from_plugin(name: &str) -> Option<Self> {
        match name {
            CACHING_SHA2_PASSWORD => Some(Method::CachingSha2),
            MYSQL_NATIVE_PASSWORD => Some(Method::Native),
            MYSQL_CLEAR_PASSWORD => Some(Method::ClearText),
            _ => None,
        }
    }

    /// The plugin that runs this method.
    #[must_use]
    pub fn plugin(self) -> &'static dyn ServerAuthPlugin {
        match self {
            Method::CachingSha2 => &CachingSha2Password,
            Method::Native => &NativePassword,
            Method::ClearText => &ClearPassword,
        }
    }
}

/// Which path an exchange took (the `path` label of
/// `sprig_mysql_auth_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthPath {
    /// The empty password.
    Empty,
    /// `caching_sha2_password` fast path.
    Fast,
    /// `caching_sha2_password` full path over TLS.
    Full,
    /// `caching_sha2_password` full path without TLS: the RSA exchange
    /// (refused until D4 is lifted).
    Rsa,
    /// `mysql_native_password`.
    Native,
    /// `mysql_clear_password`.
    Clear,
}

impl AuthPath {
    /// The metric label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AuthPath::Empty => "empty",
            AuthPath::Fast => "fast",
            AuthPath::Full => "full",
            AuthPath::Rsa => "rsa",
            AuthPath::Native => "native",
            AuthPath::Clear => "clear",
        }
    }
}

/// What the credential check found, before the login policy runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// The credential matched.
    Verified,
    /// The credential didn't match (or the account doesn't exist).
    WrongCredentials,
    /// The full path was needed without TLS, which needs the RSA exchange
    /// (decision D4: not supported yet; the client should use TLS).
    NeedsTls,
}

/// The end of an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthVerdict {
    /// Logged in; the caller sends the OK packet.
    Accept,
    /// Refused; the caller sends this ERR packet and closes.
    Refuse(ErrPacket),
}

/// One login conversation's decisions (chapter 14, "MySQL logins"): the
/// plugins call these and never learn whether the account exists.
///
/// Implementations must cost the same for an unknown account as for a real
/// one on every path: [`LoginFlow::check_password`] runs a decoy hash at
/// the default cost, and [`LoginFlow::method`] picks for an unknown account
/// what it picks for an account with only a `caching_sha2_password`
/// verifier.
#[async_trait]
pub trait LoginFlow: Send {
    /// The mechanism this login uses, given the plugin the client named in
    /// its response (`None` if it named none).
    fn method(&self, client_plugin: Option<&str>) -> Method;

    /// The `caching_sha2_password` fast path: does `scramble` match the
    /// account's fast-cache entry for `nonce`? `false` on a miss or a
    /// mismatch; the full path follows either way.
    fn check_fast(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool;

    /// `mysql_native_password`: does `scramble` (empty for the empty
    /// password) match the account's native verifier for `nonce`?
    fn check_native(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool;

    /// The full path: does the clear-text `password` match? CPU-heavy:
    /// implementations run it off the async runtime, and fill the fast
    /// cache on success.
    async fn check_password(&mut self, password: Zeroizing<Vec<u8>>) -> bool;

    /// The verdict: chapter 14's policy checks after the credential check,
    /// and the ERR packet (1045, 3118, ...) on refusal.
    async fn decide(&mut self, outcome: Outcome, path: AuthPath) -> AuthVerdict;
}

/// The connection an exchange runs on.
pub struct AuthIo<'a> {
    /// The packet transport.
    pub io: &'a mut dyn PacketIo,
    /// The listener's persona.
    pub persona: Persona,
    /// Whether the connection is TLS.
    pub tls: bool,
    /// The nonce the client's scramble answers; replaced by a fresh one on
    /// an `AuthSwitchRequest`.
    pub nonce: [u8; NONCE_LEN],
}

impl std::fmt::Debug for AuthIo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthIo")
            .field("persona", &self.persona)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

impl AuthIo<'_> {
    async fn more_data(&mut self, byte: u8) -> ProtoResult<()> {
        let p = AuthMoreData {
            data: Bytes::copy_from_slice(&[byte]),
        };
        self.io.write_packet(p.encode()).await
    }
}

/// The server side of one auth plugin (chapter 05, "Interfaces").
#[async_trait]
pub trait ServerAuthPlugin: Send + Sync {
    /// The plugin name.
    fn name(&self) -> &'static str;

    /// Runs the exchange from the client's first response `first` to the
    /// verdict. The caller sends the final OK or ERR packet.
    async fn run(
        &self,
        io: &mut AuthIo<'_>,
        first: Bytes,
        flow: &mut dyn LoginFlow,
    ) -> ProtoResult<AuthVerdict>;
}

/// Strips one trailing NUL: clear-text passwords arrive NUL-terminated.
fn password_of(packet: &[u8]) -> Zeroizing<Vec<u8>> {
    let p = packet.strip_suffix(&[0]).unwrap_or(packet);
    Zeroizing::new(p.to_vec())
}

/// `caching_sha2_password`.
#[derive(Debug, Clone, Copy, Default)]
pub struct CachingSha2Password;

#[async_trait]
impl ServerAuthPlugin for CachingSha2Password {
    fn name(&self) -> &'static str {
        CACHING_SHA2_PASSWORD
    }

    async fn run(
        &self,
        io: &mut AuthIo<'_>,
        first: Bytes,
        flow: &mut dyn LoginFlow,
    ) -> ProtoResult<AuthVerdict> {
        // Step 2: the empty password.
        if first.is_empty() {
            let ok = flow.check_password(Zeroizing::new(Vec::new())).await;
            return Ok(flow.decide(verified(ok), AuthPath::Empty).await);
        }
        // Step 3: the fast path. A miss or a mismatch falls through.
        if first.len() == SHA2_SCRAMBLE_LEN && flow.check_fast(&io.nonce, &first) {
            io.more_data(FAST_AUTH_SUCCESS).await?;
            return Ok(flow.decide(Outcome::Verified, AuthPath::Fast).await);
        }
        // Step 4: the full path.
        io.more_data(PERFORM_FULL_AUTHENTICATION).await?;
        let reply = io.io.read_packet().await?;
        if !io.tls {
            // The client asks for the public key (0x02) or sends an RSA
            // ciphertext: decision D4 refuses both.
            return Ok(flow.decide(Outcome::NeedsTls, AuthPath::Rsa).await);
        }
        let ok = flow.check_password(password_of(&reply)).await;
        Ok(flow.decide(verified(ok), AuthPath::Full).await)
    }
}

/// `mysql_native_password`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativePassword;

#[async_trait]
impl ServerAuthPlugin for NativePassword {
    fn name(&self) -> &'static str {
        MYSQL_NATIVE_PASSWORD
    }

    async fn run(
        &self,
        io: &mut AuthIo<'_>,
        first: Bytes,
        flow: &mut dyn LoginFlow,
    ) -> ProtoResult<AuthVerdict> {
        let ok = flow.check_native(&io.nonce, &first);
        let path = if first.is_empty() {
            AuthPath::Empty
        } else {
            AuthPath::Native
        };
        Ok(flow.decide(verified(ok), path).await)
    }
}

/// `mysql_clear_password`: accepted only over TLS.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClearPassword;

#[async_trait]
impl ServerAuthPlugin for ClearPassword {
    fn name(&self) -> &'static str {
        MYSQL_CLEAR_PASSWORD
    }

    async fn run(
        &self,
        io: &mut AuthIo<'_>,
        first: Bytes,
        flow: &mut dyn LoginFlow,
    ) -> ProtoResult<AuthVerdict> {
        if !io.tls {
            return Ok(AuthVerdict::Refuse(ErrPacket::std(
                io.persona,
                StdErr::SecureTransportRequired,
            )));
        }
        let ok = flow.check_password(password_of(&first)).await;
        Ok(flow.decide(verified(ok), AuthPath::Clear).await)
    }
}

fn verified(ok: bool) -> Outcome {
    if ok {
        Outcome::Verified
    } else {
        Outcome::WrongCredentials
    }
}

/// Runs a whole exchange: picks the method ([`LoginFlow::method`]),
/// switches the client to it with a fresh nonce when it started with
/// another plugin or when `force_switch` is set (`COM_CHANGE_USER`), then
/// runs the plugin.
///
/// `client_plugin` and `first` are the plugin name and auth response from
/// the handshake response (or `COM_CHANGE_USER`); `io.nonce` is the nonce
/// the client answered.
pub async fn authenticate(
    io: &mut AuthIo<'_>,
    client_plugin: Option<&str>,
    first: Bytes,
    force_switch: bool,
    flow: &mut dyn LoginFlow,
) -> ProtoResult<AuthVerdict> {
    let method = flow.method(client_plugin);
    let mut first = first;
    if force_switch || client_plugin != Some(method.plugin_name()) {
        let data = if method == Method::ClearText {
            Bytes::new()
        } else {
            io.nonce = new_nonce()?;
            let mut d = Vec::with_capacity(NONCE_LEN + 1);
            d.extend_from_slice(&io.nonce);
            d.push(0);
            Bytes::from(d)
        };
        let switch = AuthSwitchRequest {
            plugin: method.plugin_name().to_owned(),
            data,
        };
        io.io.write_packet(switch.encode()).await?;
        first = io.io.read_packet().await?;
    }
    method.plugin().run(io, first, flow).await
}

#[cfg(test)]
mod tests;
