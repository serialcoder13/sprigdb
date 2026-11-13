//! Logins (chapter 14, "MySQL logins"; chapter 05 step 4): the gateway
//! hands chapter 05's auth plugins a [`sprig_auth::MysqlLogin`] from the
//! process's [`LoginService`], for the handshake and for `COM_CHANGE_USER`.
//!
//! The service's source is the configuration switch (`[gateway]
//! login_mode`): `catalog` resolves the tenant from the login name (or
//! SNI on a tenant endpoint) and reads the account from the tenant's
//! cached catalog, with chapter 14's policy order and sandbox mode;
//! `accounts_file` is M0.2's interim file (decision D1), for raw clusters.
//! Both run the same decoy for unknown accounts, the same fast cache and
//! the same hashing slots.
//!
//! This module builds the [`LoginRequest`] from what the handshake learned:
//! the login name as sent, the TLS SNI, whether the client presented a
//! certificate the listener verified, and the client's
//! `CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS`.

use std::net::SocketAddr;

use sprig_auth::LoginRequest;
pub use sprig_auth::LoginService;
use sprig_proto_mysql::{Caps, Persona};

use crate::frontend::TlsInfo;

/// What every login shares (the login source, the fast cache, the hashing
/// slots): chapter 14's [`LoginService`].
pub type LoginDeps = LoginService;

/// The request for a login of `user` (as sent) from `peer`.
#[must_use]
pub fn request(
    persona: Persona,
    user: &[u8],
    peer: SocketAddr,
    tls: Option<&TlsInfo>,
    client_caps: Caps,
) -> LoginRequest {
    LoginRequest {
        persona,
        login_name: user.to_vec(),
        sni: tls.and_then(|t| t.sni.clone()),
        client_ip: peer.ip(),
        tls: tls.is_some(),
        client_cert: tls.is_some_and(|t| t.client_cert),
        client_dn: tls.and_then(|t| t.client_dn.clone()),
        can_handle_expired_password: client_caps.contains(Caps::CAN_HANDLE_EXPIRED_PASSWORDS),
    }
}

#[cfg(test)]
mod tests;
