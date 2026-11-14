//! Packet exchanges of each plugin against a scripted client and a mock
//! flow. The real verdicts (golden scrambles against MySQL-made hashes)
//! are tested with the gateway's flow over `sprig-auth`.

use std::future::Future;

use bytes::Bytes;
use tokio::io::DuplexStream;

use super::*;
use crate::handshake::RawIo;

#[derive(Default)]
struct Mock {
    method: Option<Method>,
    fast_hit: bool,
    native_ok: bool,
    password: Vec<u8>,
    fast_nonces: Vec<[u8; NONCE_LEN]>,
    passwords: Vec<Vec<u8>>,
    native_scrambles: Vec<Vec<u8>>,
    decided: Vec<(Outcome, AuthPath)>,
}

#[async_trait]
impl LoginFlow for Mock {
    fn method(&self, _client_plugin: Option<&str>) -> Method {
        self.method.unwrap_or(Method::CachingSha2)
    }
    fn check_fast(&mut self, nonce: &[u8; NONCE_LEN], _scramble: &[u8]) -> bool {
        self.fast_nonces.push(*nonce);
        self.fast_hit
    }
    fn check_native(&mut self, _nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool {
        self.native_scrambles.push(scramble.to_vec());
        self.native_ok
    }
    async fn check_password(&mut self, password: Zeroizing<Vec<u8>>) -> bool {
        self.passwords.push(password.to_vec());
        *password == self.password
    }
    async fn decide(&mut self, outcome: Outcome, path: AuthPath) -> AuthVerdict {
        self.decided.push((outcome, path));
        match outcome {
            Outcome::Verified => AuthVerdict::Accept,
            _ => AuthVerdict::Refuse(ErrPacket::access_denied(
                Persona::MySql84,
                "app",
                "127.0.0.1",
                true,
            )),
        }
    }
}

const NONCE: [u8; NONCE_LEN] = [b'n'; NONCE_LEN];

/// Runs `authenticate` on the server half (next sequence id 2, as after a
/// plaintext handshake response) while `client` drives the other half.
async fn exchange<C, F>(
    flow: &mut Mock,
    tls: bool,
    plugin: Option<&str>,
    first: &'static [u8],
    force_switch: bool,
    client: C,
) -> (AuthVerdict, [u8; NONCE_LEN])
where
    C: FnOnce(DuplexStream) -> F,
    F: Future<Output = ()>,
{
    let (c, mut s) = tokio::io::duplex(1 << 16);
    let mut sio = RawIo::new(&mut s, 2);
    let mut io = AuthIo {
        io: &mut sio,
        persona: Persona::MySql84,
        tls,
        nonce: NONCE,
    };
    let server = async {
        let v = authenticate(
            &mut io,
            plugin,
            Bytes::from_static(first),
            force_switch,
            flow,
        )
        .await
        .expect("exchange");
        (v, io.nonce)
    };
    let (v, ()) = tokio::join!(server, client(c));
    v
}

#[tokio::test]
async fn fast_path_sends_fast_auth_success() {
    let mut flow = Mock {
        fast_hit: true,
        ..Mock::default()
    };
    let (v, _) = exchange(
        &mut flow,
        false,
        Some(CACHING_SHA2_PASSWORD),
        &[1; 32],
        false,
        |mut d| async move {
            let mut c = RawIo::new(&mut d, 2);
            assert_eq!(c.read_packet().await.expect("read"), &[0x01, 0x03][..]);
        },
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_eq!(flow.decided, [(Outcome::Verified, AuthPath::Fast)]);
    assert_eq!(flow.fast_nonces, [NONCE]);
}

#[tokio::test]
async fn full_path_over_tls_reads_the_clear_password() {
    let mut flow = Mock {
        password: b"secret".to_vec(),
        ..Mock::default()
    };
    let (v, _) = exchange(
        &mut flow,
        true,
        Some(CACHING_SHA2_PASSWORD),
        &[1; 32],
        false,
        |mut d| async move {
            let mut c = RawIo::new(&mut d, 2);
            assert_eq!(c.read_packet().await.expect("read"), &[0x01, 0x04][..]);
            c.write_packet(Bytes::from_static(b"secret\0"))
                .await
                .expect("write");
        },
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_eq!(flow.passwords, [b"secret".to_vec()]);
    assert_eq!(flow.decided, [(Outcome::Verified, AuthPath::Full)]);
}

#[tokio::test]
async fn wrong_password_is_refused_with_the_flows_error() {
    let mut flow = Mock {
        password: b"secret".to_vec(),
        ..Mock::default()
    };
    let (v, _) = exchange(
        &mut flow,
        true,
        Some(CACHING_SHA2_PASSWORD),
        &[1; 32],
        false,
        |mut d| async move {
            let mut c = RawIo::new(&mut d, 2);
            let _ = c.read_packet().await.expect("read");
            c.write_packet(Bytes::from_static(b"guess\0"))
                .await
                .expect("write");
        },
    )
    .await;
    assert!(matches!(v, AuthVerdict::Refuse(e) if e.code == 1045));
    assert_eq!(flow.decided, [(Outcome::WrongCredentials, AuthPath::Full)]);
}

#[tokio::test]
async fn rsa_path_is_refused_without_hashing() {
    // No RSA: the client asks for the public key over plaintext.
    let mut flow = Mock::default();
    let (v, _) = exchange(
        &mut flow,
        false,
        Some(CACHING_SHA2_PASSWORD),
        &[1; 32],
        false,
        |mut d| async move {
            let mut c = RawIo::new(&mut d, 2);
            assert_eq!(c.read_packet().await.expect("read"), &[0x01, 0x04][..]);
            c.write_packet(Bytes::from_static(&[REQUEST_PUBLIC_KEY]))
                .await
                .expect("write");
        },
    )
    .await;
    assert!(matches!(v, AuthVerdict::Refuse(_)));
    assert!(flow.passwords.is_empty());
    assert_eq!(flow.decided, [(Outcome::NeedsTls, AuthPath::Rsa)]);
}

#[tokio::test]
async fn empty_password_needs_no_exchange() {
    let mut flow = Mock::default();
    let (v, _) = exchange(
        &mut flow,
        false,
        Some(CACHING_SHA2_PASSWORD),
        b"",
        false,
        |_d| async {},
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_eq!(flow.passwords, [Vec::<u8>::new()]);
    assert_eq!(flow.decided, [(Outcome::Verified, AuthPath::Empty)]);
}

/// libmysqlclient's `caching_sha2_password` (the `mysql` 8.x/9.x CLI)
/// sends one NUL byte for the empty password and then waits for OK or ERR:
/// an `AuthMoreData` there is "2027 Malformed packet" (the loginerr bug).
#[tokio::test]
async fn a_single_nul_is_the_empty_password() {
    for (password, want) in [
        (&b""[..], AuthVerdict::Accept),
        (
            b"pw",
            AuthVerdict::Refuse(ErrPacket::access_denied(
                Persona::MySql84,
                "app",
                "127.0.0.1",
                true,
            )),
        ),
    ] {
        let mut flow = Mock {
            password: password.to_vec(),
            ..Mock::default()
        };
        let (v, _) = exchange(
            &mut flow,
            false,
            Some(CACHING_SHA2_PASSWORD),
            b"\0",
            false,
            |mut d| async move {
                // Nothing is written before the caller's OK or ERR.
                let mut b = [0u8; 1];
                let r = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    tokio::io::AsyncReadExt::read(&mut d, &mut b),
                )
                .await;
                assert!(r.is_err() || matches!(r, Ok(Ok(0))), "unexpected bytes");
            },
        )
        .await;
        assert_eq!(v, want);
        assert_eq!(flow.passwords, [Vec::<u8>::new()]);
        assert!(flow.fast_nonces.is_empty());
        let outcome = if password.is_empty() {
            Outcome::Verified
        } else {
            Outcome::WrongCredentials
        };
        assert_eq!(flow.decided, [(outcome, AuthPath::Empty)]);
    }
}

/// After a switch to `mysql_native_password`, a NUL is still the empty
/// password: the flow sees an empty scramble.
#[tokio::test]
async fn native_reads_a_single_nul_as_empty() {
    let mut flow = Mock {
        method: Some(Method::Native),
        native_ok: true,
        ..Mock::default()
    };
    let (v, _) = exchange(
        &mut flow,
        false,
        Some(MYSQL_NATIVE_PASSWORD),
        b"\0",
        false,
        |_d| async {},
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_eq!(flow.native_scrambles, [Vec::<u8>::new()]);
    assert_eq!(flow.decided, [(Outcome::Verified, AuthPath::Empty)]);
}

/// `mysql --default-auth=mysql_native_password` (libmysqlclient) sends an
/// empty placeholder naming `mysql_native_password`, since the greeting's
/// nonce belongs to `caching_sha2_password`, and waits for a nonce. MySQL
/// 8.4 answers with an `AuthSwitchRequest`; taking the placeholder as the
/// empty password refused right passwords and let wrong ones into
/// empty-password accounts.
#[tokio::test]
async fn an_empty_native_placeholder_gets_a_nonce() {
    let mut flow = Mock {
        method: Some(Method::Native),
        native_ok: true,
        ..Mock::default()
    };
    let (v, nonce) = exchange(
        &mut flow,
        false,
        Some(MYSQL_NATIVE_PASSWORD),
        b"",
        false,
        |mut d| async move {
            let mut c = RawIo::new(&mut d, 2);
            let p = c.read_packet().await.expect("switch");
            let sw = AuthSwitchRequest::decode(&p).expect("decode");
            assert_eq!(sw.plugin, MYSQL_NATIVE_PASSWORD);
            assert_eq!(sw.data.len(), NONCE_LEN + 1);
            c.write_packet(Bytes::from_static(&[7; 20]))
                .await
                .expect("write");
        },
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_ne!(nonce, NONCE);
    assert_eq!(flow.native_scrambles, [vec![7u8; 20]]);
    assert_eq!(flow.decided, [(Outcome::Verified, AuthPath::Native)]);
}

#[tokio::test]
async fn another_client_plugin_gets_a_switch_with_a_fresh_nonce() {
    let mut flow = Mock {
        fast_hit: true,
        ..Mock::default()
    };
    let (v, nonce) = exchange(
        &mut flow,
        false,
        Some(MYSQL_NATIVE_PASSWORD),
        &[1; 20],
        false,
        |mut d| async move {
            let mut c = RawIo::new(&mut d, 2);
            let p = c.read_packet().await.expect("switch");
            let sw = AuthSwitchRequest::decode(&p).expect("decode");
            assert_eq!(sw.plugin, CACHING_SHA2_PASSWORD);
            assert_eq!(sw.data.len(), NONCE_LEN + 1);
            assert_eq!(sw.data.last(), Some(&0));
            assert_ne!(&sw.data[..NONCE_LEN], &NONCE);
            c.write_packet(Bytes::from_static(&[2; 32]))
                .await
                .expect("write");
            assert_eq!(c.read_packet().await.expect("read"), &[0x01, 0x03][..]);
        },
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_ne!(nonce, NONCE);
    // The fast path checked the scramble against the fresh nonce.
    assert_eq!(flow.fast_nonces, [nonce]);
}

#[tokio::test]
async fn change_user_always_switches() {
    let mut flow = Mock {
        fast_hit: true,
        ..Mock::default()
    };
    let (v, nonce) = exchange(
        &mut flow,
        false,
        Some(CACHING_SHA2_PASSWORD),
        &[1; 32],
        true,
        |mut d| async move {
            let mut c = RawIo::new(&mut d, 2);
            let p = c.read_packet().await.expect("switch");
            assert_eq!(p[0], 0xFE);
            c.write_packet(Bytes::from_static(&[2; 32]))
                .await
                .expect("write");
            let _ = c.read_packet().await.expect("read");
        },
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_ne!(nonce, NONCE);
}

#[tokio::test]
async fn native_when_the_flow_allows_it() {
    let mut flow = Mock {
        method: Some(Method::Native),
        native_ok: true,
        ..Mock::default()
    };
    let (v, _) = exchange(
        &mut flow,
        false,
        Some(MYSQL_NATIVE_PASSWORD),
        &[1; 20],
        false,
        |_d| async {},
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_eq!(flow.decided, [(Outcome::Verified, AuthPath::Native)]);
}

#[tokio::test]
async fn clear_password_needs_tls() {
    let mut flow = Mock {
        method: Some(Method::ClearText),
        password: b"pw".to_vec(),
        ..Mock::default()
    };
    let (v, _) = exchange(
        &mut flow,
        false,
        Some(MYSQL_CLEAR_PASSWORD),
        b"pw\0",
        false,
        |_d| async {},
    )
    .await;
    assert!(matches!(v, AuthVerdict::Refuse(e) if e.code == 3159));
    assert!(flow.passwords.is_empty());

    let (v, _) = exchange(
        &mut flow,
        true,
        Some(MYSQL_CLEAR_PASSWORD),
        b"pw\0",
        false,
        |_d| async {},
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
    assert_eq!(flow.decided, [(Outcome::Verified, AuthPath::Clear)]);

    // A switch to clear text carries no nonce.
    let (v, _) = exchange(
        &mut flow,
        true,
        Some(CACHING_SHA2_PASSWORD),
        &[1; 32],
        false,
        |mut d| async move {
            let mut c = RawIo::new(&mut d, 2);
            let sw =
                AuthSwitchRequest::decode(&c.read_packet().await.expect("read")).expect("decode");
            assert_eq!(sw.plugin, MYSQL_CLEAR_PASSWORD);
            assert!(sw.data.is_empty());
            c.write_packet(Bytes::from_static(b"pw\0"))
                .await
                .expect("write");
        },
    )
    .await;
    assert_eq!(v, AuthVerdict::Accept);
}

#[test]
fn plugin_names() {
    for m in [Method::CachingSha2, Method::Native, Method::ClearText] {
        assert_eq!(Method::from_plugin(m.plugin_name()), Some(m));
        assert_eq!(m.plugin().name(), m.plugin_name());
    }
    assert_eq!(Method::from_plugin("client_ed25519"), None);
}
