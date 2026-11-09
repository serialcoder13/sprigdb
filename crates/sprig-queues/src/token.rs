//! Delivery tokens (chapter 50, "Acknowledging inside a database
//! transaction"): every delivery carries `x-sprig-delivery`, which a
//! consumer passes to `sprig.mq_ack` inside its own transaction.
//!
//! A token is `q1.` and base64url of `(tenant, database, queue, lane,
//! class, seq, msg_id, owner epoch)` with the first 16 bytes of an HMAC-SHA256 under the
//! tenant's queue key (`mq_keys` in the tenant catalog, so every gateway
//! checks every other gateway's tokens). A forged or altered token is
//! 39259.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::hmac;
use sprig_proto_amqp::message::DeliveryId;
use subtle::ConstantTimeEq;

use crate::store::MsgId;

const PREFIX: &str = "q1.";
const BODY: usize = 8 + 4 + 8 + 2 + 1 + 8 + 16 + 8;
const MAC: usize = 16;

/// What a token names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenData {
    /// The tenant.
    pub tenant: u64,
    /// The vhost's database.
    pub db: u32,
    /// The delivery.
    pub id: DeliveryId,
    /// The message id (the delete checks it).
    pub msg_id: MsgId,
    /// The owner epoch it was delivered under.
    pub epoch: u64,
}

/// A tenant's token key.
pub struct TokenKey {
    key: hmac::Key,
}

impl std::fmt::Debug for TokenKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenKey").finish_non_exhaustive()
    }
}

impl TokenKey {
    /// A key from secret bytes.
    #[must_use]
    pub fn new(secret: &[u8]) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, secret),
        }
    }

    /// A token for a delivery.
    #[must_use]
    pub fn mint(
        &self,
        tenant: u64,
        db: u32,
        id: &DeliveryId,
        msg_id: &MsgId,
        epoch: u64,
    ) -> String {
        let mut raw = Vec::with_capacity(BODY + MAC);
        raw.extend_from_slice(&tenant.to_be_bytes());
        raw.extend_from_slice(&db.to_be_bytes());
        raw.extend_from_slice(&id.queue.to_be_bytes());
        raw.extend_from_slice(&id.lane.to_be_bytes());
        raw.push(id.pclass);
        raw.extend_from_slice(&id.seq.to_be_bytes());
        raw.extend_from_slice(msg_id);
        raw.extend_from_slice(&epoch.to_be_bytes());
        let tag = hmac::sign(&self.key, &raw);
        raw.extend_from_slice(&tag.as_ref()[..MAC]);
        format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(raw))
    }

    /// Checks a token; `None` when malformed or forged.
    #[must_use]
    pub fn verify(&self, token: &str) -> Option<TokenData> {
        let raw = URL_SAFE_NO_PAD
            .decode(token.trim().strip_prefix(PREFIX)?)
            .ok()?;
        if raw.len() != BODY + MAC {
            return None;
        }
        let (body, mac) = raw.split_at(BODY);
        let full = hmac::sign(&self.key, body);
        if !bool::from(full.as_ref()[..MAC].ct_eq(mac)) {
            return None;
        }
        let u64_at = |i: usize| {
            let mut a = [0u8; 8];
            a.copy_from_slice(&body[i..i + 8]);
            u64::from_be_bytes(a)
        };
        let mut msg_id = [0u8; 16];
        msg_id.copy_from_slice(&body[31..47]);
        Some(TokenData {
            tenant: u64_at(0),
            db: u32::from_be_bytes([body[8], body[9], body[10], body[11]]),
            id: DeliveryId {
                queue: u64_at(12),
                lane: u16::from_be_bytes([body[20], body[21]]),
                pclass: body[22],
                seq: u64_at(23),
            },
            msg_id,
            epoch: u64_at(47),
        })
    }
}

/// The tenant a token names, without checking it (to find the key).
#[must_use]
pub fn tenant_of(token: &str) -> Option<u64> {
    let raw = URL_SAFE_NO_PAD
        .decode(token.trim().strip_prefix(PREFIX)?)
        .ok()?;
    let mut a = [0u8; 8];
    a.copy_from_slice(raw.get(..8)?);
    Some(u64::from_be_bytes(a))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_forgery() {
        let k = TokenKey::new(b"secret-of-tenant-42");
        let id = DeliveryId {
            queue: 77,
            lane: 0,
            pclass: 3,
            seq: 88_412,
        };
        let t = k.mint(42, 3, &id, &[7; 16], 5);
        let d = k.verify(&t).unwrap_or_else(|| unreachable!());
        assert_eq!(
            (d.tenant, d.db, d.id, d.msg_id, d.epoch),
            (42, 3, id, [7; 16], 5)
        );
        assert_eq!(tenant_of(&t), Some(42));
        assert!(TokenKey::new(b"other").verify(&t).is_none());
        let mut bad = t.into_bytes();
        let i = bad.len() - 5;
        bad[i] = if bad[i] == b'A' { b'B' } else { b'A' };
        assert!(
            k.verify(&String::from_utf8(bad).unwrap_or_default())
                .is_none()
        );
    }
}
