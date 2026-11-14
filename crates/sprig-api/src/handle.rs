//! Transaction handles.
//!
//! A handle is `transactions/<h>`, where `h` is the base64url (no padding)
//! of a small protobuf: the owner's DC and gateway id, the session it names
//! on that gateway, and 128 random bits. The random part makes handles
//! unguessable; the owner's registry still checks the principal on every
//! call (39783).

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use prost::Message;
use sprig_common::{SprigCode, SprigError};

/// The prefix of every handle name.
pub const PREFIX: &str = "transactions/";

#[derive(Clone, PartialEq, ::prost::Message)]
struct Wire {
    #[prost(string, tag = "1")]
    dc: String,
    #[prost(string, tag = "2")]
    gateway_id: String,
    #[prost(uint64, tag = "3")]
    session: u64,
    #[prost(bytes = "vec", tag = "4")]
    nonce: Vec<u8>,
}

/// A decoded transaction handle.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TxnHandle {
    /// The owner's data center.
    pub dc: String,
    /// The owner gateway.
    pub gateway_id: String,
    /// The handle's number on its owner.
    pub session: u64,
    /// 128 random bits.
    pub nonce: [u8; 16],
}

impl TxnHandle {
    /// A new handle owned by `gateway_id` in `dc`, with fresh random bits.
    ///
    /// # Errors
    /// `Internal` if the operating system has no randomness.
    pub fn new(dc: &str, gateway_id: &str, session: u64) -> Result<Self, SprigError> {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce)
            .map_err(|e| SprigError::internal(format!("no randomness for a handle: {e}")))?;
        Ok(Self {
            dc: dc.to_owned(),
            gateway_id: gateway_id.to_owned(),
            session,
            nonce,
        })
    }

    /// `transactions/<h>`.
    #[must_use]
    pub fn name(&self) -> String {
        let w = Wire {
            dc: self.dc.clone(),
            gateway_id: self.gateway_id.clone(),
            session: self.session,
            nonce: self.nonce.to_vec(),
        };
        format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(w.encode_to_vec()))
    }

    /// Parses `transactions/<h>`.
    ///
    /// # Errors
    /// 39781 for anything that isn't a handle this build issues.
    pub fn parse(name: &str) -> Result<Self, SprigError> {
        let unknown = || {
            SprigError::new(
                SprigCode::TxnHandleUnknown,
                format!("{name:?} is not a transaction handle"),
            )
        };
        let h = name.strip_prefix(PREFIX).ok_or_else(unknown)?;
        let bytes = URL_SAFE_NO_PAD.decode(h).map_err(|_| unknown())?;
        let w = Wire::decode(bytes.as_slice()).map_err(|_| unknown())?;
        let nonce: [u8; 16] = w.nonce.as_slice().try_into().map_err(|_| unknown())?;
        if w.gateway_id.is_empty() {
            return Err(unknown());
        }
        Ok(Self {
            dc: w.dc,
            gateway_id: w.gateway_id,
            session: w.session,
            nonce,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_round_trip_and_differ() {
        let a = TxnHandle::new("dc-a", "gw-1", 7).unwrap();
        let b = TxnHandle::new("dc-a", "gw-1", 7).unwrap();
        assert_ne!(a.name(), b.name(), "random bits differ");
        let back = TxnHandle::parse(&a.name()).unwrap();
        assert_eq!(back, a);
        assert!(a.name().starts_with(PREFIX));
        assert!(!a.name()[PREFIX.len()..].contains(['/', '+', '=']));
    }

    #[test]
    fn junk_is_unknown() {
        for bad in [
            "",
            "transactions/",
            "transactions/!!",
            "tx/abc",
            "transactions/AAAA",
        ] {
            let e = TxnHandle::parse(bad).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::TxnHandleUnknown), "{bad}");
        }
    }
}
