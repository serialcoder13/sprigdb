//! CRC-32C (Castagnoli), the `OP_MSG` checksum: reflected polynomial
//! `0x82F63B78`, initial value and final XOR `0xFFFFFFFF`.

const POLY: u32 = 0x82F6_3B78;

const fn table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        #[allow(clippy::cast_possible_truncation)]
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

static TABLE: [u32; 256] = table();

/// The CRC-32C of `data`.
#[must_use]
pub fn checksum(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for b in data {
        c = TABLE[((c ^ u32::from(*b)) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

#[cfg(test)]
mod tests {
    #[test]
    fn check_value() {
        // The standard check value (RFC 3720, appendix B.4).
        assert_eq!(super::checksum(b"123456789"), 0xE306_9283);
        assert_eq!(super::checksum(&[0u8; 32]), 0x8A91_36AA);
    }
}
