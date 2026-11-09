//! CRC-32C (Castagnoli), the checksum of record batches v2.
//!
//! In-house, slice-by-8 with tables built at compile time: no CRC-32C crate
//! is in the tree (`crc32fast` is the IEEE polynomial), and this is the
//! whole of it.

const POLY: u32 = 0x82F6_3B78; // reflected 0x1EDC6F41

const fn tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut i = 0;
    while i < 256 {
        let mut s = 1;
        while s < 8 {
            let prev = t[s - 1][i];
            t[s][i] = (prev >> 8) ^ t[0][(prev & 0xff) as usize];
            s += 1;
        }
        i += 1;
    }
    t
}

static T: [[u32; 256]; 8] = tables();

/// The CRC-32C of `data`.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    update(0, data)
}

/// Continues a CRC-32C over more data (`crc` from an earlier call, or 0).
#[must_use]
pub fn update(crc: u32, data: &[u8]) -> u32 {
    let mut c = !crc;
    let mut chunks = data.chunks_exact(8);
    for ch in &mut chunks {
        let lo = c ^ u32::from_le_bytes([ch[0], ch[1], ch[2], ch[3]]);
        let hi = u32::from_le_bytes([ch[4], ch[5], ch[6], ch[7]]);
        c = T[7][(lo & 0xff) as usize]
            ^ T[6][((lo >> 8) & 0xff) as usize]
            ^ T[5][((lo >> 16) & 0xff) as usize]
            ^ T[4][(lo >> 24) as usize]
            ^ T[3][(hi & 0xff) as usize]
            ^ T[2][((hi >> 8) & 0xff) as usize]
            ^ T[1][((hi >> 16) & 0xff) as usize]
            ^ T[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
        c = (c >> 8) ^ T[0][((c ^ u32::from(b)) & 0xff) as usize];
    }
    !c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitwise(data: &[u8]) -> u32 {
        let mut c = !0u32;
        for &b in data {
            c ^= u32::from(b);
            for _ in 0..8 {
                c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            }
        }
        !c
    }

    #[test]
    fn known_vectors() {
        // The standard check value, and RFC 3720 (iSCSI) appendix B.4.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xffu8; 32]), 0x62A8_AB43);
        let inc: Vec<u8> = (0u8..32).collect();
        assert_eq!(crc32c(&inc), 0x46DD_794E);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn slice_by_8_matches_bitwise_and_chains() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 31 + 7) as u8).collect();
        for n in [0, 1, 7, 8, 9, 63, 64, 65, 999, 1000] {
            assert_eq!(crc32c(&data[..n]), bitwise(&data[..n]), "{n}");
        }
        let (a, b) = data.split_at(333);
        assert_eq!(update(crc32c(a), b), crc32c(&data));
    }
}
