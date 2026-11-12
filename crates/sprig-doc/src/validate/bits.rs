//! `$bitsAllSet`, `$bitsAnySet`, `$bitsAllClear` and `$bitsAnyClear`
//! (M3, decision S95): the bit positions of a mask given as a number, an
//! array of positions or BinData, tested against whole numbers (as 64-bit
//! two's complement, sign-extended past bit 63) and BinData (little-endian,
//! missing bytes clear). Other values never match.

use bson::Bson;
use sprig_proto_mongo::errors::{MongoError, code};

/// Which test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `$bitsAllSet`.
    AllSet,
    /// `$bitsAnySet`.
    AnySet,
    /// `$bitsAllClear`.
    AllClear,
    /// `$bitsAnyClear`.
    AnyClear,
}

/// A parsed `$bits*` condition.
#[derive(Debug, Clone)]
pub struct Bits {
    /// The test.
    pub kind: Kind,
    /// Bit positions, ascending.
    pub positions: Vec<u32>,
}

fn whole(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
        Bson::Double(f) if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f < i64::MAX as f64 => {
            Some(*f as i64)
        }
        Bson::Decimal128(d) => d.to_string().parse::<i64>().ok(),
        _ => None,
    }
}

impl Bits {
    /// Parses `op: arg`.
    ///
    /// # Errors
    /// 9 for a negative or fractional mask or position; 2 for another type.
    pub fn parse(op: &str, arg: &Bson) -> Result<Self, MongoError> {
        let kind = match op {
            "$bitsAllSet" => Kind::AllSet,
            "$bitsAnySet" => Kind::AnySet,
            "$bitsAllClear" => Kind::AllClear,
            _ => Kind::AnyClear,
        };
        let mut positions = Vec::new();
        match arg {
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                let Some(n) = whole(arg) else {
                    return Err(MongoError::new(
                        code::FAILED_TO_PARSE,
                        format!("Expected an integer: {op}: {arg}"),
                    ));
                };
                if n < 0 {
                    return Err(MongoError::new(
                        code::FAILED_TO_PARSE,
                        format!("Expected a positive number in: {op}: {arg}"),
                    ));
                }
                for b in 0..63u32 {
                    if (n >> b) & 1 == 1 {
                        positions.push(b);
                    }
                }
            }
            Bson::Array(a) => {
                for x in a {
                    let p = match x {
                        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                            whole(x)
                        }
                        _ => {
                            return Err(MongoError::new(
                                code::BAD_VALUE,
                                format!(
                                    "bit positions must be an integer but got: {}: {x}",
                                    positions.len()
                                ),
                            ));
                        }
                    };
                    let Some(p) = p else {
                        return Err(MongoError::new(
                            code::BAD_VALUE,
                            format!("bit positions must be an integer but got: {x}"),
                        ));
                    };
                    let Ok(p) = u32::try_from(p) else {
                        return Err(MongoError::new(
                            code::BAD_VALUE,
                            format!("bit positions must be >= 0 but got: {x}"),
                        ));
                    };
                    if !positions.contains(&p) {
                        positions.push(p);
                    }
                }
                positions.sort_unstable();
            }
            Bson::Binary(b) => {
                for (i, byte) in b.bytes.iter().enumerate() {
                    for bit in 0..8u32 {
                        if (byte >> bit) & 1 == 1 {
                            positions.push(u32::try_from(i).unwrap_or(u32::MAX / 8) * 8 + bit);
                        }
                    }
                }
            }
            other => {
                return Err(MongoError::new(
                    code::BAD_VALUE,
                    format!("{op} takes an Array, a number, or a BinData but received: {op}: {other}"),
                ));
            }
        }
        Ok(Self { kind, positions })
    }

    /// Whether `v` passes.
    #[must_use]
    pub fn matches(&self, v: &Bson) -> bool {
        let bit: Box<dyn Fn(u32) -> bool> = match v {
            Bson::Binary(b) => {
                let bytes = b.bytes.clone();
                Box::new(move |p| {
                    bytes
                        .get(p as usize / 8)
                        .is_some_and(|byte| (byte >> (p % 8)) & 1 == 1)
                })
            }
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => match whole(v) {
                Some(n) => Box::new(move |p| if p >= 63 { n < 0 } else { (n >> p) & 1 == 1 }),
                None => return false,
            },
            _ => return false,
        };
        let ps = &self.positions;
        match self.kind {
            Kind::AllSet => ps.iter().all(|p| bit(*p)),
            Kind::AnySet => ps.iter().any(|p| bit(*p)),
            Kind::AllClear => ps.iter().all(|p| !bit(*p)),
            Kind::AnyClear => ps.iter().any(|p| !bit(*p)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_and_positions() {
        let b = Bits::parse("$bitsAllSet", &Bson::Int32(0b101)).unwrap();
        assert!(b.matches(&Bson::Int32(0b1101)));
        assert!(!b.matches(&Bson::Int32(0b1001)));
        let b = Bits::parse("$bitsAnySet", &Bson::Array(vec![Bson::Int32(1), Bson::Int32(70)]))
            .unwrap();
        assert!(b.matches(&Bson::Int64(-1)));
        assert!(!b.matches(&Bson::Int64(1)));
        assert!(!b.matches(&Bson::String("x".into())));
        assert!(!b.matches(&Bson::Double(1.5)));
        assert!(Bits::parse("$bitsAllSet", &Bson::Int32(-1)).is_err());
    }
}
