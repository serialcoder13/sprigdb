//! Prepared statements across shards (chapter 17, "Prepared statements"):
//! the client's binary parameters as the planner's literals, and rows the
//! gateway combined (text values) as binary protocol values.

use bytes::Bytes;
use sprig_proto_mysql::command::{BoundParam, ParamValue};
use sprig_proto_mysql::resultset::column_type as ct;
use sprig_proto_mysql::resultset::{DateTime, Time};
use sprig_proto_mysql::{ColumnDef, Value};
use sprig_router::ParamLit;

/// `UNSIGNED_FLAG` of a column definition.
const UNSIGNED: u16 = 0x20;

fn date_text(d: &DateTime, with_time: bool) -> String {
    if with_time {
        format!(
            "TIMESTAMP'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
            d.year, d.month, d.day, d.hour, d.minute, d.second, d.micro
        )
    } else {
        format!("DATE'{:04}-{:02}-{:02}'", d.year, d.month, d.day)
    }
}

/// A client's parameter as the planner's literal. Strings take the
/// session's `character_set_client`; `DECIMAL` text stays a number.
#[must_use]
pub(crate) fn param_lit(p: &BoundParam, charset: &str) -> ParamLit {
    let ParamValue::Value(v) = &p.value else {
        return ParamLit::Null;
    };
    let t = p.type_code();
    match v {
        Value::Null => ParamLit::Null,
        Value::Int(i) => ParamLit::Int(i128::from(*i)),
        Value::UInt(u) => ParamLit::Int(i128::from(*u)),
        Value::Float(f) => ParamLit::Double(f64::from(*f)),
        Value::Double(f) => ParamLit::Double(*f),
        Value::Bytes(b) => match t {
            ct::DECIMAL | ct::NEWDECIMAL
                if !b.is_empty()
                    && b.iter().all(|c| c.is_ascii_digit() || b"+-.eE".contains(c)) =>
            {
                ParamLit::Raw(String::from_utf8_lossy(b).into_owned())
            }
            ct::BLOB | ct::TINY_BLOB | ct::MEDIUM_BLOB | ct::LONG_BLOB | ct::GEOMETRY
                if charset == "binary" =>
            {
                ParamLit::Bytes(b.to_vec())
            }
            _ => ParamLit::Str {
                bytes: b.to_vec(),
                charset: charset.to_owned(),
            },
        },
        Value::DateTime(d) => ParamLit::Raw(date_text(d, t != ct::DATE)),
        Value::Time(tm) => ParamLit::Raw(format!(
            "TIME'{}{}:{:02}:{:02}.{:06}'",
            if tm.negative { "-" } else { "" },
            u64::from(tm.days) * 24 + u64::from(tm.hour),
            tm.minute,
            tm.second,
            tm.micro
        )),
    }
}

fn digits(s: &str) -> Option<u32> {
    s.parse().ok()
}

fn parse_datetime(s: &str) -> Option<DateTime> {
    let (date, time) = s.split_once(' ').unwrap_or((s, ""));
    let mut d = date.split('-');
    let mut out = DateTime {
        year: u16::try_from(digits(d.next()?)?).ok()?,
        month: u8::try_from(digits(d.next()?)?).ok()?,
        day: u8::try_from(digits(d.next()?)?).ok()?,
        ..DateTime::default()
    };
    if !time.is_empty() {
        let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
        let mut t = hms.split(':');
        out.hour = u8::try_from(digits(t.next()?)?).ok()?;
        out.minute = u8::try_from(digits(t.next()?)?).ok()?;
        out.second = u8::try_from(digits(t.next()?)?).ok()?;
        out.micro = micros(frac)?;
    }
    Some(out)
}

fn micros(frac: &str) -> Option<u32> {
    if frac.is_empty() {
        return Some(0);
    }
    let mut f = frac.to_owned();
    f.truncate(6);
    while f.len() < 6 {
        f.push('0');
    }
    digits(&f)
}

fn parse_time(s: &str) -> Option<Time> {
    let (negative, body) = s.strip_prefix('-').map_or((false, s), |b| (true, b));
    let (hms, frac) = body.split_once('.').unwrap_or((body, ""));
    let mut t = hms.split(':');
    let hours = digits(t.next()?)?;
    Some(Time {
        negative,
        days: hours / 24,
        hour: u8::try_from(hours % 24).ok()?,
        minute: u8::try_from(digits(t.next()?)?).ok()?,
        second: u8::try_from(digits(t.next()?)?).ok()?,
        micro: micros(frac)?,
    })
}

/// A text protocol value as the binary protocol value of `def`'s type
/// (rows the gateway combined, sent to a prepared statement's client).
#[must_use]
pub(crate) fn text_value(def: &ColumnDef, v: Option<&[u8]>) -> Value {
    let Some(b) = v else {
        return Value::Null;
    };
    let s = std::str::from_utf8(b).unwrap_or("");
    let unsigned = def.flags & UNSIGNED != 0;
    let parsed = match def.col_type {
        ct::TINY | ct::SHORT | ct::LONG | ct::INT24 | ct::LONGLONG | ct::YEAR => {
            if unsigned {
                s.parse::<u64>().ok().map(Value::UInt)
            } else {
                s.parse::<i64>().ok().map(Value::Int)
            }
        }
        ct::FLOAT => s.parse::<f32>().ok().map(Value::Float),
        ct::DOUBLE => s.parse::<f64>().ok().map(Value::Double),
        ct::DATE | ct::DATETIME | ct::TIMESTAMP | ct::NEWDATE | ct::DATETIME2 | ct::TIMESTAMP2 => {
            parse_datetime(s).map(Value::DateTime)
        }
        ct::TIME | ct::TIME2 => parse_time(s).map(Value::Time),
        _ => None,
    };
    parsed.unwrap_or_else(|| Value::Bytes(Bytes::copy_from_slice(b)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(t: u8, flags: u16) -> ColumnDef {
        ColumnDef {
            col_type: t,
            flags,
            ..ColumnDef::default()
        }
    }

    #[test]
    fn text_values_become_binary_values() {
        assert_eq!(text_value(&col(ct::LONG, 0), Some(b"-5")), Value::Int(-5));
        assert_eq!(
            text_value(&col(ct::LONGLONG, UNSIGNED), Some(b"18446744073709551615")),
            Value::UInt(u64::MAX)
        );
        assert_eq!(
            text_value(&col(ct::DATETIME, 0), Some(b"2026-01-02 03:04:05.5")),
            Value::DateTime(DateTime {
                year: 2026,
                month: 1,
                day: 2,
                hour: 3,
                minute: 4,
                second: 5,
                micro: 500_000
            })
        );
        assert_eq!(
            text_value(&col(ct::TIME, 0), Some(b"-26:00:01")),
            Value::Time(Time {
                negative: true,
                days: 1,
                hour: 2,
                minute: 0,
                second: 1,
                micro: 0
            })
        );
        assert_eq!(
            text_value(&col(ct::NEWDECIMAL, 0), Some(b"1.50")),
            Value::Bytes(Bytes::from_static(b"1.50"))
        );
        assert_eq!(text_value(&col(ct::LONG, 0), None), Value::Null);
    }
}
