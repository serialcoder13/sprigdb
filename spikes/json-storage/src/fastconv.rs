//! Streaming JSON (+ `_types`) -> BSON bytes, without an intermediate tree.
//! Measures how much of to_bson's cost is the serde_json::Value route (spike follow-up).

use anyhow::{anyhow, bail, Result};
use base64::Engine;
use std::collections::HashMap;

struct P<'a> {
    b: &'a [u8],
    i: usize,
    out: Vec<u8>,
    types: Option<HashMap<String, String>>,
    path: String,
}

impl<'a> P<'a> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\n' | b'\t' | b'\r') {
            self.i += 1;
        }
    }
    fn expect(&mut self, c: u8) -> Result<()> {
        self.ws();
        if self.b.get(self.i) != Some(&c) {
            bail!("expected {}", c as char);
        }
        self.i += 1;
        Ok(())
    }
    /// Parses a JSON string; returns its decoded text.
    fn string(&mut self) -> Result<std::borrow::Cow<'a, str>> {
        self.expect(b'"')?;
        let start = self.i;
        while self.i < self.b.len() {
            match self.b[self.i] {
                b'"' => {
                    let s = std::str::from_utf8(&self.b[start..self.i])?;
                    self.i += 1;
                    return Ok(s.into());
                }
                b'\\' => {
                    // slow path: let serde_json unescape the whole literal
                    let mut j = self.i;
                    while j < self.b.len() {
                        if self.b[j] == b'\\' {
                            j += 2;
                        } else if self.b[j] == b'"' {
                            break;
                        } else {
                            j += 1;
                        }
                    }
                    let s: String = serde_json::from_slice(&self.b[start - 1..=j])?;
                    self.i = j + 1;
                    return Ok(s.into());
                }
                _ => self.i += 1,
            }
        }
        bail!("unterminated string")
    }
    fn entry(&self) -> Option<&str> {
        self.types.as_ref().and_then(|t| t.get(&self.path)).map(|s| s.as_str())
    }
    fn key(&mut self, k: &str) {
        self.out.extend_from_slice(k.as_bytes());
        self.out.push(0);
    }
    fn push_path(&mut self, k: &str) -> usize {
        let base = self.path.len();
        if self.types.is_some() {
            if !self.path.is_empty() {
                self.path.push('.');
            }
            for c in k.chars() {
                if c == '.' || c == '\\' {
                    self.path.push('\\');
                }
                self.path.push(c);
            }
        }
        base
    }
    /// Writes one element (type byte, key, value).
    fn element(&mut self, k: &str) -> Result<()> {
        self.ws();
        let tpos = self.out.len();
        self.out.push(0);
        self.key(k);
        let c = *self.b.get(self.i).ok_or_else(|| anyhow!("eof"))?;
        let t: u8 = match c {
            b'{' => {
                self.object()?;
                3
            }
            b'[' => {
                self.array()?;
                4
            }
            b'"' => {
                let s = self.string()?;
                match self.entry() {
                    None => {
                        self.str_val(&s);
                        2
                    }
                    Some(e) => {
                        let e = e.to_string();
                        self.typed_string(&e, &s)?
                    }
                }
            }
            b't' | b'f' => {
                let v = c == b't';
                self.i += if v { 4 } else { 5 };
                self.out.push(v as u8);
                8
            }
            b'n' => {
                self.i += 4;
                if self.entry() == Some("undefined") {
                    6
                } else {
                    10
                }
            }
            _ => {
                let start = self.i;
                while self.i < self.b.len() && matches!(self.b[self.i], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E') {
                    self.i += 1;
                }
                let s = std::str::from_utf8(&self.b[start..self.i])?;
                let integral = !s.contains(['.', 'e', 'E']);
                match self.entry() {
                    Some("long") => {
                        self.out.extend_from_slice(&s.parse::<i64>()?.to_le_bytes());
                        18
                    }
                    Some("decimal") => {
                        self.out.extend_from_slice(&s.parse::<bson::Decimal128>()?.bytes());
                        19
                    }
                    Some("date") => {
                        self.out.extend_from_slice(&s.parse::<i64>()?.to_le_bytes());
                        9
                    }
                    _ if integral => match s.parse::<i64>() {
                        Ok(v) if i32::try_from(v).is_ok() => {
                            self.out.extend_from_slice(&(v as i32).to_le_bytes());
                            16
                        }
                        Ok(v) => {
                            self.out.extend_from_slice(&v.to_le_bytes());
                            18
                        }
                        Err(_) => {
                            self.out.extend_from_slice(&s.parse::<f64>()?.to_le_bytes());
                            1
                        }
                    },
                    _ => {
                        self.out.extend_from_slice(&s.parse::<f64>()?.to_le_bytes());
                        1
                    }
                }
            }
        };
        self.out[tpos] = t;
        Ok(())
    }
    fn str_val(&mut self, s: &str) {
        self.out.extend_from_slice(&(s.len() as i32 + 1).to_le_bytes());
        self.out.extend_from_slice(s.as_bytes());
        self.out.push(0);
    }
    fn typed_string(&mut self, e: &str, s: &str) -> Result<u8> {
        Ok(match e {
            "objectId" => {
                let mut b = [0u8; 12];
                hex::decode_to_slice(s, &mut b)?;
                self.out.extend_from_slice(&b);
                7
            }
            "date" => {
                let v = super::json::to_bson(format!("{{\"d\":\"{s}\"}}").as_bytes(), Some(b"{\"d\":\"date\"}"))?;
                let ms = v.get_datetime("d")?.timestamp_millis();
                self.out.extend_from_slice(&ms.to_le_bytes());
                9
            }
            e if e.starts_with("binData:") => {
                let st: u8 = e[8..].parse()?;
                let start = self.out.len();
                self.out.extend_from_slice(&[0, 0, 0, 0, st]);
                base64::engine::general_purpose::STANDARD.decode_vec(s, &mut self.out)?;
                let n = (self.out.len() - start - 5) as i32;
                self.out[start..start + 4].copy_from_slice(&n.to_le_bytes());
                5
            }
            other => bail!("fastconv: {other} not handled; caller falls back to json::to_bson"),
        })
    }
    fn object(&mut self) -> Result<()> {
        self.expect(b'{')?;
        let start = self.out.len();
        self.out.extend_from_slice(&[0; 4]);
        self.ws();
        if self.b.get(self.i) == Some(&b'}') {
            self.i += 1;
        } else {
            loop {
                let k = self.string()?;
                self.expect(b':')?;
                let base = self.push_path(&k);
                self.element(&k)?;
                self.path.truncate(base);
                self.ws();
                match self.b.get(self.i) {
                    Some(b',') => self.i += 1,
                    Some(b'}') => {
                        self.i += 1;
                        break;
                    }
                    _ => bail!("bad object"),
                }
            }
        }
        self.out.push(0);
        let n = (self.out.len() - start) as i32;
        self.out[start..start + 4].copy_from_slice(&n.to_le_bytes());
        Ok(())
    }
    fn array(&mut self) -> Result<()> {
        self.expect(b'[')?;
        let start = self.out.len();
        self.out.extend_from_slice(&[0; 4]);
        self.ws();
        if self.b.get(self.i) == Some(&b']') {
            self.i += 1;
        } else {
            let mut idx = 0usize;
            loop {
                let k = idx.to_string();
                let base = self.push_path(&k);
                self.element(&k)?;
                self.path.truncate(base);
                idx += 1;
                self.ws();
                match self.b.get(self.i) {
                    Some(b',') => self.i += 1,
                    Some(b']') => {
                        self.i += 1;
                        break;
                    }
                    _ => bail!("bad array"),
                }
            }
        }
        self.out.push(0);
        let n = (self.out.len() - start) as i32;
        self.out[start..start + 4].copy_from_slice(&n.to_le_bytes());
        Ok(())
    }
}

pub fn to_bson_fast(doc: &[u8], types: Option<&[u8]>) -> Result<bson::RawDocumentBuf> {
    let types = match types {
        Some(t) => Some(serde_json::from_slice::<HashMap<String, String>>(t)?),
        None => None,
    };
    let mut p = P { b: doc, i: 0, out: Vec::with_capacity(doc.len()), types, path: String::new() };
    p.object()?;
    Ok(bson::RawDocumentBuf::from_bytes(p.out)?)
}
