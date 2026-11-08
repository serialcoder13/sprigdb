//! MongoDB wire framing: OP_MSG (2013) for everything, OP_QUERY (2004) only for the
//! legacy isMaster handshake that old drivers (Java 3.8, used by YCSB 0.17) still send.

use anyhow::{bail, Context, Result};
use bson::RawDocumentBuf;
use tokio::io::{AsyncRead, AsyncReadExt};

pub const OP_REPLY: i32 = 1;
pub const OP_QUERY: i32 = 2004;
pub const OP_MSG: i32 = 2013;

pub enum Msg {
    Query { request_id: i32, query: RawDocumentBuf },
    OpMsg { request_id: i32, body: RawDocumentBuf, seqs: Vec<(String, Vec<RawDocumentBuf>)>, more_to_come: bool },
}

fn le_i32(b: &[u8]) -> i32 {
    i32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

fn cstring(b: &[u8]) -> Result<(&str, usize)> {
    let end = b.iter().position(|&c| c == 0).context("unterminated cstring")?;
    Ok((std::str::from_utf8(&b[..end])?, end + 1))
}

fn doc_at(b: &[u8]) -> Result<(RawDocumentBuf, usize)> {
    if b.len() < 5 {
        bail!("short document");
    }
    let len = le_i32(b) as usize;
    if len > b.len() {
        bail!("document overruns message");
    }
    Ok((RawDocumentBuf::from_bytes(b[..len].to_vec())?, len))
}

pub async fn read_msg<R: AsyncRead + Unpin>(r: &mut R, buf: &mut Vec<u8>) -> Result<Option<Msg>> {
    let mut hdr = [0u8; 16];
    match r.read_exact(&mut hdr).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = le_i32(&hdr[0..]) as usize;
    if !(16..=48_000_000).contains(&len) {
        bail!("bad message length {len}");
    }
    let request_id = le_i32(&hdr[4..]);
    let op = le_i32(&hdr[12..]);
    buf.clear();
    buf.resize(len - 16, 0);
    r.read_exact(buf).await?;
    let b = &buf[..];
    match op {
        OP_MSG => {
            let flags = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            let end = if flags & 1 != 0 { b.len() - 4 } else { b.len() };
            let mut i = 4;
            let mut body = None;
            let mut seqs = Vec::new();
            while i < end {
                let kind = b[i];
                i += 1;
                match kind {
                    0 => {
                        let (d, n) = doc_at(&b[i..end])?;
                        body = Some(d);
                        i += n;
                    }
                    1 => {
                        let size = le_i32(&b[i..]) as usize;
                        let sec_end = i + size;
                        let (ident, n) = cstring(&b[i + 4..sec_end])?;
                        let ident = ident.to_string();
                        let mut j = i + 4 + n;
                        let mut docs = Vec::new();
                        while j < sec_end {
                            let (d, n) = doc_at(&b[j..sec_end])?;
                            docs.push(d);
                            j += n;
                        }
                        seqs.push((ident, docs));
                        i = sec_end;
                    }
                    k => bail!("unknown OP_MSG section kind {k}"),
                }
            }
            Ok(Some(Msg::OpMsg {
                request_id,
                body: body.context("OP_MSG without body")?,
                seqs,
                more_to_come: flags & 2 != 0,
            }))
        }
        OP_QUERY => {
            let (_ns, n) = cstring(&b[4..])?;
            let (query, _) = doc_at(&b[4 + n + 8..])?;
            Ok(Some(Msg::Query { request_id, query }))
        }
        other => bail!("unsupported opcode {other}"),
    }
}

static NEXT_ID: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);

fn header(out: &mut Vec<u8>, response_to: i32, op: i32) {
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.extend_from_slice(&NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed).to_le_bytes());
    out.extend_from_slice(&response_to.to_le_bytes());
    out.extend_from_slice(&op.to_le_bytes());
}

fn finish(mut out: Vec<u8>) -> Vec<u8> {
    let len = out.len() as i32;
    out[0..4].copy_from_slice(&len.to_le_bytes());
    out
}

pub fn op_msg_reply(response_to: i32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 21);
    header(&mut out, response_to, OP_MSG);
    out.extend_from_slice(&0u32.to_le_bytes());
    out.push(0);
    out.extend_from_slice(body);
    finish(out)
}

pub fn op_reply(response_to: i32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 36);
    header(&mut out, response_to, OP_REPLY);
    out.extend_from_slice(&8i32.to_le_bytes()); // AwaitCapable
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out.extend_from_slice(&1i32.to_le_bytes());
    out.extend_from_slice(body);
    finish(out)
}
