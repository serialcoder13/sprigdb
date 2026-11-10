//! A small SMTP client for approval emails (chapter 52, "Notifications").
//! Plain SMTP with optional `AUTH PLAIN`; `smtp+starttls://` and
//! `smtps://` are refused until TLS is added (decision AP7). No new
//! dependency: the protocol is a few lines of text (RFC 5321).

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use sprig_common::{Result, SprigCode, SprigError};

/// How long one SMTP exchange may take.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// Where and how to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmtpConfig {
    /// `host:port`.
    pub addr: String,
    /// The sender.
    pub from: String,
    /// `AUTH PLAIN` user and password, when set.
    pub auth: Option<(String, String)>,
}

impl SmtpConfig {
    /// Parses `approvals.smtp_url`. `None` when empty (email is off).
    ///
    /// # Errors
    /// 39977 for a URL this client can't use.
    pub fn parse(url: &str, from: &str, auth: Option<(String, String)>) -> Result<Option<Self>> {
        let url = url.trim();
        if url.is_empty() {
            return Ok(None);
        }
        let bad = |m: &str| {
            SprigError::new(
                SprigCode::ApprovalPolicyInvalid,
                format!("approvals.smtp_url {url:?}: {m}"),
            )
        };
        let rest = if let Some(r) = url.strip_prefix("smtp://") {
            r
        } else if url.starts_with("smtps://") || url.starts_with("smtp+starttls://") {
            return Err(bad("TLS isn't built yet; use smtp:// to a local relay"));
        } else {
            return Err(bad("expected smtp://host:port"));
        };
        let addr = rest.trim_end_matches('/');
        let addr = if addr.contains(':') {
            addr.to_owned()
        } else {
            format!("{addr}:25")
        };
        Ok(Some(Self {
            addr,
            from: from.to_owned(),
            auth,
        }))
    }
}

/// One message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    /// Recipients.
    pub to: Vec<String>,
    /// Subject (one line).
    pub subject: String,
    /// Plain-text body.
    pub body: String,
}

fn clean(s: &str) -> String {
    s.chars().filter(|c| *c != '\r' && *c != '\n').collect()
}

fn base64(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The message text with headers, dot-stuffed, ending in `CRLF.CRLF`.
#[must_use]
pub fn render(from: &str, m: &Mail, date: &str, id: &str) -> String {
    let mut s = String::new();
    s.push_str(&format!("From: {}\r\n", clean(from)));
    s.push_str(&format!(
        "To: {}\r\n",
        m.to.iter().map(|t| clean(t)).collect::<Vec<_>>().join(", ")
    ));
    s.push_str(&format!("Subject: {}\r\n", clean(&m.subject)));
    s.push_str(&format!("Date: {date}\r\n"));
    s.push_str(&format!("Message-ID: <{id}>\r\n"));
    s.push_str("MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n");
    for line in m.body.replace("\r\n", "\n").split('\n') {
        if line.starts_with('.') {
            s.push('.');
        }
        s.push_str(line);
        s.push_str("\r\n");
    }
    s.push_str(".\r\n");
    s
}

struct Session {
    r: BufReader<tokio::net::tcp::OwnedReadHalf>,
    w: tokio::net::tcp::OwnedWriteHalf,
}

impl Session {
    /// Reads one reply (all its lines); returns the code and the text.
    async fn reply(&mut self) -> Result<(u16, String)> {
        let mut text = String::new();
        loop {
            let mut line = String::new();
            let n = self.r.read_line(&mut line).await.map_err(io)?;
            if n == 0 {
                return Err(fail("the server closed the connection"));
            }
            text.push_str(&line);
            let b = line.as_bytes();
            if b.len() < 4 || b[3] != b'-' {
                let code = line.get(..3).and_then(|c| c.parse().ok()).unwrap_or(0);
                return Ok((code, text));
            }
        }
    }

    async fn cmd(&mut self, line: &str, want: u16) -> Result<String> {
        self.w.write_all(line.as_bytes()).await.map_err(io)?;
        self.w.write_all(b"\r\n").await.map_err(io)?;
        let (code, text) = self.reply().await?;
        if code / 100 != want / 100 {
            return Err(fail(&format!(
                "{} answered {}",
                line.split(' ').next().unwrap_or(line),
                text.trim()
            )));
        }
        Ok(text)
    }
}

fn io(e: std::io::Error) -> SprigError {
    fail(&e.to_string())
}

fn fail(m: &str) -> SprigError {
    SprigError::new(SprigCode::NotifyFailed, format!("email: {m}"))
}

/// Sends `m` through `c`.
///
/// # Errors
/// 39978 with the server's answer.
pub async fn send(c: &SmtpConfig, m: &Mail, date: &str, id: &str) -> Result<()> {
    if m.to.is_empty() {
        return Ok(());
    }
    let run = async {
        let stream = TcpStream::connect(&c.addr).await.map_err(io)?;
        let (r, w) = stream.into_split();
        let mut s = Session {
            r: BufReader::new(r),
            w,
        };
        let (code, text) = s.reply().await?;
        if code != 220 {
            return Err(fail(&format!("greeting {}", text.trim())));
        }
        s.cmd("EHLO sprigdb", 250).await?;
        if let Some((u, p)) = &c.auth {
            let token = base64(format!("\0{u}\0{p}").as_bytes());
            s.cmd(&format!("AUTH PLAIN {token}"), 235).await?;
        }
        s.cmd(&format!("MAIL FROM:<{}>", clean(&c.from)), 250)
            .await?;
        for t in &m.to {
            s.cmd(&format!("RCPT TO:<{}>", clean(t)), 250).await?;
        }
        s.cmd("DATA", 354).await?;
        let msg = render(&c.from, m, date, id);
        s.w.write_all(msg.as_bytes()).await.map_err(io)?;
        let (code, text) = s.reply().await?;
        if code / 100 != 2 {
            return Err(fail(&format!("DATA answered {}", text.trim())));
        }
        let _ = s.cmd("QUIT", 221).await;
        Ok(())
    };
    tokio::time::timeout(TIMEOUT, run)
        .await
        .map_err(|_| fail("timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_and_rendering() {
        assert_eq!(SmtpConfig::parse("", "a@b", None).unwrap(), None);
        let c = SmtpConfig::parse("smtp://127.0.0.1:2525", "a@b", None)
            .unwrap()
            .unwrap();
        assert_eq!(c.addr, "127.0.0.1:2525");
        assert_eq!(
            SmtpConfig::parse("smtp://mail", "a@b", None)
                .unwrap()
                .unwrap()
                .addr,
            "mail:25"
        );
        assert!(SmtpConfig::parse("smtps://mail", "a@b", None).is_err());
        let m = Mail {
            to: vec!["x@y".into()],
            subject: "hi\r\nBcc: evil".into(),
            body: "line\n.dot".into(),
        };
        let r = render("s@d", &m, "Tue, 10 Nov 2026 14:03:11 +0000", "1@sprig");
        assert!(r.contains("Subject: hiBcc: evil\r\n"));
        assert!(r.contains("\r\n..dot\r\n.\r\n"));
        assert_eq!(base64(b"\0u\0p"), "AHUAcA==");
    }

    #[tokio::test]
    async fn a_conversation_with_a_fake_server() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let (r, mut w) = s.into_split();
            let mut r = BufReader::new(r);
            w.write_all(b"220 hi\r\n").await.unwrap();
            let mut got = String::new();
            let mut in_data = false;
            loop {
                let mut line = String::new();
                if r.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                got.push_str(&line);
                if in_data {
                    if line == ".\r\n" {
                        in_data = false;
                        w.write_all(b"250 queued\r\n").await.unwrap();
                    }
                    continue;
                }
                let reply: &[u8] = if line.starts_with("EHLO") {
                    b"250-hi\r\n250 OK\r\n"
                } else if line.starts_with("DATA") {
                    in_data = true;
                    b"354 go\r\n"
                } else if line.starts_with("QUIT") {
                    w.write_all(b"221 bye\r\n").await.unwrap();
                    break;
                } else {
                    b"250 ok\r\n"
                };
                w.write_all(reply).await.unwrap();
            }
            got
        });
        let c = SmtpConfig::parse(&format!("smtp://{addr}"), "sprig@x", None)
            .unwrap()
            .unwrap();
        let m = Mail {
            to: vec!["ana@x".into(), "bo@x".into()],
            subject: "s".into(),
            body: "b".into(),
        };
        send(&c, &m, "Tue, 10 Nov 2026 14:03:11 +0000", "1@x")
            .await
            .unwrap();
        let got = server.await.unwrap();
        assert!(got.contains("RCPT TO:<ana@x>\r\nRCPT TO:<bo@x>"));
        assert!(got.contains("Subject: s\r\n"));
    }
}
