//! Server-sent events, parsed incrementally from response chunks.

/// One event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Event {
    /// The `event:` field; empty when absent.
    pub(crate) name: String,
    /// The `data:` lines joined with `\n`.
    pub(crate) data: String,
}

/// Turns bytes into events. Lines may end in `\n`, `\r\n` or `\r`.
#[derive(Debug, Default)]
pub(crate) struct Parser {
    buf: Vec<u8>,
    name: String,
    data: String,
    has_data: bool,
}

/// The most bytes one line may hold before the stream is refused.
pub(crate) const MAX_LINE: usize = 4 << 20;

impl Parser {
    /// Feeds a chunk and returns the events it completed.
    ///
    /// # Errors
    /// A line longer than [`MAX_LINE`].
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<Vec<Event>, String> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        let mut start = 0;
        let mut i = 0;
        while i < self.buf.len() {
            let c = self.buf[i];
            if c == b'\n' || c == b'\r' {
                // A `\r` at the very end may be the first half of `\r\n`:
                // wait for the next chunk.
                if c == b'\r' && i + 1 == self.buf.len() {
                    break;
                }
                let line = String::from_utf8_lossy(&self.buf[start..i]).into_owned();
                let skip = usize::from(c == b'\r' && self.buf.get(i + 1) == Some(&b'\n'));
                i += 1 + skip;
                start = i;
                self.line(&line, &mut out);
            } else {
                i += 1;
            }
        }
        self.buf.drain(..start);
        if self.buf.len() > MAX_LINE {
            return Err(format!("an event line is longer than {MAX_LINE} bytes"));
        }
        Ok(out)
    }

    /// The event left when the stream ends without a blank line.
    pub(crate) fn finish(&mut self) -> Option<Event> {
        if !self.buf.is_empty() {
            let rest = String::from_utf8_lossy(&std::mem::take(&mut self.buf)).into_owned();
            let line = rest.trim_end_matches(['\r', '\n']);
            if !line.is_empty() {
                let mut out = Vec::new();
                self.line(line, &mut out);
                if let Some(e) = out.pop() {
                    return Some(e);
                }
            }
        }
        self.dispatch()
    }

    fn line(&mut self, line: &str, out: &mut Vec<Event>) {
        if line.is_empty() {
            if let Some(e) = self.dispatch() {
                out.push(e);
            }
            return;
        }
        if line.starts_with(':') {
            return;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "event" => value.clone_into(&mut self.name),
            "data" => {
                if self.has_data {
                    self.data.push('\n');
                }
                self.data.push_str(value);
                self.has_data = true;
            }
            _ => {}
        }
    }

    fn dispatch(&mut self) -> Option<Event> {
        let name = std::mem::take(&mut self.name);
        if !self.has_data {
            return None;
        }
        self.has_data = false;
        Some(Event {
            name,
            data: std::mem::take(&mut self.data),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_split_across_chunks_and_line_endings() {
        let text = "event: a\r\ndata: {\"x\":1}\r\n\r\n: comment\n\nevent: b\ndata: one\ndata: two\n\ndata: [DONE]\r\r";
        for split in 0..text.len() {
            let mut p = Parser::default();
            let mut evs = p.feed(&text.as_bytes()[..split]).unwrap();
            evs.extend(p.feed(&text.as_bytes()[split..]).unwrap());
            evs.extend(p.finish());
            assert_eq!(
                evs,
                vec![
                    Event {
                        name: "a".into(),
                        data: "{\"x\":1}".into()
                    },
                    Event {
                        name: "b".into(),
                        data: "one\ntwo".into()
                    },
                    Event {
                        name: String::new(),
                        data: "[DONE]".into()
                    },
                ],
                "split at {split}"
            );
        }
    }

    #[test]
    fn an_unterminated_last_event_is_kept() {
        let mut p = Parser::default();
        assert!(p.feed(b"data: tail").unwrap().is_empty());
        assert_eq!(p.finish().unwrap().data, "tail");
    }
}
