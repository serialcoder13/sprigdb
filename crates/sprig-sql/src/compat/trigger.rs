//! `SHOW CREATE TRIGGER`'s statement as MySQL 8.4 prints it (chapter 28,
//! "Output on port 3306"; R4, decision X36).
//!
//! MariaDB keeps the statement as written: `trigger t before insert on
//! db.x for each row ...`. MySQL rebuilds the head from the trigger's
//! definition: ``TRIGGER `t` BEFORE INSERT ON `x` FOR EACH ROW``, without
//! `IF NOT EXISTS`, the table's schema, or `FOLLOWS`/`PRECEDES`; the
//! `DEFINER` clause and the body stay as they are.

/// One word of the head: a backquoted or bare identifier, or a keyword.
struct Word<'a> {
    /// The text, without quotes (a backquoted name's doubled backquotes
    /// undone).
    text: String,
    /// Where it ends in the statement.
    end: usize,
    /// The source, for keyword checks.
    raw: &'a str,
}

/// The next word of `s` from `at`, skipping whitespace; a name may be
/// qualified (`db.x`, `` `db`.`x` ``), and then only its last part is kept.
fn word(s: &str, at: usize) -> Option<Word<'_>> {
    let b = s.as_bytes();
    let mut i = at;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= b.len() {
        return None;
    }
    let start = i;
    let mut text = String::new();
    loop {
        if b.get(i) == Some(&b'`') {
            text.clear();
            i += 1;
            loop {
                match b.get(i)? {
                    b'`' if b.get(i + 1) == Some(&b'`') => {
                        text.push('`');
                        i += 2;
                    }
                    b'`' => {
                        i += 1;
                        break;
                    }
                    _ => {
                        let ch = s[i..].chars().next()?;
                        text.push(ch);
                        i += ch.len_utf8();
                    }
                }
            }
        } else {
            text.clear();
            let from = i;
            while i < b.len() && !b[i].is_ascii_whitespace() && b[i] != b'.' && b[i] != b'`' {
                i += 1;
            }
            if i == from {
                return None;
            }
            text.push_str(&s[from..i]);
        }
        // A qualified name: keep the last part.
        if b.get(i) == Some(&b'.') {
            i += 1;
            continue;
        }
        break;
    }
    Some(Word {
        text,
        end: i,
        raw: &s[start..i],
    })
}

fn is_kw(w: &Word<'_>, kw: &str) -> bool {
    w.raw.eq_ignore_ascii_case(kw)
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `text` (a `SHOW CREATE TRIGGER` statement as MariaDB keeps it) as MySQL
/// 8.4 prints it; `None` when it doesn't read as a trigger's statement.
#[must_use]
pub fn mysql_trigger_statement(text: &str) -> Option<String> {
    let create = word(text, 0)?;
    if !is_kw(&create, "CREATE") {
        return None;
    }
    // `DEFINER=...` up to the keyword TRIGGER, kept as it is.
    let mut at = create.end;
    let mut definer = None;
    let trig_end = loop {
        let w = word(text, at)?;
        if is_kw(&w, "TRIGGER") {
            break w.end;
        }
        if definer.is_none() && w.raw.len() >= 7 && w.raw[..7].eq_ignore_ascii_case("DEFINER") {
            // The clause runs to the next whitespace outside quotes.
            let b = text.as_bytes();
            let mut i = at;
            while b[i].is_ascii_whitespace() {
                i += 1;
            }
            let from = i;
            let mut q: Option<u8> = None;
            while i < b.len() {
                match (q, b[i]) {
                    (None, c) if c.is_ascii_whitespace() => break,
                    (None, c @ (b'`' | b'\'' | b'"')) => q = Some(c),
                    (Some(c), d) if c == d => q = None,
                    _ => {}
                }
                i += 1;
            }
            definer = Some(text[from..i].to_owned());
            at = i;
            continue;
        }
        return None;
    };
    let mut w = word(text, trig_end)?;
    if is_kw(&w, "IF") {
        let not = word(text, w.end)?;
        let exists = word(text, not.end)?;
        if !is_kw(&not, "NOT") || !is_kw(&exists, "EXISTS") {
            return None;
        }
        w = word(text, exists.end)?;
    }
    let name = w.text;
    let timing = word(text, w.end)?;
    let event = word(text, timing.end)?;
    let on = word(text, event.end)?;
    if !["BEFORE", "AFTER"].iter().any(|k| is_kw(&timing, k))
        || !["INSERT", "UPDATE", "DELETE"].iter().any(|k| is_kw(&event, k))
        || !is_kw(&on, "ON")
    {
        return None;
    }
    let table = word(text, on.end)?;
    let (f, e, r) = {
        let f = word(text, table.end)?;
        let e = word(text, f.end)?;
        let r = word(text, e.end)?;
        (f, e, r)
    };
    if !is_kw(&f, "FOR") || !is_kw(&e, "EACH") || !is_kw(&r, "ROW") {
        return None;
    }
    let mut body_at = r.end;
    if let Some(o) = word(text, body_at)
        && (is_kw(&o, "FOLLOWS") || is_kw(&o, "PRECEDES"))
    {
        body_at = word(text, o.end)?.end;
    }
    let body = text[body_at..].trim_start();
    let mut out = String::from("CREATE ");
    if let Some(d) = definer {
        out.push_str(&d);
        out.push(' ');
    }
    out.push_str(&format!(
        "TRIGGER {} {} {} ON {} FOR EACH ROW {body}",
        quote(&name),
        timing.raw.to_ascii_uppercase(),
        event.raw.to_ascii_uppercase(),
        quote(&table.text),
    ));
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::mysql_trigger_statement as m;

    /// The forms checked against MySQL 8.4.11 (R4, decision X36).
    #[test]
    fn heads_as_mysql_prints_them() {
        assert_eq!(
            m("CREATE DEFINER=`app`@`%` trigger c_x7_tr2 after update on `r4c_conn`.c_x7_t2 for each row set @x = 1").unwrap(),
            "CREATE DEFINER=`app`@`%` TRIGGER `c_x7_tr2` AFTER UPDATE ON `c_x7_t2` FOR EACH ROW set @x = 1"
        );
        assert_eq!(
            m("CREATE DEFINER=`app`@`%` TRIGGER IF NOT EXISTS `c_x7 b` BEFORE UPDATE ON `c_x7_t3` FOR EACH ROW BEGIN\n  SET @y = OLD.a;\nEND").unwrap(),
            "CREATE DEFINER=`app`@`%` TRIGGER `c_x7 b` BEFORE UPDATE ON `c_x7_t3` FOR EACH ROW BEGIN\n  SET @y = OLD.a;\nEND"
        );
        assert_eq!(
            m("CREATE DEFINER=`app`@`%` TRIGGER c_x7_c BEFORE   UPDATE ON c_x7_t3 FOR EACH ROW PRECEDES `c_x7 b` SET @z = 1").unwrap(),
            "CREATE DEFINER=`app`@`%` TRIGGER `c_x7_c` BEFORE UPDATE ON `c_x7_t3` FOR EACH ROW SET @z = 1"
        );
        assert_eq!(
            m("CREATE DEFINER=`a b`@`%` TRIGGER `x``y` BEFORE INSERT ON db.`t``1` FOR EACH ROW FOLLOWS other SET NEW.a = 1").unwrap(),
            "CREATE DEFINER=`a b`@`%` TRIGGER `x``y` BEFORE INSERT ON `t``1` FOR EACH ROW SET NEW.a = 1"
        );
        assert_eq!(
            m("CREATE TRIGGER t BEFORE INSERT ON x FOR EACH ROW SET @a = 1").unwrap(),
            "CREATE TRIGGER `t` BEFORE INSERT ON `x` FOR EACH ROW SET @a = 1"
        );
        assert!(m("CREATE PROCEDURE p() SELECT 1").is_none());
        assert!(m("not a trigger").is_none());
    }
}
