//! The writes inside a compound statement (R6 round 6, decision A4).
//!
//! The Redis front end and the key-value API send their hottest writes as
//! one `BEGIN NOT ATOMIC ... END` per command (chapter 40's guarded
//! batches). Such a text has no shape, so its writes reached no table's
//! write load, and chapter 13's cost model undercounted the write cost of
//! indexes on key and element tables (decision A2). [`writes`] finds the
//! `INSERT`, `REPLACE`, `UPDATE` and `DELETE` statements in the body, at
//! any depth of `IF ... THEN ... END IF` or `BEGIN ... END`, so the gateway
//! can fingerprint and count each one as if it had been sent alone.
//!
//! A statement starts where one may: after `BEGIN`, `;`, `THEN`, `ELSE`,
//! `DO`, `LOOP` or `REPEAT`, outside parentheses, and ends at the next `;`
//! outside parentheses (or the compound's `END`). Anything that isn't a
//! write is left out; so is a text that doesn't start with `BEGIN NOT
//! ATOMIC`.
//!
//! The batches guard most writes with `IF` (a `SET` holds two element
//! deletes that run only when the key expired or changed type), and which
//! branch ran isn't known after the fact. Each write gets the weight of
//! its branches taken as equally likely ([`InnerWrite::weight`]); the
//! gateway counts a run each time a write's weights add up to a whole one.

use std::ops::Range;

use sprig_sql::lex::{self, LexOptions, TokenKind};

/// One write statement inside a compound statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InnerWrite {
    /// Its bytes in the compound's text.
    pub range: Range<usize>,
    /// How often it runs per run of the compound, as far as the text
    /// tells: 1 outside `IF` blocks; inside one, divided by the block's
    /// branches (`THEN`, each `ELSEIF`, `ELSE`, and the empty `ELSE` a
    /// block without one has), for each enclosing block.
    pub weight: f64,
}

/// The write statements of compound statement `sql` (module docs); empty
/// for any other text.
#[must_use]
#[allow(clippy::too_many_lines)] // one walk, two passes
pub fn writes(sql: &[u8], opts: &LexOptions) -> Vec<InnerWrite> {
    let Ok(all) = lex::lex(sql, opts) else {
        return Vec::new();
    };
    let toks: Vec<lex::Token> = all.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let words: Vec<Option<String>> = toks
        .iter()
        .map(|t| {
            (t.kind == TokenKind::Word)
                .then(|| String::from_utf8_lossy(t.bytes(sql)).to_ascii_uppercase())
        })
        .collect();
    let word = |i: usize| words.get(i).and_then(Option::as_deref);
    let punct = |i: usize, c: u8| {
        toks.get(i)
            .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == [c])
    };
    if word(0) != Some("BEGIN") || word(1) != Some("NOT") || word(2) != Some("ATOMIC") {
        return Vec::new();
    }
    // Two passes over the same walk: the first counts each IF block's
    // branches, the second weighs the writes.
    let mut branches: Vec<u32> = Vec::new();
    let mut out = Vec::new();
    for pass in 0..2 {
        let mut open: Vec<usize> = Vec::new(); // IF blocks, by index
        let mut has_else: Vec<bool> = Vec::new();
        let mut next_block = 0usize;
        let mut depth = 0usize;
        let mut i = 3;
        let mut at_start = true;
        while i < toks.len() {
            if punct(i, b'(') {
                depth += 1;
                at_start = false;
            } else if punct(i, b')') {
                depth = depth.saturating_sub(1);
            } else if depth == 0 && punct(i, b';') {
                at_start = true;
            } else if depth == 0 {
                let w = word(i);
                if w == Some("END") && word(i + 1) == Some("IF") {
                    if let Some(b) = open.pop()
                        && pass == 0
                        && !has_else.pop().unwrap_or(true)
                    {
                        branches[b] += 1; // the empty ELSE
                    }
                    at_start = false;
                    i += 2;
                    continue;
                }
                if at_start && w == Some("IF") {
                    if pass == 0 {
                        branches.push(1);
                    }
                    open.push(next_block);
                    has_else.push(false);
                    next_block += 1;
                    at_start = false;
                    i += 1;
                    continue;
                }
                if let (Some("ELSEIF" | "ELSE"), Some(&b)) = (w, open.last()) {
                    if pass == 0 {
                        branches[b] += 1;
                    }
                    if w == Some("ELSE")
                        && let Some(e) = has_else.last_mut()
                    {
                        *e = true;
                    }
                }
                if pass == 1
                    && at_start
                    && matches!(w, Some("INSERT" | "REPLACE" | "UPDATE" | "DELETE"))
                {
                    // To the next `;` outside parentheses.
                    let start = toks[i].start as usize;
                    let mut d = 0usize;
                    let mut j = i + 1;
                    while j < toks.len() {
                        if punct(j, b'(') {
                            d += 1;
                        } else if punct(j, b')') {
                            d = d.saturating_sub(1);
                        } else if d == 0 && punct(j, b';') {
                            break;
                        }
                        j += 1;
                    }
                    let end = toks[j.min(toks.len()) - 1].end as usize;
                    let weight = open
                        .iter()
                        .map(|b| 1.0 / f64::from(branches.get(*b).copied().unwrap_or(1).max(1)))
                        .product();
                    out.push(InnerWrite {
                        range: start..end,
                        weight,
                    });
                    i = j;
                    continue;
                }
                at_start = matches!(
                    w,
                    Some("BEGIN" | "THEN" | "ELSE" | "DO" | "LOOP" | "REPEAT")
                );
            } else {
                at_start = false;
            }
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A guarded `HSET` batch as chapter 40 sends it (shortened).
    const HSET: &str = "BEGIN NOT ATOMIC DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; \
        RESIGNAL; END; START TRANSACTION; SELECT 1 INTO @now FROM (SELECT 1 AS one) AS d \
        LEFT JOIN `redis`.`db0` AS t ON t.k = X'61' AND t._sprig_dk = X'61' FOR UPDATE; \
        IF @x THEN DELETE FROM `redis`.`__sprig_ke_db0` WHERE k = X'61' AND _sprig_dk = X'61'; \
        DELETE FROM `redis`.`db0` WHERE k = X'61'; SET @t = NULL; END IF; \
        IF @t IS NULL THEN INSERT INTO `redis`.`db0` (k, type, g, _v, _sprig_dk) VALUES \
        (X'61', 'hash', 7, 9, X'61'); ELSE UPDATE `redis`.`db0` SET _v = 9 WHERE k = X'61'; \
        END IF; INSERT INTO `redis`.`__sprig_ke_db0` (k, g, o, e, v, _sprig_dk) VALUES \
        (X'61', 7, 0, 'f', 'v', X'61') ON DUPLICATE KEY UPDATE v = VALUES(v); \
        SELECT @rc; COMMIT; END";

    fn texts(sql: &str) -> Vec<&str> {
        writes(sql.as_bytes(), &LexOptions::mariadb())
            .into_iter()
            .map(|w| &sql[w.range])
            .collect()
    }

    fn weights(sql: &str) -> Vec<f64> {
        writes(sql.as_bytes(), &LexOptions::mariadb())
            .into_iter()
            .map(|w| w.weight)
            .collect()
    }

    #[test]
    fn a_guarded_batch_holds_its_writes() {
        let w = texts(HSET);
        assert_eq!(w.len(), 5, "{w:#?}");
        assert!(
            w[0].starts_with("DELETE FROM `redis`.`__sprig_ke_db0`") && w[0].ends_with("X'61'")
        );
        assert!(w[2].starts_with("INSERT INTO `redis`.`db0`") && w[2].ends_with("X'61')"));
        assert!(w[3].starts_with("UPDATE `redis`.`db0` SET"));
        assert!(w[4].starts_with("INSERT INTO `redis`.`__sprig_ke_db0`"));
        assert!(w[4].ends_with("ON DUPLICATE KEY UPDATE v = VALUES(v)"));
        // The clean-up under `IF @x` (no ELSE: two branches), the key row's
        // INSERT or UPDATE (two branches), the element write always.
        assert_eq!(weights(HSET), [0.5, 0.5, 0.5, 0.5, 1.0]);
        // Nested blocks multiply; ELSEIF adds a branch.
        let nested = "BEGIN NOT ATOMIC IF @a THEN IF @b = 1 THEN DELETE FROM t; ELSEIF @b = 2 \
            THEN DELETE FROM u; END IF; ELSE UPDATE t SET x = IF(@c, 1, 2); END IF; END";
        assert_eq!(
            texts(nested),
            [
                "DELETE FROM t",
                "DELETE FROM u",
                "UPDATE t SET x = IF(@c, 1, 2)"
            ]
        );
        assert_eq!(weights(nested), [0.5 / 3.0, 0.5 / 3.0, 0.5]);
    }

    #[test]
    fn other_texts_hold_none() {
        assert!(texts("INSERT INTO t VALUES (1)").is_empty());
        assert!(texts("BEGIN; INSERT INTO t VALUES (1); COMMIT").is_empty());
        // A word inside an expression doesn't start a statement.
        assert_eq!(
            texts("BEGIN NOT ATOMIC SELECT 'UPDATE x' INTO @a; SET @b = (SELECT 1); END"),
            Vec::<&str>::new()
        );
    }
}
