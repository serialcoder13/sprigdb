//! The elements of a `CREATE TABLE` text: a client's statement, or
//! MariaDB's `SHOW CREATE TABLE` answer. Read from the lexer's tokens, so
//! column definitions keep their exact text (MariaDB types such as
//! `VECTOR(768)` included) and the type layer can splice the statement
//! instead of re-printing it.

use std::ops::Range;

use sprig_sql::lex::{LexOptions, TokenKind};

use crate::grammar::Toks;

/// A column definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The name.
    pub name: String,
    /// The type with its character set and collation: `bigint(20)
    /// unsigned`, `varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`.
    pub data_type: String,
    /// No `NOT NULL` (and not an inline `PRIMARY KEY`).
    pub nullable: bool,
    /// `AUTO_INCREMENT`.
    pub auto_increment: bool,
    /// `INVISIBLE`.
    pub invisible: bool,
    /// An inline `PRIMARY KEY`.
    pub primary: bool,
    /// The `DEFAULT` expression's text.
    pub default: Option<String>,
}

/// What an element of the list is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Element {
    /// A column.
    Column(Column),
    /// `PRIMARY KEY (cols)`.
    PrimaryKey(Vec<String>),
    /// An index, key or constraint: its name (if any) and the columns it
    /// names.
    Other {
        /// The leading keyword, upper-cased: `KEY`, `UNIQUE`, `CONSTRAINT`.
        keyword: String,
        /// The index or constraint name.
        name: Option<String>,
        /// Every identifier inside its parentheses.
        columns: Vec<String>,
    },
}

/// A parsed `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    /// The elements with their byte ranges in the text.
    pub elements: Vec<(Element, Range<usize>)>,
    /// The byte offset of the list's `(`.
    pub open: usize,
    /// The byte offset of the list's `)`.
    pub close: usize,
}

const CONSTRAINT_WORDS: [&str; 12] = [
    "PRIMARY",
    "KEY",
    "INDEX",
    "UNIQUE",
    "CONSTRAINT",
    "FOREIGN",
    "CHECK",
    "FULLTEXT",
    "SPATIAL",
    "VECTOR",
    "PERIOD",
    "SYSTEM",
];

/// Words that end a column's type.
const TYPE_END: [&str; 22] = [
    "NOT",
    "NULL",
    "DEFAULT",
    "AUTO_INCREMENT",
    "COMMENT",
    "INVISIBLE",
    "PRIMARY",
    "UNIQUE",
    "KEY",
    "GENERATED",
    "AS",
    "CHECK",
    "REFERENCES",
    "ON",
    "COLUMN_FORMAT",
    "STORAGE",
    "WITH",
    "WITHOUT",
    "SERIAL",
    "VISIBLE",
    "SRID",
    "COMPRESSED",
];

impl TableDef {
    /// Parses the first parenthesized list after `CREATE TABLE name`.
    #[must_use]
    pub fn parse(sql: &str, lex: &LexOptions) -> Option<Self> {
        let t = Toks::new(sql.as_bytes(), lex)?;
        let open = (0..t.len()).find(|&i| t.punct(i, b'('))?;
        let close = t.close_of(open)?;
        let mut elements = Vec::new();
        let mut start = open + 1;
        let mut depth = 0usize;
        for i in open + 1..=close {
            if i < close && t.punct(i, b'(') {
                depth += 1;
            } else if i < close && t.punct(i, b')') {
                depth = depth.saturating_sub(1);
            }
            if (depth == 0 && i < close && t.punct(i, b',')) || i == close {
                if start < i {
                    let el = element(&t, start, i - 1)?;
                    elements.push((el, t.start(start)..t.end(i - 1)));
                }
                start = i + 1;
            }
        }
        Some(TableDef {
            elements,
            open: t.start(open),
            close: t.start(close),
        })
    }

    /// The columns, in order.
    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.elements.iter().filter_map(|(e, _)| match e {
            Element::Column(c) => Some(c),
            _ => None,
        })
    }

    /// The column `name` (case-insensitive).
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns().find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// The primary key's columns, from a `PRIMARY KEY` element or inline.
    #[must_use]
    pub fn primary_key(&self) -> Vec<String> {
        for (e, _) in &self.elements {
            if let Element::PrimaryKey(cols) = e {
                return cols.clone();
            }
        }
        self.columns()
            .filter(|c| c.primary)
            .map(|c| c.name.clone())
            .collect()
    }
}

fn element(t: &Toks<'_>, a: usize, b: usize) -> Option<Element> {
    let leading_word = t.kind(a) == Some(TokenKind::Word);
    if leading_word && CONSTRAINT_WORDS.iter().any(|w| t.is(a, w)) {
        let keyword = String::from_utf8_lossy(t.bytes(a)).to_uppercase();
        let mut name = None;
        let mut columns = Vec::new();
        let mut i = a + 1;
        // `PRIMARY KEY`, `UNIQUE KEY`, `FOREIGN KEY`, `CONSTRAINT name ...`
        if keyword == "PRIMARY" {
            let open = (i..=b).find(|&k| t.punct(k, b'('))?;
            let close = t.close_of(open)?;
            let mut k = open + 1;
            while k < close {
                if let Some(n) = t.ident(k) {
                    columns.push(n);
                    // skip a prefix length `(10)` and `ASC`/`DESC`
                    k += 1;
                    if t.punct(k, b'(') {
                        k = t.close_of(k)? + 1;
                    }
                    while k < close && !t.punct(k, b',') {
                        k += 1;
                    }
                }
                k += 1;
            }
            return Some(Element::PrimaryKey(columns));
        }
        while i <= b && !t.punct(i, b'(') {
            let named = t.kind(i) == Some(TokenKind::QuotedIdent)
                || (t.kind(i) == Some(TokenKind::Word)
                    && ![
                        "KEY", "INDEX", "FOREIGN", "CHECK", "UNIQUE", "USING", "BTREE", "HASH",
                    ]
                    .iter()
                    .any(|w| t.is(i, w)));
            if named && name.is_none() {
                name = t.ident(i);
            }
            i += 1;
        }
        for k in i..=b {
            if let Some(n) = t.ident(k) {
                columns.push(n);
            }
        }
        return Some(Element::Other {
            keyword,
            name,
            columns,
        });
    }
    let name = t.ident(a)?;
    // The type: up to a column attribute keyword at depth 0.
    let mut i = a + 1;
    let mut depth = 0usize;
    let type_start = i;
    while i <= b {
        if t.punct(i, b'(') {
            depth += 1;
        } else if t.punct(i, b')') {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && TYPE_END.iter().any(|w| t.is(i, w)) {
            break;
        }
        i += 1;
    }
    let data_type = t.text(type_start, i.saturating_sub(1).max(type_start));
    let mut col = Column {
        name,
        data_type: if i > type_start {
            data_type
        } else {
            String::new()
        },
        nullable: true,
        auto_increment: false,
        invisible: false,
        primary: false,
        default: None,
    };
    let mut k = i;
    while k <= b {
        if t.is(k, "NOT") && t.is(k + 1, "NULL") {
            col.nullable = false;
            k += 2;
            continue;
        }
        if t.is(k, "AUTO_INCREMENT") {
            col.auto_increment = true;
        } else if t.is(k, "INVISIBLE") {
            col.invisible = true;
        } else if t.is(k, "PRIMARY") && t.is(k + 1, "KEY") {
            col.primary = true;
            col.nullable = false;
        } else if t.is(k, "DEFAULT") && k < b {
            let v = k + 1;
            let end = if t.punct(v, b'(') { t.close_of(v)? } else { v };
            let mut e = end;
            // `DEFAULT current_timestamp()`
            if t.punct(e + 1, b'(')
                && let Some(c) = t.close_of(e + 1)
            {
                e = c;
            }
            col.default = Some(t.text(v, e));
            k = e + 1;
            continue;
        }
        k += 1;
    }
    Some(Element::Column(col))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHOWN: &str = "CREATE TABLE `asset` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'x, y',
  `site_id` int(11) DEFAULT NULL,
  `_type` varchar(64) NOT NULL DEFAULT 'asset' INVISIBLE,
  `created` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`name`(10)),
  KEY `_sprig_type_path` (`_type`,`id`),
  CONSTRAINT `fk` FOREIGN KEY (`site_id`) REFERENCES `site` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    #[test]
    fn show_create_table_parses() {
        let d = TableDef::parse(SHOWN, &LexOptions::mariadb()).unwrap();
        let cols: Vec<_> = d.columns().map(|c| c.name.as_str()).collect();
        assert_eq!(cols, ["id", "name", "site_id", "_type", "created"]);
        let id = d.column("ID").unwrap();
        assert_eq!(id.data_type, "bigint(20)");
        assert!(id.auto_increment && !id.nullable);
        let name = d.column("name").unwrap();
        assert_eq!(
            name.data_type,
            "varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        );
        assert_eq!(name.default.as_deref(), Some("'x, y'"));
        assert!(d.column("_type").unwrap().invisible);
        assert_eq!(
            d.column("created").unwrap().default.as_deref(),
            Some("current_timestamp()")
        );
        assert_eq!(d.primary_key(), ["id"]);
        let others: Vec<_> = d
            .elements
            .iter()
            .filter_map(|(e, _)| match e {
                Element::Other { name, .. } => name.clone(),
                _ => None,
            })
            .collect();
        assert_eq!(others, ["uk_name", "_sprig_type_path", "fk"]);
        assert!(SHOWN[d.close..].starts_with(')'));
    }

    #[test]
    fn inline_primary_keys_and_mariadb_types() {
        let d = TableDef::parse(
            "CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(768) NOT NULL, k CHAR(17) UNIQUE)",
            &LexOptions::mariadb(),
        )
        .unwrap();
        assert_eq!(d.primary_key(), ["id"]);
        assert_eq!(d.column("v").unwrap().data_type, "VECTOR(768)");
        assert_eq!(d.column("k").unwrap().data_type, "CHAR(17)");
    }
}
