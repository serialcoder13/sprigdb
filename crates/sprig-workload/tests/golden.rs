//! The golden corpora: normalized texts in
//! `tests/fixtures/normalize.txt` and shapes in `tests/fixtures/shapes.txt`.
//!
//! Format: `@` lines set options for the cases after them (`mysql` or
//! `mariadb`, `db=<name>` or `nodb`, `ltn`, `ansi`, `nbe`); `in:` is a
//! statement, with `⏎` for a line break; `out:` (or `shape:`) is what it
//! must give. `#` lines are comments.
//!
//! Every normalized text must also normalize to itself (idempotence).
//!
//! To rewrite the expected values after a deliberate change:
//! `SPRIG_BLESS=1 cargo test -p sprig-workload --test golden`, then review
//! the diff.

use sprig_common::ids::{DbId, ObjectId};
use sprig_sql::lex::LexOptions;
use sprig_workload::fingerprint::normalize;
use sprig_workload::shape::{ColumnClass, ExtractCtx, ResolvedTable, TableResolver, extract};

#[derive(Clone)]
struct Opts {
    lex: LexOptions,
    db: Option<String>,
    ltn: bool,
}

fn parse_opts(line: &str) -> Opts {
    let mut o = Opts {
        lex: LexOptions::mysql84(),
        db: Some("shop".into()),
        ltn: false,
    };
    for w in line.trim_start_matches('@').split_whitespace() {
        match w {
            "mysql" => {
                o.lex = LexOptions::mysql84()
                    .ansi_quotes(o.lex.ansi_quotes)
                    .no_backslash_escapes(o.lex.no_backslash_escapes);
            }
            "mariadb" => {
                o.lex = LexOptions::mariadb()
                    .ansi_quotes(o.lex.ansi_quotes)
                    .no_backslash_escapes(o.lex.no_backslash_escapes);
            }
            "ansi" => o.lex = o.lex.ansi_quotes(true),
            "nbe" => o.lex = o.lex.no_backslash_escapes(true),
            "ltn" => o.ltn = true,
            "nodb" => o.db = None,
            w if w.starts_with("db=") => o.db = Some(w[3..].to_owned()),
            other => panic!("unknown option {other}"),
        }
    }
    o
}

struct Resolver;

impl TableResolver for Resolver {
    fn resolve(&self, db: Option<&str>, table: &str) -> Option<ResolvedTable> {
        let id = match (db?, table) {
            ("shop", "orders") => 1201,
            ("shop", "customers") => 1188,
            ("shop", "order_items") => 1202,
            ("shop", "products") => 1210,
            ("shop", "events") => 1300,
            ("billing", "invoices") => 2001,
            _ => return None,
        };
        Some(ResolvedTable {
            object_id: ObjectId(id),
            db_id: DbId(1),
            physical: None,
        })
    }

    fn has_column(&self, t: &ResolvedTable, c: &str) -> Option<bool> {
        let cols: &[&str] = match t.object_id.0 {
            1201 => &[
                "id",
                "customer_id",
                "status",
                "created_at",
                "total",
                "email",
                "note",
            ],
            1188 => &["id", "name", "email", "country", "created_at"],
            1202 => &["id", "order_id", "product_id", "qty", "price"],
            1210 => &["id", "sku", "name", "price", "category", "doc"],
            _ => return None,
        };
        Some(cols.contains(&c))
    }

    fn column_class(&self, t: &ResolvedTable, c: &str) -> Option<ColumnClass> {
        match (t.object_id.0, c) {
            (_, "status" | "email" | "name" | "sku" | "note" | "country" | "category") => {
                Some(ColumnClass::String)
            }
            (_, "created_at") => Some(ColumnClass::Temporal),
            _ => Some(ColumnClass::Numeric),
        }
    }
}

fn run(file: &str, key: &str, f: impl Fn(&str, &Opts) -> String) {
    let path = format!("{}/tests/fixtures/{file}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).expect("fixture");
    let bless = std::env::var_os("SPRIG_BLESS").is_some();
    let mut opts = parse_opts("@");
    let mut out = String::new();
    let mut failures = Vec::new();
    let mut cases = 0;
    let mut current: Option<String> = None;
    for line in text.lines() {
        if let Some(o) = line.strip_prefix('@') {
            opts = parse_opts(o);
        }
        if let Some(sql) = line.strip_prefix("in: ") {
            current = Some(sql.replace('⏎', "\n"));
        }
        if let Some(expected) = line.strip_prefix(key) {
            let sql = current.take().expect("an in: line before each expectation");
            let got = f(&sql, &opts);
            cases += 1;
            if got != expected {
                failures.push(format!("in:  {sql}\nwant: {expected}\ngot:  {got}\n"));
            }
            if bless {
                out.push_str(key);
                out.push_str(&got);
                out.push('\n');
                continue;
            }
        }
        out.push_str(line);
        out.push('\n');
    }
    if bless {
        std::fs::write(&path, out).expect("write fixture");
        return;
    }
    assert!(cases > 0);
    assert!(
        failures.is_empty(),
        "{} of {cases} cases differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn normalize_corpus() {
    let idempotence = std::cell::RefCell::new(Vec::new());
    run("normalize.txt", "out: ", |sql, o| {
        let n = normalize(sql.as_bytes(), &o.lex, o.db.as_deref(), o.ltn);
        let again = normalize(n.text.as_bytes(), &o.lex, o.db.as_deref(), o.ltn);
        if again.text != n.text && !n.unparsed {
            idempotence
                .borrow_mut()
                .push(format!("{}\n  -> {}", n.text, again.text));
        }
        if n.unparsed {
            "<unparsed>".to_owned()
        } else {
            n.text
        }
    });
    let bad = idempotence.into_inner();
    assert!(bad.is_empty(), "not idempotent:\n{}", bad.join("\n"));
}

#[test]
fn shape_corpus() {
    run("shapes.txt", "shape: ", |sql, o| {
        let n = normalize(sql.as_bytes(), &o.lex, o.db.as_deref(), o.ltn);
        let ctx = ExtractCtx {
            lex: o.lex,
            current_db: o.db.as_deref(),
            lower_case_table_names: o.ltn,
        };
        extract(sql.as_bytes(), &n, &ctx, &Resolver).to_json()
    });
}
