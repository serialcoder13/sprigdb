//! MySQL 8.4's printing of expressions in `SHOW CREATE TABLE` (chapter 28
//! step 10, R4 round 4; gaps rows 62 and 64).
//!
//! MariaDB prints a generated column's, an expression default's and a
//! check's expression with the fewest parentheses (`` `a` * `b` - 2 ``);
//! MySQL prints its item tree: every operator in parentheses
//! (`` ((`a` * `b`) - 2) ``), `and`, `or`, `like`, `between`, `in`, `is
//! null` and `case` in lower case, `MOD` as `%`, unary minus as `-(x)`,
//! function arguments joined by `,` without spaces, `lcase` and `ucase` as
//! `lower` and `upper`, and string literals with the table's character
//! set as introducer (`_utf8mb4'x'`). Tools that compare schemas across
//! migrations (Rails, Prisma, Liquibase) and `SQLAlchemy`'s reflection read
//! this text.
//!
//! [`mysql_expr`] parses MariaDB's text with sqlparser and prints it the
//! MySQL way; an expression with anything it doesn't know (a `JSON` path
//! operator, `INTERVAL`, `NOT LIKE`, a hex literal, ...) keeps MariaDB's
//! text, whole. [`mysql_expressions`] applies it to the lines of a `SHOW
//! CREATE TABLE`.

use std::fmt::Write as _;

use sqlparser::ast::{
    BinaryOperator, CastKind, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator,
    Value,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

/// MariaDB's printed expression `text` as MySQL prints it; `cs` is the
/// table's character set (string literals' introducer). `None` when
/// `text` holds anything not handled.
#[must_use]
pub fn mysql_expr(text: &str, cs: &str) -> Option<String> {
    // sqlparser doesn't read the `MOD` operator; MariaDB prints `%` as it.
    let owned;
    let text = if text.contains(" MOD ") && !text.contains('\'') {
        owned = text.replace(" MOD ", " % ");
        owned.as_str()
    } else {
        text
    };
    let dialect = MySqlDialect {};
    let mut p = Parser::new(&dialect).try_with_sql(text).ok()?;
    let e = p.parse_expr().ok()?;
    if p.peek_token().token != Token::EOF {
        return None;
    }
    print(&e, cs)
}

fn op(o: &BinaryOperator) -> Option<&'static str> {
    Some(match o {
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::MyIntegerDivide => "DIV",
        BinaryOperator::Eq => "=",
        BinaryOperator::NotEq => "<>",
        BinaryOperator::Lt => "<",
        BinaryOperator::Gt => ">",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::Spaceship => "<=>",
        BinaryOperator::And => "and",
        BinaryOperator::Or => "or",
        BinaryOperator::Xor => "xor",
        BinaryOperator::BitwiseAnd => "&",
        BinaryOperator::BitwiseOr => "|",
        BinaryOperator::BitwiseXor => "^",
        BinaryOperator::PGBitwiseShiftLeft => "<<",
        BinaryOperator::PGBitwiseShiftRight => ">>",
        _ => return None,
    })
}

fn string(s: &str, cs: &str) -> String {
    let mut out = format!("_{cs}'");
    for c in s.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn list(items: &[Expr], cs: &str) -> Option<String> {
    let v: Option<Vec<String>> = items.iter().map(|e| print(e, cs)).collect();
    Some(v?.join(","))
}

#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one arm per expression kind
fn print(e: &Expr, cs: &str) -> Option<String> {
    Some(match e {
        Expr::Identifier(id) => format!("`{}`", id.value.replace('`', "``")),
        Expr::CompoundIdentifier(ids) => ids
            .iter()
            .map(|i| format!("`{}`", i.value.replace('`', "``")))
            .collect::<Vec<_>>()
            .join("."),
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n.clone(),
            Value::SingleQuotedString(s) => string(s, cs),
            Value::Null => "NULL".to_owned(),
            Value::Boolean(b) => b.to_string(),
            _ => return None,
        },
        Expr::Nested(inner) => print(inner, cs)?,
        Expr::BinaryOp { left, op: o, right } => {
            format!("({} {} {})", print(left, cs)?, op(o)?, print(right, cs)?)
        }
        Expr::UnaryOp { op: o, expr } => match o {
            UnaryOperator::Minus => format!("-({})", print(expr, cs)?),
            UnaryOperator::Plus => print(expr, cs)?,
            UnaryOperator::Not => format!("(not({}))", print(expr, cs)?),
            _ => return None,
        },
        Expr::IsNull(x) => format!("({} is null)", print(x, cs)?),
        Expr::IsNotNull(x) => format!("({} is not null)", print(x, cs)?),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => format!(
            "({} {}between {} and {})",
            print(expr, cs)?,
            if *negated { "not " } else { "" },
            print(low, cs)?,
            print(high, cs)?
        ),
        Expr::InList {
            expr,
            list: items,
            negated,
        } => format!(
            "({} {}in ({}))",
            print(expr, cs)?,
            if *negated { "not " } else { "" },
            list(items, cs)?
        ),
        Expr::Like {
            negated: false,
            any: false,
            expr,
            pattern,
            escape_char: None,
        } => format!("({} like {})", print(expr, cs)?, print(pattern, cs)?),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut s = String::from("(case ");
            if let Some(o) = operand {
                s.push_str(&print(o, cs)?);
                s.push(' ');
            }
            for w in conditions {
                let _ = write!(
                    s,
                    "when {} then {} ",
                    print(&w.condition, cs)?,
                    print(&w.result, cs)?
                );
            }
            if let Some(x) = else_result {
                let _ = write!(s, "else {} ", print(x, cs)?);
            }
            s.push_str("end)");
            s
        }
        Expr::Cast {
            kind: CastKind::Cast,
            expr,
            data_type,
            format: None,
            ..
        } => {
            let ty = data_type.to_string().to_ascii_lowercase();
            if ty.contains(' ') && !matches!(ty.as_str(), "unsigned integer" | "signed integer") {
                return None;
            }
            let ty = ty.trim_end_matches(" integer");
            format!("cast({} as {ty})", print(expr, cs)?)
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut v = vec![print(expr, cs)?];
            if let Some(x) = substring_from {
                v.push(print(x, cs)?);
            }
            if let Some(x) = substring_for {
                v.push(print(x, cs)?);
            }
            format!("substr({})", v.join(","))
        }
        Expr::Function(f) => {
            if f.filter.is_some() || f.over.is_some() || !f.within_group.is_empty() {
                return None;
            }
            let name = f.name.to_string().to_ascii_lowercase();
            let name = match name.as_str() {
                "lcase" => "lower".to_owned(),
                "ucase" => "upper".to_owned(),
                _ => name,
            };
            if name.contains('.') || name.contains('`') {
                return None;
            }
            // R4 round 5: `jsonops` wrote a compared JSON string constant
            // as `json_unquote(json_extract('"x"','$'))`; MySQL prints
            // what was written, `json_extract('"x"','$')`.
            if name == "json_unquote"
                && let FunctionArguments::List(l) = &f.args
                && let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(inner)))] =
                    l.args.as_slice()
                && inner.name.to_string().eq_ignore_ascii_case("json_extract")
                && let FunctionArguments::List(il) = &inner.args
                && let [
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(a))),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(b))),
                ] = il.args.as_slice()
                && let (Value::SingleQuotedString(lit), Value::SingleQuotedString(path)) =
                    (&a.value, &b.value)
                && path == "$"
                && lit.trim().len() >= 2
                && lit.trim().starts_with('"')
                && lit.trim().ends_with('"')
            {
                return Some(format!(
                    "json_extract({},{})",
                    string(lit, cs),
                    string(path, cs)
                ));
            }
            let args = match &f.args {
                FunctionArguments::None => String::new(),
                FunctionArguments::List(l) => {
                    if l.duplicate_treatment.is_some() || !l.clauses.is_empty() {
                        return None;
                    }
                    let mut v = Vec::new();
                    for a in &l.args {
                        match a {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => v.push(print(x, cs)?),
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                v.push("*".to_owned());
                            }
                            _ => return None,
                        }
                    }
                    v.join(",")
                }
                FunctionArguments::Subquery(_) => return None,
            };
            format!("{name}({args})")
        }
        _ => return None,
    })
}

/// The end of the parenthesized text starting at `open` (`text[open] ==
/// b'('`): the index of its `)`, skipping quoted text.
fn close_paren(text: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut i = open;
    while i < text.len() {
        let c = text[i];
        match quote {
            Some(q) if c == b'\\' && q != b'`' => i += 1,
            Some(q) if c == q => {
                if text.get(i + 1) == Some(&q) {
                    i += 1;
                } else {
                    quote = None;
                }
            }
            None if matches!(c, b'\'' | b'"' | b'`') => quote = Some(c),
            None if c == b'(' => depth += 1,
            None if c == b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Every `<marker>(expr)` in `line` with `expr` printed the MySQL way.
fn reprint_after(line: &str, marker: &str, cs: &str) -> String {
    let mut out = String::with_capacity(line.len() + 16);
    let mut at = 0;
    while let Some(off) = line[at..].find(marker) {
        let open = at + off + marker.len() - 1;
        out.push_str(&line[at..=open]);
        let Some(close) = close_paren(line.as_bytes(), open) else {
            at = open + 1;
            continue;
        };
        let inner = &line[open + 1..close];
        match mysql_expr(inner, cs) {
            Some(p) => out.push_str(&p),
            None => out.push_str(inner),
        }
        at = close;
    }
    out.push_str(&line[at..]);
    out
}

/// The table's character set in a `SHOW CREATE TABLE` text
/// (`DEFAULT CHARSET=x`); `utf8mb4` without one.
#[must_use]
pub fn table_charset(text: &str) -> &str {
    text.rfind("DEFAULT CHARSET=")
        .map(|i| &text[i + "DEFAULT CHARSET=".len()..])
        .and_then(|s| s.split([' ', '\n']).next())
        .filter(|s| !s.is_empty())
        .unwrap_or("utf8mb4")
}

/// A `SHOW CREATE TABLE` text with generated columns', expression
/// defaults' and checks' expressions printed the MySQL way.
#[must_use]
pub fn mysql_expressions(text: &str) -> String {
    let cs = table_charset(text).to_owned();
    text.split('\n')
        .map(|line| {
            if line.starts_with("  `") {
                let l = reprint_after(line, " GENERATED ALWAYS AS (", &cs);
                reprint_after(&l, " DEFAULT (", &cs)
            } else if line.starts_with("  CONSTRAINT ") {
                reprint_after(line, " CHECK (", &cs)
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MariaDB 11.8.9's text, and MySQL 8.4.11's for the same column
    /// (captured R4 round 4).
    const PAIRS: &[(&str, &str)] = &[
        ("`a` + 1", "(`a` + 1)"),
        ("`a` * `b` - 2", "((`a` * `b`) - 2)"),
        (
            "concat(`s`,'x',lcase(`s`))",
            "concat(`s`,_utf8mb4'x',lower(`s`))",
        ),
        (
            "case when `a` > 0 then 1 else 0 end",
            "(case when (`a` > 0) then 1 else 0 end)",
        ),
        ("if(`a` is null,0,`a`)", "if((`a` is null),0,`a`)"),
        ("cast(`a` * 2 as unsigned)", "cast((`a` * 2) as unsigned)"),
        ("year(`d`)", "year(`d`)"),
        (
            "json_unquote(json_extract(`j`,'$.k'))",
            "json_unquote(json_extract(`j`,_utf8mb4'$.k'))",
        ),
        // R4 round 5: a JSON string constant `jsonops` compared unquoted.
        (
            "json_extract(`d`,'$.\"version\"') = json_unquote(json_extract('\"stable\"','$'))",
            "(json_extract(`d`,_utf8mb4'$.\"version\"') = json_extract(_utf8mb4'\"stable\"',_utf8mb4'$'))",
        ),
        ("`a` between 1 and 5", "(`a` between 1 and 5)"),
        ("`a` in (1,2,3)", "(`a` in (1,2,3))"),
        ("-`a`", "-(`a`)"),
        ("`a` <> 1", "(`a` <> 1)"),
        ("`a` DIV 2 + `a` MOD 3", "((`a` DIV 2) + (`a` % 3))"),
        ("substr(`s`,1,3)", "substr(`s`,1,3)"),
        ("coalesce(`a`,`b`,0)", "coalesce(`a`,`b`,0)"),
        ("`a` / 3", "(`a` / 3)"),
        (
            "`a` > 1 and `b` < 2 or `a` = 5",
            "(((`a` > 1) and (`b` < 2)) or (`a` = 5))",
        ),
        ("`s` like 'ab%'", "(`s` like _utf8mb4'ab%')"),
        ("abs(`a`)", "abs(`a`)"),
        ("date_format(`d`,'%Y')", "date_format(`d`,_utf8mb4'%Y')"),
        ("`a` > 0 and `b` <> 3", "((`a` > 0) and (`b` <> 3))"),
        ("`s` in ('x','y')", "(`s` in (_utf8mb4'x',_utf8mb4'y'))"),
    ];

    #[test]
    fn mariadb_text_prints_as_mysql() {
        for (m, my) in PAIRS {
            assert_eq!(mysql_expr(m, "utf8mb4").as_deref(), Some(*my), "{m}");
        }
        assert_eq!(mysql_expr("`s` not like 'a%'", "utf8mb4"), None);
        assert_eq!(mysql_expr("`a` + 1 junk", "utf8mb4"), None);
    }

    #[test]
    fn show_create_table_lines() {
        let text = "CREATE TABLE `ex` (\n  `a` int DEFAULT NULL,\n  `s` varchar(20) DEFAULT 'x(',\n  `g1` int GENERATED ALWAYS AS (`a` + 1) VIRTUAL,\n  `g3` varchar(40) GENERATED ALWAYS AS (concat(`s`,')',lcase(`s`))) VIRTUAL,\n  `c1` int DEFAULT (`a` + 1),\n  CONSTRAINT `ck1` CHECK (`a` > 0 and `s` <> ')')\n) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci";
        assert_eq!(
            mysql_expressions(text),
            "CREATE TABLE `ex` (\n  `a` int DEFAULT NULL,\n  `s` varchar(20) DEFAULT 'x(',\n  `g1` int GENERATED ALWAYS AS ((`a` + 1)) VIRTUAL,\n  `g3` varchar(40) GENERATED ALWAYS AS (concat(`s`,_latin1')',lower(`s`))) VIRTUAL,\n  `c1` int DEFAULT ((`a` + 1)),\n  CONSTRAINT `ck1` CHECK (((`a` > 0) and (`s` <> _latin1')')))\n) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci"
        );
    }
}
