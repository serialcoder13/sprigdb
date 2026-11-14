//! Checks on SQL a model hands to a read tool, before the tool host sees
//! it. The host still runs reads in a read-only transaction as the user;
//! this refuses early, with a message the model can act on.

use sqlparser::ast::{Query, SetExpr, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

fn one(sql: &str) -> Result<Statement, String> {
    let mut stmts = Parser::parse_sql(&MySqlDialect {}, sql)
        .map_err(|e| format!("the SQL doesn't parse: {e}"))?;
    match stmts.len() {
        1 => Ok(stmts.remove(0)),
        0 => Err("there is no statement".to_owned()),
        _ => Err("send one statement at a time".to_owned()),
    }
}

fn query_reads(q: &Query) -> Result<(), String> {
    if !q.locks.is_empty() {
        return Err("locking reads (FOR UPDATE, FOR SHARE) aren't allowed".to_owned());
    }
    if let Some(w) = &q.with {
        for cte in &w.cte_tables {
            query_reads(&cte.query)?;
        }
    }
    set_reads(&q.body)
}

fn set_reads(s: &SetExpr) -> Result<(), String> {
    match s {
        SetExpr::Select(sel) => {
            if sel.into.is_some() {
                Err("SELECT ... INTO isn't allowed".to_owned())
            } else {
                Ok(())
            }
        }
        SetExpr::Query(q) => query_reads(q),
        SetExpr::SetOperation { left, right, .. } => {
            set_reads(left)?;
            set_reads(right)
        }
        SetExpr::Values(_) | SetExpr::Table(_) => Ok(()),
        _ => Err("only reads are allowed here; propose changes with propose_sql".to_owned()),
    }
}

/// `sql` is one SELECT (or VALUES, TABLE, a set operation of them), with
/// no INTO and no locking clause.
///
/// # Errors
/// Why not, for the model.
pub fn read_only(sql: &str) -> Result<(), String> {
    match one(sql)? {
        Statement::Query(q) => query_reads(&q),
        _ => Err("only one SELECT is allowed here; propose changes with propose_sql".to_owned()),
    }
}

/// `sql` is one statement EXPLAIN may plan without running it: a read,
/// INSERT, UPDATE or DELETE. Never EXPLAIN or ANALYZE itself, which could
/// run the statement.
///
/// # Errors
/// Why not, for the model.
pub fn explainable(sql: &str) -> Result<(), String> {
    match one(sql)? {
        Statement::Query(q) => query_reads(&q),
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => Ok(()),
        _ => Err("explain takes one SELECT, INSERT, UPDATE or DELETE".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_pass_and_everything_else_is_refused() {
        for ok in [
            "SELECT 1",
            "select a, count(*) from t where b = 'x' group by a",
            "WITH c AS (SELECT id FROM t) SELECT * FROM c",
            "SELECT a FROM t UNION SELECT a FROM u",
        ] {
            assert!(read_only(ok).is_ok(), "{ok}");
            assert!(explainable(ok).is_ok(), "{ok}");
        }
        for bad in [
            "DELETE FROM t",
            "UPDATE t SET a = 1",
            "DROP TABLE t",
            "SELECT 1; DROP TABLE t",
            "SELECT * FROM t FOR UPDATE",
            "SELECT a INTO @x FROM t",
            "SET autocommit = 0",
            "",
            "not sql",
        ] {
            assert!(read_only(bad).is_err(), "{bad}");
        }
        assert!(explainable("UPDATE t SET a = 1 WHERE id = 2").is_ok());
        assert!(explainable("DELETE FROM t WHERE id = 2").is_ok());
        assert!(explainable("INSERT INTO t VALUES (1)").is_ok());
        assert!(explainable("EXPLAIN SELECT 1").is_err());
        assert!(explainable("DROP TABLE t").is_err());
    }
}
