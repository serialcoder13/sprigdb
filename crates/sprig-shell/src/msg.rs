//! Stream and queue operations (chapter 53, "Data mode"): publish, tail a
//! stream, take and browse queue messages. Each is the SQL of chapters 45
//! and 50, run like any statement, so privileges and transactions are the
//! gateway's.

use std::process::ExitCode;
use std::time::Duration;

use clap::ArgMatches;

use crate::error::{ExitClass, ShellCode, ShellError};
use crate::exec::Runner;
use crate::lang::Lang;
use crate::model::{Collect, ResultEvent, ResultSet, Sink};
use crate::output::{Format, Writer};

/// A SQL string literal.
#[must_use]
pub fn lit(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for c in s.chars() {
        match c {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\0' => o.push_str("\\0"),
            c => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// A SQL name in back quotes (`db.name` quoted part by part).
#[must_use]
pub fn ident(s: &str) -> String {
    s.split('.')
        .map(|p| format!("`{}`", p.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

/// `INSERT INTO s (key, value[, headers]) VALUES (...)`.
#[must_use]
pub fn publish_sql(stream: &str, key: Option<&str>, value: &str, headers: Option<&str>) -> String {
    let k = key.map_or_else(|| "NULL".to_owned(), lit);
    match headers {
        Some(h) => format!(
            "INSERT INTO {} (`key`, `value`, headers) VALUES ({k}, {}, {})",
            ident(stream),
            lit(value),
            lit(h)
        ),
        None => format!(
            "INSERT INTO {} (`key`, `value`) VALUES ({k}, {})",
            ident(stream),
            lit(value)
        ),
    }
}

/// The newest `last` records, or those after `after` (per partition).
#[must_use]
pub fn tail_sql(stream: &str, partition: Option<u32>, last: u64, after: &[(u32, u64)]) -> String {
    let mut conds = Vec::new();
    if let Some(p) = partition {
        conds.push(format!("partition_no = {p}"));
    }
    if !after.is_empty() {
        let mut ors: Vec<String> = after
            .iter()
            .map(|(p, o)| format!("(partition_no = {p} AND offset_no > {o})"))
            .collect();
        let known: Vec<String> = after.iter().map(|(p, _)| p.to_string()).collect();
        ors.push(format!("partition_no NOT IN ({})", known.join(", ")));
        conds.push(format!("({})", ors.join(" OR ")));
    }
    let wh = if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join(" AND "))
    };
    if after.is_empty() {
        format!(
            "SELECT * FROM (SELECT partition_no, offset_no, ts, `key`, `value` FROM {}{wh} \
             ORDER BY ts DESC, partition_no, offset_no DESC LIMIT {last}) r \
             ORDER BY ts, partition_no, offset_no",
            ident(stream)
        )
    } else {
        format!(
            "SELECT partition_no, offset_no, ts, `key`, `value` FROM {}{wh} \
             ORDER BY ts, partition_no, offset_no LIMIT 1000",
            ident(stream)
        )
    }
}

/// `CALL sprig.mq_publish(...)`.
#[must_use]
pub fn mq_publish_sql(
    exchange: &str,
    routing_key: &str,
    body: &str,
    props: Option<&str>,
) -> String {
    match props {
        Some(p) => format!(
            "CALL sprig.mq_publish({}, {}, {}, {})",
            lit(exchange),
            lit(routing_key),
            lit(body),
            lit(p)
        ),
        None => format!(
            "CALL sprig.mq_publish({}, {}, {})",
            lit(exchange),
            lit(routing_key),
            lit(body)
        ),
    }
}

/// Keeps the last offset per partition from a tail result and passes rows
/// on.
struct Tracker<'a, W: std::io::Write + Send> {
    out: &'a mut Writer<W>,
    positions: &'a mut Vec<(u32, u64)>,
    quiet_header: bool,
}

impl<W: std::io::Write + Send> Sink for Tracker<'_, W> {
    fn event(&mut self, ev: ResultEvent) -> std::io::Result<()> {
        if let ResultEvent::Rows(rows) = &ev {
            for r in rows {
                let p = r.first().cloned().flatten().and_then(|s| s.parse().ok());
                let o = r.get(1).cloned().flatten().and_then(|s| s.parse().ok());
                if let (Some(p), Some(o)) = (p, o) {
                    match self.positions.iter_mut().find(|(q, _)| *q == p) {
                        Some(e) => e.1 = e.1.max(o),
                        None => self.positions.push((p, o)),
                    }
                }
            }
        }
        if self.quiet_header && matches!(ev, ResultEvent::End(_) | ResultEvent::Header(_)) {
            return Ok(());
        }
        self.out.event(ev)
    }
}

fn fail(e: &ShellError) -> ExitCode {
    eprintln!("sprig: {e}");
    ExitCode::from(e.class().code())
}

/// `sprig stream ...` and `sprig queue ...`.
pub async fn cli(runner: &mut Runner, m: &ArgMatches, format: Format) -> ExitCode {
    let mut w = Writer::new(std::io::BufWriter::new(std::io::stdout()), format);
    let r = match m.subcommand() {
        Some(("stream", s)) => stream(runner, s, &mut w, format).await,
        Some(("queue", s)) => queue(runner, m, s, &mut w).await,
        _ => Ok(()),
    };
    let _ = w.finish();
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(&e),
    }
}

async fn stream(
    runner: &mut Runner,
    s: &ArgMatches,
    w: &mut Writer<impl std::io::Write + Send>,
    format: Format,
) -> crate::error::Result<()> {
    match s.subcommand() {
        Some(("publish", p)) => {
            let sql = publish_sql(
                p.get_one::<String>("stream").map_or("", String::as_str),
                p.get_one::<String>("key").map(String::as_str),
                p.get_one::<String>("value").map_or("", String::as_str),
                p.get_one::<String>("headers").map(String::as_str),
            );
            runner.run(&sql, Lang::Sql, w).await
        }
        Some(("tail", t)) => {
            let name = t.get_one::<String>("stream").cloned().unwrap_or_default();
            let part = t.get_one::<u32>("partition").copied();
            let last = t.get_one::<u64>("last").copied().unwrap_or(20);
            let follow = t.get_flag("follow");
            let mut pos: Vec<(u32, u64)> = Vec::new();
            let sql = tail_sql(&name, part, last, &[]);
            {
                let mut tr = Tracker {
                    out: w,
                    positions: &mut pos,
                    quiet_header: false,
                };
                runner.run(&sql, Lang::Sql, &mut tr).await?;
            }
            if !follow {
                return Ok(());
            }
            // Follow: poll every second for records past the last offsets;
            // NDJSON and TSV keep streaming lines, tables print each batch.
            let quiet = matches!(format, Format::Ndjson | Format::Tsv | Format::Csv);
            loop {
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = tokio::signal::ctrl_c() => return Ok(()),
                }
                let sql = tail_sql(&name, part, last, &pos);
                let mut c = Collect::default();
                runner.run(&sql, Lang::Sql, &mut c).await?;
                let got = ResultSet::from_events(&c.0);
                if got.iter().all(|r| r.rows.is_empty()) {
                    continue;
                }
                let mut tr = Tracker {
                    out: w,
                    positions: &mut pos,
                    quiet_header: quiet,
                };
                for ev in c.0 {
                    tr.event(ev)
                        .map_err(|e| ShellError::shell(ShellCode::Cancelled, e.to_string()))?;
                }
                let _ = w.flush();
            }
        }
        _ => Ok(()),
    }
}

#[allow(clippy::many_single_char_names)]
async fn queue(
    runner: &mut Runner,
    m: &ArgMatches,
    s: &ArgMatches,
    w: &mut Writer<impl std::io::Write + Send>,
) -> crate::error::Result<()> {
    let arg = |a: &ArgMatches, k: &str| a.get_one::<String>(k).cloned().unwrap_or_default();
    match s.subcommand() {
        Some(("publish", p)) => {
            let sql = mq_publish_sql(
                &arg(p, "exchange"),
                &arg(p, "routing-key"),
                &arg(p, "body"),
                p.get_one::<String>("properties").map(String::as_str),
            );
            runner.run(&sql, Lang::Sql, w).await
        }
        Some(("browse", b)) => {
            let sql = format!(
                "SELECT seq, routing_key, redelivered, body FROM sprig.mq_browse({}) LIMIT {}",
                lit(&arg(b, "queue")),
                b.get_one::<u64>("limit").copied().unwrap_or(20)
            );
            runner.run(&sql, Lang::Sql, w).await
        }
        Some(("get", g)) => {
            let get = format!(
                "CALL sprig.mq_get({}, {}, {})",
                lit(&arg(g, "queue")),
                g.get_one::<u32>("max").copied().unwrap_or(1),
                g.get_one::<u64>("lease-ms").copied().unwrap_or(30_000)
            );
            if !g.get_flag("ack") {
                return runner.run(&get, Lang::Sql, w).await;
            }
            if !m.get_flag("yes") {
                return Err(ShellError::shell(
                    ShellCode::ConfirmationRequired,
                    "--ack removes the messages; run it with --yes",
                ));
            }
            // Take and acknowledge in one transaction: printed only if the
            // acknowledgements commit.
            runner
                .run("BEGIN", Lang::Sql, &mut Collect::default())
                .await?;
            let mut c = Collect::default();
            let r = async {
                runner.run(&get, Lang::Sql, &mut c).await?;
                let sets = ResultSet::from_events(&c.0);
                for set in &sets {
                    let ti = set.columns.iter().position(|c| c.name == "delivery_token");
                    for row in &set.rows {
                        if let Some(t) = ti.and_then(|i| row.get(i).cloned().flatten()) {
                            let ack = format!("CALL sprig.mq_ack({})", lit(&t));
                            runner.run(&ack, Lang::Sql, &mut Collect::default()).await?;
                        }
                    }
                }
                runner
                    .run("COMMIT", Lang::Sql, &mut Collect::default())
                    .await
            }
            .await;
            if let Err(e) = r {
                let _ = runner
                    .run("ROLLBACK", Lang::Sql, &mut Collect::default())
                    .await;
                return Err(e);
            }
            for ev in c.0 {
                w.event(ev)
                    .map_err(|e| ShellError::shell(ShellCode::Cancelled, e.to_string()))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// The exit class of a message command's error (for tests).
#[must_use]
pub fn class_of(e: &ShellError) -> ExitClass {
    e.class()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_forms() {
        assert_eq!(lit("it's \\ ok"), "'it''s \\\\ ok'");
        assert_eq!(ident("shop.order events"), "`shop`.`order events`");
        assert_eq!(
            publish_sql("order_events", Some("7001"), "{\"a\":1}", None),
            "INSERT INTO `order_events` (`key`, `value`) VALUES ('7001', '{\"a\":1}')"
        );
        let t = tail_sql("s", Some(2), 5, &[(2, 41)]);
        assert!(t.contains("partition_no = 2 AND ((partition_no = 2 AND offset_no > 41) OR partition_no NOT IN (2))"));
        assert_eq!(
            mq_publish_sql("orders", "order.paid", "x", None),
            "CALL sprig.mq_publish('orders', 'order.paid', 'x')"
        );
    }
}
