//! Prints a node's binlog events, for debugging.
//!
//! ```text
//! cargo run -p sprig-binlog --example binlog-dump -- \
//!     mysql://root:sprig-dev-root@127.0.0.1:13301 [GTID position] [max events]
//! cargo run -p sprig-binlog --example binlog-dump -- path/to/mariadb-bin.000001
//! ```

use sprig_binlog::{BinlogFile, BinlogSource, BinlogStream, Event, GtidPos, StreamOptions};

#[tokio::main]
async fn main() -> sprig_common::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(target) = args.first() else {
        eprintln!("usage: binlog-dump <mysql://user:pw@host:port | binlog file> [from] [max]");
        std::process::exit(2);
    };
    let from: GtidPos = args.get(1).map_or(Ok(GtidPos::new()), |s| s.parse())?;
    let max: usize = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(usize::MAX);
    if target.contains("://") {
        let src = BinlogSource::from_url(target)?;
        let mut opts = StreamOptions::new(4_000_000_999);
        if let Ok(c) = std::env::var("BINLOG_DUMP_CAPABILITY") {
            opts.capability = c.parse().unwrap_or(4);
        }
        opts.annotate_rows = true;
        let mut s = BinlogStream::connect(&src, &from, &opts).await?;
        for _ in 0..max {
            let ev = s.next().await?;
            print(&ev);
            if matches!(ev, Event::Heartbeat) {
                break;
            }
        }
        println!("position: {}", s.position());
    } else {
        let mut f = BinlogFile::open(target).await?;
        let mut n = 0;
        while let Some(ev) = f.next().await? {
            print(&ev);
            n += 1;
            if n >= max {
                break;
            }
        }
        println!("position: {}", f.position());
    }
    Ok(())
}

fn print(ev: &Event) {
    match ev {
        Event::Rows {
            table, kind, rows, ..
        } => {
            println!(
                "{kind:?} {}.{} ({} rows)",
                table.schema,
                table.table,
                rows.len()
            );
            for r in rows {
                println!("  before {:?}", r.before.as_ref().map(|i| &i.0));
                println!("  after  {:?}", r.after.as_ref().map(|i| &i.0));
            }
        }
        Event::TableMap(t) => println!(
            "TableMap {} {}.{} {:?}",
            t.table_id,
            t.schema,
            t.table,
            t.columns
                .iter()
                .map(|c| (c.name.clone(), c.kind))
                .collect::<Vec<_>>()
        ),
        other => println!("{other:?}"),
    }
}
