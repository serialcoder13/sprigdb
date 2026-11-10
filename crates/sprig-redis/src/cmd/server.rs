//! Server and cluster commands (chapter 40, "Server and cluster commands").

use std::fmt::Write as _;

use bytes::Bytes;
use sprig_kv::layout::key_slot;
use sprig_proto_redis::Frame;

use crate::ctx::{Ctx, R, fail, upper};
use crate::reply;
use crate::spec;

/// `TIME`: MariaDB's clock, which expiry also uses.
pub async fn time(cx: &mut Ctx<'_>) -> R<Frame> {
    let rows = cx
        .q("SELECT TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6))")
        .await?;
    let us = rows
        .first()
        .map_or(0, |r| sprig_kv::sql::cell_i64(r[0].as_ref()));
    Ok(Frame::Array(vec![
        Frame::bulk((us / 1_000_000).to_string()),
        Frame::bulk((us % 1_000_000).to_string()),
    ]))
}

/// What `INFO` reports about the connection's gateway.
#[derive(Debug, Clone, Default)]
pub struct InfoFacts {
    /// `redis.server_version`.
    pub version: String,
    /// Seconds since start.
    pub uptime: u64,
    /// Connected clients of this gateway.
    pub clients: usize,
    /// Blocked clients.
    pub blocked: usize,
    /// Gateway id.
    pub gateway_id: String,
    /// Keys per database: `(db, keys, with expiry)`.
    pub keyspace: Vec<(u32, i64, i64)>,
}

/// `INFO [section ...]`: a verbatim string in RESP3.
#[must_use]
pub fn info(f: &InfoFacts, sections: &[Bytes]) -> Frame {
    let want = |s: &str| {
        sections.is_empty()
            || sections.iter().any(|x| {
                let x = upper(x);
                x == "ALL" || x == "EVERYTHING" || x == "DEFAULT" || x.eq_ignore_ascii_case(s)
            })
    };
    let mut out = String::new();
    if want("SERVER") {
        let _ = write!(
            out,
            "# Server\r\nredis_version:{}\r\nredis_mode:standalone\r\nos:SprigDB\r\narch_bits:64\r\n\
             process_id:{}\r\nrun_id:{}\r\ntcp_port:6379\r\nuptime_in_seconds:{}\r\nuptime_in_days:{}\r\n\
             server_name:sprigdb\r\n\r\n",
            f.version,
            std::process::id(),
            f.gateway_id,
            f.uptime,
            f.uptime / 86_400
        );
    }
    if want("CLIENTS") {
        let _ = write!(
            out,
            "# Clients\r\nconnected_clients:{}\r\nblocked_clients:{}\r\n\r\n",
            f.clients, f.blocked
        );
    }
    if want("MEMORY") {
        out.push_str(
            "# Memory\r\nused_memory:0\r\nmaxmemory:0\r\nmaxmemory_policy:noeviction\r\n\r\n",
        );
    }
    if want("PERSISTENCE") {
        out.push_str(
            "# Persistence\r\nloading:0\r\naof_enabled:0\r\nrdb_changes_since_last_save:0\r\n\r\n",
        );
    }
    if want("REPLICATION") {
        out.push_str("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n\r\n");
    }
    if want("CLUSTER") {
        out.push_str("# Cluster\r\ncluster_enabled:0\r\n\r\n");
    }
    if want("KEYSPACE") {
        out.push_str("# Keyspace\r\n");
        for (db, keys, exp) in &f.keyspace {
            if *keys > 0 {
                let _ = write!(out, "db{db}:keys={keys},expires={exp},avg_ttl=0\r\n");
            }
        }
        out.push_str("\r\n");
    }
    while out.ends_with("\r\n\r\n") {
        out.truncate(out.len() - 2);
    }
    Frame::Verbatim(*b"txt", Bytes::from(out))
}

/// `CONFIG GET|SET|RESETSTAT|REWRITE|HELP`.
pub fn config(a: &[Bytes], databases: u32) -> Frame {
    match upper(&a[1]).as_str() {
        "GET" if a.len() >= 3 => {
            let known: Vec<(&str, String)> = vec![
                ("databases", databases.to_string()),
                ("maxmemory", "0".into()),
                ("maxmemory-policy", "noeviction".into()),
                ("notify-keyspace-events", String::new()),
                ("timeout", "0".into()),
                ("save", String::new()),
                ("appendonly", "no".into()),
                ("cluster-enabled", "no".into()),
                ("lua-time-limit", "5000".into()),
                ("busy-reply-threshold", "5000".into()),
                ("proto-max-bulk-len", (16u64 << 20).to_string()),
                ("hash-max-listpack-entries", "128".into()),
                ("set-max-intset-entries", "512".into()),
                ("zset-max-listpack-entries", "128".into()),
            ];
            let mut out = Vec::new();
            for pat in &a[2..] {
                for (k, v) in &known {
                    if crate::glob::matches_nocase(pat, k.as_bytes())
                        && !out
                            .iter()
                            .any(|(x, _): &(Frame, Frame)| *x == Frame::bulk_from(k.as_bytes()))
                    {
                        out.push((Frame::bulk_from(k.as_bytes()), Frame::bulk(v.clone())));
                    }
                }
            }
            Frame::Map(out)
        }
        "SET" if a.len() >= 4 && a.len().is_multiple_of(2) => {
            if a[2..]
                .chunks(2)
                .all(|c| upper(&c[0]) == "NOTIFY-KEYSPACE-EVENTS")
            {
                // Accepted; keyspace notifications come from the feed later.
                Frame::ok()
            } else {
                reply::restricted("config set")
            }
        }
        "RESETSTAT" | "REWRITE" => Frame::ok(),
        _ => reply::unknown_sub("config", &a[1]),
    }
}

/// `COMMAND [COUNT|INFO|DOCS|LIST|GETKEYS]`.
pub fn command(a: &[Bytes]) -> Frame {
    let info = |s: &spec::Spec| {
        let mut flags = Vec::new();
        if s.has('w') {
            flags.push(Frame::simple("write"));
        }
        if s.has('r') {
            flags.push(Frame::simple("readonly"));
        }
        if s.has('s') {
            flags.push(Frame::simple("noscript"));
        }
        if s.has('p') {
            flags.push(Frame::simple("pubsub"));
        }
        if s.has('b') {
            flags.push(Frame::simple("blocking"));
        }
        Frame::Array(vec![
            Frame::bulk_from(s.name.as_bytes()),
            Frame::Integer(i64::from(s.arity)),
            Frame::Set(flags),
            Frame::Integer(i64::from(s.first)),
            Frame::Integer(i64::from(s.last)),
            Frame::Integer(i64::from(s.step)),
            Frame::Set(Vec::new()),
            Frame::Set(Vec::new()),
            Frame::Set(Vec::new()),
            Frame::Array(Vec::new()),
        ])
    };
    let built = || spec::ALL.iter().filter(|s| !s.has('x'));
    match a.get(1).map(upper).as_deref() {
        None => Frame::Array(built().map(info).collect()),
        Some("COUNT") => Frame::Integer(built().count() as i64),
        Some("LIST") => Frame::Array(
            built()
                .map(|s| Frame::bulk_from(s.name.as_bytes()))
                .collect(),
        ),
        Some("INFO") => {
            if a.len() == 2 {
                return Frame::Array(built().map(info).collect());
            }
            Frame::Array(
                a[2..]
                    .iter()
                    .map(|n| {
                        spec::lookup(&String::from_utf8_lossy(n).to_ascii_lowercase())
                            .filter(|s| !s.has('x'))
                            .map_or(Frame::null_array(), info)
                    })
                    .collect(),
            )
        }
        Some("DOCS") => Frame::Map(Vec::new()),
        Some("GETKEYS") if a.len() >= 3 => {
            let name = String::from_utf8_lossy(&a[2]).to_ascii_lowercase();
            match spec::lookup(&name) {
                Some(s) if s.arity_ok(a.len() - 2) => {
                    let ks = s.key_indexes(a.len() - 2);
                    if ks.is_empty() {
                        return Frame::err("The command has no key arguments");
                    }
                    Frame::Array(
                        ks.into_iter()
                            .map(|i| Frame::Bulk(a[i + 2].clone()))
                            .collect(),
                    )
                }
                Some(_) => Frame::err("Invalid number of arguments specified for command"),
                None => Frame::err("Invalid command specified"),
            }
        }
        Some(_) => reply::unknown_sub("command", &a[1]),
    }
}

/// `CLUSTER SLOTS|SHARDS|NODES|INFO|KEYSLOT|MYID` for one standalone node
/// owning every slot at `addr` (`redis.advertise_address`).
pub fn cluster(a: &[Bytes], addr: &str, id: &str) -> Frame {
    let (host, port) = addr.rsplit_once(':').unwrap_or((addr, "6379"));
    let port: i64 = port.parse().unwrap_or(6379);
    let node_id = format!(
        "{:0>40}",
        id.chars()
            .filter(char::is_ascii_alphanumeric)
            .take(40)
            .collect::<String>()
    );
    match upper(&a[1]).as_str() {
        "KEYSLOT" if a.len() == 3 => Frame::Integer(i64::from(key_slot(&a[2]))),
        "SLOTS" => Frame::Array(vec![Frame::Array(vec![
            Frame::Integer(0),
            Frame::Integer(16383),
            Frame::Array(vec![
                Frame::bulk(host.to_owned()),
                Frame::Integer(port),
                Frame::bulk(node_id),
            ]),
        ])]),
        "SHARDS" => Frame::Array(vec![Frame::Map(vec![
            (
                Frame::bulk("slots"),
                Frame::Array(vec![Frame::Integer(0), Frame::Integer(16383)]),
            ),
            (
                Frame::bulk("nodes"),
                Frame::Array(vec![Frame::Map(vec![
                    (Frame::bulk("id"), Frame::bulk(node_id)),
                    (Frame::bulk("port"), Frame::Integer(port)),
                    (Frame::bulk("ip"), Frame::bulk(host.to_owned())),
                    (Frame::bulk("endpoint"), Frame::bulk(host.to_owned())),
                    (Frame::bulk("role"), Frame::bulk("master")),
                    (Frame::bulk("replication-offset"), Frame::Integer(0)),
                    (Frame::bulk("health"), Frame::bulk("online")),
                ])]),
            ),
        ])]),
        "NODES" => Frame::bulk(format!(
            "{node_id} {host}:{port}@{} myself,master - 0 0 1 connected 0-16383\n",
            port + 10_000
        )),
        "MYID" => Frame::bulk(node_id),
        "INFO" => Frame::Verbatim(
            *b"txt",
            Bytes::from(
                "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\n\
                 cluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\n\
                 cluster_size:1\r\ncluster_current_epoch:1\r\ncluster_my_epoch:1\r\n",
            ),
        ),
        _ => reply::unknown_sub("cluster", &a[1]),
    }
}

/// `ACL WHOAMI|CAT|LIST|USERS|GETUSER` (read-only, chapter 40 "Security").
pub fn acl(a: &[Bytes], user: &str) -> Frame {
    match upper(&a[1]).as_str() {
        "WHOAMI" => Frame::bulk(user.to_owned()),
        "USERS" => Frame::Array(vec![Frame::bulk(user.to_owned())]),
        "LIST" => Frame::Array(vec![Frame::bulk(format!(
            "user {user} on sanitize-payload ~* &* +@all"
        ))]),
        "CAT" => Frame::Array(
            [
                "keyspace",
                "read",
                "write",
                "set",
                "sortedset",
                "list",
                "hash",
                "string",
                "bitmap",
                "geo",
                "pubsub",
                "admin",
                "fast",
                "slow",
                "blocking",
                "connection",
                "transaction",
                "scripting",
            ]
            .iter()
            .map(|c| Frame::bulk(c.to_string()))
            .collect(),
        ),
        "GETUSER" if a.len() == 3 => {
            if &a[2][..] != user.as_bytes() {
                return Frame::null();
            }
            Frame::Map(vec![
                (Frame::bulk("flags"), Frame::Array(vec![Frame::bulk("on")])),
                (Frame::bulk("passwords"), Frame::Array(Vec::new())),
                (Frame::bulk("commands"), Frame::bulk("+@all")),
                (Frame::bulk("keys"), Frame::bulk("~*")),
                (Frame::bulk("channels"), Frame::bulk("&*")),
            ])
        }
        "SETUSER" | "DELUSER" | "LOAD" | "SAVE" | "DRYRUN" | "GENPASS" | "LOG" => {
            reply::restricted("acl setuser")
        }
        _ => reply::unknown_sub("acl", &a[1]),
    }
}

/// `SLOWLOG`, `LATENCY`, `MEMORY`, `LOLWUT` and other informational
/// commands with fixed answers.
pub fn fixed(name: &str, a: &[Bytes]) -> R<Frame> {
    Ok(match name {
        "slowlog" => match upper(&a[1]).as_str() {
            "GET" => Frame::Array(Vec::new()),
            "LEN" => Frame::Integer(0),
            "RESET" => Frame::ok(),
            _ => reply::unknown_sub("slowlog", &a[1]),
        },
        "latency" => match upper(&a[1]).as_str() {
            "LATEST" | "HISTORY" => Frame::Array(Vec::new()),
            "RESET" => Frame::Integer(0),
            _ => reply::unknown_sub("latency", &a[1]),
        },
        "memory" => match upper(&a[1]).as_str() {
            "USAGE" => Frame::null(),
            "DOCTOR" => Frame::bulk("Sam, I have no memory problems"),
            "STATS" => Frame::Map(Vec::new()),
            "PURGE" => Frame::ok(),
            _ => reply::unknown_sub("memory", &a[1]),
        },
        "lolwut" => Frame::Verbatim(*b"txt", Bytes::from_static(b"SprigDB ver. 7.2.0\n")),
        "role" => Frame::Array(vec![
            Frame::bulk("master"),
            Frame::Integer(0),
            Frame::Array(Vec::new()),
        ]),
        "lastsave" => Frame::Integer(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64),
        ),
        "save" | "bgrewriteaof" | "readonly" | "readwrite" | "asking" => Frame::ok(),
        "bgsave" => Frame::simple("Background saving started"),
        _ => return fail(reply::unknown(name.as_bytes(), &a[1..])),
    })
}
