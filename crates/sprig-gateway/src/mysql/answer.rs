//! Answers for the statements the gateway handles itself ([`Local`]):
//! persona variables, `KILL` and `SHOW PROCESSLIST`, written to any
//! [`ResultSink`]. The MySQL command loop and the HTTP `Sql` service share
//! them, so both answer the same way (chapter 04, "Dispatch into the
//! layers").
//!
//! `KILL` of a session on another gateway (its id carries another slot)
//! goes through the peer service when the gateway has one (chapter 04,
//! "Registration, KILL and write holds").

use sprig_common::ids::ConnId;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, Persona, StatusFlags};

use super::local::{Local, PersonaVar};
use crate::pipeline::{EndOfRows, ResultSink, RowFormat, RowValues, int_column, text_column};
use crate::session::{Session, SessionTable};

/// What the answers read besides the session.
pub struct LocalEnv<'a> {
    /// This gateway's sessions.
    pub sessions: &'a SessionTable,
    /// The persona the client sees.
    pub persona: Persona,
    /// `@@version` for that persona.
    pub version: &'a str,
    /// `@@max_allowed_packet`.
    pub max_allowed_packet: usize,
    /// `@@hostname`.
    pub hostname: &'a str,
    /// The status flags the answer carries.
    pub status: StatusFlags,
    /// Other gateways, for `KILL` of their sessions.
    pub peers: Option<&'a crate::peer::Peers>,
}

impl std::fmt::Debug for LocalEnv<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalEnv")
            .field("persona", &self.persona)
            .finish_non_exhaustive()
    }
}

/// Answers `l` for `me` into `sink`.
///
/// # Errors
/// The sink's error when the client is gone.
pub async fn answer(
    l: &Local,
    me: &Session,
    env: &LocalEnv<'_>,
    sink: &mut dyn ResultSink,
) -> Result<()> {
    match l {
        Local::Variables(items) => variables(items, env, sink).await,
        Local::Kill { id, query_only } => kill(*id, *query_only, me, env, sink).await,
        Local::ProcessList { full } => process_list(*full, me, env, sink).await,
    }
}

async fn synthesized(
    cols: &[ColumnDef],
    rows: &[Vec<Option<&[u8]>>],
    status: StatusFlags,
    sink: &mut dyn ResultSink,
) -> Result<()> {
    sink.begin_rows(cols, RowFormat::Text).await?;
    for r in rows {
        sink.row(RowValues::Text(r)).await?;
    }
    sink.end_rows(EndOfRows {
        status,
        warnings: 0,
    })
    .await
}

async fn variables(
    items: &[(String, PersonaVar)],
    env: &LocalEnv<'_>,
    sink: &mut dyn ResultSink,
) -> Result<()> {
    let charset = u16::from(env.persona.greeting_collation());
    let mut cols = Vec::with_capacity(items.len());
    let mut vals = Vec::with_capacity(items.len());
    for (name, var) in items {
        cols.push(if var.is_int() {
            int_column(name, true)
        } else {
            text_column(name, charset)
        });
        vals.push(match var {
            PersonaVar::Version => env.version.to_owned(),
            PersonaVar::VersionComment => format!("SprigDB {}", env!("CARGO_PKG_VERSION")),
            PersonaVar::ProtocolVersion => "10".to_owned(),
            PersonaVar::MaxAllowedPacket => env.max_allowed_packet.to_string(),
            PersonaVar::Hostname => env.hostname.to_owned(),
        });
    }
    let row: Vec<Option<&[u8]>> = vals.iter().map(|v| Some(v.as_bytes())).collect();
    synthesized(&cols, &[row], env.status, sink).await
}

/// `KILL [CONNECTION | QUERY] id`. Until chapter 29's privileges, a
/// session may kill only sessions of its own user (MySQL's rule without
/// `CONNECTION_ADMIN`).
async fn kill(
    id: ConnId,
    query_only: bool,
    me: &Session,
    env: &LocalEnv<'_>,
    sink: &mut dyn ResultSink,
) -> Result<()> {
    let user = me.principal.as_ref().map(|p| p.user.clone());
    let tenant = me.principal.as_ref().and_then(|p| p.tenant);
    let unknown = || SprigError::standard(1094, *b"HY000", format!("Unknown thread id: {id}"));
    let not_owner =
        || SprigError::standard(1095, *b"HY000", format!("You are not owner of thread {id}"));
    let Some(target) = env.sessions.get(id) else {
        // Another gateway's slot: ask it (chapter 04 step 10).
        if crate::session::slot_of(id) != env.sessions.slot()
            && let (Some(peers), Some(user)) = (env.peers, user.as_deref())
        {
            return match peers.kill(id, query_only, tenant, user).await {
                Ok(crate::peer::KillOutcome::Done) => sink.ok(OkPacket::new(env.status)).await,
                Ok(crate::peer::KillOutcome::NotOwner) => sink.error(&not_owner()).await,
                Ok(crate::peer::KillOutcome::Unknown) | Err(_) => sink.error(&unknown()).await,
            };
        }
        return sink.error(&unknown()).await;
    };
    let row = target.process_row(env.sessions.clock().now());
    if row.user.is_none() || row.user != user || row.tenant != tenant {
        return sink.error(&not_owner()).await;
    }
    if query_only {
        target.flags().kill_query();
    } else {
        target.flags().kill_conn();
    }
    sink.ok(OkPacket::new(env.status)).await
}

/// `SHOW [FULL] PROCESSLIST` and `COM_PROCESS_INFO`: this gateway's
/// sessions of the same user (chapter 29's `PROCESS` privilege later).
async fn process_list(
    full: bool,
    me: &Session,
    env: &LocalEnv<'_>,
    sink: &mut dyn ResultSink,
) -> Result<()> {
    let charset = u16::from(env.persona.greeting_collation());
    let user = me.principal.as_ref().map(|p| p.user.clone());
    let tenant = me.principal.as_ref().and_then(|p| p.tenant);
    let cols = vec![
        int_column("Id", true),
        text_column("User", charset),
        text_column("Host", charset),
        text_column("db", charset),
        text_column("Command", charset),
        int_column("Time", false),
        text_column("State", charset),
        text_column("Info", charset),
    ];
    let rows: Vec<[Option<String>; 8]> = env
        .sessions
        .list()
        .into_iter()
        .filter(|r| r.user.is_some() && r.user == user && r.tenant == tenant)
        .map(|r| {
            let info = r.info.map(|i| {
                if full {
                    i
                } else {
                    i.chars().take(100).collect()
                }
            });
            [
                Some(r.id.0.to_string()),
                r.user,
                Some(r.host.to_string()),
                r.db,
                Some(r.command.to_owned()),
                Some(r.time.as_secs().to_string()),
                Some(r.state.as_str().to_owned()),
                info,
            ]
        })
        .collect();
    let refs: Vec<Vec<Option<&[u8]>>> = rows
        .iter()
        .map(|r| r.iter().map(|v| v.as_deref().map(str::as_bytes)).collect())
        .collect();
    synthesized(&cols, &refs, env.status, sink).await
}
