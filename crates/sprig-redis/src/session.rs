//! One Redis connection: login, dispatch and routing, `MULTI`, `EXEC`
//! and `WATCH`, blocking commands and pub/sub.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_kv::layout::live;
use sprig_kv::sql::{cell_u64, ident, lit};
use sprig_kv::{SqlError, SqlRunner};
use sprig_proto_redis::num::{parse_f64, parse_i64};
use sprig_proto_redis::{Command, Frame, Proto};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::cmd;
use crate::cmd::lists::End;
use crate::cmd::server;
use crate::ctx::{Ctx, Fail, upper};
use crate::env::Env;
use crate::fabric::Interest;
use crate::lua;
use crate::pubsub::{Kind, Msg};
use crate::reply;
use crate::spec::{self, Spec};

/// A logged-in principal: its SQL, its tenant and its name.
pub struct Login {
    /// The principal's own SQL session.
    pub sql: Box<dyn SqlRunner>,
    /// The tenant (scopes channels, waits, scripts).
    pub scope: String,
    /// The user name (`ACL WHOAMI`).
    pub user: String,
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Login")
            .field("scope", &self.scope)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

/// Logins (the gateway's authentication; a plain connection in tests).
#[async_trait]
pub trait Auth: Send + Sync {
    /// Logs `user` in (`None`: one-argument `AUTH`). `Err` holds the
    /// `-...` reply.
    async fn login(&self, user: Option<&[u8]>, pass: &[u8]) -> Result<Login, Frame>;
}

/// The `WRONGPASS` reply.
#[must_use]
pub fn wrongpass() -> Frame {
    Frame::error("WRONGPASS invalid username-password pair or user is disabled.")
}

/// The scripts table in the tenant's Redis database.
fn scripts_table(database: &str) -> String {
    format!("{}.{}", ident(database), ident("__sprig_redis_scripts"))
}

/// One connection's state.
pub struct Session {
    env: Arc<Env>,
    auth: Arc<dyn Auth>,
    /// `CLIENT ID`.
    pub id: u64,
    login: Option<Login>,
    /// RESP2 or RESP3.
    pub proto: Proto,
    db: u32,
    name: Option<Bytes>,
    lib: (Option<Bytes>, Option<Bytes>),
    multi: Option<Vec<Command>>,
    multi_err: bool,
    watched: Vec<(u32, Bytes, Option<u64>)>,
    subs: BTreeSet<Bytes>,
    psubs: BTreeSet<Bytes>,
    ssubs: BTreeSet<Bytes>,
    tx: UnboundedSender<Msg>,
    rx: Option<UnboundedReceiver<Msg>>,
    /// The client sent `QUIT`: close after replying.
    pub closing: bool,
    peer: String,
    created: Instant,
    /// The data center named by `CLIENT SETNAME` (`app~fra1`).
    pub dc_hint: Option<String>,
    /// A fixed namespace in place of the Redis databases (the API's
    /// `Exec`).
    ns_override: Option<sprig_kv::Namespace>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id)
            .field("db", &self.db)
            .finish_non_exhaustive()
    }
}

/// Waits a little before rerunning after a conflict: random, growing.
async fn backoff(attempt: u32) {
    let cap = 1u64 << attempt.min(6);
    let ms = crate::ctx::new_version() % cap + 1;
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// The hub interest of a subscription.
fn interest(kind: Kind, name: &Bytes) -> Interest {
    match kind {
        Kind::Channel => Interest::Channel(name.clone()),
        Kind::Pattern => Interest::Pattern(name.clone()),
        Kind::Shard => Interest::Shard(name.clone()),
    }
}

/// A command's outcome inside `EXEC` or a script.
type Step = Result<Vec<Frame>, SqlError>;

impl Session {
    /// A new, unauthenticated connection from `peer`.
    pub fn new(env: Arc<Env>, auth: Arc<dyn Auth>, peer: &str) -> Self {
        let (tx, rx) = unbounded_channel();
        let id = env.conn_id();
        Self {
            env,
            auth,
            id,
            login: None,
            proto: Proto::Resp2,
            db: 0,
            name: None,
            lib: (None, None),
            multi: None,
            multi_err: false,
            watched: Vec::new(),
            subs: BTreeSet::new(),
            psubs: BTreeSet::new(),
            ssubs: BTreeSet::new(),
            tx,
            rx: Some(rx),
            closing: false,
            peer: peer.to_owned(),
            created: Instant::now(),
            dc_hint: None,
            ns_override: None,
        }
    }

    /// A logged-in session on one namespace (the API's `Exec`).
    pub fn with_login(
        env: Arc<Env>,
        auth: Arc<dyn Auth>,
        login: Login,
        ns: sprig_kv::Namespace,
    ) -> Self {
        let mut s = Self::new(env, auth, "api");
        s.login = Some(login);
        s.ns_override = Some(ns);
        s
    }

    /// The session's login, back (the API returns its SQL session).
    pub fn into_login(mut self) -> Option<Login> {
        self.login.take()
    }

    /// Takes the pub/sub message stream (the connection writes them).
    pub fn take_messages(&mut self) -> Option<UnboundedReceiver<Msg>> {
        self.rx.take()
    }

    /// Whether the connection is logged in.
    #[must_use]
    pub fn authenticated(&self) -> bool {
        self.login.is_some()
    }

    /// A pub/sub message as a frame for this connection.
    #[must_use]
    pub fn message_frame(&self, m: Msg) -> Frame {
        let items = match m {
            Msg::Message(c, d) => vec![Frame::bulk("message"), Frame::Bulk(c), Frame::Bulk(d)],
            Msg::PMessage(p, c, d) => vec![
                Frame::bulk("pmessage"),
                Frame::Bulk(p),
                Frame::Bulk(c),
                Frame::Bulk(d),
            ],
            Msg::SMessage(c, d) => vec![Frame::bulk("smessage"), Frame::Bulk(c), Frame::Bulk(d)],
        };
        Frame::Push(items)
    }

    fn subscribed(&self) -> bool {
        !(self.subs.is_empty() && self.psubs.is_empty() && self.ssubs.is_empty())
    }

    fn resp3(&self) -> bool {
        self.proto == Proto::Resp3
    }

    /// Ends the connection: subscriptions go, a transaction rolls back.
    pub async fn close(&mut self) {
        self.unsubscribe_all();
        if let Some(l) = &mut self.login {
            let _ = l.sql.exec("ROLLBACK").await;
        }
    }

    fn unsubscribe_all(&mut self) {
        let Some(scope) = self.login.as_ref().map(|l| l.scope.clone()) else {
            return;
        };
        for c in std::mem::take(&mut self.subs) {
            self.env.hub.unsubscribe(&scope, Kind::Channel, &c, self.id);
            self.env.release(&scope, Interest::Channel(c));
        }
        for c in std::mem::take(&mut self.psubs) {
            self.env.hub.unsubscribe(&scope, Kind::Pattern, &c, self.id);
            self.env.release(&scope, Interest::Pattern(c));
        }
        for c in std::mem::take(&mut self.ssubs) {
            self.env.hub.unsubscribe(&scope, Kind::Shard, &c, self.id);
            self.env.release(&scope, Interest::Shard(c));
        }
    }

    /// Handles one command; its replies in order (several for the
    /// subscribe family).
    pub async fn handle(&mut self, c: Command) -> Vec<Frame> {
        let Some(first) = c.args.first() else {
            return Vec::new();
        };
        let name = String::from_utf8_lossy(first).to_ascii_lowercase();
        let Some(spec) = spec::lookup(&name) else {
            if self.multi.is_some() {
                self.multi_err = true;
            }
            return vec![reply::unknown(first, &c.args[1..])];
        };
        if !spec.arity_ok(c.args.len()) {
            if self.multi.is_some() {
                self.multi_err = true;
            }
            return vec![reply::arity(&name)];
        }
        if self.login.is_none() && !spec.has('n') {
            if self.multi.is_some() {
                self.multi_err = true;
            }
            return vec![Frame::error("NOAUTH Authentication required.")];
        }
        if self.subscribed() && self.proto == Proto::Resp2 && !spec.has('S') {
            return vec![Frame::err(&format!(
                "Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
            ))];
        }
        if let Some(q) = &mut self.multi
            && !spec.has('m')
        {
            if q.len() >= self.env.config().max_multi_commands {
                self.multi_err = true;
                return vec![reply::sprig(
                    sprig_common::SprigCode::RedisMultiTooLarge,
                    "too many commands queued (redis.max_multi_commands)",
                )];
            }
            q.push(c);
            return vec![Frame::simple("QUEUED")];
        }
        // Keyspace notifications (crate::keyspace): a write's events, or
        // EXEC's commands' events, once committed.
        let queued = (name == "exec")
            .then(|| self.multi.clone())
            .flatten()
            .filter(|_| !self.multi_err);
        // A write command whose events crate::keyspace knows sends
        // them itself, and its writes are marked so the gateways'
        // notifications from the change feed pass over them; the feed
        // sends the others' (scripts, commands it doesn't know, and an
        // EXEC holding any of those).
        let mark = match &queued {
            Some(q) => q.iter().all(|c| {
                let n = c
                    .args
                    .first()
                    .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
                    .unwrap_or_default();
                !spec::lookup(&n).is_some_and(|s| s.has('w')) || crate::keyspace::covered(&n)
            }),
            None => spec.has('w') && crate::keyspace::covered(&name),
        };
        if mark && let Some(l) = self.login.as_mut() {
            l.sql.notified(true);
        }
        let frames = match self.dispatch(&name, spec, &c.args, false).await {
            Ok(f) => f,
            Err(e) => vec![reply::backend(&e)],
        };
        if mark && let Some(l) = self.login.as_mut() {
            l.sql.notified(false);
        }
        if mark
            && (spec.has('w') || queued.is_some())
            && let Some(scope) = self.login.as_ref().map(|l| l.scope.clone())
        {
            Self::notify(
                Arc::clone(&self.env),
                scope,
                self.db,
                &c.args,
                queued.as_deref(),
                &frames,
            )
            .await;
        }
        frames
    }

    /// Publishes the keyspace events of a command that answered `frames`
    /// (EXEC's: `queued` with its replies), under tenant `scope`'s flags.
    async fn notify(
        env: Arc<Env>,
        scope: String,
        db: u32,
        a: &[Bytes],
        queued: Option<&[Command]>,
        frames: &[Frame],
    ) {
        let flags =
            crate::keyspace::parse_flags(env.keyspace_flags(&scope).as_bytes()).unwrap_or(0);
        if flags == 0 {
            return;
        }
        let mut evs = Vec::new();
        match (queued, frames.first()) {
            (Some(q), Some(Frame::Array(replies))) => {
                for (c, r) in q.iter().zip(replies) {
                    evs.extend(crate::keyspace::events(&c.args, r));
                }
            }
            (None, Some(f)) => evs = crate::keyspace::events(a, f),
            _ => {}
        }
        for (channel, data) in crate::keyspace::publishes(flags, db, &evs) {
            if let Err(e) = env.publish(&scope, &channel, &data, false).await {
                tracing::debug!(error = %e, "keyspace notification not published");
            }
        }
    }

    /// A pub/sub message as this connection may see it (row-level
    /// security): a keyspace notification goes out only when the session may
    /// read its key (the gateway's [`crate::keyspace::KeyFilter`]).
    pub async fn deliver(&mut self, m: Msg) -> Option<Frame> {
        let (channel, data) = match &m {
            Msg::Message(c, d) | Msg::PMessage(_, c, d) | Msg::SMessage(c, d) => (c, d),
        };
        if let Some((db, key)) = crate::keyspace::key_of(channel, data)
            && let Some(f) = self.env.key_filter().cloned()
        {
            let cfg = self.env.config();
            let ns = self
                .ns_override
                .clone()
                .unwrap_or_else(|| sprig_kv::Namespace::redis(&cfg.logical_database, db));
            let login = self.login.as_mut()?;
            let sql = f.statement(&ns.database, &ns.table, std::slice::from_ref(&key));
            let ok = match login.sql.query(&sql).await {
                Ok(rows) => f.visible(&rows, 1).first().copied().unwrap_or(false),
                Err(e) => {
                    tracing::debug!(error = %e, "keyspace notification check failed; not delivered");
                    false
                }
            };
            if !ok {
                return None;
            }
        }
        Some(self.message_frame(m))
    }

    /// Runs a command. `in_txn`: inside `EXEC` or a script, whose
    /// transaction the caller owns; a retryable backend error is returned
    /// so the caller reruns the whole unit.
    #[allow(clippy::too_many_lines)]
    async fn dispatch(&mut self, name: &str, spec: &Spec, a: &[Bytes], in_txn: bool) -> Step {
        let one = |f: Frame| Ok(vec![f]);
        match name {
            "ping" => {
                if a.len() > 2 {
                    return one(reply::arity("ping"));
                }
                if self.subscribed() && self.proto == Proto::Resp2 {
                    return one(Frame::Array(vec![
                        Frame::bulk("pong"),
                        Frame::Bulk(a.get(1).cloned().unwrap_or_default()),
                    ]));
                }
                one(a
                    .get(1)
                    .map_or_else(|| Frame::simple("PONG"), |m| Frame::Bulk(m.clone())))
            }
            "echo" => one(Frame::Bulk(a[1].clone())),
            "quit" => {
                self.closing = true;
                one(Frame::ok())
            }
            "reset" => {
                self.multi = None;
                self.multi_err = false;
                self.watched.clear();
                self.unsubscribe_all();
                self.db = 0;
                self.proto = Proto::Resp2;
                self.name = None;
                if let Some(l) = &mut self.login {
                    let _ = l.sql.exec("ROLLBACK").await;
                }
                one(Frame::simple("RESET"))
            }
            "select" => {
                let Some(n) = parse_i64(&a[1]) else {
                    return one(reply::not_int());
                };
                if n < 0 || n >= i64::from(self.env.config().databases) {
                    return one(Frame::err("DB index is out of range"));
                }
                self.db = u32::try_from(n).unwrap_or(0);
                one(Frame::ok())
            }
            "hello" => Ok(vec![self.hello(a).await]),
            "auth" => Ok(vec![self.auth_cmd(a).await]),
            "client" => one(self.client(a)),
            "swapdb" => one(reply::unsupported(
                "swapdb",
                "not supported in this version",
            )),
            "multi" => {
                if self.multi.is_some() {
                    return one(Frame::err("MULTI calls can not be nested"));
                }
                self.multi = Some(Vec::new());
                self.multi_err = false;
                one(Frame::ok())
            }
            "discard" => {
                if self.multi.take().is_none() {
                    return one(Frame::err("DISCARD without MULTI"));
                }
                self.multi_err = false;
                self.watched.clear();
                one(Frame::ok())
            }
            "exec" => Ok(vec![self.exec().await]),
            "watch" => {
                if self.multi.is_some() {
                    return one(Frame::err("WATCH inside MULTI is not allowed"));
                }
                one(self.watch(&a[1..]).await)
            }
            "unwatch" => {
                self.watched.clear();
                one(Frame::ok())
            }
            "subscribe" | "psubscribe" | "ssubscribe" => {
                if in_txn {
                    return one(Frame::err(&format!(
                        "{} isn't allowed inside MULTI or scripts",
                        name.to_ascii_uppercase()
                    )));
                }
                Ok(self.subscribe(name, &a[1..]).await)
            }
            "unsubscribe" | "punsubscribe" | "sunsubscribe" => Ok(self.unsubscribe(name, &a[1..])),
            "publish" | "spublish" => {
                let scope = self
                    .login
                    .as_ref()
                    .map(|l| l.scope.clone())
                    .unwrap_or_default();
                match self
                    .env
                    .publish(&scope, &a[1], &a[2], name == "spublish")
                    .await
                {
                    Ok(n) => one(Frame::Integer(n)),
                    Err(e) => one(reply::sprig(
                        sprig_common::SprigCode::RedisPubsubUnavailable,
                        &format!("no pub/sub hub reachable: {e}"),
                    )),
                }
            }
            "pubsub" => one(self.pubsub(a).await),
            "eval" | "evalsha" | "eval_ro" | "evalsha_ro" => {
                Box::pin(self.eval(name, a, in_txn)).await
            }
            "fcall" | "fcall_ro" => Box::pin(self.fcall(name, a, in_txn)).await,
            "script" => Ok(vec![self.script(a).await]),
            "function" => Ok(vec![self.function(a).await]),
            "blpop" | "brpop" | "blmove" | "brpoplpush" | "blmpop" | "bzpopmin" | "bzpopmax"
            | "bzmpop" => self.blocking(name, a, in_txn).await,
            "info" => Ok(vec![self.info(&a[1..]).await]),
            "config" => {
                // CONFIG SET notify-keyspace-events: this tenant's flags on
                // this gateway (crate::keyspace).
                let scope = self
                    .login
                    .as_ref()
                    .map(|l| l.scope.clone())
                    .unwrap_or_default();
                if a.len() >= 4
                    && String::from_utf8_lossy(&a[1]).eq_ignore_ascii_case("set")
                    && a[2..].chunks(2).all(|c| {
                        String::from_utf8_lossy(&c[0])
                            .eq_ignore_ascii_case("notify-keyspace-events")
                    })
                {
                    for c in a[2..].chunks(2) {
                        let Some(f) = c.get(1).and_then(|v| crate::keyspace::parse_flags(v)) else {
                            return one(Frame::err(
                                "CONFIG SET failed (possibly related to argument \
                                 'notify-keyspace-events') - Invalid event class character. Use \
                                 'Ag$lshzxeKEtmdn'.",
                            ));
                        };
                        self.env
                            .set_keyspace_flags(&scope, crate::keyspace::flags_text(f));
                    }
                }
                let flags = self.env.keyspace_flags(&scope);
                one(server::config(a, self.env.config().databases, &flags))
            }
            "command" => one(server::command(a)),
            "cluster" => one(server::cluster(a, "127.0.0.1:6379", &self.env.gateway_id)),
            "acl" => {
                let user = self
                    .login
                    .as_ref()
                    .map_or("default", |l| l.user.as_str())
                    .to_owned();
                one(server::acl(a, &user))
            }
            "slowlog" | "latency" | "memory" | "lolwut" | "role" | "lastsave" | "save"
            | "bgsave" | "bgrewriteaof" | "readonly" | "readwrite" | "asking" => {
                one(server::fixed(name, a).unwrap_or_else(|f| match f {
                    Fail::Reply(f) => f,
                    Fail::Sql(e) => reply::backend(&e),
                }))
            }
            _ if spec.has('x') => one(cmd::unsupported(name)),
            _ => self.data(name, spec, a, in_txn).await.map(|f| vec![f]),
        }
    }

    /// Creates database `db`'s tables if this gateway hasn't yet.
    async fn ensure_db(&mut self, db: u32) -> Result<(), SqlError> {
        let env = Arc::clone(&self.env);
        let Some(login) = self.login.as_mut() else {
            return Ok(());
        };
        let mut cx = Ctx::new(login.sql.as_mut(), &env, &login.scope, db, false);
        if let Some(ns) = &self.ns_override {
            cx.ns = ns.clone();
        }
        let ns = cx.ns.clone();
        cx.ensure(&ns).await
    }

    /// A data command, in its own transaction when it writes.
    async fn data(
        &mut self,
        name: &str,
        spec: &Spec,
        a: &[Bytes],
        in_txn: bool,
    ) -> Result<Frame, SqlError> {
        let env = Arc::clone(&self.env);
        let cfg = env.config();
        if !in_txn {
            // MOVE and COPY ... DB n reach another database.
            let mut dbs = vec![self.db];
            if name == "move"
                && let Some(n) = parse_i64(&a[2])
                && (0..i64::from(cfg.databases)).contains(&n)
            {
                dbs.push(u32::try_from(n).unwrap_or(0));
            }
            if name == "copy"
                && let Some(i) = a.iter().position(|x| upper(x) == "DB")
                && let Some(n) = a.get(i + 1).and_then(|x| parse_i64(x))
                && (0..i64::from(cfg.databases)).contains(&n)
            {
                dbs.push(u32::try_from(n).unwrap_or(0));
            }
            if name == "flushall" {
                dbs.clear();
            }
            for db in dbs {
                if let Err(e) = self.ensure_db(db).await {
                    return Ok(reply::backend(&e));
                }
            }
        }
        // The hottest writes as one guarded batch.
        if !in_txn
            && cfg.guarded_batches
            && let Some(f) = self.batched(name, a).await
        {
            return Ok(f);
        }
        let write = spec.has('w');
        // FLUSHALL and FLUSHDB empty tables statement by statement (not
        // atomic across databases or shards, as on a Redis Cluster): a
        // failure inside a gateway transaction would poison the rest.
        let own = write && !in_txn && !matches!(name, "flushall" | "flushdb");
        let resp3 = self.resp3();
        let db = self.db;
        let mut attempt = 0;
        let mut recreated = false;
        loop {
            let Some(login) = self.login.as_mut() else {
                return Ok(Frame::error("NOAUTH Authentication required."));
            };
            let scope = login.scope.clone();
            let mut cx = Ctx::new(login.sql.as_mut(), &env, &scope, db, resp3);
            if let Some(ns) = &self.ns_override {
                cx.ns = ns.clone();
            }
            cx.in_txn = in_txn;
            if own && let Err(e) = cx.x("START TRANSACTION").await {
                return Ok(reply::backend(&e));
            }
            let r = cmd::run(&mut cx, name, a)
                .await
                .unwrap_or_else(|| Ok(reply::unknown(name.as_bytes(), &a[1..])));
            let pushed = std::mem::take(&mut cx.pushed);
            match r {
                Ok(f) => {
                    if own && let Err(e) = cx.x("COMMIT").await {
                        let _ = cx.x("ROLLBACK").await;
                        if e.retryable() && attempt < cfg.write_retries {
                            attempt += 1;
                            continue;
                        }
                        return Ok(reply::backend(&e));
                    }
                    if !in_txn {
                        for (d, k) in pushed {
                            env.wake(&scope, d, &k);
                        }
                    }
                    return Ok(f);
                }
                Err(Fail::Reply(f)) => {
                    if own {
                        let _ = cx.x("ROLLBACK").await;
                    }
                    return Ok(f);
                }
                Err(Fail::Sql(e)) => {
                    if in_txn {
                        if e.retryable() {
                            return Err(e);
                        }
                        return Ok(reply::backend(&e));
                    }
                    if own {
                        let _ = cx.x("ROLLBACK").await;
                    }
                    // A lost race for a new key's row (1062) reruns too.
                    let again = e.retryable() || (own && e.errno == Some(1062));
                    if again && attempt < cfg.write_retries {
                        attempt += 1;
                        backoff(attempt).await;
                        continue;
                    }
                    if again {
                        return Ok(reply::tryagain());
                    }
                    if e.errno == Some(1146) && !recreated {
                        recreated = true;
                        let ns = cx.ns.clone();
                        let key = cx.ensure_key(&ns);
                        env.forget_ensured(&key);
                        if cx.ensure(&ns).await.is_ok() {
                            continue;
                        }
                    }
                    return Ok(reply::backend(&e));
                }
            }
        }
    }

    /// Runs `name` as one guarded batch ([`crate::batch`]) when the batch
    /// path takes it, with the statement path's retries; `None` leaves it
    /// to the statement path.
    async fn batched(&mut self, name: &str, a: &[Bytes]) -> Option<Frame> {
        let env = Arc::clone(&self.env);
        let cfg = env.config();
        let db = self.db;
        let ns = self
            .ns_override
            .clone()
            .unwrap_or_else(|| sprig_kv::Namespace::redis(&cfg.logical_database, db));
        let plan = crate::batch::plan(&ns, name, a)?;
        let login = self.login.as_mut()?;
        let scope = login.scope.clone();
        let mut attempt = 0;
        let mut recreated = false;
        loop {
            match crate::batch::run_once(login.sql.as_mut(), &ns, &plan).await {
                Ok((f, pushed)) => {
                    if pushed {
                        env.wake(&scope, db, plan.key());
                    }
                    return Some(f);
                }
                Err(e) => {
                    // A lost race for a new key's row (1062) reruns too.
                    let again = e.retryable() || e.errno == Some(1062);
                    if again && attempt < cfg.write_retries {
                        attempt += 1;
                        backoff(attempt).await;
                        continue;
                    }
                    if again {
                        return Some(reply::tryagain());
                    }
                    if e.errno == Some(1146) && !recreated {
                        recreated = true;
                        let mut cx = Ctx::new(login.sql.as_mut(), &env, &scope, db, false);
                        cx.ns = ns.clone();
                        let key = cx.ensure_key(&ns);
                        env.forget_ensured(&key);
                        if cx.ensure(&ns).await.is_ok() {
                            continue;
                        }
                    }
                    return Some(reply::backend(&e));
                }
            }
        }
    }

    async fn hello(&mut self, a: &[Bytes]) -> Frame {
        let mut proto = None;
        let mut i = 1;
        if let Some(v) = a.get(1) {
            match parse_i64(v) {
                Some(2) => proto = Some(Proto::Resp2),
                Some(3) => proto = Some(Proto::Resp3),
                Some(_) => return Frame::error("NOPROTO unsupported protocol version"),
                None => return Frame::err("Protocol version is not an integer or out of range"),
            }
            i = 2;
        }
        let mut setname = None;
        while i < a.len() {
            match upper(&a[i]).as_str() {
                "AUTH" if i + 2 < a.len() => {
                    match self.auth.login(Some(&a[i + 1]), &a[i + 2]).await {
                        Ok(l) => self.set_login(l).await,
                        Err(f) => return f,
                    }
                    i += 3;
                }
                "SETNAME" if i + 1 < a.len() => {
                    setname = Some(a[i + 1].clone());
                    i += 2;
                }
                _ => {
                    return Frame::err(&format!(
                        "Syntax error in HELLO option '{}'",
                        String::from_utf8_lossy(&a[i])
                    ));
                }
            }
        }
        if self.login.is_none() {
            return Frame::error(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time",
            );
        }
        if let Some(n) = setname
            && let Err(f) = self.set_name(n)
        {
            return f;
        }
        if let Some(p) = proto {
            self.proto = p;
        }
        let cfg = self.env.config();
        Frame::Map(vec![
            (Frame::bulk("server"), Frame::bulk("redis")),
            (Frame::bulk("version"), Frame::bulk(cfg.server_version)),
            (
                Frame::bulk("proto"),
                Frame::Integer(if self.resp3() { 3 } else { 2 }),
            ),
            (Frame::bulk("id"), Frame::Integer(self.id as i64)),
            (Frame::bulk("mode"), Frame::bulk("standalone")),
            (Frame::bulk("role"), Frame::bulk("master")),
            (Frame::bulk("modules"), Frame::Array(Vec::new())),
        ])
    }

    async fn set_login(&mut self, mut l: Login) {
        if let Some(mut old) = self.login.take() {
            self.unsubscribe_all();
            let _ = old.sql.exec("ROLLBACK").await;
        }
        // Single commands run READ COMMITTED: a locking read of an absent
        // key takes no gap lock, so writers of neighbouring keys don't
        // deadlock; two inserts of one key meet on its primary key (1062)
        // and the loser reruns. EXEC and scripts ask for REPEATABLE READ.
        if let Err(e) = l
            .sql
            .exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await
        {
            tracing::debug!(error = %e, "redis session isolation not set");
        }
        // A name set before AUTH names the DC too.
        l.sql.client_dc(self.dc_hint.as_deref());
        self.login = Some(l);
    }

    async fn auth_cmd(&mut self, a: &[Bytes]) -> Frame {
        let r = match a.len() {
            2 => self.auth.login(None, &a[1]).await,
            3 => self.auth.login(Some(&a[1]), &a[2]).await,
            _ => return reply::syntax(),
        };
        match r {
            Ok(l) => {
                self.set_login(l).await;
                Frame::ok()
            }
            Err(f) => f,
        }
    }

    fn set_name(&mut self, n: Bytes) -> Result<(), Frame> {
        if n.iter().any(|b| *b <= b' ' || *b > b'~') {
            return Err(Frame::err(
                "Client names cannot contain spaces, newlines or special characters.",
            ));
        }
        // Text after the last `~` is the DC hint.
        self.dc_hint = n
            .iter()
            .rposition(|b| *b == b'~')
            .map(|p| String::from_utf8_lossy(&n[p + 1..]).into_owned())
            .filter(|s| !s.is_empty());
        if let Some(l) = self.login.as_mut() {
            l.sql.client_dc(self.dc_hint.as_deref());
        }
        self.name = if n.is_empty() { None } else { Some(n) };
        Ok(())
    }

    fn client_info(&self) -> String {
        format!(
            "id={} addr={} laddr= fd=0 name={} age={} idle=0 flags=N db={} sub={} psub={} ssub={} multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client|info user={} redir=-1 resp={} lib-name={} lib-ver={}\n",
            self.id,
            self.peer,
            self.name
                .as_ref()
                .map_or(String::new(), |n| String::from_utf8_lossy(n).into_owned()),
            self.created.elapsed().as_secs(),
            self.db,
            self.subs.len(),
            self.psubs.len(),
            self.ssubs.len(),
            self.multi.as_ref().map_or(-1, |m| m.len() as i64),
            self.login.as_ref().map_or("default", |l| l.user.as_str()),
            if self.resp3() { 3 } else { 2 },
            self.lib
                .0
                .as_ref()
                .map_or(String::new(), |n| String::from_utf8_lossy(n).into_owned()),
            self.lib
                .1
                .as_ref()
                .map_or(String::new(), |n| String::from_utf8_lossy(n).into_owned()),
        )
    }

    fn client(&mut self, a: &[Bytes]) -> Frame {
        match upper(&a[1]).as_str() {
            "ID" => Frame::Integer(self.id as i64),
            "SETNAME" if a.len() == 3 => match self.set_name(a[2].clone()) {
                Ok(()) => Frame::ok(),
                Err(f) => f,
            },
            "GETNAME" => self.name.clone().map_or_else(Frame::null, Frame::Bulk),
            "INFO" | "LIST" => Frame::Verbatim(*b"txt", Bytes::from(self.client_info())),
            "SETINFO" if a.len() == 4 => {
                if a[3].iter().any(|b| *b <= b' ' || *b > b'~') {
                    return Frame::err(
                        "lib-name and lib-ver cannot contain spaces, newlines or special characters.",
                    );
                }
                match upper(&a[2]).as_str() {
                    "LIB-NAME" => self.lib.0 = Some(a[3].clone()),
                    "LIB-VER" => self.lib.1 = Some(a[3].clone()),
                    _ => {
                        return Frame::err(&format!(
                            "Unrecognized option '{}'",
                            String::from_utf8_lossy(&a[2])
                        ));
                    }
                }
                Frame::ok()
            }
            "NO-EVICT" | "NO-TOUCH" if a.len() == 3 => match upper(&a[2]).as_str() {
                "ON" | "OFF" => Frame::ok(),
                _ => reply::syntax(),
            },
            "REPLY" if a.len() == 3 => match upper(&a[2]).as_str() {
                "ON" => Frame::ok(),
                _ => reply::unsupported("client reply off", "not offered"),
            },
            "UNBLOCK" => Frame::Integer(0),
            "GETREDIR" => Frame::Integer(-1),
            "TRACKINGINFO" => Frame::Map(vec![
                (Frame::bulk("flags"), Frame::Array(vec![Frame::bulk("off")])),
                (Frame::bulk("redirect"), Frame::Integer(-1)),
                (Frame::bulk("prefixes"), Frame::Array(Vec::new())),
            ]),
            "KILL" | "PAUSE" | "UNPAUSE" | "TRACKING" | "CACHING" | "NO-EVICT" => {
                reply::restricted(&format!("client {}", String::from_utf8_lossy(&a[1])))
            }
            "HELP" => Frame::Array(vec![Frame::simple(
                "CLIENT <subcommand> [<arg> [value] [opt] ...]: ID, SETNAME, GETNAME, INFO, LIST, SETINFO, NO-EVICT, NO-TOUCH, REPLY, UNBLOCK.",
            )]),
            _ => reply::unknown_sub("client", &a[1]),
        }
    }

    async fn watch(&mut self, keys: &[Bytes]) -> Frame {
        let env = Arc::clone(&self.env);
        if let Err(e) = self.ensure_db(self.db).await {
            return reply::backend(&e);
        }
        let db = self.db;
        let Some(login) = self.login.as_mut() else {
            return Frame::error("NOAUTH Authentication required.");
        };
        let cx = Ctx::new(login.sql.as_mut(), &env, &login.scope, db, false);
        let ns = cx.ns.clone();
        let table = ns.keys();
        drop(cx);
        for k in keys {
            if self.watched.iter().any(|(d, wk, _)| *d == db && wk == k) {
                continue;
            }
            let rows = match login
                .sql
                .query(&format!(
                    "SELECT _v FROM {table} WHERE {} AND {}",
                    sprig_kv::api::key_pred(&ns, k, ""),
                    live("")
                ))
                .await
            {
                Ok(r) => r,
                Err(e) => return reply::backend(&e),
            };
            let v = rows.first().map(|r| cell_u64(r[0].as_ref()));
            self.watched.push((db, k.clone(), v));
        }
        Frame::ok()
    }

    /// `true` when no watched key changed (locks them for the `EXEC`).
    async fn watches_hold(&mut self) -> Result<bool, SqlError> {
        let env = Arc::clone(&self.env);
        let watched = self.watched.clone();
        let Some(login) = self.login.as_mut() else {
            return Ok(false);
        };
        for (db, k, ver) in watched {
            let cx = Ctx::new(login.sql.as_mut(), &env, &login.scope, db, false);
            let ns = cx.ns.clone();
            let table = ns.keys();
            drop(cx);
            let rows = login
                .sql
                .query(&format!(
                    "SELECT _v FROM {table} WHERE {} AND {} FOR UPDATE",
                    sprig_kv::api::key_pred(&ns, &k, ""),
                    live("")
                ))
                .await?;
            let now = rows.first().map(|r| cell_u64(r[0].as_ref()));
            if now != ver {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn sql_exec(&mut self, s: &str) -> Result<u64, SqlError> {
        match self.login.as_mut() {
            Some(l) => l.sql.exec(s).await,
            None => Ok(0),
        }
    }

    async fn exec(&mut self) -> Frame {
        let Some(queue) = self.multi.take() else {
            return Frame::err("EXEC without MULTI");
        };
        if std::mem::take(&mut self.multi_err) {
            self.watched.clear();
            return Frame::error("EXECABORT Transaction discarded because of previous errors.");
        }
        // DDL commits, so every database the queue reaches gets its tables
        // before the transaction starts.
        let cfg = self.env.config();
        let mut dbs = BTreeSet::from([self.db]);
        for c in &queue {
            let n = c.name_lower();
            let idx = match n.as_str() {
                "select" => c.args.get(1),
                "move" => c.args.get(2),
                _ => None,
            };
            if let Some(d) = idx.and_then(|x| parse_i64(x))
                && (0..i64::from(cfg.databases)).contains(&d)
            {
                dbs.insert(u32::try_from(d).unwrap_or(0));
            }
        }
        for d in dbs {
            if let Err(e) = self.ensure_db(d).await {
                self.watched.clear();
                return reply::backend(&e);
            }
        }
        let start_db = self.db;
        let mut attempt = 0;
        loop {
            self.db = start_db;
            let _ = self
                .sql_exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await;
            if let Err(e) = self.sql_exec("START TRANSACTION").await {
                self.watched.clear();
                return reply::backend(&e);
            }
            match self.watches_hold().await {
                Ok(true) => {}
                Ok(false) => {
                    let _ = self.sql_exec("ROLLBACK").await;
                    self.watched.clear();
                    return Frame::null_array();
                }
                Err(e) => {
                    let _ = self.sql_exec("ROLLBACK").await;
                    if e.retryable() && attempt < cfg.write_retries {
                        attempt += 1;
                        continue;
                    }
                    self.watched.clear();
                    return reply::backend(&e);
                }
            }
            let mut replies = Vec::with_capacity(queue.len());
            let mut failed = None;
            for c in &queue {
                let name = c.name_lower();
                let Some(spec) = spec::lookup(&name) else {
                    replies.push(reply::unknown(&c.args[0], &c.args[1..]));
                    continue;
                };
                match Box::pin(self.dispatch(&name, spec, &c.args, true)).await {
                    Ok(mut f) => {
                        if f.len() == 1 {
                            replies.push(f.remove(0));
                        } else {
                            replies.push(Frame::Array(f));
                        }
                    }
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = failed {
                let _ = self.sql_exec("ROLLBACK").await;
                if attempt < cfg.write_retries {
                    attempt += 1;
                    continue;
                }
                self.watched.clear();
                let _ = e;
                return reply::tryagain();
            }
            if let Err(e) = self.sql_exec("COMMIT").await {
                let _ = self.sql_exec("ROLLBACK").await;
                if e.retryable() && attempt < cfg.write_retries {
                    attempt += 1;
                    continue;
                }
                self.watched.clear();
                return reply::backend(&e);
            }
            self.watched.clear();
            // Wake anyone blocked on keys this transaction pushed to: the
            // cheap way is to wake every key the queue names.
            if let Some(scope) = self.login.as_ref().map(|l| l.scope.clone()) {
                let mut keys: Vec<&[u8]> = Vec::new();
                for c in &queue {
                    if let Some(s) = spec::lookup(&c.name_lower())
                        && s.has('w')
                    {
                        for i in s.key_indexes(c.args.len()) {
                            keys.push(&c.args[i]);
                        }
                    }
                }
                self.env.wake_many(&scope, self.db, keys);
            }
            return Frame::Array(replies);
        }
    }

    fn sub_reply(&self, kind: &str, name: Option<&Bytes>, count: usize) -> Frame {
        let items = vec![
            Frame::bulk(kind.to_owned()),
            name.map_or_else(Frame::null, |n| Frame::Bulk(n.clone())),
            Frame::Integer(count as i64),
        ];
        if self.resp3() {
            Frame::Push(items)
        } else {
            Frame::Array(items)
        }
    }

    async fn subscribe(&mut self, name: &str, chans: &[Bytes]) -> Vec<Frame> {
        let scope = self
            .login
            .as_ref()
            .map(|l| l.scope.clone())
            .unwrap_or_default();
        let kind = match name {
            "subscribe" => Kind::Channel,
            "psubscribe" => Kind::Pattern,
            _ => Kind::Shard,
        };
        let mut out = Vec::new();
        for c in chans {
            let set = match kind {
                Kind::Channel => &mut self.subs,
                Kind::Pattern => &mut self.psubs,
                Kind::Shard => &mut self.ssubs,
            };
            if set.insert(c.clone()) {
                self.env
                    .hub
                    .subscribe(&scope, kind, c, self.id, self.tx.clone());
                // Registered with the hub before the reply, so a later
                // PUBLISH on any gateway is delivered.
                self.env.hold(&scope, interest(kind, c)).await;
                // The notifications from the change feed.
                if (kind == Kind::Pattern || c.starts_with(b"__key"))
                    && let Some(f) = self.env.keyspace_feed()
                {
                    f.watch(&scope);
                }
            }
            let count = match kind {
                Kind::Shard => self.ssubs.len(),
                _ => self.subs.len() + self.psubs.len(),
            };
            out.push(self.sub_reply(name, Some(c), count));
        }
        out
    }

    fn unsubscribe(&mut self, name: &str, chans: &[Bytes]) -> Vec<Frame> {
        let scope = self
            .login
            .as_ref()
            .map(|l| l.scope.clone())
            .unwrap_or_default();
        let kind = match name {
            "unsubscribe" => Kind::Channel,
            "punsubscribe" => Kind::Pattern,
            _ => Kind::Shard,
        };
        let targets: Vec<Bytes> = if chans.is_empty() {
            match kind {
                Kind::Channel => self.subs.iter().cloned().collect(),
                Kind::Pattern => self.psubs.iter().cloned().collect(),
                Kind::Shard => self.ssubs.iter().cloned().collect(),
            }
        } else {
            chans.to_vec()
        };
        let mut out = Vec::new();
        if targets.is_empty() {
            let count = match kind {
                Kind::Shard => self.ssubs.len(),
                _ => self.subs.len() + self.psubs.len(),
            };
            out.push(self.sub_reply(name, None, count));
        }
        for c in targets {
            let removed = match kind {
                Kind::Channel => self.subs.remove(&c),
                Kind::Pattern => self.psubs.remove(&c),
                Kind::Shard => self.ssubs.remove(&c),
            };
            if removed {
                self.env.hub.unsubscribe(&scope, kind, &c, self.id);
                self.env.release(&scope, interest(kind, &c));
            }
            let count = match kind {
                Kind::Shard => self.ssubs.len(),
                _ => self.subs.len() + self.psubs.len(),
            };
            out.push(self.sub_reply(name, Some(&c), count));
        }
        out
    }

    async fn pubsub(&mut self, a: &[Bytes]) -> Frame {
        let scope = self
            .login
            .as_ref()
            .map(|l| l.scope.clone())
            .unwrap_or_default();
        if let Some(mesh) = self.env.mesh() {
            let sub = upper(&a[1]);
            if matches!(
                sub.as_str(),
                "CHANNELS" | "SHARDCHANNELS" | "NUMSUB" | "SHARDNUMSUB" | "NUMPAT"
            ) {
                let shard = sub.starts_with("SHARD");
                let all = mesh.introspect(&scope, shard).await;
                return match sub.as_str() {
                    "CHANNELS" | "SHARDCHANNELS" => Frame::Array(
                        all.channels
                            .keys()
                            .filter(|c| a.get(2).is_none_or(|p| crate::glob::matches(p, c)))
                            .cloned()
                            .map(Frame::Bulk)
                            .collect(),
                    ),
                    "NUMPAT" => Frame::Integer(all.patterns.len() as i64),
                    _ => Frame::Array(
                        a[2..]
                            .iter()
                            .flat_map(|c| {
                                [
                                    Frame::Bulk(c.clone()),
                                    Frame::Integer(all.channels.get(c).copied().unwrap_or(0)),
                                ]
                            })
                            .collect(),
                    ),
                };
            }
        }
        match upper(&a[1]).as_str() {
            "CHANNELS" | "SHARDCHANNELS" => {
                let shard = upper(&a[1]) == "SHARDCHANNELS";
                Frame::Array(
                    self.env
                        .hub
                        .channels(&scope, a.get(2).map(|p| &p[..]), shard)
                        .into_iter()
                        .map(Frame::Bulk)
                        .collect(),
                )
            }
            "NUMSUB" | "SHARDNUMSUB" => {
                let shard = upper(&a[1]) == "SHARDNUMSUB";
                Frame::Array(
                    a[2..]
                        .iter()
                        .flat_map(|c| {
                            [
                                Frame::Bulk(c.clone()),
                                Frame::Integer(self.env.hub.numsub(&scope, c, shard)),
                            ]
                        })
                        .collect(),
                )
            }
            "NUMPAT" => Frame::Integer(self.env.hub.numpat(&scope)),
            _ => reply::unknown_sub("pubsub", &a[1]),
        }
    }

    async fn info(&mut self, sections: &[Bytes]) -> Frame {
        let env = Arc::clone(&self.env);
        let cfg = env.config();
        let mut ks = Vec::new();
        if let Some(login) = self.login.as_mut() {
            for db in 0..cfg.databases {
                let cx = Ctx::new(login.sql.as_mut(), &env, &login.scope, db, false);
                let key = cx.ensure_key(&cx.ns);
                let table = cx.ns.keys();
                drop(cx);
                if !env.is_ensured(&key) {
                    continue;
                }
                if let Ok(rows) = login
                    .sql
                    .query(&format!(
                        "SELECT COUNT(*), COUNT(expires_at) FROM {table} WHERE {}",
                        live("")
                    ))
                    .await
                    && let Some(r) = rows.first()
                {
                    ks.push((
                        db,
                        sprig_kv::sql::cell_i64(r[0].as_ref()),
                        sprig_kv::sql::cell_i64(r[1].as_ref()),
                    ));
                }
            }
        }
        server::info(
            &server::InfoFacts {
                version: cfg.server_version,
                uptime: env.started.elapsed().as_secs(),
                clients: 1,
                blocked: env.waits.blocked(),
                gateway_id: env.gateway_id.clone(),
                keyspace: ks,
            },
            sections,
        )
    }

    /// Runs one non-blocking attempt of a blocking command.
    async fn block_try(
        &mut self,
        name: &str,
        a: &[Bytes],
        keys: &[Bytes],
        in_txn: bool,
    ) -> Result<Option<Frame>, SqlError> {
        let env = Arc::clone(&self.env);
        let resp3 = self.resp3();
        let db = self.db;
        let Some(login) = self.login.as_mut() else {
            return Ok(None);
        };
        let scope = login.scope.clone();
        let mut cx = Ctx::new(login.sql.as_mut(), &env, &scope, db, resp3);
        cx.in_txn = in_txn;
        if !in_txn {
            cx.x("START TRANSACTION").await?;
        }
        let r = match name {
            "blpop" | "brpop" => {
                let end = if name == "blpop" {
                    End::Left
                } else {
                    End::Right
                };
                cmd::lists::try_mpop(&mut cx, keys, end, 1, true).await
            }
            "blmove" => match (cmd::lists::end_of(&a[3]), cmd::lists::end_of(&a[4])) {
                (Some(f), Some(t)) => cmd::lists::try_blmove(&mut cx, a, f, t).await,
                _ => Err(Fail::Reply(reply::syntax())),
            },
            "brpoplpush" => cmd::lists::try_blmove(&mut cx, a, End::Right, End::Left).await,
            "blmpop" => match cmd::lists::mpop_args(a, 2) {
                Ok((ks, end, count)) => cmd::lists::try_mpop(&mut cx, &ks, end, count, false).await,
                Err(e) => Err(e),
            },
            "bzpopmin" | "bzpopmax" => {
                cmd::zsets::try_bzpop(&mut cx, keys, name == "bzpopmax").await
            }
            _ => match cmd::zsets::zmpop_args(a, 2) {
                Ok((ks, max, count)) => cmd::zsets::try_zmpop(&mut cx, &ks, max, count).await,
                Err(e) => Err(e),
            },
        };
        let pushed = std::mem::take(&mut cx.pushed);
        match r {
            Ok(v) => {
                if !in_txn {
                    cx.x("COMMIT").await?;
                    for (d, k) in pushed {
                        env.wake(&scope, d, &k);
                    }
                }
                Ok(v)
            }
            Err(Fail::Reply(f)) => {
                if !in_txn {
                    let _ = cx.x("ROLLBACK").await;
                }
                Ok(Some(f))
            }
            Err(Fail::Sql(e)) => {
                if !in_txn {
                    let _ = cx.x("ROLLBACK").await;
                }
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn blocking(&mut self, name: &str, a: &[Bytes], in_txn: bool) -> Step {
        let (keys, t_arg): (Vec<Bytes>, &Bytes) = match name {
            "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => {
                (a[1..a.len() - 1].to_vec(), &a[a.len() - 1])
            }
            "blmove" => (vec![a[1].clone()], &a[5]),
            "brpoplpush" => (vec![a[1].clone()], &a[3]),
            _ => {
                let parsed = if name == "blmpop" {
                    cmd::lists::mpop_args(a, 2).map(|x| x.0)
                } else {
                    cmd::zsets::zmpop_args(a, 2).map(|x| x.0)
                };
                match parsed {
                    Ok(k) => (k, &a[1]),
                    Err(Fail::Reply(f)) => return Ok(vec![f]),
                    Err(Fail::Sql(e)) => return Err(e),
                }
            }
        };
        let timeout = match parse_f64(t_arg) {
            Some(t) if t < 0.0 => return Ok(vec![Frame::err("timeout is negative")]),
            Some(t) if t.is_finite() && t < 1e15 => t,
            _ => return Ok(vec![Frame::err("timeout is not a float or out of range")]),
        };
        let null = if matches!(name, "blmove" | "brpoplpush") {
            Frame::null()
        } else {
            Frame::null_array()
        };
        if in_txn {
            return Ok(vec![
                self.block_try(name, a, &keys, true).await?.unwrap_or(null),
            ]);
        }
        if let Err(e) = self.ensure_db(self.db).await {
            return Ok(vec![reply::backend(&e)]);
        }
        let env = Arc::clone(&self.env);
        let cfg = env.config();
        let scope = self
            .login
            .as_ref()
            .map(|l| l.scope.clone())
            .unwrap_or_default();
        if !env.waits.enter(&scope, cfg.max_blocked_per_tenant) {
            return Ok(vec![reply::sprig(
                sprig_common::SprigCode::RedisTooManyBlocked,
                "too many blocked clients (redis.max_blocked_per_tenant)",
            )]);
        }
        let deadline = if timeout > 0.0 {
            Some(Instant::now() + Duration::from_secs_f64(timeout))
        } else {
            None
        };
        // Waiters register with the hub first, so a push on another
        // gateway after the first try wakes this one.
        let db = self.db;
        for k in &keys {
            env.hold(&scope, Interest::Wait(db, k.clone())).await;
        }
        let mut attempt = 0;
        let out = loop {
            let notes: Vec<_> = keys
                .iter()
                .map(|k| env.waits.notify(&scope, self.db, k))
                .collect();
            let mut waits: Vec<_> = notes.iter().map(|n| Box::pin(n.notified())).collect();
            for w in &mut waits {
                w.as_mut().enable();
            }
            match self.block_try(name, a, &keys, false).await {
                Ok(Some(f)) => break f,
                Ok(None) => {}
                Err(e) if e.retryable() && attempt < 100 => {
                    attempt += 1;
                    continue;
                }
                Err(e) => break reply::backend(&e),
            }
            let now = Instant::now();
            let wait = match deadline {
                Some(d) if d <= now => break null,
                Some(d) => (d - now).min(cfg.block_poll),
                None => cfg.block_poll,
            };
            tokio::select! {
                _ = futures::future::select_all(waits) => {}
                () = tokio::time::sleep(wait) => {}
            }
        };
        for k in keys {
            env.release(&scope, Interest::Wait(db, k));
        }
        env.waits.leave(&scope);
        Ok(vec![out])
    }

    // ---- scripting ----

    async fn ensure_scripts(&mut self) -> Result<String, SqlError> {
        let env = Arc::clone(&self.env);
        let database = env.config().logical_database;
        let Some(login) = self.login.as_mut() else {
            return Ok(String::new());
        };
        let table = scripts_table(&database);
        let key = format!("{}/{database}/__scripts", login.scope);
        if !env.is_ensured(&key) {
            login
                .sql
                .exec(&format!(
                    "CREATE DATABASE IF NOT EXISTS {}",
                    ident(&database)
                ))
                .await?;
            login
                .sql
                .exec(&format!(
                    "CREATE TABLE IF NOT EXISTS {table} (\
                     kind ENUM('script','library','function') NOT NULL, \
                     name VARBINARY(255) NOT NULL, \
                     body LONGBLOB NOT NULL, \
                     flags VARCHAR(255) NOT NULL DEFAULT '', \
                     PRIMARY KEY (kind, name)) ENGINE=InnoDB"
                ))
                .await?;
            env.set_ensured(&key);
        }
        Ok(table)
    }

    async fn script_get(
        &mut self,
        kind: &str,
        name: &[u8],
    ) -> Result<Option<(String, String)>, SqlError> {
        let table = self.ensure_scripts().await?;
        let Some(login) = self.login.as_mut() else {
            return Ok(None);
        };
        let rows = login
            .sql
            .query(&format!(
                "SELECT body, flags FROM {table} WHERE kind = '{kind}' AND name = {}",
                lit(name)
            ))
            .await?;
        Ok(rows.into_iter().next().map(|r| {
            let mut it = r.into_iter();
            (
                String::from_utf8_lossy(&it.next().flatten().unwrap_or_default()).into_owned(),
                String::from_utf8_lossy(&it.next().flatten().unwrap_or_default()).into_owned(),
            )
        }))
    }

    async fn script_put(
        &mut self,
        kind: &str,
        name: &[u8],
        body: &[u8],
        flags: &str,
        replace: bool,
    ) -> Result<(), SqlError> {
        let table = self.ensure_scripts().await?;
        let Some(login) = self.login.as_mut() else {
            return Ok(());
        };
        let verb = if replace { "REPLACE" } else { "INSERT IGNORE" };
        login
            .sql
            .exec(&format!(
                "{verb} INTO {table} (kind, name, body, flags) VALUES ('{kind}', {}, {}, {})",
                lit(name),
                lit(body),
                lit(flags.as_bytes())
            ))
            .await?;
        Ok(())
    }

    fn numkeys(a: &[Bytes], at: usize) -> Result<(Vec<Bytes>, Vec<Bytes>), Frame> {
        let Some(n) = parse_i64(&a[at]) else {
            return Err(reply::not_int());
        };
        if n < 0 {
            return Err(Frame::err("Number of keys can't be negative"));
        }
        let n = usize::try_from(n).unwrap_or(usize::MAX);
        if n > a.len() - at - 1 {
            return Err(Frame::err(
                "Number of keys can't be greater than number of args",
            ));
        }
        Ok((a[at + 1..at + 1 + n].to_vec(), a[at + 1 + n..].to_vec()))
    }

    async fn eval(&mut self, name: &str, a: &[Bytes], in_txn: bool) -> Step {
        let (keys, args) = match Self::numkeys(a, 2) {
            Ok(x) => x,
            Err(f) => return Ok(vec![f]),
        };
        let by_sha = name.starts_with("evalsha");
        let source = if by_sha {
            let sha = String::from_utf8_lossy(&a[1]).to_ascii_lowercase();
            match self.script_get("script", sha.as_bytes()).await {
                Ok(Some((b, _))) => b,
                Ok(None) => {
                    return Ok(vec![Frame::error(
                        "NOSCRIPT No matching script. Please use EVAL.",
                    )]);
                }
                Err(e) => return Ok(vec![reply::backend(&e)]),
            }
        } else {
            String::from_utf8_lossy(&a[1]).into_owned()
        };
        let sha = lua::sha1_hex(source.as_bytes());
        let (flags, body) = match lua::shebang(&source) {
            Ok(x) => x,
            Err(m) => return Ok(vec![Frame::error(m)]),
        };
        let read_only = name.ends_with("_ro") || flags.no_writes;
        let out = self
            .run_script(
                lua::Body::Script {
                    source: body,
                    name: sha.clone(),
                },
                keys,
                args,
                read_only,
                in_txn,
                false,
            )
            .await?;
        if !by_sha && !in_txn {
            // Register the digest, so EVALSHA finds it on every gateway.
            if let Err(e) = self
                .script_put("script", sha.as_bytes(), source.as_bytes(), "", false)
                .await
            {
                tracing::debug!(error = %e, "redis script not registered");
            }
        }
        Ok(vec![out])
    }

    /// Runs a script or function with its transaction, serving its
    /// `redis.call`s through [`Session::dispatch`].
    #[allow(clippy::too_many_lines)]
    async fn run_script(
        &mut self,
        body: lua::Body,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        read_only: bool,
        in_txn: bool,
        function: bool,
    ) -> Result<Frame, SqlError> {
        let env = Arc::clone(&self.env);
        let cfg = env.config();
        if !in_txn && let Err(e) = self.ensure_db(self.db).await {
            return Ok(reply::backend(&e));
        }
        let scope = self
            .login
            .as_ref()
            .map(|l| l.scope.clone())
            .unwrap_or_default();
        let start_db = self.db;
        let mut attempt = 0;
        loop {
            self.db = start_db;
            if !in_txn {
                let _ = self
                    .sql_exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    .await;
                if let Err(e) = self.sql_exec("START TRANSACTION").await {
                    return Ok(reply::backend(&e));
                }
                // Lock the declared keys first, in order.
                if !read_only && !keys.is_empty() {
                    let mut ks: Vec<&Bytes> = keys.iter().filter(|k| k.len() <= 1024).collect();
                    ks.sort();
                    ks.dedup();
                    let ns = sprig_kv::Namespace::redis(&cfg.logical_database, self.db);
                    let table = ns.keys();
                    let list: Vec<String> = ks.iter().map(|k| lit(k)).collect();
                    let mut dks: Vec<String> = ks.iter().map(|k| lit(ns.dist_key(k))).collect();
                    dks.sort();
                    dks.dedup();
                    if !list.is_empty()
                        && let Some(l) = self.login.as_mut()
                        && let Err(e) = l
                            .sql
                            .query(&format!(
                                "SELECT k FROM {table} WHERE k IN ({}) AND _sprig_dk IN ({}) ORDER BY k FOR UPDATE",
                                list.join(", "),
                                dks.join(", ")
                            ))
                            .await
                    {
                        let _ = self.sql_exec("ROLLBACK").await;
                        if e.retryable() && attempt < cfg.write_retries {
                            attempt += 1;
                            continue;
                        }
                        return Ok(reply::backend(&e));
                    }
                }
            }
            let mut started =
                lua::start(body.clone(), keys.clone(), args.clone(), self.resp3(), &cfg);
            let run_id = env.scripts.add(&scope, function, &started.state);
            let mut retry: Option<SqlError> = None;
            let outcome = loop {
                tokio::select! {
                    call = started.calls.recv() => {
                        let Some(call) = call else { continue };
                        let reply = if retry.is_some() {
                            Frame::err("the script's transaction failed and will rerun")
                        } else {
                            match self.script_call(&call, &keys, read_only).await {
                                Ok(f) => f,
                                Err(e) => {
                                    started.state.cancel();
                                    retry = Some(e);
                                    Frame::err("the script's transaction failed and will rerun")
                                }
                            }
                        };
                        let _ = call.reply.send(reply);
                    }
                    done = &mut started.done => {
                        break done.unwrap_or_else(|e| lua::Outcome::Abort(Frame::err(&format!("the script stopped: {e}"))));
                    }
                }
            };
            env.scripts.remove(run_id);
            if let Some(e) = retry {
                if !in_txn {
                    let _ = self.sql_exec("ROLLBACK").await;
                    if attempt < cfg.write_retries {
                        attempt += 1;
                        continue;
                    }
                    return Ok(reply::tryagain());
                }
                return Err(e);
            }
            match outcome {
                lua::Outcome::Reply(f) => {
                    if !in_txn && let Err(e) = self.sql_exec("COMMIT").await {
                        let _ = self.sql_exec("ROLLBACK").await;
                        if e.retryable() && attempt < cfg.write_retries {
                            attempt += 1;
                            continue;
                        }
                        return Ok(reply::backend(&e));
                    }
                    if !in_txn {
                        env.wake_many(&scope, self.db, keys.iter().map(|k| &k[..]));
                    }
                    return Ok(f);
                }
                lua::Outcome::Abort(f) => {
                    if !in_txn {
                        let _ = self.sql_exec("ROLLBACK").await;
                    }
                    return Ok(f);
                }
            }
        }
    }

    /// One `redis.call` of a script.
    async fn script_call(
        &mut self,
        call: &lua::Call,
        keys: &[Bytes],
        read_only: bool,
    ) -> Result<Frame, SqlError> {
        let a = &call.args;
        let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
        let Some(spec) = spec::lookup(&name).filter(|s| !s.has('x')) else {
            return Ok(Frame::err("Unknown Redis command called from script"));
        };
        if !spec.arity_ok(a.len()) {
            return Ok(Frame::err(
                "Wrong number of args calling Redis command from script",
            ));
        }
        if spec.has('s') || spec.has('p') && name != "publish" && name != "spublish" {
            return Ok(Frame::err("This Redis command is not allowed from script"));
        }
        if read_only && spec.has('w') {
            return Ok(Frame::err(
                "Write commands are not allowed from read-only scripts.",
            ));
        }
        // Every key must be declared.
        let cfg = self.env.config();
        for i in spec.key_indexes(a.len()) {
            let k = &a[i];
            if keys.contains(k) {
                continue;
            }
            let same_shard = cfg.lua_undeclared_same_shard && {
                let h = sprig_kv::layout::routing_hash_of(sprig_kv::layout::hash_tag(k));
                keys.iter()
                    .any(|d| sprig_kv::layout::routing_hash_of(sprig_kv::layout::hash_tag(d)) == h)
            };
            if !same_shard {
                return Ok(reply::sprig(
                    sprig_common::SprigCode::RedisLuaUndeclaredKey,
                    &format!(
                        "the script used key '{}', which is not in KEYS",
                        String::from_utf8_lossy(&k[..k.len().min(64)])
                    ),
                ));
            }
        }
        let saved = self.proto;
        self.proto = if call.resp3 {
            Proto::Resp3
        } else {
            Proto::Resp2
        };
        let r = Box::pin(self.dispatch(&name, spec, a, true)).await;
        self.proto = saved;
        let mut f = r?;
        Ok(if f.len() == 1 {
            f.remove(0)
        } else {
            Frame::Array(f)
        })
    }

    async fn script(&mut self, a: &[Bytes]) -> Frame {
        let scope = self
            .login
            .as_ref()
            .map(|l| l.scope.clone())
            .unwrap_or_default();
        match upper(&a[1]).as_str() {
            "LOAD" if a.len() == 3 => {
                let body = String::from_utf8_lossy(&a[2]).into_owned();
                let stripped = match lua::shebang(&body) {
                    Ok((_, b)) => b,
                    Err(m) => return Frame::error(m),
                };
                if let Err(f) = lua::check(&stripped) {
                    return f;
                }
                let sha = lua::sha1_hex(&a[2]);
                match self
                    .script_put("script", sha.as_bytes(), &a[2], "", false)
                    .await
                {
                    Ok(()) => Frame::bulk(sha),
                    Err(e) => reply::backend(&e),
                }
            }
            "EXISTS" if a.len() >= 3 => {
                let mut out = Vec::new();
                for s in &a[2..] {
                    let sha = String::from_utf8_lossy(s).to_ascii_lowercase();
                    match self.script_get("script", sha.as_bytes()).await {
                        Ok(r) => out.push(Frame::Integer(i64::from(r.is_some()))),
                        Err(e) => return reply::backend(&e),
                    }
                }
                Frame::Array(out)
            }
            "FLUSH" if a.len() <= 3 => {
                let table = match self.ensure_scripts().await {
                    Ok(t) => t,
                    Err(e) => return reply::backend(&e),
                };
                match self
                    .sql_exec(&format!("DELETE FROM {table} WHERE kind = 'script'"))
                    .await
                {
                    Ok(_) => Frame::ok(),
                    Err(e) => reply::backend(&e),
                }
            }
            "KILL" if a.len() == 2 => {
                let after = self.env.config().lua_kill_after;
                if self.env.scripts.kill(&scope, false, after) > 0
                    || self.env.scripts.busy(&scope, false)
                {
                    Frame::ok()
                } else {
                    Frame::error("NOTBUSY No scripts in execution right now.")
                }
            }
            "DEBUG" => reply::sprig(
                sprig_common::SprigCode::RedisLuaUnsupported,
                "SCRIPT DEBUG is not offered",
            ),
            "HELP" => Frame::Array(vec![Frame::simple(
                "SCRIPT <subcommand> [<arg> [value] [opt] ...]: LOAD, EXISTS, FLUSH, KILL.",
            )]),
            _ => reply::unknown_sub("script", &a[1]),
        }
    }

    async fn fcall(&mut self, name: &str, a: &[Bytes], in_txn: bool) -> Step {
        let (keys, args) = match Self::numkeys(a, 2) {
            Ok(x) => x,
            Err(f) => return Ok(vec![f]),
        };
        let fname = String::from_utf8_lossy(&a[1]).into_owned();
        let (lib, flags) = match self.script_get("function", fname.as_bytes()).await {
            Ok(Some(x)) => x,
            // Not a Lua library function: a logic-layer function exposed
            // to Redis, if there is one (Lua functions first).
            Ok(None) => return Ok(vec![self.logic_fcall(name, &a[1], &keys, &args).await]),
            Err(e) => return Ok(vec![reply::backend(&e)]),
        };
        let no_writes = flags.split(',').any(|f| f == "no-writes");
        if name == "fcall_ro" && !no_writes {
            return Ok(vec![Frame::err(
                "Can not execute a script with write flag using *_ro command.",
            )]);
        }
        let code = match self.script_get("library", lib.as_bytes()).await {
            Ok(Some((c, _))) => c,
            Ok(None) => return Ok(vec![Frame::err("Function not found")]),
            Err(e) => return Ok(vec![reply::backend(&e)]),
        };
        let body = format!("\n{}", code.split_once('\n').map_or("", |x| x.1));
        let out = self
            .run_script(
                lua::Body::Function {
                    source: body,
                    name: fname,
                },
                keys,
                args,
                no_writes || name == "fcall_ro",
                in_txn,
                true,
            )
            .await?;
        Ok(vec![out])
    }

    /// `FCALL` of a logic-layer function, through the gateway's hook.
    async fn logic_fcall(
        &mut self,
        cmd: &str,
        fname: &[u8],
        keys: &[Bytes],
        args: &[Bytes],
    ) -> Frame {
        let env = Arc::clone(&self.env);
        let (Some(hook), Some(login)) = (env.logic(), self.login.as_mut()) else {
            return Frame::err("Function not found");
        };
        let database = env.config().logical_database;
        hook.fcall(
            login.sql.as_mut(),
            crate::logic::Fcall {
                name: fname,
                keys,
                args,
                read_only: cmd == "fcall_ro",
                database: &database,
            },
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn function(&mut self, a: &[Bytes]) -> Frame {
        let scope = self
            .login
            .as_ref()
            .map(|l| l.scope.clone())
            .unwrap_or_default();
        let table = match self.ensure_scripts().await {
            Ok(t) => t,
            Err(e) => return reply::backend(&e),
        };
        match upper(&a[1]).as_str() {
            "LOAD" if a.len() == 3 || a.len() == 4 => {
                let (replace, code) = if a.len() == 4 {
                    if upper(&a[2]) != "REPLACE" {
                        return Frame::err(&format!(
                            "Unknown option given: {}",
                            String::from_utf8_lossy(&a[2])
                        ));
                    }
                    (true, &a[3])
                } else {
                    (false, &a[2])
                };
                let code = String::from_utf8_lossy(code).into_owned();
                let cfg = self.env.config();
                let info = match tokio::task::spawn_blocking(move || {
                    lua::load_library(&code, &cfg).map(|i| (i, code))
                })
                .await
                {
                    Ok(Ok(x)) => x,
                    Ok(Err(f)) => return f,
                    Err(e) => return Frame::err(&e.to_string()),
                };
                let (info, code) = info;
                let existing = match self.script_get("library", info.name.as_bytes()).await {
                    Ok(x) => x,
                    Err(e) => return reply::backend(&e),
                };
                if existing.is_some() && !replace {
                    return Frame::err(&format!("Library '{}' already exists", info.name));
                }
                for (f, _) in &info.functions {
                    match self.script_get("function", f.as_bytes()).await {
                        Ok(Some((lib, _))) if lib != info.name => {
                            return Frame::err(&format!("Function {f} already exists"));
                        }
                        Ok(_) => {}
                        Err(e) => return reply::backend(&e),
                    }
                }
                let r: Result<(), SqlError> = async {
                    self.sql_exec("START TRANSACTION").await?;
                    self.sql_exec(&format!(
                        "DELETE FROM {table} WHERE kind = 'function' AND body = {}",
                        lit(info.name.as_bytes())
                    ))
                    .await?;
                    self.script_put("library", info.name.as_bytes(), code.as_bytes(), "", true)
                        .await?;
                    for (f, flags) in &info.functions {
                        self.script_put(
                            "function",
                            f.as_bytes(),
                            info.name.as_bytes(),
                            &flags.join(","),
                            true,
                        )
                        .await?;
                    }
                    self.sql_exec("COMMIT").await?;
                    Ok(())
                }
                .await;
                match r {
                    Ok(()) => Frame::bulk(info.name),
                    Err(e) => {
                        let _ = self.sql_exec("ROLLBACK").await;
                        reply::backend(&e)
                    }
                }
            }
            "DELETE" if a.len() == 3 => {
                match self.script_get("library", &a[2]).await {
                    Ok(Some(_)) => {}
                    Ok(None) => return Frame::err("Library not found"),
                    Err(e) => return reply::backend(&e),
                }
                let r = async {
                    self.sql_exec(&format!(
                        "DELETE FROM {table} WHERE (kind = 'library' AND name = {0}) OR (kind = 'function' AND body = {0})",
                        lit(&a[2])
                    ))
                    .await
                }
                .await;
                match r {
                    Ok(_) => Frame::ok(),
                    Err(e) => reply::backend(&e),
                }
            }
            "FLUSH" if a.len() <= 3 => {
                match self
                    .sql_exec(&format!(
                        "DELETE FROM {table} WHERE kind IN ('library', 'function')"
                    ))
                    .await
                {
                    Ok(_) => Frame::ok(),
                    Err(e) => reply::backend(&e),
                }
            }
            "LIST" => {
                let mut pattern = None;
                let mut withcode = false;
                let mut i = 2;
                while i < a.len() {
                    match upper(&a[i]).as_str() {
                        "WITHCODE" => withcode = true,
                        "LIBRARYNAME" if i + 1 < a.len() => {
                            pattern = Some(a[i + 1].clone());
                            i += 1;
                        }
                        _ => {
                            return Frame::err(&format!(
                                "Unknown argument {}",
                                String::from_utf8_lossy(&a[i])
                            ));
                        }
                    }
                    i += 1;
                }
                let Some(login) = self.login.as_mut() else {
                    return Frame::error("NOAUTH Authentication required.");
                };
                let libs = match login
                    .sql
                    .query(&format!(
                        "SELECT name, body FROM {table} WHERE kind = 'library' ORDER BY name"
                    ))
                    .await
                {
                    Ok(r) => r,
                    Err(e) => return reply::backend(&e),
                };
                let fns = match login
                    .sql
                    .query(&format!("SELECT name, body, flags FROM {table} WHERE kind = 'function' ORDER BY name"))
                    .await
                {
                    Ok(r) => r,
                    Err(e) => return reply::backend(&e),
                };
                let mut out = Vec::new();
                for l in libs {
                    let lname = l[0].clone().unwrap_or_default();
                    if pattern
                        .as_ref()
                        .is_some_and(|p| !crate::glob::matches(p, &lname))
                    {
                        continue;
                    }
                    let funcs: Vec<Frame> = fns
                        .iter()
                        .filter(|f| f[1].as_deref() == Some(lname.as_slice()))
                        .map(|f| {
                            let flags =
                                String::from_utf8_lossy(f[2].as_deref().unwrap_or_default())
                                    .into_owned();
                            Frame::Map(vec![
                                (
                                    Frame::bulk("name"),
                                    Frame::Bulk(f[0].clone().unwrap_or_default().into()),
                                ),
                                (Frame::bulk("description"), Frame::null()),
                                (
                                    Frame::bulk("flags"),
                                    Frame::Set(
                                        flags
                                            .split(',')
                                            .filter(|s| !s.is_empty())
                                            .map(|s| Frame::bulk(s.to_owned()))
                                            .collect(),
                                    ),
                                ),
                            ])
                        })
                        .collect();
                    let mut m = vec![
                        (
                            Frame::bulk("library_name"),
                            Frame::Bulk(lname.clone().into()),
                        ),
                        (Frame::bulk("engine"), Frame::bulk("LUA")),
                        (Frame::bulk("functions"), Frame::Array(funcs)),
                    ];
                    if withcode {
                        m.push((
                            Frame::bulk("library_code"),
                            Frame::Bulk(l[1].clone().unwrap_or_default().into()),
                        ));
                    }
                    out.push(Frame::Map(m));
                }
                Frame::Array(out)
            }
            "STATS" => Frame::Map(vec![
                (Frame::bulk("running_script"), Frame::null()),
                (
                    Frame::bulk("engines"),
                    Frame::Map(vec![(Frame::bulk("LUA"), Frame::Map(Vec::new()))]),
                ),
            ]),
            "KILL" => {
                let after = self.env.config().lua_kill_after;
                if self.env.scripts.kill(&scope, true, after) > 0
                    || self.env.scripts.busy(&scope, true)
                {
                    Frame::ok()
                } else {
                    Frame::error("NOTBUSY No scripts in execution right now.")
                }
            }
            "DUMP" | "RESTORE" => reply::restricted("function dump"),
            "HELP" => Frame::Array(vec![Frame::simple(
                "FUNCTION <subcommand> [<arg> [value] [opt] ...]: LOAD, DELETE, FLUSH, LIST, STATS, KILL.",
            )]),
            _ => reply::unknown_sub("function", &a[1]),
        }
    }
}
