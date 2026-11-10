//! The command line (chapter 53, "Command line" and "Exit codes").

use std::io::{IsTerminal, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Arg, ArgAction, ArgMatches, Command};

use crate::api::{Caps, DataClient};
use crate::error::{ExitClass, Result, ShellCode, ShellError};
use crate::exec::Runner;
use crate::lang::{Lang, destructive, split};
use crate::model::Sink;
use crate::native::{Native, NativeTarget, Protocol};
use crate::output::{Format, Writer};
use crate::profile::{AuthKind, Profile, ProfileFile, config_dir};
use crate::secrets::{SecretStore, store_for};
use crate::session::{Signer, http_of};

/// The `sprig` command.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn command() -> Command {
    let global = |a: Arg| a.global(true);
    Command::new("sprig")
        .about("SprigDB's shell: a full-screen TUI and a scriptable CLI")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(global(
            Arg::new("profile")
                .short('p')
                .long("profile")
                .help("Connection profile"),
        ))
        .arg(global(
            Arg::new("url")
                .long("url")
                .help("Gateway URL (overrides the profile)"),
        ))
        .arg(global(
            Arg::new("user").short('u').long("user").help("Login name"),
        ))
        .arg(global(
            Arg::new("database")
                .short('d')
                .long("database")
                .help("Database (USE)"),
        ))
        .arg(global(
            Arg::new("tenant")
                .long("tenant")
                .help("Tenant to act in (USE TENANT)"),
        ))
        .arg(
            Arg::new("execute")
                .short('e')
                .long("execute")
                .action(ArgAction::Append)
                .help("Statements to run, then exit"),
        )
        .arg(
            Arg::new("file")
                .short('f')
                .long("file")
                .help("A script to run (- for stdin)"),
        )
        .arg(
            Arg::new("lang")
                .short('l')
                .long("lang")
                .value_parser(["sql", "mongo", "redis", "cypher"])
                .help("Language of -e, -f and stdin [default: sql, or the protocol's]"),
        )
        .arg(
            Arg::new("protocol")
                .long("protocol")
                .value_parser(["http", "mysql", "mongo", "redis", "bolt"])
                .default_value("http")
                .help("Transport: the gateway's REST API, or a native protocol"),
        )
        .arg(global(
            Arg::new("format")
                .short('o')
                .long("format")
                .value_parser(["table", "json", "ndjson", "csv", "tsv"])
                .help("Output [default: table on a terminal, else tsv]"),
        ))
        .arg(global(
            Arg::new("yes")
                .short('y')
                .long("yes")
                .action(ArgAction::SetTrue)
                .help("Run destructive statements without asking"),
        ))
        .arg(
            Arg::new("fail-fast")
                .long("fail-fast")
                .action(ArgAction::SetTrue)
                .help("Stop at the first error"),
        )
        .arg(
            Arg::new("max-rows")
                .long("max-rows")
                .value_parser(clap::value_parser!(u64))
                .help("Row cap per result (0: none)"),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .help("Statement timeout, such as 30s or 500ms"),
        )
        .arg(
            Arg::new("plain")
                .long("plain")
                .action(ArgAction::SetTrue)
                .help("A line prompt instead of the TUI"),
        )
        .arg(
            Arg::new("admin")
                .long("admin")
                .action(ArgAction::SetTrue)
                .help("Open the TUI on the Admin tab"),
        )
        .subcommand(
            Command::new("login")
                .about("Sign in and keep the session (or store an API key or token)")
                .arg(
                    Arg::new("api-key")
                        .long("api-key")
                        .action(ArgAction::SetTrue)
                        .help("Store an API key (read hidden)"),
                )
                .arg(
                    Arg::new("token")
                        .long("token")
                        .action(ArgAction::SetTrue)
                        .help("Store a session token (read hidden)"),
                )
                .arg(
                    Arg::new("save-password")
                        .long("save-password")
                        .action(ArgAction::SetTrue)
                        .help("Also keep the password in the keychain"),
                )
                .arg(
                    Arg::new("password-stdin")
                        .long("password-stdin")
                        .action(ArgAction::SetTrue)
                        .help("Read the secret from standard input"),
                ),
        )
        .subcommand(
            Command::new("logout").about("End the session and forget the profile's secrets"),
        )
        .subcommand(Command::new("whoami").about("Who the gateway thinks you are"))
        .subcommand(
            Command::new("profile")
                .about("Connection profiles")
                .subcommand_required(true)
                .subcommand(Command::new("list").about("List profiles"))
                .subcommand(
                    Command::new("show")
                        .about("Show a profile")
                        .arg(Arg::new("name")),
                )
                .subcommand(
                    Command::new("add")
                        .about("Add or replace a profile")
                        .arg(Arg::new("name").required(true))
                        .arg(
                            Arg::new("auth")
                                .long("auth")
                                .value_parser(["password", "token", "api_key"]),
                        )
                        .arg(Arg::new("admin-url").long("admin-url"))
                        .arg(Arg::new("admin-token-file").long("admin-token-file"))
                        .arg(Arg::new("mysql").long("mysql"))
                        .arg(Arg::new("mongo").long("mongo"))
                        .arg(Arg::new("redis").long("redis"))
                        .arg(Arg::new("bolt").long("bolt"))
                        .arg(Arg::new("ca-file").long("ca-file"))
                        .arg(
                            Arg::new("allow-insecure-http")
                                .long("allow-insecure-http")
                                .action(ArgAction::SetTrue),
                        ),
                )
                .subcommand(
                    Command::new("remove")
                        .about("Remove a profile and its secrets")
                        .arg(Arg::new("name").required(true)),
                )
                .subcommand(
                    Command::new("use")
                        .about("Make a profile the default")
                        .arg(Arg::new("name").required(true)),
                ),
        )
        .subcommand(
            Command::new("stream")
                .about("Event streams (chapter 45)")
                .subcommand_required(true)
                .subcommand(
                    Command::new("publish")
                        .about("Publish one record")
                        .arg(Arg::new("stream").required(true))
                        .arg(Arg::new("value").required(true))
                        .arg(Arg::new("key").long("key"))
                        .arg(Arg::new("headers").long("headers").help("JSON object")),
                )
                .subcommand(
                    Command::new("tail")
                        .about("Read the newest records, and follow")
                        .arg(Arg::new("stream").required(true))
                        .arg(
                            Arg::new("last")
                                .long("last")
                                .value_parser(clap::value_parser!(u64))
                                .default_value("20"),
                        )
                        .arg(
                            Arg::new("partition")
                                .long("partition")
                                .value_parser(clap::value_parser!(u32)),
                        )
                        .arg(
                            Arg::new("follow")
                                .short('F')
                                .long("follow")
                                .action(ArgAction::SetTrue),
                        ),
                ),
        )
        .subcommand(
            Command::new("queue")
                .about("Queues (chapter 50)")
                .subcommand_required(true)
                .subcommand(
                    Command::new("publish")
                        .about("Publish to an exchange")
                        .arg(Arg::new("exchange").required(true))
                        .arg(Arg::new("routing-key").required(true))
                        .arg(Arg::new("body").required(true))
                        .arg(
                            Arg::new("properties")
                                .long("properties")
                                .help("JSON object"),
                        ),
                )
                .subcommand(
                    Command::new("get")
                        .about("Take messages (returned at the lease's end unless --ack)")
                        .arg(Arg::new("queue").required(true))
                        .arg(
                            Arg::new("max")
                                .long("max")
                                .value_parser(clap::value_parser!(u32))
                                .default_value("1"),
                        )
                        .arg(
                            Arg::new("lease-ms")
                                .long("lease-ms")
                                .value_parser(clap::value_parser!(u64))
                                .default_value("30000"),
                        )
                        .arg(
                            Arg::new("ack")
                                .long("ack")
                                .action(ArgAction::SetTrue)
                                .help("Acknowledge them (removes them; destructive)"),
                        ),
                )
                .subcommand(
                    Command::new("browse")
                        .about("Look at messages without consuming them")
                        .arg(Arg::new("queue").required(true))
                        .arg(
                            Arg::new("limit")
                                .long("limit")
                                .value_parser(clap::value_parser!(u64))
                                .default_value("20"),
                        ),
                ),
        )
        .subcommand(
            Command::new("top")
                .about("Live dashboards; --once prints one poll")
                .arg(Arg::new("once").long("once").action(ArgAction::SetTrue))
                .arg(Arg::new("json").long("json").action(ArgAction::SetTrue))
                .arg(
                    Arg::new("view")
                        .long("view")
                        .value_parser(crate::dash::VIEW_NAMES.to_vec()),
                ),
        )
        .subcommand(crate::inbox::command())
        .subcommand(
            Command::new("admin")
                .about("Every sprigctl command: sprig admin <sprigctl arguments>")
                .disable_help_flag(true)
                .arg(
                    Arg::new("args")
                        .num_args(0..)
                        .trailing_var_arg(true)
                        .allow_hyphen_values(true),
                ),
        )
}

/// The resolved profile and where its secrets are.
pub struct Ctx {
    /// The config directory.
    pub dir: PathBuf,
    /// The profiles file.
    pub file: ProfileFile,
    /// The profile's name.
    pub name: String,
    /// The profile, with command-line overrides applied.
    pub profile: Profile,
    /// The secret store.
    pub store: Box<dyn SecretStore>,
}

impl Ctx {
    /// Loads the profiles and applies `m`'s overrides.
    ///
    /// # Errors
    /// 39980.
    pub fn load(m: &ArgMatches) -> Result<Self> {
        let dir = config_dir();
        let file = ProfileFile::load(&dir)?;
        let (name, mut profile) =
            file.resolve(m.get_one::<String>("profile").map(String::as_str))?;
        if let Some(u) = m.get_one::<String>("url") {
            profile.url = Some(u.clone());
        }
        if let Some(u) = m.get_one::<String>("user") {
            profile.user = Some(u.clone());
        }
        if let Some(d) = m.get_one::<String>("database") {
            profile.database = Some(d.clone());
        }
        if let Some(t) = m.get_one::<String>("tenant") {
            profile.tenant = Some(t.clone());
        }
        let store = store_for(file.secrets, &dir);
        Ok(Self {
            dir,
            file,
            name,
            profile,
            store,
        })
    }

    /// The signer of this profile.
    #[must_use]
    pub fn signer(&self) -> Signer<'_> {
        Signer {
            name: self.name.clone(),
            profile: self.profile.clone(),
            store: self.store.as_ref(),
            dir: self.dir.clone(),
        }
    }

    /// Signs in and opens a runner over REST (and the native protocol).
    ///
    /// # Errors
    /// Sign-in and connection errors.
    pub async fn connect(&self, protocol: Protocol, caps: Caps) -> Result<Runner> {
        let http = http_of(&self.profile)?;
        let signer = self.signer();
        let mut prompt = |p: &str| crate::term::prompt_hidden(p);
        let auth = if protocol == Protocol::Http {
            signer.auth(&http, &mut prompt).await?
        } else {
            crate::api::Auth::None
        };
        let mut client = DataClient::new(http, auth);
        client.tenant.clone_from(&self.profile.tenant);
        let mut runner = Runner::new(client, self.profile.database.clone(), caps);
        if protocol != Protocol::Http {
            runner.native = Some(Native::connect(protocol, self.native_target(protocol)?).await?);
        }
        Ok(runner)
    }

    /// Where and as whom to connect with a native protocol (asks for the
    /// password on a terminal when none is stored).
    ///
    /// # Errors
    /// 39981 for the secret store.
    pub fn native_target(&self, protocol: Protocol) -> Result<NativeTarget> {
        let p = &self.profile;
        let addr = match protocol {
            Protocol::Mysql => p.mysql.clone(),
            Protocol::Mongo => p.mongo.clone(),
            Protocol::Redis => p.redis.clone(),
            Protocol::Bolt => p.bolt.clone(),
            Protocol::Http => None,
        }
        .unwrap_or_else(|| {
            let host = crate::http::Endpoint::parse(&crate::session::url_of(p))
                .map_or_else(|_| "127.0.0.1".to_owned(), |e| e.host);
            format!("{host}:{}", protocol.default_port())
        });
        let user = std::env::var("SPRIG_USER")
            .ok()
            .or_else(|| p.user.clone())
            .unwrap_or_default();
        let password = match std::env::var("SPRIG_PASSWORD") {
            Ok(pw) => pw,
            Err(_) => match self.store.get(&self.name)?.password {
                Some(pw) => pw,
                None => crate::term::prompt_hidden(&format!("Password for {user}: "))
                    .ok_or_else(|| {
                        let mut e = ShellError::server(
                            1045,
                            "",
                            format!(
                                "--protocol {}: no password (set SPRIG_PASSWORD, or sprig login -p {} --save-password)",
                                protocol_name(protocol),
                                self.name
                            ),
                        );
                        e.http_status = 401;
                        e
                    })?,
            },
        };
        Ok(NativeTarget {
            addr,
            user,
            password,
            database: p.database.clone(),
        })
    }
}

fn protocol_name(p: Protocol) -> &'static str {
    match p {
        Protocol::Http => "http",
        Protocol::Mysql => "mysql",
        Protocol::Mongo => "mongo",
        Protocol::Redis => "redis",
        Protocol::Bolt => "bolt",
    }
}

/// Parses `30s`, `500ms`, `2m` or a number of milliseconds.
///
/// # Errors
/// A message for anything else.
pub fn parse_duration_ms(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim();
    let (num, mul) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60_000)
    } else {
        (s, 1)
    };
    num.trim()
        .parse::<u64>()
        .map(|n| n * mul)
        .map_err(|_| format!("duration {s:?}: use 30s, 500ms or 2m"))
}

/// What a script run does with each statement.
#[derive(Debug, Clone, Copy)]
pub struct ScriptOpts {
    /// The language.
    pub lang: Lang,
    /// `--yes`.
    pub yes: bool,
    /// `--fail-fast`.
    pub fail_fast: bool,
}

/// Runs a script; returns the class of the first error.
pub async fn run_script(
    runner: &mut Runner,
    text: &str,
    opts: ScriptOpts,
    sink: &mut Writer<impl Write + Send>,
    errors: &mut dyn Write,
) -> ExitClass {
    let stmts = match split(text, opts.lang) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(errors, "sprig: {e}");
            return e.class();
        }
    };
    let mut first = ExitClass::Ok;
    for st in stmts {
        let r = if let Some(why) = destructive(&st.text, opts.lang).filter(|_| !opts.yes) {
            Err(ShellError::shell(
                ShellCode::ConfirmationRequired,
                format!("{why} is destructive; run it with --yes"),
            ))
        } else {
            run_cancellable(runner, &st.text, opts.lang, sink).await
        };
        if let Err(e) = r {
            let _ = sink.finish();
            let _ = writeln!(errors, "sprig: line {}: {e}", st.line);
            if first == ExitClass::Ok {
                first = e.class();
            }
            if opts.fail_fast || e.is(ShellCode::Cancelled) {
                break;
            }
        }
    }
    let _ = sink.finish();
    first
}

/// Runs one statement; `Ctrl-C` cancels it (39986).
///
/// # Errors
/// The statement's error, or 39986.
pub async fn run_cancellable(
    runner: &mut Runner,
    stmt: &str,
    lang: Lang,
    sink: &mut dyn Sink,
) -> Result<()> {
    let canceller = match runner.native.as_ref() {
        Some(n) => n.canceller(),
        None if lang == Lang::Sql => Some(crate::native::Canceller::RestKill {
            client: runner.client.clone(),
            sql: stmt.to_owned(),
        }),
        None => None,
    };
    let outcome = tokio::select! {
        r = runner.run(stmt, lang, sink) => Some(r),
        _ = tokio::signal::ctrl_c() => None,
    };
    let Some(r) = outcome else {
        // The REST stream was dropped with the future: the gateway kills
        // the statement. A native connection is killed and reopened.
        if let Some(c) = canceller {
            let _ = c.cancel().await;
        }
        if let Some(n) = runner.native.as_mut() {
            let _ = n.reset().await;
        }
        return Err(ShellError::shell(ShellCode::Cancelled, "cancelled"));
    };
    r
}

fn format_of(m: &ArgMatches) -> Format {
    match m.get_one::<String>("format") {
        Some(f) => Format::parse(f).unwrap_or(Format::Table),
        None if std::io::stdout().is_terminal() => Format::Table,
        None => Format::Tsv,
    }
}

fn caps_of(m: &ArgMatches, p: &Profile, tui: bool) -> std::result::Result<Caps, String> {
    let max_rows = m
        .get_one::<u64>("max-rows")
        .copied()
        .or(p.max_rows.filter(|_| tui))
        .unwrap_or(if tui { 10_000 } else { 0 });
    let timeout_ms = match m.get_one::<String>("timeout") {
        Some(t) => parse_duration_ms(t)?,
        None => p.timeout_ms.unwrap_or(0),
    };
    Ok(Caps {
        max_rows,
        max_bytes: 0,
        timeout_ms,
    })
}

fn lang_of(m: &ArgMatches, protocol: Protocol) -> Lang {
    m.get_one::<String>("lang")
        .and_then(|l| Lang::parse(l).ok())
        .or_else(|| protocol.lang())
        .unwrap_or_default()
}

fn exit(class: ExitClass) -> ExitCode {
    ExitCode::from(class.code())
}

fn fail(e: &ShellError) -> ExitCode {
    eprintln!("sprig: {e}");
    exit(e.class())
}

/// The `sprig` binary's entry point.
#[must_use]
pub fn main() -> ExitCode {
    let m = match command().try_get_matches() {
        Ok(m) => m,
        Err(e) => {
            let _ = e.print();
            return match e.kind() {
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => {
                    ExitCode::SUCCESS
                }
                _ => exit(ExitClass::Usage),
            };
        }
    };
    if let Some(("admin", sub)) = m.subcommand() {
        return admin_cmd(&m, sub);
    }
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("sprig: can't start the runtime: {e}");
            return exit(ExitClass::Usage);
        }
    };
    rt.block_on(run(&m))
}

fn admin_cmd(m: &ArgMatches, sub: &ArgMatches) -> ExitCode {
    let ctx = match Ctx::load(m) {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };
    let args: Vec<String> = sub
        .get_many::<String>("args")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    let mut args = args;
    if m.get_flag("yes") && !args.iter().any(|a| a == "--yes") {
        args.push("--yes".to_owned());
    }
    if let Some(f) = m.get_one::<String>("format")
        && !args.iter().any(|a| a == "-o" || a.starts_with("--output"))
        && (f == "json" || f == "table")
    {
        args.push(format!("--output={f}"));
    }
    match crate::admin::find_sprigctl()
        .and_then(|exe| crate::admin::run_sprigctl(&exe, &args, &ctx.profile))
    {
        Ok(code) => ExitCode::from(code),
        Err(e) => fail(&e),
    }
}

#[allow(clippy::too_many_lines)]
async fn run(m: &ArgMatches) -> ExitCode {
    let ctx = match Ctx::load(m) {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };
    match m.subcommand() {
        Some(("profile", sub)) => return profile_cmd(&ctx, m, sub),
        Some(("login", sub)) => return login_cmd(&ctx, sub).await,
        Some(("logout", _)) => {
            let r = match http_of(&ctx.profile) {
                Ok(h) => ctx.signer().logout(&h).await,
                Err(e) => Err(e),
            };
            return match r {
                Ok(()) => {
                    eprintln!("Signed out of profile {}.", ctx.name);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(&e),
            };
        }
        Some(("top", sub)) => return crate::dash::cli(ctx, sub).await,
        Some(("approvals", sub)) => return crate::inbox::cli(&ctx, m, sub).await,
        _ => {}
    }
    let protocol = Protocol::parse(
        m.get_one::<String>("protocol")
            .map_or("http", String::as_str),
    )
    .unwrap_or(Protocol::Http);
    let script = script_of(m, protocol);
    let tui = script.is_none() && m.subcommand().is_none();
    let caps = match caps_of(m, &ctx.profile, tui) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("sprig: {e}");
            return exit(ExitClass::Usage);
        }
    };
    if tui && !m.get_flag("plain") && std::io::stdout().is_terminal() {
        return crate::tui::run(
            ctx,
            protocol,
            caps,
            lang_of(m, protocol),
            m.get_flag("admin"),
        )
        .await;
    }
    let mut runner = match ctx.connect(protocol, caps).await {
        Ok(r) => r,
        Err(e) => return fail(&e),
    };
    match m.subcommand() {
        Some(("whoami", _)) => {
            return match runner.client.whoami().await {
                Ok(v) => {
                    println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
                    ExitCode::SUCCESS
                }
                Err(e) => fail(&e),
            };
        }
        Some(("stream" | "queue", _)) => {
            return crate::msg::cli(&mut runner, m, format_of(m)).await;
        }
        _ => {}
    }
    let opts = ScriptOpts {
        lang: lang_of(m, protocol),
        yes: m.get_flag("yes"),
        fail_fast: m.get_flag("fail-fast"),
    };
    let mut w = Writer::new(std::io::BufWriter::new(std::io::stdout()), format_of(m));
    if let Some(text) = script {
        let text = match text {
            Ok(t) => t,
            Err(e) => return fail(&e),
        };
        let class = run_script(&mut runner, &text, opts, &mut w, &mut std::io::stderr()).await;
        return exit(class);
    }
    plain(&mut runner, opts, &mut w).await
}

fn script_of(m: &ArgMatches, _protocol: Protocol) -> Option<Result<String>> {
    if let Some(es) = m.get_many::<String>("execute") {
        let lang = m.get_one::<String>("lang").map_or("sql", String::as_str);
        let sep = if lang == "redis" || lang == "mongo" {
            "\n"
        } else {
            ";\n"
        };
        return Some(Ok(es.cloned().collect::<Vec<_>>().join(sep)));
    }
    if let Some(f) = m.get_one::<String>("file") {
        if f == "-" {
            return Some(read_stdin());
        }
        return Some(
            std::fs::read_to_string(f)
                .map_err(|e| ShellError::shell(ShellCode::Parse, format!("{f}: {e}"))),
        );
    }
    if m.subcommand().is_none() && !std::io::stdin().is_terminal() {
        return Some(read_stdin());
    }
    None
}

fn read_stdin() -> Result<String> {
    let mut s = String::new();
    std::io::stdin()
        .read_to_string(&mut s)
        .map_err(|e| ShellError::shell(ShellCode::Parse, format!("standard input: {e}")))?;
    Ok(s)
}

/// `--plain`: a line prompt. A statement runs when it is complete (SQL and
/// Cypher: a `;` outside quotes; Redis and MongoDB: each line).
async fn plain(
    runner: &mut Runner,
    opts: ScriptOpts,
    w: &mut Writer<impl Write + Send>,
) -> ExitCode {
    let stdin = std::io::stdin();
    let mut buf = String::new();
    let mut first = ExitClass::Ok;
    loop {
        eprint!("{}", if buf.is_empty() { "sprig> " } else { "    -> " });
        let _ = std::io::stderr().flush();
        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if buf.is_empty() && matches!(line.trim(), "quit" | "exit" | "\\q") {
            break;
        }
        buf.push_str(&line);
        let complete = match opts.lang {
            Lang::Sql | Lang::Cypher => {
                !crate::lang::open_at_end(&buf, opts.lang == Lang::Cypher)
                    && buf.trim_end().ends_with(';')
            }
            Lang::Redis | Lang::Mongo => true,
        };
        if !complete {
            continue;
        }
        let text = std::mem::take(&mut buf);
        let c = run_script(runner, &text, opts, w, &mut std::io::stderr()).await;
        if first == ExitClass::Ok {
            first = c;
        }
    }
    exit(first)
}

fn profile_cmd(ctx: &Ctx, m: &ArgMatches, sub: &ArgMatches) -> ExitCode {
    let mut file = ctx.file.clone();
    let r: Result<()> = match sub.subcommand() {
        Some(("list", _)) => {
            for (n, p) in &file.profiles {
                let mark = if file.default.as_deref() == Some(n) {
                    "*"
                } else {
                    " "
                };
                println!(
                    "{mark} {n:<16} {:<32} {}",
                    p.url.as_deref().unwrap_or("-"),
                    p.user.as_deref().unwrap_or("-")
                );
            }
            Ok(())
        }
        Some(("show", s)) => file
            .resolve(s.get_one::<String>("name").map(String::as_str))
            .map(|(n, p)| {
                println!("# profile {n}");
                print!("{}", toml::to_string_pretty(&p).unwrap_or_default());
            }),
        Some(("add", s)) => {
            let name = s.get_one::<String>("name").cloned().unwrap_or_default();
            crate::profile::check_name(&name).and_then(|()| {
                let mut p = file.profiles.get(&name).cloned().unwrap_or_default();
                let get = |k: &str| s.get_one::<String>(k).cloned();
                p.url = m.get_one::<String>("url").cloned().or(p.url);
                p.user = m.get_one::<String>("user").cloned().or(p.user);
                p.database = m.get_one::<String>("database").cloned().or(p.database);
                p.tenant = m.get_one::<String>("tenant").cloned().or(p.tenant);
                if let Some(a) = get("auth") {
                    p.auth = match a.as_str() {
                        "token" => AuthKind::Token,
                        "api_key" => AuthKind::ApiKey,
                        _ => AuthKind::Password,
                    };
                }
                p.admin_url = get("admin-url").or(p.admin_url);
                p.admin_token_file = get("admin-token-file").or(p.admin_token_file);
                p.mysql = get("mysql").or(p.mysql);
                p.mongo = get("mongo").or(p.mongo);
                p.redis = get("redis").or(p.redis);
                p.bolt = get("bolt").or(p.bolt);
                p.ca_file = get("ca-file").or(p.ca_file);
                p.allow_insecure_http |= s.get_flag("allow-insecure-http");
                file.profiles.insert(name.clone(), p);
                if file.default.is_none() {
                    file.default = Some(name.clone());
                }
                file.save(&ctx.dir)
                    .map(|()| eprintln!("Profile {name} saved in {}.", ctx.dir.display()))
            })
        }
        Some(("remove", s)) => {
            let name = s.get_one::<String>("name").cloned().unwrap_or_default();
            if file.profiles.remove(&name).is_none() {
                Err(ShellError::shell(
                    ShellCode::ProfileInvalid,
                    format!("no profile {name:?}"),
                ))
            } else {
                if file.default.as_deref() == Some(&name) {
                    file.default = None;
                }
                ctx.store
                    .set(&name, &crate::secrets::Secret::default())
                    .and_then(|()| file.save(&ctx.dir))
            }
        }
        Some(("use", s)) => {
            let name = s.get_one::<String>("name").cloned().unwrap_or_default();
            if file.profiles.contains_key(&name) {
                file.default = Some(name);
                file.save(&ctx.dir)
            } else {
                Err(ShellError::shell(
                    ShellCode::ProfileInvalid,
                    format!("no profile {name:?}"),
                ))
            }
        }
        _ => Ok(()),
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(&e),
    }
}

async fn login_cmd(ctx: &Ctx, sub: &ArgMatches) -> ExitCode {
    let from_stdin = sub.get_flag("password-stdin");
    let read = |what: &str| {
        if from_stdin {
            crate::term::read_stdin_line()
        } else {
            crate::term::prompt_hidden(&format!("{what}: "))
        }
    };
    let signer = ctx.signer();
    let r = if sub.get_flag("api-key") {
        match read("API key") {
            Some(k) => signer.store_bearer(Some(k), None),
            None => Err(ShellError::shell(ShellCode::Parse, "no API key given")),
        }
    } else if sub.get_flag("token") {
        match read("Session token") {
            Some(t) => signer.store_bearer(None, Some(t)),
            None => Err(ShellError::shell(ShellCode::Parse, "no token given")),
        }
    } else {
        let user = ctx.profile.user.clone().unwrap_or_default();
        match read(&format!("Password for {user}")) {
            Some(pw) => match http_of(&ctx.profile) {
                Ok(h) => {
                    signer
                        .login_password(&h, &pw, sub.get_flag("save-password"))
                        .await
                }
                Err(e) => Err(e),
            },
            None => Err(ShellError::shell(ShellCode::Parse, "no password given")),
        }
    };
    match r {
        Ok(()) => {
            eprintln!(
                "Signed in: profile {}, secrets in {}.",
                ctx.name,
                ctx.store.describe()
            );
            ExitCode::SUCCESS
        }
        Err(e) => fail(&e),
    }
}

#[cfg(test)]
mod tests;
