//! Named operators:
//!
//! - `sprigctl login [--name N] [--code C | --recovery-code R]`: password
//!   (`--password-file`, `SPRIGCTL_PASSWORD` or a prompt), then a TOTP code;
//!   the first login enrols the factor (prints the `otpauth://` URI, asks
//!   for the first code, prints the recovery codes once). The session is
//!   kept per endpoint in `$SPRIGCTL_SESSIONS` (default
//!   `~/.config/sprigctl/sessions.json`, mode 0600) and used by every
//!   later command that has no `--token-file`.
//! - `sprigctl logout`, `sprigctl whoami`.
//! - `sprigctl operator create <name> --role platform|dc
//!   [--password-file F]` (`--dcs a,b` for a DC operator), `list`, `show`, `disable`, `enable`,
//!   `reset-factor`, `set-password`.
//! - `sprigctl token create --scope S... [--days N] [--dcs D,...] [--label L]
//!   [--service NAME [--kind gateway|ci] [--never-expires]]`, `list
//!   [--owner O|--all]`, `revoke <key_id>`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{BufRead, IsTerminal, Write};
use std::path::PathBuf;

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sprig_controller::operators::{
    CreateTokenResponse, LoginResponse, OperatorView, TokenView, WhoAmI,
};

use crate::client::{AdminClient, CallError, DEFAULT_ENDPOINT};
use crate::render::table;
use crate::{Failed, Output, print};

/// One stored login.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Session {
    /// The session token.
    pub token: String,
    /// The operator.
    pub operator: String,
    /// Its end.
    pub expires_ms: u64,
}

/// Where sessions are kept.
pub(crate) fn sessions_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("SPRIGCTL_SESSIONS") {
        return Some(PathBuf::from(p));
    }
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config/sprigctl/sessions.json"))
}

fn load_sessions() -> BTreeMap<String, Session> {
    sessions_path()
        .and_then(|p| std::fs::read(p).ok())
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

fn save_sessions(all: &BTreeMap<String, Session>) -> Result<(), Failed> {
    let p = sessions_path().ok_or_else(|| Failed("no $HOME; set SPRIGCTL_SESSIONS".into()))?;
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let tmp = p.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(all).unwrap_or_default();
    {
        let mut o = std::fs::OpenOptions::new();
        o.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            o.mode(0o600);
        }
        let mut f = o
            .open(&tmp)
            .map_err(|e| format!("{}: {e}", tmp.display()))?;
        f.write_all(&body)
            .map_err(|e| format!("{}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, &p).map_err(|e| format!("{}: {e}", p.display()))?;
    Ok(())
}

fn key(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_owned()
}

/// The stored, unexpired session token for `endpoint`.
pub(crate) fn session_token(endpoint: &str) -> Option<String> {
    let now = now_ms();
    load_sessions()
        .remove(&key(endpoint))
        .filter(|s| s.expires_ms > now)
        .map(|s| s.token)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn fmt_ms(ms: u64) -> String {
    if ms == 0 {
        return String::new();
    }
    sprig_approvals::events::rfc3339(
        i64::try_from(ms)
            .unwrap_or(i64::MAX / 1000)
            .saturating_mul(1000),
    )
}

fn name_arg() -> Arg {
    Arg::new("name")
        .required(true)
        .help("The operator's login name")
}

fn reason() -> Arg {
    Arg::new("reason").long("reason").help("Why (audited)")
}

fn password_file() -> Arg {
    Arg::new("password-file")
        .long("password-file")
        .help("A file holding the password (else SPRIGCTL_PASSWORD, else a prompt)")
}

/// `sprigctl login`.
pub(crate) fn login_cmd() -> Command {
    Command::new("login")
        .about("Log in as a named operator; the session is kept per endpoint")
        .arg(
            Arg::new("name")
                .long("name")
                .env("SPRIGCTL_OPERATOR")
                .help("Your operator name [default: $USER]"),
        )
        .arg(password_file())
        .arg(
            Arg::new("code")
                .long("code")
                .help("A TOTP code (else a prompt)"),
        )
        .arg(
            Arg::new("recovery-code")
                .long("recovery-code")
                .help("A recovery code instead of a TOTP code"),
        )
}

/// `sprigctl logout`.
pub(crate) fn logout_cmd() -> Command {
    Command::new("logout").about("End this endpoint's login session")
}

/// `sprigctl whoami`.
pub(crate) fn whoami_cmd() -> Command {
    Command::new("whoami").about("Who the Admin API takes you for")
}

/// `sprigctl operator`.
pub(crate) fn operator_group() -> Command {
    Command::new("operator")
        .about("Named platform and DC operators")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Create an operator (a platform operator's login, or the bootstrap token before the first login)")
                .arg(name_arg())
                .arg(
                    Arg::new("role")
                        .long("role")
                        .required(true)
                        .value_parser(["platform", "dc"])
                        .help("platform (everything) or dc (the --dc DCs only)"),
                )
                .arg(Arg::new("dcs").long("dcs").value_delimiter(',').action(ArgAction::Append).help("The DCs a DC operator operates (comma list)"))
                .arg(Arg::new("display-name").long("display-name"))
                .arg(Arg::new("email").long("email"))
                .arg(password_file())
                .arg(reason()),
        )
        .subcommand(Command::new("list").about("Every operator"))
        .subcommand(Command::new("show").about("One operator").arg(name_arg()))
        .subcommand(Command::new("disable").about("Disable an operator; their sessions and tokens end").arg(name_arg()).arg(reason()))
        .subcommand(Command::new("enable").about("Enable a disabled operator").arg(name_arg()).arg(reason()))
        .subcommand(Command::new("reset-factor").about("Forget an operator's second factor; the next login enrols one").arg(name_arg()).arg(reason()))
        .subcommand(
            Command::new("set-password")
                .about("Change a password (yours asks for the current one)")
                .arg(name_arg())
                .arg(password_file())
                .arg(Arg::new("current-file").long("current-file").help("A file holding your current password"))
                .arg(reason()),
        )
}

/// `sprigctl token`.
pub(crate) fn token_group() -> Command {
    Command::new("token")
        .about("Tokens you issue for scripts and services")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("Issue a token in your name (from a login session)")
                .arg(
                    Arg::new("scope")
                        .long("scope")
                        .action(ArgAction::Append)
                        .help(
                            "<area>:read or <area>:write, such as clusters:read or *:read (repeat)",
                        ),
                )
                .arg(
                    Arg::new("days")
                        .long("days")
                        .value_parser(clap::value_parser!(u32))
                        .help("Lifetime in days [default: 30]"),
                )
                .arg(
                    Arg::new("dcs")
                        .long("dcs")
                        .value_delimiter(',')
                        .action(ArgAction::Append)
                        .help("Only these DCs (comma list)"),
                )
                .arg(Arg::new("label").long("label").help("A note"))
                .arg(
                    Arg::new("service")
                        .long("service")
                        .help("Issue a service identity (sprigsvc_) of this name"),
                )
                .arg(
                    Arg::new("kind")
                        .long("kind")
                        .value_parser(["gateway", "ci"])
                        .help("The service identity's kind [default: ci]"),
                )
                .arg(
                    Arg::new("never-expires")
                        .long("never-expires")
                        .action(ArgAction::SetTrue)
                        .help("A service identity that never expires"),
                )
                .arg(reason()),
        )
        .subcommand(
            Command::new("list")
                .about("Your tokens and sessions")
                .arg(
                    Arg::new("owner")
                        .long("owner")
                        .help("Another operator's (platform operators)"),
                )
                .arg(
                    Arg::new("all")
                        .long("all")
                        .action(ArgAction::SetTrue)
                        .help("Everyone's (platform operators)"),
                ),
        )
        .subcommand(
            Command::new("revoke")
                .about("Revoke a token")
                .arg(Arg::new("key_id").required(true))
                .arg(reason()),
        )
}

fn read_secret(file: Option<&String>, env: &str, prompt: &str) -> Result<String, Failed> {
    if let Some(f) = file {
        return Ok(std::fs::read_to_string(f)
            .map_err(|e| format!("{f}: {e}"))?
            .trim_end_matches(['\n', '\r'])
            .to_owned());
    }
    if let Ok(v) = std::env::var(env) {
        return Ok(v);
    }
    prompt_line(prompt, true)
}

fn prompt_line(prompt: &str, hidden: bool) -> Result<String, Failed> {
    if !std::io::stdin().is_terminal() {
        return Err(Failed(format!(
            "{} needed and no terminal to ask on",
            prompt.trim_end_matches(": ")
        )));
    }
    eprint!("{prompt}");
    let _ = std::io::stderr().flush();
    let echo_off = hidden
        && std::process::Command::new("stty")
            .arg("-echo")
            .stdin(std::process::Stdio::inherit())
            .status()
            .is_ok_and(|s| s.success());
    let mut line = String::new();
    let r = std::io::stdin().lock().read_line(&mut line);
    if echo_off {
        let _ = std::process::Command::new("stty")
            .arg("echo")
            .stdin(std::process::Stdio::inherit())
            .status();
        eprintln!();
    }
    r.map_err(|e| format!("reading the terminal: {e}"))?;
    Ok(line.trim_end_matches(['\n', '\r']).to_owned())
}

fn s(m: &ArgMatches, k: &str) -> Option<String> {
    m.get_one::<String>(k).cloned()
}

fn many(m: &ArgMatches, k: &str) -> Vec<String> {
    m.get_many::<String>(k)
        .map(|v| v.cloned().collect())
        .unwrap_or_default()
}

fn operators_table(v: &[OperatorView]) -> String {
    let rows: Vec<[String; 6]> = v
        .iter()
        .map(|o| {
            [
                o.name.clone(),
                match o.role {
                    sprig_controller::operators::Role::PlatformOperator => "platform".into(),
                    sprig_controller::operators::Role::DcOperator => format!(
                        "dc {}",
                        o.dcs
                            .iter()
                            .map(sprig_common::ids::DcId::as_str)
                            .collect::<Vec<_>>()
                            .join(",")
                    ),
                },
                format!("{:?}", o.state).to_lowercase(),
                if o.totp { "totp" } else { "none" }.into(),
                fmt_ms(o.last_login_ms),
                o.created_by.clone(),
            ]
        })
        .collect();
    table(
        [
            "NAME",
            "ROLE",
            "STATE",
            "FACTOR",
            "LAST LOGIN",
            "CREATED BY",
        ],
        &rows,
    )
}

fn tokens_table(v: &[TokenView]) -> String {
    let now = now_ms();
    let rows: Vec<[String; 7]> = v
        .iter()
        .map(|t| {
            let state = if t.revoked_ms.is_some() {
                "revoked"
            } else if t.expires_ms.is_some_and(|e| e <= now) {
                "expired"
            } else {
                "live"
            };
            [
                t.key_id.clone(),
                match (&t.service, t.kind) {
                    (Some(s), _) => format!("service {s}"),
                    (None, k) => format!("{k:?}").to_lowercase(),
                },
                t.owner.clone(),
                if t.scopes.is_empty() {
                    "-".into()
                } else {
                    t.scopes.join(",")
                },
                t.expires_ms.map_or_else(|| "never".into(), fmt_ms),
                state.into(),
                t.label.clone(),
            ]
        })
        .collect();
    table(
        [
            "KEY ID", "KIND", "OWNER", "SCOPES", "EXPIRES", "STATE", "LABEL",
        ],
        &rows,
    )
}

/// `login`, `logout`, `whoami`, `operator ...`, `token ...`.
pub(crate) async fn run(
    c: &AdminClient,
    global: &ArgMatches,
    group: &str,
    sub: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let endpoint = key(global
        .get_one::<String>("endpoint")
        .map_or(DEFAULT_ENDPOINT, String::as_str));
    match group {
        "login" => login(c, &endpoint, sub, out).await,
        "logout" => {
            let r: Result<TokenView, CallError> =
                c.post("/admin/v1/operators:logout", &json!({})).await;
            let mut all = load_sessions();
            let had = all.remove(&endpoint).is_some();
            save_sessions(&all)?;
            match r {
                Ok(t) => {
                    eprintln!("logged out (session {} revoked)", t.key_id);
                    Ok(())
                }
                Err(e) if had => {
                    eprintln!("the local session is forgotten; the server said: {e}");
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        }
        "whoami" => {
            let w: WhoAmI = c.get("/admin/v1/operators:whoami").await?;
            print(out, &w, |w| {
                let mut t = String::new();
                let p = &w.principal;
                let _ = writeln!(t, "{} ({:?})", p.audit_name(), p.kind);
                let _ = writeln!(
                    t,
                    "rights: {}",
                    if p.platform {
                        "platform operator".to_owned()
                    } else if p.is_gateway() {
                        "gateway routes".to_owned()
                    } else {
                        format!(
                            "DC operator of {}",
                            p.dcs
                                .iter()
                                .map(sprig_common::ids::DcId::as_str)
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    }
                );
                if !p.scopes.is_empty() {
                    let _ = writeln!(t, "scopes: {}", p.scopes.join(", "));
                }
                if let Some(e) = w.expires_ms {
                    let _ = writeln!(t, "expires: {}", fmt_ms(e));
                }
                let _ = writeln!(
                    t,
                    "dc: {}; named operators in use: {}",
                    w.dc,
                    if w.switched {
                        "yes"
                    } else {
                        "no (bootstrap token works)"
                    }
                );
                t
            });
            Ok(())
        }
        "operator" => operator(c, sub, out).await,
        "token" => token(c, sub, out).await,
        _ => Err(Failed(format!("unknown command {group}"))),
    }
}

async fn login(c: &AdminClient, endpoint: &str, m: &ArgMatches, out: Output) -> Result<(), Failed> {
    let name = s(m, "name")
        .or_else(|| std::env::var("USER").ok())
        .ok_or_else(|| Failed("name yourself: --name".into()))?;
    let password = read_secret(
        m.get_one::<String>("password-file"),
        "SPRIGCTL_PASSWORD",
        &format!("password for {name}: "),
    )?;
    let mut body = json!({"name": name, "password": password, "client": "sprigctl"});
    if let Some(code) = s(m, "code") {
        body["code"] = code.into();
    }
    if let Some(rc) = s(m, "recovery-code") {
        body["recovery_code"] = rc.into();
    }
    let mut r: Result<LoginResponse, CallError> = c.post("/admin/v1/operators:login", &body).await;
    if let Err(CallError::Api(e)) = &r
        && e.code == 39311
    {
        let code = prompt_line("code from your authenticator: ", false)?;
        body["code"] = code.into();
        r = c.post("/admin/v1/operators:login", &body).await;
    }
    let mut r = r?;
    if let Some(ticket) = r.enrol_ticket.clone() {
        eprintln!(
            "No second factor yet. Add this to an authenticator app (it is shown once):\n\n  {}\n",
            r.otpauth_uri.clone().unwrap_or_default()
        );
        let code = match std::env::var("SPRIGCTL_ENROL_CODE") {
            Ok(c) => c,
            Err(_) => prompt_line("first code from the authenticator: ", false)?,
        };
        r = c
            .post(
                "/admin/v1/operators:enrol",
                &json!({"name": name, "ticket": ticket, "code": code, "client": "sprigctl"}),
            )
            .await?;
    }
    let token = r
        .token
        .clone()
        .ok_or_else(|| Failed("the login returned no session".into()))?;
    let mut all = load_sessions();
    all.insert(
        endpoint.to_owned(),
        Session {
            token,
            operator: r.operator.name.clone(),
            expires_ms: r.expires_ms.unwrap_or(0),
        },
    );
    save_sessions(&all)?;
    if !r.recovery_codes.is_empty() {
        eprintln!(
            "Recovery codes (each works once; keep them safe, they are shown only now):\n  {}\n",
            r.recovery_codes.join("\n  ")
        );
    }
    let shown = json!({
        "operator": r.operator.name,
        "expires_ms": r.expires_ms,
        "endpoint": endpoint,
        "recovery_codes": r.recovery_codes,
    });
    print(out, &shown, |_| {
        format!(
            "logged in as {} at {endpoint} until {}\n",
            r.operator.name,
            fmt_ms(r.expires_ms.unwrap_or(0))
        )
    });
    Ok(())
}

async fn operator(c: &AdminClient, m: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, sub)) = m.subcommand() else {
        return Err(Failed("operator: a subcommand".into()));
    };
    let name = || s(sub, "name").unwrap_or_default();
    let reason = || s(sub, "reason").unwrap_or_default();
    match cmd {
        "create" => {
            let password = read_secret(
                sub.get_one::<String>("password-file"),
                "SPRIGCTL_NEW_PASSWORD",
                &format!("first password for {}: ", name()),
            )?;
            let role = match s(sub, "role").as_deref() {
                Some("dc") => "dc_operator",
                _ => "platform_operator",
            };
            let v: OperatorView = c
                .post(
                    "/admin/v1/operators",
                    &json!({
                        "name": name(),
                        "role": role,
                        "dcs": many(sub, "dcs"),
                        "password": password,
                        "display_name": s(sub, "display-name").unwrap_or_default(),
                        "email": s(sub, "email").unwrap_or_default(),
                        "reason": reason(),
                    }),
                )
                .await?;
            print(out, &v, |v| operators_table(std::slice::from_ref(v)));
        }
        "list" => {
            let v: Vec<OperatorView> = c.get("/admin/v1/operators").await?;
            print(out, &v, |v| operators_table(v));
        }
        "show" => {
            let v: OperatorView = c.get(&format!("/admin/v1/operators/{}", name())).await?;
            print(out, &v, |v| operators_table(std::slice::from_ref(v)));
        }
        "disable" | "enable" | "reset-factor" => {
            let method = match cmd {
                "reset-factor" => "resetFactor",
                other => other,
            };
            let v: OperatorView = c
                .post(
                    &format!("/admin/v1/operators/{}:{method}", name()),
                    &json!({"reason": reason()}),
                )
                .await?;
            print(out, &v, |v| operators_table(std::slice::from_ref(v)));
        }
        "set-password" => {
            let current = match sub.get_one::<String>("current-file") {
                Some(f) => Some(read_secret(Some(f), "", "")?),
                None => std::env::var("SPRIGCTL_PASSWORD").ok(),
            };
            let password = read_secret(
                sub.get_one::<String>("password-file"),
                "SPRIGCTL_NEW_PASSWORD",
                "new password: ",
            )?;
            let mut body = json!({"password": password, "reason": reason()});
            if let Some(cur) = current {
                body["current"] = cur.into();
            }
            let v: OperatorView = c
                .post(
                    &format!("/admin/v1/operators/{}:setPassword", name()),
                    &body,
                )
                .await?;
            print(out, &v, |v| operators_table(std::slice::from_ref(v)));
        }
        other => return Err(Failed(format!("operator {other}: unknown"))),
    }
    Ok(())
}

async fn token(c: &AdminClient, m: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, sub)) = m.subcommand() else {
        return Err(Failed("token: a subcommand".into()));
    };
    match cmd {
        "create" => {
            let mut body = json!({
                "scopes": many(sub, "scope"),
                "dcs": many(sub, "dcs"),
                "label": s(sub, "label").unwrap_or_default(),
                "reason": s(sub, "reason").unwrap_or_default(),
                "never_expires": sub.get_flag("never-expires"),
            });
            if let Some(d) = sub.get_one::<u32>("days") {
                body["expires_in_days"] = (*d).into();
            }
            if let Some(svc) = s(sub, "service") {
                body["service"] = svc.into();
                body["service_kind"] = s(sub, "kind").unwrap_or_else(|| "ci".into()).into();
            }
            let r: CreateTokenResponse = c.post("/admin/v1/operator-tokens", &body).await?;
            eprintln!("The token is shown once; store it now.");
            print(out, &r, |r| {
                format!(
                    "{}\n{}",
                    r.token,
                    tokens_table(std::slice::from_ref(&r.record))
                )
            });
        }
        "list" => {
            let path = if sub.get_flag("all") {
                "/admin/v1/operator-tokens?owner=*".to_owned()
            } else if let Some(o) = s(sub, "owner") {
                format!("/admin/v1/operator-tokens?owner={o}")
            } else {
                "/admin/v1/operator-tokens".to_owned()
            };
            let v: Vec<TokenView> = c.get(&path).await?;
            print(out, &v, |v| tokens_table(v));
        }
        "revoke" => {
            let id = s(sub, "key_id").unwrap_or_default();
            let v: TokenView = c
                .post(
                    &format!("/admin/v1/operator-tokens/{id}:revoke"),
                    &json!({"reason": s(sub, "reason").unwrap_or_default()}),
                )
                .await?;
            print(out, &v, |v| tokens_table(std::slice::from_ref(v)));
        }
        other => return Err(Failed(format!("token {other}: unknown"))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn cli() -> Command {
        Command::new("sprigctl")
            .arg(Arg::new("endpoint").long("endpoint").global(true))
            .subcommand(login_cmd())
            .subcommand(logout_cmd())
            .subcommand(whoami_cmd())
            .subcommand(operator_group())
            .subcommand(token_group())
    }

    #[test]
    fn commands_parse() {
        for args in [
            vec!["login", "--name", "alice", "--code", "123456"],
            vec!["login", "--recovery-code", "abcde-fghij"],
            vec!["logout"],
            vec!["whoami"],
            vec![
                "operator",
                "create",
                "dana",
                "--role",
                "dc",
                "--dcs",
                "dc-a,dc-b",
            ],
            vec!["operator", "list"],
            vec!["operator", "disable", "dana", "--reason", "left"],
            vec!["operator", "reset-factor", "dana"],
            vec!["operator", "set-password", "dana", "--password-file", "/x"],
            vec![
                "token",
                "create",
                "--scope",
                "clusters:read",
                "--scope",
                "settings:write",
                "--days",
                "7",
            ],
            vec![
                "token",
                "create",
                "--service",
                "gw-fra1",
                "--kind",
                "gateway",
                "--never-expires",
            ],
            vec!["token", "list", "--all"],
            vec!["token", "revoke", "abcdefghijkl"],
        ] {
            let mut full = vec!["sprigctl"];
            full.extend(args.iter().copied());
            assert!(cli().try_get_matches_from(&full).is_ok(), "{args:?}");
        }
        let m = cli()
            .try_get_matches_from([
                "sprigctl", "operator", "create", "d", "--role", "dc", "--dcs", "a,b",
            ])
            .unwrap();
        let (_, op) = m.subcommand().unwrap();
        let (_, c) = op.subcommand().unwrap();
        assert_eq!(many(c, "dcs"), vec!["a".to_owned(), "b".to_owned()]);
        assert!(
            cli()
                .try_get_matches_from(["sprigctl", "operator", "create", "d", "--role", "admin"])
                .is_err()
        );
    }

    #[test]
    fn sessions_round_trip_per_endpoint() {
        let dir = std::env::temp_dir().join(format!("sprigctl-sessions-{}", std::process::id()));
        let path = dir.join("sessions.json");
        let mut all = BTreeMap::new();
        all.insert(
            key("http://127.0.0.1:7480/"),
            Session {
                token: "sprigop_x".into(),
                operator: "alice".into(),
                expires_ms: u64::MAX,
            },
        );
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, serde_json::to_vec(&all).unwrap()).unwrap();
        let back: BTreeMap<String, Session> =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(back["http://127.0.0.1:7480"].token, "sprigop_x");
        let _ = std::fs::remove_dir_all(dir);
    }
}
