//! `sprigctl ai provider add|list|test|remove`, over the Admin API's
//! `/admin/v1/ai/providers`.
//!
//! ```text
//! sprigctl ai provider add anthropic --kind anthropic --key-file ~/.anthropic-key
//! sprigctl ai provider add ollama --kind openai --base-url http://127.0.0.1:11434/v1 --no-key
//! sprigctl ai provider list
//! sprigctl ai provider test anthropic --model claude-opus-5-5
//! sprigctl ai provider remove ollama
//! ```
//!
//! The key is read from `--key-file` (`-` for standard input), or asked
//! for on the terminal without echo, or read from standard input when it
//! isn't a terminal. It is never printed, and the controller seals it with
//! the master key file; no listing shows it again. `test` makes one tiny
//! call (16 output tokens at most).

use std::io::{IsTerminal, Read};

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde_json::{Value, json};

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl ai`.
pub(crate) fn group() -> Command {
    let name = || {
        Arg::new("name")
            .required(true)
            .help("The provider's name: 1 to 64 of a-z, 0-9, _ and -")
    };
    Command::new("ai")
        .about("The AI assistants: providers")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("provider")
                .about("AI providers the assistants call")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("add")
                        .about("Add or replace a provider; its key is sealed and never shown again")
                        .arg(name())
                        .arg(
                            Arg::new("kind")
                                .long("kind")
                                .required(true)
                                .value_parser(["anthropic", "openai"])
                                .help("anthropic (the Messages API) or openai (any OpenAI-compatible Chat Completions server: OpenAI, Azure OpenAI, Ollama, vLLM, Mistral, Groq)"),
                        )
                        .arg(
                            Arg::new("base-url")
                                .long("base-url")
                                .help("The API's base URL (default https://api.anthropic.com or https://api.openai.com/v1)"),
                        )
                        .arg(
                            Arg::new("key-header")
                                .long("key-header")
                                .value_parser(["bearer", "api_key", "none"])
                                .help("openai only: how the key is sent (api_key for Azure OpenAI)"),
                        )
                        .arg(
                            Arg::new("query")
                                .long("query")
                                .help("openai only: a query string for every call, such as api-version=2024-10-21"),
                        )
                        .arg(
                            Arg::new("provider-ca-file")
                                .long("provider-ca-file")
                                .help("A CA file to trust instead of the public roots (a TLS-inspecting proxy)"),
                        )
                        .arg(
                            Arg::new("key-file")
                                .long("key-file")
                                .help("Read the API key from this file; - reads standard input"),
                        )
                        .arg(
                            Arg::new("no-key")
                                .long("no-key")
                                .action(ArgAction::SetTrue)
                                .conflicts_with("key-file")
                                .help("The provider takes no key (a local server such as Ollama)"),
                        )
                        .arg(
                            Arg::new("keep-key")
                                .long("keep-key")
                                .action(ArgAction::SetTrue)
                                .conflicts_with_all(["key-file", "no-key"])
                                .help("Replacing a provider: keep the key it has"),
                        ),
                )
                .subcommand(Command::new("list").about("List providers (keys are never shown)"))
                .subcommand(
                    Command::new("test")
                        .about("Make one tiny call to a provider")
                        .arg(name())
                        .arg(
                            Arg::new("model")
                                .long("model")
                                .required(true)
                                .help("The model to call, as the provider names it"),
                        ),
                )
                .subcommand(
                    Command::new("remove")
                        .about("Remove a provider")
                        .arg(name()),
                ),
        )
}

fn key_from(m: &ArgMatches) -> Result<Option<String>, Failed> {
    if m.get_flag("no-key") || m.get_flag("keep-key") {
        return Ok(None);
    }
    let raw = match m.get_one::<String>("key-file").map(String::as_str) {
        Some("-") => read_stdin()?,
        Some(p) => std::fs::read_to_string(p).map_err(|e| format!("--key-file {p}: {e}"))?,
        None if std::io::stdin().is_terminal() => crate::operator::prompt_line("API key: ", true)?,
        None => read_stdin()?,
    };
    let k = raw.trim().to_owned();
    if k.is_empty() {
        return Err(Failed(
            "the API key is empty (use --no-key for a provider without one)".into(),
        ));
    }
    Ok(Some(k))
}

fn read_stdin() -> Result<String, Failed> {
    let mut s = String::new();
    std::io::stdin()
        .read_to_string(&mut s)
        .map_err(|e| format!("reading the key from standard input: {e}"))?;
    Ok(s)
}

/// The add request's body, without the key.
fn settings_of(m: &ArgMatches) -> Value {
    let kind = m.get_one::<String>("kind").map_or("openai", String::as_str);
    let mut s = json!({ "kind": kind });
    if let Some(u) = m.get_one::<String>("base-url") {
        s["base_url"] = json!(u);
    }
    if let Some(c) = m.get_one::<String>("provider-ca-file") {
        s["ca_file"] = json!(c);
    }
    if kind == "openai" {
        if let Some(h) = m.get_one::<String>("key-header") {
            s["key_header"] = json!(h);
        } else if m.get_flag("no-key") {
            s["key_header"] = json!("none");
        }
        if let Some(q) = m.get_one::<String>("query") {
            s["query"] = json!(q);
        }
    }
    s
}

fn providers_table(v: &Value) -> String {
    let rows: Vec<[String; 4]> = v["providers"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|p| {
            [
                p["name"].as_str().unwrap_or_default().to_owned(),
                p["kind"].as_str().unwrap_or_default().to_owned(),
                p["settings"]["base_url"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                if p["has_key"].as_bool().unwrap_or(false) {
                    "yes".to_owned()
                } else {
                    "no".to_owned()
                },
            ]
        })
        .collect();
    if rows.is_empty() {
        return "no AI providers\n".to_owned();
    }
    table(["NAME", "KIND", "BASE URL", "KEY"], &rows)
}

/// Runs `sprigctl ai ...`.
///
/// # Errors
/// The API's; a key that can't be read.
pub(crate) async fn run(
    c: &AdminClient,
    group: &ArgMatches,
    dry_run: bool,
    out: Output,
) -> Result<(), Failed> {
    let Some(("provider", p)) = group.subcommand() else {
        return Err(Failed("sprigctl ai provider add|list|test|remove".into()));
    };
    let Some((cmd, m)) = p.subcommand() else {
        return Err(Failed("sprigctl ai provider add|list|test|remove".into()));
    };
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    match cmd {
        "add" => {
            let settings = settings_of(m);
            if dry_run {
                println!("would add AI provider {name}: {settings}");
                return Ok(());
            }
            let mut body = json!({"name": name, "settings": settings});
            if let Some(k) = key_from(m)? {
                body["key"] = json!(k);
            }
            let r: Value = c.post("/admin/v1/ai/providers", &body).await?;
            print(out, &r, |r| {
                format!(
                    "AI provider {} ({}) saved; key {}\n",
                    r["name"].as_str().unwrap_or_default(),
                    r["kind"].as_str().unwrap_or_default(),
                    if r["has_key"].as_bool().unwrap_or(false) {
                        "sealed"
                    } else {
                        "none"
                    }
                )
            });
        }
        "list" => {
            let r: Value = c.get("/admin/v1/ai/providers").await?;
            print(out, &r, providers_table);
        }
        "test" => {
            let model = m.get_one::<String>("model").cloned().unwrap_or_default();
            if dry_run {
                println!("would call AI provider {name} with model {model}");
                return Ok(());
            }
            let r: Value = c
                .post(
                    &format!("/admin/v1/ai/providers/{}:test", escape(&name)),
                    &json!({"model": model}),
                )
                .await?;
            print(out, &r, |r| {
                format!(
                    "{} answered with {} in {} ms ({}): {}\n",
                    name,
                    r["model"].as_str().unwrap_or_default(),
                    r["ms"],
                    r["stop"].as_str().unwrap_or_default(),
                    r["text"].as_str().unwrap_or_default().trim()
                )
            });
        }
        "remove" => {
            if dry_run {
                println!("would remove AI provider {name}");
                return Ok(());
            }
            let r: Value = c
                .delete(&format!("/admin/v1/ai/providers/{}", escape(&name)))
                .await?;
            print(out, &r, |r| {
                if r["removed"].as_bool().unwrap_or(false) {
                    format!("AI provider {name} removed\n")
                } else {
                    format!("there was no AI provider {name}\n")
                }
            });
        }
        other => return Err(Failed(format!("unknown command ai provider {other}"))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_builds_settings_without_the_key() {
        let matches = group().get_matches_from([
            "ai",
            "provider",
            "add",
            "ollama",
            "--kind",
            "openai",
            "--base-url",
            "http://127.0.0.1:11434/v1",
            "--no-key",
        ]);
        let (_, provider) = matches.subcommand().unwrap();
        let (_, add) = provider.subcommand().unwrap();
        let settings = settings_of(add);
        assert_eq!(settings["kind"], "openai");
        assert_eq!(settings["key_header"], "none");
        assert_eq!(key_from(add).unwrap(), None);
        let table = providers_table(&json!({"providers": [
            {"name": "ollama", "kind": "open_ai_chat", "settings": {"base_url": "http://x"}, "has_key": false}
        ]}));
        assert!(table.contains("ollama") && table.contains("no"));
    }
}
