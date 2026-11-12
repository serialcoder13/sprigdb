//! `sprig-docs-gen`: writes SprigDB's generated reference pages
//! (`docs/reference/*.md`) from the code, or checks they're current
//! (chapter 55, "Interfaces"; decision AI16).
//!
//! ```text
//! cargo run -p sprig-docs-gen -- [--root <repo>] [--check]
//!     [--sprigctl <path> --sprig <path>]     # also the command pages
//! ```
//!
//! | Page | From |
//! |---|---|
//! | `settings.md` | the settings registry, linked as in the `sprigdb` binary |
//! | `errors.md` | `SprigCode::ALL` and `docs/errors-what-to-do.toml` |
//! | `metrics.md` | the metric macros under `crates/`, and `docs/metrics-help.toml` |
//! | `api.md` | the gateway's `OpenAPI` document |
//! | `sprigctl.md`, `sprig.md` | each command's `--help` (only with the binaries' paths) |
//!
//! With `--check` it writes nothing and exits 1 if any page would change.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use sprig_docs::metrics_scan::Scanner;
use sprig_docs::reference::{
    self, ApiOp, CommandHelp, ErrorDoc, SettingDoc, api_page, cli_page, errors_page, metrics_page,
    settings_page,
};

/// The pages generated from code alone, by file name.
fn pages(root: &Path) -> Result<Vec<(String, String)>, String> {
    let error_help = read(&root.join("docs/errors-what-to-do.toml"))
        .map(|t| reference::parse_error_help(&t))
        .transpose()?
        .unwrap_or_default();
    let metric_help = read(&root.join("docs/metrics-help.toml"))
        .map(|t| reference::parse_metric_help(&t))
        .transpose()?
        .unwrap_or_default();
    Ok(vec![
        ("settings.md".into(), settings_page(&settings()?)),
        ("errors.md".into(), errors_page(&errors(), &error_help)),
        (
            "metrics.md".into(),
            metrics_page(&metrics(&root.join("crates"))?, &metric_help),
        ),
        ("api.md".into(), api_page(&api_ops())),
    ])
}

fn read(p: &Path) -> Option<String> {
    std::fs::read_to_string(p).ok()
}

fn scope_names(s: sprig_settings::Scopes) -> Vec<String> {
    ["object", "database", "tenant", "cluster", "dc", "global"]
        .iter()
        .enumerate()
        .filter(|(i, _)| s.0 & (1 << i) != 0)
        .map(|(_, n)| (*n).to_owned())
        .collect()
}

fn value_text(v: &sprig_settings::Value) -> String {
    use sprig_settings::Value;
    match v {
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Str(s) => s.clone(),
    }
}

fn bound(x: f64) -> String {
    if x.fract() == 0.0 && x.abs() < 1e15 {
        format!("{x:.0}")
    } else {
        x.to_string()
    }
}

fn settings() -> Result<Vec<SettingDoc>, String> {
    use sprig_settings::{Apply, ValueKind};
    let reg = sprig_settings::Registry::global().map_err(|e| e.message)?;
    Ok(reg
        .iter()
        .map(|s| {
            let h = s.hard();
            let bounds = match (h.min, h.max, h.allowed) {
                (_, _, Some(a)) => a.join(", "),
                (Some(lo), Some(hi), None) => format!("{} to {}", bound(lo), bound(hi)),
                (Some(lo), None, None) => format!("at least {}", bound(lo)),
                (None, Some(hi), None) => format!("at most {}", bound(hi)),
                (None, None, None) => String::new(),
            };
            SettingDoc {
                key: s.key().to_owned(),
                kind: match s.kind() {
                    ValueKind::Bool => "bool",
                    ValueKind::Int => "int",
                    ValueKind::Float => "float",
                    ValueKind::Str => "text",
                }
                .to_owned(),
                default: value_text(&s.default_value()),
                scopes: scope_names(s.scopes()),
                bounds,
                apply: match s.apply() {
                    Apply::Live => "live",
                    Apply::NewSessions => "new sessions",
                    Apply::Rebind => "rebind",
                }
                .to_owned(),
                operator_only: s.operator_only(),
                doc: s.doc().to_owned(),
            }
        })
        .collect())
}

fn errors() -> Vec<ErrorDoc> {
    use sprig_common::{SprigCode, SprigError};
    SprigCode::ALL
        .iter()
        .map(|&c| {
            let p = SprigError::new(c, "").protocol();
            ErrorDoc {
                number: c.number(),
                name: c.name().to_owned(),
                mongo: c.mongo_code_name().to_owned(),
                sqlstate: String::from_utf8_lossy(&c.sqlstate()).into_owned(),
                retryable: c.retryable(),
                http: p.http_status,
                grpc: format!("{:?}", p.grpc_status),
            }
        })
        .collect()
}

fn metrics(crates: &Path) -> Result<Vec<sprig_docs::metrics_scan::Metric>, String> {
    let mut scanner = Scanner::default();
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(crates)
        .map_err(|e| format!("{}: {e}", crates.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("src").is_dir())
        .collect();
    dirs.sort();
    for d in dirs {
        let name = d
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut files = Vec::new();
        walk(&d.join("src"), &mut files);
        files.sort();
        for f in files {
            if let Some(t) = read(&f) {
                scanner.scan(&name, &t);
            }
        }
    }
    Ok(scanner.metrics())
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

fn api_ops() -> Vec<ApiOp> {
    let doc = sprig_gateway::http::studio::openapi();
    let mut out = Vec::new();
    for (path, item) in doc["paths"].as_object().into_iter().flatten() {
        for (method, op) in item.as_object().into_iter().flatten() {
            if !matches!(method.as_str(), "get" | "post" | "put" | "patch" | "delete") {
                continue;
            }
            let s = |k: &str| op.get(k).and_then(serde_json::Value::as_str).unwrap_or("");
            out.push(ApiOp {
                tag: op["tags"][0].as_str().unwrap_or("other").to_owned(),
                method: method.to_uppercase(),
                path: path.clone(),
                id: s("operationId").to_owned(),
                summary: s("summary").to_owned(),
            });
        }
    }
    out
}

/// `tool`'s help and every subcommand's, found by reading the "Commands:"
/// section of each help text.
fn command_help(bin: &Path, tool: &str) -> Result<Vec<CommandHelp>, String> {
    fn help(bin: &Path, args: &[String]) -> Result<String, String> {
        let out = std::process::Command::new(bin)
            .args(args)
            .arg("--help")
            .env("NO_COLOR", "1")
            .output()
            .map_err(|e| format!("{}: {e}", bin.display()))?;
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }
    fn subcommands(help: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut on = false;
        for l in help.lines() {
            if l.trim_end() == "Commands:" {
                on = true;
                continue;
            }
            if on {
                if l.trim().is_empty() || !l.starts_with("  ") {
                    break;
                }
                if let Some(name) = l.split_whitespace().next()
                    && name != "help"
                {
                    out.push(name.to_owned());
                }
            }
        }
        out
    }
    let mut out = Vec::new();
    let mut queue: Vec<Vec<String>> = vec![Vec::new()];
    while let Some(args) = queue.pop() {
        let text = help(bin, &args)?;
        let mut subs: Vec<Vec<String>> = subcommands(&text)
            .into_iter()
            .map(|s| {
                let mut a = args.clone();
                a.push(s);
                a
            })
            .collect();
        subs.reverse();
        queue.extend(subs);
        let path = std::iter::once(tool.to_owned())
            .chain(args.iter().cloned())
            .collect::<Vec<_>>()
            .join(" ");
        out.push(CommandHelp { path, help: text });
    }
    Ok(out)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1));
    let check = args.iter().any(|a| a == "--check");
    let root = flag("--root").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."),
        PathBuf::from,
    );
    let mut all = match pages(&root) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("sprig-docs-gen: {e}");
            return ExitCode::from(2);
        }
    };
    for (tool, intro) in [
        (
            "sprigctl",
            "`sprigctl` is the operator's command-line tool: clusters, tenants, settings, \
             indexes, approvals, moves, failover and the rest of the Admin API.",
        ),
        (
            "sprig",
            "`sprig` is the shell for tenant users and operators: a full-screen TUI and a \
             scriptable CLI over the REST API or the native protocols.",
        ),
    ] {
        if let Some(bin) = flag(&format!("--{tool}")) {
            match command_help(Path::new(bin), tool) {
                Ok(c) => all.push((format!("{tool}.md"), cli_page(tool, intro, &c))),
                Err(e) => {
                    eprintln!("sprig-docs-gen: {e}");
                    return ExitCode::from(2);
                }
            }
        }
    }
    let dir = root.join("docs/reference");
    let mut stale = BTreeMap::new();
    for (name, text) in &all {
        let path = dir.join(name);
        if read(&path).as_deref() == Some(text.as_str()) {
            continue;
        }
        if check {
            stale.insert(name.clone(), ());
        } else if let Err(e) =
            std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&path, text))
        {
            eprintln!("sprig-docs-gen: {}: {e}", path.display());
            return ExitCode::from(2);
        } else {
            println!("wrote {}", path.display());
        }
    }
    if !stale.is_empty() {
        eprintln!(
            "docs/reference is stale: {}. Run `cargo run -p sprig-docs-gen` and commit the pages.",
            stale.keys().cloned().collect::<Vec<_>>().join(", ")
        );
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Settings from crates all over the workspace are linked in; a key
    /// missing here would be missing from the docs.
    #[test]
    fn the_registry_holds_every_crates_settings() {
        let keys: Vec<String> = settings().expect("registry").into_iter().map(|s| s.key).collect();
        for k in [
            "auto_index.min_runs",
            "studio.history_max",
            "studio.persist_drafts",
        ] {
            assert!(keys.iter().any(|x| x == k), "{k} missing");
        }
        let prefixes: std::collections::BTreeSet<&str> =
            keys.iter().filter_map(|k| k.split('.').next()).collect();
        assert!(prefixes.len() > 20, "only {prefixes:?}");
    }

    /// The generated pages in `docs/reference` match the code.
    #[test]
    fn docs_reference_is_current() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        for (name, text) in pages(&root).expect("pages") {
            let have = read(&root.join("docs/reference").join(&name)).unwrap_or_default();
            assert!(
                have == text,
                "docs/reference/{name} is stale: run `cargo run -p sprig-docs-gen`"
            );
        }
    }
}
