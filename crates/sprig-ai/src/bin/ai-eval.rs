//! `ai-eval`: runs the assistants' evaluation sets.
//!
//! ```text
//! cargo run -p sprig-ai --features eval --bin ai-eval -- [--role all|admin|triage|dba|data]
//!     [--mode stub|live] [--model provider/model] [--judge provider/model]
//!     [--set FILE] [--case ID]... [--out DIR] [--baseline FILE|last] [--tolerance X]
//! ```
//!
//! Stub mode (the default) needs nothing and sends nothing. Live mode
//! calls a real model and needs `SPRIG_AI_EVAL_LIVE=1` and the provider's
//! key (see `sprig_ai::eval::live`). Exit codes: 0 passed, 1 a case
//! failed (stub) or a score dropped against the baseline, 2 bad usage or
//! configuration.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use sprig_ai::eval::live::{self, ProviderFile};
use sprig_ai::eval::{self, CaseRecord, Judge, Mode, Summary, report};
use sprig_ai::role::Role;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RoleArg {
    All,
    Admin,
    Triage,
    Dba,
    Data,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ModeArg {
    Stub,
    Live,
}

/// Runs the AI assistants' evaluation sets.
#[derive(Debug, Parser)]
#[command(name = "ai-eval")]
struct Args {
    /// The set to run.
    #[arg(long, value_enum, default_value = "all")]
    role: RoleArg,
    /// `stub` (scripted, deterministic, offline) or `live` (a real model).
    #[arg(long, value_enum, default_value = "stub")]
    mode: ModeArg,
    /// The model, as `provider/model` (live mode).
    #[arg(long)]
    model: Option<String>,
    /// A judge model for the rubric, as `provider/model` (live mode).
    #[arg(long)]
    judge: Option<String>,
    /// A set file instead of the shipped one (one role).
    #[arg(long)]
    set: Option<PathBuf>,
    /// Only these cases.
    #[arg(long = "case")]
    cases: Vec<String>,
    /// Where results go.
    #[arg(long)]
    out: Option<PathBuf>,
    /// A summary to compare with, or `last` for the previous run of the
    /// same role and model.
    #[arg(long)]
    baseline: Option<String>,
    /// How far the mean score may drop before the run fails.
    #[arg(long, default_value_t = 0.0)]
    tolerance: f64,
}

fn default_out() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/ai-evals")
        .components()
        .collect()
}

fn roles(r: RoleArg) -> Vec<Role> {
    match r {
        RoleArg::All => Role::ALL.to_vec(),
        RoleArg::Admin => vec![Role::Admin],
        RoleArg::Triage => vec![Role::Triage],
        RoleArg::Dba => vec![Role::Dba],
        RoleArg::Data => vec![Role::Data],
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn fail(msg: &str) -> ExitCode {
    eprintln!("ai-eval: {msg}");
    ExitCode::from(2)
}

fn print_failures(records: &[CaseRecord]) {
    for r in records.iter().filter(|r| !r.pass) {
        println!("  FAIL {} (score {:.2})", r.id, r.score);
        for s in &r.safety {
            println!("    unsafe: {s}");
        }
        for s in &r.failures {
            println!("    plumbing: {s}");
        }
        for s in &r.misses {
            println!("    missed: {s}");
        }
    }
}

#[allow(clippy::too_many_lines)] // the whole command
fn main() -> ExitCode {
    let args = Args::parse();
    let mode = match args.mode {
        ModeArg::Stub => Mode::Stub,
        ModeArg::Live => Mode::Live,
    };
    let roles = roles(args.role);
    if args.set.is_some() && roles.len() != 1 {
        return fail("--set needs one --role");
    }
    let out = args.out.clone().unwrap_or_else(default_out);

    // Live: the model and the judge.
    let file: Option<ProviderFile> = match std::env::var(live::PROVIDERS_ENV) {
        Ok(p) => match std::fs::read_to_string(&p).map_err(|e| e.to_string()).and_then(|t| live::parse_providers(&t)) {
            Ok(f) => Some(f),
            Err(e) => return fail(&format!("{p}: {e}")),
        },
        Err(_) => None,
    };
    let (provider, model_id, label) = if mode == Mode::Live {
        let Some(spec) = &args.model else {
            return fail("live mode needs --model provider/model");
        };
        match live::provider(spec, file.as_ref()) {
            Ok((p, m)) => (Some(p), m, spec.clone()),
            Err(e) => return fail(&e),
        }
    } else {
        (None, "stub".to_owned(), "stub".to_owned())
    };
    let judge = match (&args.judge, mode) {
        (Some(spec), Mode::Live) => match live::provider(spec, file.as_ref()) {
            Ok((p, m)) => Some(Judge {
                provider: p,
                model: m,
            }),
            Err(e) => return fail(&e),
        },
        (Some(_), Mode::Stub) => return fail("--judge is for live mode"),
        _ => None,
    };

    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => return fail(&e.to_string()),
    };
    let reg = sprig_ai::ToolRegistry::builtin();
    let mut summaries = Vec::new();
    let mut failed = false;
    let mut verdicts = Vec::new();
    for role in roles {
        let text = match &args.set {
            Some(p) => match std::fs::read_to_string(p) {
                Ok(t) => t,
                Err(e) => return fail(&format!("{}: {e}", p.display())),
            },
            None => eval::shipped(role).to_owned(),
        };
        let set = match eval::load(&text) {
            Ok(s) if s.role == role => s,
            Ok(s) => return fail(&format!("the set is for {}, not {role}", s.role)),
            Err(e) => return fail(&format!("{role}: {e}")),
        };
        if let Err(errs) = eval::check_set(&set, &reg) {
            println!("{role}: the set fails its static checks:");
            for e in errs {
                println!("  {e}");
            }
            failed = true;
            continue;
        }
        let started = now();
        let records = rt.block_on(async {
            match &provider {
                None => eval::run_set_stub(&set, &args.cases).await,
                Some(p) => {
                    let p = Arc::clone(p);
                    eval::run_set_live(&set, &args.cases, &p, &model_id, judge.as_ref(), &mut |r| {
                        println!(
                            "  {:<10} {:>5.2} {}",
                            r.id,
                            r.score,
                            if r.pass { "pass" } else { "FAIL" }
                        );
                    })
                    .await
                }
            }
        });
        if mode == Mode::Stub {
            for p in rt.block_on(eval::probes::run(role)) {
                if !p.ok() {
                    println!("{role}: probe {} failed: {:?}", p.name, p.problems);
                    failed = true;
                }
            }
            if records.iter().any(|r| !r.pass) {
                failed = true;
            }
        }
        print_failures(&records);
        let summary = Summary::of(
            role,
            mode,
            &label,
            &eval::prompt_version(role),
            &eval::set_version(&text),
            started,
            &records,
        );
        let path = match report::write(&out, &summary, &records) {
            Ok(p) => p,
            Err(e) => return fail(&format!("writing results under {}: {e}", out.display())),
        };
        println!("{role}: results in {}", path.display());
        if let Some(b) = &args.baseline {
            let base_path = if b == "last" {
                report::last_summary(&out, role, &label, &path)
            } else {
                Some(PathBuf::from(b))
            };
            match base_path.map(|p| report::read_summary(&p).map(|s| (p, s))) {
                Some(Ok((p, base))) => {
                    let v = eval::compare(&summary, &base, args.tolerance);
                    verdicts.push(format!("baseline {}:", p.display()));
                    verdicts.extend(v.lines.iter().map(|l| format!("  {l}")));
                    if !v.ok {
                        failed = true;
                    }
                }
                Some(Err(e)) => return fail(&format!("the baseline: {e}")),
                None => verdicts.push(format!("{role}: no earlier run to compare with")),
            }
        }
        summaries.push(summary);
    }
    println!();
    print!("{}", report::table(&summaries));
    for v in verdicts {
        println!("{v}");
    }
    if failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
