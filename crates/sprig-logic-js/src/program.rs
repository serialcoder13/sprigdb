//! One engine-tier version, whatever its language: KIRun on the native
//! runtime, JavaScript on QuickJS, Lua on the shared Lua runtime.

use serde_json::Value;
use sprig_common::Result;

use crate::budget::Limits;
use crate::engine::ScriptSig;
use crate::js::{self, JsProgram};
use crate::kirun::{self, Host, Outcome, Prepared};
use crate::lua::{self, LuaProgram};

/// A version ready to run.
#[derive(Debug, Clone)]
pub enum Program {
    /// A KIRun definition.
    Kirun(Box<Prepared>),
    /// A JavaScript function.
    Js(JsProgram),
    /// A Lua function.
    Lua(LuaProgram),
}

impl Program {
    /// Parses and checks `source` written in `runtime` (`kirun`,
    /// `javascript`, `lua`). KIRun takes its signature from the
    /// definition; the others from `sig`.
    ///
    /// # Errors
    /// 39861 for source that doesn't parse; 39876 for a runtime the
    /// engine tier doesn't have (`sql`).
    pub fn new(runtime: &str, name: &str, source: &str, sig: ScriptSig) -> Result<Self> {
        Ok(match runtime {
            "kirun" => Program::Kirun(Box::new(Prepared::new(source)?)),
            "javascript" | "js" => Program::Js(JsProgram::new(name, source, sig)?),
            "lua" => Program::Lua(LuaProgram::new(name, source, sig)?),
            other => {
                return Err(sprig_common::SprigError::new(
                    sprig_common::SprigCode::LogicEngineUnavailable,
                    format!("{name}: the engine tier has no {other} runtime"),
                ));
            }
        })
    }

    /// Converts a call's arguments as SQL hands them over to the
    /// parameters' types.
    ///
    /// # Errors
    /// 39874 for the wrong number of arguments.
    pub fn coerce(&self, name: &str, args: Vec<Value>) -> Result<Vec<Value>> {
        match self {
            Program::Kirun(p) => p.coerce(args),
            Program::Js(p) => p.sig().coerce(name, args),
            Program::Lua(p) => p.sig().coerce(name, args),
        }
    }

    /// Runs the version with positional `args`.
    ///
    /// # Errors
    /// The runtime's errors (39864, 39865, 39866, 39863, 39874, a `db`
    /// statement's own error, ...).
    pub async fn run(&self, args: Vec<Value>, host: Host, limits: Limits) -> Result<Outcome> {
        match self {
            Program::Kirun(p) => kirun::run(p, args, host, limits).await,
            Program::Js(p) => js::run(p, args, host, limits).await,
            Program::Lua(p) => lua::run(p, args, host, limits).await,
        }
    }
}
