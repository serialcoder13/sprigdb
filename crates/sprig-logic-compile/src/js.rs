//! The JavaScript front end's place (chapter 41, "JavaScript front end").
//!
//! Not built yet: it needs `oxc_parser` and `oxc_semantic`, which are not
//! in the workspace. Until then every JavaScript function is placed in
//! the engine tier with the reason `js.front_end`, and runs there on
//! QuickJS (`sprig-logic-js::js`).

use crate::check::Analysis;
use crate::{Compiled, Reason};

/// "Compiles" JavaScript: always the engine tier, for now.
#[must_use]
pub fn compile(_source: &str) -> Compiled {
    Compiled {
        ir: None,
        analysis: Analysis::refused(Reason::new(
            "js.front_end",
            "source",
            "no compiler front end for JavaScript yet: it runs in the engine tier (QuickJS)",
        )),
        plsql: None,
    }
}
