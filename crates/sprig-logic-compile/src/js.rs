//! The JavaScript front end's place (chapter 41, "JavaScript front end").
//!
//! Not built yet: it needs `oxc_parser` and `oxc_semantic`, which are not
//! in the workspace. Until then every JavaScript function is placed in
//! the engine tier with the reason `js.front_end`, so it deploys and is
//! recorded, and runs once the fallback runtime exists.

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
            "the JavaScript front end isn't built yet",
        )),
        plsql: None,
    }
}
