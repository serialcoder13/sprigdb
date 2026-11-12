//! The TUI's colours (decision R6 of `sprigdb-plan/decisions/studio-tui-redesign.md`):
//! the same calm palette as Studio. Neutral greys, the coral-gold accent
//! (`#E0805A`) only for small marks (the active tab, the focused pane, the
//! connected dot), indigo keywords, burnt-orange strings, coral numbers.
//!
//! 24-bit colour when the terminal says so (`COLORTERM=truecolor|24bit`),
//! the xterm 256-colour palette otherwise, and no colour at all under
//! `NO_COLOR` (selection then shows as reverse video).

use std::sync::OnceLock;

use ratatui::style::{Color, Modifier, Style};

/// How much colour the terminal takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Depth {
    /// `NO_COLOR` or a dumb terminal.
    None,
    /// The xterm 256-colour palette.
    Ansi256,
    /// 24-bit colour.
    TrueColor,
}

/// The detected depth, once per process.
#[must_use]
pub fn depth() -> Depth {
    static D: OnceLock<Depth> = OnceLock::new();
    *D.get_or_init(|| {
        detect(
            std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()),
            std::env::var("COLORTERM").ok().as_deref(),
            std::env::var("TERM").ok().as_deref(),
        )
    })
}

/// The depth for these environment values (separate for tests).
#[must_use]
pub fn detect(no_color: bool, colorterm: Option<&str>, term: Option<&str>) -> Depth {
    if no_color || term == Some("dumb") {
        Depth::None
    } else if colorterm
        .is_some_and(|c| c.eq_ignore_ascii_case("truecolor") || c.eq_ignore_ascii_case("24bit"))
    {
        Depth::TrueColor
    } else {
        Depth::Ansi256
    }
}

/// One palette colour: its 24-bit value and its nearest xterm index.
#[derive(Debug, Clone, Copy)]
struct Swatch(u8, u8, u8, u8);

const ACCENT: Swatch = Swatch(224, 128, 90, 173);
const MUTED: Swatch = Swatch(154, 161, 171, 247);
const BORDER: Swatch = Swatch(70, 76, 86, 239);
const SEL_BG: Swatch = Swatch(48, 52, 60, 236);
const KEYWORD: Swatch = Swatch(143, 164, 255, 111);
const STRING: Swatch = Swatch(240, 163, 108, 215);
const NUMBER: Swatch = Swatch(240, 138, 106, 209);
const FUNC: Swatch = Swatch(196, 167, 245, 183);
const WARN: Swatch = Swatch(242, 174, 28, 214);
const ERROR: Swatch = Swatch(255, 138, 128, 210);
const INK: Swatch = Swatch(28, 32, 38, 234);
const ADDED: Swatch = Swatch(134, 205, 130, 114);

fn c(s: Swatch) -> Color {
    match depth() {
        Depth::None => Color::Reset,
        Depth::Ansi256 => Color::Indexed(s.3),
        Depth::TrueColor => Color::Rgb(s.0, s.1, s.2),
    }
}

fn fg(s: Swatch) -> Style {
    if depth() == Depth::None {
        Style::new()
    } else {
        Style::new().fg(c(s))
    }
}

/// Small accent marks: the connected dot, a focused title.
#[must_use]
pub fn accent() -> Style {
    fg(ACCENT)
}

/// Secondary text: hints, line numbers, notes.
#[must_use]
pub fn muted() -> Style {
    fg(MUTED)
}

/// An unfocused pane's border.
#[must_use]
pub fn border() -> Style {
    if depth() == Depth::None {
        Style::new().add_modifier(Modifier::DIM)
    } else {
        fg(BORDER)
    }
}

/// The focused pane's border.
#[must_use]
pub fn focus_border() -> Style {
    if depth() == Depth::None {
        Style::new().add_modifier(Modifier::BOLD)
    } else {
        fg(ACCENT)
    }
}

/// The active tab: accent and bold, no filled box.
#[must_use]
pub fn tab_active() -> Style {
    accent().add_modifier(Modifier::BOLD)
}

/// A selected row in a list or menu.
#[must_use]
pub fn selected() -> Style {
    if depth() == Depth::None {
        Style::new().add_modifier(Modifier::REVERSED)
    } else {
        Style::new().bg(c(SEL_BG)).add_modifier(Modifier::BOLD)
    }
}

/// A badge in the status line (mode, transaction): ink on the accent.
#[must_use]
pub fn badge() -> Style {
    if depth() == Depth::None {
        Style::new().add_modifier(Modifier::REVERSED)
    } else {
        Style::new().fg(c(INK)).bg(c(ACCENT))
    }
}

/// A warning badge (an open transaction): ink on amber.
#[must_use]
pub fn warn_badge() -> Style {
    if depth() == Depth::None {
        Style::new().add_modifier(Modifier::REVERSED | Modifier::BOLD)
    } else {
        Style::new().fg(c(INK)).bg(c(WARN))
    }
}

/// Warnings and notes that need attention.
#[must_use]
pub fn warn() -> Style {
    fg(WARN)
}

/// Errors.
#[must_use]
pub fn error() -> Style {
    fg(ERROR)
}

/// A staged new row (the table view): green.
#[must_use]
pub fn added() -> Style {
    if depth() == Depth::None {
        Style::new().add_modifier(Modifier::BOLD)
    } else {
        fg(ADDED)
    }
}

/// A staged changed cell: amber.
#[must_use]
pub fn changed() -> Style {
    if depth() == Depth::None {
        Style::new().add_modifier(Modifier::UNDERLINED)
    } else {
        fg(WARN)
    }
}

/// A row staged for deletion: red and struck through.
#[must_use]
pub fn deleted() -> Style {
    error().add_modifier(Modifier::CROSSED_OUT)
}

/// SQL keywords; JSON keys.
#[must_use]
pub fn keyword() -> Style {
    fg(KEYWORD)
}

/// String literals.
#[must_use]
pub fn string() -> Style {
    fg(STRING)
}

/// Numbers and booleans.
#[must_use]
pub fn number() -> Style {
    fg(NUMBER)
}

/// Functions, streams and sinks.
#[must_use]
pub fn func() -> Style {
    fg(FUNC)
}

/// Comments.
#[must_use]
pub fn comment() -> Style {
    muted().add_modifier(Modifier::ITALIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_follows_the_environment() {
        assert_eq!(
            detect(true, Some("truecolor"), Some("xterm-256color")),
            Depth::None
        );
        assert_eq!(detect(false, None, Some("dumb")), Depth::None);
        assert_eq!(detect(false, Some("truecolor"), None), Depth::TrueColor);
        assert_eq!(detect(false, Some("24bit"), None), Depth::TrueColor);
        assert_eq!(detect(false, None, Some("xterm-256color")), Depth::Ansi256);
    }
}
