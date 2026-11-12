//! Icons by name (decision R8 of `sprigdb-plan/decisions/studio-tui-redesign.md`):
//! the names Studio uses, drawn as single-width Unicode glyphs with no emoji
//! presentation, or as ASCII when the locale isn't UTF-8 or `SPRIG_ASCII` is set.

use std::sync::OnceLock;

/// The icons the TUI draws.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Icon {
    /// A query tab.
    Query,
    /// A table.
    Table,
    /// Dashboards.
    Dashboard,
    /// Operator commands.
    Admin,
    /// Approvals.
    Inbox,
    /// Saved queries.
    Saved,
    /// History.
    History,
    /// A command in the palette.
    Command,
    /// The connected dot.
    Dot,
}

/// Whether to draw ASCII only.
#[must_use]
pub fn ascii() -> bool {
    static A: OnceLock<bool> = OnceLock::new();
    *A.get_or_init(|| {
        // Golden screens are drawn in Unicode whatever the test machine's locale.
        if cfg!(test) {
            return false;
        }
        let set = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        if set("SPRIG_ASCII").is_some() {
            return true;
        }
        let locale = set("LC_ALL")
            .or_else(|| set("LC_CTYPE"))
            .or_else(|| set("LANG"));
        locale.is_some_and(|l| !l.to_ascii_uppercase().replace('-', "").contains("UTF8"))
    })
}

/// The glyph for `icon`.
#[must_use]
pub fn glyph(icon: Icon) -> &'static str {
    if ascii() {
        ascii_glyph(icon)
    } else {
        unicode_glyph(icon)
    }
}

/// The Unicode glyph (single width, text presentation).
#[must_use]
pub fn unicode_glyph(icon: Icon) -> &'static str {
    match icon {
        Icon::Query => "❯",
        Icon::Table => "▦",
        Icon::Dashboard => "◔",
        Icon::Admin => "◈",
        Icon::Inbox => "⚑",
        Icon::Saved => "★",
        Icon::History => "↺",
        Icon::Command => "›",
        Icon::Dot => "●",
    }
}

/// The ASCII fallback.
#[must_use]
pub fn ascii_glyph(icon: Icon) -> &'static str {
    match icon {
        Icon::Query => ">",
        Icon::Table => "#",
        Icon::Dashboard => "%",
        Icon::Admin => "@",
        Icon::Inbox => "!",
        Icon::Saved => "*",
        Icon::History => "~",
        Icon::Command => "-",
        Icon::Dot => "o",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use unicode_width::UnicodeWidthStr;

    #[test]
    fn every_glyph_is_one_cell_wide() {
        for i in [
            Icon::Query,
            Icon::Table,
            Icon::Dashboard,
            Icon::Admin,
            Icon::Inbox,
            Icon::Saved,
            Icon::History,
            Icon::Command,
            Icon::Dot,
        ] {
            assert_eq!(unicode_glyph(i).width(), 1, "{i:?}");
            assert!(ascii_glyph(i).is_ascii());
        }
    }
}
