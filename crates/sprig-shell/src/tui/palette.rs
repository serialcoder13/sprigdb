//! The command palette (Ctrl-K; the Studio and TUI redesign): one search
//! over the open tabs, the screens, the tables the explorer has loaded and
//! the commands, ranked the way Studio's palette ranks them.

/// What choosing an item does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Show screen `n` (data tabs first, then the fixed screens).
    Screen(usize),
    /// Open a new data tab.
    NewTab,
    /// Show the keys.
    Help,
    /// Export the shown result.
    Export,
    /// Reload the explorer.
    Reload,
    /// Put a statement in the current data tab's editor (not run).
    Insert(String),
    /// Open a table's view: database, name, whether it is a view.
    OpenTable(String, String, bool),
    /// Quit.
    Quit,
}

/// One row of the palette.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    /// What the row says.
    pub label: String,
    /// Grey text on the right: the kind, the database, the key.
    pub hint: String,
    /// What Enter does.
    pub action: Action,
}

/// Subsequence match with a score: a substring scores highest, earlier and
/// tighter matches higher; `None` when `query` isn't in `text` in order.
#[must_use]
pub fn score(query: &str, text: &str) -> Option<i64> {
    let q = query.to_lowercase();
    let t = text.to_lowercase();
    if q.is_empty() {
        return Some(0);
    }
    if let Some(at) = t.find(&q) {
        return Some(
            1000 - 2 * i64::try_from(at).unwrap_or(500)
                - i64::try_from(t.len() - q.len()).unwrap_or(0) / 10,
        );
    }
    let qc: Vec<char> = q.chars().collect();
    let (mut i, mut gaps, mut last) = (0usize, 0i64, None::<usize>);
    for (j, c) in t.chars().enumerate() {
        if i < qc.len() && c == qc[i] {
            if last.is_some_and(|l| j != l + 1) {
                gaps += 1;
            }
            last = Some(j);
            i += 1;
        }
    }
    (i == qc.len()).then(|| 500 - gaps * 10 - i64::try_from(t.len()).unwrap_or(0) / 10)
}

/// The items matching `query`, best first, at most `limit`.
#[must_use]
pub fn filter(items: &[Item], query: &str, limit: usize) -> Vec<Item> {
    let mut scored: Vec<(i64, &Item)> = items
        .iter()
        .filter_map(|it| {
            let s = score(query, &it.label).or_else(|| score(query, &it.hint).map(|s| s - 400))?;
            Some((s, it))
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0));
    scored
        .into_iter()
        .take(limit)
        .map(|(_, it)| it.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(label: &str, hint: &str) -> Item {
        Item {
            label: label.to_owned(),
            hint: hint.to_owned(),
            action: Action::Help,
        }
    }

    #[test]
    fn substrings_rank_before_scattered_matches() {
        let items = [
            item("parts", "table · shop"),
            item("orders", "table · shop"),
            item("Open orders", "saved"),
        ];
        let got = filter(&items, "ord", 10);
        assert_eq!(
            got.iter().map(|i| i.label.as_str()).collect::<Vec<_>>(),
            ["orders", "Open orders"]
        );
        let got = filter(&items, "prs", 10);
        assert_eq!(got[0].label, "parts");
        assert!(filter(&items, "zzz", 10).is_empty());
    }

    #[test]
    fn hints_match_after_labels() {
        let items = [
            item("orders", "table · shop"),
            item("shopping", "table · crm"),
        ];
        let got = filter(&items, "shop", 10);
        assert_eq!(got[0].label, "shopping");
        assert_eq!(got[1].label, "orders");
    }
}
