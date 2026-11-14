//! Documentation pages: reading `docs/` and parsing each page's front matter.

use std::path::{Path, PathBuf};

/// Folders under `docs/` that hold tooling, not pages.
pub const SKIPPED_DIRS: [&str; 2] = ["pdf", "tests"];

/// One Markdown page of the documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    /// The path under `docs/`, with `/` separators: `concepts/sharding.md`.
    pub path: String,
    /// `title` from the front matter.
    pub title: String,
    /// `audience` from the front matter: `user`, `dba`, `operator`.
    pub audience: Vec<String>,
    /// The Markdown after the front matter.
    pub body: String,
}

/// Parses a page: YAML front matter between `---` lines, then the body.
///
/// Only the two keys the pack needs are read, `title` (a plain or quoted
/// scalar) and `audience` (a flow list such as `[user, dba]`); others, such
/// as `sidebar`, are ignored.
///
/// # Errors
///
/// The page has no front matter or no title.
pub fn parse_page(path: &str, text: &str) -> Result<Page, String> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let rest = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
        .ok_or_else(|| format!("{path}: no front matter"))?;
    let mut front = Vec::new();
    let mut body_start = None;
    let mut offset = text.len() - rest.len();
    for line in rest.split_inclusive('\n') {
        offset += line.len();
        if line.trim_end() == "---" {
            body_start = Some(offset);
            break;
        }
        front.push(line.trim_end());
    }
    let body_start = body_start.ok_or_else(|| format!("{path}: front matter isn't closed"))?;
    let mut title = None;
    let mut audience = Vec::new();
    for line in front {
        if let Some(v) = line.strip_prefix("title:") {
            title = Some(unquote(v.trim()));
        } else if let Some(v) = line.strip_prefix("audience:") {
            let v = v.trim().trim_start_matches('[').trim_end_matches(']');
            audience = v
                .split(',')
                .map(|a| unquote(a.trim()))
                .filter(|a| !a.is_empty())
                .collect();
        }
    }
    let title = title
        .filter(|t| !t.is_empty())
        .ok_or_else(|| format!("{path}: no title in the front matter"))?;
    Ok(Page {
        path: path.to_owned(),
        title,
        audience,
        body: text[body_start..].to_owned(),
    })
}

/// A YAML scalar without its quotes.
fn unquote(s: &str) -> String {
    if let Some(inner) = s.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(n) = chars.next() {
                    out.push(n);
                }
            } else {
                out.push(c);
            }
        }
        out
    } else if let Some(inner) = s.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        inner.replace("''", "'")
    } else {
        s.to_owned()
    }
}

/// Every page under `docs_dir` (`*.md`, except in [`SKIPPED_DIRS`]),
/// sorted by path so the pack's input order never depends on the file
/// system.
///
/// # Errors
///
/// A directory or page can't be read, or a page has no front matter.
pub fn read_pages(docs_dir: &Path) -> Result<Vec<Page>, String> {
    let mut files = Vec::new();
    walk(docs_dir, docs_dir, &mut files)?;
    files.sort();
    files
        .into_iter()
        .map(|(rel, abs)| {
            let text =
                std::fs::read_to_string(&abs).map_err(|e| format!("{}: {e}", abs.display()))?;
            parse_page(&rel, &text)
        })
        .collect()
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<(), String> {
    let rd = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    for e in rd {
        let p = e.map_err(|e| format!("{}: {e}", dir.display()))?.path();
        let rel = p
            .strip_prefix(root)
            .unwrap_or(&p)
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/");
        if p.is_dir() {
            if dir == root && SKIPPED_DIRS.contains(&rel.as_str()) {
                continue;
            }
            walk(root, &p, out)?;
        } else if p.extension().is_some_and(|x| x == "md") {
            out.push((rel, p));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_front_matter() {
        let p = parse_page(
            "a/b.md",
            "---\ntitle: \"`sprig` \\\"commands\\\"\"\naudience: [user, dba]\nsidebar:\n  order: 3\n---\n\n# Hi\n",
        )
        .expect("page");
        assert_eq!(p.title, "`sprig` \"commands\"");
        assert_eq!(p.audience, ["user", "dba"]);
        assert_eq!(p.body, "\n# Hi\n");
        assert!(parse_page("x.md", "# no front matter").is_err());
        assert!(parse_page("x.md", "---\naudience: [user]\n---\n").is_err());
    }
}
