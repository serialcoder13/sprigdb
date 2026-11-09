//! A small YAML writer for the manifests the operator generates (the Helm
//! chart's `crds/`, `sprigctl`-style dumps). It writes the block style
//! `kubectl` and Helm read, from a `serde_json::Value`; strings that could
//! read as anything but a string are double-quoted (JSON escapes are valid
//! YAML escapes). It never parses YAML.

use serde_json::Value;

/// `v` as a YAML document (no leading `---`), ending in a newline.
#[must_use]
pub fn to_yaml(v: &Value) -> String {
    let mut out = String::new();
    match v {
        Value::Object(m) if !m.is_empty() => write_map(&mut out, m, 0),
        Value::Array(a) if !a.is_empty() => write_seq(&mut out, a, 0),
        other => {
            out.push_str(&scalar(other));
            out.push('\n');
        }
    }
    out
}

/// Several documents separated by `---`.
#[must_use]
pub fn to_yaml_stream(docs: &[Value]) -> String {
    docs.iter().fold(String::new(), |mut s, d| {
        s.push_str("---\n");
        s.push_str(&to_yaml(d));
        s
    })
}

fn pad(out: &mut String, n: usize) {
    for _ in 0..n {
        out.push(' ');
    }
}

fn is_block(v: &Value) -> bool {
    match v {
        Value::Object(m) => !m.is_empty(),
        Value::Array(a) => !a.is_empty(),
        _ => false,
    }
}

/// Keys in sorted order, whatever order the map keeps (`serde_json`'s
/// `preserve_order` may be on in some builds): the output is the same in
/// every build.
fn sorted(m: &serde_json::Map<String, Value>) -> Vec<(&String, &Value)> {
    let mut v: Vec<_> = m.iter().collect();
    v.sort_by(|a, b| a.0.cmp(b.0));
    v
}

fn write_map(out: &mut String, m: &serde_json::Map<String, Value>, indent: usize) {
    for (k, v) in sorted(m) {
        pad(out, indent);
        out.push_str(&string(k));
        out.push(':');
        write_value_after_key(out, v, indent);
    }
}

fn write_value_after_key(out: &mut String, v: &Value, indent: usize) {
    match v {
        Value::Object(m) if !m.is_empty() => {
            out.push('\n');
            write_map(out, m, indent + 2);
        }
        Value::Array(a) if !a.is_empty() => {
            out.push('\n');
            write_seq(out, a, indent);
        }
        Value::String(s) if s.contains('\n') => {
            // A literal block keeps multi-line text (myCnf) readable.
            out.push_str(" |");
            if !s.ends_with('\n') {
                out.push('-');
            }
            out.push('\n');
            for line in s.trim_end_matches('\n').split('\n') {
                if !line.is_empty() {
                    pad(out, indent + 2);
                    out.push_str(line);
                }
                out.push('\n');
            }
        }
        other => {
            out.push(' ');
            out.push_str(&scalar(other));
            out.push('\n');
        }
    }
}

fn write_seq(out: &mut String, a: &[Value], indent: usize) {
    for item in a {
        pad(out, indent);
        out.push('-');
        match item {
            Value::Object(m) if !m.is_empty() => {
                // The first key goes on the dash's line.
                let mut first = true;
                for (k, v) in sorted(m) {
                    if first {
                        out.push(' ');
                        first = false;
                    } else {
                        pad(out, indent + 2);
                    }
                    out.push_str(&string(k));
                    out.push(':');
                    write_value_after_key(out, v, indent + 2);
                }
            }
            Value::Array(inner) if !inner.is_empty() => {
                out.push('\n');
                write_seq(out, inner, indent + 2);
            }
            other => {
                debug_assert!(!is_block(other));
                out.push(' ');
                out.push_str(&scalar(other));
                out.push('\n');
            }
        }
    }
}

fn scalar(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => string(s),
        Value::Object(_) => "{}".into(),
        Value::Array(_) => "[]".into(),
    }
}

/// Words YAML 1.1 readers (Helm's) take for booleans or null.
const RESERVED: &[&str] = &[
    "y", "n", "yes", "no", "on", "off", "true", "false", "null", "~",
];

fn string(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '/')
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
        && !RESERVED.contains(&s.to_ascii_lowercase().as_str());
    if plain {
        s.to_owned()
    } else {
        serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn writes_block_yaml() {
        let v = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "a", "labels": {"on": "yes"}},
            "spec": {"ports": [{"port": 3306, "name": "mysql"}], "empty": [], "flags": ["x", "1.0"]},
            "cnf": "[mariadbd]\nlog_bin = mariadb-bin\n",
        });
        let y = to_yaml(&v);
        assert_eq!(
            y,
            "apiVersion: v1\ncnf: |\n  [mariadbd]\n  log_bin = mariadb-bin\nkind: Service\n\
             metadata:\n  labels:\n    \"on\": \"yes\"\n  name: a\n\
             spec:\n  empty: []\n  flags:\n  - x\n  - \"1.0\"\n  ports:\n  - name: mysql\n    port: 3306\n"
        );
    }
}
