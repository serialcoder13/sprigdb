#!/usr/bin/env python3
"""Fails if an alert or recording rule reads a metric nothing exports.

    deploy/alerts/check-metrics.py

Every metric name in an `expr:` of deploy/alerts/*.rules.yaml must be one of:
- a `sprig_*` metric some crate emits (`counter!`, `gauge!` or `histogram!`
  with the name as a literal; a histogram's `_bucket`, `_count` and `_sum`
  series count as the histogram);
- a recording rule defined in these files (`record:`);
- a metric of a component SprigDB ships but doesn't build, listed in
  EXTERNAL with the component that exports it.

An alert on a metric nobody exports never fires, so it fails silently; this
check (run by deploy/alerts/test.sh, in CI's "alert rules and dashboards"
job) makes that a build failure instead. Standard library only.
"""

import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent

# Prefixes of metrics other components export, and who exports them.
EXTERNAL = {
    "etcd_": "etcd (scrape each member's /metrics)",
}
# Prometheus's own series.
BUILTIN = {"up", "scrape_duration_seconds", "scrape_samples_scraped"}

KEYWORDS = {
    "by", "without", "on", "ignoring", "group_left", "group_right", "and", "or",
    "unless", "bool", "offset", "inf", "nan", "atan2",
}


def emitted_metrics():
    """Every sprig_* name a crate emits as a literal."""
    names = set()
    pat = re.compile(r'(?:counter|gauge|histogram)!\(\s*"(sprig_[a-z0-9_]+)"')
    for f in (ROOT / "crates").rglob("*.rs"):
        names.update(pat.findall(f.read_text(errors="replace")))
    return names


def rules(path):
    """(kind, name, expr) for each rule of a rule file: kind is alert or record."""
    lines = path.read_text().split("\n")
    out = []
    kind = name = None
    i = 0
    while i < len(lines):
        line = lines[i]
        m = re.match(r"\s*-\s*(alert|record):\s*(\S+)", line)
        if m:
            kind, name = m.group(1), m.group(2)
        m = re.match(r"(\s*)expr:\s*(.*)$", line)
        if m:
            indent, rest = len(m.group(1)), m.group(2).strip()
            if rest in ("|", ">", "|-", ">-"):
                body = []
                i += 1
                while i < len(lines) and (not lines[i].strip() or len(lines[i]) - len(lines[i].lstrip()) > indent):
                    body.append(lines[i])
                    i += 1
                out.append((kind, name, "\n".join(body)))
                continue
            out.append((kind, name, rest.strip("'\"")))
        i += 1
    return out


def metric_names(expr):
    """The metric names a PromQL expression reads."""
    e = re.sub(r'"(?:\\.|[^"\\])*"', '""', expr)
    e = re.sub(r"'(?:\\.|[^'\\])*'", "''", e)
    e = re.sub(r"\{[^}]*\}", " ", e)  # label matchers
    e = re.sub(r"\[[^\]]*\]", " ", e)  # ranges and subqueries
    # label lists of by/without/on/ignoring/group_left/group_right
    e = re.sub(r"\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)", " ", e)
    names = set()
    for m in re.finditer(r"(?<![\w:.])([a-zA-Z_:][a-zA-Z0-9_:]*)\b(\s*\()?", e):
        tok, call = m.group(1), m.group(2)
        if call or tok.lower() in KEYWORDS:
            continue
        # durations such as 5m, 1h are digits first and never match; plain
        # numbers neither.
        names.add(tok)
    return names


def histogram_base(name, emitted):
    for suf in ("_bucket", "_count", "_sum"):
        if name.endswith(suf) and name[: -len(suf)] in emitted:
            return name[: -len(suf)]
    return name


def main():
    files = sorted(HERE.glob("*.rules.yaml"))
    all_rules = [(f, r) for f in files for r in rules(f)]
    recorded = {name for _, (kind, name, _) in all_rules if kind == "record"}
    emitted = emitted_metrics()
    failed = False
    checked = 0
    for f, (kind, name, expr) in all_rules:
        for m in sorted(metric_names(expr)):
            checked += 1
            if m in recorded or m in BUILTIN or histogram_base(m, emitted) in emitted:
                continue
            if any(m.startswith(p) for p in EXTERNAL):
                continue
            print(f"{f.relative_to(ROOT)}: {kind} {name} reads {m}, which nothing exports")
            failed = True
    if not failed:
        print(f"every metric the alert and recording rules read is exported "
              f"({checked} references in {len(all_rules)} rules)")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
