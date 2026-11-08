"""Compares connector results with expected.toml and prints the matrix.

Usage: python3 -I summarize.py <results-dir> <expected.toml>[,<overlay.toml>...] <persona> <connector>...

Later files override earlier ones key by key (run.sh passes
SPRIG_CONN_EXPECTED_OVERLAY as an overlay, for target-specific outcomes).

Each connector must report the checks of its check set: [check_sets] in
expected.toml names the list per connector, "checks" by default. Checks in
SPRIG_CONN_SKIP_CHECKS (run.sh --quick) are neither required nor compared.

Reads <results-dir>/<connector>.<tls>.jsonl (one JSON object per check, as
the connector programs print them), prints a table per connector and TLS
mode, lists every mismatch with its detail, writes summary.json and
junit.xml into <results-dir>, and exits 1 on any mismatch. Runs inside the
sprigdb-conn-python image (Python 3.12, tomllib), so the host needs only
Docker.
"""

import json
import os
import sys
import tomllib
from xml.sax.saxutils import escape, quoteattr

SHORT = {
    "connect": "conn", "plain_query": "query", "types": "types", "prepared": "prep",
    "transaction": "tx", "multi_statement": "multi", "duplicate_key": "dup",
    "last_insert_id": "lastid", "large_row": "4MiB", "multi_packet": "16MiB+",
    "compression": "zip", "conn_attrs": "attrs", "prepared_biginteger": "prep-ubig",
    "prepare": "prepare", "point_select_ps": "point-ps", "point_select_text": "point-txt",
    "read_write_ps": "rw-ps", "read_write_text": "rw-txt", "cleanup": "cleanup",
    "local_infile": "infile", "local_infile_off": "infile-off", "call": "call",
    "change_user": "chuser", "reset_connection": "reset", "pool_reprepare": "pool-ps",
    "lock_wait": "lock1205", "lock_nowait": "nowait",
    "schema_validate": "validate", "pool_reconnect": "pool-reconn",
}
CELL = {"pass": "ok", "n/a": "n/a", "skip": "skip", "fail": "FAIL", None: "MISSING"}


def expected_status(exp, persona, connector, tls, check):
    for scope, name in ((persona, connector), ("all", connector), (persona, "*"), ("all", "*")):
        table = exp.get(scope, {}).get(name)
        if not isinstance(table, dict):
            continue
        sub = table.get(tls)
        if isinstance(sub, dict):
            for key in (check, "*"):
                if key in sub:
                    return sub[key]
        for key in (check, "*"):
            if isinstance(table.get(key), str):
                return table[key]
    return "pass"


def merge(base, over):
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            merge(base[k], v)
        else:
            base[k] = v
    return base


def load(path):
    results = {}
    if not os.path.exists(path):
        return results
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            results[rec["check"]] = rec
    return results


def main():
    results_dir, expected_paths, persona, *connectors = sys.argv[1:]
    exp = {}
    for path in expected_paths.split(","):
        with open(path, "rb") as f:
            merge(exp, tomllib.load(f))
    tls_modes = os.environ.get("SPRIG_CONN_TLS_MODES", "required disabled").split()
    skipped = set(os.environ.get("SPRIG_CONN_SKIP_CHECKS", "").replace(",", " ").split())
    rows, mismatches, perf, cases = [], [], [], []
    for connector in connectors:
        check_set = exp.get("check_sets", {}).get(connector, "checks")
        base = [c for c in exp[check_set] if c not in skipped]
        for tls in tls_modes:
            got = load(os.path.join(results_dir, "%s.%s.jsonl" % (connector, tls)))
            got = {k: v for k, v in got.items() if k not in skipped}
            checks = list(base) + [c for c in got if c not in base]
            cells = []
            for check in checks:
                rec = got.get(check)
                actual = rec["status"] if rec else None
                want = expected_status(exp, persona, connector, tls, check)
                ok = actual == want
                cell = CELL.get(actual, actual)
                if ok and actual == "fail":
                    cell = "xfail"  # a known failure of the reference server
                if not ok:
                    cell = "XPASS" if actual == "pass" else cell + "!"
                    mismatches.append((connector, tls, check, want, actual, rec["detail"] if rec else
                                       "not reported (see %s.%s.log)" % (connector, tls)))
                cells.append((check, cell))
                cases.append((connector, tls, check, want, actual, rec, ok))
                if connector.startswith("sysbench") and rec and actual == "pass" and "tps=" in rec.get("detail", ""):
                    perf.append(("%s/%s" % (connector, tls), check, rec["detail"]))
            rows.append(("%s/%s" % (connector, tls), cells, check_set))

    # One table per check set, in the order they first appear.
    layouts = []
    for r in rows:
        if r[2] not in layouts:
            layouts.append(r[2])
    for layout_key in layouts:
        group = [r[:2] for r in rows if r[2] == layout_key]
        headers = []
        for _, cells in group:
            for check, _ in cells:
                if check not in headers:
                    headers.append(check)
        width = max(len(r[0]) for r in group)
        cols = [max(len(SHORT.get(h, h)), 4) for h in headers]
        print("%-*s  %s" % (width, "connector/tls", "  ".join("%-*s" % (w, SHORT.get(h, h)) for h, w in zip(headers, cols))))
        for name, cells in group:
            d = dict(cells)
            print("%-*s  %s" % (width, name, "  ".join("%-*s" % (w, d.get(h, "-")) for h, w in zip(headers, cols))))
        print()
    if perf:
        print("sysbench (%s):" % persona)
        for run, check, detail in perf:
            print("  %-22s %-18s %s" % (run, check, detail))
        print()

    total = len(cases)
    print("%d checks, %d as expected, %d mismatches (persona %s)" % (total, total - len(mismatches), len(mismatches), persona))
    for connector, tls, check, want, actual, detail in mismatches:
        print("  MISMATCH %s/%s %s: expected %s, got %s: %s" % (connector, tls, check, want, actual or "nothing", detail))

    with open(os.path.join(results_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump({"persona": persona, "checks": total, "mismatches": [
            {"connector": c, "tls": t, "check": k, "expected": w, "actual": a, "detail": d}
            for c, t, k, w, a, d in mismatches]}, f, indent=2, ensure_ascii=False)
    with open(os.path.join(results_dir, "junit.xml"), "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name=%s tests="%d" failures="%d">\n'
                % (quoteattr("connectors-" + persona), total, len(mismatches)))
        for connector, tls, check, want, actual, rec, ok in cases:
            ms = (rec or {}).get("ms", 0) or 0
            f.write('  <testcase classname=%s name=%s time="%.3f">' % (
                quoteattr("%s.%s" % (connector, tls)), quoteattr(check), ms / 1000))
            if not ok:
                f.write('<failure message=%s>%s</failure>' % (
                    quoteattr("expected %s, got %s" % (want, actual)), escape((rec or {}).get("detail", ""))))
            elif actual in ("n/a", "skip"):
                f.write("<skipped message=%s/>" % quoteattr((rec or {}).get("detail", "")))
            f.write("</testcase>\n")
        f.write("</testsuite>\n")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
