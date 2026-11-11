"""Summarizes one upstream driver test suite run and compares it with
expected/<suite>.txt.

    summarize.py <suite> <target> <format> <results-dir> <expected-file>

<format> is gojson (go test -json), junit (JUnit XML files anywhere under
<results-dir>/raw: pytest, Maven Surefire, Gradle), poku (the mysql2
runner's output, one result per test file) or mtr (mysql-test-run.pl's
log, raw/mtr.log, one result per test case).

With SPRIG_CLIENTS_BASELINE=<tests.tsv> (CI's heavy-suite jobs, which run
each suite on a reference server first, in the same job), a test the
expected file doesn't name and that didn't pass on the reference isn't
counted against the target: it is reported as "fails on the reference
too". The expected file still wins where it names a test.

Writes <results-dir>/tests.tsv (test, status, first line of the failure) and
<results-dir>/summary.txt, prints the counts and every difference from the
expected file, and exits 1 when a test fails unexpectedly or an expected
failure passes. Skips are compared too but only reported.

The expected file has one line per outcome other than pass:

    <target-glob> <fail|skip|flaky> <test-glob> :: <reason>

<target-glob> matches the target label (baseline-mariadb, baseline-mysql,
gateway-mariadb, gateway-mysql); <test-glob> matches the test id. The first
matching line wins. `flaky` accepts any outcome. `exclude` lines name tests
run.sh doesn't run at all on that target (`summarize.py excludes <expected>
<target>` prints them); they count as skipped if they show up.
"""

import fnmatch
import json
import os
import re
import sys
import xml.etree.ElementTree as ET


def first_line(text):
    for line in (text or "").splitlines():
        line = line.strip()
        if line:
            return line[:240]
    return ""


def parse_gojson(raw):
    tests, output = {}, {}
    path = os.path.join(raw, "go-test.json")
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            name = ev.get("Test")
            if not name:
                continue
            action = ev.get("Action")
            if action == "output":
                output.setdefault(name, []).append(ev.get("Output", ""))
            elif action in ("pass", "fail", "skip"):
                tests[name] = action
    # A test whose subtests all report on their own is counted once per
    # subtest: drop parents that have children.
    parents = {n.rsplit("/", 1)[0] for n in tests if "/" in n}
    result = {}
    for name, status in tests.items():
        if name in parents and status != "fail":
            continue
        if name in parents and status == "fail":
            # Keep a failing parent only when no child failed (a failure in
            # the parent's own body).
            if any(
                tests[c] == "fail" for c in tests if c.startswith(name + "/")
            ):
                continue
        msg = ""
        if status != "pass":
            lines = [
                l.strip()
                for l in output.get(name, [])
                if l.strip() and not l.lstrip().startswith(("=== ", "--- "))
            ]
            best = [l for l in lines if "_test.go:" in l and not l.startswith("[mysql]")]
            msg = (best or lines or [""])[0][:240]
        result[name] = (status, msg)
    return result


def parse_junit(raw):
    result = {}
    for root, _, files in os.walk(raw):
        for fn in sorted(files):
            if not fn.endswith(".xml"):
                continue
            try:
                tree = ET.parse(os.path.join(root, fn))
            except ET.ParseError:
                continue
            for case in tree.iter("testcase"):
                cls = case.get("classname") or ""
                name = case.get("name") or ""
                tid = f"{cls}.{name}" if cls else name
                status, msg = "pass", ""
                for child in case:
                    if child.tag in ("failure", "error"):
                        status = "fail"
                        msg = first_line(child.get("message")) or first_line(child.text)
                        break
                    if child.tag == "skipped":
                        status = "skip"
                        msg = first_line(child.get("message")) or first_line(child.text)
                result[tid] = (status, msg)
    return result


ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def parse_poku(raw):
    """poku's compact reporter prints one line per test file:
    ` PASS  <path>` or ` FAIL  <path>`, then the failure's details."""
    result = {}
    path = os.path.join(raw, "poku.log")
    with open(path, encoding="utf-8", errors="replace") as f:
        lines = [ANSI.sub("", l.rstrip("\n")) for l in f]
    for i, line in enumerate(lines):
        m = re.match(r"^\s*(PASS|FAIL|SKIP|✔|✘|✗|◯|⏭)\s+(\S+\.[cm]?[jt]s)\s*$", line)
        if not m:
            continue
        mark, name = m.groups()
        status = {"PASS": "pass", "✔": "pass", "FAIL": "fail", "✘": "fail", "✗": "fail"}.get(mark, "skip")
        msg = ""
        if status == "fail":
            for nxt in lines[i + 1 : i + 40]:
                s = nxt.strip()
                if s and ("Error" in s or "assert" in s.lower() or "expected" in s.lower()):
                    msg = s[:240]
                    break
        result[name] = (status, msg)
    return result


MTR_LINE = re.compile(
    r"^\[\s*\d+%\]\s+(\S+)(?:\s+'([^']*)')?\s+\[\s*(pass|fail|skipped|disabled)\s*\]\s*(.*)$"
)


def parse_mtr(raw):
    """mysql-test-run.pl prints `[ 12%] main.group_by  [ pass ]  1234` per
    test case (with a combination in quotes when the suite has several).
    A failure's reason is the first `mysqltest: ...` or diff line after it."""
    result = {}
    path = os.path.join(raw, "mtr.log")
    with open(path, encoding="utf-8", errors="replace") as f:
        lines = [ANSI.sub("", l.rstrip("\n")) for l in f]
    for i, line in enumerate(lines):
        m = MTR_LINE.match(line.strip())
        if not m:
            continue
        name, combo, mark, tail = m.groups()
        tid = f"{name}:{combo}" if combo else name
        status = {"pass": "pass", "fail": "fail"}.get(mark, "skip")
        msg = ""
        if status == "fail":
            for nxt in lines[i + 1 : i + 60]:
                s = nxt.strip()
                if s.startswith("mysqltest:") or s.startswith(("+", "-")) and not s.startswith(("+++", "---")):
                    msg = s[:240]
                    break
        elif status == "skip":
            msg = tail.strip()[:240]
        # A retried test reports again: the last report wins.
        result[tid] = (status, msg)
    return result


def load_baseline(path):
    """tests.tsv of the same suite on the reference: test -> status."""
    base = {}
    if not path:
        return base
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                base[parts[0]] = parts[1]
    return base


def load_expected(path, target):
    rules = []
    if not os.path.exists(path):
        return rules
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            head, _, reason = line.partition(" :: ")
            parts = head.split(None, 2)
            if len(parts) != 3 or parts[1] not in ("fail", "skip", "flaky", "exclude"):
                sys.exit(f"{path}:{n}: bad line: {line}")
            tglob, status, test = parts
            if not reason.strip():
                sys.exit(f"{path}:{n}: every line needs a reason after ' :: '")
            if fnmatch.fnmatchcase(target, tglob):
                rules.append((test.strip(), status, reason.strip(), n))
    return rules


def expected_for(rules, test):
    for pattern, status, reason, n in rules:
        if fnmatch.fnmatchcase(test, pattern):
            return status, reason, n
    return "pass", "", 0


def main():
    if sys.argv[1] == "excludes":
        for test, status, _, _ in load_expected(sys.argv[2], sys.argv[3]):
            if status == "exclude":
                print(test)
        return
    suite, target, fmt, results, expected = sys.argv[1:6]
    raw = os.path.join(results, "raw")
    parser = {"gojson": parse_gojson, "junit": parse_junit, "poku": parse_poku, "mtr": parse_mtr}[fmt]
    tests = parser(raw)
    rules = load_expected(expected, target)
    baseline = load_baseline(os.environ.get("SPRIG_CLIENTS_BASELINE", ""))
    on_reference = 0

    counts = {"pass": 0, "fail": 0, "skip": 0}
    bad, info = [], []
    used = set()
    for name in sorted(tests):
        status, msg = tests[name]
        counts[status] += 1
        want, reason, n = expected_for(rules, name)
        if n:
            used.add(n)
        if want == "flaky":
            continue
        if want == "exclude":
            want = "skip"
        if not n and status != "pass" and name in baseline and baseline[name] != "pass":
            # Not ours: the reference fails or skips it too (or ran it
            # with another outcome), in the same job.
            on_reference += 1
            if status == "fail":
                info.append(f"fails on the reference too ({baseline[name]})  {name}: {msg}")
            continue
        if status == "fail" and want != "fail":
            bad.append(f"UNEXPECTED FAIL  {name}: {msg}")
        elif want == "fail" and status != "fail":
            bad.append(f"UNEXPECTED {status.upper():5}  {name} (expected fail: {reason})")
        elif status == "skip" and want != "skip":
            info.append(f"skip  {name}: {msg}")
        elif want == "skip" and status != "skip":
            info.append(f"ran (expected skip)  {name}")

    with open(os.path.join(results, "tests.tsv"), "w", encoding="utf-8") as f:
        for name in sorted(tests):
            status, msg = tests[name]
            f.write(f"{name}\t{status}\t{msg}\n")
    total = sum(counts.values())
    head = (
        f"{suite} {target}: {total} tests, {counts['pass']} pass, "
        f"{counts['fail']} fail, {counts['skip']} skip; "
        f"{len(bad)} unexpected"
    )
    if baseline:
        head += f"; {on_reference} not passing on the reference either"
    with open(os.path.join(results, "summary.txt"), "w", encoding="utf-8") as f:
        f.write(head + "\n")
        for line in bad + info:
            f.write(line + "\n")
    print(head)
    for line in bad:
        print("  " + line)
    if os.environ.get("SPRIG_CLIENTS_VERBOSE") == "1":
        for line in info:
            print("  " + line)
    if total == 0:
        print("  no test results found", file=sys.stderr)
        sys.exit(1)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
