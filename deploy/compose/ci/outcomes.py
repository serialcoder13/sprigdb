"""Compares a client suite's outcomes on a reference server with its
outcomes through SprigDB, for CI's heavy-suite jobs
(.github/workflows/heavy-protocols.yml).

    outcomes.py scenarios <ref.json> <sut.json> <expected> <target> <out-dir>
    outcomes.py tests <format> <ref-raw> <sut-raw> <expected> <target> <out-dir>

`scenarios`: each file holds one JSON object, one key per scenario with what
the client observed (a scenario harness such as clients-amqp/amqp_diff.py).
A scenario whose observation differs between the two is a difference. Keys
ending in `_seconds`, `_s` or `_rate` are timings: kept in the report,
never compared.

`tests`: a test suite's own results, <format> one of gojson (go test -json
files), junit (JUnit XML files) or tsv (lines `<test>\t<pass|fail|skip>
\t<message>`), found anywhere under each raw directory. A test that passes on
the reference and doesn't pass through SprigDB is a difference; a test that
fails on the reference too is not SprigDB's and is only reported.

The expected file lists known differences, one per line:

    <target-glob> <fail|skip|flaky|exclude> <test-glob> :: <reason>

(the format of deploy/compose/clients/expected/). `fail` and `skip` accept
that difference, `flaky` accepts any outcome, `exclude` names a test the
harness doesn't run. A listed `fail` that now matches the reference is
reported as stale, so the file shrinks as SprigDB catches up.

Writes <out-dir>/outcomes.tsv and <out-dir>/summary.txt, prints the summary,
and exits 1 on any unexpected difference or stale line, or when either side
produced no results.
"""

import fnmatch
import json
import os
import sys
import xml.etree.ElementTree as ET

TIMING = ("_seconds", "_s", "_rate")


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
            if not reason.strip():
                sys.exit(f"{path}:{n}: every line needs a reason after ' :: '")
            if fnmatch.fnmatchcase(target, parts[0]):
                rules.append((parts[2].strip(), parts[1], reason.strip()))
    return rules


def expected_for(rules, test):
    for pattern, status, reason in rules:
        if fnmatch.fnmatchcase(test, pattern):
            return status, reason
    return None, ""


def first_line(text):
    for line in (text or "").splitlines():
        if line.strip():
            return line.strip()[:240]
    return ""


def files_under(raw, suffix):
    for root, _, files in os.walk(raw):
        for fn in sorted(files):
            if fn.endswith(suffix):
                yield os.path.join(root, fn)


def parse_gojson(raw):
    tests, output = {}, {}
    for path in files_under(raw, ".json"):
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                if not line.startswith("{"):
                    continue
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                name = ev.get("Test")
                if not name:
                    continue
                name = f"{ev.get('Package', '')}.{name}".lstrip(".")
                if ev.get("Action") == "output":
                    output.setdefault(name, []).append(ev.get("Output", ""))
                elif ev.get("Action") in ("pass", "fail", "skip"):
                    tests[name] = ev["Action"]
    return {
        n: (s, "" if s == "pass" else first_line("".join(o for o in output.get(n, []) if "_test.go:" in o)))
        for n, s in tests.items()
    }


def parse_junit(raw):
    result = {}
    for path in files_under(raw, ".xml"):
        try:
            tree = ET.parse(path)
        except ET.ParseError:
            continue
        for case in tree.iter("testcase"):
            cls, name = case.get("classname") or "", case.get("name") or ""
            tid = f"{cls}.{name}" if cls else name
            status, msg = "pass", ""
            for child in case:
                if child.tag in ("failure", "error"):
                    status, msg = "fail", first_line(child.get("message")) or first_line(child.text)
                    break
                if child.tag == "skipped":
                    status, msg = "skip", first_line(child.get("message"))
            result[tid] = (status, msg)
    return result


def parse_tsv(raw):
    result = {}
    for path in files_under(raw, ".tsv"):
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 2 and parts[1] in ("pass", "fail", "skip"):
                    result[parts[0]] = (parts[1], parts[2] if len(parts) > 2 else "")
    return result


def report(target, rows, bad, info, out):
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "outcomes.tsv"), "w", encoding="utf-8") as f:
        for row in rows:
            f.write("\t".join(row) + "\n")
    counts = {}
    for row in rows:
        counts[row[1]] = counts.get(row[1], 0) + 1
    head = f"{target}: {len(rows)} compared, " + ", ".join(
        f"{v} {k}" for k, v in sorted(counts.items())) + f"; {len(bad)} unexpected"
    with open(os.path.join(out, "summary.txt"), "w", encoding="utf-8") as f:
        f.write(head + "\n")
        for line in bad + info:
            f.write(line + "\n")
    print(head)
    for line in bad:
        print("  " + line)
    if os.environ.get("SPRIG_CI_VERBOSE") == "1":
        for line in info:
            print("  " + line)
    return 1 if bad else 0


def judge(name, differs, detail, rules, bad, info):
    want, reason = expected_for(rules, name)
    if want == "flaky":
        return "flaky"
    if differs:
        if want in ("fail", "skip", "exclude"):
            info.append(f"known  {name}: {reason}")
            return "known"
        bad.append(f"UNEXPECTED  {name}: {detail}")
        return "differs"
    if want == "fail":
        bad.append(f"STALE  {name} matches the reference now (listed: {reason})")
        return "stale"
    return "same"


def scenarios(ref_path, sut_path, expected, target, out):
    with open(ref_path, encoding="utf-8") as f:
        ref = json.load(f)
    with open(sut_path, encoding="utf-8") as f:
        sut = json.load(f)
    rules = load_expected(expected, target)
    rows, bad, info = [], [], []
    for name in sorted(set(ref) | set(sut)):
        r, s = ref.get(name, "<missing>"), sut.get(name, "<missing>")
        if name.endswith(TIMING):
            info.append(f"timing  {name}: reference {r}, SprigDB {s}")
            rows.append((name, "timing", json.dumps(r), json.dumps(s)))
            continue
        differs = r != s
        verdict = judge(name, differs, f"reference {json.dumps(r)}, SprigDB {json.dumps(s)}", rules, bad, info)
        rows.append((name, verdict, json.dumps(r), json.dumps(s)))
    if not ref or not sut:
        bad.append("no scenarios on " + ("the reference" if not ref else "SprigDB"))
    return report(target, rows, bad, info, out)


def tests(fmt, ref_raw, sut_raw, expected, target, out):
    parser = {"gojson": parse_gojson, "junit": parse_junit, "tsv": parse_tsv}[fmt]
    ref, sut = parser(ref_raw), parser(sut_raw)
    rules = load_expected(expected, target)
    rows, bad, info = [], [], []
    for name in sorted(set(ref) | set(sut)):
        rs, _ = ref.get(name, ("missing", ""))
        ss, msg = sut.get(name, ("missing", ""))
        if rs != "pass":
            info.append(f"reference {rs}  {name} (SprigDB: {ss})")
            rows.append((name, f"ref-{rs}", rs, ss))
            continue
        verdict = judge(name, ss != "pass", f"{ss}: {msg}", rules, bad, info)
        rows.append((name, verdict, rs, ss))
    if not ref or not sut:
        bad.append("no test results on " + ("the reference" if not ref else "SprigDB"))
    return report(target, rows, bad, info, out)


def main():
    if len(sys.argv) == 7 and sys.argv[1] == "scenarios":
        sys.exit(scenarios(*sys.argv[2:7]))
    if len(sys.argv) == 8 and sys.argv[1] == "tests":
        sys.exit(tests(*sys.argv[2:8]))
    sys.exit(__doc__)


if __name__ == "__main__":
    main()
