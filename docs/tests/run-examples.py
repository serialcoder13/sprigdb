#!/usr/bin/env python3
"""Runs the documentation's tested examples against a real gateway.

A code block takes part when its info string has the word `test`:

    ```sql test
    CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(20));
    INSERT INTO orders VALUES (1, 'paid');
    SELECT * FROM orders;
    ```

    ```text output
    id	status
    1	paid
    ```

- `sql test` blocks run in order, page by page, through the `mysql` client,
  in a fresh database per page (`docs_<page>`), dropped at the end.
- A `text output` block right after a `sql test` block is compared with what
  the block printed (tab-separated, with headers, as `mysql --batch` prints).
- `sh test` blocks run with bash from the repository root, with SPRIG_HOST,
  SPRIG_PORT, SPRIG_USER and SPRIG_PASSWORD set.
- `sql test-error` blocks must fail.

Environment: SPRIG_HOST (127.0.0.1), SPRIG_PORT (13306), SPRIG_USER
(acme.app), SPRIG_PASSWORD (sprig-dev-app), SPRIG_SSL_MODE (DISABLED).
Usage: docs/tests/run-examples.py [page.md ...]   (default: every page)
Exit 1 if any example fails; each failure names the page and line.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent
ROOT = DOCS.parent
FENCE = re.compile(r"^```(\w+)([^\n]*)\n(.*?)^```", re.S | re.M)

HOST = os.environ.get("SPRIG_HOST", "127.0.0.1")
PORT = os.environ.get("SPRIG_PORT", "13306")
USER = os.environ.get("SPRIG_USER", "acme.app")
PASSWORD = os.environ.get("SPRIG_PASSWORD", "sprig-dev-app")
SSL = os.environ.get("SPRIG_SSL_MODE", "DISABLED")


def mysql(sql, database=None):
    cmd = [
        "mysql", "-h", HOST, "-P", PORT, "-u", USER, f"-p{PASSWORD}",
        f"--ssl-mode={SSL}", "--batch",
    ]
    if database:
        cmd += ["-D", database]
    p = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    err = "\n".join(l for l in p.stderr.splitlines() if "password on the command line" not in l)
    return p.returncode, p.stdout, err


def blocks(path):
    text = path.read_text()
    for m in FENCE.finditer(text):
        lang, info, body = m.group(1), m.group(2).split(), m.group(3)
        line = text.count("\n", 0, m.start()) + 1
        yield lang, info, body, line


def run_page(path):
    failures = []
    found = [b for b in blocks(path) if any(w.startswith("test") or w == "output" for w in b[1])]
    if not found:
        return 0, failures
    db = "docs_" + re.sub(r"[^a-z0-9]+", "_", path.stem.lower())
    mysql(f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db};")
    last_out = None
    ran = 0
    for lang, info, body, line in found:
        where = f"{path.relative_to(DOCS) if path.is_relative_to(DOCS) else path}:{line}"
        if lang == "sql" and ("test" in info or "test-error" in info):
            ran += 1
            code, out, err = mysql(body, db)
            if "test-error" in info:
                if code == 0:
                    failures.append(f"{where}: expected an error, got none")
            elif code != 0:
                failures.append(f"{where}: {err.strip()}")
            last_out = out
        elif lang == "sh" and "test" in info:
            ran += 1
            env = dict(os.environ, SPRIG_HOST=HOST, SPRIG_PORT=PORT, SPRIG_USER=USER,
                       SPRIG_PASSWORD=PASSWORD)
            p = subprocess.run(["bash", "-euo", "pipefail", "-c", body], cwd=ROOT, env=env,
                               capture_output=True, text=True)
            if p.returncode != 0:
                failures.append(f"{where}: exit {p.returncode}: {p.stderr.strip()[-400:]}")
            last_out = p.stdout
        elif lang == "text" and "output" in info and last_out is not None:
            want = [l.rstrip() for l in body.strip("\n").splitlines()]
            have = [l.rstrip() for l in last_out.strip("\n").splitlines()]
            if want != have:
                failures.append(f"{where}: output differs\n  want: {want}\n  have: {have}")
            last_out = None
    mysql(f"DROP DATABASE IF EXISTS {db};")
    return ran, failures


def main():
    pages = [Path(p).resolve() for p in sys.argv[1:]] or sorted(
        p for p in DOCS.rglob("*.md") if p.relative_to(DOCS).parts[0] not in ("pdf", "tests")
    )
    total, failed = 0, []
    for p in pages:
        ran, f = run_page(p)
        total += ran
        failed += f
    for f in failed:
        print("FAIL", f)
    print(f"docs examples: {total} run, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
