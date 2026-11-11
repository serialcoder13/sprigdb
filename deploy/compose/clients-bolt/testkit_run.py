"""Runs the Neo4j drivers' testkit integration tests (tests.neo4j, the same
discovery as testkit's own tests/neo4j/suites.py) against one server through
a running driver backend, and writes one line per test:
`<test id>\t<pass|fail|skip>\t<first line of the reason>`.

    python testkit_run.py <testkit-dir> <out.tsv>

The server comes from testkit's TEST_NEO4J_* variables and the backend from
TEST_BACKEND_HOST/PORT (default 127.0.0.1:9876).
"""

import os
import sys
import unittest


class Recorder(unittest.TextTestResult):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.rows = {}

    def _row(self, test, status, text=""):
        line = next((l.strip() for l in (text or "").splitlines()[::-1] if l.strip()), "")
        self.rows[test.id()] = (status, line[:240].replace("\t", " "))

    def addSuccess(self, test):
        super().addSuccess(test)
        self._row(test, "pass")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._row(test, "fail", self._exc_info_to_string(err, test))

    def addError(self, test, err):
        super().addError(test, err)
        self._row(test, "fail", self._exc_info_to_string(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._row(test, "skip", reason)

    def addExpectedFailure(self, test, err):
        super().addExpectedFailure(test, err)
        self._row(test, "pass")

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self._row(test, "fail", "unexpected success")


def main():
    tk, out = os.path.abspath(sys.argv[1]), sys.argv[2]
    sys.path.insert(0, tk)
    os.chdir(tk)
    suite = unittest.TestLoader().discover("tests.neo4j", top_level_dir=tk)
    runner = unittest.TextTestRunner(resultclass=Recorder, verbosity=1, stream=sys.stderr)
    result = runner.run(suite)
    with open(out, "w", encoding="utf-8") as f:
        for tid, (status, msg) in sorted(result.rows.items()):
            f.write(f"{tid}\t{status}\t{msg}\n")
    print(f"testkit: {len(result.rows)} tests, {len(result.failures) + len(result.errors)} failed, "
          f"{len(result.skipped)} skipped")


if __name__ == "__main__":
    main()
