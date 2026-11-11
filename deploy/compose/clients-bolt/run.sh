#!/usr/bin/env bash
# The Neo4j drivers' testkit (neo4j-drivers/testkit, Apache-2.0) against a
# Neo4j reference and SprigDB's Bolt listener (chapter 43, "Testing"), for
# CI's heavy-suite jobs (.github/workflows/heavy-protocols.yml). Runs on the
# CI runner itself: the driver's own testkit glue (testkit/build.py, then
# testkit/backend.py on 127.0.0.1:9876) builds and serves the driver, and
# testkit_run.py runs testkit's integration tests (tests.neo4j) through it,
# on the reference first, then on SprigDB; deploy/compose/ci/outcomes.py
# compares the two against expected/testkit-<driver>.txt.
#
#   run.sh python | go | java
#
# Pinned: testkit commit 75f4aa8 and the Python driver commit 9c876c1 (both
# from r8four's download, decisions/r8-interim.md G46; testkit's branch 6.x);
# the Go driver tag v6.4.0 (go.mod: Go 1.24; run on a laptop by r8testkit,
# 11 November 2026, decisions/r8-interim.md G66) and the Java driver tag
# 6.0.0 (not run yet, G68). Needs on the runner: git, python3 with venv,
# and Go (go) or a JDK with Maven (java).
#
#   SPRIG_BOLT_REF   reference host:port (127.0.0.1:7687), neo4j / SPRIG_BOLT_REF_PASSWORD
#   SPRIG_BOLT_SUT   SprigDB host:port (127.0.0.1:27687, deploy/compose/ci/listeners.sh),
#                    SPRIG_BOLT_USER / SPRIG_BOLT_PASSWORD (acme.app / sprig-dev-app);
#                    the tenant's database `graph` must exist (bolt.logical_database)
#   SPRIG_CI_RESULTS results root (default target/ci-results)
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
DRIVER=${1:-}
TESTKIT_SHA=75f4aa8b227a34e76a62d0ceee0fa3bea50cf38d
case $DRIVER in
python) REPO=https://github.com/neo4j/neo4j-python-driver.git REF=9c876c1fe5d915cb8704de65cb60e78194c950da ;;
go) REPO=https://github.com/neo4j/neo4j-go-driver.git REF=v6.4.0 ;;
java) REPO=https://github.com/neo4j/neo4j-java-driver.git REF=6.0.0 ;;
*) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2 ;;
esac
OUT=${SPRIG_CI_RESULTS:-$ROOT/target/ci-results}/bolt-$DRIVER
W=$OUT/work

rm -rf "$OUT"
mkdir -p "$OUT/ref" "$OUT/sut" "$W"

# fetch <repo> <commit or tag> <dir>
fetch() {
    git init -q "$3"
    git -C "$3" fetch -q --depth 1 "$1" "$2"
    git -C "$3" -c advice.detachedHead=false checkout -q FETCH_HEAD
}
fetch https://github.com/neo4j-drivers/testkit.git "$TESTKIT_SHA" "$W/testkit"
fetch "$REPO" "$REF" "$W/driver"

python3 -m venv "$W/venv"
# shellcheck disable=SC1091 # created just above
source "$W/venv/bin/activate"
pip install -q "ifaddr==0.2.0" "lark==1.2.2" "pytz==2025.2" "typing_extensions==4.15.0" >"$OUT/pip.log" 2>&1

# The driver's own glue builds it and serves the testkit protocol.
(cd "$W/driver" && TEST_BACKEND_VERSION=python python testkit/build.py) >"$OUT/build.log" 2>&1
(cd "$W/driver" && TEST_BACKEND_VERSION=python exec python testkit/backend.py) >"$OUT/backend.log" 2>&1 &
backend=$!
trap 'kill "$backend" 2>/dev/null || true' EXIT
timeout 300 bash -c 'until (echo > /dev/tcp/127.0.0.1/9876) 2>/dev/null; do sleep 2; done' ||
    { tail -n 50 "$OUT/backend.log" >&2; echo "testkit backend didn't start" >&2; exit 1; }

for side in ref sut; do
    if [[ $side == ref ]]; then
        hp=${SPRIG_BOLT_REF:-127.0.0.1:7687} user=neo4j pass=${SPRIG_BOLT_REF_PASSWORD:-sprig-dev-pass}
    else
        hp=${SPRIG_BOLT_SUT:-127.0.0.1:27687} user=${SPRIG_BOLT_USER:-acme.app} pass=${SPRIG_BOLT_PASSWORD:-sprig-dev-app}
    fi
    TEST_DRIVER_NAME=$DRIVER TEST_NEO4J_HOST=${hp%:*} TEST_NEO4J_PORT=${hp#*:} TEST_NEO4J_USER=$user TEST_NEO4J_PASS=$pass \
        TEST_NEO4J_SCHEME=bolt TEST_NEO4J_VERSION=5.26 TEST_NEO4J_EDITION=community \
        TEST_NEO4J_CLUSTER=False TEST_NEO4J_DEFAULT_DB=neo4j \
        python "$HERE/testkit_run.py" "$W/testkit" "$OUT/$side/tests.tsv" 2>"$OUT/$side/unittest.log" || true
done

python3 -I "$ROOT/deploy/compose/ci/outcomes.py" tests tsv "$OUT/ref" "$OUT/sut" \
    "$HERE/expected/testkit-$DRIVER.txt" sprig "$OUT"
