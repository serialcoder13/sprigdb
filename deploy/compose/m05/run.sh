#!/usr/bin/env bash
# Milestone M0.5's SQL isolation suite (chapter 29 step 6, chapter 46 step
# 4): crates/sprig-testkit/tests/m05_isolation.rs against the development
# gateway (gateway-dev.sh up: tenants acme and globex, admins acme.app and
# globex.app), with a second gateway (m05/gateway-2.toml, 13316/13317) for
# the revocation-propagation check, and MariaDB's grant tables audited
# through the single node's root.
#
#   deploy/compose/m05/run.sh
#
# Variables: SPRIG_M05_BIN (the directory holding sprigdb; default the one
# gateway-dev.sh up built), SPRIG_M05_PASSWORD (sprig-dev-app). Results:
# deploy/compose/m05/results/ (report.jsonl, gateway-2.log, suite.log).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
here=deploy/compose/m05
results=$here/results
mkdir -p "$results"
state=deploy/compose/.gateway-dev
bin=${SPRIG_M05_BIN:-$(cat "$state/bin" 2>/dev/null || echo target/debug)}
pw=${SPRIG_M05_PASSWORD:-sprig-dev-app}

nohup "$bin/sprigdb" --config "$here/gateway-2.toml" gateway >"$results/gateway-2.log" 2>&1 </dev/null &
gw2=$!
trap 'kill $gw2 2>/dev/null || true' EXIT
for _ in $(seq 1 120); do
    (echo >/dev/tcp/127.0.0.1/13317) 2>/dev/null && break
    kill -0 $gw2 2>/dev/null || { tail -n 30 "$results/gateway-2.log" >&2; exit 1; }
    sleep 0.5
done

SPRIG_TEST_M05_A="mysql://acme.app:$pw@127.0.0.1:13307" \
SPRIG_TEST_M05_B="mysql://globex.app:$pw@127.0.0.1:13307" \
SPRIG_TEST_M05_MYSQL_PORT=13306 \
SPRIG_TEST_M05_SECOND=127.0.0.1:13317 \
SPRIG_TEST_M05_BACKEND=mysql://root:sprig-dev-root@127.0.0.1:13301 \
SPRIG_TEST_M05_REPORT="$PWD/$results/report.jsonl" \
    cargo test -q --locked -p sprig-testkit --test m05_isolation -- --ignored --nocapture 2>&1 |
    tee "$results/suite.log"
exit "${PIPESTATUS[0]}"
