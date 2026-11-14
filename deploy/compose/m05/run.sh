#!/usr/bin/env bash
# The SQL isolation suite: crates/sprig-testkit/tests/m05_isolation.rs
# against the development gateway (gateway-dev.sh up: tenants acme and
# globex, admins acme.app and globex.app), with a second gateway
# (m05/gateway-2.toml, 13316/13317) for the revocation-propagation check,
# and MariaDB's grant tables audited through the single node's root.
#
#   deploy/compose/m05/run.sh
#
# Variables: SPRIG_M05_BIN (the directory holding sprigdb; default the one
# gateway-dev.sh up built), SPRIG_M05_PASSWORD (sprig-dev-app). To run
# against another first gateway and tenants (an agent's own, say):
# SPRIG_M05_TENANTS ("acme globex"), SPRIG_M05_MYSQL_PORT (13306),
# SPRIG_M05_MARIADB_PORT (13307), SPRIG_M05_GW2_CONFIG (m05/gateway-2.toml)
# and SPRIG_M05_GW2_PORT (its MariaDB-persona port, 13317). Results:
# deploy/compose/m05/results/ (report.jsonl, gateway-2.log, suite.log).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
here=deploy/compose/m05
results=$here/results
mkdir -p "$results"
state=deploy/compose/.gateway-dev
bin=${SPRIG_M05_BIN:-$(cat "$state/bin" 2>/dev/null || echo target/debug)}
pw=${SPRIG_M05_PASSWORD:-sprig-dev-app}
read -r ta tb <<<"${SPRIG_M05_TENANTS:-acme globex}"
mysql_port=${SPRIG_M05_MYSQL_PORT:-13306}
mariadb_port=${SPRIG_M05_MARIADB_PORT:-13307}
gw2_config=${SPRIG_M05_GW2_CONFIG:-$here/gateway-2.toml}
gw2_port=${SPRIG_M05_GW2_PORT:-13317}

nohup "$bin/sprigdb" --config "$gw2_config" gateway >"$results/gateway-2.log" 2>&1 </dev/null &
gw2=$!
trap 'kill $gw2 2>/dev/null || true' EXIT
for _ in $(seq 1 120); do
    (echo >/dev/tcp/127.0.0.1/$gw2_port) 2>/dev/null && break
    kill -0 $gw2 2>/dev/null || { tail -n 30 "$results/gateway-2.log" >&2; exit 1; }
    sleep 0.5
done

SPRIG_TEST_M05_A="mysql://$ta.app:$pw@127.0.0.1:$mariadb_port" \
SPRIG_TEST_M05_B="mysql://$tb.app:$pw@127.0.0.1:$mariadb_port" \
SPRIG_TEST_M05_MYSQL_PORT=$mysql_port \
SPRIG_TEST_M05_SECOND=127.0.0.1:$gw2_port \
SPRIG_TEST_M05_BACKEND=mysql://root:sprig-dev-root@127.0.0.1:13301 \
SPRIG_TEST_M05_REPORT="$PWD/$results/report.jsonl" \
    cargo test -q --locked -p sprig-testkit --test m05_isolation -- --ignored --nocapture 2>&1 |
    tee "$results/suite.log"
exit "${PIPESTATUS[0]}"
