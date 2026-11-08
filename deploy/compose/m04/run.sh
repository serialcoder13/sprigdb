#!/usr/bin/env bash
# M0.4's scenario suite on the three reference topologies (chapter 48, M0.4;
# chapter 13 build step 14). See README.md.
#
#   deploy/compose/m04/run.sh setup              # settings, tenants, restart
#   deploy/compose/m04/run.sh single|replication|galera
#   deploy/compose/m04/run.sh all                # the three, one at a time
#   deploy/compose/m04/run.sh o1 single|replication|galera  # decision O1's scenario
#   deploy/compose/m04/run.sh drift              # drift.sh as acme.app
#   deploy/compose/m04/run.sh teardown           # settings and tenants removed
#
# Needs gateway-dev.sh up (catalog mode). setup writes the workload settings
# the scenarios need at global scope (1-minute buckets, every fingerprint
# watched and sampled), creates the tenants m04r on dc-a-c2 (primary and
# replicas) and m04g on dc-a-c3 (Galera), each with the user app, and
# restarts the controller and the gateway: the gateway reads the recorder's
# settings at start, and the controller runs the auto-index loop on the test
# clock in $STATE/autoindex-clock. The single-node topology uses acme
# (dc-a-c1).
#
# Variables: CARGO_TARGET_DIR (target), SPRIG_GATEWAY_DEV_STATE
# (deploy/compose/.gateway-dev), SPRIG_TEST_M04_ROWS (100000).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
root=$(pwd)
state=${SPRIG_GATEWAY_DEV_STATE:-deploy/compose/.gateway-dev}
bin=$(cat "$state/bin" 2>/dev/null || echo "${CARGO_TARGET_DIR:-target}/debug")
ctl() { "$bin/sprigctl" "$@"; }
results=deploy/compose/m04/results
clock=$root/$state/autoindex-clock
say() { echo "m04: $*"; }

# Recorder settings the gateway reads at start (chapter 12).
workload_settings=(
  "workload.bucket_minutes 1"
  "workload.watch_p95_ms 0"
  "workload.examined_sample_rate 0.5"
  "workload.sample_rate 0.2"
  "workload.samples_per_bucket 10"
)

tenant_id() { ctl -o json tenant show "$1" | jq -r '.tenant_id'; }

setup() {
  local kv k v
  for kv in "${workload_settings[@]}"; do
    read -r k v <<<"$kv"
    ctl settings set "$k" "$v" --scope global --reason "m04 scenario suite" --yes >/dev/null
  done
  for t in "m04r dc-a-c2" "m04g dc-a-c3"; do
    read -r name cluster <<<"$t"
    ctl tenant create "$name" --cluster "$cluster" >/dev/null 2>&1 || true
    # A just-created catalog can be busy for a moment (39071): retry.
    for try in 1 2 3 4 5 6; do
      ctl tenant user add "$name" app --password sprig-dev-app --native --if-not-exists >/dev/null && break
      [[ $try == 6 ]] && exit 1
      sleep 2
    done
    ctl settings set auth.mysql_native_password true --scope "tenant:$name" --reason m04 --yes >/dev/null
    say "tenant $name ($(tenant_id "$name")) on $cluster"
  done
  echo 0 >"$clock"
  SPRIGDB_CONTROLLER_AUTOINDEX_TEST_CLOCK_FILE=$clock deploy/compose/gateway-dev.sh restart
}

teardown() {
  local kv k v
  echo 0 >"$clock"
  for kv in "${workload_settings[@]}"; do
    read -r k v <<<"$kv"
    ctl settings reset "$k" --scope global --reason "m04 scenario suite" --yes >/dev/null || true
  done
  for name in m04r m04g; do
    ctl tenant delete "$name" --yes >/dev/null 2>&1 && say "tenant $name deleted" || say "tenant $name not deleted"
  done
  deploy/compose/gateway-dev.sh restart
}

topology() { # single|replication|galera [test]
  local tenant catalog test=${2:-chapter_13_scenarios_on_one_topology} out=$1
  case $1 in
    single) tenant=acme catalog=mysql://root:sprig-dev-root@127.0.0.1:13301 ;;
    replication) tenant=m04r catalog=mysql://root:sprig-dev-root@127.0.0.1:13311 ;;
    galera) tenant=m04g catalog=mysql://root:sprig-dev-root@127.0.0.1:13321 ;;
    *) echo "unknown topology $1" >&2; exit 2 ;;
  esac
  [[ $test == chapter_13_scenarios_on_one_topology ]] || out=$1-o1
  mkdir -p "$results"
  say "$1: tenant $tenant ($test)"
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep sprigdb-dev || true
  SPRIG_TEST_M04_GATEWAY=${SPRIG_TEST_M04_GATEWAY:-127.0.0.1:13307} \
    SPRIG_TEST_M04_TENANT="$tenant:$(tenant_id "$tenant")" \
    SPRIG_TEST_M04_CATALOG=$catalog SPRIG_TEST_M04_CLOCK_FILE=$clock SPRIG_TEST_M04_TOPOLOGY=$1 \
    SPRIG_TEST_M04_REPORT=$root/$results/$out.json SPRIG_TEST_M04_SPRIGCTL=$bin/sprigctl \
    cargo test -p sprig-testkit --test m04_scenarios "$test" -- --ignored --nocapture 2>&1 \
    | tee "$results/$out.log"
  [[ ${PIPESTATUS[0]} == 0 ]]
}

case ${1:-} in
  setup) setup ;;
  teardown) teardown ;;
  single | replication | galera) topology "$1" ;;
  all) for t in single replication galera; do topology "$t"; done ;;
  o1) topology "${2:-single}" usage_based_removal_on_one_topology ;;
  drift) deploy/compose/m04/drift.sh acme.app sprig-dev-app ;;
  *) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2 ;;
esac
