#!/usr/bin/env bash
# sysbench-tpcc (Percona-Lab, pinned in the Dockerfile) at a tiny scale
# through one endpoint. Prints one JSON line per step, in the connector
# checks' format (../README.md):
#
#   prepare   create and load the TPC-C tables (SPRIG_TPCC_SCALE warehouses, 1)
#   run       the TPC-C mix for SPRIG_TPCC_TIME seconds (20) on
#             SPRIG_TPCC_THREADS threads (2), server-side prepared statements
#   check     tpcc_check.lua's consistency checks
#   cleanup   drop the tables
#
# Plaintext only, for the same reason as sysbench.sh. sysbench ignores
# 1213, 1205 and 1020 by default (deadlocks, lock waits, and MariaDB's
# innodb_snapshot_isolation conflicts); the run's detail counts them.
set -uo pipefail

TLS=$SPRIG_TLS
SCALE=${SPRIG_TPCC_SCALE:-1}
THREADS=${SPRIG_TPCC_THREADS:-2}
TIME=${SPRIG_TPCC_TIME:-20}
cd /tpcc
COMMON=(--db-driver=mysql "--mysql-host=$SPRIG_HOST" "--mysql-port=$SPRIG_PORT" "--mysql-user=$SPRIG_USER"
        "--mysql-password=$SPRIG_PASSWORD" "--mysql-db=$SPRIG_DATABASE" --tables=1 "--scale=$SCALE" --mysql-ssl=off)
OUT=$(mktemp)
trap 'rm -f "$OUT"' EXIT

emit() { # check status detail ms
  local d=${3//\\/\\\\}
  d=${d//\"/\\\"}
  printf '{"connector":"sysbench-tpcc","tls":"%s","check":"%s","status":"%s","detail":"%s","ms":%s}\n' \
    "$TLS" "$1" "$2" "$(printf '%s' "$d" | tr '\n\t' '  ' | head -c 300)" "${4:-0}"
}

step() { # check detail-prefix sysbench-args...
  local name=$1 prefix=$2 t0 rc
  shift 2
  t0=$(date +%s%3N)
  sysbench "${COMMON[@]}" "$@" >"$OUT" 2>&1
  rc=$?
  local ms=$(($(date +%s%3N) - t0))
  if ((rc != 0)) || grep -q -E 'FATAL|ALERT' "$OUT"; then
    emit "$name" fail "exit $rc: $(grep -m3 -E 'FATAL|ALERT|error|Error' "$OUT")" "$ms"
    return 1
  fi
  local tps p95 errs
  tps=$(sed -n 's/.*transactions: *[0-9]* *(\([0-9.]*\) per sec.).*/\1/p' "$OUT")
  p95=$(sed -n 's/.*95th percentile: *\([0-9.]*\).*/\1/p' "$OUT")
  errs=$(sed -n 's/.*ignored errors: *\([0-9]*\) .*/\1/p' "$OUT" | head -1)
  emit "$name" pass "$prefix${tps:+ tps=$tps p95_ms=$p95 ignored_errors=${errs:-0} threads=$THREADS time=${TIME}s}" "$ms"
}

STEPS=(prepare run check cleanup)
if [[ $TLS == required ]]; then
  for s in "${STEPS[@]}"; do emit "$s" n/a "sysbench 1.0.20 can't encrypt without a client certificate and CA"; done
  exit 0
fi

sysbench "${COMMON[@]}" ./tpcc.lua cleanup >/dev/null 2>&1
if step prepare "scale=$SCALE, 9 tables" ./tpcc.lua prepare --threads=1; then
  step run "" ./tpcc.lua run --threads="$THREADS" --time="$TIME" --report-interval=0 --db-ps-mode=auto
  if step check "consistency checks passed" ./tpcc.lua check --threads=1; then
    grep -q -i -E 'fail|inconsisten' "$OUT" && emit check fail "$(grep -m2 -i -E 'fail|inconsisten' "$OUT")"
  fi
else
  emit run fail "prepare failed"
  emit check fail "prepare failed"
fi
step cleanup "" ./tpcc.lua cleanup
exit 0
