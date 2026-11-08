#!/usr/bin/env bash
# sysbench OLTP through one endpoint: oltp_point_select and oltp_read_write,
# each with server-side prepared statements (--db-ps-mode=auto) and with
# plain queries (--db-ps-mode=disable). Prints one JSON line per step, in
# the connector checks' format (../README.md).
#
# Plaintext only (see below). Small by default, so a run takes about a
# minute:
#   SPRIG_SYSBENCH_TABLES (2), SPRIG_SYSBENCH_TABLE_SIZE (10000),
#   SPRIG_SYSBENCH_THREADS (4), SPRIG_SYSBENCH_TIME (10 seconds per step).
set -uo pipefail

TLS=$SPRIG_TLS
TABLES=${SPRIG_SYSBENCH_TABLES:-2}
SIZE=${SPRIG_SYSBENCH_TABLE_SIZE:-10000}
THREADS=${SPRIG_SYSBENCH_THREADS:-4}
TIME=${SPRIG_SYSBENCH_TIME:-10}
COMMON=(--db-driver=mysql "--mysql-host=$SPRIG_HOST" "--mysql-port=$SPRIG_PORT" "--mysql-user=$SPRIG_USER"
        "--mysql-password=$SPRIG_PASSWORD" "--mysql-db=$SPRIG_DATABASE" "--tables=$TABLES" "--table-size=$SIZE"
        "--mysql-ssl=$([[ $TLS == required ]] && echo on || echo off)")
OUT=$(mktemp)
trap 'rm -f "$OUT"' EXIT

emit() { # check status detail ms
  local d=${3//\\/\\\\}
  d=${d//\"/\\\"}
  printf '{"connector":"sysbench","tls":"%s","check":"%s","status":"%s","detail":"%s","ms":%s}\n' \
    "$TLS" "$1" "$2" "$(printf '%s' "$d" | tr '\n\t' '  ' | head -c 300)" "${4:-0}"
}

step() { # check sysbench-args...
  local name=$1 t0 rc
  shift
  t0=$(date +%s%3N)
  sysbench "${COMMON[@]}" "$@" >"$OUT" 2>&1
  rc=$?
  local ms=$(($(date +%s%3N) - t0))
  if ((rc != 0)) || grep -q 'FATAL' "$OUT"; then
    emit "$name" fail "exit $rc: $(grep -m3 -E 'FATAL|error' "$OUT")" "$ms"
    return 1
  fi
  local tps p95 errs
  tps=$(sed -n 's/.*transactions: *[0-9]* *(\([0-9.]*\) per sec.).*/\1/p' "$OUT")
  p95=$(sed -n 's/.*95th percentile: *\([0-9.]*\).*/\1/p' "$OUT")
  errs=$(sed -n 's/.*errors: *\([0-9]*\) .*/\1/p' "$OUT" | head -1)
  if [[ -n $tps ]]; then
    emit "$name" pass "tps=$tps p95_ms=$p95 ignored_errors=${errs:-0} threads=$THREADS time=${TIME}s" "$ms"
  else
    emit "$name" pass "" "$ms"
  fi
}

STEPS=(prepare point_select_ps point_select_text read_write_ps read_write_text cleanup)
if [[ $TLS == required ]]; then
  # sysbench 1.0's --mysql-ssl=on loads client-key.pem, client-cert.pem and
  # cacert.pem from its working directory and verifies the server with that
  # CA; it has no "encrypt without verifying" mode. The connector checks
  # cover TLS, so sysbench runs in plaintext only.
  for s in "${STEPS[@]}"; do emit "$s" n/a "sysbench 1.0.20 can't encrypt without a client certificate and CA"; done
  exit 0
fi

RUN=(--threads="$THREADS" --time="$TIME" --report-interval=0)
sysbench "${COMMON[@]}" oltp_read_write cleanup >/dev/null 2>&1
if step prepare oltp_read_write prepare --threads="$TABLES"; then
  step point_select_ps oltp_point_select run --db-ps-mode=auto "${RUN[@]}"
  step point_select_text oltp_point_select run --db-ps-mode=disable "${RUN[@]}"
  step read_write_ps oltp_read_write run --db-ps-mode=auto "${RUN[@]}"
  step read_write_text oltp_read_write run --db-ps-mode=disable "${RUN[@]}"
else
  for s in point_select_ps point_select_text read_write_ps read_write_text; do emit "$s" fail "prepare failed"; done
fi
step cleanup oltp_read_write cleanup
exit 0
