#!/usr/bin/env bash
# Runs the tier 1 connector matrix and sysbench against one MySQL-protocol
# endpoint and compares the results with expected.toml.
#
#   run.sh [--quick] <host> <port> <persona> [connector...]
#
# <persona> is mysql or mariadb: which reference the endpoint stands for
# (MySQL 8.4 or MariaDB directly today; the gateway's 3306 or 3307 later).
# With no connectors, runs all of them:
#
#   drivers:  mysql-connector-j mariadb-connector-j go-sql-driver mysql2
#             pymysql mysqlclient pdo_mysql mysqli mysql-cli mariadb-cli
#   ORMs:     sqlalchemy-pymysql sqlalchemy-mysqlclient hibernate
#             hibernate-mariadb prisma
#   tools:    mysqldump mariadb-dump sysbench sysbench-tpcc
#
# --quick (for CI on every pull request) runs only the drivers and
# SQLAlchemy, and leaves out their slow checks (large_row, multi_packet,
# lock_wait): about a minute instead of the full run's several.
#
# Each connector runs once per TLS mode (required, then disabled), one
# container at a time, built from the `connectors` Compose profile. Prints
# the matrix and exits non-zero if any check differs from expected.toml.
#
# Environment:
#   SPRIG_CONN_USER / SPRIG_CONN_PASSWORD   root / sprig-dev-root
#   SPRIG_CONN_DATABASE                     sprig_conn
#   SPRIG_CONN_TLS_MODES                    "required disabled"
#   SPRIG_CONN_SETUP=0     don't create the database or raise the server's
#                          max_allowed_packet to 64 MiB (both need privileges;
#                          a failure here is only a warning)
#   SPRIG_CONN_BUILD=0     don't build the images first
#   SPRIG_CONN_RESULTS     results directory (default
#                          connectors/results/<persona>-<host>-<port>)
#   SPRIG_CONN_EXPECTED_OVERLAY   a TOML file merged over expected.toml, for
#                          outcomes that belong to one target, not a persona
#   SPRIG_CONN_SKIP_CHECKS checks to leave out everywhere (--quick sets
#                          "large_row multi_packet lock_wait")
#   SPRIG_SYSBENCH_*       see sysbench/sysbench.sh and sysbench/tpcc.sh
set -euo pipefail

usage() { sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2; }
QUICK=
if [[ ${1:-} == --quick ]]; then QUICK=1; shift; fi
[[ $# -ge 3 ]] || usage
HOST=$1 PORT=$2 PERSONA=$3
shift 3
[[ $PERSONA == mysql || $PERSONA == mariadb ]] || { echo "persona must be mysql or mariadb" >&2; exit 2; }

DRIVERS=(mysql-connector-j mariadb-connector-j go-sql-driver mysql2 pymysql mysqlclient pdo_mysql mysqli
         mysql-cli mariadb-cli)
ALL=("${DRIVERS[@]}" sqlalchemy-pymysql sqlalchemy-mysqlclient hibernate hibernate-mariadb prisma
     mysqldump mariadb-dump persona-sql sysbench sysbench-tpcc)
QUICK_SET=("${DRIVERS[@]}" sqlalchemy-pymysql sqlalchemy-mysqlclient)
if [[ $# -gt 0 ]]; then
  CONNECTORS=("$@")
elif [[ -n $QUICK ]]; then
  CONNECTORS=("${QUICK_SET[@]}")
else
  CONNECTORS=("${ALL[@]}")
fi
if [[ -n $QUICK ]]; then
  export SPRIG_CONN_SKIP_CHECKS=${SPRIG_CONN_SKIP_CHECKS:-large_row multi_packet lock_wait}
fi
export SPRIG_CONN_SKIP_CHECKS=${SPRIG_CONN_SKIP_CHECKS:-}

service_of() {
  case $1 in
    mysql-connector-j | mariadb-connector-j) echo conn-java ;;
    go-sql-driver) echo conn-go ;;
    mysql2) echo conn-node ;;
    pymysql | mysqlclient | sqlalchemy-pymysql | sqlalchemy-mysqlclient) echo conn-python ;;
    pdo_mysql | mysqli) echo conn-php ;;
    mysql-cli | mysqldump | persona-sql) echo conn-mysql-cli ;;
    mariadb-cli | mariadb-dump) echo conn-mariadb-cli ;;
    hibernate | hibernate-mariadb) echo conn-hibernate ;;
    prisma) echo conn-prisma ;;
    sysbench | sysbench-tpcc) echo sysbench ;;
    *) echo "unknown connector: $1 (known: ${ALL[*]})" >&2; return 1 ;;
  esac
}

HERE=$(cd "$(dirname "$0")" && pwd)
COMPOSE_DIR=$(dirname "$HERE")
DC=(docker compose --project-directory "$COMPOSE_DIR" -f "$COMPOSE_DIR/compose.yaml" --profile connectors)
USER_=${SPRIG_CONN_USER:-root}
PASSWORD=${SPRIG_CONN_PASSWORD:-sprig-dev-root}
DATABASE=${SPRIG_CONN_DATABASE:-sprig_conn}
export SPRIG_CONN_TLS_MODES=${SPRIG_CONN_TLS_MODES:-required disabled}
RESULTS=${SPRIG_CONN_RESULTS:-$HERE/results/$PERSONA-$HOST-$PORT}

# A loopback target is the host's own port: containers reach it through the
# host network on Linux and through host.docker.internal on Docker Desktop.
TARGET=$HOST
case $HOST in
  127.0.0.1 | localhost | ::1)
    if [[ $(docker info --format '{{.OperatingSystem}}' 2>/dev/null) == *"Docker Desktop"* ]]; then
      TARGET=host.docker.internal
    else
      export SPRIG_CONN_NETWORK_MODE=${SPRIG_CONN_NETWORK_MODE:-host}
    fi
    ;;
esac

# Bash 3.2 (macOS) compatible: no mapfile.
services=(conn-python) # the summarizer runs in the Python image
for c in "${CONNECTORS[@]}"; do
  svc=$(service_of "$c")
  [[ " ${services[*]} " == *" $svc "* ]] || services+=("$svc")
done
if [[ ${SPRIG_CONN_BUILD:-1} == 1 ]]; then
  echo "building ${services[*]}"
  "${DC[@]}" build --quiet "${services[@]}"
fi

# `docker compose run` one connector: mem_limit, network and image come from
# compose.yaml; the container is removed afterwards.
run_one() { # service env... -- [command...]
  local svc=$1
  shift
  local envs=()
  while [[ $1 != -- ]]; do envs+=(-e "$1"); shift; done
  shift
  local extra=()
  # persona-sql (compat/): the corpora and golden outputs mounted, run by
  # the mysql 8.4 client of the conn-mysql-cli image.
  [[ " ${envs[*]} " == *" SPRIG_CONNECTOR=persona-sql "* ]] &&
    extra=(-v "$HERE/compat:/compat:ro" --entrypoint /compat/check.sh)
  "${DC[@]}" run --rm -T --no-deps ${extra[@]+"${extra[@]}"} "${envs[@]}" "$svc" "$@"
}

mkdir -p "$RESULTS"
if [[ ${SPRIG_CONN_SETUP:-1} == 1 ]]; then
  echo "setup: database $DATABASE, max_allowed_packet >= 64 MiB, local_infile on $HOST:$PORT"
  setup_sql="CREATE DATABASE IF NOT EXISTS \`$DATABASE\`;
SELECT @@GLOBAL.max_allowed_packet < 67108864 INTO @raise;
SET @s = IF(@raise, 'SET GLOBAL max_allowed_packet = 67108864', 'DO 0');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;
SET @s = IF(@@GLOBAL.local_infile, 'DO 0', 'SET GLOBAL local_infile = 1');
PREPARE s FROM @s; EXECUTE s; DEALLOCATE PREPARE s;"
  ok=
  # TLS first: it also warms caching_sha2_password's cache on MySQL.
  for tls in --ssl --skip-ssl; do
    if "${DC[@]}" run --rm -T --no-deps --entrypoint mariadb conn-mariadb-cli -h "$TARGET" -P "$PORT" \
         -u "$USER_" "-p$PASSWORD" $tls --skip-ssl-verify-server-cert -e "$setup_sql" \
         >"$RESULTS/setup.log" 2>&1; then
      ok=1
      break
    fi
  done
  [[ -n $ok ]] || echo "warning: setup failed (see $RESULTS/setup.log); continuing" >&2
fi

rm -f "$RESULTS"/*.jsonl "$RESULTS"/*.[a-z]*.log
start=$(date +%s)
for c in "${CONNECTORS[@]}"; do
  svc=$(service_of "$c")
  for tls in $SPRIG_CONN_TLS_MODES; do
    t0=$(date +%s)
    printf '%-20s %-9s ' "$c" "$tls"
    rc=0
    run_one "$svc" SPRIG_CONNECTOR="$c" SPRIG_HOST="$TARGET" SPRIG_PORT="$PORT" SPRIG_USER="$USER_" \
      SPRIG_PASSWORD="$PASSWORD" SPRIG_DATABASE="$DATABASE" SPRIG_TLS="$tls" SPRIG_PERSONA="$PERSONA" \
      SPRIG_CONN_SKIP_CHECKS="$SPRIG_CONN_SKIP_CHECKS" \
      SPRIG_SYSBENCH_TABLES="${SPRIG_SYSBENCH_TABLES:-2}" SPRIG_SYSBENCH_TABLE_SIZE="${SPRIG_SYSBENCH_TABLE_SIZE:-10000}" \
      SPRIG_SYSBENCH_THREADS="${SPRIG_SYSBENCH_THREADS:-4}" SPRIG_SYSBENCH_TIME="${SPRIG_SYSBENCH_TIME:-10}" \
      SPRIG_TPCC_SCALE="${SPRIG_TPCC_SCALE:-1}" SPRIG_TPCC_THREADS="${SPRIG_TPCC_THREADS:-2}" \
      SPRIG_TPCC_TIME="${SPRIG_TPCC_TIME:-20}" -- \
      >"$RESULTS/$c.$tls.jsonl" 2>"$RESULTS/$c.$tls.log" || rc=$?
    n=$(grep -c '"status"' "$RESULTS/$c.$tls.jsonl" || true)
    printf '%3d checks  %4ss%s\n' "$n" $(($(date +%s) - t0)) "$([[ $rc == 0 ]] || echo "  (exit $rc, see $RESULTS/$c.$tls.log)")"
  done
done
echo "ran in $(($(date +%s) - start))s; results in $RESULTS"
echo

expected=/connectors/expected.toml
mounts=(-v "$HERE:/connectors:ro" -v "$RESULTS:/results")
if [[ -n ${SPRIG_CONN_EXPECTED_OVERLAY:-} ]]; then
  overlay=$(cd "$(dirname "$SPRIG_CONN_EXPECTED_OVERLAY")" && pwd)/$(basename "$SPRIG_CONN_EXPECTED_OVERLAY")
  mounts+=(-v "$overlay:/overlay.toml:ro")
  expected+=,/overlay.toml
fi
docker run --rm --name "sprigdb-conn-summary-$$" --network none -e SPRIG_CONN_TLS_MODES -e SPRIG_CONN_SKIP_CHECKS \
  "${mounts[@]}" \
  --entrypoint python3 sprigdb-conn-python:dev -I /connectors/summarize.py /results "$expected" \
  "$PERSONA" "${CONNECTORS[@]}"
