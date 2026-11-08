#!/usr/bin/env bash
# Milestone M0.3's "done when" harness (chapter 48), against one endpoint:
#
#   run.sh <host> <port> <persona> <tenantA-user> <tenantB-user> <password>
#
#   # through the gateway (tenants acme and globex on the single node)
#   run.sh 127.0.0.1 13307 mariadb acme.app globex.app sprig-dev-app
#   run.sh 127.0.0.1 13306 mysql   acme.app globex.app sprig-dev-app
#   # baseline: direct MariaDB, two plain users standing in for the tenants
#   SPRIG_M03_MODE=baseline run.sh 127.0.0.1 13301 mariadb m03_acme m03_globex sprig-dev-app
#
# Both tenants run, in order: reset (drop their suite databases), Flyway
# (migrate, info, validate: the same six migrations into the same logical
# names), seed (each tenant's own rows, table and database), Hibernate
# (ddl-auto=validate, reads, writes, a procedure), the dumps (mysqldump on
# persona mysql, mariadb-dump on mariadb: contents, physical names,
# definers, --all-databases, restore into a new database) and the
# isolation checks (SHOW, information_schema, SELECT, the other tenant's
# names). Then the two Rust tests: the gateway-level leak suite
# (m03_leaks) and the settings check (m03_settings). One container at a
# time, each with the mem_limit of the `m03` Compose profile. Prints the
# matrix and exits non-zero if any check differs from expected.txt.
#
# Environment:
#   SPRIG_M03_MODE        gateway (default) or baseline. Baseline prefixes
#                         every database with "<tag>_" and, unless
#                         SPRIG_M03_SETUP=0, first creates the two users with
#                         grants on their own prefix only, as
#                         SPRIG_M03_ROOT_USER / SPRIG_M03_ROOT_PASSWORD
#                         (root / sprig-dev-root)
#   SPRIG_M03_TLS         disabled (default) or required, for the tools
#                         (the Rust leak suite reads raw bytes: always plain)
#   SPRIG_M03_SUITES      default "flyway hibernate dump isolation leaks settings"
#   SPRIG_M03_BUILD=0     don't build the images first
#   SPRIG_M03_RESULTS     results directory (default m03/results/<mode>-<persona>-<host>-<port>)
#   SPRIG_M03_EXPECTED_OVERLAY  more expected outcomes, in expected.txt's format
#   SPRIG_M03_OBSERVERS   more URLs for the settings check's observers, e.g.
#                         a second gateway: mysql://acme.app:sprig-dev-app@127.0.0.1:13317
#   SPRIG_M03_BACKEND     a direct root URL of the cluster (gateway mode), so
#                         the leak suite probes the real physical schema names
#   SPRIG_M03_BACKEND_USERS  backend accounts that must never show up as a
#                         DEFINER or as Flyway's installed_by (root)
#   CARGO_TARGET_DIR      for the Rust tests (cargo's default otherwise)
set -euo pipefail

usage() { sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2; }
[[ $# -eq 6 ]] || usage
HOST=$1 PORT=$2 PERSONA=$3 USER_A=$4 USER_B=$5 PASSWORD=$6
[[ $PERSONA == mysql || $PERSONA == mariadb ]] || { echo "persona must be mysql or mariadb" >&2; exit 2; }
MODE=${SPRIG_M03_MODE:-gateway}
[[ $MODE == gateway || $MODE == baseline ]] || { echo "SPRIG_M03_MODE must be gateway or baseline" >&2; exit 2; }
TLS=${SPRIG_M03_TLS:-disabled}
SUITES=${SPRIG_M03_SUITES:-flyway hibernate dump isolation leaks settings}
BACKEND_USERS=${SPRIG_M03_BACKEND_USERS:-root}

HERE=$(cd "$(dirname "$0")" && pwd)
COMPOSE_DIR=$(dirname "$HERE")
REPO=$(cd "$COMPOSE_DIR/../.." && pwd)
DC=(docker compose --project-directory "$COMPOSE_DIR" -f "$COMPOSE_DIR/compose.yaml" --profile m03)
RESULTS=${SPRIG_M03_RESULTS:-$HERE/results/$MODE-$PERSONA-$HOST-$PORT}
if [[ $PERSONA == mysql ]]; then CLI_SVC=m03-mysql-cli; else CLI_SVC=m03-mariadb-cli; fi

tag_of() { printf '%s' "$1" | tr -c 'A-Za-z0-9' '_'; }
prefix_of() { if [[ $MODE == baseline ]]; then printf '%s_' "$(tag_of "$1")"; fi; }
has() { [[ " $SUITES " == *" $1 "* ]]; }

# A loopback target is the host's own port: containers reach it through the
# host network on Linux and through host.docker.internal on Docker Desktop
# (as connectors/run.sh does).
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

mkdir -p "$RESULTS"
rm -f "$RESULTS"/*.jsonl "$RESULTS"/*.log "$RESULTS"/*.report "$RESULTS"/*.out "$RESULTS"/*.json
LOG=$RESULTS/run.log
: >"$LOG"

if [[ ${SPRIG_M03_BUILD:-1} == 1 ]]; then
  services=("$CLI_SVC")
  has hibernate && services+=(m03-hibernate)
  echo "building ${services[*]}"
  "${DC[@]}" build --quiet "${services[@]}"
fi

cli() { # step... ; the suite in the persona's CLI image, both tenants
  "${DC[@]}" run --rm -T --no-deps -e M03_HOST="$TARGET" -e M03_PORT="$PORT" -e M03_TLS="$TLS" -e M03_MODE="$MODE" \
    -e M03_USER_A="$USER_A" -e M03_USER_B="$USER_B" -e M03_PASSWORD="$PASSWORD" -e M03_BACKEND_USERS="$BACKEND_USERS" \
    "$CLI_SVC" "$@"
}

stage() { # name command... ; JSON lines to <name>.jsonl, stderr to <name>.log
  local name=$1 t0 rc=0
  shift
  t0=$(date +%s)
  printf '%-28s ' "$name"
  "$@" >"$RESULTS/$name.jsonl" 2>"$RESULTS/$name.log" || rc=$?
  printf '%3d checks %4ss%s\n' "$(grep -c '"status"' "$RESULTS/$name.jsonl" || true)" $(($(date +%s) - t0)) \
    "$([[ $rc == 0 ]] || echo "  (exit $rc, see $RESULTS/$name.log)")"
}

# --- baseline setup: two users, each with its own database prefix -------------

if [[ $MODE == baseline && ${SPRIG_M03_SETUP:-1} == 1 ]]; then
  root=${SPRIG_M03_ROOT_USER:-root} rootpw=${SPRIG_M03_ROOT_PASSWORD:-sprig-dev-root}
  setup=""
  for u in "$USER_A" "$USER_B"; do
    p=$(prefix_of "$u")
    setup+="CREATE USER IF NOT EXISTS '$u'@'%' IDENTIFIED BY '$PASSWORD';
ALTER USER '$u'@'%' IDENTIFIED BY '$PASSWORD';
GRANT ALL PRIVILEGES ON \`${p//_/\\_}%\`.* TO '$u'@'%';
"
  done
  echo "baseline setup: users $USER_A and $USER_B with grants on their own prefix, as $root"
  "${DC[@]}" run --rm -T --no-deps --entrypoint mariadb m03-mariadb-cli -h "$TARGET" -P "$PORT" -u "$root" "-p$rootpw" \
    --skip-ssl -e "$setup" >>"$LOG" 2>&1 || { echo "baseline setup failed, see $LOG" >&2; exit 1; }
fi

start=$(date +%s)
# A TLS login first (as connectors/run.sh does): it fills
# caching_sha2_password's cache, so the plaintext logins that follow need no
# RSA key exchange.
stage reset cli login reset

# --- Flyway --------------------------------------------------------------------

flyway_tenant() { # user
  local u=$1 t p url out
  t=$(tag_of "$u") p=$(prefix_of "$u")
  if [[ $PERSONA == mysql ]]; then url="jdbc:mysql://$TARGET:$PORT/"; else url="jdbc:mariadb://$TARGET:$PORT/"; fi
  # Flyway 13 ships MariaDB Connector/J 2.7 and uses it for both schemes.
  if [[ $TLS == required ]]; then url+="?useSsl=true&trustServerCertificate=true"; else url+="?useSsl=false"; fi
  url+="&allowPublicKeyRetrieval=true"
  local args=("-url=$url" "-user=$u" "-password=$PASSWORD" "-schemas=${p}shop,${p}crm" "-defaultSchema=${p}shop"
              "-placeholders.shop=${p}shop" "-placeholders.crm=${p}crm" "-locations=filesystem:/flyway/sql"
              "-outputType=json" "-connectRetries=0")
  for op in migrate info validate; do
    local t0 ms status detail
    t0=$(date +%s)
    out=$("${DC[@]}" run --rm -T --no-deps m03-flyway "${args[@]}" "$op" 2>>"$RESULTS/flyway.log") || true
    printf '%s\n' "$out" >"$RESULTS/flyway.$t.$op.json"
    ms=$((($(date +%s) - t0) * 1000))
    if ! jq -e . >/dev/null 2>&1 <<<"$out"; then
      status=fail detail="no JSON from flyway $op: $(head -c 300 <<<"$out")"
    elif jq -e '.error' >/dev/null <<<"$out"; then
      status=fail detail=$(jq -r '"\(.error.errorCode // ""): \(.error.message // "")"' <<<"$out")
    else
      case $op in
        migrate)
          if jq -e '.success == true and .migrationsExecuted == 6' >/dev/null <<<"$out"; then status=pass; else status=fail; fi
          detail=$(jq -r '"Flyway \(.flywayVersion) (\(.databaseType)): \(.migrationsExecuted) migrations into \(.schemaName), target version \(.targetSchemaVersion)\(if (.warnings|length) > 0 then "; warnings: " + (.warnings|join("; ")) else "" end)"' <<<"$out")
          ;;
        info)
          # Versioned migrations only: Flyway lists its schema creation too.
          if jq -e '[.migrations[] | select(.category == "Versioned")] | length == 6 and all(.[]; .state == "Success")' >/dev/null <<<"$out" &&
             jq -e '.schemaVersion == "6"' >/dev/null <<<"$out"; then status=pass; else status=fail; fi
          detail=$(jq -r '"schema version \(.schemaVersion), installed by \([.migrations[].installedBy] | unique | join(",")): " + ([.migrations[] | select(.category == "Versioned") | "V\(.version) \(.state)"] | join(", "))' <<<"$out")
          ;;
        validate)
          if jq -e '.validationSuccessful == true' >/dev/null <<<"$out"; then status=pass; else status=fail; fi
          detail=$(jq -r '"validated \(.validateCount // "?") migrations\(if .errorDetails then ": " + (.errorDetails|tostring) else "" end)\(if (.invalidMigrations|length) > 0 then "; invalid: " + (.invalidMigrations|tostring) else "" end)"' <<<"$out")
          ;;
      esac
    fi
    jq -cn --arg tenant "$t" --arg check "$op" --arg status "$status" --arg detail "${detail:0:400}" --argjson ms "$ms" \
      '{suite: "flyway", tenant: $tenant, check: $check, status: $status, detail: $detail, ms: $ms}'
  done
}

flyway_both() { flyway_tenant "$USER_A"; flyway_tenant "$USER_B"; }
has flyway && stage flyway flyway_both

# Each tenant's own rows, then the Flyway history compared across tenants.
stage seed cli seed history

# --- Hibernate -----------------------------------------------------------------

hibernate_all() {
  local connectors=(hibernate-mariadb) c u o
  # MySQL Connector/J on the MySQL persona only; it fails validation on
  # information_schema.KEYWORDS (expected.txt).
  [[ $PERSONA == mysql ]] && connectors=(hibernate hibernate-mariadb)
  for c in "${connectors[@]}"; do
    for u in "$USER_A" "$USER_B"; do
      if [[ $u == "$USER_A" ]]; then o=$USER_B; else o=$USER_A; fi
      "${DC[@]}" run --rm -T --no-deps -e SPRIG_CONNECTOR="$c" -e SPRIG_HOST="$TARGET" -e SPRIG_PORT="$PORT" \
        -e SPRIG_USER="$u" -e SPRIG_PASSWORD="$PASSWORD" -e SPRIG_TLS="$TLS" -e SPRIG_PERSONA="$PERSONA" \
        -e SPRIG_DB_PREFIX="$(prefix_of "$u")" -e SPRIG_TAG="$(tag_of "$u")" -e SPRIG_OTHER_TAG="$(tag_of "$o")" \
        m03-hibernate || echo "hibernate $c $u exited $?" >&2
    done
  done
}
has hibernate && stage hibernate hibernate_all

has dump && stage dump cli dump
has isolation && stage isolation cli isolation

# --- the Rust tests: gateway-level leak suite and settings ---------------------

url_of() { printf 'mysql://%s:%s@%s:%s' "$1" "$PASSWORD" "$HOST" "$2"; }
rust_test() { # test-name
  local env=(SPRIG_TEST_M03_A="$(url_of "$USER_A" "$PORT")" SPRIG_TEST_M03_B="$(url_of "$USER_B" "$PORT")"
             SPRIG_TEST_M03_REPORT="$RESULTS/$1.report" SPRIG_TEST_M03_PERSONA="$PERSONA")
  [[ $MODE == baseline ]] && env+=(SPRIG_TEST_M03_BASELINE=1)
  [[ -n ${SPRIG_M03_OBSERVERS:-} ]] && env+=(SPRIG_TEST_M03_OBSERVERS="$SPRIG_M03_OBSERVERS")
  [[ -n ${SPRIG_M03_BACKEND:-} ]] && env+=(SPRIG_TEST_M03_BACKEND="$SPRIG_M03_BACKEND")
  rm -f "$RESULTS/$1.report"
  (cd "$REPO" && env "${env[@]}" cargo test -q --locked -p sprig-testkit --test "$1" -- --ignored --nocapture) \
    >"$RESULTS/$1.out" 2>&1 || echo "cargo test $1 failed, see $RESULTS/$1.out" >&2
  # The test writes its own JSON lines; a test that died before writing any
  # is one failed check.
  if [[ -s $RESULTS/$1.report ]]; then
    cat "$RESULTS/$1.report"
  else
    jq -cn --arg s "${1#m03_}" --arg d "no report; see $RESULTS/$1.out" \
      '{suite: $s, tenant: "-", check: "run", status: "fail", detail: $d, ms: 0}'
  fi
}
if has leaks || has settings; then
  printf '%-28s ' "cargo build (tests)"
  t0=$(date +%s)
  (cd "$REPO" && cargo test -q --locked -p sprig-testkit --test m03_leaks --test m03_settings --no-run) \
    >"$RESULTS/cargo-build.log" 2>&1 || { echo "failed, see $RESULTS/cargo-build.log"; exit 1; }
  echo "          $(($(date +%s) - t0))s"
fi
has leaks && stage leaks rust_test m03_leaks
has settings && stage settings rust_test m03_settings

echo "ran in $(($(date +%s) - start))s; results in $RESULTS"
echo

# --- matrix and expected outcomes -----------------------------------------------

# expected.txt: mode persona suite connector check status reason...; `*`
# matches anything. The last matching line wins. A check passes the matrix
# only when its status equals the expected one (default pass).
expected() { # mode persona suite connector check
  local files=("$HERE/expected.txt")
  [[ -n ${SPRIG_M03_EXPECTED_OVERLAY:-} ]] && files+=("$SPRIG_M03_EXPECTED_OVERLAY")
  local want=pass why="" m p s c k st rest
  # The caller's `IFS=$'\t' read ... < <(expected ...)` reaches in here.
  while IFS=$' \t' read -r m p s c k st rest; do
    [[ -z $m || $m == \#* ]] && continue
    [[ $m == "*" || $m == "$1" ]] && [[ $p == "*" || $p == "$2" ]] && [[ $s == "*" || $s == "$3" ]] &&
      [[ $c == "*" || $c == "$4" ]] && [[ $k == "*" || $k == "$5" ]] && { want=$st why=$rest; }
  done < <(cat "${files[@]}")
  printf '%s\t%s\n' "$want" "$why"
}

bad=0 total=0
printf '%-10s %-10s %-18s %-20s %-7s %-7s %s\n' SUITE TENANT CONNECTOR CHECK STATUS WANT DETAIL
while IFS=$'\t' read -r suite tenant connector check status detail; do
  total=$((total + 1))
  IFS=$'\t' read -r want why < <(expected "$MODE" "$PERSONA" "$suite" "$connector" "$check")
  mark=" "
  if [[ $status != "$want" ]]; then mark="X"; bad=$((bad + 1)); fi
  note=$detail
  [[ $status == "$want" && $want != pass && -n $why ]] && note="(expected: ${why#\# }) $detail"
  printf '%s %-9s %-10s %-18s %-20s %-7s %-7s %s\n' "$mark" "$suite" "${tenant:0:10}" "${connector:--}" "$check" "$status" \
    "$want" "${note:0:160}"
done < <(for st in reset flyway seed hibernate dump isolation leaks settings; do
           [[ -f $RESULTS/$st.jsonl ]] && cat "$RESULTS/$st.jsonl"
         done | jq -r 'select(.status) | [.suite, .tenant, (.connector // "-"), .check, .status, (.detail | gsub("[\t\n]"; " "))] | @tsv')

# Every expected suite must have reported something.
for s in $SUITES; do
  grep -q "\"suite\":\"$s\"" "$RESULTS"/*.jsonl 2>/dev/null || { echo "X $s reported nothing"; bad=$((bad + 1)); }
done
echo
echo "$total checks, $bad differ from expected.txt (mode $MODE, persona $PERSONA, $HOST:$PORT)"
[[ $bad == 0 ]]
