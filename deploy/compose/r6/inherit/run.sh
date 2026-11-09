#!/usr/bin/env bash
# R6 order 2 (chapter 42) through a gateway: what schema tools see of an
# inheritable table.
#
#   run.sh <host> <mysql-port> <mariadb-port> <user> <password>
#   run.sh 127.0.0.1 38306 38307 r6-inh-a.app sprig-dev-app
#
# Per persona (mysql on the first port, mariadb on the second), in a fresh
# database r6i_tools_<my|ma> of the user's tenant, with a space r6itools:
#
# 1. Flyway 13 (migrate, info, validate) runs ./flyway/sql: an inheritable
#    table, a share to r6itools and three rows.
# 2. Hibernate ddl-auto=validate over the table, then JPA writes in the
#    root space and in r6itools (an override and a delete that hides), and
#    the root's rows unchanged (MySQL Connector/J on mysql, MariaDB
#    Connector/J on mariadb).
# 3. A dump of the root space: mysqldump (the host's client) on mysql,
#    mariadb-dump (the connector matrix's CLI image) on mariadb, with
#    --single-transaction --skip-add-locks. The dump must hold the merged
#    rows as a plain table: PRIMARY KEY (`id`), no _sprig_ names, no
#    physical names. It is loaded into r6i_tools_<p>_copy and read back.
#
# Prints one line per check and exits non-zero if any fails. Needs docker,
# the mysql client and jq. Takes one of the shared heavy-job lock slots.
set -euo pipefail

[[ $# -eq 5 ]] || { sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2; }
HOST=$1 MY_PORT=$2 MA_PORT=$3 USER_=$4 PASS=$5
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${SPRIG_R6I_RESULTS:-$HERE/results}
mkdir -p "$OUT"
TARGET=$HOST
case $HOST in 127.0.0.1 | localhost) TARGET=host.docker.internal ;; esac
fails=0
check() { # name status detail
  printf '%-34s %-4s %s\n' "$1" "$2" "$3"
  [[ $2 == pass ]] || fails=$((fails + 1))
}

mkdir -p /tmp/claude-502
L=""
until mkdir /tmp/claude-502/sprig-heavy.lock 2>/dev/null && L=/tmp/claude-502/sprig-heavy.lock || { mkdir /tmp/claude-502/sprig-heavy-2.lock 2>/dev/null && L=/tmp/claude-502/sprig-heavy-2.lock; }; do sleep 20; done
echo "r6inherit $(date)" >"$L/owner"
trap 'rm -rf "$L"' EXIT

sql() { # port db sql...
  local port=$1 db=$2
  shift 2
  mysql --ssl-mode=REQUIRED -h"$HOST" -P"$port" -u"$USER_" -p"$PASS" --batch --skip-column-names ${db:+"$db"} -e "$*" 2>&1 |
    grep -v 'Using a password on the command line' || true
}

leaks() { grep -Eo '\bt[0-9]+_(d[0-9]+|sprig|u[0-9]+)[A-Za-z0-9_]*' "$1" | sort -u | head -5 || true; }

if ! docker image inspect sprigdb-r6i-hibernate:dev >/dev/null 2>&1 || [[ ${SPRIG_R6I_BUILD:-0} == 1 ]]; then
  docker build -q -t sprigdb-r6i-hibernate:dev --build-context conn="$HERE/../../connectors/hibernate" "$HERE/hibernate" >/dev/null
fi

sql "$MY_PORT" "" "CREATE SPACE r6itools" >/dev/null
for persona in mysql mariadb; do
  if [[ $persona == mysql ]]; then port=$MY_PORT p=my scheme=mysql conn=hibernate; else port=$MA_PORT p=ma scheme=mariadb conn=hibernate-mariadb; fi
  db=r6i_tools_$p copy=r6i_tools_${p}_copy
  sql "$port" "" "DROP SHARE IF EXISTS r6i_tools_$p; DROP DATABASE IF EXISTS $db; DROP DATABASE IF EXISTS $copy" >/dev/null

  # 1. Flyway.
  for op in migrate info validate; do
    out=$(docker run --rm --memory=384m -v "$HERE/flyway/sql:/flyway/sql:ro" -e JAVA_ARGS="-Xmx160m -XX:+UseSerialGC" \
      flyway/flyway:13.8.1 "-url=jdbc:$scheme://$TARGET:$port/?useSsl=true&trustServerCertificate=true&allowPublicKeyRetrieval=true" \
      "-user=$USER_" "-password=$PASS" "-schemas=$db" "-defaultSchema=$db" -locations=filesystem:/flyway/sql \
      "-placeholders.p=$p" -outputType=json -connectRetries=0 "$op" 2>"$OUT/flyway.$p.$op.log") || true
    printf '%s\n' "$out" >"$OUT/flyway.$p.$op.json"
    case $op in
      migrate) ok=$(jq -r 'if .success == true and .migrationsExecuted == 3 then "pass" else "fail" end' <<<"$out" 2>/dev/null || echo fail)
        detail=$(jq -r '"\(.migrationsExecuted) migrations, target \(.targetSchemaVersion)"' <<<"$out" 2>/dev/null || head -c 200 <<<"$out") ;;
      info) ok=$(jq -r 'if ([.migrations[] | select(.version != "" and .state == "Success")] | length) == 3 then "pass" else "fail" end' <<<"$out" 2>/dev/null || echo fail)
        detail=$(jq -r '[.migrations[] | "\(.version) \(.state)"] | join(", ")' <<<"$out" 2>/dev/null || head -c 200 <<<"$out") ;;
      validate) ok=$(jq -r 'if .validationSuccessful == true then "pass" else "fail" end' <<<"$out" 2>/dev/null || echo fail)
        detail=$(jq -r '"validationSuccessful=\(.validationSuccessful)"' <<<"$out" 2>/dev/null || head -c 200 <<<"$out") ;;
    esac
    check "$persona flyway $op" "$ok" "$detail"
  done
  got=$(sql "$port" "$db" "SET @sprig_space = 'r6itools'; SELECT GROUP_CONCAT(id ORDER BY id) FROM products")
  [[ $got == "1,2" ]] && check "$persona flyway share" pass "r6itools sees $got" || check "$persona flyway share" fail "r6itools sees $got"

  # 2. Hibernate.
  docker run --rm --memory=512m -e SPRIG_CONNECTOR=$conn -e SPRIG_HOST="$TARGET" -e SPRIG_PORT="$port" \
    -e SPRIG_USER="$USER_" -e SPRIG_PASSWORD="$PASS" -e SPRIG_DB="$db" -e SPRIG_SPACE=r6itools sprigdb-r6i-hibernate:dev \
    >"$OUT/hibernate.$p.jsonl" 2>"$OUT/hibernate.$p.log" || true
  if [[ ! -s $OUT/hibernate.$p.jsonl ]]; then check "$persona hibernate" fail "no output, see $OUT/hibernate.$p.log"; fi
  while IFS= read -r line; do
    check "$persona hibernate $(jq -r .check <<<"$line")" "$(jq -r .status <<<"$line")" "$(jq -r .detail <<<"$line")"
  done < <(grep '^{' "$OUT/hibernate.$p.jsonl" || true)

  # 3. Dump of the root space and a round trip.
  dump=$OUT/dump.$p.sql
  if [[ $persona == mysql ]]; then
    mysqldump --ssl-mode=REQUIRED -h"$HOST" -P"$port" -u"$USER_" -p"$PASS" --single-transaction --skip-add-locks \
      --set-gtid-purged=OFF --column-statistics=0 --no-tablespaces "$db" >"$dump" 2>"$OUT/dump.$p.log" || true
  else
    docker run --rm --memory=256m --entrypoint mariadb-dump sprigdb-conn-mariadb-cli:dev --ssl --skip-ssl-verify-server-cert \
      -h "$TARGET" -P "$port" -u "$USER_" -p"$PASS" --single-transaction --skip-add-locks "$db" >"$dump" 2>"$OUT/dump.$p.log" || true
  fi
  if grep -q 'PRIMARY KEY (`id`)' "$dump" && grep -q 'INSERT INTO `products`' "$dump"; then
    check "$persona dump table" pass "products with PRIMARY KEY (id) and its rows"
  else
    check "$persona dump table" fail "$(head -c 300 "$OUT/dump.$p.log")"
  fi
  if grep -Eq '_sprig_' "$dump"; then check "$persona dump hidden names" fail "$(grep -Eo '.{20}_sprig_.{20}' "$dump" | head -2 | tr '\n' ' ')"; else check "$persona dump hidden names" pass "no _sprig_ in the dump"; fi
  l=$(leaks "$dump"); [[ -z $l ]] && check "$persona dump physical names" pass "none" || check "$persona dump physical names" fail "$l"
  sql "$port" "" "CREATE DATABASE $copy" >/dev/null
  load=$(mysql --ssl-mode=REQUIRED -h"$HOST" -P"$port" -u"$USER_" -p"$PASS" "$copy" <"$dump" 2>&1 | grep -v 'Using a password' || true)
  [[ -z $load ]] && check "$persona dump restore" pass "loaded into $copy" || check "$persona dump restore" fail "$(head -c 300 <<<"$load")"
  a=$(sql "$port" "$db" "SELECT * FROM products ORDER BY id"); b=$(sql "$port" "$copy" "SELECT * FROM products ORDER BY id")
  [[ -n $a && $a == "$b" ]] && check "$persona dump round trip" pass "$(wc -l <<<"$a" | tr -d ' ') rows equal" || check "$persona dump round trip" fail "[$a] vs [$b]"

  [[ ${SPRIG_R6I_KEEP:-0} == 1 ]] || sql "$port" "" "DROP SHARE IF EXISTS r6i_tools_$p; DROP DATABASE IF EXISTS $db; DROP DATABASE IF EXISTS $copy" >/dev/null
done
[[ ${SPRIG_R6I_KEEP:-0} == 1 ]] || sql "$MY_PORT" "" "DROP SPACE IF EXISTS r6itools CASCADE" >/dev/null

echo "failures: $fails"
exit $((fails > 0))
