#!/usr/bin/env bash
# The automatic-indexing drift suite: schema tools see no drift with
# automatic indexes present.
#
#   deploy/compose/m04/drift.sh [tenant-user] [password]     # acme.app, sprig-dev-app
#
# Needs gateway-dev.sh up with the m04 settings (m04/run.sh sets them) and the
# m03 images (m03/run.sh builds them). Steps, as tenant A:
#
#   1. m03/run.sh with the Flyway suite only: shop and crm migrated (V1-V6),
#      for both tenants, through the MariaDB persona.
#   2. shop.orders grows to 30,000 rows and a lookup by qty runs until
#      an automatic index exists on it (SHOW INDEX with
#      @sprig_show_auto_indexes = 1).
#   3. With the index present: Flyway info and validate; Hibernate
#      ddl-auto=validate (MariaDB Connector/J, both personas' ports are the
#      same tenant); mysqldump (8.4, MySQL persona) and mariadb-dump (11.8,
#      MariaDB persona) of shop, with and without data; SHOW CREATE TABLE,
#      SHOW INDEX and information_schema.STATISTICS through both ports.
#   4. Every output is checked for `sprig_auto_`: none may appear. The
#      index must still exist (it is hidden, not gone) and EXPLAIN must show
#      it (EXPLAIN is not filtered).
#
# Results: deploy/compose/m04/results/drift/ (one file per tool, and
# drift.jsonl with one line per check). Exit status 0 when every check passes.
set -euo pipefail

USER_A=${1:-acme.app}
PASSWORD=${2:-sprig-dev-app}
USER_B=${SPRIG_M04_OTHER_USER:-globex.app}
HERE=$(cd "$(dirname "$0")" && pwd)
COMPOSE_DIR=$(dirname "$HERE")
OUT=${SPRIG_M04_RESULTS:-$HERE/results}/drift
DC=(docker compose --project-directory "$COMPOSE_DIR" -f "$COMPOSE_DIR/compose.yaml" --profile m03)
TARGET=host.docker.internal
[[ $(docker info --format '{{.OperatingSystem}}' 2>/dev/null) == *"Docker Desktop"* ]] || {
  TARGET=127.0.0.1
  export SPRIG_CONN_NETWORK_MODE=host
}
mkdir -p "$OUT"
rm -rf "${OUT:?}"/*
JSONL=$OUT/drift.jsonl

check() { # name status detail
  jq -cn --arg c "$1" --arg s "$2" --arg d "$3" '{suite: "drift", check: $c, status: $s, detail: $d}' | tee -a "$JSONL"
}
my() { # port args... ; mysql 8.4 client through the gateway
  mysql -h 127.0.0.1 -P "$1" -u "$USER_A" "-p$PASSWORD" --ssl-mode=REQUIRED -N -B "${@:2}" 2>/dev/null
}

# 1. Flyway's migrations.
echo "drift: migrating shop and crm (m03/run.sh, Flyway suite)"
SPRIG_M03_SUITES=flyway SPRIG_M03_BUILD=${SPRIG_M03_BUILD:-0} SPRIG_M03_RESULTS=$OUT/m03 \
  "$COMPOSE_DIR/m03/run.sh" 127.0.0.1 13307 mariadb "$USER_A" "$USER_B" "$PASSWORD" >"$OUT/m03-run.log" 2>&1 ||
  { tail -20 "$OUT/m03-run.log"; echo "drift: m03 flyway failed" >&2; exit 1; }

# 2. An automatic index on shop.orders.
echo "drift: growing shop.orders and querying it until it has an automatic index"
my 13306 -e "SPRIG SET (auto_index.window_hours = 1, auto_index.eval_interval_minutes = 1,
  auto_index.p95_threshold_ms = 2, auto_index.max_new_per_day = 50, auto_index.rollout_pause_minutes = 0)"
# customer_id already has the foreign key's index; qty has none.
my 13306 shop -e "INSERT INTO orders (customer_id, product_id, qty, placed_on, status, note)
  SELECT (SELECT MIN(id) FROM crm.customers), (SELECT MIN(id) FROM products), 1 + seq % 3000,
  '2026-01-01' + INTERVAL seq % 300 DAY, 'NEW', REPEAT('o', 60) FROM seq_1_to_30000"
auto=""
t0=$(date +%s)
while [[ -z $auto && $(($(date +%s) - t0)) -lt 900 ]]; do
  q=""
  for _ in $(seq 1 40); do q+="SELECT id, placed_on FROM orders WHERE qty = $((RANDOM % 3000 + 1));"; done
  my 13306 shop -e "$q" >/dev/null
  auto=$(my 13306 shop -e "SET @sprig_show_auto_indexes = 1; SHOW INDEX FROM orders" |
    awk '$3 ~ /^sprig_auto_/ {print $3}' | sort -u | head -1)
done
if [[ -z $auto ]]; then
  check index_built fail "no automatic index on shop.orders after 15 minutes"
  exit 1
fi
check index_built pass "$auto on shop.orders after $(($(date +%s) - t0)) s"

# 3. The tools, with the index present.
for op in info validate; do
  out=$("${DC[@]}" run --rm -T --no-deps m03-flyway "-url=jdbc:mariadb://$TARGET:13307/?useSsl=false&allowPublicKeyRetrieval=true" \
    "-user=$USER_A" "-password=$PASSWORD" "-schemas=shop,crm" "-defaultSchema=shop" "-placeholders.shop=shop" \
    "-placeholders.crm=crm" "-locations=filesystem:/flyway/sql" "-outputType=json" "-connectRetries=0" "$op" 2>>"$OUT/flyway.log") || true
  printf '%s\n' "$out" >"$OUT/flyway.$op.json"
  case $op in
    info) jq -e '[.migrations[] | select(.category == "Versioned")] | length == 6 and all(.[]; .state == "Success")' \
      >/dev/null 2>&1 <<<"$out" && check flyway_info pass "6 migrations Success" || check flyway_info fail "$(head -c 300 <<<"$out")" ;;
    validate) jq -e '.validationSuccessful == true' >/dev/null 2>&1 <<<"$out" &&
      check flyway_validate pass "validated" || check flyway_validate fail "$(head -c 300 <<<"$out")" ;;
  esac
done

"${DC[@]}" run --rm -T --no-deps -e SPRIG_CONNECTOR=hibernate-mariadb -e SPRIG_HOST="$TARGET" -e SPRIG_PORT=13307 \
  -e SPRIG_USER="$USER_A" -e SPRIG_PASSWORD="$PASSWORD" -e SPRIG_TLS=disabled -e SPRIG_PERSONA=mariadb \
  -e SPRIG_DB_PREFIX= -e SPRIG_TAG="$(printf '%s' "$USER_A" | tr -c 'A-Za-z0-9' '_')" \
  -e SPRIG_OTHER_TAG="$(printf '%s' "$USER_B" | tr -c 'A-Za-z0-9' '_')" m03-hibernate >"$OUT/hibernate.jsonl" 2>"$OUT/hibernate.log" || true
hv=$(jq -r 'select(.check == "schema_validate") | .status' "$OUT/hibernate.jsonl" 2>/dev/null | head -1)
[[ $hv == pass ]] && check hibernate_validate pass "ddl-auto=validate" ||
  check hibernate_validate fail "$(jq -c 'select(.check == "schema_validate")' "$OUT/hibernate.jsonl" 2>/dev/null | head -c 300)"

mysqldump -h 127.0.0.1 -P 13306 -u "$USER_A" "-p$PASSWORD" --ssl-mode=REQUIRED --databases shop --no-tablespaces \
  --skip-lock-tables --single-transaction >"$OUT/mysqldump-shop.sql" 2>"$OUT/mysqldump.log" &&
  check mysqldump pass "$(wc -c <"$OUT/mysqldump-shop.sql") bytes" || check mysqldump fail "$(tail -3 "$OUT/mysqldump.log")"
mysqldump -h 127.0.0.1 -P 13306 -u "$USER_A" "-p$PASSWORD" --ssl-mode=REQUIRED --no-data --no-tablespaces \
  --skip-lock-tables shop orders >"$OUT/mysqldump-orders-nodata.sql" 2>>"$OUT/mysqldump.log" ||
  check mysqldump_nodata fail "$(tail -3 "$OUT/mysqldump.log")"
"${DC[@]}" run --rm -T --no-deps --entrypoint mariadb-dump m03-mariadb-cli -h "$TARGET" -P 13307 -u "$USER_A" \
  "-p$PASSWORD" --skip-ssl --databases shop --skip-lock-tables --single-transaction >"$OUT/mariadb-dump-shop.sql" 2>"$OUT/mariadb-dump.log" &&
  check mariadb_dump pass "$(wc -c <"$OUT/mariadb-dump-shop.sql") bytes" || check mariadb_dump fail "$(tail -3 "$OUT/mariadb-dump.log")"
for p in 13306 13307; do
  my $p shop -E -e "SHOW CREATE TABLE orders" >"$OUT/show-create-$p.txt" || true
  my $p shop -e "SHOW INDEX FROM orders" >"$OUT/show-index-$p.txt" || true
  my $p -e "SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'shop' AND TABLE_NAME = 'orders'" \
    >"$OUT/statistics-$p.txt" || true
done

# 4. No sprig_auto_ anywhere; the index is still there and EXPLAIN uses it.
leaks=$(grep -l 'sprig_auto_' "$OUT"/flyway.*.json "$OUT"/hibernate.jsonl "$OUT"/*.sql "$OUT"/show-*.txt "$OUT"/statistics-*.txt 2>/dev/null || true)
if [[ -z $leaks ]]; then
  check no_sprig_auto pass "no sprig_auto_ in Flyway, Hibernate, dumps, SHOW CREATE, SHOW INDEX, STATISTICS"
else
  check no_sprig_auto fail "sprig_auto_ in: $leaks"
fi
still=$(my 13306 shop -e "SET @sprig_show_auto_indexes = 1; SHOW INDEX FROM orders" | awk '$3 ~ /^sprig_auto_/ {print $3}' | sort -u | tr '\n' ' ')
[[ $still == *"$auto"* ]] && check index_present pass "$still(shown with @sprig_show_auto_indexes = 1)" ||
  check index_present fail "the index is gone: $still"
my 13306 shop -e "EXPLAIN SELECT id, placed_on FROM orders WHERE qty = 7" >"$OUT/explain.txt" || true
grep -q "$auto" "$OUT/explain.txt" && check explain_shows_it pass "EXPLAIN uses $auto" ||
  check explain_shows_it fail "$(tr '\n' ' ' <"$OUT/explain.txt" | head -c 300)"

my 13306 -e "SPRIG RESET (auto_index.window_hours, auto_index.eval_interval_minutes, auto_index.p95_threshold_ms,
  auto_index.max_new_per_day, auto_index.rollout_pause_minutes)" || true
fails=$(jq -r 'select(.status != "pass") | .check' "$JSONL" | wc -l | tr -d ' ')
echo "drift: $(wc -l <"$JSONL" | tr -d ' ') checks, $fails failed; results in $OUT"
[[ $fails == 0 ]]
