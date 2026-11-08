#!/usr/bin/env bash
# M0.3 schema-tool suite, CLI part (../README.md). Runs inside the connector
# matrix's CLI images (`mysql` + `mysqldump` 8.4, or `mariadb` +
# `mariadb-dump` 11.8) for both tenants, and prints one JSON line per check:
#
#   {"suite":"dump","tenant":"acme_app","check":"restore","status":"pass","detail":"...","ms":120}
#
#   suite.sh login       a TLS login per tenant: shows who the endpoint thinks
#                        it is, and fills caching_sha2_password's cache so
#                        plaintext logins (the leak suite's) need no RSA key
#   suite.sh reset       drop each tenant's suite databases (a clean start)
#   suite.sh seed        each tenant's own rows, private table and database
#   suite.sh history     Flyway's history: equal checksums for both tenants,
#                        installed_by never a backend account
#   suite.sh dump        dump, inspect and restore each tenant's databases
#   suite.sh isolation   SHOW, information_schema and SELECT, tenant by tenant
#
# Several steps may be given; they run in order.
#
# Environment (run.sh sets it): M03_HOST, M03_PORT, M03_TLS (disabled |
# required), M03_MODE (gateway | baseline), M03_USER_A, M03_USER_B,
# M03_PASSWORD, M03_BACKEND_USERS (backend accounts that must never appear
# as a DEFINER; default "root").
#
# Names: a tenant's tag is its user name with every other character than
# [A-Za-z0-9] turned into `_` (acme.app -> acme_app). Through the gateway
# both tenants use the same logical names (shop, crm, shop_restored) plus
# vault_<tag>; in baseline mode, where both "tenants" are plain users of one
# MariaDB, every database name is prefixed with "<tag>_" and MariaDB grants
# keep the users apart.
set -uo pipefail

[[ $# -gt 0 ]] || { echo "usage: suite.sh reset|seed|history|dump|isolation..." >&2; exit 2; }
MODE=${M03_MODE:-gateway}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
BACKEND_USERS=${M03_BACKEND_USERS:-root}

if command -v mysql >/dev/null; then
  CLI=mysql DUMP=mysqldump
  if [[ ${M03_TLS:-disabled} == required ]]; then TLS_ARGS=(--ssl-mode=REQUIRED); else TLS_ARGS=(--ssl-mode=DISABLED --get-server-public-key); fi
  # COLUMN_STATISTICS is the translator's (M0.5); GTIDs and tablespaces need
  # privileges a tenant doesn't have.
  DUMP_OPTS=(--single-transaction --routines --triggers --hex-blob --no-tablespaces --column-statistics=0 --set-gtid-purged=OFF)
else
  CLI=mariadb DUMP=mariadb-dump
  if [[ ${M03_TLS:-disabled} == required ]]; then TLS_ARGS=(--ssl --skip-ssl-verify-server-cert); else TLS_ARGS=(--skip-ssl); fi
  DUMP_OPTS=(--single-transaction --routines --triggers --hex-blob --no-tablespaces)
fi
printf '[client]\npassword="%s"\n' "$M03_PASSWORD" >"$WORK/my.cnf"

tag_of() { printf '%s' "$1" | tr -c 'A-Za-z0-9' '_'; }
prefix_of() { if [[ $MODE == baseline ]]; then printf '%s_' "$(tag_of "$1")"; fi; }

# The physical-name detector of chapter 07 (sprig_sql::names::physical_name_leaks):
# `t<digits>_` not preceded by a letter or digit, up to the end of the identifier.
leaks() { grep -a -o -E '(^|[^A-Za-z0-9])t[0-9]+_[A-Za-z0-9_$]*' "$1" | sed -E 's/^[^t]//' | sort -u | tr '\n' ' '; }

emit() { # suite tenant check status detail ms
  local d
  d=$(printf '%s' "$5" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/ /g' | tr '\n' ' ' | head -c 400)
  printf '{"suite":"%s","tenant":"%s","check":"%s","status":"%s","detail":"%s","ms":%s}\n' "$1" "$2" "$3" "$4" "$d" "${6:-0}"
}

now_ms() { date +%s%3N; }

# sql <user> [client args...] <<< statements; stdout tab-separated, stderr in $WORK/err.
sql() {
  local user=$1
  shift
  "$CLI" "--defaults-extra-file=$WORK/my.cnf" -h "$M03_HOST" -P "$M03_PORT" -u "$user" "${TLS_ARGS[@]}" \
    --default-character-set=utf8mb4 --batch --raw --skip-column-names "$@" 2>"$WORK/err"
}

dump() { # <user> [dump args...]
  local user=$1
  shift
  "$DUMP" "--defaults-extra-file=$WORK/my.cnf" -h "$M03_HOST" -P "$M03_PORT" -u "$user" "${TLS_ARGS[@]}" \
    --default-character-set=utf8mb4 "${DUMP_OPTS[@]}" "$@" 2>"$WORK/err"
}

err() { grep -v -i 'using a password' "$WORK/err" | tr '\n' ' ' | head -c 300; }

# Runs check function $3 for suite $1, tenant $2; the function prints its
# detail and returns 0 (pass), 1 (fail) or 3 (n/a).
run() {
  local t0 detail rc status
  t0=$(now_ms)
  : >"$WORK/err"
  detail=$("$3" 2>&1)
  rc=$?
  case $rc in 0) status=pass ;; 3) status=n/a ;; *) status=fail ;; esac
  emit "$1" "$2" "${3#c_}" "$status" "$detail" $(($(now_ms) - t0))
  return $rc
}

# Fails (gateway) or reports n/a (baseline) when file $1 holds a physical name.
check_leaks() {
  local found
  found=$(leaks "$1")
  if [[ -n $found ]]; then
    echo "physical names: $found"
    [[ $MODE == baseline ]] && return 3
    return 1
  fi
  [[ $MODE == baseline ]] && { echo "n/a in baseline (no physical names exist; detector found none in $(wc -c <"$1" | tr -d ' ') bytes)"; return 3; }
  echo "no physical name in $(wc -c <"$1" | tr -d ' ') bytes"
}

# The tenant being checked and the other one.
set_tenant() { # <user> <other user>
  U=$1 O=$2
  T=$(tag_of "$U") OT=$(tag_of "$O")
  P=$(prefix_of "$U") OP=$(prefix_of "$O")
  SHOP=${P}shop CRM=${P}crm RESTORED=${P}shop_restored VAULT=${P}vault_$T
  OSHOP=${OP}shop OVAULT=${OP}vault_$OT
}

# --- login, reset and seed -----------------------------------------------------

c_login() {
  local tls out
  if [[ $CLI == mysql ]]; then tls=(--ssl-mode=REQUIRED); else tls=(--ssl --skip-ssl-verify-server-cert); fi
  out=$("$CLI" "--defaults-extra-file=$WORK/my.cnf" -h "$M03_HOST" -P "$M03_PORT" -u "$U" "${tls[@]}" --batch --raw \
          --skip-column-names -e "SELECT CURRENT_USER(), VERSION(), @@version_comment" 2>"$WORK/err") || { err; return 1; }
  echo "TLS login: ${out//$'\t'/, }"
}

c_reset() {
  sql "$U" <<<"DROP DATABASE IF EXISTS \`$RESTORED\`; DROP DATABASE IF EXISTS \`$SHOP\`;
DROP DATABASE IF EXISTS \`$CRM\`; DROP DATABASE IF EXISTS \`$VAULT\`;" || { err; return 1; }
  echo "dropped $SHOP, $CRM, $RESTORED, $VAULT if they existed"
}

c_seed() {
  sql "$U" <<SQL || { err; return 1; }
INSERT INTO $CRM.customers (email, name) VALUES ('$T@m03.test', 'Tenant $T');
CALL $SHOP.place_order('$T@m03.test', 'SKU-2', 3);
CREATE TABLE $SHOP.only_$T (id INT PRIMARY KEY, secret_$T VARCHAR(64) NOT NULL) ENGINE = InnoDB;
INSERT INTO $SHOP.only_$T VALUES (1, '$T-secret');
CREATE DATABASE $VAULT;
CREATE TABLE $VAULT.secrets (id INT PRIMARY KEY, v VARCHAR(64) NOT NULL) ENGINE = InnoDB;
INSERT INTO $VAULT.secrets VALUES (1, '$T-vault-secret');
SQL
  echo "customer $T@m03.test and an order, table $SHOP.only_$T, database $VAULT"
}

# Flyway's history table as each tenant sees it: the same six versions with
# the same checksums, installed by the tenant's own account.
c_history() {
  local mine theirs who b
  mine=$(sql "$U" <<<"SELECT version, checksum, success FROM \`$SHOP\`.flyway_schema_history WHERE version IS NOT NULL ORDER BY installed_rank") || { err; return 1; }
  theirs=$(sql "$O" <<<"SELECT version, checksum, success FROM \`$OSHOP\`.flyway_schema_history WHERE version IS NOT NULL ORDER BY installed_rank") || { err; return 1; }
  [[ $(wc -l <<<"$mine") == 6 ]] || { echo "want 6 versions, got: ${mine//$'\n'/; }"; return 1; }
  [[ $mine == "$theirs" ]] || { echo "differs from $OT's: [${mine//$'\n'/; }] vs [${theirs//$'\n'/; }]"; return 1; }
  who=$(sql "$U" <<<"SELECT DISTINCT installed_by FROM \`$SHOP\`.flyway_schema_history") || { err; return 1; }
  for b in $BACKEND_USERS; do
    grep -q -x -F "$b" <<<"$who" && { echo "installed_by is the backend account $b"; return 1; }
  done
  echo "6 versions, checksums equal to $OT's ($(cut -f2 <<<"$mine" | tr '\n' ' ')), installed_by ${who//$'\n'/ }"
}

# --- dump ---------------------------------------------------------------------

# The tenant's databases, with CREATE DATABASE and USE: every name the dump
# writes must be logical, and only this tenant's.
c_dump_databases() {
  dump "$U" --databases "$SHOP" "$CRM" "$VAULT" >"$WORK/dbs.sql" || { echo "$DUMP failed: $(err)"; return 1; }
  local f=$WORK/dbs.sql bad="" got want
  got=$(grep -a -o -E '^CREATE DATABASE /\*!32312 IF NOT EXISTS\*/ `[^`]+`' "$f" | sed -E 's/.*`(.*)`/\1/' | sort | tr '\n' ' ')
  want=$(printf '%s\n' "$SHOP" "$CRM" "$VAULT" | sort | tr '\n' ' ')
  [[ $got == "$want" ]] || bad+="databases [$got] want [$want]; "
  got=$(grep -a -o -E '^CREATE TABLE `[^`]+`' "$f" | sed -E 's/.*`(.*)`/\1/' | sort | tr '\n' ' ')
  want=$(printf '%s\n' customers flyway_schema_history "only_$T" orders products secrets | sort | tr '\n' ' ')
  [[ $got == "$want" ]] || bad+="tables [$got] want [$want]; "
  grep -a -q -E 'VIEW `order_totals`' "$f" || bad+="no view order_totals; "
  grep -a -q -E 'PROCEDURE `place_order`' "$f" || bad+="no procedure place_order; "
  grep -a -q -E "REFERENCES \`$CRM\`\\.\`customers\`" "$f" || bad+="no cross-database FK to \`$CRM\`.\`customers\`; "
  grep -a -q -F "$T@m03.test" "$f" || bad+="own row $T@m03.test missing; "
  grep -a -q -F "$T-vault-secret" "$f" || bad+="own vault row missing; "
  for x in "$OT@m03.test" "$OT-secret" "only_$OT" "vault_$OT"; do
    grep -a -q -F "$x" "$f" && bad+="the other tenant's $x is in the dump; "
  done
  [[ -z $bad ]] || { echo "$bad"; return 1; }
  echo "$("$DUMP" --version | sed -E 's/ +/ /g; s/ for .*//' | cut -c1-40), $(wc -c <"$f" | tr -d ' ') bytes: 3 databases, 6 tables, view, procedure, cross-database FK, own rows only"
}

c_dump_leaks() {
  [[ -s $WORK/dbs.sql ]] || { echo "no dump"; return 1; }
  check_leaks "$WORK/dbs.sql"
}

# DEFINER clauses name the tenant's account, never a backend one.
c_dump_definers() {
  [[ -s $WORK/dbs.sql ]] || { echo "no dump"; return 1; }
  local defs b
  defs=$(grep -a -o -E 'DEFINER=`[^`]*`@`[^`]*`' "$WORK/dbs.sql" | sort -u | tr '\n' ' ')
  for b in $BACKEND_USERS; do
    [[ $defs == *"DEFINER=\`$b\`@"* ]] && { echo "a backend account is a DEFINER: $defs"; return 1; }
  done
  echo "definers: ${defs:-none}"
}

# Everything the tenant can see: only its own databases.
c_dump_all() {
  dump "$U" --all-databases >"$WORK/all.sql" || { echo "$DUMP --all-databases failed: $(err)"; return 1; }
  local f=$WORK/all.sql dbs bad=
  dbs=$(grep -a -o -E '^CREATE DATABASE /\*!32312 IF NOT EXISTS\*/ `[^`]+`' "$f" | sed -E 's/.*`(.*)`/\1/' | sort | tr '\n' ' ')
  for x in "$SHOP" "$CRM" "$VAULT"; do [[ " $dbs " == *" $x "* ]] || bad+="$x missing; "; done
  [[ " $dbs " == *" $OVAULT "* ]] && bad+="the other tenant's $OVAULT is there; "
  for x in "$OT@m03.test" "$OT-secret" "$OT-vault-secret" "only_$OT"; do
    grep -a -q -F "$x" "$f" && bad+="the other tenant's $x is in the dump; "
  done
  if [[ $MODE == gateway ]]; then
    local l
    l=$(leaks "$f")
    [[ -z $l ]] || bad+="physical names: $l; "
  fi
  [[ -z $bad ]] || { echo "$bad (databases: $dbs)"; return 1; }
  echo "databases: $dbs($(wc -c <"$f" | tr -d ' ') bytes)"
}

# The shop database alone (no CREATE DATABASE or USE), restored into a new
# logical database of the same tenant, then compared.
c_restore() {
  dump "$U" "$SHOP" >"$WORK/shop.sql" || { echo "$DUMP $SHOP failed: $(err)"; return 1; }
  sql "$U" <<<"DROP DATABASE IF EXISTS \`$RESTORED\`; CREATE DATABASE \`$RESTORED\`" || { err; return 1; }
  sql "$U" -D "$RESTORED" <"$WORK/shop.sql" || { echo "restore failed: $(grep -a -m1 '^ERROR' "$WORK/err")"; return 1; }
  local t a b bad=
  for t in products orders "only_$T" flyway_schema_history; do
    a=$(sql "$U" <<<"SELECT COUNT(*) FROM \`$SHOP\`.\`$t\`") || { err; return 1; }
    b=$(sql "$U" <<<"SELECT COUNT(*) FROM \`$RESTORED\`.\`$t\`") || { err; return 1; }
    [[ $a == "$b" ]] || bad+="$t: $a vs $b rows; "
    a=$(sql "$U" -D "$SHOP" <<<"SHOW CREATE TABLE \`$t\`") || { err; return 1; }
    b=$(sql "$U" -D "$RESTORED" <<<"SHOW CREATE TABLE \`$t\`") || { err; return 1; }
    [[ $a == "$b" ]] || bad+="SHOW CREATE TABLE $t differs; "
  done
  a=$(sql "$U" <<<"SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = '$RESTORED';
SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '$RESTORED';
SELECT COUNT(*) FROM \`$RESTORED\`.order_totals") || { err; return 1; }
  [[ ${a%%$'\n'*} == 1 ]] || bad+="procedure missing in $RESTORED; "
  [[ $(sed -n 2p <<<"$a") == 1 ]] || bad+="view missing in $RESTORED; "
  [[ -z $bad ]] || { echo "$bad"; return 1; }
  echo "into $RESTORED: 4 tables with equal counts and SHOW CREATE TABLE, the procedure and the view ($(sed -n 3p <<<"$a") rows)"
}

# --- isolation ----------------------------------------------------------------

# Every isolation answer is also scanned for physical names (gateway mode).
scan_into() { cat >>"$WORK/iso.out"; }

c_show_databases() {
  local out bad=
  out=$(sql "$U" <<<"SHOW DATABASES") || { err; return 1; }
  printf '%s\n' "$out" | scan_into
  for x in "$SHOP" "$CRM" "$VAULT"; do grep -q -x -F "$x" <<<"$out" || bad+="$x missing; "; done
  grep -q -x -F "$OVAULT" <<<"$out" && bad+="the other tenant's $OVAULT is listed; "
  [[ -z $bad ]] || { echo "$bad (got: ${out//$'\n'/ })"; return 1; }
  echo "${out//$'\n'/ }"
}

c_show_tables() {
  local out want
  out=$(sql "$U" <<<"SHOW TABLES FROM \`$SHOP\`") || { err; return 1; }
  printf '%s\n' "$out" | scan_into
  out=$(sort <<<"$out" | tr '\n' ' ')
  want=$(printf '%s\n' flyway_schema_history "only_$T" order_totals orders products | sort | tr '\n' ' ')
  [[ $out == "$want" ]] || { echo "SHOW TABLES FROM $SHOP: [$out] want [$want]"; return 1; }
  echo "$SHOP: $out"
}

c_information_schema() {
  local out bad=
  out=$(sql "$U" <<<"SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES
 WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys') ORDER BY 1, 2") || { err; return 1; }
  printf '%s\n' "$out" | scan_into
  grep -q -P "^\Q$SHOP\E\tonly_$T\t" <<<"$out" || bad+="$SHOP.only_$T missing from TABLES; "
  grep -q -F "only_$OT" <<<"$out" && bad+="the other tenant's only_$OT in TABLES; "
  grep -q -F "vault_$OT" <<<"$out" && bad+="the other tenant's vault in TABLES; "
  # (the dump step's restore put a copy of only_<tag> into shop_restored)
  out=$(sql "$U" <<<"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS
 WHERE COLUMN_NAME LIKE 'secret%' ORDER BY 1, 2, 3") || { err; return 1; }
  printf '%s\n' "$out" | scan_into
  grep -q -P "^\Q$SHOP\E\tonly_$T\tsecret_$T\$" <<<"$out" || bad+="COLUMNS: $SHOP.only_$T.secret_$T missing; "
  grep -q -F "secret_$OT" <<<"$out" && bad+="the other tenant's column secret_$OT in COLUMNS; "
  out=$(sql "$U" <<<"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY 1") || { err; return 1; }
  printf '%s\n' "$out" | scan_into
  grep -q -x -F "$OVAULT" <<<"$out" && bad+="the other tenant's $OVAULT in SCHEMATA; "
  out=$(sql "$U" <<<"SELECT CONSTRAINT_SCHEMA, TABLE_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME
 FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = 'fk_orders_customer' AND CONSTRAINT_SCHEMA = '$SHOP'") || { err; return 1; }
  printf '%s\n' "$out" | scan_into
  [[ $out == "$SHOP"$'\t'orders$'\t'"$CRM"$'\t'customers ]] || bad+="KEY_COLUMN_USAGE fk_orders_customer: [${out//$'\n'/; }]; "
  [[ -z $bad ]] || { echo "$bad"; return 1; }
  echo "TABLES, COLUMNS, SCHEMATA and KEY_COLUMN_USAGE show only this tenant's objects; the cross-database FK reads $SHOP.orders -> $CRM.customers"
}

c_select_rows() {
  local out bad=
  out=$(sql "$U" <<<"SELECT email FROM \`$CRM\`.customers ORDER BY email;
SELECT secret_$T FROM \`$SHOP\`.only_$T; SELECT v FROM \`$VAULT\`.secrets;
SELECT email, sku, total FROM \`$SHOP\`.order_totals ORDER BY id") || { err; return 1; }
  printf '%s\n' "$out" | scan_into
  grep -q -x -F "$T@m03.test" <<<"$out" || bad+="own customer missing; "
  grep -q -F "$OT" <<<"$out" && bad+="the other tenant's rows are visible; "
  grep -q -x -F "$T-secret" <<<"$out" || bad+="own secret missing; "
  [[ -z $bad ]] || { echo "$bad (got: ${out//$'\n'/; })"; return 1; }
  echo "$(grep -c '@' <<<"$out") rows with e-mails, own secrets, none of $OT's"
}

# The other tenant's names, as this tenant would type them: each must fail.
c_other_tenant() {
  local s bad="" n=0 errs=""
  for s in "USE \`$OVAULT\`" "SELECT * FROM \`$OVAULT\`.secrets" "SHOW TABLES FROM \`$OVAULT\`" \
           "SELECT * FROM \`$OSHOP\`.only_$OT" "SELECT * FROM \`$SHOP\`.only_$OT" \
           "SELECT secret_$OT FROM \`$SHOP\`.only_$T"; do
    n=$((n + 1))
    if out=$(sql "$U" <<<"$s"); then
      bad+="'$s' succeeded (${out//$'\n'/; }); "
    else
      errs+="$(grep -a -o -E '^ERROR [0-9]+' "$WORK/err" | cut -d' ' -f2) "
      cat "$WORK/err" | scan_into
    fi
  done
  [[ -z $bad ]] || { echo "$bad"; return 1; }
  echo "$n statements naming the other tenant's objects all failed (errors: $errs)"
}

c_isolation_leaks() {
  [[ -s $WORK/iso.out ]] || { echo "nothing captured"; return 1; }
  check_leaks "$WORK/iso.out"
}

# --- main ---------------------------------------------------------------------

for STEP in "$@"; do
for pair in "$M03_USER_A $M03_USER_B" "$M03_USER_B $M03_USER_A"; do
  # shellcheck disable=SC2086 # two words
  set_tenant $pair
  case $STEP in
    login) run setup "$T" c_login ;;
    reset) run setup "$T" c_reset ;;
    seed) run setup "$T" c_seed ;;
    history) run flyway "$T" c_history ;;
    dump)
      run dump "$T" c_dump_databases
      run dump "$T" c_dump_leaks
      run dump "$T" c_dump_definers
      run dump "$T" c_dump_all
      run dump "$T" c_restore
      ;;
    isolation)
      : >"$WORK/iso.out"
      for c in c_show_databases c_show_tables c_information_schema c_select_rows c_other_tenant c_isolation_leaks; do
        run isolation "$T" "$c"
      done
      ;;
    *) echo "unknown step $STEP" >&2; exit 2 ;;
  esac
done
done
exit 0
