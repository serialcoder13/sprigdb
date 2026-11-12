#!/usr/bin/env bash
# The persona-compat checks (connector `persona-sql`, chapter 28 R4): SQL
# corpora run with the `mysql` 8.4 client and compared, value for value,
# with the output of the reference server the persona stands for (golden/
# mysql: MySQL 8.4.11; golden/mariadb: MariaDB 11.8.9), captured with
# SPRIG_COMPAT_CAPTURE=1 against those servers as root.
#
# Runs in the conn-mysql-cli image with this directory mounted at /compat.
# Prints one JSON line per check, like ../cli/check.sh. Output is
# normalized only where the two can't agree by design: "at line N" and the
# client address in access-denied messages.
set -uo pipefail

TLS=$SPRIG_TLS
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
HERE=/compat
GOLDEN=$HERE/golden/$SPRIG_PERSONA
DB=$SPRIG_DATABASE
# A tenant login (`acme.app`) logs in the corpus's users as `acme.r4c_u1`.
PREFIX=
[[ $SPRIG_USER == *.* ]] && PREFIX=${SPRIG_USER%%.*}.

base_args() { # user password
  printf '[client]\npassword="%s"\n' "$2" >"$WORK/my-$1.cnf"
  ARGS=("--defaults-extra-file=$WORK/my-$1.cnf" -h "$SPRIG_HOST" -P "$SPRIG_PORT" -u "$1" -D "$DB"
        --batch --raw --skip-column-names --default-character-set=utf8mb4 --force)
  if [[ $TLS == required ]]; then ARGS+=(--ssl-mode=REQUIRED); else ARGS+=(--ssl-mode=DISABLED --get-server-public-key); fi
}

json() { sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/\\t/g' | tr '\n' ' ' | head -c 300; }

emit() { # check status detail ms
  printf '{"connector":"persona-sql","tls":"%s","check":"%s","status":"%s","detail":"%s","ms":%s}\n' \
    "$TLS" "$1" "$2" "$(printf '%s' "$3" | json)" "${4:-0}"
}

now_ms() { date +%s%3N; }

normalize() {
  sed -E -e 's/ at line [0-9]+//' -e "s/$DB/@DB@/g" -e "s/'@'[0-9a-fA-F.:]+'/'@'HOST'/g" -e "s/\`@\`[0-9a-fA-F.:]+\`/\`@\`HOST\`/g"
}

# run_as user password file: the corpus's output and errors, normalized.
run_as() {
  base_args "$1" "$2"
  sed "s/@DB@/$DB/g" "$3" | mysql "${ARGS[@]}" 2>&1 | grep -v 'Using a password' | normalize
}

admin() { run_as "$SPRIG_USER" "$SPRIG_PASSWORD" "$1"; }

# The corpus for this persona: <check>.<persona>.sql when the servers'
# syntax differs (MariaDB has one active role and other GRANT forms).
corpus() { if [[ -f $HERE/$1.$SPRIG_PERSONA.sql ]]; then echo "$HERE/$1.$SPRIG_PERSONA.sql"; else echo "$HERE/$1.sql"; fi; }

c_sql_mode() { admin "$(corpus sql_mode)"; }
c_max_execution_time() { admin "$(corpus max_execution_time)"; }
c_errors() { admin "$(corpus errors)"; }
c_show_index() { admin "$(corpus show_index)"; }
c_show_grants() { admin "$(corpus show_grants)"; }
c_roles() {
  printf 'GRANT r4c_r2 TO r4c_u1;\n' >"$WORK/regrant.sql"
  admin "$WORK/regrant.sql"
  # Always over TLS: a first plaintext login with caching_sha2_password
  # needs the RSA exchange, which the gateway doesn't offer (chapter 05).
  local tls=$TLS
  TLS=required
  run_as "${PREFIX}r4c_u1" 'Sprig-dev-1!' "$(corpus roles)"
  TLS=$tls
  admin $HERE/cleanup.sql
}
c_gis() { admin "$(corpus gis)"; }
c_stored_mode() { admin "$(corpus stored_mode)"; }
c_group_by() { admin "$(corpus group_by)"; }
# Result metadata: each column's name and type (--column-type-info).
c_int_meta() {
  base_args "$SPRIG_USER" "$SPRIG_PASSWORD"
  sed "s/@DB@/$DB/g" "$(corpus int_meta)" | mysql "${ARGS[@]}" --column-type-info --table 2>&1 |
    grep -E '^(Field +[0-9]+:|Type:|ERROR)' | paste - - | sed -E 's/Field +[0-9]+: +//; s/[[:space:]]+Type: +/ /'
}
c_mysql_compat() { admin "$(corpus mysql_compat)"; }
# R4 round 3: generated NOT NULL, default collations, LATERAL, functional
# key parts; and per-session status counters (a fresh login of its own).
c_mysql_ddl() { admin "$(corpus mysql_ddl)"; }
c_session_status() { admin "$(corpus session_status)"; }
# R4 round 4: performance_schema and sys (3306 only).
c_perf_schema() { admin "$(corpus perf_schema)"; }
# R4 round 4: LATERAL and rollup inside views and stored programs; MySQL's
# printing of expressions in SHOW CREATE TABLE (3306 only).
c_mysql_stored() { admin "$(corpus mysql_stored)"; }
# R4 round 5: JSON operators, mixed functional key parts and their
# printing, a descending index over a foreign key, lateral aggregate
# names, stored code as written, USE performance_schema, statement
# digests (3306 only).
c_mysql_json_index() { admin "$(corpus mysql_json_index)"; }
# R4 round 6: MySQL's answers to DDL that MariaDB answers differently
# (gaps rows 67 and 78: collation names, foreign key errors, long names,
# MyISAM's descending parts, DEFAULT (now()), escaped defaults, a column
# of a multi-column unique index, KILL QUERY of the session's own
# statement, stored programs' character sets; 3306 only).
c_mysql_ddl_answers() { admin "$(corpus mysql_ddl_answers)"; }

CHECKS=${SPRIG_COMPAT_CHECKS:-"sql_mode max_execution_time errors show_index show_grants roles gis stored_mode group_by int_meta mysql_compat mysql_ddl session_status perf_schema mysql_stored mysql_json_index mysql_ddl_answers"}
for check in $CHECKS; do
  if [[ $check == gis && $SPRIG_PERSONA != mysql ]]; then
    emit gis n/a "MySQL's SRID 4326 semantics are the MySQL persona's only"
    continue
  fi
  if [[ $check == mysql_stored && $SPRIG_PERSONA != mysql ]]; then
    emit mysql_stored n/a "MySQL's LATERAL and rollup rules and expression printing are 3306's"
    continue
  fi
  if [[ $check == mysql_json_index && $SPRIG_PERSONA != mysql ]]; then
    emit mysql_json_index n/a "MySQL's JSON operators, functional key parts and performance_schema are 3306's"
    continue
  fi
  if [[ $check == mysql_ddl_answers && $SPRIG_PERSONA != mysql ]]; then
    emit mysql_ddl_answers n/a "MySQL's DDL answers are 3306's; 3307 keeps MariaDB's"
    continue
  fi
  if [[ $check == perf_schema && $SPRIG_PERSONA != mysql ]]; then
    emit perf_schema n/a "3306 answers performance_schema and sys; 3307 relays MariaDB's"
    continue
  fi
  if [[ $check == session_status && $SPRIG_PERSONA != mysql ]]; then
    # The gateway counts by MySQL's rules on both ports; MariaDB also
    # counts SHOW STATUS as a SELECT (decisions/r4-interim.md Z4).
    emit session_status n/a "counted by MySQL's rules on both ports"
    continue
  fi
  t0=$(now_ms)
  "c_$check" >"$WORK/$check.out"
  t1=$(now_ms)
  if [[ ${SPRIG_COMPAT_CAPTURE:-0} == 1 ]]; then
    cp "$WORK/$check.out" "/capture/$check.out"
    emit "$check" pass "captured $(wc -l <"$WORK/$check.out") lines" $((t1 - t0))
  elif [[ ! -f $GOLDEN/$check.out ]]; then
    emit "$check" fail "no golden output $GOLDEN/$check.out" $((t1 - t0))
  else
    # Known, documented differences are taken out of both sides first.
    sed -f "$HERE/known.$SPRIG_PERSONA.sed" "$GOLDEN/$check.out" >"$WORK/$check.want"
    sed -i -f "$HERE/known.$SPRIG_PERSONA.sed" "$WORK/$check.out"
  fi
  if [[ ${SPRIG_COMPAT_CAPTURE:-0} == 1 || ! -f $GOLDEN/$check.out ]]; then
    :
  elif [[ "$(cat "$WORK/$check.want")" == "$(cat "$WORK/$check.out")" ]]; then
    emit "$check" pass "$(wc -l <"$WORK/$check.out" | tr -d ' ') lines equal to the reference" $((t1 - t0))
  else
    # The first differing lines (the image has no diff).
    want=() got=()
    while IFS= read -r l; do want+=("$l"); done <"$WORK/$check.want"
    while IFS= read -r l; do got+=("$l"); done <"$WORK/$check.out"
    detail=
    for ((i = 0; i < ${#want[@]} || i < ${#got[@]}; i++)); do
      if [[ ${want[i]-} != "${got[i]-}" ]]; then
        detail="line $((i + 1)): want <${want[i]-}> got <${got[i]-}>"
        break
      fi
    done
    emit "$check" fail "$detail" $((t1 - t0))
  fi
done
exit 0
