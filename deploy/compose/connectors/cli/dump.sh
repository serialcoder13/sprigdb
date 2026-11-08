#!/usr/bin/env bash
# Dump round trip for `mysqldump` 8.4 and `mariadb-dump` 11.8
# (SPRIG_CONNECTOR "mysqldump" or "mariadb-dump"; check.sh hands over).
#
# Builds a schema with every common type, a generated column, a foreign
# key, a trigger, a view, a procedure and a function; dumps it with the
# usual options; restores it into a new schema with the same family's
# client; and compares. Prints one JSON line per step, in the connector
# checks' format (../README.md):
#
#   source    create the source schema and its data (1,003 rows)
#   dump      <tool> --single-transaction --routines --triggers --events --hex-blob
#   restore   <client> -D <new schema> < dump
#   checksum  CHECKSUM TABLE and row counts equal for every table
#   objects   SHOW CREATE TABLE equal; the view, procedure, function and
#             trigger exist and work in the copy
set -uo pipefail

CONNECTOR=$SPRIG_CONNECTOR
TLS=$SPRIG_TLS
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
SRC="sprig_dump_${CONNECTOR//-/_}_${TLS}_src"
DST="sprig_dump_${CONNECTOR//-/_}_${TLS}_dst"
TABLES=(parent types audit)

printf '[client]\npassword="%s"\n' "$SPRIG_PASSWORD" >"$WORK/my.cnf"
ARGS=("--defaults-extra-file=$WORK/my.cnf" -h "$SPRIG_HOST" -P "$SPRIG_PORT" -u "$SPRIG_USER"
      --default-character-set=utf8mb4 --max-allowed-packet=256M)
if [[ $CONNECTOR == mysqldump ]]; then
  CLI=mysql DUMP=mysqldump
  if [[ $TLS == required ]]; then ARGS+=(--ssl-mode=REQUIRED); else ARGS+=(--ssl-mode=DISABLED --get-server-public-key); fi
else
  CLI=mariadb DUMP=mariadb-dump
  if [[ $TLS == required ]]; then ARGS+=(--ssl --skip-ssl-verify-server-cert); else ARGS+=(--skip-ssl); fi
fi

emit() { # check status detail ms
  local d
  d=$(printf '%s' "$3" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/ /g' | tr '\n' ' ' | head -c 300)
  printf '{"connector":"%s","tls":"%s","check":"%s","status":"%s","detail":"%s","ms":%s}\n' \
    "$CONNECTOR" "$TLS" "$1" "$2" "$d" "${4:-0}"
}

sql() { # [client args...] < statements
  "$CLI" "${ARGS[@]}" --batch --raw --skip-column-names "$@" 2>"$WORK/err" | cat
  return "${PIPESTATUS[0]}"
}

fail() { echo "$*" >"$WORK/why"; return 1; }
now_ms() { date +%s%3N; }

run() { # name function
  local t0 detail rc
  t0=$(now_ms)
  : >"$WORK/why"
  : >"$WORK/err"
  detail=$("$2" 2>>"$WORK/why")
  rc=$?
  if ((rc == 0)); then
    emit "$1" pass "$detail" $(($(now_ms) - t0))
  else
    emit "$1" fail "$(cat "$WORK/why" "$WORK/err" 2>/dev/null)" $(($(now_ms) - t0))
  fi
  return $rc
}

c_source() {
  sql <<SQL || return 1
DROP DATABASE IF EXISTS $SRC;
DROP DATABASE IF EXISTS $DST;
CREATE DATABASE $SRC DEFAULT CHARACTER SET utf8mb4;
USE $SRC;
SET time_zone = '+00:00';
CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL, UNIQUE KEY uq_name (name)) ENGINE = InnoDB;
CREATE TABLE types (
  id INT AUTO_INCREMENT PRIMARY KEY,
  c_tiny TINYINT, c_small SMALLINT UNSIGNED, c_med MEDIUMINT, c_int INT, c_big BIGINT, c_ubig BIGINT UNSIGNED,
  c_dec DECIMAL(20,6), c_float FLOAT, c_double DOUBLE, c_bit BIT(10), c_bool BOOLEAN,
  c_char CHAR(4), c_vchar VARCHAR(64), c_text TEXT, c_ltext LONGTEXT,
  c_bin BINARY(4), c_vbin VARBINARY(16), c_blob BLOB,
  c_enum ENUM('a', 'b', 'c'), c_set SET('x', 'y', 'z'),
  c_date DATE, c_dt DATETIME(6), c_ts TIMESTAMP(3) NULL DEFAULT NULL, c_time TIME(2), c_year YEAR,
  c_json JSON, c_geo POINT NULL, c_def VARCHAR(10) NOT NULL DEFAULT 'dflt',
  c_gen BIGINT AS (c_int * 2) VIRTUAL,
  parent_id INT NULL,
  KEY ix_vchar (c_vchar),
  CONSTRAINT fk_types_parent FOREIGN KEY (parent_id) REFERENCES parent (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'all common types';
CREATE TABLE audit (id INT AUTO_INCREMENT PRIMARY KEY, type_id INT NOT NULL, note VARCHAR(32)) ENGINE = InnoDB;
CREATE TRIGGER trg_types_ai AFTER INSERT ON types FOR EACH ROW
  INSERT INTO audit (type_id, note) VALUES (NEW.id, CONCAT('inserted ', IFNULL(NEW.c_vchar, 'NULL')));
CREATE VIEW v_types AS SELECT t.id, t.c_vchar, t.c_gen, p.name FROM types t LEFT JOIN parent p ON p.id = t.parent_id;
DELIMITER //
CREATE PROCEDURE p_count(IN min_id INT, OUT n INT)
BEGIN
  SELECT COUNT(*) INTO n FROM types WHERE id >= min_id;
  SELECT n AS counted;
END//
CREATE FUNCTION f_double(x INT) RETURNS INT DETERMINISTIC RETURN x * 2//
DELIMITER ;
INSERT INTO parent VALUES (1, 'one'), (2, 'twö 🌱');
INSERT INTO types (c_tiny, c_small, c_med, c_int, c_big, c_ubig, c_dec, c_float, c_double, c_bit, c_bool,
  c_char, c_vchar, c_text, c_ltext, c_bin, c_vbin, c_blob, c_enum, c_set, c_date, c_dt, c_ts, c_time, c_year,
  c_json, c_geo, parent_id) VALUES
 (-128, 65535, -8388608, -2147483648, -9223372036854775808, 18446744073709551615, -12345678901234.567890,
  1.5, -0.00225, b'1010101010', TRUE, 'abcd', 'héllo wörld 🌱', 'Ünïcödé 日本語 ''q'' "d" back\\\\slash\nnew line\ttab',
  REPEAT('long ', 10000), X'00FF0A0D', X'00FF807F0A0D27225C', X'000102030405FEFF', 'b', 'x,z', '2024-02-29',
  '2024-02-29 23:59:58.123456', '2024-02-29 12:34:56.789', '-838:59:59.00', 2155, '{"a": 1, "b": [true, null], "s": "🌱"}',
  ST_GeomFromText('POINT(1.5 -2.25)'), 2),
 (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 (0, 0, 0, 0, 0, 0, 0, -0.0, 1e308, b'0', FALSE, '', '', '', '', X'00000000', X'', X'', 'a', '', '1000-01-01',
  '1000-01-01 00:00:00', '1970-01-01 00:00:01', '00:00:00', 1901, '[]', ST_GeomFromText('POINT(0 0)'), 1);
INSERT INTO types (c_int, c_vchar, c_dec, c_dt, c_json, parent_id)
  WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 1000)
  SELECT n, CONCAT('row ', n), n / 7, '2024-01-01' + INTERVAL n MINUTE, JSON_OBJECT('n', n), 1 + n % 2 FROM seq;
SQL
  echo "3 tables (1,003 rows of 30 columns), trigger, view, procedure, function"
}

version() { "$1" --version | head -1 | sed -e 's/  */ /g' -e 's/ for .*//' | cut -c1-48; }

# The usual invocation. When it fails, mysqldump is retried with
# --column-statistics=0 (8.4 still reads information_schema.COLUMN_STATISTICS
# by default, which MariaDB lacks) so the round trip is still tested; the
# step itself stays failed, with both outcomes in its detail.
DUMP_OPTS=(--single-transaction --routines --triggers --events --hex-blob)
c_dump() {
  local warn
  if "$DUMP" "${ARGS[@]}" "${DUMP_OPTS[@]}" "$SRC" >"$WORK/dump.sql" 2>"$WORK/err"; then
    warn=$(grep -v -i 'using a password' "$WORK/err" | tr '\n' ' ' | head -c 150)
    echo "$(version "$DUMP"), $(wc -c <"$WORK/dump.sql" | tr -d ' ') bytes${warn:+; stderr: $warn}"
    return 0
  fi
  warn=$(grep -v -i 'using a password' "$WORK/err" | tr -s ' \n' ' ' | head -c 170)
  : >"$WORK/dump.sql"
  : >"$WORK/err"
  if [[ $DUMP == mysqldump ]] &&
     "$DUMP" "${ARGS[@]}" "${DUMP_OPTS[@]}" --column-statistics=0 "$SRC" >"$WORK/dump.sql" 2>"$WORK/err2"; then
    fail "the usual options fail ($warn); with --column-statistics=0 the dump works and the round trip continues"
  else
    : >"$WORK/dump.sql"
    fail "$warn"
  fi
}

c_restore() {
  sql <<<"CREATE DATABASE $DST DEFAULT CHARACTER SET utf8mb4" || return 1
  sql -D "$DST" <"$WORK/dump.sql" || return 1
  echo "into $DST with $(version "$CLI")"
}

c_checksum() {
  local t src dst bad=
  for t in "${TABLES[@]}"; do
    src=$(sql <<<"CHECKSUM TABLE $SRC.$t; SELECT COUNT(*) FROM $SRC.$t") || return 1
    dst=$(sql <<<"CHECKSUM TABLE $DST.$t; SELECT COUNT(*) FROM $DST.$t") || return 1
    src=${src#*$'\t'} dst=${dst#*$'\t'}
    [[ $src == "$dst" ]] || bad+="$t: ${src//$'\n'/ rows } vs ${dst//$'\n'/ rows }; "
  done
  [[ -z $bad ]] || { fail "differs: $bad"; return; }
  echo "${#TABLES[@]} tables equal (CHECKSUM TABLE and row counts)"
}

c_objects() {
  local t a b bad=
  for t in "${TABLES[@]}" v_types; do
    a=$(sql -D "$SRC" <<<"SHOW CREATE TABLE $t") || return 1
    b=$(sql -D "$DST" <<<"SHOW CREATE TABLE $t") || return 1
    [[ ${a//$SRC/X} == "${b//$DST/X}" ]] || bad+="$t; "
  done
  [[ -z $bad ]] || {
    diff <(sql -D "$SRC" <<<"SHOW CREATE TABLE ${bad%%;*}" | sed "s/$SRC/X/g" | tr ',' '\n') \
         <(sql -D "$DST" <<<"SHOW CREATE TABLE ${bad%%;*}" | sed "s/$DST/X/g" | tr ',' '\n') | grep '^[<>]' | head -4 >"$WORK/diff"
    fail "SHOW CREATE differs for: $bad $(cat "$WORK/diff")"; return
  }
  local out
  out=$(sql -D "$DST" <<'SQL'
SELECT COUNT(*), SUM(c_gen) FROM v_types;
CALL p_count(1000, @n);
SELECT @n, f_double(21);
INSERT INTO types (c_int, c_vchar) VALUES (5, 'after restore');
SELECT COUNT(*), MAX(note) FROM audit WHERE note = 'inserted after restore';
SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = DATABASE();
SQL
  ) || return 1
  [[ $out == $'1003\t-4293966296\n4\n4\t42\n1\tinserted after restore\n1' ]] || { fail "objects: ${out//$'\n'/ | }"; return; }
  echo "SHOW CREATE equal for 3 tables and the view; view, procedure (resultset + OUT), function and trigger work"
}

ok=1
for step in source dump restore checksum objects; do
  if [[ -z $ok ]]; then
    emit "$step" fail "an earlier step failed"
    continue
  fi
  run "$step" "c_$step" || ok=
  # A dump that only the retry produced still feeds the rest of the round trip.
  [[ $step == dump && -s $WORK/dump.sql ]] && ok=1
done
sql <<<"DROP DATABASE IF EXISTS $SRC; DROP DATABASE IF EXISTS $DST" || true
exit 0
