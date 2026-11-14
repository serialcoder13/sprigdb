#!/usr/bin/env bash
# Connector check for the `mysql` (8.4) and `mariadb` command-line clients.
#
# Runs the fixed script described in ../README.md and prints one JSON line
# per check on stdout. Configuration comes from SPRIG_* environment
# variables (see ../run.sh); SPRIG_CONNECTOR is "mysql-cli" or "mariadb-cli".
#
# The CLIs speak only the text protocol, so `prepared` uses SQL PREPARE and
# EXECUTE, and `last_insert_id` reads LAST_INSERT_ID() (a CLI doesn't show
# the OK packet's insert id). `multi_statement` sends two statements in one
# COM_QUERY by switching the client's delimiter to `$$`.
set -uo pipefail

# The dump tools ship in the same images: mysqldump with mysql, mariadb-dump with mariadb.
case $SPRIG_CONNECTOR in mysqldump | mariadb-dump) exec /dump.sh ;; esac

CONNECTOR=$SPRIG_CONNECTOR
TLS=$SPRIG_TLS
PREFIX="t_${CONNECTOR//-/_}_${TLS}"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# The password goes in an option file, which must be the first argument.
printf '[client]\npassword="%s"\n' "$SPRIG_PASSWORD" >"$WORK/my.cnf"
ARGS=("--defaults-extra-file=$WORK/my.cnf" -h "$SPRIG_HOST" -P "$SPRIG_PORT" -u "$SPRIG_USER" -D "$SPRIG_DATABASE"
      --batch --raw --skip-column-names --default-character-set=utf8mb4 --max-allowed-packet=256M
      "--init-command=SET time_zone = '+00:00'")
if [[ $CONNECTOR == mysql-cli ]]; then
  CLI=mysql
  if [[ $TLS == required ]]; then ARGS+=(--ssl-mode=REQUIRED); else ARGS+=(--ssl-mode=DISABLED --get-server-public-key); fi
else
  CLI=mariadb
  # --ssl without verification: the servers' certificates are self-signed.
  if [[ $TLS == required ]]; then ARGS+=(--ssl --skip-ssl-verify-server-cert); else ARGS+=(--skip-ssl); fi
fi

json() { # json-escape stdin
  sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/\\t/g' | tr '\n' ' ' | head -c 300
}

emit() { # check status detail ms
  printf '{"connector":"%s","tls":"%s","check":"%s","status":"%s","detail":"%s","ms":%s}\n' \
    "$CONNECTOR" "$TLS" "$1" "$2" "$(printf '%s' "$3" | json)" "${4:-0}"
}

# sql [extra client args...] < statements: stdout to stdout, errors to $WORK/err.
sql() {
  "$CLI" "${ARGS[@]}" "$@" 2>"$WORK/err" | cat
  return "${PIPESTATUS[0]}"
}

fail() { echo "$*" >"$WORK/why"; return 1; }

now_ms() { date +%s%3N; }

run() { # name function
  local name=$1 t0 t1 detail rc
  t0=$(now_ms)
  : >"$WORK/why"
  detail=$("$2" 2>>"$WORK/why")
  rc=$?
  t1=$(now_ms)
  case $rc in
    0) emit "$name" pass "$detail" $((t1 - t0)) ;;
    2) emit "$name" n/a "$detail" ;;
    3) emit "$name" skip "$detail" ;;
    *) emit "$name" fail "$(cat "$WORK/why" "$WORK/err" 2>/dev/null)" $((t1 - t0)) ;;
  esac
}

COLS="c_tiny, c_small, c_int, c_big, c_ubig, c_dec, c_float, c_double, c_vchar, c_text, HEX(c_vbin), HEX(c_blob), c_date, c_dt, c_ts, c_time, c_json"
DDL="(
  id INT PRIMARY KEY,
  c_tiny TINYINT, c_small SMALLINT, c_int INT, c_big BIGINT, c_ubig BIGINT UNSIGNED,
  c_dec DECIMAL(20,6), c_float FLOAT, c_double DOUBLE,
  c_vchar VARCHAR(64), c_text TEXT, c_vbin VARBINARY(16), c_blob BLOB,
  c_date DATE, c_dt DATETIME(6), c_ts TIMESTAMP NULL, c_time TIME, c_json JSON
) DEFAULT CHARSET = utf8mb4"
VCHAR='héllo wörld 🌱'
TEXT='Ünïcödé 日本語 🚀🌱 '\''quoted'\'' "double" back\slash'
TEXT_SQL='Ünïcödé 日本語 🚀🌱 \'\''quoted\'\'' "double" back\\slash'
JSON_TEXT='{"a": 1, "b": [true, null], "s": "🌱"}'
VBIN=00FF807F0A0D27225C
BLOB=$(for i in $(seq 0 255); do printf '%02X' "$i"; done)
EXPECTED_ROW=$(printf '%s\t' -128 32767 -2147483648 9223372036854775807 18446744073709551615 \
  -12345678901234.567890 1.5 -0.00225 "$VCHAR" "$TEXT" "$VBIN" "$BLOB" 2024-02-29 \
  '2024-02-29 23:59:58.123456' '2024-02-29 12:34:56' 838:59:59 "$JSON_TEXT")
EXPECTED_ROW=${EXPECTED_ROW%$'\t'}
NULL_ROW=$(printf 'NULL\t%.0s' $(seq 17))
NULL_ROW=${NULL_ROW%$'\t'}

compare_rows() { # output-file
  local want
  want=$(printf '%s\n%s' "$EXPECTED_ROW" "$NULL_ROW")
  if [[ $(cat "$1") != "$want" ]]; then
    diff <(tr '\t' '\n' <<<"$want") <(tr '\t' '\n' <"$1") | grep '^[<>]' | head -6 | tr '\n' ' ' >&2
    return 1
  fi
}

c_connect() {
  local out
  out=$(sql <<<"SHOW SESSION STATUS LIKE 'Ssl_cipher'") || return 1
  local cipher=${out#Ssl_cipher}
  cipher=${cipher//[[:space:]]/}
  if [[ $TLS == required && -z $cipher ]]; then fail "TLS required but the session isn't encrypted"; return; fi
  if [[ $TLS != required && -n $cipher ]]; then fail "TLS disabled but the session is encrypted ($cipher)"; return; fi
  echo "cipher=${cipher:-none}, $("$CLI" --version | head -c 80)"
}

c_plain_query() {
  local out
  out=$(sql <<<"SELECT 1 + 1, 'sprig', VERSION()") || return 1
  [[ $out == $'2\tsprig\t'* ]] || { fail "got $out"; return; }
  echo "server ${out##*$'\t'}"
}

c_types() {
  local t=${PREFIX}_types
  sql <<SQL >"$WORK/out" || return 1
DROP TABLE IF EXISTS $t;
CREATE TABLE $t $DDL;
INSERT INTO $t VALUES (1, -128, 32767, -2147483648, 9223372036854775807, 18446744073709551615,
  -12345678901234.567890, 1.5, -0.00225, '$VCHAR', '$TEXT_SQL', X'$VBIN', X'$BLOB', '2024-02-29',
  '2024-02-29 23:59:58.123456', '2024-02-29 12:34:56', '838:59:59', '$JSON_TEXT'),
  (2$(printf ', NULL%.0s' $(seq 17)));
SELECT $COLS FROM $t ORDER BY id;
DROP TABLE $t;
SQL
  compare_rows "$WORK/out" || return 1
  echo "17 columns, 2 rows"
}

c_prepared() {
  local t=${PREFIX}_ps
  sql <<SQL >"$WORK/out" || return 1
DROP TABLE IF EXISTS $t;
CREATE TABLE $t $DDL;
PREPARE ins FROM 'INSERT INTO $t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
SET @id = 10, @a = -128, @b = 32767, @c = -2147483648, @d = 9223372036854775807,
    @e = 18446744073709551615, @f = -12345678901234.567890, @g = 1.5, @h = -0.00225e0,
    @i = '$VCHAR', @j = '$TEXT_SQL', @k = X'$VBIN', @l = X'$BLOB', @m = '2024-02-29',
    @n = '2024-02-29 23:59:58.123456', @o = '2024-02-29 12:34:56', @p = '838:59:59', @q = '$JSON_TEXT';
EXECUTE ins USING @id, @a, @b, @c, @d, @e, @f, @g, @h, @i, @j, @k, @l, @m, @n, @o, @p, @q;
SET @id = 11, @a = NULL, @b = NULL, @c = NULL, @d = NULL, @e = NULL, @f = NULL, @g = NULL, @h = NULL,
    @i = NULL, @j = NULL, @k = NULL, @l = NULL, @m = NULL, @n = NULL, @o = NULL, @p = NULL, @q = NULL;
EXECUTE ins USING @id, @a, @b, @c, @d, @e, @f, @g, @h, @i, @j, @k, @l, @m, @n, @o, @p, @q;
DEALLOCATE PREPARE ins;
PREPARE sel FROM 'SELECT $COLS FROM $t WHERE id = ?';
SET @id = 10;
EXECUTE sel USING @id;
SET @id = 11;
EXECUTE sel USING @id;
DEALLOCATE PREPARE sel;
DROP TABLE $t;
SQL
  compare_rows "$WORK/out" || return 1
  echo "SQL PREPARE/EXECUTE (text protocol), 2 executions each"
}

c_transaction() {
  local t=${PREFIX}_tx out
  out=$(sql <<SQL
DROP TABLE IF EXISTS $t;
CREATE TABLE $t (id INT PRIMARY KEY) ENGINE = InnoDB;
START TRANSACTION;
INSERT INTO $t VALUES (1);
ROLLBACK;
SELECT COUNT(*) FROM $t;
START TRANSACTION;
INSERT INTO $t VALUES (2);
COMMIT;
SQL
  ) || return 1
  [[ $out == 0 ]] || { fail "row survived rollback ($out)"; return; }
  out=$(sql <<<"SELECT COUNT(*) FROM $t WHERE id = 2; DROP TABLE $t;") || return 1
  [[ $out == 1 ]] || { fail "committed row not visible to another session ($out)"; return; }
}

c_multi_statement() {
  local out
  out=$(sql '--delimiter=$$' <<<"SELECT 1 AS a; SELECT 2 AS b, 'two' AS c\$\$") || return 1
  [[ $out == $'1\n2\ttwo' ]] || { fail "got $out"; return; }
  echo "2 resultsets in one COM_QUERY"
}

c_duplicate_key() {
  local t=${PREFIX}_dup
  sql <<<"DROP TABLE IF EXISTS $t; CREATE TABLE $t (id INT PRIMARY KEY); INSERT INTO $t VALUES (1);" || return 1
  if sql <<<"INSERT INTO $t VALUES (1)"; then fail "no error raised"; return; fi
  grep -q '^ERROR 1062 (23000)' "$WORK/err" || { fail "got $(head -c 200 "$WORK/err")"; return; }
  sql <<<"DROP TABLE $t" || return 1
  echo "1062/23000"
}

c_last_insert_id() {
  local t=${PREFIX}_ai out
  out=$(sql <<SQL
DROP TABLE IF EXISTS $t;
CREATE TABLE $t (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT = 100;
INSERT INTO $t (v) VALUES (1);
SELECT LAST_INSERT_ID();
INSERT INTO $t (v) VALUES (2), (3), (4);
SELECT LAST_INSERT_ID();
DROP TABLE $t;
SQL
  ) || return 1
  [[ $out == $'100\n101' ]] || { fail "LAST_INSERT_ID() gave $out"; return; }
  echo "LAST_INSERT_ID() only"
}

round_trip() { # size label [client args...]
  local size=$1 t=${PREFIX}_$2
  shift 2
  head -c "$size" /dev/urandom >"$WORK/data"
  od -An -v -tx1 "$WORK/data" | tr -d ' \n' | tr a-f A-F >"$WORK/hex"
  {
    printf 'DROP TABLE IF EXISTS %s; CREATE TABLE %s (id INT PRIMARY KEY, b LONGBLOB);\n' "$t" "$t"
    printf "INSERT INTO %s VALUES (1, X'" "$t"
    cat "$WORK/hex"
    printf "');\n"
  } >"$WORK/upload.sql"
  sql "$@" <"$WORK/upload.sql" || return 1
  local sha
  sha=$(sql "$@" <<<"SELECT SHA2(b, 256) FROM $t WHERE id = 1") || return 1
  [[ $sha == "$(sha256sum "$WORK/data" | cut -d' ' -f1)" ]] || { fail "server-side hash differs: the upload was corrupted"; return; }
  # HEX doubles the row, so the download is a 2x-sized row packet.
  sql "$@" <<<"SELECT HEX(b) FROM $t WHERE id = 1" | tr -d '\n' >"$WORK/down" || return 1
  [[ $(sha256sum <"$WORK/hex") == "$(sha256sum <"$WORK/down")" ]] || { fail "download differs ($(wc -c <"$WORK/down") hex chars, want $((size * 2)))"; return; }
  sql "$@" <<<"DROP TABLE $t" || return 1
  echo "$size bytes"
}

c_large_row() { round_trip $((4 * 1024 * 1024)) big; }

c_multi_packet() {
  local limit size=$((16 * 1024 * 1024 + 1024))
  limit=$(sql <<<"SELECT @@max_allowed_packet") || return 1
  # The upload is a hex literal: twice the size.
  ((limit >= 2 * size + 1024)) || { fail "server max_allowed_packet $limit is too small for $size bytes"; return; }
  round_trip "$size" mp
}

c_compression() {
  local out
  out=$(sql --compress <<<"SHOW SESSION STATUS LIKE 'Compression'; SELECT LENGTH(REPEAT('a', 1000000))") || return 1
  [[ $out == $'Compression\tON\n1000000' ]] || { fail "got $out"; return; }
  echo "Compression=ON, $(round_trip $((1024 * 1024)) zip --compress)"
}

c_conn_attrs() {
  local out
  out=$(sql <<<"SELECT @@performance_schema") || return 1
  [[ $out == 1 ]] || { echo "performance_schema is off on the server"; return 3; }
  # The CLIs can't add attributes; check the client library's own.
  out=$(sql <<<"SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs WHERE PROCESSLIST_ID = CONNECTION_ID() AND ATTR_NAME = '_client_name'") || return 1
  [[ -n $out ]] || { fail "_client_name attribute missing"; return; }
  echo "_client_name=$out"
}

# --- protocol feature checks --------------------------------------------------

PERSONA=${SPRIG_PERSONA:-mariadb}
INFILE_ROWS='1=héllo 🌱|2=quoted, comma|3=NULL'
LOADED="SELECT IFNULL(GROUP_CONCAT(CONCAT(id, '=', IFNULL(v, 'NULL')) ORDER BY id SEPARATOR '|'), '') FROM"

infile_setup() { # table: creates the table and $WORK/data.csv; exit 3 if the server has local_infile off
  printf '1,héllo 🌱\n2,"quoted, comma"\n3,\\N\n' >"$WORK/data.csv"
  local out
  out=$(sql <<<"DROP TABLE IF EXISTS $1; CREATE TABLE $1 (id INT PRIMARY KEY, v VARCHAR(32)) DEFAULT CHARSET = utf8mb4; SELECT @@GLOBAL.local_infile") || return 1
  [[ $out == 0 ]] && { echo "local_infile is off on the server"; return 3; }
  return 0
}

load_sql() { # table
  printf "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\\\n'" "$WORK/data.csv" "$1"
}

c_local_infile() {
  local t=${PREFIX}_infile out rc
  infile_setup "$t"; rc=$?; ((rc == 0)) || return $rc
  out=$(sql --local-infile=1 <<<"$(load_sql "$t"); $LOADED $t; DROP TABLE $t;") || return 1
  [[ $out == "$INFILE_ROWS" ]] || { fail "loaded: $out"; return; }
  echo "3 rows (--local-infile=1)"
}

c_local_infile_off() {
  local t=${PREFIX}_infile_off out rc want=4166
  [[ $PERSONA == mysql ]] && want=3948
  infile_setup "$t"; rc=$?; ((rc == 0)) || return $rc
  if sql --local-infile=0 <<<"$(load_sql "$t")" >/dev/null; then fail "the load succeeded"; return; fi
  local how
  how=$(head -c 200 "$WORK/err")
  if [[ $how =~ ^ERROR\ (1148|3948|4166) ]] && [[ ${BASH_REMATCH[1]} != "$want" ]]; then
    fail "server refused with $how, want $want"; return
  fi
  out=$(sql <<<"$LOADED $t; DROP TABLE $t;") || return 1
  [[ -z $out ]] || { fail "rows were loaded: $out"; return; }
  echo "refused: $how"
}

c_call() {
  local p=${PREFIX}_proc out
  out=$(sql <<SQL
DROP PROCEDURE IF EXISTS $p;
DELIMITER //
CREATE PROCEDURE $p(IN a INT, OUT b INT, INOUT c INT)
BEGIN
  SELECT a AS x;
  SELECT a + 1 AS y, 'two' AS z UNION ALL SELECT a + 2, 'three';
  SET b = a * 10;
  SET c = c + 1;
END//
DELIMITER ;
SET @b = NULL, @c = 5;
CALL $p(1, @b, @c);
SELECT @b, @c;
DROP PROCEDURE $p;
SQL
  ) || return 1
  [[ $out == $'1\n2\ttwo\n3\tthree\n10\t6' ]] || { fail "got $out"; return; }
  echo "text CALL: 2 resultsets, OUT 10, INOUT 6"
}

c_change_user() { echo "the $CLI client has no change-user command"; return 2; }

c_reset_connection() {
  [[ $CONNECTOR == mysql-cli ]] || { echo "the mariadb client has no resetconnection command"; return 2; }
  local out
  # --force: the last statement must fail (the temporary table is gone).
  out=$(sql --force <<'SQL'
SET @sprig_mark = 42, SESSION auto_increment_increment = 7;
CREATE TEMPORARY TABLE sprig_tmp (id INT);
SELECT CONNECTION_ID();
resetconnection
SELECT CONNECTION_ID(), IFNULL(@sprig_mark, 'NULL'), @@SESSION.auto_increment_increment;
SELECT COUNT(*) FROM sprig_tmp;
SQL
  )
  local id=${out%%$'\n'*}
  [[ $out == "$id"$'\n'"$id"$'\tNULL\t1' ]] || { fail "session not reset: $out"; return; }
  grep -q '^ERROR 1146' "$WORK/err" || { fail "temporary table probe: $(head -c 200 "$WORK/err")"; return; }
  echo "resetconnection (COM_RESET_CONNECTION): same connection $id; user variable, session variable and temporary table cleared"
}

c_pool_reprepare() { echo "the $CLI client has no pool or server-side prepared statements"; return 2; }

# lock_holder table seconds: a background session that holds row 1 FOR UPDATE
# for that long; returns once the lock is taken.
lock_holder() {
  local t=$1
  sql <<<"DROP TABLE IF EXISTS $t; CREATE TABLE $t (id INT PRIMARY KEY, v INT) ENGINE = InnoDB; INSERT INTO $t VALUES (1, 0), (2, 0);" || return 1
  : >"$WORK/holder.out"
  "$CLI" "${ARGS[@]}" --unbuffered >"$WORK/holder.out" 2>"$WORK/holder.err" <<<"START TRANSACTION; SELECT 'locked', v FROM $t WHERE id = 1 FOR UPDATE; DO SLEEP($2); COMMIT;" &
  HOLDER=$!
  local i
  for i in $(seq 100); do
    grep -q locked "$WORK/holder.out" && return 0
    sleep 0.1
  done
  fail "the holder never got the lock: $(cat "$WORK/holder.err")"
}

c_lock_wait() {
  local t=${PREFIX}_lock out t0 ms
  lock_holder "$t" 2.5 || return 1
  t0=$(now_ms)
  out=$(sql --force <<SQL
SET SESSION innodb_lock_wait_timeout = 1;
START TRANSACTION;
SELECT v FROM $t WHERE id = 2 FOR UPDATE;
SELECT v FROM $t WHERE id = 1 FOR UPDATE;
UPDATE $t SET v = 2 WHERE id = 2;
COMMIT;
SQL
  )
  ms=$(($(now_ms) - t0))
  wait "$HOLDER"
  grep -q '^ERROR 1205' "$WORK/err" || { fail "got $(head -c 200 "$WORK/err")"; return; }
  ((ms >= 900 && ms < 10000)) || { fail "timed out after $ms ms"; return; }
  out=$(sql <<<"SELECT v FROM $t WHERE id = 2; DROP TABLE $t;") || return 1
  [[ $out == 2 ]] || { fail "the waiting transaction lost its work ($out)"; return; }
  echo "$(grep -o '^ERROR 1205 ([0-9A-Z]*)' "$WORK/err") after about $ms ms (with connect); the transaction survived"
}

c_lock_nowait() {
  local t=${PREFIX}_lock out want=1205
  [[ $PERSONA == mysql ]] && want=3572
  lock_holder "$t" 1 || return 1
  out=$(sql --force <<SQL
START TRANSACTION;
SELECT v FROM $t WHERE id = 1 FOR UPDATE NOWAIT;
SELECT id FROM $t ORDER BY id FOR UPDATE SKIP LOCKED;
ROLLBACK;
SQL
  )
  wait "$HOLDER"
  grep -q "^ERROR $want" "$WORK/err" || { fail "NOWAIT: got $(head -c 200 "$WORK/err"), want $want"; return; }
  [[ $out == 2 ]] || { fail "SKIP LOCKED returned $out"; return; }
  sql <<<"DROP TABLE $t" || return 1
  echo "NOWAIT $(grep -o "^ERROR $want ([0-9A-Z]*)" "$WORK/err"), SKIP LOCKED returned row 2"
}

CONNECTED=
SKIPPED=" ${SPRIG_CONN_SKIP_CHECKS:-} "; SKIPPED=${SKIPPED//,/ }
for check in connect plain_query types prepared transaction multi_statement duplicate_key \
             last_insert_id large_row multi_packet compression conn_attrs local_infile local_infile_off \
             call change_user reset_connection pool_reprepare lock_wait lock_nowait; do
  [[ $SKIPPED == *" $check "* ]] && continue
  if [[ $check != connect && -z $CONNECTED ]]; then
    emit "$check" fail "no connection"
    continue
  fi
  line=$(run "$check" "c_$check")
  echo "$line"
  [[ $check == connect && $line == *'"status":"pass"'* ]] && CONNECTED=1
done
exit 0
