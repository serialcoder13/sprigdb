"""Connector check for PyMySQL and mysqlclient (MySQLdb).

Runs the fixed script described in ../README.md and prints one JSON line per
check on stdout. Configuration comes from SPRIG_* environment variables (see
../run.sh); SPRIG_CONNECTOR picks the driver: "pymysql" or "mysqlclient".
"""

import datetime
import decimal
import hashlib
import json
import os
import sys
import time
import traceback

CONNECTOR = os.environ["SPRIG_CONNECTOR"]
HOST = os.environ["SPRIG_HOST"]
PORT = int(os.environ["SPRIG_PORT"])
USER = os.environ["SPRIG_USER"]
PASSWORD = os.environ["SPRIG_PASSWORD"]
DATABASE = os.environ["SPRIG_DATABASE"]
TLS = os.environ["SPRIG_TLS"]  # required | disabled
PERSONA = os.environ.get("SPRIG_PERSONA", "mariadb")
PREFIX = "t_%s_%s" % (CONNECTOR.replace("-", "_"), TLS)
# Checks run.sh asked to leave out (--quick): not run, not reported.
SKIPPED = set(os.environ.get("SPRIG_CONN_SKIP_CHECKS", "").replace(",", " ").split())

if CONNECTOR.startswith("sqlalchemy-"):
    # SQLAlchemy 2 over either driver: a script of its own in the same image.
    import runpy

    runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqla.py"), run_name="__main__")
    sys.exit(0)

if CONNECTOR == "pymysql":
    import pymysql as db

    MULTI = db.constants.CLIENT.MULTI_STATEMENTS
else:
    import MySQLdb as db
    from MySQLdb.constants import CLIENT

    MULTI = CLIENT.MULTI_STATEMENTS


class NA(Exception):
    """The connector can't do this; reported as n/a."""


class Skip(Exception):
    """The server can't show it; reported as skip."""


def emit(check, status, detail="", ms=0.0):
    rec = {"connector": CONNECTOR, "tls": TLS, "check": check, "status": status,
           "detail": detail[:300], "ms": round(ms, 1)}
    print(json.dumps(rec, ensure_ascii=False), flush=True)


def run(check, fn):
    t0 = time.monotonic()
    try:
        detail = fn() or ""
        emit(check, "pass", detail, (time.monotonic() - t0) * 1000)
    except NA as e:
        emit(check, "n/a", str(e))
    except Skip as e:
        emit(check, "skip", str(e))
    except Exception as e:  # noqa: BLE001 - every failure is a result
        tb = traceback.format_exc().strip().splitlines()[-1]
        emit(check, "fail", "%s: %s | %s" % (type(e).__name__, e, tb), (time.monotonic() - t0) * 1000)


def expect(cond, msg):
    if not cond:
        raise AssertionError(msg)


def connect(compress=False, attrs=False, **extra):
    kw = dict(host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE,
              charset="utf8mb4", autocommit=True, client_flag=MULTI, **extra)
    if CONNECTOR == "pymysql":
        kw["max_allowed_packet"] = 64 * 1024 * 1024
        if TLS == "required":
            # Encrypt without verifying the self-signed certificate.
            kw["ssl"] = {"check_hostname": False}
        else:
            kw["ssl_disabled"] = True
        if attrs:
            kw["program_name"] = "sprig-conn-pymysql"
        if compress:
            kw["compress"] = True
    else:
        kw["ssl_mode"] = "REQUIRED" if TLS == "required" else "DISABLED"
        if compress:
            kw["compress"] = True
    c = db.connect(**kw)
    with c.cursor() as cur:
        cur.execute("SET time_zone = '+00:00'")
    return c


def q(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


# --- the shared data set ---------------------------------------------------

COLS = ["c_tiny", "c_small", "c_int", "c_big", "c_ubig", "c_dec", "c_float", "c_double",
        "c_vchar", "c_text", "c_vbin", "c_blob", "c_date", "c_dt", "c_ts", "c_time", "c_json"]
DDL = """CREATE TABLE {t} (
  id INT PRIMARY KEY,
  c_tiny TINYINT, c_small SMALLINT, c_int INT, c_big BIGINT, c_ubig BIGINT UNSIGNED,
  c_dec DECIMAL(20,6), c_float FLOAT, c_double DOUBLE,
  c_vchar VARCHAR(64), c_text TEXT, c_vbin VARBINARY(16), c_blob BLOB,
  c_date DATE, c_dt DATETIME(6), c_ts TIMESTAMP NULL, c_time TIME, c_json JSON
) DEFAULT CHARSET = utf8mb4"""
BLOB = bytes(range(256))
VBIN = bytes([0x00, 0xFF, 0x80, 0x7F, 0x0A, 0x0D, 0x27, 0x22, 0x5C])
VCHAR = "héllo wörld 🌱"
TEXT = "Ünïcödé 日本語 🚀🌱 'quoted' \"double\" back\\slash"
JSON_TEXT = '{"a": 1, "b": [true, null], "s": "🌱"}'
# Canonical text of every column of row 1.
EXPECTED = {
    "c_tiny": "-128", "c_small": "32767", "c_int": "-2147483648",
    "c_big": "9223372036854775807", "c_ubig": "18446744073709551615",
    "c_dec": "-12345678901234.567890", "c_float": "1.5", "c_double": "-0.00225",
    "c_vchar": VCHAR, "c_text": TEXT, "c_vbin": VBIN.hex(), "c_blob": BLOB.hex(),
    "c_date": "2024-02-29", "c_dt": "2024-02-29 23:59:58.123456",
    "c_ts": "2024-02-29 12:34:56", "c_time": "838:59:59", "c_json": JSON_TEXT,
}
INSERT_LITERAL = (
    "INSERT INTO {t} VALUES (1, -128, 32767, -2147483648, 9223372036854775807, "
    "18446744073709551615, -12345678901234.567890, 1.5, -0.00225, %s, %s, X'%s', X'%s', "
    "'2024-02-29', '2024-02-29 23:59:58.123456', '2024-02-29 12:34:56', '838:59:59', %s), "
    "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
    "NULL, NULL, NULL)"
)


def canon(col, v):
    """A driver value as the canonical text of EXPECTED."""
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        if col in ("c_vbin", "c_blob"):
            return bytes(v).hex()
        v = bytes(v).decode("utf-8")
    if isinstance(v, float):
        return repr(v) if col == "c_double" else repr(float(v))
    if isinstance(v, datetime.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f") if col == "c_dt" else v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, datetime.timedelta):
        s = int(v.total_seconds())
        sign = "-" if s < 0 else ""
        s = abs(s)
        return "%s%d:%02d:%02d" % (sign, s // 3600, s // 60 % 60, s % 60)
    if isinstance(v, decimal.Decimal):
        return str(v)
    return str(v)


def compare_row(row, expected):
    bad = []
    for col, v in zip(COLS, row):
        want = expected[col]
        got = canon(col, v)
        if col == "c_json" and got is not None and want is not None:
            ok = json.loads(got) == json.loads(want)
        elif col in ("c_float", "c_double") and got is not None and want is not None:
            ok = float(got) == float(want)
        else:
            ok = got == want
        if not ok:
            bad.append("%s: got %r want %r" % (col, got, want))
    expect(not bad, "; ".join(bad))


# --- checks ------------------------------------------------------------------

conn = None


def c_connect():
    global conn
    conn = connect()
    cipher = q(conn, "SHOW SESSION STATUS LIKE 'Ssl_cipher'")[0][1]
    cipher = cipher.decode() if isinstance(cipher, bytes) else cipher
    if TLS == "required":
        expect(cipher, "TLS required but the session isn't encrypted")
    else:
        expect(not cipher, "TLS disabled but the session is encrypted (%s)" % cipher)
    return "cipher=%s" % (cipher or "none")


def c_plain_query():
    rows = q(conn, "SELECT 1 + 1, 'sprig', VERSION()")
    expect(rows[0][0] == 2 and rows[0][1] == "sprig", "got %r" % (rows,))
    return "server %s" % rows[0][2]


def c_types():
    t = PREFIX + "_types"
    q(conn, "DROP TABLE IF EXISTS " + t)
    q(conn, DDL.format(t=t))
    sql = INSERT_LITERAL.replace("X'%s', X'%s'", "X'" + VBIN.hex() + "', X'" + BLOB.hex() + "'")
    q(conn, sql.format(t=t), (VCHAR, TEXT, JSON_TEXT))
    rows = q(conn, "SELECT %s FROM %s ORDER BY id" % (", ".join(COLS), t))
    expect(len(rows) == 2, "want 2 rows, got %d" % len(rows))
    compare_row(rows[0], EXPECTED)
    compare_row(rows[1], {c: None for c in COLS})
    q(conn, "DROP TABLE " + t)
    return "17 columns, 2 rows"


def c_prepared():
    raise NA("%s has no binary-protocol prepared statements (client-side interpolation only)" % CONNECTOR)


def c_transaction():
    t = PREFIX + "_tx"
    q(conn, "DROP TABLE IF EXISTS " + t)
    q(conn, "CREATE TABLE %s (id INT PRIMARY KEY) ENGINE = InnoDB" % t)
    conn.begin()
    q(conn, "INSERT INTO %s VALUES (1)" % t)
    conn.rollback()
    expect(q(conn, "SELECT COUNT(*) FROM " + t)[0][0] == 0, "row survived rollback")
    conn.begin()
    q(conn, "INSERT INTO %s VALUES (2)" % t)
    conn.commit()
    other = connect()
    n = q(other, "SELECT COUNT(*) FROM %s WHERE id = 2" % t)[0][0]
    other.close()
    expect(n == 1, "committed row not visible to another session")
    q(conn, "DROP TABLE " + t)


def c_multi_statement():
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS a; SELECT 2 AS b, 'two' AS c")
        first = cur.fetchall()
        expect(cur.nextset(), "no second result")
        second = cur.fetchall()
        expect(not cur.nextset(), "unexpected third result")
    expect(first == ((1,),) and second == ((2, "two"),), "got %r %r" % (first, second))
    return "2 resultsets"


def c_duplicate_key():
    t = PREFIX + "_dup"
    q(conn, "DROP TABLE IF EXISTS " + t)
    q(conn, "CREATE TABLE %s (id INT PRIMARY KEY)" % t)
    q(conn, "INSERT INTO %s VALUES (1)" % t)
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO %s VALUES (1)" % t)
    except db.IntegrityError as e:
        errno = e.args[0]
        if CONNECTOR == "pymysql":
            sqlstate = getattr(e, "sqlstate", None)
        else:
            # Read before anything else talks to the server.
            sqlstate = conn.sqlstate()
        cur.close()
        expect(errno == 1062, "errno %r" % errno)
        expect(sqlstate == "23000", "sqlstate %r" % sqlstate)
        q(conn, "DROP TABLE " + t)
        return "1062/23000"
    raise AssertionError("no error raised")


def c_last_insert_id():
    t = PREFIX + "_ai"
    q(conn, "DROP TABLE IF EXISTS " + t)
    q(conn, "CREATE TABLE %s (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT = 100" % t)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO %s (v) VALUES (1)" % t)
        first = cur.lastrowid
        cur.execute("INSERT INTO %s (v) VALUES (2), (3), (4)" % t)
        multi = cur.lastrowid
    lid = q(conn, "SELECT LAST_INSERT_ID()")[0][0]
    expect(first == 100, "first insert id %r" % first)
    expect(multi == 101 and lid == 101, "multi-row insert id %r, LAST_INSERT_ID() %r" % (multi, lid))
    q(conn, "DROP TABLE " + t)


def round_trip(c, size, label):
    t = PREFIX + "_" + label
    q(c, "DROP TABLE IF EXISTS " + t)
    q(c, "CREATE TABLE %s (id INT PRIMARY KEY, b LONGBLOB)" % t)
    data = os.urandom(size)
    q(c, "INSERT INTO %s VALUES (1, %%s)" % t, (data,))
    got, sha = q(c, "SELECT b, SHA2(b, 256) FROM %s WHERE id = 1" % t)[0]
    sha = sha.decode() if isinstance(sha, bytes) else sha
    expect(sha == hashlib.sha256(data).hexdigest(), "server-side hash differs: the upload was corrupted")
    expect(bytes(got) == data, "download differs (%d bytes, want %d)" % (len(got), size))
    q(c, "DROP TABLE " + t)
    return "%d bytes" % size


def c_large_row():
    return round_trip(conn, 4 * 1024 * 1024, "big")


def c_multi_packet():
    limit = q(conn, "SELECT @@max_allowed_packet")[0][0]
    size = 16 * 1024 * 1024 + 1024
    expect(limit >= 2 * size + 1024, "server max_allowed_packet %d is too small for %d bytes" % (limit, size))
    return round_trip(conn, size, "mp")


def c_compression():
    try:
        c = connect(compress=True)
    except (NotImplementedError, TypeError, ValueError) as e:
        raise NA("no compressed protocol: %s" % e)
    status = q(c, "SHOW SESSION STATUS LIKE 'Compression'")[0][1]
    status = status.decode() if isinstance(status, bytes) else status
    expect(status == "ON", "Compression status %r" % status)
    n = q(c, "SELECT LENGTH(REPEAT('a', 1000000))")[0][0]
    expect(n == 1000000, "got %r" % n)
    detail = round_trip(c, 1024 * 1024, "zip")
    c.close()
    return "Compression=ON, " + detail


def c_conn_attrs():
    if CONNECTOR == "pymysql":
        c, want = connect(attrs=True), ("program_name", "sprig-conn-pymysql")
    else:
        # mysqlclient has no API to add attributes; libmariadb sends its own.
        c, want = connect(), ("_client_name", None)
    try:
        if int(q(c, "SELECT @@performance_schema")[0][0]) != 1:
            raise Skip("performance_schema is off on the server")
        rows = q(c, "SELECT ATTR_NAME, ATTR_VALUE FROM performance_schema.session_connect_attrs "
                    "WHERE PROCESSLIST_ID = CONNECTION_ID()")
    finally:
        c.close()
    attrs = {canon("", k): canon("", v) for k, v in rows}
    expect(want[0] in attrs and (want[1] is None or attrs[want[0]] == want[1]),
           "%s not in %r" % (want[0], attrs))
    return "%s=%s" % (want[0], attrs[want[0]])


# --- protocol feature checks --------------------------------------------------

INFILE = '1,héllo 🌱\n2,"quoted, comma"\n3,\\N\n'
INFILE_ROWS = "1=héllo 🌱|2=quoted, comma|3=NULL"


def load_sql(t, path):
    return ("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s CHARACTER SET utf8mb4 "
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n'" % (path, t))


def loaded(c, t):
    v = q(c, "SELECT IFNULL(GROUP_CONCAT(CONCAT(id, '=', IFNULL(v, 'NULL')) ORDER BY id SEPARATOR '|'), '') "
             "FROM " + t)[0][0]
    return canon("", v)


def infile_table(c, t):
    q(c, "DROP TABLE IF EXISTS " + t)
    q(c, "CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(32)) DEFAULT CHARSET = utf8mb4" % t)
    if int(q(c, "SELECT @@GLOBAL.local_infile")[0][0]) == 0:
        raise Skip("local_infile is off on the server")


def infile_file():
    import tempfile

    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(INFILE)
    return path


def c_local_infile():
    t, path = PREFIX + "_infile", infile_file()
    c = connect(local_infile=True)
    try:
        infile_table(c, t)
        with c.cursor() as cur:
            n = cur.execute(load_sql(t, path))
        rows = loaded(c, t)
        expect(n == 3 and rows == INFILE_ROWS, "loaded %r rows: %r" % (n, rows))
        q(c, "DROP TABLE " + t)
        return "3 rows (local_infile=True)"
    finally:
        c.close()
        os.remove(path)


def infile_refusal():
    """The server's refusal when the client doesn't offer CLIENT_LOCAL_FILES."""
    return 3948 if PERSONA == "mysql" else 4166


def c_local_infile_off():
    t, path = PREFIX + "_infile_off", infile_file()
    c = connect(local_infile=False)
    try:
        infile_table(c, t)
        how = None
        try:
            q(c, load_sql(t, path))
        except db.Error as e:
            errno = e.args[0] if e.args and isinstance(e.args[0], int) else 0
            how = "%s %s" % (errno, e.args[1] if len(e.args) > 1 else e)
            if errno in (1148, 3948, 4166):
                expect(errno == infile_refusal(), "server refused with %s, want %d" % (how, infile_refusal()))
        expect(how, "the load succeeded")
        rows = loaded(c, t)
        expect(rows == "", "rows were loaded: %r" % rows)
        q(c, "DROP TABLE " + t)
        return "refused: " + how
    finally:
        c.close()
        os.remove(path)


PROC = """CREATE PROCEDURE {p}(IN a INT, OUT b INT, INOUT c INT)
BEGIN
  SELECT a AS x;
  SELECT a + 1 AS y, 'two' AS z UNION ALL SELECT a + 2, 'three';
  SET b = a * 10;
  SET c = c + 1;
END"""


def c_call():
    p = PREFIX + "_proc"
    q(conn, "DROP PROCEDURE IF EXISTS " + p)
    q(conn, PROC.format(p=p))
    q(conn, "SET @b = NULL, @c = 5")
    sets = []
    with conn.cursor() as cur:
        cur.execute("CALL %s(1, @b, @c)" % p)
        while True:
            if cur.description:
                sets.append(";".join(",".join(canon("", v) for v in r) for r in cur.fetchall()))
            if not cur.nextset():
                break
    b, c = q(conn, "SELECT @b, @c")[0]
    q(conn, "DROP PROCEDURE " + p)
    expect(sets == ["1", "2,two;3,three"], "resultsets %r" % sets)
    expect((b, c) == (10, 6), "OUT b=%r INOUT c=%r" % (b, c))
    return "text CALL: 2 resultsets, OUT 10, INOUT 6"


def mark_session(c):
    q(c, "SET @sprig_mark = 42, SESSION auto_increment_increment = 7")
    q(c, "CREATE TEMPORARY TABLE sprig_tmp (id INT)")
    return q(c, "SELECT CONNECTION_ID()")[0][0]


def assert_reset(c, cid):
    got = q(c, "SELECT CONNECTION_ID(), @sprig_mark, @@SESSION.auto_increment_increment")[0]
    state = "id %s, @sprig_mark %r, auto_increment_increment %s" % got
    expect(got[0] == cid, "connection id changed from %s: %s" % (cid, state))
    expect(got[1] is None and got[2] == 1, "session not reset: " + state)
    try:
        q(c, "SELECT COUNT(*) FROM sprig_tmp")
    except db.Error as e:
        expect(e.args[0] == 1146, "temporary table probe: %r" % (e.args,))
        return "same connection %s; user variable, session variable and temporary table cleared" % cid
    raise AssertionError("the temporary table survived")


def c_change_user():
    if CONNECTOR == "pymysql":
        raise NA("PyMySQL has no change-user API")
    c = connect()
    try:
        cid = mark_session(c)
        c.change_user(USER, PASSWORD, DATABASE)
        return "change_user (COM_CHANGE_USER): " + assert_reset(c, cid)
    finally:
        c.close()


def c_reset_connection():
    raise NA("%s has no reset-connection API" % CONNECTOR)


def c_pool_reprepare():
    raise NA("%s has no server-side prepared statements (and no pool; see sqlalchemy-*)" % CONNECTOR)


def lock_table():
    t = PREFIX + "_lock"
    q(conn, "DROP TABLE IF EXISTS " + t)
    q(conn, "CREATE TABLE %s (id INT PRIMARY KEY, v INT) ENGINE = InnoDB" % t)
    q(conn, "INSERT INTO %s VALUES (1, 0), (2, 0)" % t)
    return t


def lock_pair(t):
    a, b = connect(), connect()
    a.begin()
    q(a, "SELECT v FROM %s WHERE id = 1 FOR UPDATE" % t)
    q(b, "SET SESSION innodb_lock_wait_timeout = 1")
    b.begin()
    return a, b


def errno_of(e):
    return e.args[0] if e.args and isinstance(e.args[0], int) else 0


def c_lock_wait():
    t = lock_table()
    a, b = lock_pair(t)
    try:
        q(b, "SELECT v FROM %s WHERE id = 2 FOR UPDATE" % t)
        t0 = time.monotonic()
        try:
            q(b, "SELECT v FROM %s WHERE id = 1 FOR UPDATE" % t)
            raise AssertionError("the second session got the lock")
        except db.Error as e:
            expect(errno_of(e) == 1205, "got %r" % (e.args,))
        ms = int((time.monotonic() - t0) * 1000)
        expect(900 <= ms < 10000, "timed out after %d ms" % ms)
        # Only the statement is rolled back: the transaction still holds row 2.
        q(b, "UPDATE %s SET v = 2 WHERE id = 2" % t)
        b.commit()
        a.commit()
        expect(q(conn, "SELECT v FROM %s WHERE id = 2" % t)[0][0] == 2, "the waiting transaction lost its work")
        q(conn, "DROP TABLE " + t)
        return "1205 after %d ms; the transaction survived" % ms
    finally:
        a.close()
        b.close()


def c_lock_nowait():
    t = lock_table()
    a, b = lock_pair(t)
    want = 3572 if PERSONA == "mysql" else 1205
    try:
        try:
            q(b, "SELECT v FROM %s WHERE id = 1 FOR UPDATE NOWAIT" % t)
            raise AssertionError("NOWAIT got the lock")
        except db.Error as e:
            got = errno_of(e)
            expect(got == want, "NOWAIT: got %r, want %d" % (e.args, want))
        ids = [r[0] for r in q(b, "SELECT id FROM %s ORDER BY id FOR UPDATE SKIP LOCKED" % t)]
        expect(ids == [2], "SKIP LOCKED returned %r" % ids)
        b.rollback()
        a.rollback()
        q(conn, "DROP TABLE " + t)
        return "NOWAIT %d, SKIP LOCKED returned row 2" % got
    finally:
        a.close()
        b.close()


CHECKS = [
    ("connect", c_connect), ("plain_query", c_plain_query), ("types", c_types),
    ("prepared", c_prepared), ("transaction", c_transaction),
    ("multi_statement", c_multi_statement), ("duplicate_key", c_duplicate_key),
    ("last_insert_id", c_last_insert_id), ("large_row", c_large_row),
    ("multi_packet", c_multi_packet), ("compression", c_compression),
    ("conn_attrs", c_conn_attrs), ("local_infile", c_local_infile),
    ("local_infile_off", c_local_infile_off), ("call", c_call), ("change_user", c_change_user),
    ("reset_connection", c_reset_connection), ("pool_reprepare", c_pool_reprepare),
    ("lock_wait", c_lock_wait), ("lock_nowait", c_lock_nowait),
]


def main():
    for name, fn in CHECKS:
        if name in SKIPPED:
            continue
        if conn is None and name != "connect":
            emit(name, "fail", "no connection")
            continue
        run(name, fn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
