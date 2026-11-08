<?php
// Connector check for PHP PDO_MySQL and mysqli (both on mysqlnd).
//
// Runs the fixed script described in ../README.md and prints one JSON line
// per check on stdout. Configuration comes from SPRIG_* environment
// variables (see ../run.sh); SPRIG_CONNECTOR is "pdo_mysql" or "mysqli".

declare(strict_types=1);

$CONNECTOR = getenv('SPRIG_CONNECTOR');
$TLS = getenv('SPRIG_TLS');
$PREFIX = "t_{$CONNECTOR}_{$TLS}";
$PDO = $CONNECTOR === 'pdo_mysql';
$PERSONA = getenv('SPRIG_PERSONA') ?: 'mariadb';
// Checks run.sh asked to leave out (--quick): not run, not reported.
$SKIPPED = preg_split('/[ ,]+/', trim((string) getenv('SPRIG_CONN_SKIP_CHECKS')), -1, PREG_SPLIT_NO_EMPTY);

final class NA extends Exception {}
final class Skip extends Exception {}

function emit(string $check, string $status, string $detail = '', float $ms = 0.0): void
{
    global $CONNECTOR, $TLS;
    echo json_encode([
        'connector' => $CONNECTOR, 'tls' => $TLS, 'check' => $check, 'status' => $status,
        'detail' => mb_substr($detail, 0, 300), 'ms' => round($ms, 1),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE), "\n";
}

function expect(bool $cond, string $msg): void
{
    if (!$cond) {
        throw new RuntimeException($msg);
    }
}

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

/**
 * A new session: PDO (emulated prepares, PDO's default) or mysqli. $extra
 * holds PDO attributes or mysqli options; $persistent asks for a
 * persistent connection (PDO::ATTR_PERSISTENT, mysqli's "p:" host).
 */
function connect(bool $compress = false, array $extra = [], bool $persistent = false)
{
    global $PDO, $TLS;
    $host = getenv('SPRIG_HOST');
    $port = (int) getenv('SPRIG_PORT');
    if ($PDO) {
        $opts = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => true,
            Pdo\Mysql::ATTR_MULTI_STATEMENTS => true,
        ];
        if ($TLS === 'required') {
            // Any SSL option turns TLS on; don't verify the self-signed certificate.
            $opts[Pdo\Mysql::ATTR_SSL_CIPHER] = 'DEFAULT';
            $opts[Pdo\Mysql::ATTR_SSL_VERIFY_SERVER_CERT] = false;
        }
        if ($compress) {
            $opts[Pdo\Mysql::ATTR_COMPRESS] = true;
        }
        if ($persistent) {
            $opts[PDO::ATTR_PERSISTENT] = true;
        }
        $opts = $extra + $opts;
        $c = new PDO(sprintf('mysql:host=%s;port=%d;dbname=%s;charset=utf8mb4', $host, $port,
            getenv('SPRIG_DATABASE')), getenv('SPRIG_USER'), getenv('SPRIG_PASSWORD'), $opts);
        $c->exec("SET time_zone = '+00:00'");
        return $c;
    }
    $c = mysqli_init();
    $flags = 0;
    if ($TLS === 'required') {
        $c->ssl_set(null, null, null, null, null);
        $flags |= MYSQLI_CLIENT_SSL | MYSQLI_CLIENT_SSL_DONT_VERIFY_SERVER_CERT;
    }
    if ($compress) {
        $flags |= MYSQLI_CLIENT_COMPRESS;
    }
    foreach ($extra as $opt => $value) {
        $c->options($opt, $value);
    }
    $c->real_connect(($persistent ? 'p:' : '') . $host, getenv('SPRIG_USER'), getenv('SPRIG_PASSWORD'), getenv('SPRIG_DATABASE'),
        $port, null, $flags);
    $c->set_charset('utf8mb4');
    $c->query("SET time_zone = '+00:00'");
    return $c;
}

/** Rows of a text-protocol query, as lists. */
function rows($c, string $sql): array
{
    global $PDO;
    if ($PDO) {
        return $c->query($sql)->fetchAll(PDO::FETCH_NUM);
    }
    $r = $c->query($sql);
    if ($r === true) {
        return [];
    }
    return $r->fetch_all(MYSQLI_NUM);
}

function exec_sql($c, string $sql): void
{
    global $PDO;
    $PDO ? $c->exec($sql) : $c->query($sql);
}

function scalar($c, string $sql)
{
    return rows($c, $sql)[0][0];
}

// --- the shared data set ---------------------------------------------------

const COLS = ['c_tiny', 'c_small', 'c_int', 'c_big', 'c_ubig', 'c_dec', 'c_float', 'c_double',
    'c_vchar', 'c_text', 'c_vbin', 'c_blob', 'c_date', 'c_dt', 'c_ts', 'c_time', 'c_json'];
function ddl(string $t): string
{
    return "CREATE TABLE $t (
  id INT PRIMARY KEY,
  c_tiny TINYINT, c_small SMALLINT, c_int INT, c_big BIGINT, c_ubig BIGINT UNSIGNED,
  c_dec DECIMAL(20,6), c_float FLOAT, c_double DOUBLE,
  c_vchar VARCHAR(64), c_text TEXT, c_vbin VARBINARY(16), c_blob BLOB,
  c_date DATE, c_dt DATETIME(6), c_ts TIMESTAMP NULL, c_time TIME, c_json JSON
) DEFAULT CHARSET = utf8mb4";
}
$BLOB = implode('', array_map('chr', range(0, 255)));
$VBIN = "\x00\xFF\x80\x7F\x0A\x0D\x27\x22\x5C";
$VCHAR = 'héllo wörld 🌱';
$TEXT = 'Ünïcödé 日本語 🚀🌱 \'quoted\' "double" back\\slash';
$JSON = '{"a": 1, "b": [true, null], "s": "🌱"}';
$EXPECTED = [
    'c_tiny' => '-128', 'c_small' => '32767', 'c_int' => '-2147483648',
    'c_big' => '9223372036854775807', 'c_ubig' => '18446744073709551615',
    'c_dec' => '-12345678901234.567890', 'c_float' => '1.5', 'c_double' => '-0.00225',
    'c_vchar' => $VCHAR, 'c_text' => $TEXT, 'c_vbin' => bin2hex($VBIN), 'c_blob' => bin2hex($BLOB),
    'c_date' => '2024-02-29', 'c_dt' => '2024-02-29 23:59:58.123456',
    'c_ts' => '2024-02-29 12:34:56', 'c_time' => '838:59:59', 'c_json' => $JSON,
];

function canon(string $col, $v): ?string
{
    if ($v === null) {
        return null;
    }
    if ($col === 'c_vbin' || $col === 'c_blob') {
        return bin2hex((string) $v);
    }
    return (string) $v;
}

function compare_row(array $row, bool $nulls): void
{
    global $EXPECTED;
    $bad = [];
    foreach (COLS as $i => $col) {
        $got = canon($col, $row[$i]);
        $want = $nulls ? null : $EXPECTED[$col];
        $ok = $got === $want;
        if (!$ok && $got !== null && $want !== null) {
            if ($col === 'c_json') {
                $ok = json_decode($got, true) == json_decode($want, true);
            } elseif ($col === 'c_float' || $col === 'c_double') {
                $ok = (float) $got === (float) $want;
            }
        }
        if (!$ok) {
            $bad[] = sprintf('%s: got %s (%s) want %s', $col, json_encode($got, JSON_INVALID_UTF8_SUBSTITUTE),
                get_debug_type($row[$i]), json_encode($want));
        }
    }
    expect(!$bad, implode('; ', $bad));
}

function quote($c, string $s): string
{
    global $PDO;
    return $PDO ? $c->quote($s) : "'" . $c->real_escape_string($s) . "'";
}

// --- checks ------------------------------------------------------------------

$conn = null;

function c_connect(): string
{
    global $conn, $TLS;
    $conn = connect();
    $cipher = rows($conn, "SHOW SESSION STATUS LIKE 'Ssl_cipher'")[0][1];
    if ($TLS === 'required') {
        expect($cipher !== '', "TLS required but the session isn't encrypted");
    } else {
        expect($cipher === '', "TLS disabled but the session is encrypted ($cipher)");
    }
    return 'cipher=' . ($cipher ?: 'none');
}

function c_plain_query(): string
{
    global $conn;
    $r = rows($conn, "SELECT 1 + 1, 'sprig', VERSION()")[0];
    expect((int) $r[0] === 2 && $r[1] === 'sprig', 'got ' . json_encode($r));
    return "server {$r[2]}";
}

function c_types(): string
{
    global $conn, $PREFIX, $VCHAR, $TEXT, $VBIN, $BLOB, $JSON;
    $t = "{$PREFIX}_types";
    exec_sql($conn, "DROP TABLE IF EXISTS $t");
    exec_sql($conn, ddl($t));
    exec_sql($conn, "INSERT INTO $t VALUES (1, -128, 32767, -2147483648, 9223372036854775807, "
        . "18446744073709551615, -12345678901234.567890, 1.5, -0.00225, " . quote($conn, $VCHAR) . ', '
        . quote($conn, $TEXT) . ", X'" . bin2hex($VBIN) . "', X'" . bin2hex($BLOB) . "', '2024-02-29', "
        . "'2024-02-29 23:59:58.123456', '2024-02-29 12:34:56', '838:59:59', " . quote($conn, $JSON) . '), '
        . '(2' . str_repeat(', NULL', 17) . ')');
    // Text protocol: PDO with emulated prepares, mysqli::query.
    $rows = rows($conn, 'SELECT ' . implode(', ', COLS) . " FROM $t ORDER BY id");
    expect(count($rows) === 2, 'want 2 rows, got ' . count($rows));
    compare_row($rows[0], false);
    compare_row($rows[1], true);
    exec_sql($conn, "DROP TABLE $t");
    return '17 columns, 2 rows';
}

function c_prepared(): string
{
    global $conn, $PDO, $PREFIX, $VCHAR, $TEXT, $VBIN, $BLOB, $JSON;
    $t = "{$PREFIX}_ps";
    exec_sql($conn, "DROP TABLE IF EXISTS $t");
    exec_sql($conn, ddl($t));
    $ins = "INSERT INTO $t VALUES (?" . str_repeat(', ?', count(COLS)) . ')';
    $sel = 'SELECT ' . implode(', ', COLS) . " FROM $t WHERE id = ?";
    $values = [-128, 32767, -2147483648, PHP_INT_MAX, '18446744073709551615', '-12345678901234.567890',
        1.5, -0.00225, $VCHAR, $TEXT, $VBIN, $BLOB, '2024-02-29', '2024-02-29 23:59:58.123456',
        '2024-02-29 12:34:56', '838:59:59', $JSON];
    if ($PDO) {
        $conn->setAttribute(PDO::ATTR_EMULATE_PREPARES, false); // real COM_STMT_PREPARE
        try {
            $st = $conn->prepare($ins);
            foreach ([10 => $values, 11 => array_fill(0, count(COLS), null)] as $id => $vals) {
                $st->bindValue(1, $id, PDO::PARAM_INT);
                foreach ($vals as $i => $v) {
                    $type = match (true) {
                        $v === null => PDO::PARAM_NULL,
                        is_int($v) => PDO::PARAM_INT,
                        $i === 10 || $i === 11 => PDO::PARAM_LOB,
                        default => PDO::PARAM_STR,
                    };
                    $st->bindValue($i + 2, $v, $type);
                }
                $st->execute();
            }
            $q = $conn->prepare($sel);
            foreach ([10, 11] as $id) {
                $q->execute([$id]);
                $rows = $q->fetchAll(PDO::FETCH_NUM);
                expect(count($rows) === 1, "id $id: " . count($rows) . ' rows');
                try {
                    compare_row($rows[0], $id === 11);
                } catch (RuntimeException $e) {
                    throw new RuntimeException("id $id: " . $e->getMessage());
                }
            }
        } finally {
            $conn->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
        }
    } else {
        $st = $conn->prepare($ins);
        $id = 10;
        $v = $values;
        // Blobs go as COM_STMT_SEND_LONG_DATA ('b').
        $st->bind_param('iiiiissddssbbsssss', $id, $v[0], $v[1], $v[2], $v[3], $v[4], $v[5], $v[6], $v[7],
            $v[8], $v[9], $v[10], $v[11], $v[12], $v[13], $v[14], $v[15], $v[16]);
        $st->send_long_data(11, $VBIN);
        $st->send_long_data(12, $BLOB);
        $st->execute();
        // Re-execute the same statement with every bound variable NULL.
        $id = 11;
        foreach ($v as $i => $_) {
            $v[$i] = null;
        }
        $st->execute();
        $st->close();
        $q = $conn->prepare($sel);
        $qid = 0;
        $q->bind_param('i', $qid);
        foreach ([10, 11] as $qid) {
            $q->execute();
            $rows = $q->get_result()->fetch_all(MYSQLI_NUM);
            expect(count($rows) === 1, "id $qid: " . count($rows) . ' rows');
            try {
                compare_row($rows[0], $qid === 11);
            } catch (RuntimeException $e) {
                throw new RuntimeException("id $qid: " . $e->getMessage());
            }
        }
        $q->close();
    }
    exec_sql($conn, "DROP TABLE $t");
    return 'server-side, 2 executions each';
}

function c_transaction(): void
{
    global $conn, $PDO, $PREFIX;
    $t = "{$PREFIX}_tx";
    exec_sql($conn, "DROP TABLE IF EXISTS $t");
    exec_sql($conn, "CREATE TABLE $t (id INT PRIMARY KEY) ENGINE = InnoDB");
    $PDO ? $conn->beginTransaction() : $conn->begin_transaction();
    exec_sql($conn, "INSERT INTO $t VALUES (1)");
    $conn->rollback();
    expect((int) scalar($conn, "SELECT COUNT(*) FROM $t") === 0, 'row survived rollback');
    $PDO ? $conn->beginTransaction() : $conn->begin_transaction();
    exec_sql($conn, "INSERT INTO $t VALUES (2)");
    $conn->commit();
    $other = connect();
    $n = (int) scalar($other, "SELECT COUNT(*) FROM $t WHERE id = 2");
    $PDO ? $other = null : $other->close();
    expect($n === 1, 'committed row not visible to another session');
    exec_sql($conn, "DROP TABLE $t");
}

function c_multi_statement(): string
{
    global $conn, $PDO;
    $sql = "SELECT 1 AS a; SELECT 2 AS b, 'two' AS c";
    if ($PDO) {
        $st = $conn->query($sql);
        $first = $st->fetchAll(PDO::FETCH_NUM);
        expect($st->nextRowset(), 'no second result');
        $second = $st->fetchAll(PDO::FETCH_NUM);
        expect(!$st->nextRowset(), 'unexpected third result');
    } else {
        $conn->multi_query($sql);
        $first = $conn->store_result()->fetch_all(MYSQLI_NUM);
        expect($conn->next_result(), 'no second result');
        $second = $conn->store_result()->fetch_all(MYSQLI_NUM);
        expect(!$conn->more_results(), 'unexpected third result');
    }
    expect((int) $first[0][0] === 1 && (int) $second[0][0] === 2 && $second[0][1] === 'two',
        'got ' . json_encode([$first, $second]));
    return '2 resultsets';
}

function c_duplicate_key(): string
{
    global $conn, $PDO, $PREFIX;
    $t = "{$PREFIX}_dup";
    exec_sql($conn, "DROP TABLE IF EXISTS $t");
    exec_sql($conn, "CREATE TABLE $t (id INT PRIMARY KEY)");
    exec_sql($conn, "INSERT INTO $t VALUES (1)");
    try {
        exec_sql($conn, "INSERT INTO $t VALUES (1)");
    } catch (PDOException $e) {
        [$state, $errno] = $e->errorInfo;
    } catch (mysqli_sql_exception $e) {
        [$state, $errno] = [$e->getSqlState(), $e->getCode()];
    }
    expect(isset($errno), 'no error raised');
    expect($errno === 1062 && $state === '23000', "got $errno/$state");
    exec_sql($conn, "DROP TABLE $t");
    return '1062/23000';
}

function c_last_insert_id(): void
{
    global $conn, $PDO, $PREFIX;
    $t = "{$PREFIX}_ai";
    exec_sql($conn, "DROP TABLE IF EXISTS $t");
    exec_sql($conn, "CREATE TABLE $t (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT = 100");
    exec_sql($conn, "INSERT INTO $t (v) VALUES (1)");
    $first = $PDO ? (int) $conn->lastInsertId() : (int) $conn->insert_id;
    exec_sql($conn, "INSERT INTO $t (v) VALUES (2), (3), (4)");
    $multi = $PDO ? (int) $conn->lastInsertId() : (int) $conn->insert_id;
    $lid = (int) scalar($conn, 'SELECT LAST_INSERT_ID()');
    expect($first === 100, "first insert id $first");
    expect($multi === 101 && $lid === 101, "multi-row insert id $multi, LAST_INSERT_ID() $lid");
    exec_sql($conn, "DROP TABLE $t");
}

function round_trip($c, int $size, string $label): string
{
    global $PDO, $PREFIX;
    $t = "{$PREFIX}_{$label}";
    exec_sql($c, "DROP TABLE IF EXISTS $t");
    exec_sql($c, "CREATE TABLE $t (id INT PRIMARY KEY, b LONGBLOB)");
    $data = random_bytes($size);
    if ($PDO) {
        $st = $c->prepare("INSERT INTO $t VALUES (1, ?)");
        $st->bindValue(1, $data, PDO::PARAM_LOB);
        $st->execute();
    } else {
        $st = $c->prepare("INSERT INTO $t VALUES (1, ?)");
        $null = null;
        $st->bind_param('b', $null);
        foreach (str_split($data, 1024 * 1024) as $chunk) {
            $st->send_long_data(0, $chunk);
        }
        $st->execute();
        $st->close();
    }
    [$got, $sha] = rows($c, "SELECT b, SHA2(b, 256) FROM $t WHERE id = 1")[0];
    expect($sha === hash('sha256', $data), 'server-side hash differs: the upload was corrupted');
    expect($got === $data, sprintf('download differs (%d bytes, want %d)', strlen((string) $got), $size));
    exec_sql($c, "DROP TABLE $t");
    return "$size bytes";
}

function c_large_row(): string
{
    global $conn;
    return round_trip($conn, 4 * 1024 * 1024, 'big');
}

function c_multi_packet(): string
{
    global $conn;
    $limit = (int) scalar($conn, 'SELECT @@max_allowed_packet');
    $size = 16 * 1024 * 1024 + 1024;
    expect($limit >= 2 * $size + 1024, "server max_allowed_packet $limit is too small for $size bytes");
    return round_trip($conn, $size, 'mp');
}

function c_compression(): string
{
    global $PDO;
    $c = connect(true);
    $status = rows($c, "SHOW SESSION STATUS LIKE 'Compression'")[0][1];
    expect($status === 'ON', "Compression status $status");
    expect((int) scalar($c, "SELECT LENGTH(REPEAT('a', 1000000))") === 1000000, 'REPEAT length');
    $d = round_trip($c, 1024 * 1024, 'zip');
    $PDO ? $c = null : $c->close();
    return "Compression=ON, $d";
}

function c_conn_attrs(): string
{
    global $PDO;
    // mysqlnd has no API to add attributes; it sends its own.
    $c = connect();
    if ((int) scalar($c, 'SELECT @@performance_schema') !== 1) {
        throw new Skip('performance_schema is off on the server');
    }
    $rows = rows($c, 'SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs '
        . "WHERE PROCESSLIST_ID = CONNECTION_ID() AND ATTR_NAME = '_client_name'");
    $PDO ? $c = null : $c->close();
    expect(count($rows) === 1 && $rows[0][0] === 'mysqlnd', '_client_name: ' . json_encode($rows));
    return '_client_name=mysqlnd';
}

// --- chapter 05 matrix items ----------------------------------------------------

const INFILE = "1,héllo 🌱\n2,\"quoted, comma\"\n3,\\N\n";
const INFILE_ROWS = '1=héllo 🌱|2=quoted, comma|3=NULL';

function load_sql(string $t, string $f): string
{
    return "LOAD DATA LOCAL INFILE '$f' INTO TABLE $t CHARACTER SET utf8mb4 "
        . "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n'";
}

function loaded($c, string $t): string
{
    return (string) scalar($c, "SELECT IFNULL(GROUP_CONCAT(CONCAT(id, '=', IFNULL(v, 'NULL')) ORDER BY id SEPARATOR '|'), '') FROM $t");
}

function infile_table($c, string $t): void
{
    exec_sql($c, "DROP TABLE IF EXISTS $t");
    exec_sql($c, "CREATE TABLE $t (id INT PRIMARY KEY, v VARCHAR(32)) DEFAULT CHARSET = utf8mb4");
    if ((int) scalar($c, 'SELECT @@GLOBAL.local_infile') === 0) {
        throw new Skip('local_infile is off on the server');
    }
}

function infile_file(): string
{
    $f = tempnam(sys_get_temp_dir(), 'sprig');
    file_put_contents($f, INFILE);
    return $f;
}

function close_conn(&$c): void
{
    global $PDO;
    $PDO ? $c = null : $c->close();
}

function c_local_infile(): string
{
    global $PDO, $PREFIX;
    $t = "{$PREFIX}_infile";
    $f = infile_file();
    // mysqlnd sends a file only from the allowed directory (PHP 8.1+) or with local infile on.
    $c = connect(false, $PDO ? [Pdo\Mysql::ATTR_LOCAL_INFILE => true]
        : [MYSQLI_OPT_LOCAL_INFILE => true, MYSQLI_OPT_LOAD_DATA_LOCAL_DIR => dirname($f)]);
    try {
        infile_table($c, $t);
        $n = $PDO ? $c->exec(load_sql($t, $f)) : ($c->query(load_sql($t, $f)) ? $c->affected_rows : -1);
        $rows = loaded($c, $t);
        expect($n === 3 && $rows === INFILE_ROWS, "loaded $n rows: $rows");
        exec_sql($c, "DROP TABLE $t");
        return '3 rows';
    } finally {
        close_conn($c);
        unlink($f);
    }
}

/** errno and SQLSTATE of a driver exception. */
function err_of(Throwable $e): array
{
    if ($e instanceof PDOException && is_array($e->errorInfo)) {
        return [(int) $e->errorInfo[1], (string) $e->errorInfo[0]];
    }
    if ($e instanceof mysqli_sql_exception) {
        return [$e->getCode(), $e->getSqlState()];
    }
    return [0, ''];
}

function c_local_infile_off(): string
{
    global $PDO, $PREFIX, $PERSONA;
    $t = "{$PREFIX}_infile_off";
    $f = infile_file();
    $c = connect(false, $PDO ? [Pdo\Mysql::ATTR_LOCAL_INFILE => false] : [MYSQLI_OPT_LOCAL_INFILE => false]);
    try {
        infile_table($c, $t);
        $how = null;
        try {
            exec_sql($c, load_sql($t, $f));
        } catch (PDOException | mysqli_sql_exception $e) {
            [$errno, $state] = err_of($e);
            $how = "$errno/$state " . $e->getMessage();
            $want = $PERSONA === 'mysql' ? 3948 : 4166;
            if (in_array($errno, [1148, 3948, 4166], true)) {
                expect($errno === $want, "server refused with $how, want $want");
            }
        }
        expect($how !== null, 'the load succeeded');
        $rows = loaded($c, $t);
        expect($rows === '', "rows were loaded: $rows");
        exec_sql($c, "DROP TABLE $t");
        return "refused: $how";
    } finally {
        close_conn($c);
        unlink($f);
    }
}

function proc_ddl(string $p): string
{
    return "CREATE PROCEDURE $p(IN a INT, OUT b INT, INOUT c INT)
BEGIN
  SELECT a AS x;
  SELECT a + 1 AS y, 'two' AS z UNION ALL SELECT a + 2, 'three';
  SET b = a * 10;
  SET c = c + 1;
END";
}

function c_call(): string
{
    global $conn, $PDO, $PREFIX;
    $p = "{$PREFIX}_proc";
    exec_sql($conn, "DROP PROCEDURE IF EXISTS $p");
    exec_sql($conn, proc_ddl($p));
    exec_sql($conn, 'SET @b = NULL, @c = 5');
    $sets = [];
    $fmt = fn (array $rows) => implode(';', array_map(fn ($r) => implode(',', $r), $rows));
    if ($PDO) {
        $how = 'text CALL (emulated prepares)';
        $st = $conn->query("CALL $p(1, @b, @c)");
        do {
            if ($st->columnCount() > 0) {
                $sets[] = $fmt($st->fetchAll(PDO::FETCH_NUM));
            }
        } while ($st->nextRowset());
        $st->closeCursor();
    } else {
        $how = 'prepared CALL';
        $st = $conn->prepare("CALL $p(?, @b, @c)");
        $a = 1;
        $st->bind_param('i', $a);
        $st->execute();
        do {
            $r = $st->get_result();
            if ($r !== false) {
                $sets[] = $fmt($r->fetch_all(MYSQLI_NUM));
            }
        } while ($st->more_results() && $st->next_result());
        $st->close();
    }
    [$b, $c] = rows($conn, 'SELECT @b, @c')[0];
    exec_sql($conn, "DROP PROCEDURE $p");
    expect($sets === ['1', '2,two;3,three'], 'resultsets ' . json_encode($sets));
    expect((int) $b === 10 && (int) $c === 6, "OUT b=$b INOUT c=$c");
    return "$how: 2 resultsets, OUT 10, INOUT 6";
}

function mark_session($c): string
{
    exec_sql($c, 'SET @sprig_mark = 42, SESSION auto_increment_increment = 7');
    exec_sql($c, 'CREATE TEMPORARY TABLE sprig_tmp (id INT)');
    return (string) scalar($c, 'SELECT CONNECTION_ID()');
}

function assert_reset($c, string $id): string
{
    [$cid, $mark, $inc] = rows($c, 'SELECT CONNECTION_ID(), @sprig_mark, @@SESSION.auto_increment_increment')[0];
    $state = "id $cid, @sprig_mark " . json_encode($mark) . ", auto_increment_increment $inc";
    expect((string) $cid === $id, "connection id changed from $id: $state");
    expect($mark === null && (int) $inc === 1, "session not reset: $state");
    try {
        rows($c, 'SELECT COUNT(*) FROM sprig_tmp');
    } catch (PDOException | mysqli_sql_exception $e) {
        expect(err_of($e)[0] === 1146, 'temporary table probe: ' . $e->getMessage());
        return "same connection $id; user variable, session variable and temporary table cleared";
    }
    throw new RuntimeException('the temporary table survived');
}

function c_change_user(): string
{
    global $PDO;
    if ($PDO) {
        throw new NA('PDO has no change-user API');
    }
    $c = connect();
    try {
        $id = mark_session($c);
        expect($c->change_user(getenv('SPRIG_USER'), getenv('SPRIG_PASSWORD'), getenv('SPRIG_DATABASE')), 'change_user failed');
        return 'mysqli::change_user (COM_CHANGE_USER): ' . assert_reset($c, $id);
    } finally {
        $c->close();
    }
}

function c_reset_connection(): string
{
    global $CONNECTOR;
    throw new NA("$CONNECTOR has no reset-connection API");
}

/**
 * PHP's pool is the persistent connection. Execute a server-side prepared
 * statement on one, KILL it from another session, release it, then ask for
 * a persistent connection again: PHP must notice it died (mysqli re-uses a
 * persistent link with COM_CHANGE_USER, PDO pings it), connect anew, and
 * the statement is prepared again.
 */
function c_pool_reprepare(): string
{
    global $conn, $PDO;
    $sql = 'SELECT ? + 1, CONNECTION_ID()';
    $run = function ($c, int $v) use ($PDO, $sql): array {
        if ($PDO) {
            $c->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
            $st = $c->prepare($sql);
            $st->execute([$v]);
            return $st->fetch(PDO::FETCH_NUM);
        }
        $st = $c->prepare($sql);
        $st->bind_param('i', $v);
        $st->execute();
        $r = $st->get_result()->fetch_row();
        $st->close();
        return $r;
    };
    $c = connect(false, [], true);
    [$n, $id1] = $run($c, 41);
    expect((int) $n === 42, "got $n");
    close_conn($c);
    $c = connect(false, [], true);
    [$n, $again] = $run($c, 41);
    expect((string) $again === (string) $id1, "the persistent connection wasn't re-used ($id1, then $again)");
    close_conn($c);
    exec_sql($conn, "KILL $id1");
    // Wait until the killed session has left the processlist (its socket is closed).
    for ($i = 0; $i < 100 && (int) scalar($conn, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = $id1") !== 0; $i++) {
        usleep(50000);
    }
    usleep(100000);
    $c = connect(false, [], true);
    [$n, $id2] = $run($c, 99);
    close_conn($c);
    expect((int) $n === 100, "after the kill: got $n");
    expect((string) $id2 !== (string) $id1, "the killed connection $id1 was handed out again");
    return "persistent connection $id1 re-used, killed, re-prepared on $id2";
}

function lock_table(): string
{
    global $conn, $PREFIX;
    $t = "{$PREFIX}_lock";
    exec_sql($conn, "DROP TABLE IF EXISTS $t");
    exec_sql($conn, "CREATE TABLE $t (id INT PRIMARY KEY, v INT) ENGINE = InnoDB");
    exec_sql($conn, "INSERT INTO $t VALUES (1, 0), (2, 0)");
    return $t;
}

function begin($c): void
{
    global $PDO;
    $PDO ? $c->beginTransaction() : $c->begin_transaction();
}

function lock_pair(string $t): array
{
    $a = connect();
    $b = connect();
    begin($a);
    rows($a, "SELECT v FROM $t WHERE id = 1 FOR UPDATE");
    exec_sql($b, 'SET SESSION innodb_lock_wait_timeout = 1');
    begin($b);
    return [$a, $b];
}

function c_lock_wait(): string
{
    global $conn;
    $t = lock_table();
    [$a, $b] = lock_pair($t);
    try {
        rows($b, "SELECT v FROM $t WHERE id = 2 FOR UPDATE");
        $t0 = hrtime(true);
        $got = null;
        try {
            rows($b, "SELECT v FROM $t WHERE id = 1 FOR UPDATE");
        } catch (PDOException | mysqli_sql_exception $e) {
            [$errno, $state] = err_of($e);
            $got = "$errno/$state";
            expect($errno === 1205, "got $got " . $e->getMessage());
        }
        expect($got !== null, 'the second session got the lock');
        $ms = intdiv(hrtime(true) - $t0, 1000000);
        expect($ms >= 900 && $ms < 10000, "timed out after $ms ms");
        // Only the statement is rolled back: the transaction still holds row 2.
        exec_sql($b, "UPDATE $t SET v = 2 WHERE id = 2");
        $b->commit();
        $a->commit();
        expect((int) scalar($conn, "SELECT v FROM $t WHERE id = 2") === 2, 'the waiting transaction lost its work');
        exec_sql($conn, "DROP TABLE $t");
        return "$got after $ms ms; the transaction survived";
    } finally {
        close_conn($a);
        close_conn($b);
    }
}

function c_lock_nowait(): string
{
    global $conn, $PERSONA;
    $t = lock_table();
    [$a, $b] = lock_pair($t);
    $want = $PERSONA === 'mysql' ? 3572 : 1205;
    try {
        $got = null;
        try {
            rows($b, "SELECT v FROM $t WHERE id = 1 FOR UPDATE NOWAIT");
        } catch (PDOException | mysqli_sql_exception $e) {
            [$errno, $state] = err_of($e);
            $got = "$errno/$state";
            expect($errno === $want, "NOWAIT: got $got " . $e->getMessage() . ", want $want");
        }
        expect($got !== null, 'NOWAIT got the lock');
        $ids = array_map(fn ($r) => (int) $r[0], rows($b, "SELECT id FROM $t ORDER BY id FOR UPDATE SKIP LOCKED"));
        expect($ids === [2], 'SKIP LOCKED returned ' . json_encode($ids));
        $b->rollback();
        $a->rollback();
        exec_sql($conn, "DROP TABLE $t");
        return "NOWAIT $got, SKIP LOCKED returned row 2";
    } finally {
        close_conn($a);
        close_conn($b);
    }
}

$checks = [
    'connect' => 'c_connect', 'plain_query' => 'c_plain_query', 'types' => 'c_types',
    'prepared' => 'c_prepared', 'transaction' => 'c_transaction',
    'multi_statement' => 'c_multi_statement', 'duplicate_key' => 'c_duplicate_key',
    'last_insert_id' => 'c_last_insert_id', 'large_row' => 'c_large_row',
    'multi_packet' => 'c_multi_packet', 'compression' => 'c_compression',
    'conn_attrs' => 'c_conn_attrs', 'local_infile' => 'c_local_infile',
    'local_infile_off' => 'c_local_infile_off', 'call' => 'c_call', 'change_user' => 'c_change_user',
    'reset_connection' => 'c_reset_connection', 'pool_reprepare' => 'c_pool_reprepare',
    'lock_wait' => 'c_lock_wait', 'lock_nowait' => 'c_lock_nowait',
];
foreach ($checks as $name => $fn) {
    if (in_array($name, $SKIPPED, true)) {
        continue;
    }
    if ($conn === null && $name !== 'connect') {
        emit($name, 'fail', 'no connection');
        continue;
    }
    $t0 = hrtime(true);
    try {
        $detail = $fn();
        emit($name, 'pass', (string) ($detail ?? ''), (hrtime(true) - $t0) / 1e6);
    } catch (NA $e) {
        emit($name, 'n/a', $e->getMessage());
    } catch (Skip $e) {
        emit($name, 'skip', $e->getMessage());
    } catch (Throwable $e) {
        emit($name, 'fail', get_class($e) . ': ' . $e->getMessage(), (hrtime(true) - $t0) / 1e6);
    }
}
