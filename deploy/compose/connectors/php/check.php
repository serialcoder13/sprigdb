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

/** A new session: PDO (emulated prepares, PDO's default) or mysqli. */
function connect(bool $compress = false)
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
    $c->real_connect($host, getenv('SPRIG_USER'), getenv('SPRIG_PASSWORD'), getenv('SPRIG_DATABASE'),
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

$checks = [
    'connect' => 'c_connect', 'plain_query' => 'c_plain_query', 'types' => 'c_types',
    'prepared' => 'c_prepared', 'transaction' => 'c_transaction',
    'multi_statement' => 'c_multi_statement', 'duplicate_key' => 'c_duplicate_key',
    'last_insert_id' => 'c_last_insert_id', 'large_row' => 'c_large_row',
    'multi_packet' => 'c_multi_packet', 'compression' => 'c_compression',
    'conn_attrs' => 'c_conn_attrs',
];
foreach ($checks as $name => $fn) {
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
