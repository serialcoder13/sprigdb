// Connector check for go-sql-driver/mysql.
//
// Runs the fixed script described in ../README.md and prints one JSON line
// per check on stdout. Configuration comes from SPRIG_* environment
// variables (see ../run.sh).
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const connector = "go-sql-driver"

var (
	host     = os.Getenv("SPRIG_HOST")
	port     = os.Getenv("SPRIG_PORT")
	user     = os.Getenv("SPRIG_USER")
	password = os.Getenv("SPRIG_PASSWORD")
	database = os.Getenv("SPRIG_DATABASE")
	tlsMode  = os.Getenv("SPRIG_TLS")
	prefix   = "t_go_sql_driver_" + tlsMode
	ctx      = context.Background()
	conn     *sql.Conn
	pool     *sql.DB
)

type na string
type skip string

func (e na) Error() string   { return string(e) }
func (e skip) Error() string { return string(e) }

func emit(check, status, detail string, ms float64) {
	if len(detail) > 300 {
		detail = detail[:300]
	}
	b, _ := json.Marshal(map[string]any{"connector": connector, "tls": tlsMode, "check": check,
		"status": status, "detail": detail, "ms": float64(int(ms*10)) / 10})
	fmt.Println(string(b))
}

func run(check string, fn func() (string, error)) {
	t0 := time.Now()
	detail, err := fn()
	ms := float64(time.Since(t0).Microseconds()) / 1000
	var n na
	var s skip
	switch {
	case err == nil:
		emit(check, "pass", detail, ms)
	case errors.As(err, &n):
		emit(check, "n/a", err.Error(), 0)
	case errors.As(err, &s):
		emit(check, "skip", err.Error(), 0)
	default:
		emit(check, "fail", err.Error(), ms)
	}
}

func dsn(extra string) string {
	tls := "false"
	if tlsMode == "required" {
		tls = "skip-verify" // encrypt, don't verify the self-signed certificate
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?tls=%s&multiStatements=true&parseTime=true&loc=UTC"+
		"&time_zone=%%27%%2B00:00%%27&maxAllowedPacket=0&charset=utf8mb4%s",
		user, password, host, port, database, tls, extra)
}

func open(extra string) (*sql.DB, *sql.Conn, error) {
	db, err := sql.Open("mysql", dsn(extra))
	if err != nil {
		return nil, nil, err
	}
	c, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return db, c, nil
}

func exec(c *sql.Conn, q string, args ...any) error {
	_, err := c.ExecContext(ctx, q, args...)
	return err
}

func scalar(c *sql.Conn, q string, args ...any) (string, error) {
	var v sql.NullString
	err := c.QueryRowContext(ctx, q, args...).Scan(&v)
	return v.String, err
}

// --- the shared data set ---------------------------------------------------

var cols = []string{"c_tiny", "c_small", "c_int", "c_big", "c_ubig", "c_dec", "c_float", "c_double",
	"c_vchar", "c_text", "c_vbin", "c_blob", "c_date", "c_dt", "c_ts", "c_time", "c_json"}

const ddl = `CREATE TABLE %s (
  id INT PRIMARY KEY,
  c_tiny TINYINT, c_small SMALLINT, c_int INT, c_big BIGINT, c_ubig BIGINT UNSIGNED,
  c_dec DECIMAL(20,6), c_float FLOAT, c_double DOUBLE,
  c_vchar VARCHAR(64), c_text TEXT, c_vbin VARBINARY(16), c_blob BLOB,
  c_date DATE, c_dt DATETIME(6), c_ts TIMESTAMP NULL, c_time TIME, c_json JSON
) DEFAULT CHARSET = utf8mb4`

var (
	blob = func() []byte {
		b := make([]byte, 256)
		for i := range b {
			b[i] = byte(i)
		}
		return b
	}()
	vbin     = []byte{0x00, 0xFF, 0x80, 0x7F, 0x0A, 0x0D, 0x27, 0x22, 0x5C}
	vchar    = "héllo wörld 🌱"
	text     = "Ünïcödé 日本語 🚀🌱 'quoted' \"double\" back\\slash"
	jsonText = `{"a": 1, "b": [true, null], "s": "🌱"}`
	expected = map[string]string{
		"c_tiny": "-128", "c_small": "32767", "c_int": "-2147483648",
		"c_big": "9223372036854775807", "c_ubig": "18446744073709551615",
		"c_dec": "-12345678901234.567890", "c_float": "1.5", "c_double": "-0.00225",
		"c_vchar": vchar, "c_text": text, "c_vbin": hex.EncodeToString(vbin), "c_blob": hex.EncodeToString(blob),
		"c_date": "2024-02-29", "c_dt": "2024-02-29 23:59:58.123456",
		"c_ts": "2024-02-29 12:34:56", "c_time": "838:59:59", "c_json": jsonText,
	}
)

func canon(col string, v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case []byte:
		if col == "c_vbin" || col == "c_blob" {
			return hex.EncodeToString(x), true
		}
		return string(x), true
	case string:
		return x, true
	case int64:
		return strconv.FormatInt(x, 10), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32), true
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), true
	case time.Time:
		switch col {
		case "c_date":
			return x.Format("2006-01-02"), true
		case "c_dt":
			return x.Format("2006-01-02 15:04:05.000000"), true
		default:
			return x.Format("2006-01-02 15:04:05"), true
		}
	default:
		return fmt.Sprintf("%T %v", v, v), true
	}
}

func sameJSON(a, b string) bool {
	var x, y any
	if json.Unmarshal([]byte(a), &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

func compareRow(vals []any, nulls bool) error {
	var bad []string
	for i, col := range cols {
		got, ok := canon(col, vals[i])
		if nulls {
			if ok {
				bad = append(bad, fmt.Sprintf("%s: got %q want NULL", col, got))
			}
			continue
		}
		want := expected[col]
		match := ok && got == want
		if ok && col == "c_json" {
			match = sameJSON(got, want)
		}
		if ok && (col == "c_float" || col == "c_double") {
			g, _ := strconv.ParseFloat(got, 64)
			w, _ := strconv.ParseFloat(want, 64)
			match = g == w
		}
		if !match {
			bad = append(bad, fmt.Sprintf("%s: got %q (%T) want %q", col, got, vals[i], want))
		}
	}
	if len(bad) > 0 {
		return errors.New(strings.Join(bad, "; "))
	}
	return nil
}

func scanRows(rows *sql.Rows) ([][]any, error) {
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}

// --- checks ------------------------------------------------------------------

func checkConnect() (string, error) {
	var err error
	pool, conn, err = open("")
	if err != nil {
		return "", err
	}
	return tlsState(conn)
}

func tlsState(c *sql.Conn) (string, error) {
	var name, cipher string
	if err := c.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); err != nil {
		return "", err
	}
	if tlsMode == "required" && cipher == "" {
		return "", errors.New("TLS required but the session isn't encrypted")
	}
	if tlsMode != "required" && cipher != "" {
		return "", fmt.Errorf("TLS disabled but the session is encrypted (%s)", cipher)
	}
	if cipher == "" {
		cipher = "none"
	}
	return "cipher=" + cipher, nil
}

func checkPlainQuery() (string, error) {
	var n int
	var s, v string
	if err := conn.QueryRowContext(ctx, "SELECT 1 + 1, 'sprig', VERSION()").Scan(&n, &s, &v); err != nil {
		return "", err
	}
	if n != 2 || s != "sprig" {
		return "", fmt.Errorf("got %d %q", n, s)
	}
	return "server " + v, nil
}

func checkTypes() (string, error) {
	t := prefix + "_types"
	exec(conn, "DROP TABLE IF EXISTS "+t)
	if err := exec(conn, fmt.Sprintf(ddl, t)); err != nil {
		return "", err
	}
	q := fmt.Sprintf("INSERT INTO %s VALUES (1, -128, 32767, -2147483648, 9223372036854775807, "+
		"18446744073709551615, -12345678901234.567890, 1.5, -0.00225, '%s', '%s', X'%s', X'%s', "+
		"'2024-02-29', '2024-02-29 23:59:58.123456', '2024-02-29 12:34:56', '838:59:59', '%s'), "+
		"(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
		t, sqlEscape(vchar), sqlEscape(text), hex.EncodeToString(vbin), hex.EncodeToString(blob), sqlEscape(jsonText))
	if err := exec(conn, q); err != nil {
		return "", err
	}
	// No arguments: the text protocol (COM_QUERY).
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s ORDER BY id", strings.Join(cols, ", "), t))
	if err != nil {
		return "", err
	}
	got, err := scanRows(rows)
	if err != nil {
		return "", err
	}
	if len(got) != 2 {
		return "", fmt.Errorf("want 2 rows, got %d", len(got))
	}
	if err := compareRow(got[0], false); err != nil {
		return "", err
	}
	if err := compareRow(got[1], true); err != nil {
		return "", err
	}
	exec(conn, "DROP TABLE "+t)
	return "17 columns, 2 rows", nil
}

func sqlEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s)
}

func checkPrepared() (string, error) {
	t := prefix + "_ps"
	exec(conn, "DROP TABLE IF EXISTS "+t)
	if err := exec(conn, fmt.Sprintf(ddl, t)); err != nil {
		return "", err
	}
	ins, err := conn.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?%s)", t, strings.Repeat(", ?", len(cols))))
	if err != nil {
		return "", err
	}
	defer ins.Close()
	dt := time.Date(2024, 2, 29, 23, 59, 58, 123456000, time.UTC)
	ts := time.Date(2024, 2, 29, 12, 34, 56, 0, time.UTC)
	date := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	if _, err := ins.ExecContext(ctx, 10, int8(-128), int16(32767), int32(-2147483648), int64(9223372036854775807),
		uint64(18446744073709551615), "-12345678901234.567890", float32(1.5), -0.00225, vchar, text, vbin, blob,
		date, dt, ts, "838:59:59", jsonText); err != nil {
		return "", fmt.Errorf("first execute: %w", err)
	}
	nulls := make([]any, len(cols)+1)
	nulls[0] = 11
	if _, err := ins.ExecContext(ctx, nulls...); err != nil {
		return "", fmt.Errorf("re-execute with NULLs: %w", err)
	}
	sel, err := conn.PrepareContext(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", strings.Join(cols, ", "), t))
	if err != nil {
		return "", err
	}
	defer sel.Close()
	for _, id := range []int{10, 11} {
		rows, err := sel.QueryContext(ctx, id)
		if err != nil {
			return "", err
		}
		got, err := scanRows(rows)
		if err != nil {
			return "", err
		}
		if len(got) != 1 {
			return "", fmt.Errorf("id %d: %d rows", id, len(got))
		}
		if err := compareRow(got[0], id == 11); err != nil {
			return "", fmt.Errorf("id %d: %w", id, err)
		}
	}
	exec(conn, "DROP TABLE "+t)
	return "server-side, 2 executions each", nil
}

func count(c *sql.Conn, q string) (int, error) {
	var n int
	err := c.QueryRowContext(ctx, q).Scan(&n)
	return n, err
}

func checkTransaction() (string, error) {
	t := prefix + "_tx"
	exec(conn, "DROP TABLE IF EXISTS "+t)
	if err := exec(conn, "CREATE TABLE "+t+" (id INT PRIMARY KEY) ENGINE = InnoDB"); err != nil {
		return "", err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+t+" VALUES (1)"); err != nil {
		return "", err
	}
	if err := tx.Rollback(); err != nil {
		return "", err
	}
	if n, err := count(conn, "SELECT COUNT(*) FROM "+t); err != nil || n != 0 {
		return "", fmt.Errorf("row survived rollback (%d, %v)", n, err)
	}
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+t+" VALUES (2)"); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	other, err := pool.Conn(ctx) // a second session from the pool
	if err != nil {
		return "", err
	}
	n, err := count(other, "SELECT COUNT(*) FROM "+t+" WHERE id = 2")
	other.Close()
	if err != nil || n != 1 {
		return "", fmt.Errorf("committed row not visible to another session (%d, %v)", n, err)
	}
	exec(conn, "DROP TABLE "+t)
	return "", nil
}

func checkMultiStatement() (string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT 1 AS a; SELECT 2 AS b, 'two' AS c")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var a, b int
	var c string
	if !rows.Next() {
		return "", errors.New("first result empty")
	}
	if err := rows.Scan(&a); err != nil {
		return "", err
	}
	if rows.Next() {
		return "", errors.New("first result has 2 rows")
	}
	if !rows.NextResultSet() || !rows.Next() {
		return "", fmt.Errorf("no second result (%v)", rows.Err())
	}
	if err := rows.Scan(&b, &c); err != nil {
		return "", err
	}
	if rows.NextResultSet() {
		return "", errors.New("unexpected third result")
	}
	if a != 1 || b != 2 || c != "two" {
		return "", fmt.Errorf("got %d %d %q", a, b, c)
	}
	return "2 resultsets", nil
}

func checkDuplicateKey() (string, error) {
	t := prefix + "_dup"
	exec(conn, "DROP TABLE IF EXISTS "+t)
	if err := exec(conn, "CREATE TABLE "+t+" (id INT PRIMARY KEY)"); err != nil {
		return "", err
	}
	exec(conn, "INSERT INTO "+t+" VALUES (1)")
	err := exec(conn, "INSERT INTO "+t+" VALUES (1)")
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		return "", fmt.Errorf("want a MySQLError, got %v", err)
	}
	if me.Number != 1062 || string(me.SQLState[:]) != "23000" {
		return "", fmt.Errorf("got %d/%s", me.Number, string(me.SQLState[:]))
	}
	exec(conn, "DROP TABLE "+t)
	return "1062/23000", nil
}

func checkLastInsertID() (string, error) {
	t := prefix + "_ai"
	exec(conn, "DROP TABLE IF EXISTS "+t)
	if err := exec(conn, "CREATE TABLE "+t+" (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT = 100"); err != nil {
		return "", err
	}
	r, err := conn.ExecContext(ctx, "INSERT INTO "+t+" (v) VALUES (1)")
	if err != nil {
		return "", err
	}
	first, _ := r.LastInsertId()
	r, err = conn.ExecContext(ctx, "INSERT INTO "+t+" (v) VALUES (2), (3), (4)")
	if err != nil {
		return "", err
	}
	multi, _ := r.LastInsertId()
	lid, err := scalar(conn, "SELECT LAST_INSERT_ID()")
	if err != nil {
		return "", err
	}
	if first != 100 || multi != 101 || lid != "101" {
		return "", fmt.Errorf("insert ids %d, %d, LAST_INSERT_ID() %s", first, multi, lid)
	}
	exec(conn, "DROP TABLE "+t)
	return "", nil
}

func roundTrip(c *sql.Conn, size int, label string) (string, error) {
	t := prefix + "_" + label
	exec(c, "DROP TABLE IF EXISTS "+t)
	if err := exec(c, "CREATE TABLE "+t+" (id INT PRIMARY KEY, b LONGBLOB)"); err != nil {
		return "", err
	}
	data := make([]byte, size)
	rand.Read(data)
	if err := exec(c, "INSERT INTO "+t+" VALUES (1, ?)", data); err != nil {
		return "", err
	}
	var got []byte
	var sha string
	if err := c.QueryRowContext(ctx, "SELECT b, SHA2(b, 256) FROM "+t+" WHERE id = 1").Scan(&got, &sha); err != nil {
		return "", err
	}
	want := sha256.Sum256(data)
	if sha != hex.EncodeToString(want[:]) {
		return "", errors.New("server-side hash differs: the upload was corrupted")
	}
	if !bytes.Equal(got, data) {
		return "", fmt.Errorf("download differs (%d bytes, want %d)", len(got), size)
	}
	exec(c, "DROP TABLE "+t)
	return fmt.Sprintf("%d bytes", size), nil
}

func checkLargeRow() (string, error) { return roundTrip(conn, 4*1024*1024, "big") }

func checkMultiPacket() (string, error) {
	limit, err := scalar(conn, "SELECT @@max_allowed_packet")
	if err != nil {
		return "", err
	}
	size := 16*1024*1024 + 1024
	if n, _ := strconv.Atoi(limit); n < 2*size+1024 {
		return "", fmt.Errorf("server max_allowed_packet %s is too small for %d bytes", limit, size)
	}
	return roundTrip(conn, size, "mp")
}

func checkCompression() (string, error) {
	db, c, err := open("&compress=true")
	if err != nil {
		return "", err
	}
	defer db.Close()
	defer c.Close()
	var name, status string
	if err := c.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Compression'").Scan(&name, &status); err != nil {
		return "", err
	}
	if status != "ON" {
		return "", fmt.Errorf("Compression status %q", status)
	}
	if n, err := scalar(c, "SELECT LENGTH(REPEAT('a', 1000000))"); err != nil || n != "1000000" {
		return "", fmt.Errorf("got %s %v", n, err)
	}
	d, err := roundTrip(c, 1024*1024, "zip")
	if err != nil {
		return "", err
	}
	return "Compression=ON, " + d, nil
}

func checkConnAttrs() (string, error) {
	db, c, err := open("&connectionAttributes=sprig_check:go-sql-driver")
	if err != nil {
		return "", err
	}
	defer db.Close()
	defer c.Close()
	if ps, err := scalar(c, "SELECT @@performance_schema"); err != nil {
		return "", err
	} else if ps != "1" {
		return "", skip("performance_schema is off on the server")
	}
	v, err := scalar(c, "SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs "+
		"WHERE PROCESSLIST_ID = CONNECTION_ID() AND ATTR_NAME = 'sprig_check'")
	if err != nil {
		return "", fmt.Errorf("sprig_check attribute: %w", err)
	}
	if v != "go-sql-driver" {
		return "", fmt.Errorf("sprig_check=%q", v)
	}
	return "sprig_check=" + v, nil
}

func main() {
	checks := []struct {
		name string
		fn   func() (string, error)
	}{
		{"connect", checkConnect}, {"plain_query", checkPlainQuery}, {"types", checkTypes},
		{"prepared", checkPrepared}, {"transaction", checkTransaction},
		{"multi_statement", checkMultiStatement}, {"duplicate_key", checkDuplicateKey},
		{"last_insert_id", checkLastInsertID}, {"large_row", checkLargeRow},
		{"multi_packet", checkMultiPacket}, {"compression", checkCompression},
		{"conn_attrs", checkConnAttrs},
	}
	for _, c := range checks {
		if conn == nil && c.name != "connect" {
			emit(c.name, "fail", "no connection", 0)
			continue
		}
		run(c.name, c.fn)
	}
}
