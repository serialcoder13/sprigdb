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
	"slices"
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
	persona  = os.Getenv("SPRIG_PERSONA")
	prefix   = "t_go_sql_driver_" + tlsMode
	// Checks run.sh asked to leave out (--quick): not run, not reported.
	skipped = strings.Fields(strings.ReplaceAll(os.Getenv("SPRIG_CONN_SKIP_CHECKS"), ",", " "))
	ctx     = context.Background()
	conn    *sql.Conn
	pool    *sql.DB
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

// --- chapter 05 matrix items --------------------------------------------------

const (
	infileData = "1,héllo 🌱\n2,\"quoted, comma\"\n3,\\N\n"
	infileRows = "1=héllo 🌱|2=quoted, comma|3=NULL"
)

func infileTable(c *sql.Conn, t string) error {
	exec(c, "DROP TABLE IF EXISTS "+t)
	if err := exec(c, "CREATE TABLE "+t+" (id INT PRIMARY KEY, v VARCHAR(32)) DEFAULT CHARSET = utf8mb4"); err != nil {
		return err
	}
	if v, err := scalar(c, "SELECT @@GLOBAL.local_infile"); err != nil {
		return err
	} else if v == "0" {
		return skip("local_infile is off on the server")
	}
	return nil
}

func loadSQL(t, path string) string {
	return "LOAD DATA LOCAL INFILE '" + path + "' INTO TABLE " + t + " CHARACTER SET utf8mb4 " +
		"FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n'"
}

func loaded(c *sql.Conn, t string) (string, error) {
	return scalar(c, "SELECT IFNULL(GROUP_CONCAT(CONCAT(id, '=', IFNULL(v, 'NULL')) ORDER BY id SEPARATOR '|'), '') FROM "+t)
}

func infileFile() (string, error) {
	f, err := os.CreateTemp("", "sprig*.csv")
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = f.WriteString(infileData)
	return f.Name(), err
}

func checkLocalInfile() (string, error) {
	t := prefix + "_infile"
	path, err := infileFile()
	if err != nil {
		return "", err
	}
	defer os.Remove(path)
	mysql.RegisterLocalFile(path)
	defer mysql.DeregisterLocalFile(path)
	if err := infileTable(conn, t); err != nil {
		return "", err
	}
	r, err := conn.ExecContext(ctx, loadSQL(t, path))
	if err != nil {
		return "", err
	}
	n, _ := r.RowsAffected()
	rows, err := loaded(conn, t)
	if err != nil || n != 3 || rows != infileRows {
		return "", fmt.Errorf("loaded %d rows: %q %v", n, rows, err)
	}
	exec(conn, "DROP TABLE "+t)
	return "3 rows (RegisterLocalFile)", nil
}

// The server's refusal when the client doesn't offer CLIENT_LOCAL_FILES.
func infileRefusal() uint16 {
	if persona == "mysql" {
		return 3948
	}
	return 4166
}

func checkLocalInfileOff() (string, error) {
	t := prefix + "_infile_off"
	path, err := infileFile()
	if err != nil {
		return "", err
	}
	defer os.Remove(path)
	if err := infileTable(conn, t); err != nil {
		return "", err
	}
	// Not registered and no allowAllFiles: the driver must refuse the server's request.
	err = exec(conn, loadSQL(t, path))
	if err == nil {
		return "", errors.New("the load succeeded")
	}
	how := "client-side: " + err.Error()
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		how = fmt.Sprintf("%d/%s %s", me.Number, string(me.SQLState[:]), me.Message)
		if (me.Number == 1148 || me.Number == 3948 || me.Number == 4166) && me.Number != infileRefusal() {
			return "", fmt.Errorf("server refused with %s, want %d", how, infileRefusal())
		}
	}
	if rows, err := loaded(conn, t); err != nil || rows != "" {
		return "", fmt.Errorf("rows were loaded: %q %v", rows, err)
	}
	exec(conn, "DROP TABLE "+t)
	return "refused: " + how, nil
}

const procDDL = `CREATE PROCEDURE %s(IN a INT, OUT b INT, INOUT c INT)
BEGIN
  SELECT a AS x;
  SELECT a + 1 AS y, 'two' AS z UNION ALL SELECT a + 2, 'three';
  SET b = a * 10;
  SET c = c + 1;
END`

func checkCall() (string, error) {
	p := prefix + "_proc"
	exec(conn, "DROP PROCEDURE IF EXISTS "+p)
	if err := exec(conn, fmt.Sprintf(procDDL, p)); err != nil {
		return "", err
	}
	if err := exec(conn, "SET @b = NULL, @c = 5"); err != nil {
		return "", err
	}
	// An argument makes it a server-side prepared CALL (binary protocol).
	rows, err := conn.QueryContext(ctx, "CALL "+p+"(?, @b, @c)", 1)
	if err != nil {
		return "", err
	}
	var sets []string
	for {
		var set string
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return "", err
			}
			for i, v := range vals {
				if i > 0 {
					set += ","
				}
				set += v.String
			}
			set += ";"
		}
		if len(cols) > 0 {
			sets = append(sets, set)
		}
		if !rows.NextResultSet() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()
	var b, c int
	if err := conn.QueryRowContext(ctx, "SELECT @b, @c").Scan(&b, &c); err != nil {
		return "", err
	}
	exec(conn, "DROP PROCEDURE "+p)
	if len(sets) != 2 || sets[0] != "1;" || sets[1] != "2,two;3,three;" {
		return "", fmt.Errorf("resultsets %q", sets)
	}
	if b != 10 || c != 6 {
		return "", fmt.Errorf("OUT b=%d INOUT c=%d", b, c)
	}
	return "prepared CALL: 2 resultsets, OUT 10, INOUT 6", nil
}

func checkChangeUser() (string, error) {
	return "", na("go-sql-driver/mysql has no change-user API")
}

func checkResetConnection() (string, error) {
	// database/sql calls the driver's ResetSession when a connection goes back
	// to the pool, but go-sql-driver only checks liveness there: it never
	// sends COM_RESET_CONNECTION, so session state leaks between borrowers.
	return "", na("go-sql-driver/mysql's ResetSession is a liveness check only, never COM_RESET_CONNECTION")
}

// A one-connection pool and a pool-level prepared statement: execute it,
// KILL the pooled connection from another session, execute again.
// database/sql must drop the dead connection (the driver's liveness check
// returns ErrBadConn), reconnect and re-prepare the statement by itself.
func checkPoolReprepare() (string, error) {
	db, err := sql.Open("mysql", dsn(""))
	if err != nil {
		return "", err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	st, err := db.PrepareContext(ctx, "SELECT ? + 1, CONNECTION_ID()")
	if err != nil {
		return "", err
	}
	defer st.Close()
	var n int
	var id1, id2 string
	for _, v := range []int{41, 41} {
		if err := st.QueryRowContext(ctx, v).Scan(&n, &id1); err != nil || n != 42 {
			return "", fmt.Errorf("got %d %v", n, err)
		}
	}
	if err := exec(conn, "KILL "+id1); err != nil {
		return "", err
	}
	waitGone(id1)
	if err := st.QueryRowContext(ctx, 99).Scan(&n, &id2); err != nil {
		return "", fmt.Errorf("after the kill: %w", err)
	}
	if n != 100 || id1 == id2 {
		return "", fmt.Errorf("after the kill: got %d on connection %s (killed %s)", n, id2, id1)
	}
	return fmt.Sprintf("database/sql pool: connection %s killed, re-prepared on %s", id1, id2), nil
}

// waitGone waits until the killed session has left the processlist, so the
// pool is reused only after the server has closed its socket.
func waitGone(id string) {
	for i := 0; i < 100; i++ {
		if n, err := scalar(conn, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = "+id); err == nil && n == "0" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
}

func lockTable() (string, error) {
	t := prefix + "_lock"
	exec(conn, "DROP TABLE IF EXISTS "+t)
	if err := exec(conn, "CREATE TABLE "+t+" (id INT PRIMARY KEY, v INT) ENGINE = InnoDB"); err != nil {
		return "", err
	}
	return t, exec(conn, "INSERT INTO "+t+" VALUES (1, 0), (2, 0)")
}

// Two sessions, each its own pool, with the first holding row 1 FOR UPDATE.
func lockPair(t string) (a, b *sql.Tx, done func(), err error) {
	dbA, cA, err := open("")
	if err != nil {
		return nil, nil, nil, err
	}
	dbB, cB, err := open("&innodb_lock_wait_timeout=1")
	if err != nil {
		dbA.Close()
		return nil, nil, nil, err
	}
	done = func() { cA.Close(); cB.Close(); dbA.Close(); dbB.Close() }
	if a, err = cA.BeginTx(ctx, nil); err == nil {
		var v int
		err = a.QueryRowContext(ctx, "SELECT v FROM "+t+" WHERE id = 1 FOR UPDATE").Scan(&v)
	}
	if err == nil {
		b, err = cB.BeginTx(ctx, nil)
	}
	if err != nil {
		done()
	}
	return a, b, done, err
}

func errnoOf(err error) (uint16, string) {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number, fmt.Sprintf("%d/%s", me.Number, string(me.SQLState[:]))
	}
	return 0, fmt.Sprintf("%v", err)
}

func checkLockWait() (string, error) {
	t, err := lockTable()
	if err != nil {
		return "", err
	}
	a, b, done, err := lockPair(t)
	if err != nil {
		return "", err
	}
	defer done()
	var v int
	if err := b.QueryRowContext(ctx, "SELECT v FROM "+t+" WHERE id = 2 FOR UPDATE").Scan(&v); err != nil {
		return "", err
	}
	t0 := time.Now()
	err = b.QueryRowContext(ctx, "SELECT v FROM "+t+" WHERE id = 1 FOR UPDATE").Scan(&v)
	ms := time.Since(t0).Milliseconds()
	if err == nil {
		return "", errors.New("the second session got the lock")
	}
	n, got := errnoOf(err)
	if n != 1205 {
		return "", fmt.Errorf("got %s", got)
	}
	if ms < 900 || ms > 10000 {
		return "", fmt.Errorf("timed out after %d ms", ms)
	}
	// Only the statement is rolled back: the transaction still holds row 2.
	if _, err := b.ExecContext(ctx, "UPDATE "+t+" SET v = 2 WHERE id = 2"); err != nil {
		return "", err
	}
	if err := b.Commit(); err != nil {
		return "", err
	}
	a.Commit()
	if s, _ := scalar(conn, "SELECT v FROM "+t+" WHERE id = 2"); s != "2" {
		return "", errors.New("the waiting transaction lost its work")
	}
	exec(conn, "DROP TABLE "+t)
	return fmt.Sprintf("%s after %d ms; the transaction survived", got, ms), nil
}

func checkLockNowait() (string, error) {
	t, err := lockTable()
	if err != nil {
		return "", err
	}
	a, b, done, err := lockPair(t)
	if err != nil {
		return "", err
	}
	defer done()
	defer a.Rollback()
	defer b.Rollback()
	want := uint16(1205)
	if persona == "mysql" {
		want = 3572
	}
	var v int
	err = b.QueryRowContext(ctx, "SELECT v FROM "+t+" WHERE id = 1 FOR UPDATE NOWAIT").Scan(&v)
	if err == nil {
		return "", errors.New("NOWAIT got the lock")
	}
	n, got := errnoOf(err)
	if n != want {
		return "", fmt.Errorf("NOWAIT: got %s, want %d", got, want)
	}
	rows, err := b.QueryContext(ctx, "SELECT id FROM "+t+" ORDER BY id FOR UPDATE SKIP LOCKED")
	if err != nil {
		return "", err
	}
	var ids []int
	for rows.Next() {
		var id int
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	if !slices.Equal(ids, []int{2}) {
		return "", fmt.Errorf("SKIP LOCKED returned %v", ids)
	}
	b.Rollback()
	a.Rollback()
	exec(conn, "DROP TABLE "+t)
	return "NOWAIT " + got + ", SKIP LOCKED returned row 2", nil
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
		{"conn_attrs", checkConnAttrs}, {"local_infile", checkLocalInfile},
		{"local_infile_off", checkLocalInfileOff}, {"call", checkCall}, {"change_user", checkChangeUser},
		{"reset_connection", checkResetConnection}, {"pool_reprepare", checkPoolReprepare},
		{"lock_wait", checkLockWait}, {"lock_nowait", checkLockNowait},
	}
	for _, c := range checks {
		if slices.Contains(skipped, c.name) {
			continue
		}
		if conn == nil && c.name != "connect" {
			emit(c.name, "fail", "no connection", 0)
			continue
		}
		run(c.name, c.fn)
	}
}
