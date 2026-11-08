// Connector check for MySQL Connector/J and MariaDB Connector/J.
//
// Runs the fixed script described in ../README.md and prints one JSON line
// per check on stdout. Configuration comes from SPRIG_* environment
// variables (see ../run.sh); SPRIG_CONNECTOR is "mysql-connector-j" or
// "mariadb-connector-j".

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class Check {
    static final String CONNECTOR = System.getenv("SPRIG_CONNECTOR");
    static final boolean MYSQL_CJ = CONNECTOR.equals("mysql-connector-j");
    static final String TLS = System.getenv("SPRIG_TLS");
    static final String PREFIX = "t_" + CONNECTOR.replace('-', '_') + "_" + TLS;
    static final String PERSONA = Objects.requireNonNullElse(System.getenv("SPRIG_PERSONA"), "mariadb");
    /** Checks run.sh asked to leave out (--quick): not run, not reported. */
    static final Set<String> SKIPPED = Set.of(Objects.requireNonNullElse(System.getenv("SPRIG_CONN_SKIP_CHECKS"), "")
            .trim().split("[ ,]+"));
    static final HexFormat HEX = HexFormat.of();
    static Connection conn;

    static final class NA extends Exception {
        NA(String m) { super(m); }
    }

    static final class Skip extends Exception {
        Skip(String m) { super(m); }
    }

    interface CheckFn {
        String run() throws Exception;
    }

    static String json(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
                }
            }
        }
        return b.append('"').toString();
    }

    static void emit(String check, String status, String detail, double ms) {
        if (detail.length() > 300) detail = detail.substring(0, 300);
        System.out.println("{\"connector\":" + json(CONNECTOR) + ",\"tls\":" + json(TLS) + ",\"check\":"
                + json(check) + ",\"status\":" + json(status) + ",\"detail\":" + json(detail) + ",\"ms\":"
                + Math.round(ms * 10) / 10.0 + "}");
        System.out.flush();
    }

    static void expect(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    static String url(String extra) {
        String base = (MYSQL_CJ ? "jdbc:mysql://" : "jdbc:mariadb://") + System.getenv("SPRIG_HOST") + ":"
                + System.getenv("SPRIG_PORT") + "/" + System.getenv("SPRIG_DATABASE");
        String opts;
        if (MYSQL_CJ) {
            opts = "sslMode=" + (TLS.equals("required") ? "REQUIRED" : "DISABLED") // REQUIRED: no verification
                    + "&allowPublicKeyRetrieval=true&allowMultiQueries=true&useServerPrepStmts=true"
                    + "&connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true&characterEncoding=utf8";
        } else {
            opts = "sslMode=" + (TLS.equals("required") ? "trust" : "disable") // trust: no verification
                    + "&allowPublicKeyRetrieval=true&allowMultiQueries=true&useServerPrepStmts=true";
        }
        return base + "?" + opts + extra;
    }

    static Connection connect(String extra) throws SQLException {
        Connection c = DriverManager.getConnection(url(extra), System.getenv("SPRIG_USER"), System.getenv("SPRIG_PASSWORD"));
        try (Statement s = c.createStatement()) {
            s.execute("SET time_zone = '+00:00'");
        }
        return c;
    }

    static String scalar(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            return r.next() ? r.getString(1) : null;
        }
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    // --- the shared data set -----------------------------------------------

    static final List<String> COLS = List.of("c_tiny", "c_small", "c_int", "c_big", "c_ubig", "c_dec", "c_float",
            "c_double", "c_vchar", "c_text", "c_vbin", "c_blob", "c_date", "c_dt", "c_ts", "c_time", "c_json");
    static final String DDL = """
            CREATE TABLE %s (
              id INT PRIMARY KEY,
              c_tiny TINYINT, c_small SMALLINT, c_int INT, c_big BIGINT, c_ubig BIGINT UNSIGNED,
              c_dec DECIMAL(20,6), c_float FLOAT, c_double DOUBLE,
              c_vchar VARCHAR(64), c_text TEXT, c_vbin VARBINARY(16), c_blob BLOB,
              c_date DATE, c_dt DATETIME(6), c_ts TIMESTAMP NULL, c_time TIME, c_json JSON
            ) DEFAULT CHARSET = utf8mb4""";
    static final byte[] BLOB = new byte[256];
    static final byte[] VBIN = {0x00, (byte) 0xFF, (byte) 0x80, 0x7F, 0x0A, 0x0D, 0x27, 0x22, 0x5C};
    static final String VCHAR = "héllo wörld 🌱";
    static final String TEXT = "Ünïcödé 日本語 🚀🌱 'quoted' \"double\" back\\slash";
    static final String JSON_TEXT = "{\"a\": 1, \"b\": [true, null], \"s\": \"🌱\"}";
    static final Map<String, String> EXPECTED = new LinkedHashMap<>();

    static {
        for (int i = 0; i < 256; i++) BLOB[i] = (byte) i;
        String[] vals = {"-128", "32767", "-2147483648", "9223372036854775807", "18446744073709551615",
                "-12345678901234.567890", "1.5", "-0.00225", VCHAR, TEXT, HEX.formatHex(VBIN), HEX.formatHex(BLOB),
                "2024-02-29", "2024-02-29 23:59:58.123456", "2024-02-29 12:34:56", "838:59:59", JSON_TEXT};
        for (int i = 0; i < vals.length; i++) EXPECTED.put(COLS.get(i), vals[i]);
    }

    static String sqlEscape(String s) {
        return s.replace("\\", "\\\\").replace("'", "\\'");
    }

    /** JSON text without insignificant whitespace, for comparing what MySQL re-serializes. */
    static String squashJson(String s) {
        StringBuilder b = new StringBuilder();
        boolean inString = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' && (i == 0 || s.charAt(i - 1) != '\\')) inString = !inString;
            if (inString || !Character.isWhitespace(c)) b.append(c);
        }
        return b.toString();
    }

    static void compareRow(ResultSet r, boolean nulls) throws SQLException {
        List<String> bad = new ArrayList<>();
        for (int i = 0; i < COLS.size(); i++) {
            String col = COLS.get(i);
            String got;
            if (col.equals("c_vbin") || col.equals("c_blob")) {
                byte[] b = r.getBytes(i + 1);
                got = b == null ? null : HEX.formatHex(b);
            } else {
                got = r.getString(i + 1);
            }
            String want = nulls ? null : EXPECTED.get(col);
            boolean ok = Objects.equals(got, want);
            if (!ok && got != null && want != null) {
                if (col.equals("c_json")) ok = squashJson(got).equals(squashJson(want));
                if (col.equals("c_float") || col.equals("c_double"))
                    ok = Double.parseDouble(got) == Double.parseDouble(want);
            }
            if (!ok) bad.add(col + ": got " + got + " want " + want);
        }
        expect(bad.isEmpty(), String.join("; ", bad));
    }

    // --- checks --------------------------------------------------------------

    static String connectCheck() throws Exception {
        conn = connect("");
        try (Statement s = conn.createStatement(); ResultSet r = s.executeQuery("SHOW SESSION STATUS LIKE 'Ssl_cipher'")) {
            r.next();
            String cipher = r.getString(2);
            if (TLS.equals("required")) expect(!cipher.isEmpty(), "TLS required but the session isn't encrypted");
            else expect(cipher.isEmpty(), "TLS disabled but the session is encrypted (" + cipher + ")");
            return "cipher=" + (cipher.isEmpty() ? "none" : cipher);
        }
    }

    static String plainQuery() throws Exception {
        try (Statement s = conn.createStatement(); ResultSet r = s.executeQuery("SELECT 1 + 1, 'sprig', VERSION()")) {
            r.next();
            expect(r.getInt(1) == 2 && r.getString(2).equals("sprig"), "got " + r.getInt(1) + " " + r.getString(2));
            return "server " + r.getString(3);
        }
    }

    static String types() throws Exception {
        String t = PREFIX + "_types";
        exec(conn, "DROP TABLE IF EXISTS " + t);
        exec(conn, DDL.formatted(t));
        exec(conn, "INSERT INTO " + t + " VALUES (1, -128, 32767, -2147483648, 9223372036854775807, "
                + "18446744073709551615, -12345678901234.567890, 1.5, -0.00225, '" + sqlEscape(VCHAR) + "', '"
                + sqlEscape(TEXT) + "', X'" + HEX.formatHex(VBIN) + "', X'" + HEX.formatHex(BLOB) + "', '2024-02-29', "
                + "'2024-02-29 23:59:58.123456', '2024-02-29 12:34:56', '838:59:59', '" + sqlEscape(JSON_TEXT) + "'), "
                + "(2" + ", NULL".repeat(17) + ")");
        // A plain Statement: the text protocol (COM_QUERY).
        try (Statement s = conn.createStatement();
             ResultSet r = s.executeQuery("SELECT " + String.join(", ", COLS) + " FROM " + t + " ORDER BY id")) {
            expect(r.next(), "no rows");
            compareRow(r, false);
            expect(r.next(), "one row only");
            compareRow(r, true);
            expect(!r.next(), "more than 2 rows");
        }
        exec(conn, "DROP TABLE " + t);
        return "17 columns, 2 rows";
    }

    static String prepared() throws Exception {
        String t = PREFIX + "_ps";
        exec(conn, "DROP TABLE IF EXISTS " + t);
        exec(conn, DDL.formatted(t));
        // useServerPrepStmts=true: COM_STMT_PREPARE + COM_STMT_EXECUTE.
        try (PreparedStatement p = conn.prepareStatement("INSERT INTO " + t + " VALUES (?" + ", ?".repeat(COLS.size()) + ")")) {
            p.setInt(1, 10);
            p.setByte(2, (byte) -128);
            p.setShort(3, (short) 32767);
            p.setInt(4, Integer.MIN_VALUE);
            p.setLong(5, Long.MAX_VALUE);
            p.setString(6, "18446744073709551615"); // BigInteger: see preparedBigInteger
            p.setBigDecimal(7, new BigDecimal("-12345678901234.567890"));
            p.setFloat(8, 1.5f);
            p.setDouble(9, -0.00225);
            p.setString(10, VCHAR);
            p.setString(11, TEXT);
            p.setBytes(12, VBIN);
            p.setBytes(13, BLOB);
            p.setObject(14, LocalDate.of(2024, 2, 29));
            p.setObject(15, LocalDateTime.of(2024, 2, 29, 23, 59, 58, 123_456_000));
            p.setObject(16, LocalDateTime.of(2024, 2, 29, 12, 34, 56));
            p.setString(17, "838:59:59");
            p.setString(18, JSON_TEXT);
            p.executeUpdate();
            // Re-execute the same statement with NULLs.
            p.setInt(1, 11);
            for (int i = 2; i <= COLS.size() + 1; i++) p.setNull(i, Types.NULL);
            p.executeUpdate();
        }
        try (PreparedStatement q = conn.prepareStatement("SELECT " + String.join(", ", COLS) + " FROM " + t + " WHERE id = ?")) {
            for (int id : new int[] {10, 11}) {
                q.setInt(1, id);
                try (ResultSet r = q.executeQuery()) {
                    expect(r.next(), "id " + id + ": no row");
                    try {
                        compareRow(r, id == 11);
                    } catch (AssertionError e) {
                        throw new AssertionError("id " + id + ": " + e.getMessage());
                    }
                }
            }
        }
        exec(conn, "DROP TABLE " + t);
        return "server-side, 2 executions each";
    }

    /**
     * BIGINT UNSIGNED above Long.MAX_VALUE bound as a BigInteger with a
     * server-side prepared statement. MySQL Connector/J 9.7 against MariaDB
     * fails here (1264, out of range) while MySQL 8.4 accepts it; recorded in
     * expected.toml.
     */
    static String preparedBigInteger() throws Exception {
        String t = PREFIX + "_ubig";
        exec(conn, "DROP TABLE IF EXISTS " + t);
        exec(conn, "CREATE TABLE " + t + " (id INT PRIMARY KEY, v BIGINT UNSIGNED)");
        try (PreparedStatement p = conn.prepareStatement("INSERT INTO " + t + " VALUES (?, ?)")) {
            p.setInt(1, 1);
            p.setObject(2, new BigInteger("18446744073709551615"));
            p.executeUpdate();
            String v = scalar(conn, "SELECT v FROM " + t);
            expect("18446744073709551615".equals(v), "got " + v);
            return "";
        } finally {
            exec(conn, "DROP TABLE IF EXISTS " + t);
        }
    }

    static String transaction() throws Exception {
        String t = PREFIX + "_tx";
        exec(conn, "DROP TABLE IF EXISTS " + t);
        exec(conn, "CREATE TABLE " + t + " (id INT PRIMARY KEY) ENGINE = InnoDB");
        conn.setAutoCommit(false);
        try {
            exec(conn, "INSERT INTO " + t + " VALUES (1)");
            conn.rollback();
            expect(scalar(conn, "SELECT COUNT(*) FROM " + t).equals("0"), "row survived rollback");
            exec(conn, "INSERT INTO " + t + " VALUES (2)");
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        try (Connection other = connect("")) {
            expect(scalar(other, "SELECT COUNT(*) FROM " + t + " WHERE id = 2").equals("1"),
                    "committed row not visible to another session");
        }
        exec(conn, "DROP TABLE " + t);
        return "";
    }

    static String multiStatement() throws Exception {
        try (Statement s = conn.createStatement()) {
            expect(s.execute("SELECT 1 AS a; SELECT 2 AS b, 'two' AS c"), "first result isn't a resultset");
            String first;
            try (ResultSet r = s.getResultSet()) {
                r.next();
                first = r.getString(1);
            }
            expect(s.getMoreResults(), "no second result");
            String second;
            try (ResultSet r = s.getResultSet()) {
                r.next();
                second = r.getString(1) + "," + r.getString(2);
            }
            expect(!s.getMoreResults() && s.getUpdateCount() == -1, "unexpected third result");
            expect(first.equals("1") && second.equals("2,two"), "got " + first + " " + second);
        }
        return "2 resultsets";
    }

    static String duplicateKey() throws Exception {
        String t = PREFIX + "_dup";
        exec(conn, "DROP TABLE IF EXISTS " + t);
        exec(conn, "CREATE TABLE " + t + " (id INT PRIMARY KEY)");
        exec(conn, "INSERT INTO " + t + " VALUES (1)");
        try {
            exec(conn, "INSERT INTO " + t + " VALUES (1)");
        } catch (SQLException e) {
            expect(e.getErrorCode() == 1062 && "23000".equals(e.getSQLState()),
                    "got " + e.getErrorCode() + "/" + e.getSQLState() + " " + e.getClass().getSimpleName());
            exec(conn, "DROP TABLE " + t);
            return "1062/23000 " + e.getClass().getSimpleName();
        }
        throw new AssertionError("no error raised");
    }

    static long generatedKey(Statement s) throws SQLException {
        try (ResultSet k = s.getGeneratedKeys()) {
            expect(k.next(), "no generated key");
            return k.getLong(1);
        }
    }

    static String lastInsertId() throws Exception {
        String t = PREFIX + "_ai";
        exec(conn, "DROP TABLE IF EXISTS " + t);
        exec(conn, "CREATE TABLE " + t + " (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT = 100");
        long first, multi;
        try (Statement s = conn.createStatement()) {
            s.executeUpdate("INSERT INTO " + t + " (v) VALUES (1)", Statement.RETURN_GENERATED_KEYS);
            first = generatedKey(s);
            s.executeUpdate("INSERT INTO " + t + " (v) VALUES (2), (3), (4)", Statement.RETURN_GENERATED_KEYS);
            multi = generatedKey(s);
        }
        String lid = scalar(conn, "SELECT LAST_INSERT_ID()");
        expect(first == 100, "first insert id " + first);
        expect(multi == 101 && lid.equals("101"), "multi-row insert id " + multi + ", LAST_INSERT_ID() " + lid);
        exec(conn, "DROP TABLE " + t);
        return "";
    }

    static String roundTrip(Connection c, int size, String label) throws Exception {
        String t = PREFIX + "_" + label;
        exec(c, "DROP TABLE IF EXISTS " + t);
        exec(c, "CREATE TABLE " + t + " (id INT PRIMARY KEY, b LONGBLOB)");
        byte[] data = new byte[size];
        new SecureRandom().nextBytes(data);
        try (PreparedStatement p = c.prepareStatement("INSERT INTO " + t + " VALUES (1, ?)")) {
            p.setBytes(1, data);
            p.executeUpdate();
        }
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT b, SHA2(b, 256) FROM " + t + " WHERE id = 1")) {
            expect(r.next(), "no row");
            byte[] got = r.getBytes(1);
            String sha = HEX.formatHex(MessageDigest.getInstance("SHA-256").digest(data));
            expect(sha.equals(r.getString(2)), "server-side hash differs: the upload was corrupted");
            expect(Arrays.equals(got, data), "download differs (" + got.length + " bytes, want " + size + ")");
        }
        exec(c, "DROP TABLE " + t);
        return size + " bytes";
    }

    static String largeRow() throws Exception {
        return roundTrip(conn, 4 * 1024 * 1024, "big");
    }

    static String multiPacket() throws Exception {
        long limit = Long.parseLong(scalar(conn, "SELECT @@max_allowed_packet"));
        int size = 16 * 1024 * 1024 + 1024;
        expect(limit >= 2L * size + 1024, "server max_allowed_packet " + limit + " is too small for " + size + " bytes");
        return roundTrip(conn, size, "mp");
    }

    static String compression() throws Exception {
        try (Connection c = connect("&useCompression=true")) {
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SHOW SESSION STATUS LIKE 'Compression'")) {
                r.next();
                expect(r.getString(2).equals("ON"), "Compression status " + r.getString(2));
            }
            expect(scalar(c, "SELECT LENGTH(REPEAT('a', 1000000))").equals("1000000"), "REPEAT length");
            return "Compression=ON, " + roundTrip(c, 1024 * 1024, "zip");
        }
    }

    static String connAttrs() throws Exception {
        try (Connection c = connect("&connectionAttributes=sprig_check:" + CONNECTOR)) {
            if (!scalar(c, "SELECT @@performance_schema").equals("1"))
                throw new Skip("performance_schema is off on the server");
            String v = scalar(c, "SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs "
                    + "WHERE PROCESSLIST_ID = CONNECTION_ID() AND ATTR_NAME = 'sprig_check'");
            expect(CONNECTOR.equals(v), "sprig_check=" + v);
            return "sprig_check=" + v;
        }
    }

    // --- chapter 05 matrix items ------------------------------------------------

    /** The file LOAD DATA LOCAL INFILE sends, and what the table must hold afterwards. */
    static final String INFILE = "1,héllo 🌱\n2,\"quoted, comma\"\n3,\\N\n";
    static final String INFILE_ROWS = "1=héllo 🌱|2=quoted, comma|3=NULL";

    static String loadSql(String t, Path f) {
        return "LOAD DATA LOCAL INFILE '" + f + "' INTO TABLE " + t + " CHARACTER SET utf8mb4 "
                + "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n'";
    }

    static String loaded(Connection c, String t) throws SQLException {
        return scalar(c, "SELECT IFNULL(GROUP_CONCAT(CONCAT(id, '=', IFNULL(v, 'NULL')) ORDER BY id SEPARATOR '|'), '') FROM " + t);
    }

    static void infileTable(Connection c, String t) throws Exception {
        exec(c, "DROP TABLE IF EXISTS " + t);
        exec(c, "CREATE TABLE " + t + " (id INT PRIMARY KEY, v VARCHAR(32)) DEFAULT CHARSET = utf8mb4");
        if (scalar(c, "SELECT @@GLOBAL.local_infile").equals("0")) throw new Skip("local_infile is off on the server");
    }

    static String localInfile() throws Exception {
        String t = PREFIX + "_infile";
        Path f = Files.createTempFile("sprig", ".csv");
        Files.writeString(f, INFILE, StandardCharsets.UTF_8);
        try (Connection c = connect(MYSQL_CJ ? "&allowLoadLocalInfile=true" : "&allowLocalInfile=true")) {
            infileTable(c, t);
            int n;
            try (Statement s = c.createStatement()) {
                n = s.executeUpdate(loadSql(t, f));
            }
            String rows = loaded(c, t);
            expect(n == 3 && rows.equals(INFILE_ROWS), "loaded " + n + " rows: " + rows);
            exec(c, "DROP TABLE " + t);
            return "3 rows";
        } finally {
            Files.deleteIfExists(f);
        }
    }

    /** The server's refusal when the client doesn't offer CLIENT_LOCAL_FILES, per persona. */
    static int infileRefusal() {
        return PERSONA.equals("mysql") ? 3948 : 4166;
    }

    static String localInfileOff() throws Exception {
        String t = PREFIX + "_infile_off";
        Path f = Files.createTempFile("sprig", ".csv");
        Files.writeString(f, INFILE, StandardCharsets.UTF_8);
        try (Connection c = connect(MYSQL_CJ ? "&allowLoadLocalInfile=false" : "&allowLocalInfile=false")) {
            infileTable(c, t);
            String how;
            try (Statement s = c.createStatement()) {
                s.executeUpdate(loadSql(t, f));
                throw new AssertionError("the load succeeded");
            } catch (SQLException e) {
                how = e.getErrorCode() + "/" + e.getSQLState() + " " + e.getMessage();
                if (e.getErrorCode() == 1148 || e.getErrorCode() == 3948 || e.getErrorCode() == 4166)
                    expect(e.getErrorCode() == infileRefusal(), "server refused with " + how + ", want " + infileRefusal());
            }
            expect(loaded(c, t).isEmpty(), "rows were loaded: " + loaded(c, t));
            exec(c, "DROP TABLE " + t);
            return "refused: " + how;
        } finally {
            Files.deleteIfExists(f);
        }
    }

    /** Two resultsets, then an OUT and an INOUT parameter. */
    static final String PROC = """
            CREATE PROCEDURE %s(IN a INT, OUT b INT, INOUT c INT)
            BEGIN
              SELECT a AS x;
              SELECT a + 1 AS y, 'two' AS z UNION ALL SELECT a + 2, 'three';
              SET b = a * 10;
              SET c = c + 1;
            END""";

    static String call() throws Exception {
        String p = PREFIX + "_proc";
        exec(conn, "DROP PROCEDURE IF EXISTS " + p);
        exec(conn, PROC.formatted(p));
        List<String> results = new ArrayList<>();
        int b, c;
        try (CallableStatement cs = conn.prepareCall("{CALL " + p + "(?, ?, ?)}")) {
            cs.setInt(1, 1);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.setInt(3, 5);
            cs.registerOutParameter(3, Types.INTEGER);
            boolean isRs = cs.execute();
            while (true) {
                if (isRs) {
                    StringBuilder r = new StringBuilder();
                    try (ResultSet rs = cs.getResultSet()) {
                        int n = rs.getMetaData().getColumnCount();
                        while (rs.next()) {
                            for (int i = 1; i <= n; i++) r.append(i > 1 ? "," : "").append(rs.getString(i));
                            r.append(';');
                        }
                    }
                    results.add(r.toString());
                } else if (cs.getUpdateCount() == -1) {
                    break;
                }
                isRs = cs.getMoreResults();
            }
            b = cs.getInt(2);
            c = cs.getInt(3);
        }
        exec(conn, "DROP PROCEDURE " + p);
        // Drivers may surface the OUT-parameter row as a resultset of their own; the first two must be ours.
        expect(results.size() >= 2 && results.get(0).equals("1;") && results.get(1).equals("2,two;3,three;"),
                "resultsets " + results);
        expect(b == 10 && c == 6, "OUT b=" + b + " INOUT c=" + c);
        return results.size() + " resultsets, OUT 10, INOUT 6";
    }

    /** Leaves state a session reset must clear; returns the connection id. */
    static String markSession(Connection c) throws SQLException {
        exec(c, "SET @sprig_mark = 42, SESSION auto_increment_increment = 7");
        exec(c, "CREATE TEMPORARY TABLE sprig_tmp (id INT)");
        return scalar(c, "SELECT CONNECTION_ID()");
    }

    static String assertReset(Connection c, String id) throws SQLException {
        String state;
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery("SELECT CONNECTION_ID(), @sprig_mark, @@SESSION.auto_increment_increment, CURRENT_USER()")) {
            r.next();
            state = "id " + r.getString(1) + ", @sprig_mark " + r.getString(2) + ", auto_increment_increment " + r.getString(3);
            expect(r.getString(1).equals(id), "connection id changed from " + id + ": " + state);
            expect(r.getString(2) == null && r.getString(3).equals("1"), "session not reset: " + state);
        }
        try {
            scalar(c, "SELECT COUNT(*) FROM sprig_tmp");
            throw new AssertionError("the temporary table survived");
        } catch (SQLException e) {
            expect(e.getErrorCode() == 1146, "temporary table probe: " + e.getErrorCode() + " " + e.getMessage());
        }
        expect(scalar(c, "SELECT 1 + 1").equals("2"), "session unusable after the reset");
        return "same connection " + id + "; user variable, session variable and temporary table cleared";
    }

    static String changeUser() throws Exception {
        if (!MYSQL_CJ) throw new NA("MariaDB Connector/J 3 has no change-user API");
        try (Connection c = connect("")) {
            String id = markSession(c);
            Class<?> jc = Class.forName("com.mysql.cj.jdbc.JdbcConnection");
            jc.getMethod("changeUser", String.class, String.class)
                    .invoke(c.unwrap(jc), System.getenv("SPRIG_USER"), System.getenv("SPRIG_PASSWORD"));
            return "JdbcConnection.changeUser (COM_CHANGE_USER): " + assertReset(c, id);
        }
    }

    static String resetConnection() throws Exception {
        // MariaDB Connector/J's reset() sends COM_RESET_CONNECTION only with useResetConnection.
        try (Connection c = connect(MYSQL_CJ ? "" : "&useResetConnection=true")) {
            String id = markSession(c);
            String how;
            if (MYSQL_CJ) {
                Class<?> jc = Class.forName("com.mysql.cj.jdbc.JdbcConnection");
                jc.getMethod("resetServerState").invoke(c.unwrap(jc));
                how = "JdbcConnection.resetServerState";
            } else {
                // Connector/J 3.5 sends COM_RESET_CONNECTION only to a server whose version says MariaDB;
                // against MySQL (and the gateway's 3306) reset() only clears the driver's own state.
                if (!scalar(c, "SELECT VERSION()").contains("MariaDB"))
                    throw new NA("MariaDB Connector/J's reset() sends COM_RESET_CONNECTION only to MariaDB servers");
                Class<?> mc = Class.forName("org.mariadb.jdbc.Connection");
                mc.getMethod("reset").invoke(c.unwrap(mc));
                how = "Connection.reset with useResetConnection=true";
            }
            return how + " (COM_RESET_CONNECTION): " + assertReset(c, id);
        }
    }

    /**
     * A one-connection HikariCP pool with the driver's prepared-statement
     * cache: execute a server-side prepared statement, KILL the pooled
     * connection from another session, then prepare and execute again. The
     * pool must notice and reconnect, and the statement is prepared afresh.
     */
    static String poolReprepare() throws Exception {
        System.setProperty("com.zaxxer.hikari.aliveBypassWindowMs", "0"); // validate on every borrow
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(url(MYSQL_CJ ? "&cachePrepStmts=true" : "&cachePrepStmts=true&prepStmtCacheSize=50"));
        hc.setUsername(System.getenv("SPRIG_USER"));
        hc.setPassword(System.getenv("SPRIG_PASSWORD"));
        hc.setMaximumPoolSize(1);
        hc.setPoolName("sprig-" + CONNECTOR);
        String sql = "SELECT ? + 1, CONNECTION_ID()";
        try (HikariDataSource ds = new HikariDataSource(hc)) {
            String id1, id2;
            for (int round = 0; round < 2; round++) {
                try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
                    p.setInt(1, 41);
                    try (ResultSet r = p.executeQuery()) {
                        r.next();
                        expect(r.getInt(1) == 42, "got " + r.getInt(1));
                    }
                }
            }
            try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
                p.setInt(1, 1);
                try (ResultSet r = p.executeQuery()) {
                    r.next();
                    id1 = r.getString(2);
                }
            }
            exec(conn, "KILL " + id1);
            waitGone(id1);
            try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
                p.setInt(1, 99);
                try (ResultSet r = p.executeQuery()) {
                    r.next();
                    expect(r.getInt(1) == 100, "after the kill: got " + r.getInt(1));
                    id2 = r.getString(2);
                }
            }
            expect(!id1.equals(id2), "the killed connection " + id1 + " was handed out again");
            return "HikariCP 6.3.0: connection " + id1 + " killed, re-prepared on " + id2;
        }
    }

    /** Waits until a killed session has left the processlist (its socket is closed). */
    static void waitGone(String id) throws Exception {
        for (int i = 0; i < 100 && !scalar(conn, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = " + id).equals("0"); i++) Thread.sleep(50);
        Thread.sleep(100);
    }

    static String lockTable(Connection c) throws SQLException {
        String t = PREFIX + "_lock";
        exec(c, "DROP TABLE IF EXISTS " + t);
        exec(c, "CREATE TABLE " + t + " (id INT PRIMARY KEY, v INT) ENGINE = InnoDB");
        exec(c, "INSERT INTO " + t + " VALUES (1, 0), (2, 0)");
        return t;
    }

    static String lockWait() throws Exception {
        String t = lockTable(conn);
        try (Connection a = connect(""); Connection b = connect("")) {
            a.setAutoCommit(false);
            scalar(a, "SELECT v FROM " + t + " WHERE id = 1 FOR UPDATE");
            exec(b, "SET SESSION innodb_lock_wait_timeout = 1");
            b.setAutoCommit(false);
            scalar(b, "SELECT v FROM " + t + " WHERE id = 2 FOR UPDATE");
            long t0 = System.nanoTime();
            String got;
            try {
                scalar(b, "SELECT v FROM " + t + " WHERE id = 1 FOR UPDATE");
                throw new AssertionError("the second session got the lock");
            } catch (SQLException e) {
                got = e.getErrorCode() + "/" + e.getSQLState();
                expect(e.getErrorCode() == 1205, "got " + got + " " + e.getMessage());
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            expect(ms >= 900 && ms < 10_000, "timed out after " + ms + " ms");
            // Only the statement is rolled back: the transaction still holds row 2.
            exec(b, "UPDATE " + t + " SET v = 2 WHERE id = 2");
            b.commit();
            a.commit();
            expect(scalar(conn, "SELECT v FROM " + t + " WHERE id = 2").equals("2"), "the waiting transaction lost its work");
            exec(conn, "DROP TABLE " + t);
            return got + " after " + ms + " ms; the transaction survived";
        }
    }

    static String lockNowait() throws Exception {
        String t = lockTable(conn);
        int want = PERSONA.equals("mysql") ? 3572 : 1205;
        String got;
        try (Connection a = connect(""); Connection b = connect("")) {
            a.setAutoCommit(false);
            scalar(a, "SELECT v FROM " + t + " WHERE id = 1 FOR UPDATE");
            b.setAutoCommit(false);
            try {
                scalar(b, "SELECT v FROM " + t + " WHERE id = 1 FOR UPDATE NOWAIT");
                throw new AssertionError("NOWAIT got the lock");
            } catch (SQLException e) {
                got = e.getErrorCode() + "/" + e.getSQLState();
                expect(e.getErrorCode() == want, "NOWAIT: got " + got + " " + e.getMessage() + ", want " + want);
            }
            StringBuilder ids = new StringBuilder();
            try (Statement s = b.createStatement();
                 ResultSet r = s.executeQuery("SELECT id FROM " + t + " ORDER BY id FOR UPDATE SKIP LOCKED")) {
                while (r.next()) ids.append(r.getString(1)).append(' ');
            }
            expect(ids.toString().equals("2 "), "SKIP LOCKED returned " + ids);
            b.rollback();
            a.rollback();
        }
        exec(conn, "DROP TABLE " + t);
        return "NOWAIT " + got + ", SKIP LOCKED returned row 2";
    }

    public static void main(String[] args) {
        Map<String, CheckFn> checks = new LinkedHashMap<>();
        checks.put("connect", Check::connectCheck);
        checks.put("plain_query", Check::plainQuery);
        checks.put("types", Check::types);
        checks.put("prepared", Check::prepared);
        checks.put("prepared_biginteger", Check::preparedBigInteger);
        checks.put("transaction", Check::transaction);
        checks.put("multi_statement", Check::multiStatement);
        checks.put("duplicate_key", Check::duplicateKey);
        checks.put("last_insert_id", Check::lastInsertId);
        checks.put("large_row", Check::largeRow);
        checks.put("multi_packet", Check::multiPacket);
        checks.put("compression", Check::compression);
        checks.put("conn_attrs", Check::connAttrs);
        checks.put("local_infile", Check::localInfile);
        checks.put("local_infile_off", Check::localInfileOff);
        checks.put("call", Check::call);
        checks.put("change_user", Check::changeUser);
        checks.put("reset_connection", Check::resetConnection);
        checks.put("pool_reprepare", Check::poolReprepare);
        checks.put("lock_wait", Check::lockWait);
        checks.put("lock_nowait", Check::lockNowait);
        for (var e : checks.entrySet()) {
            String name = e.getKey();
            if (SKIPPED.contains(name)) continue;
            if (conn == null && !name.equals("connect")) {
                emit(name, "fail", "no connection", 0);
                continue;
            }
            long t0 = System.nanoTime();
            try {
                String detail = e.getValue().run();
                emit(name, "pass", detail == null ? "" : detail, (System.nanoTime() - t0) / 1e6);
            } catch (NA x) {
                emit(name, "n/a", x.getMessage(), 0);
            } catch (Skip x) {
                emit(name, "skip", x.getMessage(), 0);
            } catch (Throwable x) {
                emit(name, "fail", x.getClass().getSimpleName() + ": " + x.getMessage(), (System.nanoTime() - t0) / 1e6);
            }
        }
        try {
            if (conn != null) conn.close();
        } catch (SQLException ignored) {
            // closing is best effort
        }
    }
}
