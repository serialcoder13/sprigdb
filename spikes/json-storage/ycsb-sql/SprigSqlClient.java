// Throwaway spike: a YCSB binding that sends MariaDB exactly the SQL the json-storage
// prototype generates (chapter 31 layout), so the prototype's own cost can be separated
// from MariaDB's. Documents are rendered client-side the same way (base64 + _types).
package sprig.spike;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SprigSqlClient extends DB {
  private static final AtomicLong HLC = new AtomicLong();
  private Connection conn;
  private String schema;
  private final Map<String, PreparedStatement> stmts = new HashMap<>();

  @Override
  public void init() throws DBException {
    Properties p = getProperties();
    schema = p.getProperty("sprig.schema", "ycsb");
    try {
      conn = DriverManager.getConnection(
          p.getProperty("sprig.url", "jdbc:mariadb://maria:3306/?useServerPrepStmts=true&cachePrepStmts=true"),
          p.getProperty("sprig.user", "root"), p.getProperty("sprig.password", "spike"));
      conn.setAutoCommit(true);
      try (Statement s = conn.createStatement()) {
        s.execute("CREATE DATABASE IF NOT EXISTS `" + schema + "`");
        s.execute("CREATE TABLE IF NOT EXISTS `" + schema + "`.`usertable` (_id VARBINARY(1024) NOT NULL, "
            + "_t VARCHAR(255) NULL, doc JSON NOT NULL, _types JSON NULL, _v BIGINT UNSIGNED NOT NULL, "
            + "PRIMARY KEY (_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin");
      }
    } catch (SQLException e) {
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new DBException(e);
    }
  }

  private PreparedStatement ps(String sql) throws SQLException {
    PreparedStatement s = stmts.get(sql);
    if (s == null) {
      s = conn.prepareStatement(sql);
      stmts.put(sql, s);
    }
    return s;
  }

  private static long stamp() {
    long now = System.currentTimeMillis() << 16;
    return HLC.updateAndGet(c -> Math.max(now, c + 1));
  }

  /** chapter 31 _id encoding for a string: class byte 40, then UTF-8 (YCSB keys hold no NUL). */
  private static byte[] key(String k) {
    byte[] b = k.getBytes(StandardCharsets.UTF_8);
    byte[] out = new byte[b.length + 1];
    out[0] = 40;
    System.arraycopy(b, 0, out, 1, b.length);
    return out;
  }

  private static final Base64.Encoder B64 = Base64.getEncoder();

  private String tbl(String table) {
    return "`" + schema + "`.`" + table + "`";
  }

  private static void fill(ResultSet rs, Map<String, ByteIterator> result) throws SQLException {
    // The gateway would turn this into BSON; raw SQL just hands back the text.
    result.put("doc", new ByteArrayByteIterator(rs.getBytes(1)));
    byte[] t = rs.getBytes(2);
    if (t != null) {
      result.put("_types", new ByteArrayByteIterator(t));
    }
  }

  @Override
  public Status read(String table, String k, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      PreparedStatement s = ps("SELECT doc, _types FROM " + tbl(table) + " WHERE _id = ? LIMIT 1 OFFSET 0");
      s.setBytes(1, key(k));
      try (ResultSet rs = s.executeQuery()) {
        if (!rs.next()) {
          return Status.NOT_FOUND;
        }
        fill(rs, result);
      }
      return Status.OK;
    } catch (SQLException e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int n, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      PreparedStatement s = ps("SELECT doc, _types FROM " + tbl(table)
          + " WHERE _id >= ? AND _id >= ? AND _id < ? ORDER BY _id LIMIT " + n + " OFFSET 0");
      s.setBytes(1, key(startkey));
      s.setBytes(2, new byte[] {40});
      s.setBytes(3, new byte[] {41});
      try (ResultSet rs = s.executeQuery()) {
        while (rs.next()) {
          HashMap<String, ByteIterator> m = new HashMap<>();
          fill(rs, m);
          result.add(m);
        }
      }
      return result.isEmpty() ? Status.NOT_FOUND : Status.OK;
    } catch (SQLException e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String k, Map<String, ByteIterator> values) {
    // Same statement as the prototype's $set pushdown: JSON_SET of each field, the type map
    // entry binData:0, and the "no entries under p" guard per field.
    StringBuilder docArgs = new StringBuilder();
    StringBuilder tyArgs = new StringBuilder();
    StringBuilder guards = new StringBuilder();
    List<String> vals = new ArrayList<>();
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      String f = e.getKey();
      docArgs.append(", '$.").append(f).append("', ?");
      tyArgs.append(tyArgs.length() == 0 ? "" : ", ").append("'$.\"").append(f).append("\"', ?");
      guards.append(" AND (_types IS NULL OR JSON_SEARCH(JSON_KEYS(_types),'one','").append(f).append(".%') IS NULL)");
      vals.add(B64.encodeToString(e.getValue().toArray()));
    }
    String sql = "UPDATE " + tbl(table) + " SET _v = GREATEST(?, _v + 1), doc = JSON_SET(doc" + docArgs
        + "), _types = JSON_SET(COALESCE(_types, '{}'), " + tyArgs + ") WHERE _id = ?" + guards + " LIMIT 1";
    try {
      PreparedStatement s = ps(sql);
      int i = 1;
      s.setLong(i++, stamp());
      for (String v : vals) {
        s.setString(i++, v);
      }
      for (int j = 0; j < vals.size(); j++) {
        s.setString(i++, "binData:0");
      }
      s.setBytes(i, key(k));
      return s.executeUpdate() == 1 ? Status.OK : Status.NOT_FOUND;
    } catch (SQLException e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String k, Map<String, ByteIterator> values) {
    StringBuilder doc = new StringBuilder(2048).append("{\"_id\":\"").append(k).append('"');
    StringBuilder types = new StringBuilder(256).append('{');
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      doc.append(",\"").append(e.getKey()).append("\":\"").append(B64.encodeToString(e.getValue().toArray())).append('"');
      types.append(types.length() == 1 ? "" : ",").append('"').append(e.getKey()).append("\":\"binData:0\"");
    }
    doc.append('}');
    types.append('}');
    try {
      PreparedStatement s = ps("INSERT INTO " + tbl(table) + " (_id, doc, _types, _v) VALUES (?,?,?,?)");
      s.setBytes(1, key(k));
      s.setString(2, doc.toString());
      s.setString(3, types.toString());
      s.setLong(4, stamp());
      s.executeUpdate();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String k) {
    try {
      PreparedStatement s = ps("DELETE FROM " + tbl(table) + " WHERE _id = ? LIMIT 1");
      s.setBytes(1, key(k));
      return s.executeUpdate() == 1 ? Status.OK : Status.NOT_FOUND;
    } catch (SQLException e) {
      System.err.println(e);
      return Status.ERROR;
    }
  }
}
