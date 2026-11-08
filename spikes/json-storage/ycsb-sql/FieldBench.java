// Throwaway spike: secondary-field filters and typed updates, which YCSB never issues.
// Targets: "mongo" (MongoDB), "proto" (the prototype; both through mongo-java-driver 3.8,
// the driver YCSB 0.17 uses) and "sql" (the SQL the prototype generates, over JDBC).
//
//   java FieldBench <target> <url> load <n>
//   java FieldBench <target> <url> run <op> <threads> <seconds> <n>
//   ops: eq   find({"customer.id": k})                                     index {customer.id: 1}
//        top  find({status: s, created: {$lte: d}}).sort({created: -1}).limit(20)
//                                                                          index {status: 1, created: -1}
//        upd  updateOne({_id}, {$set: {status: s}, $inc: {qty: NumberLong(1)}})
//        ins  insertOne(order with ObjectId, int64, Decimal128, date, subdocument)
package sprig.spike;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.HdrHistogram.Histogram;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class FieldBench {
  static final long BASE_MS = 1_767_225_600_000L; // 2026-01-01
  static final int CUSTOMERS = 50_000, STATUSES = 20;
  static final AtomicLong HLC = new AtomicLong();
  static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
  static final DateTimeFormatter SQLDT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

  static ObjectId oid(long i) {
    return new ObjectId(ByteBuffer.allocate(12).putInt(0x6721c5a4).putLong(i).array());
  }

  static Document order(ObjectId id, long i) {
    return new Document("_id", id).append("status", "s" + (i % STATUSES))
        .append("customer", new Document("id", (int) (i % CUSTOMERS)))
        .append("qty", Long.valueOf(i % 100)).append("total", new Decimal128(new BigDecimal("59.90")))
        .append("created", new java.util.Date(BASE_MS + i * 1000));
  }

  interface Op { void run(ThreadLocalRandom r) throws Exception; }

  public static void main(String[] a) throws Exception {
    String target = a[0], url = a[1], phase = a[2];
    if (phase.equals("load")) {
      int n = Integer.parseInt(a[3]);
      MongoClient c = new MongoClient(new MongoClientURI(url));
      MongoCollection<Document> coll = c.getDatabase("bench").getCollection("orders");
      coll.drop();
      long t0 = System.nanoTime();
      List<Document> batch = new ArrayList<>();
      for (long i = 0; i < n; i++) {
        batch.add(order(oid(i), i));
        if (batch.size() == 1000) { coll.insertMany(batch); batch.clear(); }
      }
      if (!batch.isEmpty()) coll.insertMany(batch);
      double s = (System.nanoTime() - t0) / 1e9;
      coll.createIndex(Indexes.ascending("customer.id"));
      coll.createIndex(Indexes.compoundIndex(Indexes.ascending("status"), Indexes.descending("created")));
      System.out.printf("load %s n=%d %.1fs %.0f docs/s (insertMany 1000, 1 thread)%n", target, n, s, n / s);
      c.close();
      return;
    }
    String op = a[3];
    int threads = Integer.parseInt(a[4]), secs = Integer.parseInt(a[5]), n = Integer.parseInt(a[6]);
    List<Thread> ts = new ArrayList<>();
    List<Histogram> hs = Collections.synchronizedList(new ArrayList<>());
    long warmEnd = System.nanoTime() + 3_000_000_000L, end = warmEnd + secs * 1_000_000_000L;
    MongoClient mc = target.equals("sql") ? null : new MongoClient(new MongoClientURI(url));
    for (int t = 0; t < threads; t++) {
      Op o = target.equals("sql") ? sqlOp(url, op, n) : mongoOp(mc.getDatabase("bench").getCollection("orders"), op, n);
      Thread th = new Thread(() -> {
        Histogram h = new Histogram(60_000_000L, 3);
        ThreadLocalRandom r = ThreadLocalRandom.current();
        try {
          while (true) {
            long s = System.nanoTime();
            if (s > end) break;
            o.run(r);
            long e = System.nanoTime();
            if (s > warmEnd) h.recordValue((e - s) / 1000);
          }
        } catch (Exception ex) { ex.printStackTrace(); }
        hs.add(h);
      });
      ts.add(th);
      th.start();
    }
    for (Thread th : ts) th.join();
    Histogram all = new Histogram(60_000_000L, 3);
    for (Histogram h : hs) all.add(h);
    System.out.printf("%s %s t=%d ops/s=%.0f p50=%d p95=%d p99=%d avg=%.0f (us)%n", target, op, threads,
        all.getTotalCount() / (double) secs, all.getValueAtPercentile(50), all.getValueAtPercentile(95),
        all.getValueAtPercentile(99), all.getMean());
    if (mc != null) mc.close();
  }

  static Op mongoOp(MongoCollection<Document> c, String op, int n) {
    switch (op) {
      case "eq": return r -> { for (Document d : c.find(Filters.eq("customer.id", r.nextInt(CUSTOMERS)))) d.size(); };
      case "top": return r -> {
        for (Document d : c.find(Filters.and(Filters.eq("status", "s" + r.nextInt(STATUSES)),
            Filters.lte("created", new java.util.Date(BASE_MS + r.nextLong(n) * 1000))))
            .sort(Sorts.descending("created")).limit(20)) d.size();
      };
      case "upd": return r -> {
        if (c.updateOne(Filters.eq("_id", oid(r.nextLong(n))), Updates.combine(Updates.set("status", "s" + r.nextInt(STATUSES)),
            Updates.inc("qty", 1L))).getMatchedCount() != 1) throw new IllegalStateException("no match");
      };
      case "ins": return r -> c.insertOne(order(new ObjectId(), r.nextLong(n)));
      default: throw new IllegalArgumentException(op);
    }
  }

  // The statements the prototype logs with SPIKE_LOG_SQL=1, sent straight to MariaDB.
  static Op sqlOp(String url, String op, int n) throws SQLException {
    Connection conn = DriverManager.getConnection(url, "root", "spike");
    String t = "`bench`.`orders`";
    switch (op) {
      case "eq": {
        PreparedStatement s = conn.prepareStatement("SELECT doc, _types FROM " + t + " WHERE `_sprig_p_customer_id` = ? LIMIT 100000 OFFSET 0");
        return r -> { s.setLong(1, r.nextInt(CUSTOMERS)); drain(s); };
      }
      case "top": {
        PreparedStatement s = conn.prepareStatement("SELECT doc, _types FROM " + t + " WHERE `_sprig_p_status` = ? AND `_sprig_p_created` <= ? ORDER BY `_sprig_p_created` DESC, _id LIMIT 20 OFFSET 0");
        return r -> {
          s.setString(1, "s" + r.nextInt(STATUSES));
          s.setString(2, ISO.format(Instant.ofEpochMilli(BASE_MS + r.nextLong(n) * 1000)));
          drain(s);
        };
      }
      case "upd": {
        PreparedStatement s = conn.prepareStatement("UPDATE " + t + " SET _v = GREATEST(?, _v + 1), doc = JSON_SET(doc, '$.status', ?, '$.qty', IFNULL(CAST(JSON_VALUE(doc,'$.qty') AS SIGNED), 0) + ?), _types = JSON_SET(COALESCE(JSON_REMOVE(_types, '$.\"status\"'), '{}'), '$.\"qty\"', 'long') WHERE _id = ? AND (_types IS NULL OR JSON_SEARCH(JSON_KEYS(_types),'one','status.%') IS NULL) AND (JSON_EXTRACT(doc,'$.qty') IS NULL OR JSON_TYPE(JSON_EXTRACT(doc,'$.qty')) = 'INTEGER') AND COALESCE(JSON_VALUE(_types,'$.\"qty\"'),'long') = 'long' LIMIT 1");
        return r -> {
          s.setLong(1, stamp());
          s.setString(2, "s" + r.nextInt(STATUSES));
          s.setLong(3, 1);
          s.setBytes(4, key(oid(r.nextLong(n))));
          if (s.executeUpdate() != 1) throw new IllegalStateException("no match");
        };
      }
      case "ins": {
        PreparedStatement s = conn.prepareStatement("INSERT INTO " + t + " (_id, doc, _types, _v) VALUES (?,?,?,?)");
        return r -> {
          ObjectId id = new ObjectId();
          long i = r.nextLong(n);
          s.setBytes(1, key(id));
          s.setString(2, "{\"_id\":\"" + id.toHexString() + "\",\"status\":\"s" + (i % STATUSES) + "\",\"customer\":{\"id\":"
              + (i % CUSTOMERS) + "},\"qty\":" + (i % 100) + ",\"total\":59.90,\"created\":\""
              + ISO.format(Instant.ofEpochMilli(BASE_MS + i * 1000)) + "\"}");
          s.setString(3, "{\"_id\":\"objectId\",\"qty\":\"long\",\"total\":\"decimal\",\"created\":\"date\"}");
          s.setLong(4, stamp());
          s.executeUpdate();
        };
      }
      default: throw new IllegalArgumentException(op);
    }
  }

  static void drain(PreparedStatement s) throws SQLException {
    try (ResultSet rs = s.executeQuery()) { while (rs.next()) { rs.getBytes(1); rs.getBytes(2); } }
  }

  static byte[] key(ObjectId id) {
    byte[] out = new byte[13];
    out[0] = 80;
    System.arraycopy(id.toByteArray(), 0, out, 1, 12);
    return out;
  }

  static long stamp() {
    long now = System.currentTimeMillis() << 16;
    return HLC.updateAndGet(c -> Math.max(now, c + 1));
  }
}
