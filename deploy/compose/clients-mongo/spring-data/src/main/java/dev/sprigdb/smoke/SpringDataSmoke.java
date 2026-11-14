package dev.sprigdb.smoke;

import static org.springframework.data.mongodb.core.query.Criteria.where;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * SprigDB's smoke suite for Spring Data MongoDB (authentication, CRUD through the
 * template and repositories, types, indexes, aggregation, transactions, errors, metadata).
 * SPRIG_MONGO_URI names the endpoint (a tenant login on the gateway, or a MongoDB server). A plain
 * program (no test framework): each check prints PASS or FAIL with its reason, and the exit code
 * is the number of failures.
 */
public class SpringDataSmoke {
  static void assertTrue(boolean b) {
    if (!b) throw new AssertionError("expected true");
  }

  static void assertNotNull(Object o) {
    if (o == null) throw new AssertionError("expected a value");
  }

  static void assertEquals(Object want, Object got) {
    boolean same =
        (want instanceof Number a && got instanceof Number b && !(a instanceof Double))
            ? a.longValue() == b.longValue()
            : java.util.Objects.equals(want, got);
    if (!same) throw new AssertionError("expected " + want + ", got " + got);
  }

  static void assertArrayEquals(byte[] want, byte[] got) {
    if (!java.util.Arrays.equals(want, got)) throw new AssertionError("bytes differ");
  }

  interface Body {
    void run() throws Throwable;
  }

  static <T extends Throwable> void assertThrows(Class<T> type, Body body) {
    try {
      body.run();
    } catch (Throwable t) {
      if (type.isInstance(t)) return;
      throw new AssertionError("expected " + type.getSimpleName() + ", got " + t, t);
    }
    throw new AssertionError("expected " + type.getSimpleName() + ", nothing thrown");
  }

  public static void main(String[] args) {
    connect();
    SpringDataSmoke s = new SpringDataSmoke();
    Object[][] checks = {
      {"templateCrud", (Body) s::templateCrud},
      {"optimisticLocking", (Body) s::optimisticLocking},
      {"repositoryQueries", (Body) s::repositoryQueries},
      {"typesRoundTrip", (Body) s::typesRoundTrip},
      {"indexesAndDuplicateKeys", (Body) s::indexesAndDuplicateKeys},
      {"aggregation", (Body) s::aggregation},
      {"transactions", (Body) s::transactions},
      {"metadataAndErrors", (Body) s::metadataAndErrors},
    };
    int failed = 0;
    for (Object[] c : checks) {
      try {
        ((Body) c[1]).run();
        System.out.println("PASS " + c[0]);
      } catch (Throwable t) {
        failed++;
        System.out.println("FAIL " + c[0] + ": " + t);
      }
    }
    close();
    System.out.println((checks.length - failed) + " of " + checks.length + " passed");
    System.exit(failed);
  }
  static final String URI =
      System.getenv().getOrDefault("SPRIG_MONGO_URI", "mongodb://127.0.0.1:25737/?directConnection=true");
  static final String DB = System.getenv().getOrDefault("SPRIG_MONGO_DB", "spring_smoke");
  static MongoClient client;
  static MongoTemplate template;
  static MongoDatabaseFactory factory;

  @org.springframework.data.mongodb.core.mapping.Document("people")
  public static class Person {
    @Id public String id;
    public String name;
    public int age;
    public List<String> tags;
    public Address address;
    @Version public Long version;

    public Person() {}

    Person(String name, int age, List<String> tags) {
      this.name = name;
      this.age = age;
      this.tags = tags;
      this.address = new Address("Main St", 1);
    }
  }

  public record Address(String street, int number) {}

  public interface PersonRepository extends MongoRepository<Person, String> {
    List<Person> findByNameStartingWithOrderByAgeDesc(String prefix);

    List<Person> findByAgeBetween(int from, int to);

    long countByTagsContaining(String tag);
  }

  static void connect() {
    client =
        MongoClients.create(
            MongoClientSettings.builder().applyConnectionString(new ConnectionString(URI)).build());
    factory = new SimpleMongoClientDatabaseFactory(client, DB);
    template = new MongoTemplate(factory);
    client.getDatabase(DB).drop();
  }

  static void close() {
    client.getDatabase(DB).drop();
    client.close();
  }

  void templateCrud() {
    Person p = template.insert(new Person("ada", 36, List.of("math", "eng")));
    assertNotNull(p.id);
    assertEquals(0L, p.version);
    Person found = template.findById(p.id, Person.class);
    assertEquals("ada", found.name);
    assertEquals("Main St", found.address.street());
    template.updateFirst(
        Query.query(where("name").is("ada")), new Update().inc("age", 1).push("tags", "cs"), Person.class);
    found = template.findById(p.id, Person.class);
    assertEquals(37, found.age);
    assertEquals(List.of("math", "eng", "cs"), found.tags);
    found.age = 40;
    template.save(found);
    assertEquals(40, template.findById(p.id, Person.class).age);
    assertEquals(1, template.remove(Query.query(where("_id").is(p.id)), Person.class).getDeletedCount());
  }

  void optimisticLocking() {
    Person p = template.insert(new Person("grace", 45, List.of("navy")));
    Person a = template.findById(p.id, Person.class);
    Person b = template.findById(p.id, Person.class);
    a.age = 46;
    template.save(a);
    b.age = 47;
    assertThrows(
        org.springframework.dao.OptimisticLockingFailureException.class, () -> template.save(b));
  }

  void repositoryQueries() {
    PersonRepository repo = new MongoRepositoryFactory(template).getRepository(PersonRepository.class);
    repo.deleteAll();
    repo.saveAll(
        List.of(
            new Person("alan", 41, List.of("cs")),
            new Person("alonzo", 92, List.of("cs", "math")),
            new Person("barbara", 30, List.of("cs"))));
    List<Person> al = repo.findByNameStartingWithOrderByAgeDesc("al");
    assertEquals(List.of("alonzo", "alan"), al.stream().map(x -> x.name).toList());
    assertEquals(2, repo.findByAgeBetween(29, 50).size());
    assertEquals(3, repo.countByTagsContaining("cs"));
    assertEquals(3, repo.count());
    assertEquals(
        "barbara", repo.findAll(Sort.by("age")).get(0).name);
  }

  void typesRoundTrip() {
    Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    Document d =
        new Document("_id", new ObjectId())
            .append("i", 7)
            .append("l", 7L)
            .append("d", 1.5)
            .append("dec", new Decimal128(new BigDecimal("59.90")))
            .append("t", java.util.Date.from(now))
            .append("b", new org.bson.types.Binary(new byte[] {1, 2, 3}))
            .append("n", null)
            .append("arr", List.of(1, "two", Map.of("three", 3)));
    template.getCollection("types").insertOne(d);
    Document back = template.getCollection("types").find(new Document("_id", d.get("_id"))).first();
    assertEquals(Integer.class, back.get("i").getClass());
    assertEquals(Long.class, back.get("l").getClass());
    assertEquals(Double.class, back.get("d").getClass());
    assertEquals(new Decimal128(new BigDecimal("59.90")), back.get("dec"));
    assertEquals(java.util.Date.from(now), back.get("t"));
    assertArrayEquals(new byte[] {1, 2, 3}, ((org.bson.types.Binary) back.get("b")).getData());
    assertTrue(back.containsKey("n"));
    assertEquals(d.get("arr"), back.get("arr"));
  }

  void indexesAndDuplicateKeys() {
    template.indexOps("users").createIndex(new Index().on("email", Sort.Direction.ASC).unique());
    List<IndexInfo> idx = template.indexOps("users").getIndexInfo();
    assertTrue(idx.stream().anyMatch(i -> i.isUnique() && i.getName().startsWith("email")));
    template.getCollection("users").insertOne(new Document("email", "a@x"));
    assertThrows(
        DuplicateKeyException.class,
        () -> template.insert(new Document("email", "a@x"), "users"));
  }

  void aggregation() {
    template.dropCollection("orders");
    template.insert(
        List.of(
            new Document("cust", "a").append("total", 10),
            new Document("cust", "a").append("total", 5),
            new Document("cust", "b").append("total", 7)),
        "orders");
    Aggregation agg =
        Aggregation.newAggregation(
            Aggregation.group("cust").sum("total").as("sum").count().as("n"),
            Aggregation.sort(Sort.Direction.DESC, "sum"));
    List<Document> out = template.aggregate(agg, "orders", Document.class).getMappedResults();
    assertEquals(2, out.size());
    assertEquals("a", out.get(0).get("_id"));
    assertEquals(15, ((Number) out.get(0).get("sum")).intValue());
    assertEquals(2, ((Number) out.get(0).get("n")).intValue());
  }

  void transactions() {
    template.createCollection("accounts");
    template.insert(new Document("_id", "x").append("bal", 100), "accounts");
    template.insert(new Document("_id", "y").append("bal", 0), "accounts");
    MongoTransactionManager tm = new MongoTransactionManager(factory);
    TransactionTemplate tt = new TransactionTemplate(tm);
    tt.executeWithoutResult(
        s -> {
          template.updateFirst(Query.query(where("_id").is("x")), new Update().inc("bal", -30), "accounts");
          template.updateFirst(Query.query(where("_id").is("y")), new Update().inc("bal", 30), "accounts");
        });
    assertEquals(70, template.findById("x", Document.class, "accounts").getInteger("bal"));
    assertThrows(
        IllegalStateException.class,
        () ->
            tt.executeWithoutResult(
                s -> {
                  template.updateFirst(
                      Query.query(where("_id").is("x")), new Update().inc("bal", -70), "accounts");
                  throw new IllegalStateException("roll back");
                }));
    assertEquals(70, template.findById("x", Document.class, "accounts").getInteger("bal"));
  }

  void metadataAndErrors() {
    assertTrue(template.collectionExists("people"));
    assertTrue(template.getCollectionNames().contains("orders"));
    Document hello = template.executeCommand(new Document("hello", 1));
    assertTrue(hello.containsKey("maxWireVersion"));
    Document build = template.executeCommand(new Document("buildInfo", 1));
    assertNotNull(build.get("version"));
    assertThrows(
        org.springframework.dao.DataAccessException.class,
        () -> template.executeCommand(new Document("noSuchCommand", 1)));
  }
}
