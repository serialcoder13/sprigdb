// Connector check for Hibernate 6 through Spring Data JPA (../README.md).
//
// SPRIG_CONNECTOR is "hibernate" (MySQL Connector/J) or "hibernate-mariadb"
// (MariaDB Connector/J). The schema is created with plain SQL first, so
// that `schema_validate` (ddl-auto=validate) checks Hibernate's view of a
// schema the server describes, not one Hibernate wrote itself. Prints one
// JSON line per check on stdout; Spring's own logging is kept to errors.
package sprig;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.RollbackException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootApplication
@EnableJpaRepositories(considerNestedRepositories = true)
public class App {
    static final String CONNECTOR = System.getenv("SPRIG_CONNECTOR");
    static final boolean MARIADB_DRIVER = CONNECTOR.equals("hibernate-mariadb");
    static final String TLS = System.getenv("SPRIG_TLS");
    static final String PERSONA = Objects.requireNonNullElse(System.getenv("SPRIG_PERSONA"), "mariadb");
    static final String PREFIX = "t_" + CONNECTOR.replace('-', '_') + "_" + TLS;
    static final Set<String> SKIPPED = Set.of(Objects.requireNonNullElse(System.getenv("SPRIG_CONN_SKIP_CHECKS"), "")
            .trim().split("[ ,]+"));

    static ConfigurableApplicationContext ctx;
    static Repos.Authors authors;
    static Repos.Books books;
    static TransactionTemplate tx;
    static EntityManagerFactory emf;

    static final class Skip extends Exception {
        private static final long serialVersionUID = 1L;

        Skip(String m) {
            super(m);
        }
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
        System.out.println("{\"connector\":" + json(CONNECTOR) + ",\"tls\":" + json(TLS) + ",\"check\":" + json(check)
                + ",\"status\":" + json(status) + ",\"detail\":" + json(detail) + ",\"ms\":" + Math.round(ms * 10) / 10.0 + "}");
        System.out.flush();
    }

    static void expect(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    static String url() {
        String host = System.getenv("SPRIG_HOST") + ":" + System.getenv("SPRIG_PORT") + "/" + System.getenv("SPRIG_DATABASE");
        if (MARIADB_DRIVER) {
            return "jdbc:mariadb://" + host + "?sslMode=" + (TLS.equals("required") ? "trust" : "disable")
                    + "&allowPublicKeyRetrieval=true";
        }
        return "jdbc:mysql://" + host + "?sslMode=" + (TLS.equals("required") ? "REQUIRED" : "DISABLED")
                + "&allowPublicKeyRetrieval=true&connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true";
    }

    static Connection jdbc() throws SQLException {
        return DriverManager.getConnection(url(), System.getenv("SPRIG_USER"), System.getenv("SPRIG_PASSWORD"));
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

    // --- schema, created with SQL, matching Hibernate 6's MySQL/MariaDB mapping -----

    static final String AUTHOR = PREFIX + "_author";
    static final String BOOK = PREFIX + "_book";

    static void dropSchema(Connection c) throws SQLException {
        exec(c, "DROP TABLE IF EXISTS " + BOOK + ", " + AUTHOR);
    }

    static void createSchema(Connection c) throws SQLException {
        dropSchema(c);
        exec(c, "CREATE TABLE " + AUTHOR + " (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL, "
                + "CONSTRAINT " + AUTHOR + "_uq UNIQUE (name)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4");
        exec(c, "CREATE TABLE " + BOOK + " (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, version BIGINT NOT NULL, "
                + "title VARCHAR(128) NOT NULL, price DECIMAL(10,2) NOT NULL, published DATETIME(6) NULL, "
                + "release_date DATE NULL, in_print BIT(1) NOT NULL, status ENUM('DRAFT','PUBLISHED') NULL, "
                + "cover LONGBLOB NULL, attrs JSON NULL, author_id BIGINT NOT NULL, "
                + "CONSTRAINT " + BOOK + "_fk FOREIGN KEY (author_id) REFERENCES " + AUTHOR + " (id)"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4");
    }

    // --- checks -----------------------------------------------------------------

    static String connect() throws Exception {
        try (Connection c = jdbc()) {
            String cipher;
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SHOW SESSION STATUS LIKE 'Ssl_cipher'")) {
                r.next();
                cipher = r.getString(2);
            }
            if (TLS.equals("required")) expect(!cipher.isEmpty(), "TLS required but the session isn't encrypted");
            else expect(cipher.isEmpty(), "TLS disabled but the session is encrypted (" + cipher + ")");
            createSchema(c);
            serverVersion = scalar(c, "SELECT VERSION()");
            return c.getMetaData().getDriverName() + " " + c.getMetaData().getDriverVersion() + ", cipher="
                    + (cipher.isEmpty() ? "none" : cipher);
        }
    }

    static String serverVersion = "";

    /**
     * Starts Spring with ddl-auto=validate. If that fails, the failure is the
     * check's result, but Spring is started again without validation (and,
     * when Hibernate couldn't read the driver's metadata, with the product
     * and version named from VERSION()) so the remaining checks still run.
     */
    static String schemaValidate() throws Exception {
        try {
            ctx = start(Map.of());
        } catch (Exception e) {
            Map<String, Object> fallback = new HashMap<>();
            fallback.put("spring.jpa.hibernate.ddl-auto", "none");
            String msg = String.valueOf(rootOf(e).getMessage());
            if (msg.contains("Unable to determine Dialect") || msg.contains("JDBC metadata")) {
                String[] v = serverVersion.split("[.-]");
                fallback.put("spring.jpa.properties.hibernate.boot.allow_jdbc_metadata_access", "false");
                fallback.put("spring.jpa.properties.jakarta.persistence.database-product-name",
                        serverVersion.contains("MariaDB") ? "MariaDB" : "MySQL");
                fallback.put("spring.jpa.properties.jakarta.persistence.database-major-version", v[0]);
                fallback.put("spring.jpa.properties.jakarta.persistence.database-minor-version", v[1]);
            }
            ctx = start(fallback);
            fellBack = String.join(", ", fallback.keySet().stream().map(k -> k.replaceAll(".*\\.", "")).sorted().toList());
            throw e;
        } finally {
            if (ctx != null) {
                authors = ctx.getBean(Repos.Authors.class);
                books = ctx.getBean(Repos.Books.class);
                tx = ctx.getBean(TransactionTemplate.class);
                emf = ctx.getBean(EntityManagerFactory.class);
            }
        }
        return "ddl-auto=validate passed for 2 entities (BIGINT identity, DECIMAL, DATETIME(6), DATE, BIT, ENUM, "
                + "LONGBLOB, JSON, FK) against a schema created with SQL";
    }

    static String fellBack = null;

    static Throwable rootOf(Throwable x) {
        Throwable root = x;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    static ConfigurableApplicationContext start(Map<String, Object> overrides) {
        Map<String, Object> p = new HashMap<>();
        p.put("spring.datasource.url", url());
        p.put("spring.datasource.username", System.getenv("SPRIG_USER"));
        p.put("spring.datasource.password", System.getenv("SPRIG_PASSWORD"));
        p.put("spring.datasource.hikari.maximum-pool-size", "4");
        p.put("spring.jpa.hibernate.ddl-auto", "validate");
        p.put("spring.jpa.hibernate.naming.physical-strategy", PrefixNaming.class.getName());
        p.put("spring.jpa.open-in-view", "false");
        p.put("spring.jpa.properties.hibernate.jdbc.time_zone", "UTC");
        p.put("spring.main.banner-mode", "off");
        p.put("spring.main.log-startup-info", "false");
        p.put("logging.level.root", "OFF");
        p.putAll(overrides);
        return new SpringApplicationBuilder(App.class).web(WebApplicationType.NONE).properties(p).run();
    }

    static String dialect() {
        Dialect d = emf.unwrap(SessionFactoryImplementor.class).getJdbcServices().getDialect();
        String name = d.getClass().getSimpleName();
        String detail = name + " " + d.getVersion() + ", Hibernate " + org.hibernate.Version.getVersionString()
                + (fellBack == null ? "" : " (Spring restarted with " + fellBack + ")");
        expect(name.startsWith("MariaDB") == PERSONA.equals("mariadb"), "persona " + PERSONA + " but " + detail);
        return detail;
    }

    static void clean() {
        tx.executeWithoutResult(s -> authors.deleteAll());
    }

    static final byte[] COVER = new byte[256];
    static final LocalDateTime PUBLISHED = LocalDateTime.of(2024, 2, 29, 23, 59, 58, 123_456_000);

    static {
        for (int i = 0; i < 256; i++) COVER[i] = (byte) i;
    }

    static Book fullBook(String title) {
        Book b = new Book(title, new BigDecimal("12.34"));
        b.published = PUBLISHED;
        b.releaseDate = LocalDate.of(2024, 2, 29);
        b.inPrint = true;
        b.status = Book.Status.PUBLISHED;
        b.cover = COVER;
        b.attrs = new LinkedHashMap<>(Map.of("a", 1, "s", "🌱", "b", Arrays.asList(true, null)));
        return b;
    }

    static String crud() {
        clean();
        Author a = new Author("Ünïcödé 🌱 crud");
        Book saved = a.add(fullBook("héllo wörld 🌱"));
        authors.save(a);
        Long id = saved.id;
        expect(id != null, "no generated id");
        tx.executeWithoutResult(s -> {
            Book b = books.findById(id).orElseThrow();
            List<String> bad = new ArrayList<>();
            if (!b.title.equals("héllo wörld 🌱")) bad.add("title " + b.title);
            if (b.price.compareTo(new BigDecimal("12.34")) != 0) bad.add("price " + b.price);
            if (!PUBLISHED.equals(b.published)) bad.add("published " + b.published);
            if (!LocalDate.of(2024, 2, 29).equals(b.releaseDate)) bad.add("releaseDate " + b.releaseDate);
            if (!b.inPrint) bad.add("inPrint");
            if (b.status != Book.Status.PUBLISHED) bad.add("status " + b.status);
            if (!Arrays.equals(COVER, b.cover)) bad.add("cover " + (b.cover == null ? null : b.cover.length));
            if (b.attrs == null || !b.attrs.get("s").equals("🌱") || ((Number) b.attrs.get("a")).intValue() != 1
                    || !Arrays.asList(true, null).equals(b.attrs.get("b"))) bad.add("attrs " + b.attrs);
            Author author = (Author) org.hibernate.Hibernate.unproxy(b.author); // a lazy proxy: no field access
            if (!author.name.equals("Ünïcödé 🌱 crud")) bad.add("author " + author.name);
            expect(bad.isEmpty(), String.join("; ", bad));
            b.title = "updated"; // flushed by dirty checking
        });
        Book b = books.findById(id).orElseThrow();
        expect(b.title.equals("updated") && b.version == 1, "after update: " + b.title + " v" + b.version);
        tx.executeWithoutResult(s -> authors.delete(authors.findByName("Ünïcödé 🌱 crud").orElseThrow()));
        expect(books.count() == 0 && authors.count() == 0, "cascade delete left rows");
        return "save with cascade, 9 typed fields read back, dirty-checked update (@Version 0 -> 1), cascade delete";
    }

    static String query() throws Exception {
        clean();
        Author a = new Author("query author");
        for (int i = 1; i <= 12; i++) a.add(new Book("row " + i, new BigDecimal(i + ".50")));
        Author b = new Author("other author");
        b.add(new Book("lonely", new BigDecimal("1.00")));
        authors.saveAll(List.of(a, b));
        List<Book> found = books.findByTitleContainingOrderByIdAsc("row 1");
        expect(found.size() == 4, "derived query found " + found.size()); // row 1, 10, 11, 12
        List<Object[]> stats = books.statsByAuthor();
        expect(stats.size() == 2 && stats.get(1)[0].equals("query author") && ((Number) stats.get(1)[1]).longValue() == 12
                && new BigDecimal("84.00").compareTo((BigDecimal) stats.get(1)[2]) == 0,
                "JPQL aggregate " + stats.stream().map(Arrays::toString).toList());
        Page<Book> page = books.findAll(PageRequest.of(1, 5, Sort.by("id")));
        expect(page.getNumberOfElements() == 5 && page.getTotalElements() == 13 && page.getContent().get(0).title.equals("row 6"),
                "page 2: " + page.getNumberOfElements() + " of " + page.getTotalElements());
        EntityManager em = emf.createEntityManager();
        try {
            Number n = (Number) em.createNativeQuery("SELECT COUNT(*) FROM " + BOOK + " WHERE price > ?")
                    .setParameter(1, new BigDecimal("6")).getSingleResult();
            expect(n.longValue() == 7, "native query " + n);
        } finally {
            em.close();
        }
        tx.executeWithoutResult(s -> authors.deleteAll());
        return "derived query, JPQL join + GROUP BY, paging (LIMIT/OFFSET + count), native query";
    }

    static String transaction() throws Exception {
        clean();
        try {
            tx.executeWithoutResult(s -> {
                authors.saveAndFlush(new Author("rolled back"));
                throw new IllegalStateException("roll me back");
            });
            throw new AssertionError("no exception");
        } catch (IllegalStateException expected) {
            // rolled back
        }
        expect(authors.findByName("rolled back").isEmpty(), "row survived rollback");
        tx.executeWithoutResult(s -> authors.save(new Author("kept")));
        try (Connection c = jdbc()) {
            expect("1".equals(scalar(c, "SELECT COUNT(*) FROM " + AUTHOR + " WHERE name = 'kept'")),
                    "committed row not visible to another session");
        }
        tx.executeWithoutResult(s -> authors.deleteAll());
        return "rollback on exception, commit visible to another session";
    }

    static String optimisticLock() {
        clean();
        Author a = new Author("lock author");
        Book saved = a.add(new Book("v0", BigDecimal.ONE));
        authors.save(a);
        EntityManager em1 = emf.createEntityManager();
        EntityManager em2 = emf.createEntityManager();
        try {
            em1.getTransaction().begin();
            em2.getTransaction().begin();
            Book x1 = em1.find(Book.class, saved.id);
            Book x2 = em2.find(Book.class, saved.id);
            x1.title = "first";
            em1.getTransaction().commit();
            x2.title = "second";
            try {
                em2.getTransaction().commit();
                throw new AssertionError("the stale update committed");
            } catch (RollbackException e) {
                expect(e.getCause() instanceof OptimisticLockException, "cause " + e.getCause());
            }
        } finally {
            em1.close();
            em2.close();
        }
        Book b = books.findById(saved.id).orElseThrow();
        expect(b.title.equals("first") && b.version == 1, "after the conflict: " + b.title + " v" + b.version);
        tx.executeWithoutResult(s -> authors.deleteAll());
        return "stale @Version update rejected with OptimisticLockException";
    }

    public static void main(String[] args) {
        Map<String, CheckFn> checks = new LinkedHashMap<>();
        checks.put("connect", App::connect);
        checks.put("schema_validate", App::schemaValidate);
        checks.put("dialect", App::dialect);
        checks.put("crud", App::crud);
        checks.put("query", App::query);
        checks.put("transaction", App::transaction);
        checks.put("optimistic_lock", App::optimisticLock);
        boolean connected = false;
        for (var e : checks.entrySet()) {
            String name = e.getKey();
            if (SKIPPED.contains(name)) continue;
            if (!name.equals("connect") && !connected) {
                emit(name, "fail", "no connection", 0);
                continue;
            }
            if (!name.equals("connect") && !name.equals("schema_validate") && ctx == null) {
                emit(name, "fail", "no Spring context", 0);
                continue;
            }
            long t0 = System.nanoTime();
            try {
                String detail = e.getValue().run();
                emit(name, "pass", detail == null ? "" : detail, (System.nanoTime() - t0) / 1e6);
                if (name.equals("connect")) connected = true;
            } catch (Skip x) {
                emit(name, "skip", x.getMessage(), 0);
            } catch (Throwable x) {
                Throwable root = rootOf(x);
                String msg = x.getClass().getSimpleName() + ": " + x.getMessage();
                if (root != x) msg = root.getClass().getSimpleName() + ": " + root.getMessage() + " | " + msg;
                emit(name, "fail", msg, (System.nanoTime() - t0) / 1e6);
            }
        }
        if (ctx != null) ctx.close();
        try (Connection c = jdbc()) {
            dropSchema(c);
        } catch (SQLException ignored) {
            // best effort
        }
        System.exit(0);
    }
}
