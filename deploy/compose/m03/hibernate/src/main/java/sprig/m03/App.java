// Tenant harness schema-tool suite, Hibernate part (../../../../../../README.md).
//
// Spring Data JPA with ddl-auto=validate against the schema Flyway created
// for one tenant (crm.customers, shop.products, shop.orders and the view
// shop.order_totals), then reads and writes through it. Run once per
// tenant; prints one JSON line per check on stdout, in the harness's format:
//
//   {"suite":"hibernate","tenant":"acme_app","connector":"hibernate-mariadb",
//    "check":"schema_validate","status":"pass","detail":"...","ms":812.0}
//
// Environment: SPRIG_CONNECTOR (hibernate = MySQL Connector/J,
// hibernate-mariadb = MariaDB Connector/J with MariaDBDialect), SPRIG_HOST,
// SPRIG_PORT, SPRIG_USER, SPRIG_PASSWORD, SPRIG_TLS (disabled | required),
// SPRIG_PERSONA, SPRIG_DB_PREFIX (baseline only), SPRIG_TAG and
// SPRIG_OTHER_TAG (this tenant's and the other tenant's row markers).
package sprig.m03;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.ParameterMode;
import jakarta.persistence.StoredProcedureQuery;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootApplication
@EnableJpaRepositories(considerNestedRepositories = true)
public class App {
    static final String CONNECTOR = Objects.requireNonNullElse(System.getenv("SPRIG_CONNECTOR"), "hibernate-mariadb");
    static final boolean MARIADB_DRIVER = CONNECTOR.equals("hibernate-mariadb");
    static final String TLS = Objects.requireNonNullElse(System.getenv("SPRIG_TLS"), "disabled");
    static final String PERSONA = Objects.requireNonNullElse(System.getenv("SPRIG_PERSONA"), "mariadb");
    static final String PREFIX = Objects.requireNonNullElse(System.getenv("SPRIG_DB_PREFIX"), "");
    static final String TAG = System.getenv("SPRIG_TAG");
    static final String OTHER_TAG = System.getenv("SPRIG_OTHER_TAG");

    static ConfigurableApplicationContext ctx;
    static EntityManagerFactory emf;
    static TransactionTemplate tx;
    static String fellBack;
    static String serverVersion = "";

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
        if (detail.length() > 400) detail = detail.substring(0, 400);
        System.out.println("{\"suite\":\"hibernate\",\"tenant\":" + json(TAG) + ",\"connector\":" + json(CONNECTOR)
                + ",\"check\":" + json(check) + ",\"status\":" + json(status) + ",\"detail\":" + json(detail)
                + ",\"ms\":" + Math.round(ms * 10) / 10.0 + "}");
        System.out.flush();
    }

    static void expect(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    static String db(String logical) {
        return PREFIX + logical;
    }

    static String url() {
        String hostDb = System.getenv("SPRIG_HOST") + ":" + System.getenv("SPRIG_PORT") + "/" + db("shop");
        if (MARIADB_DRIVER) {
            return "jdbc:mariadb://" + hostDb + "?sslMode=" + (TLS.equals("required") ? "trust" : "disable")
                    + "&allowPublicKeyRetrieval=true";
        }
        return "jdbc:mysql://" + hostDb + "?sslMode=" + (TLS.equals("required") ? "REQUIRED" : "DISABLED")
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

    static Throwable rootOf(Throwable x) {
        Throwable root = x;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    // --- checks -----------------------------------------------------------------

    static String connect() throws Exception {
        try (Connection c = jdbc()) {
            serverVersion = scalar(c, "SELECT VERSION()");
            String current = scalar(c, "SELECT DATABASE()");
            expect(db("shop").equals(current), "DATABASE() is " + current + ", want " + db("shop"));
            return c.getMetaData().getDriverName() + " " + c.getMetaData().getDriverVersion() + "; server " + serverVersion
                    + "; DATABASE()=" + current;
        }
    }

    static ConfigurableApplicationContext start(Map<String, Object> overrides) {
        Map<String, Object> p = new HashMap<>();
        p.put("spring.datasource.url", url());
        p.put("spring.datasource.username", System.getenv("SPRIG_USER"));
        p.put("spring.datasource.password", System.getenv("SPRIG_PASSWORD"));
        p.put("spring.datasource.hikari.maximum-pool-size", "2");
        p.put("spring.jpa.hibernate.ddl-auto", "validate");
        p.put("spring.jpa.hibernate.naming.physical-strategy", Naming.class.getName());
        p.put("spring.jpa.open-in-view", "false");
        p.put("spring.jpa.properties.hibernate.jdbc.time_zone", "UTC");
        if (MARIADB_DRIVER) p.put("spring.jpa.database-platform", "org.hibernate.dialect.MariaDBDialect");
        p.put("spring.flyway.enabled", "false");
        p.put("spring.main.banner-mode", "off");
        p.put("spring.main.log-startup-info", "false");
        p.put("logging.level.root", "OFF");
        p.putAll(overrides);
        return new SpringApplicationBuilder(App.class).web(WebApplicationType.NONE).properties(p).run();
    }

    /**
     * Starts Spring with ddl-auto=validate. If that fails, the failure is the
     * check's result, but Spring is started again without validation (and,
     * when Hibernate couldn't read the driver's metadata, with the product
     * and version from VERSION()) so the remaining checks still run. The
     * connector matrix's Hibernate check does the same.
     */
    static String schemaValidate() throws Exception {
        try {
            ctx = start(Map.of());
        } catch (Exception e) {
            Map<String, Object> fallback = new HashMap<>();
            fallback.put("spring.jpa.hibernate.ddl-auto", "none");
            String msg = String.valueOf(rootOf(e).getMessage());
            if (msg.contains("Unable to determine Dialect") || msg.contains("JDBC metadata") || msg.contains("KEYWORDS")) {
                String[] v = serverVersion.split("[.-]");
                fallback.put("spring.jpa.properties.hibernate.boot.allow_jdbc_metadata_access", "false");
                fallback.put("spring.jpa.properties.jakarta.persistence.database-product-name",
                        serverVersion.contains("MariaDB") ? "MariaDB" : "MySQL");
                fallback.put("spring.jpa.properties.jakarta.persistence.database-major-version", v[0]);
                fallback.put("spring.jpa.properties.jakarta.persistence.database-minor-version", v[1]);
            }
            ctx = start(fallback);
            fellBack = String.join(", ", fallback.keySet().stream().map(k -> k.replaceAll(".*\\.", "")).sorted().toList());
            throw new Exception(msg, e);
        } finally {
            if (ctx != null) {
                emf = ctx.getBean(EntityManagerFactory.class);
                tx = ctx.getBean(TransactionTemplate.class);
            }
        }
        return "ddl-auto=validate passed: " + db("crm") + ".customers, " + db("shop")
                + ".products, .orders (FK into " + db("crm") + ") and the view .order_totals";
    }

    static String dialect() {
        Dialect d = emf.unwrap(SessionFactoryImplementor.class).getJdbcServices().getDialect();
        String name = d.getClass().getSimpleName();
        String detail = name + " " + d.getVersion() + ", Hibernate " + org.hibernate.Version.getVersionString()
                + (fellBack == null ? "" : " (Spring restarted with " + fellBack + ")");
        if (MARIADB_DRIVER) expect(name.startsWith("MariaDB"), "MariaDB Connector/J but " + detail);
        return detail;
    }

    /** Only this tenant's rows: the seeds, its own marker, never the other tenant's. */
    static String read() {
        Repos.Customers customers = ctx.getBean(Repos.Customers.class);
        List<String> emails = customers.findAll().stream().map(c -> c.email).sorted().toList();
        expect(emails.contains("ada@example.test") && emails.contains("grace@example.test"), "seed customers missing: " + emails);
        expect(emails.contains(TAG + "@m03.test"), "own customer " + TAG + "@m03.test missing: " + emails);
        expect(emails.stream().noneMatch(e -> e.startsWith(OTHER_TAG)), "the other tenant's customer is visible: " + emails);
        long products = ctx.getBean(Repos.Products.class).count();
        expect(products >= 3, products + " products");
        long totals = ctx.getBean(Repos.Totals.class).count();
        expect(totals >= 2, totals + " rows in the view");
        return emails.size() + " customers " + emails + ", " + products + " products, " + totals + " rows in order_totals";
    }

    /** A JPA insert, then the procedure through StoredProcedureQuery, then JPQL and the view. */
    static String write() {
        String email = TAG + "-hibernate@m03.test";
        Repos.Customers customers = ctx.getBean(Repos.Customers.class);
        tx.executeWithoutResult(s -> {
            Customer old = customers.findByEmail(email);
            if (old == null) customers.save(new Customer(email, "Hibernate " + TAG));
        });
        EntityManager em = emf.createEntityManager();
        Object placed;
        try {
            em.getTransaction().begin();
            StoredProcedureQuery q = em.createStoredProcedureQuery(db("shop") + ".place_order");
            q.registerStoredProcedureParameter(1, String.class, ParameterMode.IN);
            q.registerStoredProcedureParameter(2, String.class, ParameterMode.IN);
            q.registerStoredProcedureParameter(3, Integer.class, ParameterMode.IN);
            q.setParameter(1, email).setParameter(2, "SKU-3").setParameter(3, 7);
            List<?> rows = q.getResultList();
            placed = rows.isEmpty() ? null : rows.get(0);
            em.getTransaction().commit();
        } finally {
            em.close();
        }
        List<PurchaseOrder> mine = tx.execute(s -> ctx.getBean(Repos.Orders.class).placedBy(email));
        expect(mine != null && !mine.isEmpty() && mine.get(0).qty == 7, "the order placed by the procedure isn't there");
        List<OrderTotal> totals = ctx.getBean(Repos.Totals.class).findByEmail(email);
        expect(!totals.isEmpty() && totals.get(0).total.doubleValue() == 29.75, "order_totals for " + email + ": " + totals.size());
        return "saved " + email + "; CALL " + db("shop") + ".place_order returned placed=" + placed + "; JPQL join found "
                + mine.size() + " order(s); order_totals total=" + totals.get(0).total;
    }

    /** A native query: DATABASE() must be the logical name. */
    static String nativeQuery() {
        EntityManager em = emf.createEntityManager();
        try {
            Object current = em.createNativeQuery("SELECT DATABASE()").getSingleResult();
            expect(db("shop").equals(String.valueOf(current)), "DATABASE() is " + current);
            Object n = em.createNativeQuery("SELECT COUNT(*) FROM orders").getSingleResult();
            return "DATABASE()=" + current + ", " + n + " orders (unqualified)";
        } finally {
            em.close();
        }
    }

    static boolean check(String name, CheckFn fn) {
        long t0 = System.nanoTime();
        try {
            String detail = fn.run();
            emit(name, "pass", detail, (System.nanoTime() - t0) / 1e6);
            return true;
        } catch (Throwable e) {
            Throwable root = rootOf(e);
            String msg = e.getMessage() == null ? root.toString() : e.getMessage();
            if (root != e && root.getMessage() != null && !msg.contains(root.getMessage())) msg += " (" + root.getMessage() + ")";
            emit(name, "fail", msg, (System.nanoTime() - t0) / 1e6);
            return false;
        }
    }

    public static void main(String[] args) {
        if (!check("connect", App::connect)) {
            for (String c : List.of("schema_validate", "dialect", "read", "write", "native_query")) {
                emit(c, "fail", "connect failed", 0);
            }
            return;
        }
        check("schema_validate", App::schemaValidate);
        if (ctx == null) {
            for (String c : List.of("dialect", "read", "write", "native_query")) emit(c, "fail", "Spring didn't start", 0);
            return;
        }
        check("dialect", App::dialect);
        check("read", App::read);
        check("write", App::write);
        check("native_query", App::nativeQuery);
        ctx.close();
    }
}
