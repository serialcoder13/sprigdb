// R6 Hibernate check (chapter 42): ddl-auto=validate over an inheritable
// table (products, created by ../flyway), then JPA writes and reads in the
// root space and in a child space (the connection's init SQL sets
// @sprig_space). One JSON line per check.
//
// Environment: SPRIG_CONNECTOR (hibernate = MySQL Connector/J,
// hibernate-mariadb = MariaDB Connector/J), SPRIG_HOST, SPRIG_PORT,
// SPRIG_USER, SPRIG_PASSWORD, SPRIG_DB, SPRIG_SPACE (the child space).
package sprig.r6i;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class App {
    static final String CONNECTOR = Objects.requireNonNullElse(System.getenv("SPRIG_CONNECTOR"), "hibernate");
    static final boolean MARIADB = CONNECTOR.equals("hibernate-mariadb");
    static int failures = 0;

    static void emit(String check, boolean ok, String detail) {
        if (!ok) failures++;
        String d = detail.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
        if (d.length() > 400) d = d.substring(0, 400);
        System.out.println("{\"suite\":\"r6i-hibernate\",\"connector\":\"" + CONNECTOR + "\",\"check\":\"" + check
                + "\",\"status\":\"" + (ok ? "pass" : "fail") + "\",\"detail\":\"" + d + "\"}");
        System.out.flush();
    }

    static Throwable rootOf(Throwable x) {
        while (x.getCause() != null && x.getCause() != x) x = x.getCause();
        return x;
    }

    static ConfigurableApplicationContext start(String space) {
        String hostDb = System.getenv("SPRIG_HOST") + ":" + System.getenv("SPRIG_PORT") + "/" + System.getenv("SPRIG_DB");
        String url = MARIADB
                ? "jdbc:mariadb://" + hostDb + "?sslMode=trust&allowPublicKeyRetrieval=true"
                : "jdbc:mysql://" + hostDb + "?sslMode=REQUIRED&allowPublicKeyRetrieval=true&connectionTimeZone=UTC";
        Map<String, Object> p = new HashMap<>();
        p.put("spring.datasource.url", url);
        p.put("spring.datasource.username", System.getenv("SPRIG_USER"));
        p.put("spring.datasource.password", System.getenv("SPRIG_PASSWORD"));
        p.put("spring.datasource.hikari.maximum-pool-size", "1");
        if (space != null) p.put("spring.datasource.hikari.connection-init-sql", "SET @sprig_space = '" + space + "'");
        p.put("spring.jpa.hibernate.ddl-auto", "validate");
        p.put("spring.jpa.hibernate.naming.physical-strategy",
                "org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl");
        p.put("spring.jpa.open-in-view", "false");
        if (MARIADB) p.put("spring.jpa.database-platform", "org.hibernate.dialect.MariaDBDialect");
        p.put("spring.flyway.enabled", "false");
        p.put("spring.main.banner-mode", "off");
        p.put("logging.level.root", "OFF");
        return new SpringApplicationBuilder(App.class).web(WebApplicationType.NONE).properties(p).run();
    }

    static long count(EntityManager em) {
        return em.createQuery("select count(p) from Product p", Long.class).getSingleResult();
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext root;
        try {
            root = start(null);
            emit("schema_validate", true, "ddl-auto=validate passed for products (no _sprig_ columns seen)");
        } catch (Exception e) {
            emit("schema_validate", false, String.valueOf(rootOf(e).getMessage()));
            System.exit(1);
            return;
        }
        String space = System.getenv("SPRIG_SPACE");
        try {
            EntityManager em = root.getBean(EntityManagerFactory.class).createEntityManager();
            long before = count(em);
            em.getTransaction().begin();
            em.persist(new Product(900, "jpa", "grey", 9, "IN"));
            em.getTransaction().commit();
            em.clear();
            Product p = em.find(Product.class, 900);
            emit("root_write", p != null && "grey".equals(p.colour) && count(em) == before + 1,
                    "persisted product 900 in the root space; count " + count(em) + " (was " + before + ")");
            em.close();
        } catch (Exception e) {
            emit("root_write", false, String.valueOf(rootOf(e).getMessage()));
        }
        try (ConfigurableApplicationContext child = start(space)) {
            EntityManager em = child.getBean(EntityManagerFactory.class).createEntityManager();
            Product p = em.find(Product.class, 900);
            emit("child_inherits", p != null && "grey".equals(p.colour), "space " + space + " sees product 900: " + (p != null));
            em.getTransaction().begin();
            p.colour = "teal";
            em.getTransaction().commit();
            em.clear();
            Product q = em.find(Product.class, 900);
            emit("child_override", q != null && "teal".equals(q.colour), "colour in " + space + ": " + (q == null ? null : q.colour));
            em.getTransaction().begin();
            em.remove(em.find(Product.class, 1));
            em.getTransaction().commit();
            emit("child_hide", em.find(Product.class, 1) == null, "product 1 hidden in " + space);
            em.close();
        } catch (Exception e) {
            emit("child", false, String.valueOf(rootOf(e).getMessage()));
        }
        try {
            EntityManager em = root.getBean(EntityManagerFactory.class).createEntityManager();
            Product p = em.find(Product.class, 900);
            Product one = em.find(Product.class, 1);
            emit("root_untouched", p != null && "grey".equals(p.colour) && one != null,
                    "root still has 900 grey and product 1: " + (p != null && one != null));
            em.getTransaction().begin();
            em.remove(em.find(Product.class, 900));
            em.getTransaction().commit();
            em.close();
        } catch (Exception e) {
            emit("root_untouched", false, String.valueOf(rootOf(e).getMessage()));
        }
        root.close();
        System.exit(failures == 0 ? 0 : 1);
    }
}
