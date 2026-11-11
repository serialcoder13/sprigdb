// R6 Hibernate check (chapter 32): ddl-auto=validate over typed tables
// (asset, vehicle, truck, created by ../../flyway), then a JPA write
// through a subtype and polymorphic reads; then Hibernate's SINGLE_TABLE
// inheritance with @DiscriminatorColumn over fleet, which SprigDB types by
// that column (fleet_truck, fleet_car). One JSON line per check.
//
// Environment: SPRIG_CONNECTOR (hibernate = MySQL Connector/J,
// hibernate-mariadb = MariaDB Connector/J), SPRIG_HOST, SPRIG_PORT,
// SPRIG_USER, SPRIG_PASSWORD, SPRIG_DB.
package sprig.r6;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.math.BigDecimal;
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
        System.out.println("{\"suite\":\"r6-hibernate\",\"connector\":\"" + CONNECTOR + "\",\"check\":\"" + check
                + "\",\"status\":\"" + (ok ? "pass" : "fail") + "\",\"detail\":\"" + d + "\"}");
        System.out.flush();
    }

    static Throwable rootOf(Throwable x) {
        while (x.getCause() != null && x.getCause() != x) x = x.getCause();
        return x;
    }

    public static void main(String[] args) {
        String hostDb = System.getenv("SPRIG_HOST") + ":" + System.getenv("SPRIG_PORT") + "/" + System.getenv("SPRIG_DB");
        String url = MARIADB
                ? "jdbc:mariadb://" + hostDb + "?sslMode=trust&allowPublicKeyRetrieval=true"
                : "jdbc:mysql://" + hostDb + "?sslMode=REQUIRED&allowPublicKeyRetrieval=true&connectionTimeZone=UTC";
        Map<String, Object> p = new HashMap<>();
        p.put("spring.datasource.url", url);
        p.put("spring.datasource.username", System.getenv("SPRIG_USER"));
        p.put("spring.datasource.password", System.getenv("SPRIG_PASSWORD"));
        p.put("spring.datasource.hikari.maximum-pool-size", "2");
        p.put("spring.jpa.hibernate.ddl-auto", "validate");
        p.put("spring.jpa.hibernate.naming.physical-strategy",
                "org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl");
        p.put("spring.jpa.open-in-view", "false");
        if (MARIADB) p.put("spring.jpa.database-platform", "org.hibernate.dialect.MariaDBDialect");
        p.put("spring.flyway.enabled", "false");
        p.put("spring.main.banner-mode", "off");
        p.put("logging.level.root", "OFF");
        ConfigurableApplicationContext ctx;
        try {
            ctx = new SpringApplicationBuilder(App.class).web(WebApplicationType.NONE).properties(p).run();
            emit("schema_validate", true, "ddl-auto=validate passed for asset, vehicle (inherited columns), truck");
        } catch (Exception e) {
            emit("schema_validate", false, String.valueOf(rootOf(e).getMessage()));
            System.exit(1);
            return;
        }
        EntityManagerFactory emf = ctx.getBean(EntityManagerFactory.class);
        try {
            EntityManager em = emf.createEntityManager();
            em.getTransaction().begin();
            em.persist(new Truck(900L, "jpa-truck", 2, "JPA00000000000900", new BigDecimal("42.50")));
            em.getTransaction().commit();
            em.clear();
            Truck t = em.find(Truck.class, 900L);
            Vehicle v = em.find(Vehicle.class, 900L);
            Asset a = em.find(Asset.class, 900L);
            boolean ok = t != null && v != null && a != null && v.vin.equals(t.vin) && a.name.equals("jpa-truck")
                    && t.payloadKg.compareTo(new BigDecimal("42.50")) == 0;
            emit("write_through_subtype", ok, "persisted truck 900; found as truck, vehicle and asset: " + ok);
            long assets = em.createQuery("select count(a) from Asset a", Long.class).getSingleResult();
            long vehicles = em.createQuery("select count(v) from Vehicle v where v.siteId > 0", Long.class).getSingleResult();
            long trucks = em.createQuery("select count(t) from Truck t", Long.class).getSingleResult();
            emit("polymorphic_reads", assets == 7 && vehicles == 5 && trucks == 3,
                    "assets " + assets + " (want 7), vehicles " + vehicles + " (want 5), trucks " + trucks + " (want 3)");
            em.getTransaction().begin();
            em.remove(em.find(Truck.class, 900L));
            em.getTransaction().commit();
            long after = em.createQuery("select count(a) from Asset a", Long.class).getSingleResult();
            emit("delete_through_subtype", after == 6, "assets after deleting truck 900: " + after);
            em.close();
        } catch (Exception e) {
            emit("jpa", false, String.valueOf(rootOf(e).getMessage()));
        }
        // Hibernate SINGLE_TABLE + @DiscriminatorColumn over a table SprigDB
        // types by the same column: Hibernate writes and reads its table,
        // SQL reads the subtypes, and both agree.
        try {
            EntityManager em = emf.createEntityManager();
            em.getTransaction().begin();
            em.persist(new FleetTruck(900L, "jpa-fleet-truck", new BigDecimal("42.50")));
            em.getTransaction().commit();
            em.clear();
            long hqlTrucks = em.createQuery("select count(t) from FleetTruck t", Long.class).getSingleResult();
            long sqlTrucks = ((Number) em.createNativeQuery("select count(*) from fleet_truck").getSingleResult()).longValue();
            String vtype = String.valueOf(em.createNativeQuery("select vtype from fleet where id = 900").getSingleResult());
            emit("single_table_write", hqlTrucks == 2 && sqlTrucks == 2 && vtype.equals("TRUCK"),
                    "persisted FleetTruck 900 (vtype " + vtype + "); trucks via HQL " + hqlTrucks + ", via fleet_truck " + sqlTrucks + " (want 2)");
            long all = ((Number) em.createNativeQuery("select count(*) from fleet").getSingleResult()).longValue();
            long only = ((Number) em.createNativeQuery("select count(*) from only fleet").getSingleResult()).longValue();
            long cars = ((Number) em.createNativeQuery("select count(*) from fleet_car").getSingleResult()).longValue();
            emit("single_table_typed_reads", all == 5 && only == 2 && cars == 1,
                    "fleet " + all + " (want 5), only fleet " + only + " (want 2: VEHICLE and the undeclared SMALLTRUCK), fleet_car " + cars + " (want 1)");
            em.getTransaction().begin();
            em.createNativeQuery("update fleet set vtype = 'CAR' where id = 900").executeUpdate();
            em.getTransaction().commit();
            em.clear();
            FleetCar moved = em.find(FleetCar.class, 900L);
            long carsAfter = ((Number) em.createNativeQuery("select count(*) from fleet_car").getSingleResult()).longValue();
            emit("single_table_move", moved != null && carsAfter == 2,
                    "vtype TRUCK -> CAR: Hibernate finds FleetCar 900 " + (moved != null) + ", fleet_car " + carsAfter + " (want 2)");
            em.getTransaction().begin();
            em.remove(em.find(FleetCar.class, 900L));
            em.getTransaction().commit();
            long left = ((Number) em.createNativeQuery("select count(*) from fleet").getSingleResult()).longValue();
            emit("single_table_delete", left == 4, "fleet after deleting FleetCar 900: " + left);
            em.close();
        } catch (Exception e) {
            emit("single_table", false, String.valueOf(rootOf(e).getMessage()));
        }
        ctx.close();
        System.exit(failures == 0 ? 0 : 1);
    }
}
