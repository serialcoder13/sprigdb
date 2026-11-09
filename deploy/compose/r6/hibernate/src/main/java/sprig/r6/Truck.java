package sprig.r6;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;

/** A subtype two levels down, written through JPA. */
@Entity
@Table(name = "truck")
public class Truck {
    @Id
    Long id;

    @Column(nullable = false, length = 200)
    String name;

    @Column(name = "site_id")
    Integer siteId;

    @Column(nullable = false, columnDefinition = "char(17)")
    String vin;

    @Column(name = "payload_kg", precision = 10, scale = 2)
    BigDecimal payloadKg;

    protected Truck() {}

    Truck(Long id, String name, Integer siteId, String vin, BigDecimal payloadKg) {
        this.id = id;
        this.name = name;
        this.siteId = siteId;
        this.vin = vin;
        this.payloadKg = payloadKg;
    }
}
