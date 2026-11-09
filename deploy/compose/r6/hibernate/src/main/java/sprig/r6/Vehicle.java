package sprig.r6;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** A subtype read with its inherited columns. */
@Entity
@Table(name = "vehicle")
public class Vehicle {
    @Id
    Long id;

    @Column(nullable = false, length = 200)
    String name;

    @Column(name = "site_id")
    Integer siteId;

    @Column(nullable = false, columnDefinition = "char(17)")
    String vin;

    protected Vehicle() {}
}
