package sprig.r6;

import jakarta.persistence.Column;
import jakarta.persistence.DiscriminatorColumn;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.Table;

/**
 * Hibernate's own single-table inheritance over the table SprigDB types by
 * its discriminator (fleet, fleet_truck, fleet_car from ../../flyway V4).
 */
@Entity
@Table(name = "fleet")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "vtype", length = 31)
@DiscriminatorValue("VEHICLE")
public class Fleet {
    @Id
    Long id;

    @Column(nullable = false, length = 50)
    String name;

    protected Fleet() {}

    Fleet(Long id, String name) {
        this.id = id;
        this.name = name;
    }
}
