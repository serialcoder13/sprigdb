package sprig.r6;

import jakarta.persistence.Column;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import java.math.BigDecimal;

/** vtype = 'TRUCK': SprigDB's fleet_truck. */
@Entity
@DiscriminatorValue("TRUCK")
public class FleetTruck extends Fleet {
    @Column(name = "payload_kg", precision = 10, scale = 2)
    BigDecimal payloadKg;

    protected FleetTruck() {}

    FleetTruck(Long id, String name, BigDecimal payloadKg) {
        super(id, name);
        this.payloadKg = payloadKg;
    }
}
