package sprig.r6;

import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;

/** vtype = 'CAR': SprigDB's fleet_car. */
@Entity
@DiscriminatorValue("CAR")
public class FleetCar extends Fleet {
    Integer seats;

    protected FleetCar() {}
}
