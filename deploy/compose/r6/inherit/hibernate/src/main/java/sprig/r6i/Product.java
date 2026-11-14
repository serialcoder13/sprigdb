package sprig.r6i;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** An inheritable table: the merged view of the session's space. */
@Entity
@Table(name = "products")
public class Product {
    @Id
    Integer id;

    @Column(length = 40)
    String name;

    @Column(length = 20)
    String colour;

    Integer price;

    @Column(length = 2, columnDefinition = "char(2)")
    String region;

    protected Product() {}

    Product(Integer id, String name, String colour, Integer price, String region) {
        this.id = id;
        this.name = name;
        this.colour = colour;
        this.price = price;
        this.region = region;
    }
}
