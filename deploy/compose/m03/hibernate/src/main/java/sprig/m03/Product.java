package sprig.m03;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;

/** shop.products (V2). */
@Entity
@Table(name = "products", catalog = "shop")
public class Product {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @Column(nullable = false, length = 32, unique = true)
    String sku;

    @Column(nullable = false, length = 200)
    String title;

    @Column(nullable = false, precision = 12, scale = 2)
    BigDecimal price;

    @Column(nullable = false)
    Boolean active;

    protected Product() {}
}
