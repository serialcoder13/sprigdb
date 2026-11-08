package sprig.m03;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;

/** crm.customers (V1). The catalog is the logical database; Naming prefixes it in baseline mode. */
@Entity
@Table(name = "customers", catalog = "crm")
public class Customer {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Integer id;

    @Column(nullable = false, length = 190, unique = true)
    String email;

    @Column(nullable = false, length = 100)
    String name;

    @Column(nullable = false, insertable = false, updatable = false)
    LocalDateTime createdAt;

    protected Customer() {}

    Customer(String email, String name) {
        this.email = email;
        this.name = name;
    }
}
