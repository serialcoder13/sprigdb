package sprig.m03;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.time.LocalDate;

/** shop.orders (V2, V3): one foreign key in the same database, one into crm. */
@Entity
@Table(name = "orders", catalog = "shop")
public class PurchaseOrder {
    enum Status { NEW, PAID, SHIPPED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "customer_id", nullable = false)
    Customer customer;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "product_id", nullable = false)
    Product product;

    @Column(nullable = false)
    Integer qty;

    @Column(nullable = false)
    LocalDate placedOn;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    Status status;

    @Column(length = 255)
    String note;

    protected PurchaseOrder() {}
}
