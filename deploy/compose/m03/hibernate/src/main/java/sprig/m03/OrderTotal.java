package sprig.m03;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import org.hibernate.annotations.Immutable;

/** The view shop.order_totals (V4), which joins across both databases. Validated like a table. */
@Entity
@Immutable
@Table(name = "order_totals", catalog = "shop")
public class OrderTotal {
    @Id
    Long id;

    String email;

    String sku;

    BigDecimal total;

    protected OrderTotal() {}
}
