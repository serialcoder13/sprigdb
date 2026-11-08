package sprig.m03;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/** Spring Data repositories. */
public final class Repos {
    private Repos() {}

    public interface Customers extends JpaRepository<Customer, Integer> {
        Customer findByEmail(String email);
    }

    public interface Products extends JpaRepository<Product, Long> {}

    public interface Orders extends JpaRepository<PurchaseOrder, Long> {
        @Query("select o from PurchaseOrder o join fetch o.customer c join fetch o.product p where c.email = ?1")
        List<PurchaseOrder> placedBy(String email);
    }

    public interface Totals extends JpaRepository<OrderTotal, Long> {
        List<OrderTotal> findByEmail(String email);
    }
}
