package sprig.r6;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** The root type: every asset, trucks and cars included. */
@Entity
@Table(name = "asset")
public class Asset {
    @Id
    Long id;

    @Column(nullable = false, length = 200)
    String name;

    @Column(name = "site_id")
    Integer siteId;

    protected Asset() {}
}
