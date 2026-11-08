package sprig;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "book")
public class Book {
    enum Status { DRAFT, PUBLISHED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @Version
    long version;

    @Column(nullable = false, length = 128)
    String title;

    @Column(nullable = false, precision = 10, scale = 2)
    BigDecimal price;

    LocalDateTime published; // datetime(6)

    LocalDate releaseDate;

    boolean inPrint; // bit

    @Enumerated(EnumType.STRING) // a native ENUM on MySQL and MariaDB since Hibernate 6.2
    Status status;

    @Lob
    @Column(length = 16 * 1024 * 1024)
    byte[] cover; // longblob

    @JdbcTypeCode(SqlTypes.JSON)
    Map<String, Object> attrs;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "author_id")
    Author author;

    protected Book() {}

    Book(String title, BigDecimal price) {
        this.title = title;
        this.price = price;
    }
}
