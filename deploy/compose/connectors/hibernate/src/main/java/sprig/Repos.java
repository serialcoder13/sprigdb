package sprig;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/** The Spring Data repositories (nested, see App's @EnableJpaRepositories). */
public final class Repos {
    private Repos() {}

    public interface Authors extends JpaRepository<Author, Long> {
        Optional<Author> findByName(String name);
    }

    public interface Books extends JpaRepository<Book, Long> {
        List<Book> findByTitleContainingOrderByIdAsc(String part);

        @Query("select a.name, count(b), sum(b.price) from Book b join b.author a group by a.name order by a.name")
        List<Object[]> statsByAuthor();
    }
}
