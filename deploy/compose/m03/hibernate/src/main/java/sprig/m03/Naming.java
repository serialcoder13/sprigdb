package sprig.m03;

import org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;

/**
 * Spring's default naming, with each catalog (a MySQL database) given the
 * stand-in tenant's prefix in baseline mode (SPRIG_DB_PREFIX). Through the
 * gateway the prefix is empty: both tenants use the same logical names.
 */
public class Naming extends CamelCaseToUnderscoresNamingStrategy {
    @Override
    public Identifier toPhysicalCatalogName(Identifier name, JdbcEnvironment env) {
        if (name == null) return null;
        return Identifier.toIdentifier(App.PREFIX + name.getText());
    }
}
