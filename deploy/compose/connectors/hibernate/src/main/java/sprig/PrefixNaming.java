package sprig;

import org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;

/** Spring's default naming, with each table prefixed per connector and TLS mode (App.PREFIX). */
public class PrefixNaming extends CamelCaseToUnderscoresNamingStrategy {
    @Override
    public Identifier toPhysicalTableName(Identifier name, JdbcEnvironment env) {
        return Identifier.toIdentifier(App.PREFIX + "_" + super.toPhysicalTableName(name, env).getText());
    }
}
