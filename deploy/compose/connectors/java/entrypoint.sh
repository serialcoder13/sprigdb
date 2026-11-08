#!/bin/sh
# Puts only the selected driver on the classpath.
case "$SPRIG_CONNECTOR" in
  mysql-connector-j) jar=/app/mysql/connector.jar ;;
  mariadb-connector-j) jar=/app/mariadb/connector.jar ;;
  *) echo "unknown SPRIG_CONNECTOR: $SPRIG_CONNECTOR" >&2; exit 2 ;;
esac
exec java -Xmx512m -cp "/app/classes:$jar:/app/lib/*" Check
