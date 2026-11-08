# MySQL Connector/J and MariaDB Connector/J, pinned by version and SHA-256.
ARG JDK=eclipse-temurin:21.0.8_9-jdk
ARG JRE=eclipse-temurin:21.0.8_9-jre
FROM ${JDK} AS build
ARG MYSQL_CJ=9.7.0
ARG MYSQL_CJ_SHA256=0353648eaa1c91e0f4020c959abf756bc866ffd583df22ae6b6f6e0cbd43eb44
ARG MARIADB_CJ=3.5.11
ARG MARIADB_CJ_SHA256=42604178b9e57ec08ee0035079961516f086e455d0141f0b22c6eff29fb2fff4
WORKDIR /app
RUN mkdir -p mysql mariadb \
 && curl -sSfL -o mysql/connector.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CJ}/mysql-connector-j-${MYSQL_CJ}.jar \
 && curl -sSfL -o mariadb/connector.jar https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/${MARIADB_CJ}/mariadb-java-client-${MARIADB_CJ}.jar \
 && echo "${MYSQL_CJ_SHA256}  mysql/connector.jar" | sha256sum -c - \
 && echo "${MARIADB_CJ_SHA256}  mariadb/connector.jar" | sha256sum -c -
COPY Check.java .
RUN javac -Xlint:all -d classes Check.java

FROM ${JRE}
COPY --from=build /app /app
COPY entrypoint.sh /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
