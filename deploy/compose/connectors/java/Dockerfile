# MySQL Connector/J and MariaDB Connector/J, pinned by version and SHA-256,
# plus HikariCP (the pool for pool_reprepare) with slf4j's API and no-op
# binding.
ARG JDK=eclipse-temurin:21.0.8_9-jdk
ARG JRE=eclipse-temurin:21.0.8_9-jre
FROM ${JDK} AS build
ARG MYSQL_CJ=9.7.0
ARG MYSQL_CJ_SHA256=0353648eaa1c91e0f4020c959abf756bc866ffd583df22ae6b6f6e0cbd43eb44
ARG MARIADB_CJ=3.5.11
ARG MARIADB_CJ_SHA256=42604178b9e57ec08ee0035079961516f086e455d0141f0b22c6eff29fb2fff4
ARG HIKARI=6.3.0
ARG HIKARI_SHA256=07c6344059af30a135144209c7c8bd666b8823124422ec859864d209d4ab7ca1
ARG SLF4J=2.0.17
ARG SLF4J_API_SHA256=7b751d952061954d5abfed7181c1f645d336091b679891591d63329c622eb832
ARG SLF4J_NOP_SHA256=3716f83649ec66161a2edefd4f49df34d1dd1c51cdcf941996c6987260f0a829
WORKDIR /app
RUN mkdir -p mysql mariadb lib \
 && M=https://repo1.maven.org/maven2 \
 && curl -sSfL -o mysql/connector.jar $M/com/mysql/mysql-connector-j/${MYSQL_CJ}/mysql-connector-j-${MYSQL_CJ}.jar \
 && curl -sSfL -o mariadb/connector.jar $M/org/mariadb/jdbc/mariadb-java-client/${MARIADB_CJ}/mariadb-java-client-${MARIADB_CJ}.jar \
 && curl -sSfL -o lib/hikari.jar $M/com/zaxxer/HikariCP/${HIKARI}/HikariCP-${HIKARI}.jar \
 && curl -sSfL -o lib/slf4j-api.jar $M/org/slf4j/slf4j-api/${SLF4J}/slf4j-api-${SLF4J}.jar \
 && curl -sSfL -o lib/slf4j-nop.jar $M/org/slf4j/slf4j-nop/${SLF4J}/slf4j-nop-${SLF4J}.jar \
 && echo "${MYSQL_CJ_SHA256}  mysql/connector.jar" | sha256sum -c - \
 && echo "${MARIADB_CJ_SHA256}  mariadb/connector.jar" | sha256sum -c - \
 && echo "${HIKARI_SHA256}  lib/hikari.jar" | sha256sum -c - \
 && echo "${SLF4J_API_SHA256}  lib/slf4j-api.jar" | sha256sum -c - \
 && echo "${SLF4J_NOP_SHA256}  lib/slf4j-nop.jar" | sha256sum -c -
COPY Check.java .
RUN javac -Xlint:all -cp lib/hikari.jar -d classes Check.java

FROM ${JRE}
COPY --from=build /app /app
COPY entrypoint.sh /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
