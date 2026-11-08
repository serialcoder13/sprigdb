# M0.3 Hibernate schema validation (../README.md): Spring Data JPA with
# ddl-auto=validate against the schema Flyway created, over MySQL
# Connector/J 9.7.0 or MariaDB Connector/J 3.5.11.
#
# The build is the connector matrix's (../../connectors/hibernate): the same
# JDK, Maven pinned by SHA-512 and the very same pom.xml (taken from the
# `conn` build context, see compose.yaml), so Docker's layer cache shares
# the dependency download with the conn-hibernate image. Only src differs.
ARG JDK=eclipse-temurin:21.0.8_9-jdk
ARG JRE=eclipse-temurin:21.0.8_9-jre
FROM ${JDK} AS build
ARG MAVEN=3.9.11
ARG MAVEN_SHA512=bcfe4fe305c962ace56ac7b5fc7a08b87d5abd8b7e89027ab251069faebee516b0ded8961445d6d91ec1985dfe30f8153268843c89aa392733d1a3ec956c9978
RUN curl -sSfL -o /tmp/maven.tgz https://archive.apache.org/dist/maven/maven-3/${MAVEN}/binaries/apache-maven-${MAVEN}-bin.tar.gz \
 && echo "${MAVEN_SHA512}  /tmp/maven.tgz" | sha512sum -c - \
 && tar -xzf /tmp/maven.tgz -C /opt && ln -s /opt/apache-maven-${MAVEN}/bin/mvn /usr/local/bin/mvn
WORKDIR /src
COPY --from=conn pom.xml .
RUN mvn -B -q dependency:go-offline
# Docker's VM is shared and small: keep Maven's own heap modest.
ENV MAVEN_OPTS=-Xmx384m
COPY src src
RUN mvn -B -q package -DskipTests

FROM ${JRE}
COPY --from=build /src/target/check.jar /app/m03.jar
ENTRYPOINT ["java", "-Xmx256m", "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", "-jar", "/app/m03.jar"]
