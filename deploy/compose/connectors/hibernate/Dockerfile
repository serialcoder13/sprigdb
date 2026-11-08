# Hibernate 6 via Spring Data JPA (pom.xml pins Spring Boot 3.5.7, so
# Hibernate 6.6.33.Final), over MySQL Connector/J or MariaDB Connector/J.
# Maven 3.9.11 is pinned by SHA-512; the JDK and JRE are the java/ image's.
ARG JDK=eclipse-temurin:21.0.8_9-jdk
ARG JRE=eclipse-temurin:21.0.8_9-jre
FROM ${JDK} AS build
ARG MAVEN=3.9.11
ARG MAVEN_SHA512=bcfe4fe305c962ace56ac7b5fc7a08b87d5abd8b7e89027ab251069faebee516b0ded8961445d6d91ec1985dfe30f8153268843c89aa392733d1a3ec956c9978
RUN curl -sSfL -o /tmp/maven.tgz https://archive.apache.org/dist/maven/maven-3/${MAVEN}/binaries/apache-maven-${MAVEN}-bin.tar.gz \
 && echo "${MAVEN_SHA512}  /tmp/maven.tgz" | sha512sum -c - \
 && tar -xzf /tmp/maven.tgz -C /opt && ln -s /opt/apache-maven-${MAVEN}/bin/mvn /usr/local/bin/mvn
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src src
RUN mvn -B -q package -DskipTests

FROM ${JRE}
COPY --from=build /src/target/check.jar /app/check.jar
ENTRYPOINT ["java", "-Xmx384m", "-XX:TieredStopAtLevel=1", "-jar", "/app/check.jar"]
