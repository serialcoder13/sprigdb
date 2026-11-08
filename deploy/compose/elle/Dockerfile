# elle-cli (https://github.com/ligurio/elle-cli, EPL-2.0) for checking
# operation histories with Elle and Knossos (chapter 46, "Workloads and
# checkers"). A test tool run as a separate process; nothing of it ships.
#
#   docker build -t sprigdb-elle:dev deploy/compose/elle
#   docker run --rm -v "$PWD:/work:ro" sprigdb-elle:dev --model list-append /work/history.edn
# The JDK image: the JRE lacks the jdk.random module Jepsen needs.
FROM eclipse-temurin:21-jdk
ARG ELLE_VERSION=0.1.11
ARG ELLE_SHA256=9d3c72eaf3ccc4d0c7dc1da710264dee01f03af58c9fa177fec3f78ff5af1779
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates \
    && curl -fsSL -o /tmp/elle.zip \
       "https://github.com/ligurio/elle-cli/releases/download/${ELLE_VERSION}/elle-cli-bin-${ELLE_VERSION}.zip" \
    && echo "${ELLE_SHA256}  /tmp/elle.zip" | sha256sum -c - \
    && unzip -j /tmp/elle.zip "target/elle-cli-${ELLE_VERSION}-standalone.jar" -d /opt \
    && mv "/opt/elle-cli-${ELLE_VERSION}-standalone.jar" /opt/elle-cli.jar \
    && rm /tmp/elle.zip && apt-get purge -y curl unzip && rm -rf /var/lib/apt/lists/*
WORKDIR /work
ENTRYPOINT ["java", "-jar", "/opt/elle-cli.jar"]
