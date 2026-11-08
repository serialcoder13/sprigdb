# sysbench 1.0.20 from Debian 13, on libmariadb (MariaDB Connector/C).
FROM debian:13.7-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends sysbench=1.0.20+ds-7+b1 \
 && rm -rf /var/lib/apt/lists/*
COPY sysbench.sh /sysbench.sh
ENTRYPOINT ["/sysbench.sh"]
