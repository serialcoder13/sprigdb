# sysbench 1.0.20 from Debian 13, on libmariadb (MariaDB Connector/C), and
# Percona-Lab's sysbench-tpcc scripts at a pinned commit (tpcc.sh).
FROM debian:13.7-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends sysbench=1.0.20+ds-7+b1 \
 && rm -rf /var/lib/apt/lists/*
ARG TPCC=https://raw.githubusercontent.com/Percona-Lab/sysbench-tpcc/f110afa8023c7924b1ba00177232a9090624acb5
ADD --checksum=sha256:c1d3cc99bec35b2c42f1870bb006670be07b18281c7d482d6de7f2ef4efef007 ${TPCC}/tpcc.lua /tpcc/
ADD --checksum=sha256:614b27d5fb3d47df6d9a5b6e3c2d990a5f1a61d458fd55c045f36cc4b2107143 ${TPCC}/tpcc_check.lua /tpcc/
ADD --checksum=sha256:0f3c9111eb6de79577a23fc65b6ee9f6adfe404739403412ae3eb323d761ea63 ${TPCC}/tpcc_common.lua /tpcc/
ADD --checksum=sha256:217c32f705da7670229871835612464d996071a590259ca7f40812051d596c23 ${TPCC}/tpcc_run.lua /tpcc/
RUN chmod 0644 /tpcc/*.lua
COPY sysbench.sh tpcc.sh /
ENTRYPOINT ["/sysbench.sh"]
