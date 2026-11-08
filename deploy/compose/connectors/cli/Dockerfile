# The `mysql` or `mariadb` command-line client, and `mysqldump` or
# `mariadb-dump`, taken from the server image that ships them:
# BASE=mysql:8.4.11 or BASE=mariadb:11.8.9.
ARG BASE=mysql:8.4.11
FROM ${BASE}
COPY check.sh dump.sh /
ENTRYPOINT ["/check.sh"]
