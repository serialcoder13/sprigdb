# The `mysql` or `mariadb` command-line client, taken from the server image
# that ships it: BASE=mysql:8.4.11 or BASE=mariadb:11.8.9.
ARG BASE=mysql:8.4.11
FROM ${BASE}
COPY check.sh /check.sh
ENTRYPOINT ["/check.sh"]
