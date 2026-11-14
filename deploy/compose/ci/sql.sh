#!/usr/bin/env bash
# Runs SQL through the CI stack's gateway as the tenant login acme.app, with
# the mariadb client of a pinned image on the host network (CI runners are
# Linux). Logs in over TLS, which also fills the gateway's
# caching_sha2_password cache for later plaintext logins (the gateway has
# no RSA key exchange).
#
#   sql.sh "<statements>"            MySQL persona, 127.0.0.1:13306
#   SPRIG_CI_SQL_PORT=13307 sql.sh "<statements>"
set -euo pipefail
[[ $# -eq 1 ]] || { echo "usage: sql.sh '<statements>'" >&2; exit 2; }
docker run --rm -i --name "ci-sql-$$" --network host "${SPRIG_CI_MARIADB_IMAGE:-mariadb:11.8.9}" \
    mariadb --ssl --disable-ssl-verify-server-cert -h 127.0.0.1 -P "${SPRIG_CI_SQL_PORT:-13306}" \
    -u "${SPRIG_CI_SQL_USER:-acme.app}" -p"${SPRIG_CI_SQL_PASSWORD:-sprig-dev-app}" -e "$1"
