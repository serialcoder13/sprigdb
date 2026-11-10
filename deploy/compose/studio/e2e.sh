#!/usr/bin/env bash
# Studio's end-to-end suite (chapter 44 step 14 onward) against a gateway
# that deploy/compose/gateway-dev.sh brought up in catalog mode.
#
#   deploy/compose/studio/e2e.sh setup   # HTTP listener, operator login, sharded data, tokens
#   deploy/compose/studio/e2e.sh run     # Playwright with the installed Google Chrome
#
# setup:
#  1. binds the HTTP listener to 127.0.0.1:$STUDIO_HTTP_PORT (18080) with TLS
#     off before enabling it (Studio treats loopback as a secure context);
#  2. lists acme.app in studio.operator_logins (global) and gives tenant acme
#     a short studio.cursor_idle_seconds;
#  3. makes the sharding advisor shard studio2_shop (customers and orders)
#     with lowered advisor thresholds for acme only;
#  4. restarts the processes with an Admin API token, the gateways' narrow
#     credential and the operator token (decisions ST21 and ST34), all made
#     here under the state directory.
# Needs curl, jq, the mysql client and openssl; Node 22 with pnpm for run.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
state=${SPRIG_GATEWAY_DEV_STATE:-deploy/compose/.gateway-dev}
# shellcheck disable=SC1091
[[ -f $state/env ]] && source "$state/env"
etcd=${etcd:-${SPRIG_ETCD:-http://127.0.0.1:2379}}
admin_port=${admin_port:-${SPRIG_ADMIN_PORT:-7480}}
mysql_port=${mysql_port:-${SPRIG_GATEWAY_MYSQL_PORT:-13306}}
gw_telemetry_port=${gw_telemetry_port:-${SPRIG_GW_TELEMETRY_PORT:-9190}}
http_port=${STUDIO_HTTP_PORT:-18080}
bin=$(cat "$state/bin" 2>/dev/null || echo "${CARGO_TARGET_DIR:-target}/debug")
secrets=$state/studio-secrets
ctl() { "$bin/sprigctl" --endpoint "http://127.0.0.1:$admin_port" --yes "$@"; }
sql() { mysql -h127.0.0.1 -P"$mysql_port" -uacme.app -psprig-dev-app --ssl-mode=REQUIRED -e "$1" 2>&1 | grep -v "Using a password" || true; }
b64() { printf '%s' "$1" | base64 | tr -d '\n'; }
put_dc() {
    local v
    v=$(jq -cn --argjson v "$2" '{v: 1, value: $v, set_by: "op:studio-e2e", reason: "Studio e2e"}')
    curl -fsS -X POST "$etcd/v3/kv/put" -d "$(jq -cn --arg k "$(b64 "/sprig/dc/dc-a/settings/$1")" --arg v "$(b64 "$v")" '{key: $k, value: $v}')" >/dev/null
}

case ${1:-} in
setup)
    put_dc listener.http.bind "\"127.0.0.1:$http_port\""
    put_dc listener.http.tls '"off"'
    sleep 1
    put_dc listener.http.enabled true
    ctl settings set --scope global studio.operator_logins acme.app --reason "Studio e2e" --actor studio-e2e
    sql "ALTER TENANT acme SPRIG SET (studio.cursor_idle_seconds = 3)"
    sql "CREATE DATABASE IF NOT EXISTS studio2_shop; CREATE TABLE IF NOT EXISTS studio2_shop.customers (customer_id BIGINT PRIMARY KEY, name VARCHAR(40)); CREATE TABLE IF NOT EXISTS studio2_shop.orders (order_id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, total DECIMAL(10,2), CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES customers(customer_id)); SET SESSION cte_max_recursion_depth = 100000; INSERT IGNORE INTO studio2_shop.customers WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5000) SELECT n, CONCAT('c', n) FROM s; INSERT IGNORE INTO studio2_shop.orders WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 3000) SELECT n, 1 + n % 600, n FROM s; ANALYZE TABLE studio2_shop.customers, studio2_shop.orders"
    for i in $(seq 1 30); do sql "SELECT * FROM studio2_shop.orders o JOIN studio2_shop.customers c USING (customer_id) WHERE c.customer_id = $i" >/dev/null; done
    ctl settings set --scope db:acme.studio2_shop sharding.min_shards 2 --reason "Studio e2e" --actor studio-e2e
    ctl settings set --scope tenant:acme advisor.min_observe_hours 0 --reason "Studio e2e" --actor studio-e2e
    ctl settings set --scope tenant:acme advisor.min_units 10 --reason "Studio e2e" --actor studio-e2e
    # The advisor needs a little observed history; retry until it applies.
    for _ in $(seq 1 20); do
        ctl -o json advisor run acme | jq -e '.verdict == "apply"' >/dev/null && break
        sleep 15
    done
    mkdir -p "$secrets" && chmod 700 "$secrets"
    openssl rand -hex 32 >"$secrets/admin.token"
    openssl rand -hex 32 >"$secrets/gateway.token"
    SPRIGDB_CONTROLLER_ADMIN_TOKEN_FILE=$PWD/$secrets/admin.token \
    SPRIGDB_CONTROLLER_GATEWAY_TOKEN_FILE=$PWD/$secrets/gateway.token \
    SPRIGDB_GATEWAY_CONTROLLER_TOKEN_FILE=$PWD/$secrets/gateway.token \
    SPRIGDB_GATEWAY_OPERATOR_TOKEN_FILE=$PWD/$secrets/admin.token \
        deploy/compose/gateway-dev.sh restart
    for _ in $(seq 1 60); do curl -fsS -o /dev/null "http://127.0.0.1:$http_port/studio/" && break; sleep 1; done
    ;;
run)
    shift
    specs=("$@")
    [[ ${#specs[@]} -gt 0 ]] || specs=(e2e/studio.spec.ts e2e/security.spec.ts e2e/studio2.spec.ts)
    mkdir -p "$state/screenshots"
    cd studio
    STUDIO_URL=http://127.0.0.1:$http_port \
    STUDIO_METRICS=http://127.0.0.1:$gw_telemetry_port/metrics \
    STUDIO_SHOTS=${STUDIO_SHOTS:-../$state/screenshots} STUDIO_OUT=${STUDIO_OUT:-../$state/playwright-results} \
        pnpm e2e "${specs[@]}"
    ;;
*)
    echo "usage: $0 setup | run [spec files]" >&2
    exit 2
    ;;
esac
