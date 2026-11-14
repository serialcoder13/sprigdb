#!/usr/bin/env bash
# Studio's end-to-end suite against a gateway
# that deploy/compose/gateway-dev.sh brought up in catalog mode.
#
#   deploy/compose/studio/e2e.sh setup     # everything the suite needs, once (rerunnable)
#   deploy/compose/studio/e2e.sh run [specs or Playwright options]
#   deploy/compose/studio/e2e.sh restart   # gateway-dev.sh restart, keeping the tokens below
#   deploy/compose/studio/e2e.sh teardown  # what setup made that `gateway-dev.sh down` doesn't remove
#
# setup (each step can run again):
#  1. binds the HTTP and AMQP listeners to 127.0.0.1:$STUDIO_HTTP_PORT and
#     :$STUDIO_AMQP_PORT (bound before enabled, rule 8g), HTTP with TLS off
#     (Studio treats loopback as a secure context);
#  2. lists acme.app in studio.operator_logins and sets workload.watch_p95_ms
#     to 5 (both global: so a private deployment, not a shared one), gives tenant acme a
#     short studio.cursor_idle_seconds, the sink destination allowlist (the
#     suite's webhook receiver and a closed port, hooks.example.com) and the
#     logic outbound allowlist (rates.example.com), and SPRIG_SINK_ADMIN to
#     acme.app;
#  3. makes the child tenant $STUDIO_CHILD_TENANT under acme, with user app;
#  4. makes the sharding advisor shard studio2_shop (customers and orders)
#     with lowered advisor thresholds for acme only;
#  5. raises open index recommendations in approvals_shop (auto_index.mode
#     = approve and lowered thresholds for that database only, a filled
#     table and 60 slow lookups; about 2 minutes the first time, after the
#     restart of step 6);
#  6. restarts the processes with an Admin API token and the gateways'
#     narrow credential, made under the state
#     directory;
#  7. named operators: studio-e2e, a platform operator
#     for this script, signs in (enrolling TOTP; its secret is kept under
#     the state directory) and issues a token, which the gateway then uses
#     for Studio's operator mode instead of the bootstrap token (refused
#     once named operators are in use); and one operator per browser,
#     studio-e2e-chromium, -firefox and -webkit, for e2e/operators.spec.ts.
#
# teardown, then `SPRIGCTL_TOKEN_FILE=<state>/studio-secrets/operator.token
# gateway-dev.sh down`: after the switch to named operators sprigctl needs
# that token to delete the tenants.
#
# run: forgets the browser operators' second factors (their first sign-in
# enrols one), then runs Playwright with every address the specs need. With
# no spec named, the whole suite on Chromium (the installed Google Chrome),
# Firefox and WebKit.
#
# Ports (rule 15: every address explicit; defaults for the shared dev
# stack): STUDIO_HTTP_PORT (18080), STUDIO_AMQP_PORT (18682),
# STUDIO_HOOK_PORT (18881, the specs' own webhook receiver), STUDIO_CLOSED_PORT
# (18999, nothing listens). STUDIO_CHILD_TENANT (studiokid).
# Needs curl, jq, the mysql client, openssl and Node 22 (with pnpm for run).
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
amqp_port=${STUDIO_AMQP_PORT:-18682}
hook_port=${STUDIO_HOOK_PORT:-18881}
closed_port=${STUDIO_CLOSED_PORT:-18999}
kid=${STUDIO_CHILD_TENANT:-studiokid}
admin=http://127.0.0.1:$admin_port
bin=$(cat "$state/bin" 2>/dev/null || echo "${CARGO_TARGET_DIR:-target}/debug")
secrets=$state/studio-secrets
ops=(studio-e2e-chromium studio-e2e-firefox studio-e2e-webkit)

# The Admin API credential: the script operator's token once it exists,
# else the bootstrap token once the controller has one, else none (the
# loopback bootstrap of a fresh deployment).
token_file() {
    if [[ -s $secrets/operator.token ]]; then
        echo "$secrets/operator.token"
    elif [[ -s $secrets/admin.token && -f $secrets/restarted ]]; then
        echo "$secrets/admin.token"
    fi
}
ctl() {
    local t
    t=$(token_file)
    "$bin/sprigctl" --endpoint "$admin" ${t:+--token-file "$t"} --yes "$@"
}
api() { # METHOD PATH [JSON] [BEARER]
    curl -sS -X "$1" "$admin$2" -H 'content-type: application/json' ${4:+-H "authorization: Bearer $4"} ${3:+-d "$3"}
}
sql() { mysql -h127.0.0.1 -P"$mysql_port" -uacme.app -psprig-dev-app --ssl-mode=REQUIRED -e "$1" 2>&1 | grep -v "Using a password" || true; }
b64() { printf '%s' "$1" | base64 | tr -d '\n'; }
put_dc() {
    local v
    v=$(jq -cn --argjson v "$2" '{v: 1, value: $v, set_by: "op:studio-e2e", reason: "Studio e2e"}')
    curl -fsS -X POST "$etcd/v3/kv/put" -d "$(jq -cn --arg k "$(b64 "/sprig/dc/dc-a/settings/$1")" --arg v "$(b64 "$v")" '{key: $k, value: $v}')" >/dev/null
}
say() { echo "studio-e2e: $*"; }

# The current RFC 6238 code (SHA-1, 6 digits, 30 s) of a base32 secret.
totp() {
    node -e '
const s = process.argv[1].toUpperCase().replace(/=+$/, "");
const a = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
let bits = 0, v = 0; const out = [];
for (const c of s) { v = (v << 5) | a.indexOf(c); bits += 5; if (bits >= 8) { out.push((v >>> (bits - 8)) & 255); bits -= 8; } }
const m = Buffer.alloc(8); m.writeBigUInt64BE(BigInt(Math.floor(Date.now() / 30000)));
const h = require("node:crypto").createHmac("sha1", Buffer.from(out)).update(m).digest();
const o = h[h.length - 1] & 15;
console.log(String((h.readUInt32BE(o) & 0x7fffffff) % 1000000).padStart(6, "0"));' "$1"
}

# Restarts the processes with the tokens setup made (step 6, 7).
restart() {
    local extra=()
    if [[ -s $secrets/admin.token ]]; then
        extra+=(SPRIGDB_CONTROLLER_ADMIN_TOKEN_FILE="$PWD/$secrets/admin.token"
            SPRIGDB_CONTROLLER_GATEWAY_TOKEN_FILE="$PWD/$secrets/gateway.token"
            SPRIGDB_GATEWAY_CONTROLLER_TOKEN_FILE="$PWD/$secrets/gateway.token")
        if [[ -s $secrets/operator.token ]]; then
            extra+=(SPRIGDB_GATEWAY_OPERATOR_TOKEN_FILE="$PWD/$secrets/operator.token")
        else
            extra+=(SPRIGDB_GATEWAY_OPERATOR_TOKEN_FILE="$PWD/$secrets/admin.token")
        fi
    fi
    local t
    t=$(token_file)
    [[ -n $t ]] && extra+=(SPRIGCTL_TOKEN_FILE="$PWD/$t")
    env ${extra[@]+"${extra[@]}"} deploy/compose/gateway-dev.sh restart
    for _ in $(seq 1 60); do curl -fsS -o /dev/null "http://127.0.0.1:$http_port/studio/" && return 0; sleep 1; done
    say "the gateway's HTTP listener isn't answering on 127.0.0.1:$http_port"
    return 1
}

# Signs operator $1 in with password file $2 (enrolling TOTP the first time,
# its secret kept in $secrets/$1.totp) and prints the session token.
op_login() {
    local name=$1 pw r code=null ticket uri secret
    pw=$(cat "$2")
    [[ -s $secrets/$name.totp ]] && code="\"$(totp "$(cat "$secrets/$name.totp")")\""
    r=$(api POST /admin/v1/operators:login "$(jq -cn --arg n "$name" --arg p "$pw" --argjson c "$code" '{name: $n, password: $p, code: $c, client: "studio-e2e"}')")
    ticket=$(jq -r '.enrol_ticket // empty' <<<"$r")
    if [[ -n $ticket ]]; then
        uri=$(jq -r '.otpauth_uri' <<<"$r")
        secret=$(sed -E 's/.*[?&]secret=([A-Z2-7]+).*/\1/' <<<"$uri")
        (umask 077 && echo "$secret" >"$secrets/$name.totp")
        r=$(api POST /admin/v1/operators:enrol "$(jq -cn --arg n "$name" --arg t "$ticket" --arg c "$(totp "$secret")" '{name: $n, ticket: $t, code: $c, client: "studio-e2e"}')")
    fi
    jq -er '.token' <<<"$r" || { say "operator $name couldn't sign in: $r" >&2; return 1; }
}

setup() {
    mkdir -p "$secrets" && chmod 700 "$secrets"
    # 1. Listeners.
    put_dc listener.http.bind "\"127.0.0.1:$http_port\""
    put_dc listener.http.tls '"off"'
    put_dc listener.amqp.bind "\"127.0.0.1:$amqp_port\""
    sleep 1
    put_dc listener.http.enabled true
    put_dc listener.amqp.enabled true
    # 2. Settings and rights.
    ctl settings set --scope global studio.operator_logins acme.app --reason "Studio e2e" --actor studio-e2e
    # Workload probes for fingerprints slower than 5 ms: the approvals_shop
    # lookups of step 5 take about 20 ms, under the default 50. Gateways
    # read it at the global scope when they start (step 6 restarts them).
    ctl settings set --scope global workload.watch_p95_ms 5 --reason "Studio e2e" --actor studio-e2e
    ctl settings set --scope tenant:acme sink.destination_allowlist \
        "[\"127.0.0.1:$hook_port\", \"127.0.0.1:$closed_port\", \"hooks.example.com\"]" --reason "Studio e2e" --actor studio-e2e
    ctl settings set --scope tenant:acme logic.outbound_allowlist '["rates.example.com"]' --reason "Studio e2e" --actor studio-e2e
    sql "ALTER TENANT acme SPRIG SET (studio.cursor_idle_seconds = 3)"
    sql "GRANT SPRIG_SINK_ADMIN ON *.* TO app"
    # 3. The child tenant (rule 8a: deleted by name in teardown).
    if ! ctl tenant list 2>/dev/null | grep -qw "$kid"; then
        # On acme's cluster: shares to a child need both on one cluster.
        ctl tenant create "$kid" --under acme --cluster dc-a-c1
        echo "$kid" >>"$secrets/tenants"
    fi
    ctl tenant user add "$kid" app --password sprig-dev-app --native --if-not-exists
    # 4. Sharding.
    sql "CREATE DATABASE IF NOT EXISTS studio2_shop; CREATE TABLE IF NOT EXISTS studio2_shop.customers (customer_id BIGINT PRIMARY KEY, name VARCHAR(40)); CREATE TABLE IF NOT EXISTS studio2_shop.orders (order_id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, total DECIMAL(10,2), CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES customers(customer_id)); SET SESSION cte_max_recursion_depth = 100000; INSERT IGNORE INTO studio2_shop.customers WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5000) SELECT n, CONCAT('c', n) FROM s; INSERT IGNORE INTO studio2_shop.orders WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 3000) SELECT n, 1 + n % 600, n FROM s; ANALYZE TABLE studio2_shop.customers, studio2_shop.orders"
    for i in $(seq 1 30); do sql "SELECT * FROM studio2_shop.orders o JOIN studio2_shop.customers c USING (customer_id) WHERE c.customer_id = $i" >/dev/null; done
    ctl settings set --scope db:acme.studio2_shop sharding.min_shards 2 --reason "Studio e2e" --actor studio-e2e
    ctl settings set --scope tenant:acme advisor.min_observe_hours 0 --reason "Studio e2e" --actor studio-e2e
    ctl settings set --scope tenant:acme advisor.min_units 10 --reason "Studio e2e" --actor studio-e2e
    # The advisor needs a little observed history; retry until it applies
    # (39471 once the tenant has table groups: an earlier setup sharded it).
    local out
    for _ in $(seq 1 20); do
        out=$(ctl -o json advisor run acme 2>&1 || true)
        jq -e '.verdict == "apply"' >/dev/null 2>&1 <<<"$out" && break
        grep -q 39471 <<<"$out" && break
        sleep 15
    done
    # 5. Open index recommendations in approvals_shop.
    sql "CREATE DATABASE IF NOT EXISTS approvals_shop"
    sql "ALTER DATABASE approvals_shop SPRIG SET (auto_index.mode = 'approve', workload.examined_sample_rate = 1, workload.sample_rate = 1, auto_index.window_hours = 1, auto_index.p95_threshold_ms = 5, auto_index.min_runs = 20, auto_index.min_net_ms_per_hour = 1, auto_index.min_benefit_ratio = 0)"
    # The cooldown counts every policy's, the tenant's too: without 0 a
    # fingerprint evaluated before it had probes waits a day.
    sql "ALTER TENANT acme SPRIG SET (auto_index.eval_interval_minutes = 1, auto_index.cooldown_hours = 0)"
    sql "CREATE TABLE IF NOT EXISTS approvals_shop.parts (id BIGINT PRIMARY KEY AUTO_INCREMENT, sku INT NOT NULL, status VARCHAR(12) NOT NULL, note VARCHAR(100) NOT NULL) ENGINE=InnoDB"
    sql "CREATE TABLE IF NOT EXISTS approvals_shop.orders (id BIGINT PRIMARY KEY, part_id BIGINT NOT NULL) ENGINE=InnoDB"
    if [[ $(sql "SELECT COUNT(*) AS n FROM approvals_shop.parts" | tail -1) -lt 200000 ]]; then
        sql "SET SESSION cte_max_recursion_depth = 1000; INSERT INTO approvals_shop.parts (sku, status, note) WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < 499) SELECT x.n, IF(y.n % 10 = 0, 'open', 'paid'), REPEAT('x', 60) FROM s x, s y WHERE y.n < 400"
    fi
    open_index() { mysql -h127.0.0.1 -P"$mysql_port" -uacme.app -psprig-dev-app --ssl-mode=REQUIRED -N -e "SHOW SPRIG RECOMMENDATIONS FOR TABLE approvals_shop.parts" 2>/dev/null | awk '$2 ~ /^index\./ && ($4 == "open" || $4 == "snoozed" || $4 == "ignored")' | wc -l | tr -d ' '; }
    local raise=false
    # (Live items only: a run snoozes them, and the spec reopens them.)
    [[ $(open_index) -lt 1 ]] && raise=true
    # Slow lookups on parts.sku: once the fingerprint is watched (its p95
    # above workload.watch_p95_ms), its runs are probed for the advisor.
    lookups() { for i in $(seq 1 20); do sql "SELECT COUNT(*), MAX(note) FROM approvals_shop.parts WHERE sku = $((i % 500))" >/dev/null; done; }
    # 6. Tokens. The restart also starts the controller's auto_index loop
    # afresh, so it evaluates at once with the 1-minute interval above
    # (a loop already waiting may otherwise sleep up to 10 minutes); the
    # lookups run after it, as a restart forgets which fingerprints are
    # watched.
    [[ -s $secrets/admin.token ]] || (umask 077 && openssl rand -hex 32 >"$secrets/admin.token")
    [[ -s $secrets/gateway.token ]] || (umask 077 && openssl rand -hex 32 >"$secrets/gateway.token")
    if [[ ! -f $secrets/restarted ]]; then
        touch "$secrets/restarted"
        restart
    elif $raise; then
        restart
    fi
    if $raise; then
        say "waiting for index recommendations in approvals_shop (about 2 minutes)"
        for _ in $(seq 1 72); do [[ $(open_index) -ge 1 ]] && break; lookups; sleep 10; done
        [[ $(open_index) -ge 1 ]] || { say "no index recommendation in approvals_shop after 12 minutes"; exit 1; }
    fi
    # 7. Named operators.
    [[ -s $secrets/operator.password ]] || (umask 077 && openssl rand -hex 16 >"$secrets/operator.password")
    local name
    for name in studio-e2e "${ops[@]}"; do
        if ! ctl operator show "$name" >/dev/null 2>&1; then
            ctl operator create "$name" --role platform --password-file "$secrets/operator.password" --reason "Studio e2e"
        fi
    done
    if [[ ! -s $secrets/operator.token ]]; then
        local session tok
        session=$(op_login studio-e2e "$secrets/operator.password")
        tok=$(api POST /admin/v1/operator-tokens '{"label": "Studio e2e: the gateway operator token and this script", "scopes": ["*:read", "*:write"], "expires_in_days": 30}' "$session" | jq -er '.token')
        (umask 077 && echo "$tok" >"$secrets/operator.token")
        restart
    fi
    say "ready: http://127.0.0.1:$http_port/studio/, AMQP 127.0.0.1:$amqp_port, child tenant $kid"
}

case ${1:-} in
setup) setup ;;
restart) restart ;;
run)
    shift
    # The browser operators enrol again on their first sign-in. Resetting a
    # factor takes a platform operator's login session (a token can't), so
    # the script operator signs in; a TOTP code is good once, hence the
    # second try in the next 30-second step.
    session=$(op_login studio-e2e "$secrets/operator.password" 2>/dev/null) ||
        { sleep 30; session=$(op_login studio-e2e "$secrets/operator.password"); }
    for name in "${ops[@]}"; do
        api POST "/admin/v1/operators/$name:resetFactor" '{"reason": "Studio e2e rerun"}' "$session" | jq -e '.name // .operator // true' >/dev/null
    done
    api POST /admin/v1/operators:logout '{}' "$session" >/dev/null || true
    mkdir -p "$state/screenshots"
    cd studio
    STUDIO_URL=http://127.0.0.1:$http_port \
    STUDIO_METRICS=http://127.0.0.1:$gw_telemetry_port/metrics \
    STUDIO_AMQP=127.0.0.1:$amqp_port STUDIO_HOOK_PORT=$hook_port STUDIO_CLOSED=127.0.0.1:$closed_port \
    STUDIO_CHILD=$kid.app \
    STUDIO_OPERATOR='studio-e2e-{browser}' STUDIO_OPERATOR_PASSWORD="$(cat "../$secrets/operator.password")" \
    STUDIO_SHOTS=${STUDIO_SHOTS:-../$state/screenshots} STUDIO_OUT=${STUDIO_OUT:-../$state/playwright-results} \
        npx --no-install playwright test "$@"
    ;;
teardown)
    # The child tenant (by its exact name), the operators and their tokens.
    if [[ -s $secrets/tenants ]]; then
        while read -r t; do [[ -n $t ]] && ctl tenant delete "$t" || true; done <"$secrets/tenants"
        rm -f "$secrets/tenants"
    fi
    for name in "${ops[@]}"; do ctl operator disable "$name" --reason "Studio e2e teardown" >/dev/null 2>&1 || true; done
    say "child tenant deleted and browser operators disabled; SPRIGCTL_TOKEN_FILE=$PWD/$(token_file) deploy/compose/gateway-dev.sh down deletes the rest"
    ;;
*)
    echo "usage: $0 setup | run [specs] | restart | teardown" >&2
    exit 2
    ;;
esac
