#!/usr/bin/env bash
# Turns on the gateway's protocol listeners for CI's heavy-suite jobs
# (.github/workflows/heavy-*.yml), on the stack deploy/compose/gateway-dev.sh
# started: each listener's address is written before it is enabled
# (agent brief rule 8g), at dc scope in that stack's etcd.
#
#   listeners.sh <listener>...      mongo, kafka, amqp, bolt, redis
#
# Ports (never the protocols' defaults, so a reference server of the same
# protocol can run on its usual port next to the gateway):
#
#   mongo 127.0.0.1:28017 (TLS optional)   kafka 127.0.0.1:29092 (TLS optional)
#   amqp  127.0.0.1:25672 (plaintext)      bolt  127.0.0.1:27687 (TLS optional)
#   redis 127.0.0.1:26379 (TLS optional)
#
# Kafka advertises 127.0.0.1 unless SPRIG_CI_KAFKA_HOST says otherwise (a
# client in a container on the host network reaches it there). AMQP and Redis
# bind plain user names to the tenant SPRIG_CI_TENANT (default acme), so
# `app` / sprig-dev-app logs in as gateway-dev.sh's acme.app.
#
# Waits until each port accepts connections (60 s).
set -euo pipefail

etcd=${SPRIG_ETCD:-http://127.0.0.1:2379}
dc=${SPRIG_CI_DC:-dc-a}
tenant=${SPRIG_CI_TENANT:-acme}

b64() { printf '%s' "$1" | base64 | tr -d '\n'; }

# set <key> <json value>
set_setting() {
    local rec
    rec=$(jq -cn --argjson v "$2" '{v: 1, value: $v, set_by: "op:ci", reason: "deploy/compose/ci/listeners.sh"}')
    curl -fsS -X POST "$etcd/v3/kv/put" \
        -d "$(jq -cn --arg k "$(b64 "/sprig/dc/$dc/settings/$1")" --arg v "$(b64 "$rec")" '{key: $k, value: $v}')" >/dev/null
}

wait_port() {
    local port=$1
    timeout 60 bash -c "until (echo > /dev/tcp/127.0.0.1/$port) 2>/dev/null; do sleep 1; done" ||
        { echo "listeners: nothing listens on 127.0.0.1:$port after 60 s" >&2; exit 1; }
}

[[ $# -gt 0 ]] || { sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2; }
for l in "$@"; do
    case $l in
    mongo) port=28017 tls=optional ;;
    kafka) port=29092 tls=optional ;;
    amqp) port=25672 tls=off ;;
    bolt) port=27687 tls=optional ;;
    redis) port=26379 tls=optional ;;
    *) echo "listeners: unknown listener $l" >&2; exit 2 ;;
    esac
    set_setting "listener.$l.bind" "\"127.0.0.1:$port\""
    set_setting "listener.$l.tls" "\"$tls\""
    case $l in
    kafka) set_setting kafka.advertised_host "\"${SPRIG_CI_KAFKA_HOST:-127.0.0.1}\"" ;;
    amqp | redis) set_setting "listener.$l.tenant" "\"$tenant\"" ;;
    esac
    sleep 1
    set_setting "listener.$l.enabled" true
    wait_port "$port"
    echo "listeners: $l on 127.0.0.1:$port (TLS $tls)"
done
