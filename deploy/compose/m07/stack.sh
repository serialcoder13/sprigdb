#!/usr/bin/env bash
# M0.7's private stack (chapters 04, 23, 37): its own etcd and MariaDB
# (compose.yaml here, project sprigdb-dev-cp), one controller and two
# gateways on the host, in data center dc-m7. Tests that change dc-scope
# listener, TLS and gateway settings, drain gateways or run load use it, so
# the shared dev stack's gateways never see those changes.
#
#   deploy/compose/m07/stack.sh up       # containers, binaries, processes, tenant
#   deploy/compose/m07/stack.sh status
#   deploy/compose/m07/stack.sh gateway 1|2 start|stop
#   deploy/compose/m07/stack.sh down     # stop processes, remove containers
#
# Ports (127.0.0.1): etcd 12479, MariaDB 13391; controller Admin 19480,
# telemetry 19180; gateway 1: MySQL 19306, MariaDB 19307, HTTP 19080, HTTPS
# 19443, peer 19403, telemetry 19190; gateway 2: 19316, 19317, 19090, 19453,
# 19413, 19191. Tenant m7a, user app, password sprig-dev-app.
#
# Variables: CARGO_TARGET_DIR (target), SPRIG_M7_PROFILE (debug or release).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
root=$(pwd)
here=deploy/compose/m07
state=$here/.state
profile=${SPRIG_M7_PROFILE:-debug}
bin=$(cat "$state/bin" 2>/dev/null || echo "${CARGO_TARGET_DIR:-target}/$profile")
etcd=http://127.0.0.1:12479
dc=dc-m7
admin=http://127.0.0.1:19480
certs=deploy/compose/gateway-certs
# The gateways' certificate files: copies, so rotating them (soak tests)
# never touches the shared dev gateway's. server-b.pem is a second
# certificate from the same dev CA, for rotation.
mycerts=$state/certs

say() { echo "m07: $*"; }
die() { echo "m07: $*" >&2; exit 1; }
ctl() { "$bin/sprigctl" --endpoint "$admin" "$@"; }

etcd_put() {
    curl -fsS -X POST "$etcd/v3/kv/put" \
        -d "$(jq -cn --arg k "$1" --arg v "$2" '{key: ($k|@base64), value: ($v|@base64)}')" >/dev/null
}

# A dc-scope setting record; $2 is the value as JSON.
set_setting() {
    etcd_put "/sprig/dc/$dc/settings/$1" "$(jq -cn --argjson v "$2" \
        '{v: 1, value: $v, set_by: "op:m07", reason: "deploy/compose/m07/stack.sh"}')"
}

pid_of() { cat "$state/$1.pid" 2>/dev/null || true; }
running() {
    local pid
    pid=$(pid_of "$1")
    [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null && ps -p "$pid" -o command= | grep -q sprigdb
}

start() {
    local name=$1 config=$2 role=$3 ready=$4
    if running "$name"; then
        say "$name already running"
        return
    fi
    [[ -f $state/$name.log ]] && mv -f "$state/$name.log" "$state/$name.log.1"
    RUST_LOG=${RUST_LOG:-info} nohup "$bin/sprigdb" --config "$config" "$role" >"$state/$name.log" 2>&1 </dev/null &
    echo $! >"$state/$name.pid"
    for _ in $(seq 1 120); do
        curl -fsS -o /dev/null "$ready" 2>/dev/null && { say "$name ready (pid $(pid_of "$name"))"; return; }
        running "$name" || { tail -n 30 "$state/$name.log" >&2; die "$name exited"; }
        sleep 0.5
    done
    die "$name not ready; see $state/$name.log"
}

stop() {
    local name=$1 pid
    running "$name" || { rm -f "$state/$name.pid"; return; }
    pid=$(pid_of "$name")
    kill -TERM "$pid"
    for _ in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid"
    rm -f "$state/$name.pid"
    say "$name stopped"
}

# The dev CA's server certificate (a) and a second one (b) for rotation.
make_certs() {
    mkdir -p "$mycerts"
    [[ -s $mycerts/server-a.pem ]] && return
    cp "$certs/server.pem" "$mycerts/server-a.pem"
    cp "$certs/server-key.pem" "$mycerts/server-a-key.pem"
    (
        cd "$mycerts"
        openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
            -keyout server-b-key.pem -out b.csr 2>/dev/null
        printf '%s\n' "subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:host.docker.internal" \
            "basicConstraints=CA:FALSE" "keyUsage=digitalSignature,keyEncipherment" \
            "extendedKeyUsage=serverAuth" >b.ext
        openssl x509 -req -in b.csr -CA "$root/$certs/ca.pem" -CAkey "$root/$certs/ca-key.pem" \
            -CAcreateserial -CAserial b.srl -days 825 -extfile b.ext -out server-b.pem 2>/dev/null
        rm -f b.csr b.ext b.srl
    )
    cp "$mycerts/server-a.pem" "$mycerts/server.pem"
    cp "$mycerts/server-a-key.pem" "$mycerts/server-key.pem"
}

gw_ready() { echo "http://127.0.0.1:$((19189 + $1))/readyz"; }

up() {
    mkdir -p "$state"
    echo '*' >"$state/.gitignore"
    docker compose -f "$here/compose.yaml" up -d --wait
    [[ -x $bin/sprigdb ]] || die "no $bin/sprigdb: cargo build -p sprigdb -p sprigctl first"
    echo "$bin" >"$state/bin"
    start controller "$here/controller.toml" controller http://127.0.0.1:19180/readyz
    for _ in $(seq 1 60); do
        ctl -o json dc show "$dc" 2>/dev/null | jq -e '.leader != null' >/dev/null && break
        sleep 0.5
    done
    if ! ctl -o json cluster list | jq -e '.clusters[] | select(.name == "m7-single")' >/dev/null; then
        (umask 077 && printf '[sprig_ctl]\nuser = "root"\npassword = "sprig-dev-root"\ntls = "skip_verify"\n[sprig_repl]\nuser = "root"\npassword = "sprig-dev-root"\ntls = "skip_verify"\n' >"$state/creds.toml")
        ctl cluster register m7-single --topology single --node 127.0.0.1:13391 \
            --credentials "file:$root/$state/creds.toml" >/dev/null
        say "cluster m7-single registered"
    fi
    # Listeners of both gateways come from listen_override; TLS and
    # certificates are dc-scope settings of dc-m7.
    [[ -s $certs/server.pem ]] || die "no dev certificates in $certs: run deploy/compose/gateway-dev.sh up once (it makes them)"
    make_certs
    set_setting tls.bundles.default.cert_path "\"$root/$mycerts/server.pem\""
    set_setting tls.bundles.default.key_path "\"$root/$mycerts/server-key.pem\""
    set_setting listener.mysql.tls '"optional"'
    set_setting listener.mariadb.tls '"optional"'
    set_setting listener.http.tls '"off"'
    set_setting listener.https.tls '"required"'
    set_setting gateway.drain_lb_delay_seconds 2
    set_setting listener.drain_seconds 5
    gateway 1 start
    gateway 2 start
    local cid
    cid=$(ctl -o json cluster list | jq -r '.clusters[] | select(.name == "m7-single") | .cluster_id')
    ctl tenant create m7a --cluster "$cid" >/dev/null 2>&1 || true
    ctl tenant user add m7a app --password sprig-dev-app --native --if-not-exists >/dev/null
    ctl settings set auth.mysql_native_password true --scope tenant:m7a --reason m07 >/dev/null
    say "up: tenant m7a on $cid; log in as m7a.app / sprig-dev-app"
}

gateway() {
    local n=$1 what=$2
    case $what in
        start) start "gateway-$n" "$here/gateway-$n.toml" gateway "$(gw_ready "$n")" ;;
        stop) stop "gateway-$n" ;;
        *) die "gateway $n start|stop" ;;
    esac
}

down() {
    stop gateway-1
    stop gateway-2
    stop controller
    docker compose -f "$here/compose.yaml" down -v
    rm -rf "$state"
}

status() {
    for p in controller gateway-1 gateway-2; do
        if running "$p"; then say "$p running (pid $(pid_of "$p"))"; else say "$p stopped"; fi
    done
    docker compose -f "$here/compose.yaml" ps
}

case ${1:-} in
    up) up ;;
    down) down ;;
    status) status ;;
    gateway) gateway "${2:?n}" "${3:?start|stop}" ;;
    *) die "usage: $0 up|down|status|gateway N start|stop" ;;
esac
