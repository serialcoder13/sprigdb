#!/usr/bin/env bash
# Chapter 24's move test bed (milestone M0.7): two simulated data centers,
# mum1 and fra1, from the three-DC layout (compose.3dc.yaml with
# dc/compose.yaml), and on the host one controller, one worker and one
# gateway per DC, in single-etcd mode (one etcd serves the global tier and
# both DC tiers). Every process dials its DC's proxy (SPRIG_DIAL_MAP), so
# connections between DCs pay netem's delay.
#
#   deploy/compose/dc/run.sh up        # containers, processes, clusters, tenant
#   deploy/compose/dc/run.sh status
#   deploy/compose/dc/run.sh restart <name>   # ctl-mum1, worker-fra1, gw-fra1, ...
#   deploy/compose/dc/run.sh latency   # round trips between the DC subnets
#   deploy/compose/dc/run.sh down      # stop processes, remove containers
#
# Ports (127.0.0.1): gateway mum1 MySQL 22306, MariaDB 22307, HTTP 22318;
# gateway fra1 MySQL 22316, MariaDB 22317, HTTP 22322; controllers' Admin
# API mum1 22326, fra1 22327. Tenant dc-move (homed in mum1 at first),
# user app, password sprig-dev-app.
#
# Variables: CARGO_TARGET_DIR (target), SPRIG_DC_PROFILE (debug),
# SPRIG3DC_DELAY_MUM1_FRA1_MS (30: one way).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
root=$(pwd)
here=deploy/compose/dc
state=$here/.state
profile=${SPRIG_DC_PROFILE:-debug}
bin=${CARGO_TARGET_DIR:-target}/$profile
project=sprigdb-3dc
etcd=http://127.0.0.1:34179
tenant=dc-move
certs=deploy/compose/gateway-certs

say() { echo "dc: $*"; }
die() { echo "dc: $*" >&2; exit 1; }
compose() { docker compose -p "$project" -f deploy/compose/compose.3dc.yaml -f "$here/compose.yaml" "$@"; }
ctl() { local dc=$1; shift; "$bin/sprigctl" --endpoint "http://127.0.0.1:$(admin_port "$dc")" "$@"; }
admin_port() { case $1 in mum1) echo 22326 ;; fra1) echo 22327 ;; esac; }

# Per DC: proxy ports for mum1-db-1, fra1-db-1, etcd.
proxy_base() { case $1 in mum1) echo 3441 ;; fra1) echo 3442 ;; esac; }
dial_map() {
    local b
    b=$(proxy_base "$1")
    echo "10.10.1.21:3306=127.0.0.1:${b}1,10.10.2.21:3306=127.0.0.1:${b}2"
}
seed() { echo "http://127.0.0.1:$(proxy_base "$1")9"; }

etcd_put() {
    curl -fsS -X POST "$etcd/v3/kv/put" \
        -d "$(jq -cn --arg k "$1" --arg v "$2" '{key: ($k|@base64), value: ($v|@base64)}')" >/dev/null
}
set_setting() { # dc key json
    etcd_put "/sprig/dc/$1/settings/$2" "$(jq -cn --argjson v "$3" \
        '{v: 1, value: $v, set_by: "op:dc-test", reason: "deploy/compose/dc/run.sh"}')"
}

configs() {
    mkdir -p "$state"
    echo '*' >"$state/.gitignore"
    for dc in mum1 fra1; do
        local n=0 boot=true
        [[ $dc == fra1 ]] && n=1 && boot=false
        cat >"$state/ctl-$dc.toml" <<EOF
[node]
dc = "$dc"
id = "ctl-$dc"
[meta]
dc_seeds = ["$(seed "$dc")"]
global_seeds = []
deployment_id = "sprigdb-dc-test"
[controller]
bootstrap = $boot
admin_addr = "127.0.0.1:$(admin_port "$dc")"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:$((22328 + n))"
EOF
        cat >"$state/worker-$dc.toml" <<EOF
[node]
dc = "$dc"
id = "worker-$dc"
[meta]
dc_seeds = ["$(seed "$dc")"]
global_seeds = []
deployment_id = "sprigdb-dc-test"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:$((22330 + n))"
EOF
        local my ma ht hs pe te
        if [[ $dc == mum1 ]]; then my=22306 ma=22307 ht=22318 hs=22319 pe=22320 te=22321
        else my=22316 ma=22317 ht=22322 hs=22323 pe=22324 te=22325; fi
        cat >"$state/gw-$dc.toml" <<EOF
[node]
dc = "$dc"
id = "gw-$dc"
[meta]
dc_seeds = ["$(seed "$dc")"]
global_seeds = []
deployment_id = "sprigdb-dc-test"
[gateway]
login_mode = "catalog"
backends_file = "$root/deploy/compose/m07/backends.toml"
advertise_addr = "127.0.0.1"
[gateway.listen_override]
mysql = "127.0.0.1:$my"
mariadb = "127.0.0.1:$ma"
http = "127.0.0.1:$ht"
https = "127.0.0.1:$hs"
peer = "127.0.0.1:$pe"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:$te"
EOF
    done
}

pid_of() { cat "$state/$1.pid" 2>/dev/null || true; }
running() {
    local pid
    pid=$(pid_of "$1")
    [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null && ps -p "$pid" -o command= | grep -q sprigdb
}

ready_url() {
    case $1 in
        ctl-mum1) echo 22328 ;; ctl-fra1) echo 22329 ;;
        worker-mum1) echo 22330 ;; worker-fra1) echo 22331 ;;
        gw-mum1) echo 22321 ;; gw-fra1) echo 22325 ;;
    esac
}

start() { # name
    local name=$1 dc=${1##*-} role
    case $name in ctl-*) role=controller ;; worker-*) role=worker ;; gw-*) role=gateway ;; esac
    if running "$name"; then say "$name already running"; return; fi
    [[ -f $state/$name.log ]] && mv -f "$state/$name.log" "$state/$name.log.1"
    SPRIG_DIAL_MAP=$(dial_map "$dc") RUST_LOG=${RUST_LOG:-info} \
        nohup "$bin/sprigdb" --config "$state/$name.toml" "$role" >"$state/$name.log" 2>&1 </dev/null &
    echo $! >"$state/$name.pid"
    for _ in $(seq 1 240); do
        curl -fsS -o /dev/null "http://127.0.0.1:$(ready_url "$name")/readyz" 2>/dev/null && {
            say "$name ready (pid $(pid_of "$name"))"; return; }
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
    for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid"
    rm -f "$state/$name.pid"
    say "$name stopped"
}

wait_leader() {
    for _ in $(seq 1 60); do
        ctl "$1" -o json dc show "$1" 2>/dev/null | jq -e '.leader != null' >/dev/null && return
        sleep 0.5
    done
    die "no leader in $1"
}

register_cluster() { # dc name topology
    if ! ctl "$1" -o json cluster list | jq -e --arg n "$2" '.clusters[] | select(.name == $n)' >/dev/null; then
        ctl "$1" cluster register "$2" --topology "$3" --node "$4" \
            --credentials "file:$root/$state/creds.toml" >/dev/null
        say "cluster $2 registered in $1"
    fi
}

up() {
    [[ -x $bin/sprigdb && -x $bin/sprigctl ]] || die "no $bin/sprigdb: cargo build -p sprigdb -p sprigctl first"
    [[ -s $certs/server.pem ]] || die "no dev certificates in $certs"
    configs
    compose up -d --wait netem mum1-etcd mum1-db-1 fra1-db-1 proxy-mum1 proxy-fra1
    (umask 077 && printf '[sprig_ctl]\nuser = "root"\npassword = "sprig-dev-root"\ntls = "skip_verify"\n[sprig_repl]\nuser = "root"\npassword = "sprig-dev-root"\ntls = "skip_verify"\n' >"$state/creds.toml")
    for dc in mum1 fra1; do
        set_setting "$dc" tls.bundles.default.cert_path "\"$root/$certs/server.pem\""
        set_setting "$dc" tls.bundles.default.key_path "\"$root/$certs/server-key.pem\""
        set_setting "$dc" listener.mysql.tls '"optional"'
        set_setting "$dc" listener.mariadb.tls '"optional"'
        set_setting "$dc" listener.http.tls '"off"'
        set_setting "$dc" listener.https.tls '"required"'
    done
    start ctl-mum1
    wait_leader mum1
    ctl mum1 dc register fra1 --tags jurisdiction=EU >/dev/null 2>&1 || true
    start ctl-fra1
    wait_leader fra1
    register_cluster mum1 mum1-single single 10.10.1.21:3306
    register_cluster fra1 fra1-galera galera 10.10.2.21:3306
    start worker-mum1
    start worker-fra1
    start gw-mum1
    start gw-fra1
    local cid
    cid=$(ctl mum1 -o json cluster list | jq -r '.clusters[] | select(.name == "mum1-single") | .cluster_id')
    ctl mum1 tenant create "$tenant" --cluster "$cid" >/dev/null 2>&1 || true
    ctl mum1 tenant user add "$tenant" app --password sprig-dev-app --native --if-not-exists >/dev/null
    ctl mum1 settings set auth.mysql_native_password true --scope "tenant:$tenant" --reason dc-test >/dev/null
    say "up: tenant $tenant on $cid; mysql -h127.0.0.1 -P22307 -u $tenant.app -psprig-dev-app"
}

down() {
    for p in gw-fra1 gw-mum1 worker-fra1 worker-mum1 ctl-fra1 ctl-mum1; do stop "$p"; done
    compose down -v
    rm -rf "$state"
}

status() {
    for p in ctl-mum1 ctl-fra1 worker-mum1 worker-fra1 gw-mum1 gw-fra1; do
        if running "$p"; then say "$p running (pid $(pid_of "$p"))"; else say "$p stopped"; fi
    done
    compose ps
}

latency() {
    for spec in "mum1 10.10.1.250 10.10.1.21" "mum1 10.10.1.250 10.10.2.21" "fra1 10.10.2.250 10.10.1.21"; do
        # shellcheck disable=SC2086
        set -- $spec
        printf '%s -> %s: ' "$1" "$3"
        docker run --rm --network "${project}_wan" --ip "$2" --entrypoint sh sprigdb-netem:dev \
            -c "sleep 2 && ping -c 5 -q $3" | awk -F'/' '/min\/avg\/max/ { print $4 " ms avg" }'
    done
}

case ${1:-} in
    up) up ;;
    down) down ;;
    status) status ;;
    latency) latency ;;
    restart) stop "${2:?name}"; start "$2" ;;
    stop) stop "${2:?name}" ;;
    start) start "${2:?name}" ;;
    *) die "usage: $0 up|down|status|latency|restart <name>" ;;
esac
