#!/usr/bin/env bash
# The balancer's live test bed (milestone M0.6, chapter 11): "the balancer
# evens out a deliberately skewed fleet without oscillating".
#
# A deployment of its own (sprigdb-bl, DC dc-bl) on its own etcd container
# (sprigdb-dev-bl-etcd, 127.0.0.1:18379, 256 MB), so its balancer only ever
# sees the bl- tenants, over the Compose stack's three MariaDB clusters
# (single, primary + replicas, Galera), registered with 1 GB of declared
# storage each. Tenant ids start at 970000001, far above the shared dev
# deployment's, so schema and account names (t<id>_*) never collide; the
# ids this bed made are kept in .state/tenant_ids, and down sweeps only
# those.
#
#   deploy/compose/balancer/run.sh up     # etcd, controller, worker, gateway, tenants
#   deploy/compose/balancer/run.sh down   # drop the tenants, stop, remove etcd
#   deploy/compose/balancer/run.sh ctl …  # sprigctl against this deployment
#
# Then: SPRIG_TEST_BALANCER_LIVE=1 SPRIGCTL=$CARGO_TARGET_DIR/debug/sprigctl \
#   cargo test -p sprig-controller --test live_balance -- --nocapture
#
# Listeners: MySQL persona 18306, MariaDB persona 18307 (TLS optional);
# Admin API 18480. Tenants bl-01 … bl-10 (8 on dc-bl-c1, one each on c2
# and c3), each with the user app (password sprig-dev-app). Logs and pids
# in deploy/compose/balancer/.state/.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
root=$(pwd)
here=deploy/compose/balancer
state=$here/.state
mkdir -p "$state"
printf '*\n' >"$state/.gitignore"
bin=${CARGO_TARGET_DIR:-target}/debug
etcd=http://127.0.0.1:18379
admin=http://127.0.0.1:18480
dc=dc-bl
container=sprigdb-dev-bl-etcd
pw=sprig-dev-app
first_tenant_id=990000001
# name|topology|ports
clusters=(
    "bl-single|single|13301"
    "bl-repl|primary_replica|13311,13312,13313"
    "bl-galera|galera|13321,13322,13323"
)
# tenant|cluster: most of them on one cluster.
tenants=(
    bl-01:dc-bl-c1 bl-02:dc-bl-c1 bl-03:dc-bl-c1 bl-04:dc-bl-c1
    bl-05:dc-bl-c1 bl-06:dc-bl-c1 bl-07:dc-bl-c1 bl-08:dc-bl-c1
    bl-09:dc-bl-c2 bl-10:dc-bl-c3
)

say() { echo "balancer-bed: $*"; }
die() {
    echo "balancer-bed: $*" >&2
    exit 1
}
ctl() { "$bin/sprigctl" --endpoint "$admin" "$@"; }
b64() { printf '%s' "$1" | base64 | tr -d '\n'; }
etcd_get() {
    curl -fsS -X POST "$etcd/v3/kv/range" -d "$(jq -cn --arg k "$(b64 "$1")" '{key: $k}')" |
        jq -r '.kvs[0].value // empty | @base64d'
}
etcd_put() {
    curl -fsS -X POST "$etcd/v3/kv/put" \
        -d "$(jq -cn --arg k "$(b64 "$1")" --arg v "$(b64 "$2")" '{key: $k, value: $v}')" >/dev/null
}
setting() { # key json-value [scope]
    local key=/sprig/dc/$dc/settings/$1
    if [[ ${3:-dc} == global ]]; then key=/sprig/g/settings/$1; fi
    etcd_put "$key" "$(jq -cn --argjson v "$2" \
        '{v: 1, value: $v, set_by: "op:balancer-bed", reason: "deploy/compose/balancer/run.sh"}')"
}

running() {
    local pid
    pid=$(cat "$state/$1.pid" 2>/dev/null || true)
    [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null && ps -p "$pid" -o command= | grep -q sprigdb
}

start() { # name config role telemetry-port [node id]
    local name=$1 config=$2 role=$3 port=$4 id=${5:-}
    if running "$name"; then return; fi
    local extra=()
    if [[ -n $id ]]; then extra=(SPRIGDB_NODE_ID="$id"); fi
    env ${extra[@]+"${extra[@]}"} SPRIGDB_TELEMETRY_BOOTSTRAP_ADDR="127.0.0.1:$port" \
        nohup "$bin/sprigdb" --config "$config" "$role" >>"$state/$name.log" 2>&1 </dev/null &
    echo $! >"$state/$name.pid"
    for _ in $(seq 1 120); do
        curl -fsS -o /dev/null "http://127.0.0.1:$port/readyz" 2>/dev/null && {
            say "$name ready (pid $(cat "$state/$name.pid"))"
            return
        }
        running "$name" || {
            tail -n 30 "$state/$name.log" >&2
            die "$name exited"
        }
        sleep 0.5
    done
    die "$name not ready; see $state/$name.log"
}

stop() {
    local name=$1 pid
    running "$name" || {
        rm -f "$state/$name.pid"
        return 0
    }
    pid=$(cat "$state/$name.pid")
    kill -TERM "$pid"
    for _ in $(seq 1 80); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
    done
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid"
    rm -f "$state/$name.pid"
    say "$name stopped"
}

up() {
    if ! docker ps --format '{{.Names}}' | grep -qx "$container"; then
        docker rm -f "$container" >/dev/null 2>&1 || true
        docker run -d --name "$container" --memory 256m -p 127.0.0.1:18379:2379 \
            quay.io/coreos/etcd:v3.6.5 etcd --name=bl --data-dir=/tmp/etcd-data \
            --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://127.0.0.1:18379 \
            >/dev/null
        for _ in $(seq 1 60); do
            curl -fsS -o /dev/null "$etcd/version" 2>/dev/null && break
            sleep 0.5
        done
        say "etcd $container up on 18379"
    fi
    [[ -n ${SPRIG_BL_NO_BUILD:-} ]] || cargo build -q -p sprigdb -p sprigctl
    start controller "$here/controller.toml" controller 18180
    for _ in $(seq 1 60); do
        ctl -o json dc show "$dc" 2>/dev/null | jq -e '.leader != null' >/dev/null && break
        sleep 0.5
    done
    # Tenant ids far above the shared deployment's.
    local next
    next=$(etcd_get /sprig/g/ids/tenant | jq -r '.next // 0')
    if ((${next:-0} < first_tenant_id)); then
        etcd_put /sprig/g/ids/tenant "{\"next\":$first_tenant_id,\"v\":1}"
    fi
    (umask 077 && cat >"$state/cluster-credentials.toml") <<'TOML'
# Written by deploy/compose/balancer/run.sh: compose.yaml's MariaDB root, development only.
[sprig_ctl]
user = "root"
password = "sprig-dev-root"
tls = "skip_verify"
[sprig_repl]
user = "root"
password = "sprig-dev-root"
tls = "skip_verify"
TOML
    local list c name topology ports args
    list=$(ctl -o json cluster list)
    for c in "${clusters[@]}"; do
        IFS='|' read -r name topology ports <<<"$c"
        if jq -e --arg n "$name" '.clusters[] | select(.name == $n)' <<<"$list" >/dev/null; then continue; fi
        args=(cluster register "$name" --topology "$topology" --storage-gb 1
            --credentials "file:$root/$state/cluster-credentials.toml")
        IFS=',' read -ra port_list <<<"$ports"
        for p in "${port_list[@]}"; do args+=(--node "127.0.0.1:$p"); done
        ctl -o json "${args[@]}" | jq -r '"registered \(.cluster.cluster_id) \(.cluster.name) \(.cluster.admin_state)"'
    done
    # Listeners and the gateway's workload statistics.
    setting listener.mysql.tls '"optional"'
    setting listener.mariadb.tls '"optional"'
    setting tls.bundles.default.cert_path "\"$root/deploy/compose/gateway-certs/server.pem\""
    setting tls.bundles.default.key_path "\"$root/deploy/compose/gateway-certs/server-key.pem\""
    setting workload.flush_seconds 10
    setting workload.bucket_minutes 1 global
    # The balancer: recommend only until the test switches it to auto.
    setting balancer.mode '"recommend"'
    # Automatic indexing off: this bed tests placement, and in the first run
    # its loop held the controller's connections to dc-bl-c1.
    setting auto_index.mode '"off"'
    start worker "$here/controller.toml" worker 18185 bl-wkr-1
    start gateway "$here/gateway.toml" gateway 18190
    local t cl
    for tc in "${tenants[@]}"; do
        t=${tc%%:*} cl=${tc#*:}
        ctl tenant show "$t" >/dev/null 2>&1 || ctl tenant create "$t" --cluster "$cl" >/dev/null
        ctl tenant user add "$t" app --password "$pw" --native --if-not-exists >/dev/null
        ctl settings set auth.mysql_native_password true --scope "tenant:$t" >/dev/null || true
        local id
        id=$(ctl -o json tenant show "$t" | jq -r .tenant_id)
        grep -qx "$id" "$state/tenant_ids" 2>/dev/null || echo "$id" >>"$state/tenant_ids"
        say "tenant $t ($id) on $cl"
    done
    for _ in $(seq 60); do
        nc -z 127.0.0.1 18306 2>/dev/null && break
        sleep 1
    done
    say "up: mysql -h127.0.0.1 -P18306 -u bl-01.app -p$pw"
}

# Leftover t<id>_* schemas and t<id>_u* accounts of this bed's tenant ids
# (and no one else's), on every node that takes writes.
sweep() {
    local port q id
    [[ -s $state/tenant_ids ]] || return 0
    local ids
    # shellcheck disable=SC2207 # one number per line
    ids=($(cat "$state/tenant_ids"))
    for port in 13301 13311 13321; do
        for id in "${ids[@]}"; do
            [[ $id =~ ^[0-9]+$ ]] || continue
            q="SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 't${id}\\_%'"
            for s in $(mysql -h127.0.0.1 -P$port -uroot -psprig-dev-root -N -B -e "$q" 2>/dev/null); do
                mysql -h127.0.0.1 -P$port -uroot -psprig-dev-root -e "DROP DATABASE \`$s\`" 2>/dev/null &&
                    say "dropped $s on :$port"
            done
            q="SELECT CONCAT(QUOTE(user),'@',QUOTE(host)) FROM mysql.user WHERE user LIKE 't${id}\\_u%'"
            for u in $(mysql -h127.0.0.1 -P$port -uroot -psprig-dev-root -N -B -e "$q" 2>/dev/null); do
                mysql -h127.0.0.1 -P$port -uroot -psprig-dev-root -e "DROP USER $u" 2>/dev/null &&
                    say "dropped user $u on :$port"
            done
        done
    done
}

# The mover's rows of this bed's jobs in __sprig_sys (chapter 10's
# watermark, position and XA tables), on every writer.
forget_jobs() {
    local ids port tbl
    ids=$(ctl -o json job list 2>/dev/null | jq -r '[.jobs[].id | gsub("-"; "") | "UNHEX(\"" + . + "\")"] | join(",")')
    [[ -n $ids ]] || return 0
    for port in 13301 13311 13321; do
        for tbl in __sprig_watermark __sprig_mover_pos __sprig_mover_xa; do
            mysql -h127.0.0.1 -P$port -uroot -psprig-dev-root -N -B \
                -e "DELETE FROM __sprig_sys.$tbl WHERE job_id IN ($ids); SELECT ROW_COUNT()" 2>/dev/null |
                sed "s/^/balancer-bed: :$port $tbl rows deleted: /" || true
        done
    done
}

down() {
    if running controller; then
        forget_jobs
        for tc in "${tenants[@]}"; do
            t=${tc%%:*}
            ctl tenant show "$t" >/dev/null 2>&1 || continue
            local dbs
            dbs=$(mysql -h127.0.0.1 -P18306 -u "$t.app" -p$pw --ssl-mode=REQUIRED -N -B \
                -e "SHOW DATABASES" 2>/dev/null || true)
            for db in $dbs; do
                case $db in information_schema | performance_schema | mysql | sys) continue ;; esac
                mysql -h127.0.0.1 -P18306 -u "$t.app" -p$pw --ssl-mode=REQUIRED \
                    -e "DROP DATABASE \`$db\`" 2>/dev/null || say "couldn't drop $t's $db"
            done
            ctl tenant delete "$t" --yes >/dev/null 2>&1 || say "tenant $t not deleted"
        done
        sleep 5
    fi
    stop gateway
    stop worker
    stop controller
    sweep
    docker rm -f "$container" >/dev/null 2>&1 && say "etcd $container removed (with every job record)"
    rm -f "$state/tenant_ids"
}

case ${1:-} in
up) up ;;
down) down ;;
sweep) sweep ;;
ctl)
    shift
    ctl "$@"
    ;;
*) die "usage: $0 up|down|sweep|ctl …" ;;
esac
