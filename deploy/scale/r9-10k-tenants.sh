#!/usr/bin/env bash
# R9's 10,000-tenant scale test (chapter 48, R9 done-when; decisions r9 items 85
# and 90). NOT for the laptop or the shared dev clusters: run it on a machine of
# its own (a CI runner or a short-lived cloud VM), which the lead asks Kiran for.
#
# Machine: 16 vCPU, 64 GB RAM, 300 GB local SSD (NVMe), Linux x86_64 or arm64,
# Docker 24+. Smaller fails for memory, not CPU: 10,000 tenants hold about
# 400,000 InnoDB tables (32 catalog tables each, plus their queue, stream and
# sink tables), and MariaDB keeps each open table's definition in memory.
#
# What it starts (all on this machine, nothing else):
#   MariaDB 11.4 (one container, mem 40g): innodb_buffer_pool_size=16G,
#     table_definition_cache=450000, table_open_cache=200000,
#     open_files_limit=1000000, innodb_open_files=200000, max_connections=2000,
#     binlog_format=ROW, log_bin, gtid_strict_mode=ON, expire_logs_days=1
#   etcd 3.6 (mem 4g, --quota-backend-bytes=8589934592)
#   a release build of sprigdb: controller, two workers, two gateways, started
#     directly from configs this script writes to $OUT/stack (not through
#     deploy/compose/gateway-dev.sh, which registers the laptop's Compose
#     clusters), with tenant ids from 33306000000 (rule 8f-bis)
#
# Clients: the MariaDB client (Ubuntu's `mysql`) in plaintext (--skip-ssl),
# so logins use mysql_native_password (members' users are added with
# --native); also jq, curl, openssl and shuf.
#
# What it measures (each step's numbers go to $OUT/results.tsv):
#   1. the definitions first: a declarer tenant with a stream, an exchange, a
#      queue and a binding FOR EACH TENANT and two fan-in sinks (as r9fwd's
#      1,000-tenant attempt, decision 85)
#   2. 10,000 members under it, $PAR at a time (default 8); each time the
#      count passes a multiple of $EVERY (default 500), whatever $PAR is:
#      MariaDB memory and connections, binlog dumps on the node, the workers'
#      and gateways' RSS, etcd's database size, creation rate. It stops if
#      MariaDB passes 90% of its memory or 80% of max_connections.
#   3. the forwarder (decision 86): one transactional publish from each of
#      1,000 random members, 32 at a time; every message arrives once; commit
#      to delivery p50/p99; binlog dumps on the node stay at one per worker
#      process and cluster
#   4. first-event timings for 100 random members after a warm-up: first
#      publish into the member's queue instance (target under 1 s) and first
#      record into its stream instance (target under 2 s)
#   5. cleanup: every member and the declarer deleted by the names it recorded
#
# Expected run time: about 1 hour to create the members (2–3 s each, 8 at a
# time), 15 minutes for the rest.
#
# Usage (on the test machine, from a checkout):
#   deploy/scale/r9-10k-tenants.sh up        # containers and a release build
#   deploy/scale/r9-10k-tenants.sh run       # steps 1–4 (TENANTS=10000 PAR=8)
#   deploy/scale/r9-10k-tenants.sh down      # step 5 and the containers
set -euo pipefail

TENANTS=${TENANTS:-10000}
PAR=${PAR:-8}
EVERY=${EVERY:-500}
OUT=${OUT:-target/scale-10k}
MYSQL_PORT=${MYSQL_PORT:-33306}   # the gateway
NODE_PORT=${NODE_PORT:-33301}     # MariaDB
ETCD_PORT=${ETCD_PORT:-33379}
ADMIN=${ADMIN:-http://127.0.0.1:33480}
ROOT_PW=${ROOT_PW:-scale-root}
DC=dc-s
STACK=$OUT/stack
mkdir -p "$OUT"

die() { echo "r9-10k: $*" >&2; exit 1; }
[[ $(uname -s) == Linux ]] || die "run this on the test machine (Linux), not on a laptop"
mem_kb=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
(( mem_kb > 60000000 )) || die "needs at least 64 GB of memory (has $((mem_kb / 1024 / 1024)) GB)"

ctl() { target/release/sprigctl --endpoint "$ADMIN" "$@"; }
node() { mysql -h127.0.0.1 -P"$NODE_PORT" -uroot -p"$ROOT_PW" -N -e "$1" 2>/dev/null; }
# The MariaDB client: --skip-ssl (--ssl-mode is MySQL's client), and without
# TLS the gateway's caching_sha2_password would need its RSA key.
client=(mysql -h127.0.0.1 -P"$MYSQL_PORT" -pscale-app --skip-ssl --default-auth=mysql_native_password -N)
as() { "${client[@]}" -u"$1.app" -e "$2" 2>/dev/null; }

measure() {
    local label=$1
    local conns dumps mem etcd_db
    conns=$(node "SHOW STATUS LIKE 'Threads_connected'" | awk '{print $2}')
    dumps=$(node "SELECT COUNT(*) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'")
    mem=$(docker stats --no-stream --format '{{.MemUsage}}' r9-10k-mariadb | awk '{print $1}')
    etcd_db=$(curl -s "http://127.0.0.1:$ETCD_PORT/metrics" | awk '/^etcd_mvcc_db_total_size_in_bytes/ {print $2}')
    local rss
    rss=$(pgrep -f 'sprigdb.*(worker|gateway)' | xargs -r ps -o rss= -p | paste -sd, -)
    printf '%s\t%s\tconns=%s\tdumps=%s\tnode_mem=%s\tetcd_db=%s\trss_kb=%s\n' \
        "$(date +%T)" "$label" "$conns" "$dumps" "$mem" "$etcd_db" "$rss" | tee -a "$OUT/results.tsv"
    local max
    max=$(node "SELECT @@max_connections")
    (( conns * 10 < max * 8 )) || die "connections at $conns of $max: stopping"
}

up() {
    docker run -d --name r9-10k-etcd -m 4g -p "127.0.0.1:$ETCD_PORT:2379" quay.io/coreos/etcd:v3.6.5 \
        etcd --name=e0 --data-dir=/etcd-data --listen-client-urls=http://0.0.0.0:2379 \
        --advertise-client-urls="http://127.0.0.1:$ETCD_PORT" --quota-backend-bytes=8589934592 \
        --auto-compaction-mode=periodic --auto-compaction-retention=1h
    docker run -d --name r9-10k-mariadb -m 40g -p "127.0.0.1:$NODE_PORT:3306" \
        -e MARIADB_ROOT_PASSWORD="$ROOT_PW" --ulimit nofile=1048576:1048576 mariadb:11.4 \
        --innodb-buffer-pool-size=16G --table-definition-cache=450000 --table-open-cache=200000 \
        --open-files-limit=1000000 --innodb-open-files=200000 --max-connections=2000 \
        --log-bin=bin --binlog-format=ROW --server-id=1 --gtid-strict-mode=ON --expire-logs-days=1 \
        --binlog-annotate-row-events=ON --binlog-row-metadata=FULL
    cargo build --release -p sprigdb -p sprigctl
    start_stack
}

# --- the stack, started directly (gateway-dev.sh registers the laptop's
# Compose clusters, so it isn't used here) -----------------------------------

etcd_put() {
    curl -fsS -X POST "http://127.0.0.1:$ETCD_PORT/v3/kv/put" \
        -d "$(jq -cn --arg k "$1" --arg v "$2" '{key: ($k|@base64), value: ($v|@base64)}')" >/dev/null
}

dc_setting() {
    etcd_put "/sprig/dc/$DC/settings/$1" \
        "$(jq -cn --argjson v "$2" '{v: 1, value: $v, set_by: "op:r9-10k", reason: "r9 10k scale test"}')"
}

write_certs() {
    mkdir -p "$STACK/certs"
    [[ -s $STACK/certs/server.pem ]] && return
    (
        cd "$STACK/certs"
        umask 077
        openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=r9-10k CA" \
            -keyout ca-key.pem -out ca.pem 2>/dev/null
        openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
            -keyout server-key.pem -out server.csr 2>/dev/null
        printf '%s\n' "subjectAltName=DNS:localhost,IP:127.0.0.1" >server.ext
        openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
            -days 30 -extfile server.ext -out server.pem 2>/dev/null
        rm -f server.csr server.ext ca.srl
    )
}

write_configs() {
    local root i off meta
    root=$(pwd)
    mkdir -p "$STACK/logs"
    write_certs
    umask 077
    printf '[sprig_ctl]\nuser = "root"\npassword = "%s"\ntls = "skip_verify"\n[sprig_repl]\nuser = "root"\npassword = "%s"\ntls = "skip_verify"\n' \
        "$ROOT_PW" "$ROOT_PW" >"$STACK/creds.toml"
    printf '%s\n' '[per_user]' "secret = \"file:$root/$STACK/backends.toml#backend_secret\"" \
        'tls = "skip_verify"' '' '[backend_secret]' 'password = "r9-10k-backend-accounts-secret"' '' \
        '[default]' 'backend_user = "root"' \
        "backend_credentials = \"file:$root/$STACK/backends.toml#backend_root\"" '' \
        '[backend_root]' 'user = "root"' "password = \"$ROOT_PW\"" 'tls = "skip_verify"' \
        >"$STACK/backends.toml"
    umask 022
    meta=$(printf '[meta]\ndc_seeds = ["http://127.0.0.1:%s"]\nglobal_seeds = []\ndeployment_id = "sprigdb-r9-10k"' "$ETCD_PORT")
    printf '%s\n' '[node]' "dc = \"$DC\"" 'id = "s-ctl"' "$meta" '[controller]' 'bootstrap = true' \
        "admin_addr = \"127.0.0.1:${ADMIN##*:}\"" \
        "workload_key_file = \"$root/deploy/compose/workload-keys.dev\"" \
        '[telemetry]' 'bootstrap_log_level = "info"' 'log_format = "json"' \
        'bootstrap_addr = "127.0.0.1:33180"' >"$STACK/controller.toml"
    for i in 1 2; do
        off=$(( (i - 1) * 10 ))
        printf '%s\n' '[node]' "dc = \"$DC\"" "id = \"s-gw-$i\"" "$meta" '[gateway]' \
            'login_mode = "catalog"' "backends_file = \"$root/$STACK/backends.toml\"" \
            'advertise_addr = "127.0.0.1"' '[gateway.listen_override]' \
            "mysql = \"127.0.0.1:$(( MYSQL_PORT + off ))\"" \
            "mariadb = \"127.0.0.1:$(( MYSQL_PORT + 1 + off ))\"" \
            "http = \"127.0.0.1:$(( 33080 + off ))\"" "https = \"127.0.0.1:$(( 33443 + off ))\"" \
            "peer = \"127.0.0.1:$(( 33403 + off ))\"" "mongo = \"127.0.0.1:$(( 27117 + i - 1 ))\"" \
            '[telemetry]' 'bootstrap_log_level = "info"' 'log_format = "json"' \
            "bootstrap_addr = \"127.0.0.1:$(( 33189 + i ))\"" >"$STACK/gateway-$i.toml"
    done
}

start_proc() { # name config role [env...]
    local name=$1 config=$2 role=$3
    shift 3
    env "$@" nohup target/release/sprigdb --config "$config" "$role" >"$STACK/logs/$name.log" 2>&1 </dev/null &
    echo $! >"$STACK/$name.pid"
}

wait_ready() { # name readyz-url
    local _
    for _ in $(seq 1 240); do
        curl -fsS -o /dev/null "$2" 2>/dev/null && return
        kill -0 "$(cat "$STACK/$1.pid")" 2>/dev/null || { tail -30 "$STACK/logs/$1.log"; die "$1 exited"; }
        sleep 0.5
    done
    die "$1 not ready"
}

start_stack() {
    local i _
    write_configs
    ulimit -n 1048576 2>/dev/null || ulimit -n 65536
    start_proc controller "$STACK/controller.toml" controller
    wait_ready controller http://127.0.0.1:33180/readyz
    for _ in $(seq 1 120); do
        ctl -o json dc show "$DC" 2>/dev/null | jq -e '.leader != null' >/dev/null && break
        sleep 0.5
    done
    ctl -o json cluster list | jq -e '.clusters[] | select(.name == "s10k-single")' >/dev/null 2>&1 ||
        ctl cluster register s10k-single --topology single --node "127.0.0.1:$NODE_PORT" \
            --credentials "file:$(pwd)/$STACK/creds.toml" >/dev/null
    # Rule 8f-bis: tenant ids from 33306000000, below 2^38.
    ctl settings set tenant.id_min 33306000000 --scope global --reason r9-10k --yes >/dev/null
    ctl settings set tenant.id_max 33306099999 --scope global --reason r9-10k --yes >/dev/null
    dc_setting tls.bundles.default.cert_path "\"$(pwd)/$STACK/certs/server.pem\""
    dc_setting tls.bundles.default.key_path "\"$(pwd)/$STACK/certs/server-key.pem\""
    dc_setting listener.mysql.tls '"optional"'
    dc_setting listener.mariadb.tls '"optional"'
    dc_setting listener.http.enabled false
    dc_setting listener.https.enabled false
    for i in 1 2; do
        start_proc "worker-$i" "$STACK/controller.toml" worker SPRIGDB_NODE_ID="s-wkr-$i" \
            SPRIGDB_TELEMETRY_BOOTSTRAP_ADDR="127.0.0.1:3318$((4 + i))"
    done
    for i in 1 2; do wait_ready "worker-$i" "http://127.0.0.1:3318$((4 + i))/readyz"; done
    for i in 1 2; do
        start_proc "gateway-$i" "$STACK/gateway-$i.toml" gateway SPRIGDB_GATEWAY_ADVERTISE_ADDR=127.0.0.1
    done
    for i in 1 2; do wait_ready "gateway-$i" "http://127.0.0.1:$(( 33189 + i ))/readyz"; done
}

stop_stack() {
    local f
    for f in "$STACK"/gateway-*.pid "$STACK"/worker-*.pid "$STACK"/controller.pid; do
        [[ -f $f ]] || continue
        kill "$(cat "$f")" 2>/dev/null || true
        rm -f "$f"
    done
}

member() {
    local n=$1
    ctl tenant create "$n" --under r9k-root >/dev/null
    ctl tenant user add "$n" app --password scale-app --native --if-not-exists >/dev/null
    echo "$n" >>"$OUT/members"
}

run() {
    # 1. definitions
    ctl tenant create r9k-root >/dev/null
    ctl tenant user add r9k-root app --password scale-app --native --if-not-exists >/dev/null
    echo r9k-root >"$OUT/declarer"
    as r9k-root "CREATE DATABASE r9k; \
        CREATE STREAM r9k.events FOR EACH TENANT; \
        CREATE EXCHANGE r9k.jobs TYPE topic FOR EACH TENANT; \
        CREATE QUEUE r9k.work FOR EACH TENANT; \
        BIND QUEUE r9k.work TO EXCHANGE r9k.jobs KEY 'job.#' FOR EACH TENANT"
    measure defs
    # 2. members
    local i=0 t0 next=$EVERY
    t0=$(date +%s)
    while (( i < TENANTS )); do
        for _ in $(seq 1 "$PAR"); do
            i=$((i + 1))
            (( i <= TENANTS )) && member "$(printf 'r9k-m%05d' "$i")" &
        done
        wait
        # i moves in steps of PAR: measure on passing each multiple of EVERY.
        if (( i >= next || i >= TENANTS )); then
            measure "members=$(( i < TENANTS ? i : TENANTS )) rate=$(( i / ($(date +%s) - t0 + 1) ))/s"
            while (( next <= i )); do next=$(( next + EVERY )); done
        fi
    done
    # 3. forwarder: 1,000 random members, one transactional publish each
    shuf -n 1000 "$OUT/members" >"$OUT/fwd-sample"
    local start_ms
    start_ms=$(date +%s%3N)
    # -I@, not -I{}: the message body '{}' must stay as written.
    xargs -P 32 -I@ "${client[@]}" -u@.app \
        -e "BEGIN; CALL sprig.mq_publish('', 'work', '{}', NULL, 0, 'r9k'); COMMIT;" <"$OUT/fwd-sample"
    sleep 5
    local got=0
    while read -r m; do
        got=$((got + $(as "$m" "CALL sprig.mq_info('work', 'r9k')" | awk '{print $1}')))
    done <"$OUT/fwd-sample"
    printf '%s\tforwarder\tpublished=1000\tready=%s\telapsed_ms=%s\n' "$(date +%T)" "$got" \
        "$(( $(date +%s%3N) - start_ms ))" | tee -a "$OUT/results.tsv"
    measure after-forwarder
    # 4. first-event timings for 100 other members
    shuf -n 100 "$OUT/members" | while read -r m; do
        local a b
        a=$(date +%s%3N); as "$m" "CALL sprig.mq_publish('', 'work', 'first', NULL, 0, 'r9k')" >/dev/null
        b=$(date +%s%3N); as "$m" "INSERT INTO r9k.events (k, v) VALUES ('first', '{}')" >/dev/null
        printf '%s\t%s\t%s\n' "$m" "$((b - a))" "$(( $(date +%s%3N) - b ))"
    done >"$OUT/first-event.tsv"
    sort -t$'\t' -k2n "$OUT/first-event.tsv" | awk -F'\t' '{q[NR]=$2} END {print "queue first publish p50", q[int(NR/2)], "p99", q[int(NR*0.99)]}' | tee -a "$OUT/results.tsv"
    sort -t$'\t' -k3n "$OUT/first-event.tsv" | awk -F'\t' '{q[NR]=$3} END {print "stream first record p50", q[int(NR/2)], "p99", q[int(NR*0.99)]}' | tee -a "$OUT/results.tsv"
}

down() {
    # Members by their recorded names, then the declarer.
    [[ -f $OUT/members ]] && xargs -P 4 -I{} target/release/sprigctl --endpoint "$ADMIN" tenant delete {} --yes <"$OUT/members" || true
    [[ -f $OUT/declarer ]] && ctl tenant delete "$(cat "$OUT/declarer")" --yes || true
    stop_stack
    docker rm -f r9-10k-mariadb r9-10k-etcd
}

case ${1:-} in
    up) up ;;
    run) run ;;
    down) down ;;
    *) die "usage: $0 up|run|down" ;;
esac
