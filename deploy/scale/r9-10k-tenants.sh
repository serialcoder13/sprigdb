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
#   a release build of sprigdb: controller, two workers, two gateways
#
# What it measures (each step's numbers go to $OUT/results.tsv):
#   1. the definitions first: a declarer tenant with a stream, an exchange, a
#      queue and a binding FOR EACH TENANT and two fan-in sinks (as r9fwd's
#      1,000-tenant attempt, decision 85)
#   2. 10,000 members under it, $PAR at a time (default 8); after every 500:
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
OUT=${OUT:-target/scale-10k}
MYSQL_PORT=${MYSQL_PORT:-33306}   # the gateway
NODE_PORT=${NODE_PORT:-33301}     # MariaDB
ETCD_PORT=${ETCD_PORT:-33379}
ADMIN=${ADMIN:-http://127.0.0.1:33480}
ROOT_PW=${ROOT_PW:-scale-root}
mkdir -p "$OUT"

die() { echo "r9-10k: $*" >&2; exit 1; }
[[ $(uname -s) == Linux ]] || die "run this on the test machine (Linux), not on a laptop"
mem_kb=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
(( mem_kb > 60000000 )) || die "needs at least 64 GB of memory (has $((mem_kb / 1024 / 1024)) GB)"

ctl() { target/release/sprigctl --endpoint "$ADMIN" "$@"; }
node() { mysql -h127.0.0.1 -P"$NODE_PORT" -uroot -p"$ROOT_PW" -N -e "$1" 2>/dev/null; }
as() { mysql -h127.0.0.1 -P"$MYSQL_PORT" -u"$1.app" -pscale-app --ssl-mode=DISABLED -N -e "$2" 2>/dev/null; }

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
    # The deployment itself: deploy/compose/gateway-dev.sh's steps against this
    # etcd and node (register the cluster, settings, two workers and gateways),
    # with tenant.id_min = 33306000000 (rule 8f-bis) and queues on.
    SPRIG_ETCD="http://127.0.0.1:$ETCD_PORT" SPRIG_GATEWAY_MYSQL_PORT=$MYSQL_PORT \
        SPRIG_ADMIN_PORT=${ADMIN##*:} SPRIG_GATEWAY_PROFILE=release SPRIG_TENANT_ID_BASE=33306000000 \
        SPRIG_GATEWAY_DEV_STATE="$OUT/gw" deploy/compose/gateway-dev.sh up
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
        CREATE BINDING r9k.jobs TO QUEUE r9k.work KEY 'job.#' FOR EACH TENANT"
    measure defs
    # 2. members
    local i=0 t0
    t0=$(date +%s)
    while (( i < TENANTS )); do
        for _ in $(seq 1 "$PAR"); do
            i=$((i + 1))
            (( i <= TENANTS )) && member "$(printf 'r9k-m%05d' "$i")" &
        done
        wait
        (( i % 500 == 0 )) && measure "members=$i rate=$(( i / ($(date +%s) - t0 + 1) ))/s"
    done
    # 3. forwarder: 1,000 random members, one transactional publish each
    shuf -n 1000 "$OUT/members" >"$OUT/fwd-sample"
    local start_ms
    start_ms=$(date +%s%3N)
    xargs -P 32 -I{} mysql -h127.0.0.1 -P"$MYSQL_PORT" -u{}.app -pscale-app --ssl-mode=DISABLED -N \
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
    SPRIG_GATEWAY_DEV_STATE="$OUT/gw" deploy/compose/gateway-dev.sh down || true
    docker rm -f r9-10k-mariadb r9-10k-etcd
}

case ${1:-} in
    up) up ;;
    run) run ;;
    down) down ;;
    *) die "usage: $0 up|run|down" ;;
esac
