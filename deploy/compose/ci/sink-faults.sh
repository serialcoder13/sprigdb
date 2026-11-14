#!/usr/bin/env bash
# The sink fault-injection suite: a queue sink delivers N messages through
# toxiproxy to PostgreSQL (EXACTLY ONCE), Kafka or MinIO (at least once); at
# about a quarter delivered a fault starts:
#
#   cut        the proxy refuses and drops connections for F s (20)
#   down       the destination's container stopped for F s (45)
#   slow       300 ms (+-100) latency both ways for F s (30)
#   stall      data stops and connections hang for F s (20)
#   long-cut   cut for F s (240), past sink.pause_after_minutes (1 here):
#              the sink pauses and its probe resumes it
#   long-down  down for F s (240), the same
#
# The run passes when every message arrives (all N distinct; for PostgreSQL
# also no duplicate), the queue is empty, nothing is dead-lettered and the
# sink is running again. Results go to target/ci-results/sink-faults.
#
#   sink-faults.sh up                           containers and proxies
#   sink-faults.sh run pg|kafka|s3 <fault> [N]  one run (exit 1 when it fails)
#   sink-faults.sh down
#
# Runs against the CI stack (.github/actions/sprig-stack with the amqp
# listener: tenant acme, login acme.app, MySQL persona 127.0.0.1:13306).
# Every client names its host and port. Images are pinned; containers get
# at most 768 MB (toxiproxy 128 MB) and are removed by `down`.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."

net=ci-sinkf
toxi=http://127.0.0.1:${SPRIG_CI_TOXI_API:-28474}
sql_port=${SPRIG_CI_SQL_PORT:-13306}
user=${SPRIG_CI_SQL_USER:-acme.app}
pass=${SPRIG_CI_SQL_PASSWORD:-sprig-dev-app}
mariadb_image=${SPRIG_CI_MARIADB_IMAGE:-mariadb:11.8.9}
kafka_image=apache/kafka:3.9.0
pg_image=postgres:16.10
minio_image=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z
mc_image=quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z
toxi_image=ghcr.io/shopify/toxiproxy:2.12.0
py_image=${SPRIG_CI_PY_IMAGE:-python:3.12.12-slim-bookworm}
out=target/ci-results/sink-faults
mkdir -p "$out"

say() { echo "sink-faults: $*" >&2; }

sql() {
    docker run --rm -i --network host "$mariadb_image" \
        mariadb --ssl --disable-ssl-verify-server-cert -h 127.0.0.1 -P "$sql_port" \
        -u "$user" -p"$pass" -N -e "$1"
}

up() {
    docker network create "$net" >/dev/null
    docker run -d --name ci-sinkf-pg --network "$net" --memory 768m \
        -e POSTGRES_PASSWORD=ci-secret -e POSTGRES_DB=qcr "$pg_image" >/dev/null
    docker run -d --name ci-sinkf-minio --network "$net" --memory 768m \
        -e MINIO_ROOT_USER=ci -e MINIO_ROOT_PASSWORD=ci-secret-key "$minio_image" server /data >/dev/null
    docker run -d --name ci-sinkf-kafka --network "$net" --memory 768m -p 127.0.0.1:29092:29092 \
        -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_LISTENERS=PLAINTEXT://:29092,PROXIED://:29094,CONTROLLER://:9093 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:29092,PROXIED://127.0.0.1:29093 \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PROXIED:PLAINTEXT \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
        -e KAFKA_HEAP_OPTS='-Xmx384m -Xms384m' "$kafka_image" >/dev/null
    docker run -d --name ci-sinkf-toxi --network "$net" --memory 128m \
        -p 127.0.0.1:28474:8474 -p 127.0.0.1:29093:29093 -p 127.0.0.1:25433:25433 -p 127.0.0.1:29001:29001 \
        "$toxi_image" >/dev/null
    timeout 120 bash -c "until curl -fsS $toxi/version >/dev/null 2>&1; do sleep 1; done"
    curl -fsS -X POST "$toxi/proxies" -d '{"name":"kafka","listen":"0.0.0.0:29093","upstream":"ci-sinkf-kafka:29094"}' >/dev/null
    curl -fsS -X POST "$toxi/proxies" -d '{"name":"pg","listen":"0.0.0.0:25433","upstream":"ci-sinkf-pg:5432"}' >/dev/null
    curl -fsS -X POST "$toxi/proxies" -d '{"name":"s3","listen":"0.0.0.0:29001","upstream":"ci-sinkf-minio:9000"}' >/dev/null
    timeout 120 bash -c 'until docker exec ci-sinkf-pg pg_isready -h 127.0.0.1 -p 5432 -U postgres >/dev/null 2>&1; do sleep 2; done'
    timeout 180 bash -c 'until docker exec ci-sinkf-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:29092 >/dev/null 2>&1; do sleep 3; done'
    timeout 120 bash -c "until docker run --rm --network $net --entrypoint sh $mc_image -c 'mc alias set l http://ci-sinkf-minio:9000 ci ci-secret-key >/dev/null && mc mb -p l/qcr' >/dev/null 2>&1; do sleep 2; done"
    # Sinks may reach the proxies; short retries so the faults show in
    # minutes, and pausing after 1 minute for the long faults.
    target/release/sprigctl --endpoint http://127.0.0.1:7480 settings set sink.destination_allowlist \
        '["127.0.0.1:29093","127.0.0.1:25433","127.0.0.1:29001"]' --scope global --reason ci-sink-faults
    target/release/sprigctl --endpoint http://127.0.0.1:7480 settings set sink.pause_after_minutes 1 \
        --scope tenant:acme --reason ci-sink-faults
    sql "CREATE DATABASE IF NOT EXISTS qcr"
    say "up"
}

down() {
    docker rm -f ci-sinkf-toxi ci-sinkf-kafka ci-sinkf-pg ci-sinkf-minio >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
}

# total and distinct message numbers at the destination of queue $q.
count() {
    local kind=$1 q=$2
    case $kind in
    kafka) docker run --rm --network "container:ci-sinkf-kafka" --entrypoint /opt/kafka/bin/kafka-console-consumer.sh \
               "$kafka_image" --bootstrap-server 127.0.0.1:29092 --topic "$q" --from-beginning \
               --isolation-level read_committed --timeout-ms 5000 2>/dev/null ;;
    s3) docker run --rm --network "$net" --entrypoint sh "$mc_image" -c \
            "mc alias set l http://ci-sinkf-minio:9000 ci ci-secret-key >/dev/null && for o in \$(mc find l/qcr/$q/); do mc cat \$o; done" 2>/dev/null |
            gunzip -c 2>/dev/null ;;
    pg) docker exec ci-sinkf-pg psql -h 127.0.0.1 -p 5432 -U postgres -d qcr -At \
            -c "SELECT '\"n\": ' || n FROM \"$q\"" 2>/dev/null ;;
    esac | grep -oE 'n\\?": ?[0-9]+' | grep -oE '[0-9]+$' |
        awk '{n++; s[$0]=1} END {d=0; for (k in s) d++; print n+0, d}'
}

run() {
    local kind=$1 fault=$2 n=${3:-2000} secs proxy dest q
    q=f_${kind}_${fault//-/_}_$(date +%H%M%S)
    case $fault in
    cut | stall) secs=20 ;; down) secs=45 ;; slow) secs=30 ;; long-cut | long-down) secs=240 ;;
    *) say "unknown fault $fault"; return 2 ;;
    esac
    case $kind in
    kafka) proxy=kafka dest=ci-sinkf-kafka ;; pg) proxy=pg dest=ci-sinkf-pg ;; s3) proxy=s3 dest=ci-sinkf-minio ;;
    *) say "unknown target $kind"; return 2 ;;
    esac
    # The queue and its N messages, through the AMQP listener (pika 1.3.2,
    # as clients-amqp's diff job).
    docker run --rm --network host "$py_image" sh -c 'pip install -q --no-cache-dir "pika==1.3.2" >/dev/null 2>&1 &&
        python -I -c "
import json, sys, pika
q, n, user, pw, port = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5])
c = pika.BlockingConnection(pika.ConnectionParameters(host=\"127.0.0.1\", port=port, virtual_host=\"qcr\",
    credentials=pika.PlainCredentials(user, pw)))
ch = c.channel(); ch.queue_declare(q, durable=True); ch.confirm_delivery()
for i in range(n):
    ch.basic_publish(\"\", q, json.dumps({\"n\": i}).encode(), pika.BasicProperties(message_id=f\"m{i}\", delivery_mode=2))
c.close()
" "$0" "$1" "$2" "$3" "$4"' "$q" "$n" "$user" "$pass" "${SPRIG_CI_AMQP_PORT:-25672}"
    local i
    case $kind in
    kafka)
        sql "CREATE CONNECTION IF NOT EXISTS qcr.kx TYPE KAFKA WITH (bootstrap = '127.0.0.1:29093', request_timeout_ms = 10000)"
        sql "CREATE SINK qcr.s_$q FROM QUEUE $q TO KAFKA CONNECTION kx TOPIC '$q' WITH (batch_max_wait_ms = 100)" ;;
    pg)
        sql "CREATE CONNECTION IF NOT EXISTS qcr.px TYPE POSTGRESQL WITH (host = '127.0.0.1', port = 25433, database = 'qcr', tls = 'off') CREDENTIALS (username = 'postgres', password = 'ci-secret')"
        sql "CREATE SINK qcr.s_$q FROM QUEUE $q TO DATABASE CONNECTION px TABLE '$q' GUARANTEE EXACTLY ONCE WITH (batch_max_wait_ms = 100, schema_policy = 'create', primary_key = 'n')" ;;
    s3)
        sql "CREATE CONNECTION IF NOT EXISTS qcr.sx TYPE S3 WITH (endpoint = 'http://127.0.0.1:29001', bucket = 'qcr', region = 'us-east-1', path_style = 'on') CREDENTIALS (access_key_id = 'ci', secret_access_key = 'ci-secret-key')"
        sql "CREATE SINK qcr.s_$q FROM QUEUE $q TO S3 CONNECTION sx PREFIX '$q/' WITH (batch_max_wait_ms = 100, s3.file_max_records = 200, s3.file_max_age_seconds = 2)" ;;
    esac
    local t0 c
    t0=$(date +%s)
    for i in $(seq 1 240); do
        c=$(count "$kind" "$q" | awk '{print $2}')
        ((${c:-0} >= n / 4)) && break
        sleep 0.5
    done
    say "$kind $fault starts at $(count "$kind" "$q") after $(($(date +%s) - t0)) s"
    case $fault in
    cut | long-cut)
        curl -fsS -X POST "$toxi/proxies/$proxy" -d '{"enabled": false}' >/dev/null
        sleep "$secs"
        curl -fsS -X POST "$toxi/proxies/$proxy" -d '{"enabled": true}' >/dev/null ;;
    down | long-down) docker stop "$dest" >/dev/null; sleep "$secs"; docker start "$dest" >/dev/null ;;
    slow)
        curl -fsS -X POST "$toxi/proxies/$proxy/toxics" -d '{"name":"lat_down","type":"latency","stream":"downstream","attributes":{"latency":300,"jitter":100}}' >/dev/null
        curl -fsS -X POST "$toxi/proxies/$proxy/toxics" -d '{"name":"lat_up","type":"latency","stream":"upstream","attributes":{"latency":300,"jitter":100}}' >/dev/null
        sleep "$secs"
        curl -fsS -X DELETE "$toxi/proxies/$proxy/toxics/lat_down"
        curl -fsS -X DELETE "$toxi/proxies/$proxy/toxics/lat_up" ;;
    stall)
        curl -fsS -X POST "$toxi/proxies/$proxy/toxics" -d '{"name":"stall","type":"timeout","stream":"downstream","attributes":{"timeout":0}}' >/dev/null
        sleep "$secs"
        curl -fsS -X DELETE "$toxi/proxies/$proxy/toxics/stall" ;;
    esac
    local t1 total distinct left state dead result=pass
    t1=$(date +%s)
    say "$kind $fault ends at $(count "$kind" "$q")"
    for i in $(seq 1 300); do
        c=$(count "$kind" "$q" | awk '{print $2}')
        ((${c:-0} >= n)) && break
        sleep 2
    done
    sleep 8
    read -r total distinct < <(count "$kind" "$q")
    left=$(sql "CALL sprig.mq_info('$q', 'qcr')" | awk '{print $1 + $2}')
    state=$(sql "SHOW SINK STATUS qcr.s_$q" | head -1 | awk '{print $2}')
    dead=$(sql "SELECT COUNT(*) FROM qcr.\`s_${q}_dead\`" 2>/dev/null || echo 0)
    if ((distinct != n)) || ((left != 0)) || [[ $state != running ]] || ((${dead:-0} != 0)); then result=fail; fi
    if [[ $kind == pg ]] && ((total != n)); then result=fail; fi
    echo "$kind $fault: $distinct of $n distinct, $total in all, $(($(date +%s) - t1)) s after the fault; queue $left; sink $state; dead letters ${dead:-0}: $result" |
        tee -a "$out/results.txt"
    sql "DROP SINK qcr.s_$q" >/dev/null
    [[ $result == pass ]]
}

case ${1:-} in
up) up ;;
down) down ;;
run) shift; run "$@" ;;
*) echo "usage: sink-faults.sh up | run pg|kafka|s3 <fault> [N] | down" >&2; exit 2 ;;
esac
