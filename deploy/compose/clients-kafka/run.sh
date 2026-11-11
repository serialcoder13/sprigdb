#!/usr/bin/env bash
# Kafka client checks against an Apache Kafka reference and SprigDB's Kafka
# listener (chapter 45, "Testing"; chapter 46, "Differential testing"), for
# CI's heavy-suite jobs (.github/workflows/heavy-protocols.yml). Each check
# creates its topic on both sides, runs on the reference, then on SprigDB,
# and deploy/compose/ci/outcomes.py compares the two observation objects
# against expected/<check>.txt.
#
#   run.sh python   kafka-python 2.0.6 (kafka_suite.py, r9final's checks)
#   run.sh jvm      the Java client, through apache/kafka 3.9.0's own tools:
#                   kafka-verifiable-producer and -consumer (1,000 records,
#                   acks=all, a group with commits), kafka-producer-perf-test
#                   (20,000 records of 200 bytes; rate recorded) and
#                   kafka-consumer-groups --describe
#   run.sh go       franz-go v1.18.0 (go/)
#   run.sh dotnet   Confluent.Kafka 2.5.3 (dotnet/; librdkafka inside)
#
# SprigDB's topics are streams named <database>.<stream> (chapter 45), so
# every check uses topic kt.<check> in database kt on both sides; the job
# creates database kt first. SprigDB is reached with SASL SCRAM-SHA-512 as
# the tenant login, the reference in PLAINTEXT.
#
#   SPRIG_KAFKA_REF   reference bootstrap (127.0.0.1:9092; its container
#                     ci-ref-kafka creates the topic)
#   SPRIG_KAFKA_SUT   SprigDB bootstrap (127.0.0.1:29092, deploy/compose/ci/listeners.sh)
#   SPRIG_KAFKA_USER / SPRIG_KAFKA_PASSWORD   acme.app / sprig-dev-app
#   SPRIG_CI_RESULTS  results root (default target/ci-results)
# The containers' scripts are single-quoted on purpose: they expand inside.
# shellcheck disable=SC2016
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
CHECK=${1:-}
REF=${SPRIG_KAFKA_REF:-127.0.0.1:9092}
SUT=${SPRIG_KAFKA_SUT:-127.0.0.1:29092}
USER_=${SPRIG_KAFKA_USER:-acme.app}
PASS=${SPRIG_KAFKA_PASSWORD:-sprig-dev-app}
OUT=${SPRIG_CI_RESULTS:-$ROOT/target/ci-results}/kafka-$CHECK
TOPIC=kt.$CHECK
GROUP=ci-$CHECK-g1
PY_IMAGE=python:3.12.12-slim-bookworm
GO_IMAGE=golang:1.23.4-bookworm
KAFKA_IMAGE=apache/kafka:3.9.0
DOTNET_IMAGE=mcr.microsoft.com/dotnet/sdk:8.0.404

case $CHECK in
python | jvm | go | dotnet) ;;
*) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2 ;;
esac

rm -rf "$OUT"
mkdir -p "$OUT/ref" "$OUT/sut"

dock() {
    local image=$1
    shift
    docker run --rm --name "ci-kafka-$CHECK-$$" --network host -v "$HERE:/h:ro" -v "$OUT:/out" "$image" "$@"
}

# The topic, three partitions, on both sides.
docker exec ci-ref-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "$TOPIC" --partitions 3 --replication-factor 1 >"$OUT/ref/topic.log" 2>&1
"$ROOT/deploy/compose/ci/sql.sh" "CREATE STREAM IF NOT EXISTS \`kt\`.\`$CHECK\` PARTITIONS 3" >"$OUT/sut/topic.log" 2>&1

# Bootstrap, user ("-" for none) and the JVM tools' client config, per side.
side_args() {
    if [[ $1 == ref ]]; then
        echo "$REF - -"
        : >"$OUT/ref/client.properties"
    else
        echo "$SUT $USER_ $PASS"
        cat >"$OUT/sut/client.properties" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$USER_" password="$PASS";
EOF
    fi
}

for side in ref sut; do
    read -r boot user pass <<<"$(side_args $side)"
    case $CHECK in
    python)
        dock "$PY_IMAGE" sh -c 'pip install -q --no-cache-dir "kafka-python==2.0.6" > /out/pip.log 2>&1 &&
            python -I /h/kafka_suite.py "$0" "$1" "$2" "$3" "$4" > /out/'$side'/out.json 2> /out/'$side'/err.log' \
            "$boot" "$user" "$pass" "$TOPIC" "$GROUP" || true
        ;;
    jvm)
        dock "$KAFKA_IMAGE" sh -c 'B=/opt/kafka/bin; C=/out/'$side'/client.properties; R=/out/'$side';
            timeout 300 $B/kafka-verifiable-producer.sh --bootstrap-server "$0" --topic "$1" --max-messages 1000 \
              --acks -1 --producer.config $C > $R/verifiable-producer.log 2>&1;
            timeout 300 $B/kafka-verifiable-consumer.sh --bootstrap-server "$0" --topic "$1" --group-id "$2" \
              --max-messages 1000 --reset-policy earliest --consumer.config $C > $R/verifiable-consumer.log 2>&1;
            timeout 300 $B/kafka-producer-perf-test.sh --topic "$1" --num-records 20000 --record-size 200 \
              --throughput -1 --producer-props bootstrap.servers="$0" acks=all --producer.config $C > $R/producer-perf.log 2>&1;
            timeout 120 $B/kafka-consumer-groups.sh --bootstrap-server "$0" --describe --group "$2" \
              --command-config $C > $R/groups.log 2>&1; true' "$boot" "$TOPIC" "$GROUP"
        python3 -I "$HERE/jvm_summary.py" "$OUT/$side" >"$OUT/$side/out.json"
        ;;
    go)
        dock "$GO_IMAGE" sh -c 'cp -r /h/go /tmp/s && cd /tmp/s && go mod tidy > /out/go-mod.log 2>&1 &&
            go run . "$0" "$1" "$2" "$3" "$4" > /out/'$side'/out.json 2> /out/'$side'/err.log' \
            "$boot" "$user" "$pass" "$TOPIC" "$GROUP" || true
        ;;
    dotnet)
        dock "$DOTNET_IMAGE" sh -c 'mkdir -p /tmp/s && cp /h/dotnet/* /tmp/s/ && cd /tmp/s &&
            DOTNET_CLI_TELEMETRY_OPTOUT=1 dotnet run -c Release -- "$0" "$1" "$2" "$3" "$4" \
              > /out/'$side'/out.json 2> /out/'$side'/err.log' "$boot" "$user" "$pass" "$TOPIC" "$GROUP" || true
        ;;
    esac
done

python3 -I "$ROOT/deploy/compose/ci/outcomes.py" scenarios "$OUT/ref/out.json" "$OUT/sut/out.json" \
    "$HERE/expected/$CHECK.txt" sprig "$OUT"
