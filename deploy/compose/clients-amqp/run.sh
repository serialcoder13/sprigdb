#!/usr/bin/env bash
# AMQP 0-9-1 client checks against a RabbitMQ reference and SprigDB's AMQP
# listener (chapter 50, "Testing"; chapter 46, "Differential testing"), for
# CI's heavy-suite jobs (.github/workflows/heavy-protocols.yml). Every check
# runs on the reference first, then on SprigDB, and
# deploy/compose/ci/outcomes.py compares the two against expected/<check>.txt.
#
#   run.sh diff       pika 1.3.2 scenarios (amqp_diff.py, r9final's 21)
#   run.sh go         amqp091-go v1.10.0's own integration tests (AMQP_URL)
#   run.sh java       RabbitMQ Java client 5.22.0 scenarios (java/)
#   run.sh dotnet     RabbitMQ.Client 6.8.1 scenarios (dotnet/)
#   run.sh perftest   RabbitMQ PerfTest 2.21.0, 30 s per side: fails on any
#                     error or a side that delivers nothing; rates recorded
#
# Every client runs in a pinned official image on the host network (CI
# runners are Linux); downloads happen there. Endpoints:
#   SPRIG_AMQP_REF   reference host:port (127.0.0.1:5672)
#   SPRIG_AMQP_SUT   SprigDB host:port (127.0.0.1:25672, deploy/compose/ci/listeners.sh)
#   SPRIG_AMQP_USER / SPRIG_AMQP_PASSWORD   app / sprig-dev-app on both (the
#                    listener binds plain names to tenant acme; the reference
#                    is started with the same default user)
#   SPRIG_AMQP_VHOST  / (SprigDB: database mq, which must exist)
#   SPRIG_CI_RESULTS  results root (default target/ci-results)
# The containers' scripts are single-quoted on purpose: they expand inside.
# shellcheck disable=SC2016
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
CHECK=${1:-}
REF=${SPRIG_AMQP_REF:-127.0.0.1:5672}
SUT=${SPRIG_AMQP_SUT:-127.0.0.1:25672}
USER_=${SPRIG_AMQP_USER:-app}
PASS=${SPRIG_AMQP_PASSWORD:-sprig-dev-app}
VHOST=${SPRIG_AMQP_VHOST:-/}
OUT=${SPRIG_CI_RESULTS:-$ROOT/target/ci-results}/amqp-$CHECK
PY_IMAGE=python:3.12.12-slim-bookworm
GO_IMAGE=golang:1.23.4-bookworm
MAVEN_IMAGE=maven:3.9.11-eclipse-temurin-21
DOTNET_IMAGE=mcr.microsoft.com/dotnet/sdk:8.0.404

rm -rf "$OUT"
mkdir -p "$OUT/ref" "$OUT/sut" "$OUT/cache"

# dock <image> <command...>: the harness at /h (read-only), results at /out.
dock() {
    local image=$1
    shift
    docker run --rm --name "ci-amqp-$CHECK-$$" --network host -v "$HERE:/h:ro" -v "$OUT:/out" "$image" "$@"
}

# host:port of a side.
hp() { if [[ $1 == ref ]]; then echo "$REF"; else echo "$SUT"; fi; }

compare_scenarios() {
    python3 -I "$ROOT/deploy/compose/ci/outcomes.py" scenarios "$OUT/ref/out.json" "$OUT/sut/out.json" \
        "$HERE/expected/$CHECK.txt" sprig "$OUT"
}

case $CHECK in
diff)
    for side in ref sut; do
        IFS=: read -r h p <<<"$(hp $side)"
        dock "$PY_IMAGE" sh -c 'pip install -q --no-cache-dir "pika==1.3.2" > /out/pip.log 2>&1 &&
            python -I /h/amqp_diff.py "$0" "$1" "$2" "$3" "$4" ci.diff > /out/'$side'/out.json' \
            "$h" "$p" "$USER_" "$PASS" "$VHOST"
    done
    compare_scenarios
    ;;
go)
    # Upstream's integration tests (`-tags integration`) read AMQP_URL.
    for side in ref sut; do
        url="amqp://$USER_:$PASS@$(hp $side)/"
        [[ $VHOST != / ]] && url+=$VHOST
        dock "$GO_IMAGE" sh -c 'git clone -q --depth 1 --branch v1.10.0 https://github.com/rabbitmq/amqp091-go.git /src &&
            cd /src && AMQP_URL="$0" go test -tags integration -json -count=1 -timeout 25m ./... \
              > /out/'$side'/go-test.json 2> /out/'$side'/go-test.err; true' "$url"
    done
    python3 -I "$ROOT/deploy/compose/ci/outcomes.py" tests gojson "$OUT/ref" "$OUT/sut" \
        "$HERE/expected/go.txt" sprig "$OUT"
    ;;
java)
    dock "$MAVEN_IMAGE" sh -c 'cp /h/java/pom.xml /tmp/pom.xml && cd /tmp &&
        mvn -q -B dependency:copy-dependencies -DoutputDirectory=/out/cache/libs > /out/mvn.log 2>&1'
    for side in ref sut; do
        IFS=: read -r h p <<<"$(hp $side)"
        dock "$MAVEN_IMAGE" sh -c 'java -cp "/out/cache/libs/*" /h/java/Scenarios.java "$0" "$1" "$2" "$3" "$4" ci.java \
            > /out/'$side'/out.json' "$h" "$p" "$USER_" "$PASS" "$VHOST"
    done
    compare_scenarios
    ;;
dotnet)
    for side in ref sut; do
        IFS=: read -r h p <<<"$(hp $side)"
        dock "$DOTNET_IMAGE" sh -c 'mkdir -p /tmp/s && cp /h/dotnet/* /tmp/s/ && cd /tmp/s &&
            DOTNET_CLI_TELEMETRY_OPTOUT=1 dotnet run -c Release -- "$0" "$1" "$2" "$3" "$4" ci.dotnet \
              > /out/'$side'/out.json 2> /out/'$side'/dotnet.err' "$h" "$p" "$USER_" "$PASS" "$VHOST"
    done
    compare_scenarios
    ;;
perftest)
    dock "$MAVEN_IMAGE" sh -c 'cp /h/perftest/pom.xml /tmp/pom.xml && cd /tmp &&
        mvn -q -B dependency:copy-dependencies -DoutputDirectory=/out/cache/libs > /out/mvn.log 2>&1'
    bad=0
    for side in ref sut; do
        url="amqp://$USER_:$PASS@$(hp $side)/"
        [[ $VHOST != / ]] && url+=$VHOST
        # Two producers and two consumers on one named queue, persistent
        # messages of 1 KB, publisher confirms (100 in flight), 30 s.
        dock "$MAVEN_IMAGE" java -cp "/out/cache/libs/*" com.rabbitmq.perf.PerfTest \
            --uri "$url" --producers 2 --consumers 2 --queue ci.perf --auto-delete true \
            --size 1000 --flag persistent --confirm 100 --time 30 >"$OUT/$side/perftest.log" 2>&1 ||
            { echo "perftest $side: exit $?" >&2; bad=1; }
        send=$(sed -n 's/.*sending rate avg: \([0-9]*\).*/\1/p' "$OUT/$side/perftest.log" | tail -n 1)
        recv=$(sed -n 's/.*receiving rate avg: \([0-9]*\).*/\1/p' "$OUT/$side/perftest.log" | tail -n 1)
        echo "perftest $side: sending ${send:-?} msg/s, receiving ${recv:-?} msg/s" | tee -a "$OUT/summary.txt"
        if grep -qiE 'exception|error' "$OUT/$side/perftest.log" || [[ -z $recv || $recv == 0 ]]; then
            echo "perftest $side: errors or nothing received (see $OUT/$side/perftest.log)" | tee -a "$OUT/summary.txt" >&2
            bad=1
        fi
    done
    exit $bad
    ;;
*)
    sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'
    exit 2
    ;;
esac
