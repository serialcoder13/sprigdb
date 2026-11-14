#!/usr/bin/env bash
# Reference servers for CI's heavy-suite jobs. Each suite runs on the
# reference first, then through the gateway, and a test that fails on the
# reference too is not counted against SprigDB
# (deploy/compose/clients/summarize.py, SPRIG_CLIENTS_BASELINE).
#
#   refs.sh up <ref>...     start and wait until it answers
#   refs.sh down <ref>...   remove (exact container names)
#
#   mysql     mysql:8.4.11 on 127.0.0.1:23306, root / sprig-dev-root, 3 GB
#   mariadb   mariadb:11.8.9 on 127.0.0.1:23307, root / sprig-dev-root, 3 GB
#
# Unlike the laptop's 768 MB references (agent brief rule 4) these use the
# servers' normal memory profile, 64 MiB max_allowed_packet and local_infile,
# so the Connector/J blob and big-packet tests can run against them.
set -euo pipefail

MYSQL_IMAGE=${SPRIG_CI_MYSQL_IMAGE:-mysql:8.4.11}
MARIADB_IMAGE=${SPRIG_CI_MARIADB_IMAGE:-mariadb:11.8.9}
PASS=sprig-dev-root

up() {
    case $1 in
    mysql)
        docker run -d --name ci-ref-mysql --memory 3g -p 127.0.0.1:23306:3306 \
            -e MYSQL_ROOT_PASSWORD=$PASS -e MYSQL_ROOT_HOST=% "$MYSQL_IMAGE" \
            --max-allowed-packet=67108864 --local-infile=1 --log-bin-trust-function-creators=1 \
            --max-connections=500 >/dev/null
        ;;
    mariadb)
        docker run -d --name ci-ref-mariadb --memory 3g -p 127.0.0.1:23307:3306 \
            -e MARIADB_ROOT_PASSWORD=$PASS -e MARIADB_ROOT_HOST=% "$MARIADB_IMAGE" \
            --max-allowed-packet=67108864 --local-infile=1 --log-bin-trust-function-creators=1 \
            --max-connections=500 >/dev/null
        ;;
    *) echo "refs: unknown reference $1" >&2; exit 2 ;;
    esac
    local name=ci-ref-$1 cli=mysql
    [[ $1 == mariadb ]] && cli=mariadb
    for _ in $(seq 1 120); do
        # The entrypoint's temporary server listens only on the socket; the
        # real one answers on TCP.
        if docker exec "$name" "$cli" -h 127.0.0.1 -uroot -p$PASS -e 'SELECT 1' >/dev/null 2>&1; then
            echo "refs: $1 up ($(docker exec "$name" "$cli" -N -h 127.0.0.1 -uroot -p$PASS -e 'SELECT VERSION()' 2>/dev/null))"
            return
        fi
        sleep 2
    done
    docker logs --tail 50 "$name" >&2
    echo "refs: $1 didn't come up" >&2
    exit 1
}

[[ $# -ge 2 ]] || { sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2; }
cmd=$1
shift
for r in "$@"; do
    case $cmd in
    up) up "$r" ;;
    down) docker rm -f "ci-ref-$r" >/dev/null 2>&1 || true ;;
    *) echo "refs: up or down" >&2; exit 2 ;;
    esac
done
