#!/usr/bin/env bash
# The mover's live gateway test bed: a second gateway in
# dc-a on 17306/17307 (gateway.toml) and the tenant `moversb` with the user
# app, made through the running controller's Admin API (gateway-dev.sh up
# must be running: it registers the clusters).
#
#   deploy/compose/mover/run.sh up      # build, start the gateway, make the tenant
#   deploy/compose/mover/run.sh down    # stop the gateway, drop the tenant
#
# Then: SPRIG_TEST_MOVER_GATEWAY=1 cargo test -p sprig-mover --test live_gateway
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
state=deploy/compose/mover/.state
mkdir -p "$state"
printf '*\n' >"$state/.gitignore"
target=${CARGO_TARGET_DIR:-target}
bin=$target/debug
admin=${SPRIG_ADMIN:-http://127.0.0.1:7480}
tenant=moversb
pw=sprig-dev-app
ctl() { "$bin/sprigctl" --endpoint "$admin" "$@"; }

case ${1:-} in
up)
    cargo build -q -p sprigdb -p sprigctl
    if ! [[ -f $state/gateway.pid ]] || ! kill -0 "$(cat "$state/gateway.pid")" 2>/dev/null; then
        nohup "$bin/sprigdb" --config deploy/compose/mover/gateway.toml gateway \
            >"$state/gateway.log" 2>&1 &
        echo $! >"$state/gateway.pid"
    fi
    if ! ctl tenant show "$tenant" >/dev/null 2>&1; then
        ctl tenant create "$tenant" --cluster dc-a-c1
    fi
    ctl tenant user add "$tenant" app --password "$pw" --native --if-not-exists >/dev/null
    ctl settings set auth.mysql_native_password true --scope "tenant:$tenant" >/dev/null || true
    ctl -o json tenant show "$tenant" | jq -r .tenant_id >"$state/tenant_id"
    for _ in $(seq 60); do
        nc -z 127.0.0.1 17306 2>/dev/null && break
        sleep 1
    done
    echo "mover gateway up: mysql -h127.0.0.1 -P17306 -u $tenant.app -p$pw"
    ;;
down)
    if [[ -f $state/gateway.pid ]]; then
        kill "$(cat "$state/gateway.pid")" 2>/dev/null || true
        rm -f "$state/gateway.pid"
    fi
    ;;
*)
    echo "usage: $0 up|down" >&2
    exit 2
    ;;
esac
