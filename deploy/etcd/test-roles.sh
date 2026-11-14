#!/bin/sh
# Checks roles.sh against a throwaway etcd container (never the shared dev
# etcd): each role writes only its prefixes.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
name=sprig-sim-etcd-roles-$$
docker run -d --name "$name" --memory 256m quay.io/coreos/etcd:v3.6.5 etcd \
    --listen-client-urls=http://0.0.0.0:2379 \
    --advertise-client-urls=http://sprig-etcd-roles.invalid:2379 >/dev/null
trap 'docker rm -f -v "$name" >/dev/null' EXIT
sleep 2
export ETCDCTL="docker exec $name etcdctl" SPRIG_ETCD_TEST_PASSWORD=pw
"$here/roles.sh" dc fra1
"$here/roles.sh" global fra1 ams1
$ETCDCTL auth enable >/dev/null
fail=0
ok() { if docker exec "$name" etcdctl --user "$1:pw" put "$2" x >/dev/null 2>&1; then :; else echo "FAIL: $1 can't write $2"; fail=1; fi; }
no() { if docker exec "$name" etcdctl --user "$1:pw" put "$2" x >/dev/null 2>&1; then echo "FAIL: $1 wrote $2"; fail=1; fi; }
ok sprig-gateway /sprig/dc/fra1/gateways/gw1
no sprig-gateway /sprig/dc/fra1/clusters/fra1-c1
no sprig-gateway /sprig/g/tenants/42
ok sprig-worker /sprig/dc/fra1/jobs/j1
no sprig-worker /sprig/dc/fra1/shardmaps/42/0
ok sprig-agent /sprig/dc/fra1/leases/writer/fra1-c1
no sprig-agent /sprig/dc/fra1/clusters/fra1-c1
ok sprig-controller /sprig/dc/fra1/clusters/fra1-c1
ok sprig-dc-fra1 /sprig/g/dc-leases/fra1
ok sprig-dc-fra1 /sprig/g/latency/fra1/ams1
no sprig-dc-fra1 /sprig/g/dc-leases/ams1
no sprig-dc-fra1 /sprig/g/tenants/42
ok sprig-controller /sprig/g/tenants/42
docker exec "$name" etcdctl --user sprig-agent:pw get /sprig/dc/fra1/clusters/fra1-c1 >/dev/null || { echo "FAIL: agent can't read clusters/"; fail=1; }
# Endpoint discovery needs MemberList.
docker exec "$name" etcdctl --user sprig-gateway:pw member list >/dev/null || { echo "FAIL: a prefix-limited user can't list members"; fail=1; }
[ $fail -eq 0 ] && echo "etcd roles: every check passed"
exit $fail
