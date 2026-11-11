#!/usr/bin/env bash
# Smoke test of a running Compose-on-VM bundle: logs in over TLS on all nine
# client protocols and does one operation on each (RUNBOOK.md, "Install").
#
#   SMOKE_PASSWORD=... smoke/smoke.sh <tenant.user> [host]
#
# Needs the mysql client, mongosh and node (18+) on the machine running it,
# and the bundle's secrets/tls/ca.pem. Reads the ports from ../.env.
set -uo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
set -a
# shellcheck disable=SC1091
source "$here/.env"
set +a
login=${1:?usage: SMOKE_PASSWORD=... smoke.sh <tenant.user> [host]}
host=${2:-${SPRIG_PUBLIC_HOST:-127.0.0.1}}
pw=${SMOKE_PASSWORD:?set SMOKE_PASSWORD}
ca=$here/secrets/tls/ca.pem
fail=0
db=smoke_$(date +%s)

my() {
    local port=$1
    shift
    mysql -h "$host" -P "$port" -u "$login" "-p$pw" --ssl-mode=VERIFY_CA --ssl-ca="$ca" -N -B "$@" 2>&1 | grep -v "Using a password"
}
report() { printf '%s  %-26s %s\n' "$1" "$2" "$3"; [[ $1 == PASS ]] || fail=1; }

out=$(my "${SPRIG_MYSQL_PORT:-3306}" -e "CREATE DATABASE $db; CREATE TABLE $db.t (id INT PRIMARY KEY, v VARCHAR(20)); INSERT INTO $db.t VALUES (1,'mysql'),(2,'persona'); SELECT COUNT(*) FROM $db.t; SELECT @@version")
if [[ $(sed -n 1p <<<"$out") == 2 ]]; then report PASS "MySQL (TLS)" "rows 2, version $(sed -n 2p <<<"$out")"; else report FAIL "MySQL (TLS)" "$out"; fi

out=$(my "${SPRIG_MARIADB_PORT:-3307}" -e "SELECT GROUP_CONCAT(v ORDER BY id) FROM $db.t; SELECT @@version; SHOW STATUS LIKE 'Ssl_cipher'")
if [[ $(sed -n 1p <<<"$out") == mysql,persona ]]; then report PASS "MariaDB (TLS)" "read back, version $(sed -n 2p <<<"$out"), $(sed -n 3p <<<"$out" | tr '\t' ' ')"; else report FAIL "MariaDB (TLS)" "$out"; fi

out=$(mongosh --quiet "mongodb://$login:$pw@$host:${SPRIG_MONGO_PORT:-27017}/?authSource=admin&directConnection=true&tls=true&tlsCAFile=$ca" \
    --eval "const d = db.getSiblingDB('${db}_docs'); d.c.insertOne({_id: 1, k: 'mongo'}); print(d.c.countDocuments({k: 'mongo'})); d.dropDatabase()" 2>&1)
if [[ $(head -1 <<<"$out") == 1 ]]; then report PASS "MongoDB (TLS)" "insertOne, countDocuments 1"; else report FAIL "MongoDB (TLS)" "$out"; fi

# Bolt's default graph lives in the logical database `graph`, and AMQP's vhost
# `/` in `mq` (amqp.default_vhost_database): make them for the test if the
# tenant has none, and drop them again afterwards.
made=()
for d in graph mq; do
    if [[ -z $(my "${SPRIG_MYSQL_PORT:-3306}" -e "SHOW DATABASES LIKE '$d'") ]]; then
        my "${SPRIG_MYSQL_PORT:-3306}" -e "CREATE DATABASE $d" >/dev/null && made+=("$d")
    fi
done
node "$here/smoke/smoke.mjs" "$host" "$ca" "$login" "$pw" \
    "{\"https\":${SPRIG_HTTPS_PORT:-8443},\"redis\":${SPRIG_REDIS_PORT:-6379},\"bolt\":${SPRIG_BOLT_PORT:-7687},\"kafka\":${SPRIG_KAFKA_PORT:-9092},\"amqps\":${SPRIG_AMQPS_PORT:-5671}}" || fail=1

# A plaintext attempt must be refused on the MySQL port (TLS required).
if mysql -h "$host" -P "${SPRIG_MYSQL_PORT:-3306}" -u "$login" "-p$pw" --ssl-mode=DISABLED -e "SELECT 1" >/dev/null 2>&1; then
    report FAIL "MySQL without TLS" "a plaintext login worked"
else
    report PASS "MySQL without TLS" "refused, as required"
fi

my "${SPRIG_MYSQL_PORT:-3306}" -e "DROP DATABASE $db" >/dev/null
for d in ${made[@]+"${made[@]}"}; do my "${SPRIG_MYSQL_PORT:-3306}" -e "DROP DATABASE $d" >/dev/null; done
exit $fail
