#!/usr/bin/env bash
# Operations for the SprigDB Compose-on-VM bundle (RUNBOOK.md has the
# procedures; chapter 27, "Compose on VMs").
#
#   ./sprig-vm.sh init                  make secrets/ (CA, certificates, passwords, tokens)
#   ./sprig-vm.sh up                    start everything, then configure (idempotent)
#   ./sprig-vm.sh configure             register the cluster, write listener and TLS settings
#   ./sprig-vm.sh status                containers, clusters, listeners
#   ./sprig-vm.sh ctl <args>            sprigctl inside the controller (admin token)
#   ./sprig-vm.sh tenant <name> <user>  create a tenant and a user (password on stdin)
#   ./sprig-vm.sh backup                MariaDB (mariadb-backup) + etcd snapshot into backups/<ts>/
#   ./sprig-vm.sh restore <ts>          stop, put a backup back, start
#   ./sprig-vm.sh upgrade <image>       backup, then roll controller, worker, gateway to <image>
#   ./sprig-vm.sh rollback              roll back to the image before the last upgrade
#   ./sprig-vm.sh rotate-certs          new server certificates from the same CA, live
#   ./sprig-vm.sh down                  stop (data stays in the volumes)
#
# Reads .env next to this file (copy .env.example). Needs docker compose v2,
# openssl and, for init, a POSIX shell. Run as a user that may use docker.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$here"
[[ -f .env ]] || { echo "sprig-vm: no .env here; copy .env.example to .env first" >&2; exit 1; }
set -a
# shellcheck disable=SC1091
source ./.env
set +a
dc=${SPRIG_DC:-dc1}
public_host=${SPRIG_PUBLIC_HOST:-127.0.0.1}
cluster_name=${SPRIG_CLUSTER_NAME:-$dc-main}
dc_compose() { docker compose --project-directory "$here" -f "$here/compose.yaml" "$@"; }
say() { echo "sprig-vm: $*"; }
die() { echo "sprig-vm: $*" >&2; exit 1; }
# sprigctl in the controller container. Before named operators take over it
# uses the bootstrap token; after, the service token RUNBOOK.md's "First
# operator" issues to this script (secrets/sprig/ops-token).
ctl() {
    local tok=/run/sprig/sprig/admin-token
    [[ -s secrets/sprig/ops-token ]] && tok=/run/sprig/sprig/ops-token
    dc_compose exec -T controller sprigctl --endpoint http://127.0.0.1:7480 --token-file "$tok" "$@"
}

# --- init ------------------------------------------------------------------------

rand() { openssl rand -hex "${1:-32}"; }

# cert <name> <CN> <subjectAltName> <extendedKeyUsage>
cert() {
    local n=$1 cn=$2 san=$3 eku=$4 d=secrets/tls
    openssl req -newkey rsa:3072 -nodes -subj "/O=sprigdb/CN=$cn" -keyout "$d/$n-key.pem.new" -out "$d/$n.csr" 2>/dev/null
    printf '%s\n' "subjectAltName=$san" "basicConstraints=CA:FALSE" \
        "keyUsage=digitalSignature,keyEncipherment" "extendedKeyUsage=$eku" >"$d/$n.ext"
    openssl x509 -req -in "$d/$n.csr" -CA "$d/ca.pem" -CAkey secrets/ca/ca-key.pem -CAserial secrets/ca/ca.srl -CAcreateserial \
        -days "${SPRIG_CERT_DAYS:-397}" -extfile "$d/$n.ext" -out "$d/$n.pem.new" 2>/dev/null
    rm -f "$d/$n.csr" "$d/$n.ext"
    chmod 644 "$d/$n.pem.new" "$d/$n-key.pem.new"
    # Key first, then certificate, each by rename: readers see whole files.
    mv -f "$d/$n-key.pem.new" "$d/$n-key.pem"
    mv -f "$d/$n.pem.new" "$d/$n.pem"
}

server_certs() {
    local extra=""
    [[ -n ${SPRIG_TLS_SANS:-} ]] && extra=",${SPRIG_TLS_SANS}"
    local pub="DNS:localhost,IP:127.0.0.1"
    if [[ $public_host =~ ^[0-9.]+$ ]]; then pub="$pub,IP:$public_host"; else pub="$pub,DNS:$public_host"; fi
    cert gateway "$public_host" "$pub$extra" serverAuth
    cert etcd etcd "DNS:etcd,DNS:localhost,IP:127.0.0.1" serverAuth,clientAuth
    cert mariadb mariadb "DNS:mariadb-1,DNS:mariadb-2,DNS:mariadb-3,DNS:localhost" serverAuth
}

init() {
    command -v openssl >/dev/null || die "needs openssl"
    [[ -e secrets/ca/ca-key.pem ]] && die "secrets/ exists already; it holds this deployment's CA and passwords (remove it only for a new deployment)"
    umask 077
    mkdir -p secrets/ca secrets/tls secrets/sprig secrets/mariadb backups
    # secrets/ stays 0700 (only this user reads it on the host); the files the
    # containers read are 0644 inside it, because each container runs as its
    # own uid (sprigdb 10001, mysql 999, etcd root).
    chmod 700 secrets
    chmod 755 secrets/tls secrets/sprig secrets/mariadb
    openssl req -x509 -newkey rsa:4096 -nodes -days 3650 -subj "/O=sprigdb/CN=SprigDB $dc CA" \
        -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -keyout secrets/ca/ca-key.pem -out secrets/tls/ca.pem 2>/dev/null
    chmod 644 secrets/tls/ca.pem
    server_certs
    cert client sprig-client "DNS:sprig-client" clientAuth
    (
        cd secrets
        rand >mariadb/root-password
        rand >mariadb/sprig_ctl-password
        rand >mariadb/sprig_repl-password
        rand >sprig/admin-token
        rand >sprig/gateway-token
        echo "1 $(rand)" >sprig/workload-keys
        cat >sprig/cluster-credentials.toml <<EOF
# Cluster credentials (chapter 27): sprigctl cluster register --credentials file:/run/sprig/sprig/cluster-credentials.toml
[sprig_ctl]
user = "sprig_ctl"
password = "$(cat mariadb/sprig_ctl-password)"
tls = "verify"
ca_file = "/run/sprig/tls/ca.pem"

[sprig_repl]
user = "sprig_repl"
password = "$(cat mariadb/sprig_repl-password)"
tls = "verify"
ca_file = "/run/sprig/tls/ca.pem"
EOF
        cat >sprig/backends.toml <<EOF
# The gateway's backend accounts (chapter 29): one MariaDB account per user,
# t<tenant>_u<user>, with passwords derived from backend_secret.
[per_user]
secret = "file:backends.toml#backend_secret"
tls = "verify"
ca_file = "/run/sprig/tls/ca.pem"

[backend_secret]
password = "$(rand)"
EOF
        chmod 644 mariadb/* sprig/*
    )
    say "secrets/ made: CA (secrets/ca, keep it offline after init), certificates for $public_host, passwords, tokens"
    say "back up secrets/ now, separately from the data backups (RUNBOOK.md, \"Backups\")"
}

# --- up and configure ----------------------------------------------------------------

set_dc() { ctl settings set --scope "dc:$dc" --reason "sprig-vm.sh configure" --yes "$1" "$2" >/dev/null; }

configure() {
    local list
    list=$(ctl -o json cluster list)
    if ! jq -e --arg n "$cluster_name" '.clusters[]? | select(.name == $n)' <<<"$list" >/dev/null; then
        say "registering $cluster_name (single, mariadb-1:3306)"
        ctl -o json cluster register "$cluster_name" --dc "$dc" --topology single --node mariadb-1:3306 \
            --credentials file:/run/sprig/sprig/cluster-credentials.toml \
            --memory-gb "${SPRIG_CLUSTER_MEMORY_GB:-4}" --storage-gb "${SPRIG_CLUSTER_STORAGE_GB:-100}" --yes |
            jq -c '{cluster: .cluster.cluster_id, state: .cluster.admin_state}'
    fi
    say "listener and TLS settings (dc $dc)"
    set_dc tls.bundles.default.cert_path /run/sprig/tls/gateway.pem
    set_dc tls.bundles.default.key_path /run/sprig/tls/gateway-key.pem
    set_dc tls.min_version "${SPRIG_TLS_MIN_VERSION:-1.2}"
    local l port
    for l in mysql:SPRIG_MYSQL_PORT:3306 mariadb:SPRIG_MARIADB_PORT:3307 https:SPRIG_HTTPS_PORT:8443 \
        mongo:SPRIG_MONGO_PORT:27017 redis:SPRIG_REDIS_PORT:6379 bolt:SPRIG_BOLT_PORT:7687 \
        kafka:SPRIG_KAFKA_PORT:9092 amqps:SPRIG_AMQPS_PORT:5671; do
        IFS=: read -r name var def <<<"$l"
        port=${!var:-$def}
        # The address first, then TLS, then enabled (agent brief rule 8g).
        set_dc "listener.$name.bind" "0.0.0.0:$port"
        set_dc "listener.$name.tls" required
        set_dc "listener.$name.enabled" true
    done
    # No plaintext listeners: plain HTTP and plain AMQP are off.
    set_dc listener.http.enabled false
    set_dc listener.amqp.enabled false
    set_dc kafka.advertised_host "$public_host"
    say "configured; listeners come up within a few seconds"
}

up() {
    [[ -f secrets/tls/ca.pem ]] || die "no secrets/: run ./sprig-vm.sh init first"
    mkdir -p backups
    dc_compose up -d --wait --wait-timeout "${SPRIG_UP_TIMEOUT:-300}"
    configure
}

status() {
    dc_compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}'
    ctl cluster list || true
}

tenant() {
    local t=${1:?tenant name} u=${2:?user name} pw
    IFS= read -r pw || die "the password comes on stdin"
    ctl tenant create "$t" --yes
    ctl tenant user add "$t" "$u" --password "$pw" --if-not-exists --yes
    say "tenant $t: log in as $t.$u"
}

# --- backups -----------------------------------------------------------------------------

backup() {
    local ts=${1:-$(date -u +%Y%m%dT%H%M%SZ)} d
    d=backups/$ts
    mkdir -p "$d"
    say "MariaDB: mariadb-backup into $d/mariadb-1"
    dc_compose exec -T mariadb-1 bash -c "set -e
        p=\$(cat /run/sprig/mariadb/root-password)
        mariadb-backup --backup --user=root --password=\"\$p\" --target-dir=/backups/$ts/mariadb-1 >/backups/$ts/mariadb-backup.log 2>&1
        mariadb-backup --prepare --target-dir=/backups/$ts/mariadb-1 >>/backups/$ts/mariadb-backup.log 2>&1"
    say "etcd: snapshot into $d/etcd.db"
    dc_compose exec -T etcd etcdctl --endpoints=https://127.0.0.1:2379 --cacert=/tls/ca.pem \
        --cert=/tls/client.pem --key=/tls/client-key.pem snapshot save "/backups/$ts/etcd.db" >"$d/etcd-snapshot.log" 2>&1
    dc_compose images --format json >"$d/images.json" 2>/dev/null || true
    grep -E '^SPRIGDB_IMAGE=' .env >"$d/image.env" || echo "SPRIGDB_IMAGE=${SPRIGDB_IMAGE:-sprigdb:dev}" >"$d/image.env"
    du -sh "$d" | awk -v ts="$ts" '{print "sprig-vm: backup " ts " done (" $1 ")"}'
}

restore() {
    local ts=${1:?backup timestamp (a directory under backups/)} d
    d=backups/$ts
    [[ -d $d/mariadb-1 && -f $d/etcd.db ]] || die "$d isn't a complete backup"
    local proj=${COMPOSE_PROJECT_NAME:-sprigdb}
    say "stopping SprigDB, MariaDB and etcd"
    dc_compose stop gateway worker controller mariadb-1 etcd
    say "MariaDB: copying $d/mariadb-1 back"
    dc_compose run --rm --no-deps -T -u root --entrypoint bash mariadb-1 -c "set -e
        find /var/lib/mysql -mindepth 1 -delete
        mariadb-backup --copy-back --target-dir=/backups/$ts/mariadb-1 --datadir=/var/lib/mysql >/backups/$ts/restore.log 2>&1
        chown -R mysql:mysql /var/lib/mysql"
    say "etcd: restoring $d/etcd.db"
    # The etcd image has no shell: clear the data directory from another one.
    docker run --rm -v "${proj}_etcd-data:/d" "${MARIADB_IMAGE:-mariadb:11.8}" bash -c 'rm -rf /d/data'
    dc_compose run --rm --no-deps -T --entrypoint etcdutl etcd snapshot restore "/backups/$ts/etcd.db" \
        --data-dir /etcd-data/data --name etcd-0 --initial-cluster etcd-0=https://etcd:2380 \
        --initial-advertise-peer-urls https://etcd:2380 >"$d/etcd-restore.log" 2>&1
    say "starting"
    dc_compose up -d --wait --wait-timeout "${SPRIG_UP_TIMEOUT:-300}"
    say "restored $ts"
}

# --- upgrades ----------------------------------------------------------------------------

set_image() {
    if grep -q '^SPRIGDB_IMAGE=' .env; then
        sed -i.bak "s|^SPRIGDB_IMAGE=.*|SPRIGDB_IMAGE=$1|" .env && rm -f .env.bak
    else
        echo "SPRIGDB_IMAGE=$1" >>.env
    fi
    export SPRIGDB_IMAGE=$1
}

# Controller first (it migrates the deployment's records), then the worker,
# then the gateway (it drains its sessions for gateway.drain_timeout_seconds).
roll() {
    local s
    for s in controller worker gateway; do
        say "$s -> $SPRIGDB_IMAGE"
        dc_compose up -d --no-deps --wait --wait-timeout "${SPRIG_UP_TIMEOUT:-300}" "$s"
    done
}

upgrade() {
    local img=${1:?image, for example sprigdb:1.2.0}
    docker image inspect "$img" >/dev/null 2>&1 || die "no image $img on this host (docker load or pull it first)"
    mkdir -p .state
    echo "${SPRIGDB_IMAGE:-sprigdb:dev}" >.state/previous-image
    backup "pre-upgrade-$(date -u +%Y%m%dT%H%M%SZ)"
    set_image "$img"
    roll
    say "upgraded to $img (previous: $(cat .state/previous-image); ./sprig-vm.sh rollback goes back)"
}

rollback() {
    [[ -f .state/previous-image ]] || die "no previous image recorded"
    local prev
    prev=$(cat .state/previous-image)
    set_image "$prev"
    roll
    say "rolled back to $prev"
}

# --- certificates ----------------------------------------------------------------------------

rotate_certs() {
    [[ -f secrets/ca/ca-key.pem ]] || die "the CA key isn't in secrets/ca (bring it back from where you keep it)"
    server_certs
    # The gateway re-reads its bundle within two polls; MariaDB on FLUSH SSL;
    # etcd on each new connection.
    dc_compose exec -T mariadb-1 bash -c 'mariadb -uroot -p"$(cat /run/sprig/mariadb/root-password)" -e "FLUSH SSL"'
    say "server certificates renewed (gateway, etcd, MariaDB)"
}

cmd=${1:-}
shift || true
case $cmd in
init) init ;;
up) up ;;
configure) configure ;;
status) status ;;
ctl) ctl "$@" ;;
tenant) tenant "$@" ;;
backup) backup "$@" ;;
restore) restore "$@" ;;
upgrade) upgrade "$@" ;;
rollback) rollback ;;
rotate-certs) rotate_certs ;;
down) dc_compose down ;;
*) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2 ;;
esac
