#!/usr/bin/env bash
# Operations for the SprigDB Compose-on-VM bundle (RUNBOOK.md has the
# procedures; chapter 27, "Compose on VMs").
#
#   ./sprig-vm.sh init                  make secrets/ (CA, certificates, passwords, tokens, backup key pair)
#   ./sprig-vm.sh check-secrets         check secrets/'s owners and modes (up runs it too)
#   ./sprig-vm.sh up-etcd               start only this VM's etcd member (several VMs: on each, first)
#   ./sprig-vm.sh up                    start everything, then configure (idempotent)
#   ./sprig-vm.sh configure             register the cluster, write listener and TLS settings
#   ./sprig-vm.sh status                containers, etcd members, clusters
#   ./sprig-vm.sh ctl <args>            sprigctl inside the controller (HTTPS, admin or ops token)
#   ./sprig-vm.sh tenant <name> <user>  create a tenant and a user (password on stdin)
#   ./sprig-vm.sh backup                every MariaDB here + an etcd snapshot, encrypted, into backups/<ts>/
#                                       (and to SPRIG_BACKUP_COPY_TO when set)
#   ./sprig-vm.sh verify-backup <ts> [dir]   check a backup's checksums (no key needed)
#   ./sprig-vm.sh restore <ts> [dir]    stop, put a backup back (from backups/ or dir/<ts>), start
#   ./sprig-vm.sh etcd-rejoin           replace this VM's etcd member (lost disk, or after a restore)
#   ./sprig-vm.sh upgrade <image>       backup, then roll this VM's controller, worker, gateway to <image>
#   ./sprig-vm.sh rollback              roll back to the image before the last upgrade
#   ./sprig-vm.sh rotate-certs          new server certificates from the same CA, live
#   ./sprig-vm.sh stop                  stop this VM's containers (data stays)
#   ./sprig-vm.sh down                  stop and remove this VM's containers (data stays in the volumes)
#
# Reads .env next to this file (copy .env.example). Needs docker compose v2,
# openssl and jq. Run as the user that owns secrets/ and may use docker.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$here"
say() { echo "sprig-vm: $*"; }
die() { echo "sprig-vm: $*" >&2; exit 1; }

# --- secrets checks (no .env needed) ---------------------------------------------------

# stat as "<uid> <octal mode> <type>", on Linux and macOS.
st() {
    if stat -c '%u %a %F' "$1" >/dev/null 2>&1; then stat -c '%u %a %F' "$1"
    else stat -f '%u %Lp %HT' "$1"; fi
}

# secrets/ must be this user's, closed to others (0700), with nothing
# inside that another user can change: every directory and file owned by
# this user (or root) and not group- or world-writable, no symbolic links.
# The certificates, keys and passwords the containers read are 0644 (each
# container reads them as its own uid), and the two offline keys, when
# present, are 0600. Prints each problem; returns 1 if there is one.
check_secrets() {
    local me bad=0 line uid mode type f
    me=$(id -u)
    [[ -d secrets ]] || { echo "  secrets/ is missing (./sprig-vm.sh init)"; return 1; }
    read -r uid mode type <<<"$(st secrets)"
    [[ $uid == "$me" ]] || { echo "  secrets/ is owned by uid $uid, not by you ($me): chown -R $me secrets"; bad=1; }
    (( (8#$mode & 8#077) == 0 )) || { echo "  secrets/ is mode $mode: chmod 700 secrets"; bad=1; }
    [[ -r secrets && -x secrets ]] || { echo "  you can't read secrets/"; return 1; }
    while IFS= read -r -d '' f; do
        if [[ -L $f ]]; then echo "  $f is a symbolic link: replace it with the file"; bad=1; continue; fi
        read -r uid mode type <<<"$(st "$f")"
        [[ $uid == "$me" || $uid == 0 ]] || { echo "  $f is owned by uid $uid: chown $me $f"; bad=1; }
        (( (8#$mode & 8#022) == 0 )) || { echo "  $f is writable by group or others (mode $mode): chmod go-w $f"; bad=1; }
        case $f in
        secrets/ca | secrets/ca/* | secrets/backup/backup-key.pem)
            [[ -d $f ]] && continue
            (( (8#$mode & 8#077) == 0 )) || { echo "  $f is an offline key and must be 0600 (mode $mode): chmod 600 $f"; bad=1; } ;;
        *)
            if [[ -f $f ]] && (( (8#$mode & 8#004) == 0 )); then
                echo "  $f isn't readable by the containers' users (mode $mode): chmod 644 $f"; bad=1
            fi ;;
        esac
    done < <(find secrets -mindepth 1 -print0)
    return $bad
}

secrets_ok() {
    local out
    if ! out=$(check_secrets); then
        echo "$out" >&2
        die "secrets/ failed its checks (above); fix them, then run this again"
    fi
    [[ -e secrets/ca/ca-key.pem ]] && say "note: the CA key is on this VM (secrets/ca); move it off after use"
    [[ -e secrets/backup/backup-key.pem ]] && say "note: the backups' private key is on this VM (secrets/backup/backup-key.pem); move it off after use"
    return 0
}

if [[ ${1:-} == check-secrets ]]; then
    if out=$(check_secrets); then echo "sprig-vm: secrets/ ok"; exit 0; else echo "$out"; exit 1; fi
fi

[[ -f .env ]] || die "no .env here; copy .env.example to .env first"
set -a
# shellcheck disable=SC1091
source ./.env
set +a
dc=${SPRIG_DC:-dc1}
public_host=${SPRIG_PUBLIC_HOST:-127.0.0.1}
cluster_name=${SPRIG_CLUSTER_NAME:-$dc-main}
admin_port=${SPRIG_ADMIN_PORT:-7480}
# COMPOSE_FILE in .env (compose.yaml:compose.ha.yaml, compose.gateway-vm.yaml,
# compose.yaml:compose.mariadb-2.yaml, ...) is honoured; otherwise compose.yaml.
dc_compose() {
    if [[ -n ${COMPOSE_FILE:-} ]]; then docker compose --project-directory "$here" "$@"
    else docker compose --project-directory "$here" -f "$here/compose.yaml" "$@"; fi
}
# This VM's services (read once; grep -q on a pipe would trip pipefail).
svc_list=
services() { [[ -n $svc_list ]] || svc_list=$(dc_compose config --services 2>/dev/null); echo "$svc_list"; }
has_service() { local l; l=$(services); grep -qx "$1" <<<"$l"; }
# The MariaDB containers of this VM: mariadb-1, mariadb-2, ...
mariadb_services() { local l; l=$(services); grep -E '^mariadb-[0-9]+$' <<<"$l" | sort -t- -k2 -n || true; }

# sprigctl in the controller container, over HTTPS (the Admin API's
# certificate is checked against the deployment CA). Before named operators
# take over it uses the bootstrap token; after, the service token RUNBOOK.md's
# "First operator" issues to this script (secrets/sprig/ops-token).
# SPRIGCTL_BIN runs a sprigctl on the host instead, against the published
# Admin port (a newer sprigctl than the image's, say).
ctl() {
    local tok=sprig/admin-token
    [[ -s secrets/sprig/ops-token ]] && tok=sprig/ops-token
    if [[ -n ${SPRIGCTL_BIN:-} ]]; then
        "$SPRIGCTL_BIN" --endpoint "https://127.0.0.1:$admin_port" --ca-file secrets/tls/ca.pem \
            --token-file "secrets/$tok" "$@"
    else
        dc_compose exec -T controller sprigctl --endpoint "https://127.0.0.1:$admin_port" \
            --ca-file /run/sprig/tls/ca.pem --token-file "/run/sprig/$tok" "$@"
    fi
}

# etcd: every member's client URL, from SPRIG_ETCD_SEEDS (a TOML list).
etcd_endpoints() {
    local s=${SPRIG_ETCD_SEEDS:-'["https://etcd:2379"]'}
    s=${s//[\[\]\" ]/}
    echo "$s"
}
# etcdctl against the whole cluster, from a one-off container on this VM's
# network (works while the local member is down).
etcdctl_all() {
    dc_compose run --rm --no-deps -T --entrypoint etcdctl etcd --endpoints="$(etcd_endpoints)" \
        --cacert=/tls/ca.pem --cert=/tls/client.pem --key=/tls/client-key.pem "$@"
}
# A shell with the sprigdb image, as root, for chores on volumes.
chore() {
    local vol=$1
    shift
    docker run --rm -u 0 --entrypoint bash -v "$vol:/d" "${SPRIGDB_IMAGE:-sprigdb:dev}" -c "$*"
}
project=${COMPOSE_PROJECT_NAME:-sprigdb}

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

# The names other VMs reach this deployment's internal services at
# (SPRIG_INTERNAL_SANS: every VM's private address), added to the etcd,
# MariaDB, controller and process certificates.
internal_sans() { [[ -n ${SPRIG_INTERNAL_SANS:-} ]] && echo ",${SPRIG_INTERNAL_SANS}" || true; }

server_certs() {
    local extra="" i
    [[ -n ${SPRIG_TLS_SANS:-} ]] && extra=",${SPRIG_TLS_SANS}"
    i=$(internal_sans)
    local pub="DNS:localhost,IP:127.0.0.1"
    if [[ $public_host =~ ^[0-9.]+$ ]]; then pub="$pub,IP:$public_host"; else pub="$pub,DNS:$public_host"; fi
    cert gateway "$public_host" "$pub$extra" serverAuth
    cert etcd etcd "DNS:etcd,DNS:localhost,IP:127.0.0.1$i" serverAuth,clientAuth
    cert mariadb mariadb "DNS:mariadb-1,DNS:mariadb-2,DNS:mariadb-3,DNS:localhost,IP:127.0.0.1$i" serverAuth
    cert controller controller "DNS:controller,DNS:localhost,IP:127.0.0.1$i" serverAuth
}

# Every SprigDB process's identity: etcd's client certificate, and the
# gateways' peer service (mutual TLS both ways), so server and client.
process_cert() {
    cert client sprig-client "DNS:sprig-client,DNS:gateway,DNS:controller,DNS:localhost,IP:127.0.0.1$(internal_sans)" serverAuth,clientAuth
}

# The backups' key pair: the public key encrypts each backup's data key on
# the VM; the private key opens them, and lives off the VM like the CA key.
backup_keys() {
    mkdir -p secrets/backup
    chmod 755 secrets/backup
    (umask 077 && openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out secrets/backup/backup-key.pem 2>/dev/null)
    openssl pkey -in secrets/backup/backup-key.pem -pubout -out secrets/backup/backup-pub.pem
    chmod 600 secrets/backup/backup-key.pem
    chmod 644 secrets/backup/backup-pub.pem
}

init() {
    command -v openssl >/dev/null || die "needs openssl"
    [[ -e secrets/ca/ca-key.pem || -e secrets/tls/ca.pem ]] &&
        die "secrets/ exists already; it holds this deployment's CA and passwords (remove it only for a new deployment)"
    umask 077
    mkdir -p secrets/ca secrets/tls secrets/sprig secrets/mariadb backups
    # secrets/ stays 0700 (only this user reads it on the host); the files the
    # containers read are 0644 inside it, because each container runs as its
    # own uid (sprigdb 10001, mysql 999, etcd root).
    chmod 700 secrets secrets/ca
    chmod 755 secrets/tls secrets/sprig secrets/mariadb
    openssl req -x509 -newkey rsa:4096 -nodes -days 3650 -subj "/O=sprigdb/CN=SprigDB $dc CA" \
        -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -keyout secrets/ca/ca-key.pem -out secrets/tls/ca.pem 2>/dev/null
    chmod 644 secrets/tls/ca.pem
    server_certs
    process_cert
    backup_keys
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
    chmod 600 secrets/ca/*
    say "secrets/ made: CA (secrets/ca), certificates for $public_host, passwords, tokens, the backups' key pair"
    say "now: back up secrets/ somewhere safe, then move secrets/ca and secrets/backup/backup-key.pem off this VM (RUNBOOK.md, \"Install\")"
}

# Bundles made before hardening round 2 lack the controller's certificate,
# the backups' key pair, and a process certificate that also serves (the
# peer service): make what is missing.
ensure_secrets() {
    if [[ ! -s secrets/tls/controller.pem ]] || ! openssl x509 -in secrets/tls/client.pem -noout -ext extendedKeyUsage 2>/dev/null | grep -q 'Server Auth'; then
        [[ -f secrets/ca/ca-key.pem ]] ||
            die "secrets/tls needs the controller certificate (HTTPS Admin API) and a server-capable client.pem: bring secrets/ca back for one run of up"
        [[ -s secrets/tls/controller.pem ]] || { say "making the controller's certificate (Admin API over HTTPS)"; cert controller controller "DNS:controller,DNS:localhost,IP:127.0.0.1$(internal_sans)" serverAuth; }
        openssl x509 -in secrets/tls/client.pem -noout -ext extendedKeyUsage 2>/dev/null | grep -q 'Server Auth' ||
            { say "re-issuing the process certificate (client.pem) for the peer service too"; process_cert; }
    fi
    if [[ ! -s secrets/backup/backup-pub.pem ]] && has_service mariadb-1; then
        say "making the backups' key pair (secrets/backup); move backup-key.pem off this VM"
        backup_keys
    fi
}

# --- up and configure ----------------------------------------------------------------

set_dc() { ctl settings set --scope "dc:$dc" --reason "sprig-vm.sh configure" --yes "$1" "$2" >/dev/null; }

configure() {
    has_service mariadb-1 || { say "no MariaDB on this VM: the first VM configures the DC"; return 0; }
    local list node=mariadb-1:3306
    # Several VMs: the other VMs' gateways reach the backend at this VM's
    # private address.
    [[ -n ${SPRIG_PRIVATE_HOST:-} ]] && node="$SPRIG_PRIVATE_HOST:${SPRIG_BACKEND_PORT:-3306}"
    list=$(ctl -o json cluster list)
    if ! jq -e --arg n "$cluster_name" '.clusters[]? | select(.name == $n)' <<<"$list" >/dev/null; then
        say "registering $cluster_name (single, $node)"
        ctl -o json cluster register "$cluster_name" --dc "$dc" --topology single --node "$node" \
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

wait_healthy() {
    local s=$1 i
    for i in $(seq 1 "${2:-120}"); do
        [[ $(docker inspect -f '{{.State.Health.Status}}' "$(dc_compose ps -q "$s")" 2>/dev/null) == healthy ]] && return 0
        sleep 2
    done
    return 1
}

up_etcd() {
    secrets_ok
    dc_compose up -d etcd
    say "etcd member ${SPRIG_ETCD_NAME:-etcd-0} started; it is healthy once a majority of members runs"
}

up() {
    [[ -f secrets/tls/ca.pem ]] || die "no secrets/: run ./sprig-vm.sh init first (other VMs: copy the first VM's secrets/)"
    ensure_secrets
    secrets_ok
    mkdir -p backups
    dc_compose up -d --wait --wait-timeout "${SPRIG_UP_TIMEOUT:-300}"
    configure
}

status() {
    dc_compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}'
    etcdctl_all endpoint status -w table 2>/dev/null || true
    ctl cluster list || true
}

tenant() {
    local t=${1:?tenant name} u=${2:?user name} pw
    IFS= read -r pw || die "the password comes on stdin"
    ctl tenant create "$t" --yes
    ctl tenant user add "$t" "$u" --password "$pw" --if-not-exists --yes
    say "tenant $t: log in as $t.$u"
}

# --- etcd members ------------------------------------------------------------------

# Replace this VM's member: remove it from the cluster (if listed), add it
# again, and start it with an empty data directory; it copies the data from
# the others. For a member that lost its disk, and on the other VMs after a
# restore (RUNBOOK.md, "Restore").
etcd_rejoin() {
    local name=${SPRIG_ETCD_NAME:?SPRIG_ETCD_NAME} host=${SPRIG_PRIVATE_HOST:?SPRIG_PRIVATE_HOST}
    local peer="https://$host:${SPRIG_ETCD_PEER_PORT:-2380}" id out ic
    say "stopping this VM's etcd member ($name)"
    dc_compose stop etcd >/dev/null 2>&1 || true
    id=$(etcdctl_all member list | awk -F', ' -v n="$name" -v p="$peer" '$3 == n || $4 == p {print $1}')
    if [[ -n $id ]]; then
        say "removing member $id ($name) from the cluster"
        etcdctl_all member remove "$id" >/dev/null
    fi
    out=$(etcdctl_all member add "$name" --peer-urls="$peer")
    ic=$(sed -n 's/^ETCD_INITIAL_CLUSTER="\(.*\)"$/\1/p' <<<"$out")
    [[ -n $ic ]] || die "member add didn't say the initial cluster: $out"
    say "emptying $name's data directory"
    chore "${project}_etcd-data" 'rm -rf /d/data'
    SPRIG_ETCD_INITIAL_CLUSTER=$ic SPRIG_ETCD_CLUSTER_STATE=existing dc_compose up -d --no-deps --force-recreate etcd
    wait_healthy etcd 90 || die "etcd member $name didn't become healthy (docker compose logs etcd)"
    etcdctl_all member list -w table
    say "member $name rejoined"
}

# --- backups -----------------------------------------------------------------------------

# Each backup gets a fresh 256-bit data key, made on the host and sealed
# with secrets/backup/backup-pub.pem (RSA-OAEP, SHA-256) into data-key.enc.
# Each file is streamed straight from mariadb-backup / etcd through zstd and
# AES-256-CTR inside the MariaDB container (the key reaches it on stdin, and
# stays in its memory), so no unencrypted copy is written to the disk, except
# etcd's snapshot for the second it takes to encrypt it. SHA256SUMS covers
# every file.
backup() {
    local ts=${1:-$(date -u +%Y%m%dT%H%M%SZ)} d s uidgid enc_svc k
    d=backups/$ts
    uidgid="$(id -u):$(id -g)"
    enc_svc=$(mariadb_services | head -1)
    [[ -n $enc_svc ]] || die "no MariaDB on this VM: backups run on the VM with MariaDB (it snapshots the whole etcd cluster too)"
    [[ -s secrets/backup/backup-pub.pem ]] || die "no secrets/backup/backup-pub.pem (./sprig-vm.sh up makes one)"
    mkdir -p "$d"
    k=$(openssl rand -hex 32)
    printf '%s' "$k" | openssl pkeyutl -encrypt -pubin -inkey secrets/backup/backup-pub.pem \
        -pkeyopt rsa_padding_mode:oaep -pkeyopt rsa_oaep_md:sha256 -out "$d/data-key.enc"
    for s in $(mariadb_services); do
        say "MariaDB $s: mariadb-backup, compressed and encrypted, into $d/$s.xbstream.zst.enc"
        printf '%s' "$k" | dc_compose exec -T "$s" bash -c "set -euo pipefail; umask 077
            cat >/dev/shm/sprig-backup-key
            trap 'rm -f /dev/shm/sprig-backup-key' EXIT
            p=\$(cat /run/sprig/mariadb/root-password)
            mariadb-backup --backup --stream=xbstream --user=root --password=\"\$p\" --target-dir=/tmp 2>/backups/$ts/$s.log |
                zstd -q -T2 |
                openssl enc -aes-256-ctr -pbkdf2 -salt -pass file:/dev/shm/sprig-backup-key -out /backups/$ts/$s.xbstream.zst.enc
            grep -q 'completed OK' /backups/$ts/$s.log"
    done
    if has_service etcd; then
        say "etcd: snapshot of the cluster, encrypted, into $d/etcd.db.zst.enc"
        dc_compose exec -T etcd etcdctl --endpoints="https://127.0.0.1:${SPRIG_ETCD_CLIENT_PORT:-2379}" --cacert=/tls/ca.pem \
            --cert=/tls/client.pem --key=/tls/client-key.pem snapshot save "/backups/$ts/.etcd.db.part" >"$d/etcd-snapshot.log" 2>&1
        # etcdctl can only write a file: encrypt it at once and remove it.
        printf '%s' "$k" | dc_compose exec -T "$enc_svc" bash -c "set -euo pipefail; umask 077
            cat >/dev/shm/sprig-backup-key
            trap 'rm -f /dev/shm/sprig-backup-key' EXIT
            zstd -q </backups/$ts/.etcd.db.part |
                openssl enc -aes-256-ctr -pbkdf2 -salt -pass file:/dev/shm/sprig-backup-key -out /backups/$ts/etcd.db.zst.enc
            shred -u /backups/$ts/.etcd.db.part 2>/dev/null || rm -f /backups/$ts/.etcd.db.part"
    fi
    k=
    dc_compose images --format json >"$d/images.json" 2>/dev/null || true
    grep -E '^SPRIGDB_IMAGE=' .env >"$d/image.env" || echo "SPRIGDB_IMAGE=${SPRIGDB_IMAGE:-sprigdb:dev}" >"$d/image.env"
    {
        echo "sprigdb-backup v2"
        echo "created $ts"
        echo "dc $dc"
        echo "mariadb $(mariadb_services | tr '\n' ' ')"
        echo "encryption aes-256-ctr, pbkdf2 (openssl enc); data key rsa-oaep-sha256 (data-key.enc)"
    } >"$d/MANIFEST"
    dc_compose exec -T "$enc_svc" bash -c "cd /backups/$ts && chown -R $uidgid . && chmod 600 *.enc *.log"
    (cd "$d" && sums_make)
    du -sh "$d" | awk -v ts="$ts" '{print "sprig-vm: backup " ts " done (" $1 ", encrypted)"}'
    copy_off "$ts"
    prune
}

# SPRIG_BACKUP_COPY_TO: a directory (a mounted disk or network share) or
# host:path (rsync over SSH). Only the encrypted files leave the VM; the
# copy is checked against SHA256SUMS.
copy_off() {
    local ts=$1 to=${SPRIG_BACKUP_COPY_TO:-}
    [[ -n $to ]] || { say "SPRIG_BACKUP_COPY_TO isn't set: the backup stays on this VM only"; return 0; }
    local files=(MANIFEST SHA256SUMS image.env)
    local f
    for f in backups/"$ts"/*.enc; do files+=("$(basename "$f")"); done
    if [[ $to == /* ]]; then
        mkdir -p "$to/$ts.part"
        (cd "backups/$ts" && cp -p "${files[@]}" "$to/$ts.part/")
        (cd "$to/$ts.part" && sums_check) || die "the copy in $to/$ts.part doesn't match SHA256SUMS"
        rm -rf "${to:?}/$ts"
        mv "$to/$ts.part" "$to/$ts"
    else
        command -v rsync >/dev/null || die "SPRIG_BACKUP_COPY_TO=$to needs rsync"
        (cd "backups/$ts" && rsync -a --partial -- "${files[@]}" "$to/$ts/")
    fi
    say "copied to $to/$ts"
}

sums_make() {
    if command -v sha256sum >/dev/null; then sha256sum MANIFEST image.env ./*.enc >SHA256SUMS
    else shasum -a 256 MANIFEST image.env ./*.enc >SHA256SUMS; fi
}

sums_check() {
    if command -v sha256sum >/dev/null; then sha256sum -c --quiet SHA256SUMS
    else shasum -a 256 -c --quiet SHA256SUMS; fi
}

# Keep the newest SPRIG_BACKUP_KEEP (7) backups in backups/; older ones that
# this script made (they have a MANIFEST) are removed. The off-VM copies are
# pruned by the storage's own retention.
prune() {
    local keep=${SPRIG_BACKUP_KEEP:-7} x
    for x in $(cd backups && for m in */MANIFEST; do [[ -f $m ]] && dirname "$m"; done | sort |
        awk -v k="$keep" '{a[NR] = $0} END {for (i = 1; i <= NR - k; i++) print a[i]}'); do
        say "removing old backup backups/$x"
        rm -rf "backups/${x:?}"
    done
}

verify_backup() {
    local ts=${1:?backup timestamp} from=${2:-backups}
    [[ -f $from/$ts/SHA256SUMS ]] || die "$from/$ts has no SHA256SUMS"
    (cd "$from/$ts" && sums_check) || die "$from/$ts doesn't match its SHA256SUMS"
    say "$from/$ts: every file matches SHA256SUMS"
}

# The private key comes back for a restore: SPRIG_BACKUP_KEY or
# secrets/backup/backup-key.pem. Only the small data key is decrypted on
# the host; it reaches the containers on stdin.
data_key() {
    local d=$1 key=${SPRIG_BACKUP_KEY:-secrets/backup/backup-key.pem}
    [[ -f $key ]] || die "restoring needs the backups' private key: put it at secrets/backup/backup-key.pem (mode 0600) or set SPRIG_BACKUP_KEY"
    openssl pkeyutl -decrypt -inkey "$key" -pkeyopt rsa_padding_mode:oaep -pkeyopt rsa_oaep_md:sha256 -in "$d/data-key.enc"
}

restore() {
    local ts=${1:?backup timestamp (a directory under backups/, or under the second argument)} from=${2:-} d s k
    d=backups/$ts
    if [[ -n $from ]]; then
        [[ -d $from/$ts ]] || die "no $from/$ts"
        say "fetching $from/$ts into $d"
        mkdir -p "$d"
        cp -p "$from/$ts"/* "$d/"
    fi
    [[ -f $d/MANIFEST && -f $d/data-key.enc ]] || die "$d isn't a complete encrypted backup"
    verify_backup "$ts"
    k=$(data_key "$d") || die "the private key doesn't open $d/data-key.enc"
    local dbs=() inbak=()
    read -r -a dbs <<<"$(mariadb_services | tr '\n' ' ')"
    for s in "${dbs[@]}"; do [[ -f $d/$s.xbstream.zst.enc ]] || die "$d has no copy of $s"; inbak+=("$s"); done
    say "stopping SprigDB, MariaDB and etcd on this VM (stop the other VMs' SprigDB first: RUNBOOK.md, \"Restore\")"
    dc_compose stop gateway worker controller "${dbs[@]}" etcd 2>/dev/null || dc_compose stop
    for s in "${inbak[@]}"; do
        say "MariaDB $s: decrypting, unpacking and preparing $d/$s.xbstream.zst.enc"
        printf '%s' "$k" | dc_compose run --rm --no-deps -T -u root --entrypoint bash "$s" -c "set -euo pipefail; umask 077
            cat >/dev/shm/k
            find /var/lib/mysql -mindepth 1 -delete
            openssl enc -d -aes-256-ctr -pbkdf2 -pass file:/dev/shm/k -in /backups/$ts/$s.xbstream.zst.enc | zstd -dq | mbstream -x -C /var/lib/mysql
            rm -f /dev/shm/k
            mariadb-backup --prepare --target-dir=/var/lib/mysql >/backups/$ts/$s-restore.log 2>&1
            chown -R mysql:mysql /var/lib/mysql"
    done
    if has_service etcd && [[ -f $d/etcd.db.zst.enc ]]; then
        local name=${SPRIG_ETCD_NAME:-etcd-0} peer
        peer="https://${SPRIG_PRIVATE_HOST:-etcd}:${SPRIG_ETCD_PEER_PORT:-2380}"
        say "etcd: restoring the snapshot as a one-member cluster ($name); the other VMs rejoin with etcd-rejoin"
        printf '%s' "$k" | dc_compose run --rm --no-deps -T -u root --entrypoint bash "${inbak[0]}" -c "set -euo pipefail; umask 077
            cat >/dev/shm/k
            openssl enc -d -aes-256-ctr -pbkdf2 -pass file:/dev/shm/k -in /backups/$ts/etcd.db.zst.enc | zstd -dq >/backups/$ts/.etcd.db.part
            rm -f /dev/shm/k"
        chore "${project}_etcd-data" 'rm -rf /d/data'
        dc_compose run --rm --no-deps -T --entrypoint etcdutl etcd snapshot restore "/backups/$ts/.etcd.db.part" \
            --data-dir /etcd-data/data --name "$name" --initial-cluster "$name=$peer" \
            --initial-cluster-token "${SPRIG_DEPLOYMENT_ID:-sprigdb}-etcd" \
            --initial-advertise-peer-urls "$peer" >"$d/etcd-restore.log" 2>&1
        rm -f "$d/.etcd.db.part"
    fi
    k=
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
# Only the roles this VM runs.
roll() {
    local s
    for s in controller worker gateway; do
        has_service "$s" || continue
        say "$s -> $SPRIGDB_IMAGE"
        dc_compose up -d --no-deps --wait --wait-timeout "${SPRIG_UP_TIMEOUT:-300}" "$s"
    done
}

upgrade() {
    local img=${1:?image, for example sprigdb:1.2.0}
    docker image inspect "$img" >/dev/null 2>&1 || die "no image $img on this host (docker load or pull it first)"
    mkdir -p .state
    echo "${SPRIGDB_IMAGE:-sprigdb:dev}" >.state/previous-image
    if [[ -n $(mariadb_services) ]]; then backup "pre-upgrade-$(date -u +%Y%m%dT%H%M%SZ)"
    else say "no MariaDB on this VM: the first VM's upgrade took the backup"; fi
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
    # etcd on each new connection; the controller's Admin API on restart.
    local s
    for s in $(mariadb_services); do
        dc_compose exec -T "$s" bash -c 'mariadb -uroot -p"$(cat /run/sprig/mariadb/root-password)" -e "FLUSH SSL"'
    done
    say "server certificates renewed (gateway, etcd, MariaDB, controller); copy secrets/tls to the other VMs"
}

cmd=${1:-}
shift || true
case $cmd in
init) init ;;
up-etcd) up_etcd ;;
up) up ;;
configure) configure ;;
status) status ;;
ctl) ctl "$@" ;;
tenant) tenant "$@" ;;
backup) backup "$@" ;;
verify-backup) verify_backup "$@" ;;
restore) restore "$@" ;;
etcd-rejoin) etcd_rejoin ;;
upgrade) upgrade "$@" ;;
rollback) rollback ;;
rotate-certs) rotate_certs ;;
stop) dc_compose stop ;;
down) dc_compose down ;;
*) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2 ;;
esac
