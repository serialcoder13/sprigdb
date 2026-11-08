#!/usr/bin/env bash
# Runs a controller, a worker and a gateway on the host against the Compose stack
# (milestones M0.2 and M0.3), so real MySQL and MariaDB clients can log in to
# the gateway and run statements on a registered cluster.
#
# Two login modes (SPRIG_GATEWAY_LOGIN_MODE):
#
# - catalog (the default, M0.3): tenants with their own logical databases.
#   up creates the tenants acme and globex on dc-a-single (dc-a-c1), each
#   with the user app (password sprig-dev-app, also a mysql_native_password
#   verifier, with auth.mysql_native_password on for both tenants): log in as
#   acme.app or globex.app. Their sessions run as per-user backend accounts
#   (t<tenant>_u<user>, chapter 29, [per_user] in gateway-backends.dev.toml),
#   granted only on the tenant's own schemas; DDL creating
#   databases and stored programs runs as the cluster's sprig_ctl account.
#   down drops the two tenants' databases through the gateway (needs the
#   mysql client) and deletes the tenants.
# - accounts_file (M0.2): the logins app, app_repl and app_galera of
#   gateway-accounts.dev.toml on the three raw clusters, names unchanged.
#   The connector matrix runs against this mode.
#
#   deploy/compose/gateway-dev.sh up       # build, start, register, configure
#   deploy/compose/gateway-dev.sh status
#   deploy/compose/gateway-dev.sh restart  # restart the processes, keeping tenants
#   deploy/compose/gateway-dev.sh tls required|optional|off
#   deploy/compose/gateway-dev.sh down     # stop, and undo what up wrote to etcd
#
# Needs the Compose stack (`docker compose --profile all up -d --wait`),
# curl, jq and openssl. Runs from anywhere: it works from the repository
# root. Processes, logs and what up created live in deploy/compose/.gateway-dev/
# (git-ignored); the dev certificates in deploy/compose/gateway-certs/.
# Re-running up leaves running processes alone, so after a code change run
# down and up again.
#
# Listeners (decision D6): MySQL persona 127.0.0.1:13306, MariaDB persona
# 127.0.0.1:13307. They're live settings in etcd, not config, so up writes
# them at dc scope. TLS is `optional` unless SPRIG_GATEWAY_TLS says otherwise:
# the connector matrix runs every connector with TLS required and then
# disabled, and sysbench only speaks plaintext, so both must log in. A client
# that asks for TLS still gets it. `tls required` switches both listeners
# live (the gateway's own default); `tls optional` switches back.
#
# Variables: SPRIG_GATEWAY_LOGIN_MODE (catalog), SPRIG_GATEWAY_TLS (optional),
# CARGO_TARGET_DIR (target),
# SPRIG_GATEWAY_PROFILE (debug; release for latency and sysbench numbers), and,
# to run a second instance next to the default one (say against your own
# etcd), SPRIG_GATEWAY_DEV_STATE (deploy/compose/.gateway-dev),
# SPRIG_ETCD (http://127.0.0.1:2379), SPRIG_GATEWAY_MYSQL_PORT (13306),
# SPRIG_GATEWAY_MARIADB_PORT (13307), SPRIG_ADMIN_PORT (7480),
# SPRIG_CTL_TELEMETRY_PORT (9180) and SPRIG_GW_TELEMETRY_PORT (9190). up
# records them in the state directory, so status and down need only
# SPRIG_GATEWAY_DEV_STATE; while an instance is up, its recorded values win.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
root=$(pwd)

state=${SPRIG_GATEWAY_DEV_STATE:-deploy/compose/.gateway-dev}
state=${state%/}
# The values of the instance that is up, written by up.
if [[ -f $state/env ]]; then
    # shellcheck disable=SC1091 # written by up
    source "$state/env"
fi
etcd=${etcd:-${SPRIG_ETCD:-http://127.0.0.1:2379}}
mysql_port=${mysql_port:-${SPRIG_GATEWAY_MYSQL_PORT:-13306}}
mariadb_port=${mariadb_port:-${SPRIG_GATEWAY_MARIADB_PORT:-13307}}
admin_port=${admin_port:-${SPRIG_ADMIN_PORT:-7480}}
ctl_telemetry_port=${ctl_telemetry_port:-${SPRIG_CTL_TELEMETRY_PORT:-9180}}
gw_telemetry_port=${gw_telemetry_port:-${SPRIG_GW_TELEMETRY_PORT:-9190}}
wk_telemetry_port=${wk_telemetry_port:-${SPRIG_WK_TELEMETRY_PORT:-9185}}
login_mode=${login_mode:-${SPRIG_GATEWAY_LOGIN_MODE:-catalog}}
case $login_mode in catalog | accounts_file) ;; *) echo "gateway-dev: SPRIG_GATEWAY_LOGIN_MODE must be catalog or accounts_file" >&2; exit 1 ;; esac
# The tenants catalog mode creates, each with the user app.
tenants=(acme globex)
tenant_password=sprig-dev-app
dc="dc-a"
certs=deploy/compose/gateway-certs
# status and down use the binaries up built, whatever CARGO_TARGET_DIR is then.
profile=${SPRIG_GATEWAY_PROFILE:-debug}
case $profile in debug | release) ;; *) echo "gateway-dev: SPRIG_GATEWAY_PROFILE must be debug or release" >&2; exit 1 ;; esac
bin=$(cat "$state/bin" 2>/dev/null || echo "${CARGO_TARGET_DIR:-target}/$profile")
# The Admin API and the two roles' telemetry. sprigdb.dev.toml and
# sprigdb.gateway.dev.toml hold the defaults; start passes the values above,
# with the etcd endpoint, as SPRIGDB_* overrides.
admin=http://127.0.0.1:$admin_port
ctl_ready=http://127.0.0.1:$ctl_telemetry_port/readyz
gw_ready=http://127.0.0.1:$gw_telemetry_port/readyz
wk_ready=http://127.0.0.1:$wk_telemetry_port/readyz

# Registration order fixes the ids (numbers are allocated max + 1 across the
# deployment), and gateway-accounts.dev.toml names these ids:
# name|topology|ports|expected id.
clusters=(
    "dc-a-single|single|13301|dc-a-c1"
    "dc-a-repl|primary_replica|13311,13312,13313|dc-a-c2"
    "dc-a-galera|galera|13321,13322,13323|dc-a-c3"
)

# Every setting up writes, so down can remove them. The HTTP listeners are
# switched off: the gateway has no HTTP front end yet, and 8080 is often
# taken on development machines.
settings=(
    listener.mysql.bind listener.mariadb.bind
    listener.mysql.tls listener.mariadb.tls
    tls.bundles.default.cert_path tls.bundles.default.key_path
    listener.http.enabled listener.https.enabled
    workload.flush_seconds
)

say() { echo "gateway-dev: $*"; }
die() {
    echo "gateway-dev: $*" >&2
    exit 1
}

# --- etcd, through its JSON gateway (no etcdctl needed) ----------------------

b64() { printf '%s' "$1" | base64 | tr -d '\n'; }

kv() { curl -fsS -X POST "$etcd/v3/kv/$1" -d "$2"; }

# The first key after every key that starts with $1.
prefix_end() {
    local p=$1 last
    last=$(printf '%d' "'${p: -1}")
    printf '%s' "${p%?}"
    # shellcheck disable=SC2059 # the octal escape is the format
    printf "\\$(printf '%03o' $((last + 1)))"
}

etcd_get() {
    kv range "$(jq -cn --arg k "$(b64 "$1")" '{key: $k}')" | jq -r '.kvs[0].value // empty | @base64d'
}

etcd_put() {
    kv put "$(jq -cn --arg k "$(b64 "$1")" --arg v "$(b64 "$2")" '{key: $k, value: $v}')" >/dev/null
}

etcd_del() {
    kv deleterange "$(jq -cn --arg k "$(b64 "$1")" '{key: $k}')" | jq -r '.deleted // "0"'
}

etcd_del_prefix() {
    kv deleterange "$(jq -cn --arg k "$(b64 "$1")" --arg e "$(b64 "$(prefix_end "$1")")" \
        '{key: $k, range_end: $e}')" | jq -r '.deleted // "0"'
}

# The kvs under a prefix, as the range call's JSON.
etcd_range() {
    kv range "$(jq -cn --arg k "$(b64 "$1")" --arg e "$(b64 "$(prefix_end "$1")")" \
        '{key: $k, range_end: $e}')"
}

# A setting record at dc scope (chapter 23, "Setting records"); $2 is the
# value as JSON.
set_setting() {
    etcd_put "/sprig/dc/$dc/settings/$1" "$(jq -cn --argjson v "$2" \
        '{v: 1, value: $v, set_by: "op:gateway-dev", reason: "deploy/compose/gateway-dev.sh"}')"
}

get_setting() {
    etcd_get "/sprig/dc/$dc/settings/$1" | jq -c '.value' 2>/dev/null || true
}

# --- processes ---------------------------------------------------------------

pid_of() { cat "$state/$1.pid" 2>/dev/null || true; }

# Alive and still ours: a stale pid file can name a recycled pid.
running() {
    local pid
    pid=$(pid_of "$1")
    [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null &&
        ps -p "$pid" -o command= | grep -q "sprigdb"
}

start() {
    local name=$1 config=$2 role=$3
    if running "$name"; then
        say "$name already running (pid $(pid_of "$name"))"
        return
    fi
    # One previous log is kept; a trace-level gateway log grows fast.
    if [[ -f $state/$name.log ]]; then mv -f "$state/$name.log" "$state/$name.log.1"; fi
    local telemetry=$ctl_telemetry_port extra=()
    if [[ $role == gateway ]]; then telemetry=$gw_telemetry_port; fi
    # The worker reads the controller's config under its own node id.
    if [[ $role == worker ]]; then telemetry=$wk_telemetry_port; extra=(SPRIGDB_NODE_ID=wkr-1); fi
    env ${extra[@]+"${extra[@]}"} SPRIGDB_META_DC_SEEDS="[\"$etcd\"]" \
        SPRIGDB_GATEWAY_LOGIN_MODE="$login_mode" \
        SPRIGDB_CONTROLLER_ADMIN_ADDR="127.0.0.1:$admin_port" \
        SPRIGDB_TELEMETRY_BOOTSTRAP_ADDR="127.0.0.1:$telemetry" \
        nohup "$bin/sprigdb" --config "$config" "$role" >"$state/$name.log" 2>&1 </dev/null &
    echo $! >"$state/$name.pid"
    say "$name started (pid $!, log $state/$name.log)"
}

wait_ready() {
    local name=$1 url=$2
    for _ in $(seq 1 120); do
        curl -fsS -o /dev/null "$url" 2>/dev/null && return
        if ! running "$name"; then
            tail -n 30 "$state/$name.log" >&2
            die "$name exited; log above"
        fi
        sleep 0.5
    done
    die "$name not ready after 60 s; see $state/$name.log"
}

listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

stop() {
    local name=$1 pid
    if ! running "$name"; then
        rm -f "$state/$name.pid"
        return
    fi
    pid=$(pid_of "$name")
    kill -TERM "$pid"
    # The gateway drains for up to gateway.drain_timeout_seconds (30).
    for _ in $(seq 1 80); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
        say "$name didn't stop in 40 s; killing it"
        kill -KILL "$pid"
    fi
    rm -f "$state/$name.pid"
    say "$name stopped"
}

ctl() { "$bin/sprigctl" --endpoint "$admin" "$@"; }

# --- dev certificates ---------------------------------------------------------

# A self-signed CA and a server certificate for localhost, 127.0.0.1 and
# host.docker.internal (the connector containers' name for the host), made
# once. Clients that verify use ca.pem.
make_certs() {
    if [[ -s $certs/server.pem && -s $certs/server-key.pem ]] &&
        openssl x509 -checkend 86400 -noout -in "$certs/server.pem" >/dev/null; then
        return
    fi
    say "making development certificates in $certs/"
    mkdir -p "$certs"
    (
        cd "$certs"
        umask 077
        openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=SprigDB dev CA" \
            -addext "basicConstraints=critical,CA:TRUE" \
            -addext "keyUsage=critical,keyCertSign,cRLSign" \
            -keyout ca-key.pem -out ca.pem 2>/dev/null
        openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
            -keyout server-key.pem -out server.csr 2>/dev/null
        printf '%s\n' "subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:host.docker.internal" \
            "basicConstraints=CA:FALSE" "keyUsage=digitalSignature,keyEncipherment" \
            "extendedKeyUsage=serverAuth" >server.ext
        openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
            -days 825 -extfile server.ext -out server.pem 2>/dev/null
        rm -f server.csr server.ext ca.srl
        chmod 644 ca.pem server.pem
    )
}

# --- clusters ------------------------------------------------------------------

# Registers the three Compose clusters unless they are, and checks they got
# the ids the accounts file names. Checks before registering anything, so a
# wrong order leaves etcd as it was.
register_clusters() {
    local list max next name topology ports want have
    list=$(ctl -o json cluster list)
    max=$(etcd_range /sprig/g/cluster-ids/ |
        jq '[.kvs[]? | .key | @base64d | split("/") | last | tonumber] | max // 0')
    next=$max
    local todo=()
    for c in "${clusters[@]}"; do
        IFS='|' read -r name topology ports want <<<"$c"
        have=$(jq -r --arg n "$name" '.clusters[] | select(.name == $n) | .cluster_id' <<<"$list")
        if [[ -n $have ]]; then
            [[ $have == "$want" ]] || die "$name is registered as $have, but gateway-accounts.dev.toml expects $want.
  Run '$0 down' (it removes clusters up registered), or remove the registrations by hand."
            continue
        fi
        next=$((next + 1))
        [[ $dc-c$next == "$want" ]] || die "$name would be registered as $dc-c$next, but gateway-accounts.dev.toml expects $want:
  cluster numbers are allocated max + 1 across the deployment, and /sprig/g/cluster-ids/ already goes up to $max.
  Remove the other registrations (or run '$0 down' if this script made them) and try again."
        todo+=("$c")
    done

    # The credentials the controller probes with: compose.yaml's root.
    (umask 077 && cat >"$state/cluster-credentials.toml") <<'TOML'
# Written by gateway-dev.sh: compose.yaml's MariaDB root, development only.
[sprig_ctl]
user = "root"
password = "sprig-dev-root"
tls = "skip_verify"
[sprig_repl]
user = "root"
password = "sprig-dev-root"
tls = "skip_verify"
TOML

    for c in ${todo[@]+"${todo[@]}"}; do
        IFS='|' read -r name topology ports want <<<"$c"
        local args=(cluster register "$name" --topology "$topology"
            --credentials "file:$root/$state/cluster-credentials.toml")
        IFS=',' read -ra port_list <<<"$ports"
        for p in "${port_list[@]}"; do args+=(--node "127.0.0.1:$p"); done
        say "registering $name ($topology)"
        local out
        out=$(ctl -o json "${args[@]}") || die "sprigctl cluster register $name failed: $out"
        have=$(jq -r '.cluster.cluster_id // empty' <<<"$out")
        [[ -n $have ]] || die "$name wasn't registered: $(jq -c '.precheck' <<<"$out")"
        echo "$have $(jq -r '.cluster.cluster_number' <<<"$out")" >>"$state/clusters"
        [[ $have == "$want" ]] || die "$name got $have, but gateway-accounts.dev.toml expects $want"
        say "$name is $have ($(jq -r '.cluster.admin_state' <<<"$out"))"
    done
}

# --- tenants (catalog mode) -------------------------------------------------------

# Creates the tenants and their users unless they exist (both calls are
# idempotent), and turns on mysql_native_password for them, so plaintext
# clients without the caching_sha2_password fast cache can log in too.
provision_tenants() {
    local t out
    for t in "${tenants[@]}"; do
        out=$(ctl tenant create "$t" --cluster dc-a-c1 2>&1) || die "sprigctl tenant create $t failed: $out"
        local id
        id=$(ctl -o json tenant show "$t" | jq -r '.tenant_id')
        grep -q "^$t " "$state/tenants" 2>/dev/null || echo "$t $id" >>"$state/tenants"
        out=$(ctl tenant user add "$t" app --password "$tenant_password" --native --if-not-exists 2>&1) ||
            die "sprigctl tenant user add $t app failed: $out"
        out=$(ctl settings set auth.mysql_native_password true --scope "tenant:$t" \
            --reason "deploy/compose/gateway-dev.sh" 2>&1) ||
            die "enabling mysql_native_password for $t failed: $out"
        say "tenant $t ready: log in as $t.app"
    done
}

# Drops the tenants' databases through the gateway, then deletes them.
remove_tenants() {
    [[ -f $state/tenants ]] || return 0
    local t dbs db q
    if ! command -v mysql >/dev/null; then
        say "no mysql client: tenants $(cut -d' ' -f1 "$state/tenants" | tr '\n' ' ')left in place (drop their databases, then sprigctl tenant delete)"
        return 0
    fi
    q='`'
    while read -r t _id; do
        [[ -n $t ]] || continue
        local my=(mysql -h 127.0.0.1 -P "$mysql_port" -u "$t.app" "-p$tenant_password" --ssl-mode=REQUIRED -N -B)
        # A few passes: a database another one's foreign keys point into
        # can only go after it.
        local pass left
        for pass in 1 2 3 4; do
            if ! dbs=$("${my[@]}" -e "SHOW DATABASES" 2>/dev/null); then
                say "couldn't log in as $t.app to drop its databases"
                break
            fi
            left=0
            while read -r db; do
                case $db in information_schema | performance_schema | mysql | sys | "") continue ;; esac
                if ! "${my[@]}" -e "DROP DATABASE $q${db//$q/$q$q}$q" >/dev/null 2>&1; then
                    left=$((left + 1))
                    [[ $pass == 4 ]] && say "couldn't drop $t's database $db"
                fi
            done <<<"$dbs"
            [[ $left == 0 ]] && break
        done
        if ! ctl tenant delete "$t" --yes >/dev/null 2>&1; then
            say "tenant $t not deleted (databases left?)"
            continue
        fi
        # Cleanup (dropping the catalog) may finish in the controller's loop:
        # wait for the entry to go before the controller stops.
        local gone=false
        for _ in $(seq 1 40); do
            if ! ctl tenant show "$t" >/dev/null 2>&1; then
                gone=true
                break
            fi
            sleep 0.5
        done
        if $gone; then
            say "tenant $t deleted"
            sed -i.bak "/^$t /d" "$state/tenants" && rm -f "$state/tenants.bak"
        else
            say "tenant $t is still deleting: its t${_id:-<id>}_* schemas stay (later tenants get higher ids and never adopt them); drop them by hand"
        fi
    done < <(cat "$state/tenants")
}

# --- commands --------------------------------------------------------------------

up() {
    command -v jq >/dev/null || die "needs jq"
    command -v openssl >/dev/null || die "needs openssl"
    curl -fsS -o /dev/null "$etcd/version" ||
        die "no etcd at $etcd: start the Compose stack (cd deploy/compose && docker compose --profile all up -d --wait)"
    mkdir -p "$state"
    echo '*' >"$state/.gitignore"
    printf '%s\n' "etcd=$etcd" "mysql_port=$mysql_port" "mariadb_port=$mariadb_port" \
        "admin_port=$admin_port" "ctl_telemetry_port=$ctl_telemetry_port" \
        "gw_telemetry_port=$gw_telemetry_port" "wk_telemetry_port=$wk_telemetry_port" \
        "login_mode=$login_mode" >"$state/env"

    # Ports held by anything other than our processes.
    local port
    for port in "$admin_port" "$ctl_telemetry_port"; do
        running controller || ! listening "$port" || die "127.0.0.1:$port is taken by another process"
    done
    for port in "$gw_telemetry_port" "$mysql_port" "$mariadb_port"; do
        running gateway || ! listening "$port" || die "127.0.0.1:$port is taken by another process"
    done

    say "building sprigdb and sprigctl ($profile)"
    local release=()
    if [[ $profile == release ]]; then release=(--release); fi
    cargo build -q ${release[@]+"${release[@]}"} -p sprigdb -p sprigctl
    bin=${CARGO_TARGET_DIR:-target}/$profile
    echo "$bin" >"$state/bin"

    # Whether the controller bootstraps the deployment here; then down removes
    # all of dc-a, not only what up registered and set.
    if [[ ! -f $state/etcd-created ]]; then
        if [[ -z $(etcd_get /sprig/g/settings/system.deployment_id) ]]; then
            echo bootstrap >"$state/etcd-created"
        else
            echo existing >"$state/etcd-created"
        fi
    fi

    start controller deploy/compose/sprigdb.dev.toml controller
    wait_ready controller "$ctl_ready"
    # Registration needs the DC leader.
    has_leader() { ctl -o json dc show "$dc" 2>/dev/null | jq -e '.leader != null' >/dev/null; }
    for _ in $(seq 1 60); do
        has_leader && break
        sleep 0.5
    done
    has_leader || die "no DC leader for $dc after 30 s; see $state/controller.log"

    register_clusters

    # The worker runs the mover's jobs: tenant moves from sprigctl shard
    # move and from the balancer in auto mode (chapters 10 and 11).
    start worker deploy/compose/sprigdb.dev.toml worker
    wait_ready worker "$wk_ready"

    make_certs
    local tls=${SPRIG_GATEWAY_TLS:-optional}
    case $tls in required | optional | off) ;; *) die "SPRIG_GATEWAY_TLS must be required, optional or off" ;; esac
    set_setting listener.mysql.bind "\"127.0.0.1:$mysql_port\""
    set_setting listener.mariadb.bind "\"127.0.0.1:$mariadb_port\""
    set_setting listener.mysql.tls "\"$tls\""
    set_setting listener.mariadb.tls "\"$tls\""
    set_setting tls.bundles.default.cert_path "\"$root/$certs/server.pem\""
    set_setting tls.bundles.default.key_path "\"$root/$certs/server-key.pem\""
    set_setting listener.http.enabled false
    set_setting listener.https.enabled false
    # Workload statistics reach the tenants' catalogs within seconds, not
    # 15 minutes (chapter 12, workload.flush_seconds).
    set_setting workload.flush_seconds 10

    start gateway deploy/compose/sprigdb.gateway.dev.toml gateway
    wait_ready gateway "$gw_ready"
    for _ in $(seq 1 20); do
        listening "$mysql_port" && listening "$mariadb_port" && break
        sleep 0.5
    done
    if ! listening "$mysql_port" || ! listening "$mariadb_port"; then
        grep -i "listener" "$state/gateway.log" | tail -n 10 >&2 || true
        die "the gateway isn't listening on $mysql_port and $mariadb_port; see $state/gateway.log"
    fi

    if [[ $login_mode == catalog ]]; then
        provision_tenants
        cat <<EOF

gateway-dev: up (login mode catalog). Listeners: MySQL persona 127.0.0.1:$mysql_port, MariaDB persona 127.0.0.1:$mariadb_port (TLS $tls).
Logins (password $tenant_password): acme.app and globex.app, tenants acme and globex on dc-a-c1 (single).
Each tenant sees only its own databases; CREATE DATABASE makes one.

  mysql -h 127.0.0.1 -P $mysql_port -u acme.app -p$tenant_password --ssl-mode=REQUIRED
  mysql -h 127.0.0.1 -P $mysql_port -u globex.app -p$tenant_password --ssl-mode=DISABLED --default-auth=mysql_native_password

Logs: $state/controller.log, $state/gateway.log. Stop with: $0 down
M0.2's logins (app, app_repl, app_galera): SPRIG_GATEWAY_LOGIN_MODE=accounts_file $0 up
EOF
        return
    fi
    cat <<EOF

gateway-dev: up (login mode accounts_file). Listeners: MySQL persona 127.0.0.1:$mysql_port, MariaDB persona 127.0.0.1:$mariadb_port (TLS $tls).
Logins (password sprig-dev-app): app -> dc-a-c1 (single), app_repl -> dc-a-c2 (primary + replicas), app_galera -> dc-a-c3 (Galera).

  mysql -h 127.0.0.1 -P $mysql_port -u app -psprig-dev-app --ssl-mode=VERIFY_IDENTITY --ssl-ca=$root/$certs/ca.pem
  mysql -h 127.0.0.1 -P $mysql_port -u app -psprig-dev-app --ssl-mode=DISABLED
  mariadb -h 127.0.0.1 -P $mariadb_port -u app -psprig-dev-app --ssl --ssl-ca=$root/$certs/ca.pem
  docker run --rm mariadb:11.8 mariadb -h host.docker.internal -P $mariadb_port -u app -psprig-dev-app -e 'SELECT @@version_comment'
  SPRIG_CONN_USER=app SPRIG_CONN_PASSWORD=sprig-dev-app deploy/compose/connectors/run.sh 127.0.0.1 $mysql_port mysql

Logs: $state/controller.log, $state/gateway.log. Stop with: $0 down
EOF
}

down() {
    if [[ ! -d $state ]]; then
        say "not up (no $state/)"
        return
    fi
    if running gateway && running controller; then remove_tenants; fi
    stop gateway
    stop worker
    stop controller

    local n=0 created
    created=$(cat "$state/etcd-created" 2>/dev/null || echo existing)
    for key in "${settings[@]}"; do
        n=$((n + $(etcd_del "/sprig/dc/$dc/settings/$key")))
    done
    if [[ -f $state/clusters ]]; then
        while read -r id number; do
            n=$((n + $(etcd_del "/sprig/dc/$dc/clusters/$id")))
            n=$((n + $(etcd_del_prefix "/sprig/dc/$dc/cluster-settings/$id/")))
            n=$((n + $(etcd_del "/sprig/g/cluster-ids/$number")))
        done <"$state/clusters"
    fi
    if [[ $created == bootstrap ]]; then
        # The controller bootstrapped the deployment for this run: remove the
        # DC tier, dc-a's cluster ids and the bootstrap records too, as the
        # M0.1 check does.
        n=$((n + $(etcd_del_prefix "/sprig/dc/$dc/")))
        while read -r key; do
            [[ -n $key ]] && n=$((n + $(etcd_del "$key")))
        done < <(etcd_range /sprig/g/cluster-ids/ |
            jq -r --arg dc "$dc" '.kvs[]? | select((.value | @base64d | fromjson | .dc_id) == $dc) | .key | @base64d')
        for key in /sprig/g/settings/system.deployment_id "/sprig/g/dcs/$dc" /sprig/g/ids/tenant; do
            n=$((n + $(etcd_del "$key")))
        done
        # Tenants up created that weren't deleted above: their directory
        # entries go with the deployment (their schemas stay on the cluster).
        if [[ -s $state/tenants ]]; then
            while read -r t id; do
                [[ -n $t ]] || continue
                say "removing tenant $t ($id) from the directory; its t${id}_* schemas stay on dc-a-single"
                n=$((n + $(etcd_del "/sprig/g/tenants/$id") + $(etcd_del "/sprig/g/tenant-names/$t")))
            done <"$state/tenants"
        fi
    fi
    say "deleted $n etcd keys"
    rm -rf "$state"
    say "down"
}

status() {
    local name port
    for name in controller worker gateway; do
        if running "$name"; then
            say "$name running (pid $(pid_of "$name"))"
        else
            say "$name not running"
        fi
    done
    say "state $state/, etcd $etcd"
    for port in "$admin_port" "$mysql_port" "$mariadb_port"; do
        if listening "$port"; then say "127.0.0.1:$port listening"; else say "127.0.0.1:$port closed"; fi
    done
    if curl -fsS -o /dev/null "$etcd/version" 2>/dev/null; then
        for key in "${settings[@]}"; do
            local value
            value=$(get_setting "$key")
            say "setting $key = ${value:-(unset: the built-in default)}"
        done
    else
        say "no etcd at $etcd"
    fi
    if running controller; then ctl cluster list || true; fi
}

# Stops and starts the controller and the gateway with the binaries up
# recorded (rebuild them first), keeping tenants, clusters and settings: a
# code change, or process settings the gateway reads only at start (chapter
# 12's recorder settings), take effect. The controller inherits the
# environment, so SPRIGDB_CONTROLLER_* overrides apply.
restart() {
    [[ -d $state ]] || die "not up (no $state/)"
    stop gateway
    stop worker
    stop controller
    start controller deploy/compose/sprigdb.dev.toml controller
    wait_ready controller "$ctl_ready"
    start worker deploy/compose/sprigdb.dev.toml worker
    wait_ready worker "$wk_ready"
    start gateway deploy/compose/sprigdb.gateway.dev.toml gateway
    wait_ready gateway "$gw_ready"
    for _ in $(seq 1 20); do
        listening "$mysql_port" && listening "$mariadb_port" && break
        sleep 0.5
    done
    say "restarted"
}

tls() {
    case ${1:-} in required | optional | off) ;; *) die "usage: $0 tls required|optional|off" ;; esac
    set_setting listener.mysql.tls "\"$1\""
    set_setting listener.mariadb.tls "\"$1\""
    say "both listeners now TLS $1 (applies live; a stricter mode drains older sessions)"
}

case ${1:-} in
    up) up ;;
    down) down ;;
    status) status ;;
    restart) restart ;;
    tls) tls "${2:-}" ;;
    *) die "usage: $0 up|down|status|restart|tls required|optional|off" ;;
esac
