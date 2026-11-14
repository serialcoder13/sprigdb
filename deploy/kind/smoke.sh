#!/usr/bin/env bash
# End-to-end smoke test of charts/sprigdb on a local kind cluster.
#
#   deploy/kind/smoke.sh                 # every phase
#   SMOKE_PHASES="lint install" deploy/kind/smoke.sh
#
# Phases (in this order; SMOKE_PHASES picks a subset, each phase after
# "install" needs the release it leaves behind):
#   lint      helm lint and helm template: the defaults, every listener and
#             clientTls on, and the render-time refusals.
#   install   kind cluster, images, certificates, a test MariaDB, then
#             helm install with values-smoke.yaml (TLS on, every optional
#             listener on, two controllers).
#   admin     the controller StatefulSet and its headless Service, each
#             controller's registered Admin address, HTTPS with
#             `sprigctl --ca-file`, the NetworkPolicy on the Admin port.
#   clients   a cluster registered, a tenant made, logins on the MySQL and
#             MariaDB ports, the Service's ports, and REST, gRPC, Redis,
#             Bolt, Kafka and AMQP (TLS and plain) clients.
#   mtls      helm upgrade with a client CA and client_cert required on the
#             MySQL port: with a client certificate a login works, without
#             one it is refused; a hand-set client_cert survives an upgrade
#             without clientCa.
#   metrics   the controller, gateway, operator and worker telemetry
#             Services and each pod's /metrics.
#   backup    sprig_backup_last_success_timestamp_seconds from the operator,
#             after a stand-in backup Job (mariadb-operator isn't installed).
#   upgrade   a fresh install of the previous chart layout (controllers as a
#             Deployment, plain-HTTP Admin API), then helm upgrade to this
#             chart: the StatefulSet replaces the Deployment, the data stays.
#
# cert-manager, trust-manager, mariadb-operator and Prometheus Operator are
# not installed: stub CRDs (stub-crds.yaml) take their objects, and the
# script makes the Secrets each Certificate names with openssl, from the
# chart's own rendered Certificates (names, subject, DNS names).
#
# Needs: docker, kind, helm, kubectl, openssl, jq, git (read-only: the
# previous chart comes from `git archive`). Images: the SprigDB image
# ($SMOKE_IMAGE; built with deploy/compose-vm/build-image.sh when missing
# and SMOKE_BUILD=1), mariadb:11.8, quay.io/coreos/etcd:v3.6.5 and
# node:22-bookworm-slim (pulled by docker when missing).
#
# Environment:
#   SMOKE_CLUSTER   kind cluster name (sprigdb-smoke); created when missing,
#                   deleted at the end unless SMOKE_KEEP=1 or it existed.
#   SMOKE_IMAGE     the SprigDB image (sprigdb:kind-smoke).
#   SMOKE_BUILD     1: build SMOKE_IMAGE when it is missing.
#   SMOKE_NODE_IMAGE kind's node image (kindest/node:v1.36.4).
#   SMOKE_NODE_MEMORY cap on the kind node container (6g).
#   SMOKE_OLD_REF   git ref of the previous chart layout (38e7122).
#   SMOKE_WORK      work directory (a new temporary one).
#   KIND, HELM, KUBECTL  the binaries (from PATH).
#
# Every client command names its host and port. Nothing listens on the
# host: all clients run in pods inside the kind cluster.
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../.." && pwd)
KIND=${KIND:-kind}
HELM=${HELM:-helm}
KUBECTL=${KUBECTL:-kubectl}
cluster=${SMOKE_CLUSTER:-sprigdb-smoke}
image=${SMOKE_IMAGE:-sprigdb:kind-smoke}
node_image=${SMOKE_NODE_IMAGE:-kindest/node:v1.36.4}
old_ref=${SMOKE_OLD_REF:-38e7122}
phases=${SMOKE_PHASES:-lint install admin clients mtls metrics backup upgrade}
work=${SMOKE_WORK:-$(mktemp -d "${TMPDIR:-/tmp}/sprig-kind-smoke.XXXXXX")}
mkdir -p "$work"
ns=sprigdb
dc=kind1
chart=$repo/charts/sprigdb
values=$here/values-smoke.yaml
kcfg=$work/kubeconfig
gw=sprigdb-gateway.$ns.svc
ctl_svc=sprigdb-controller.$ns.svc
tenant=smoke
user=app
login=$tenant.$user
fail=0
created_cluster=0

say() { printf '\n== %s\n' "$*"; }
pass() { printf 'PASS  %-46s %s\n' "$1" "${2:-}"; }
bad() { printf 'FAIL  %-46s %s\n' "$1" "${2:-}"; fail=1; }
die() { printf 'smoke: %s\n' "$*" >&2; exit 2; }
want() { [[ " $phases " == *" $1 "* ]]; }
k() { "$KUBECTL" --kubeconfig "$kcfg" -n "$ns" "$@"; }
h() { "$HELM" --kubeconfig "$kcfg" -n "$ns" "$@"; }
# Runs a command in one of the test pods.
in_ctl() { k exec smoke-ctl -- "$@"; }
in_node() { k exec smoke-node -- "$@"; }
in_sql() { k exec smoke-sql -- "$@"; }
# sprigctl through the controller Service; a call only the DC leader serves
# is refused elsewhere with 39504 naming the leader, and is then sent to
# the leader's own name under the headless Service (as the settings Job
# does). SMOKE_ADMIN_SCHEME=http for the previous chart's plain Admin API.
sprigctl() {
    local s=${SMOKE_ADMIN_SCHEME:-https} out leader
    local tls=(--ca-file /ca/ca.crt)
    [[ $s == http ]] && tls=()
    if out=$(in_ctl sprigctl --endpoint "$s://$ctl_svc:7480" ${tls[@]+"${tls[@]}"} "$@" 2>&1); then
        [[ -z $out ]] || printf '%s\n' "$out"
        return 0
    fi
    leader=$(sed -n 's/.*send the request to the leader, \([A-Za-z0-9._-]*\).*/\1/p' <<<"$out" | head -1)
    if [[ -n $leader ]]; then
        if [[ $s == https ]]; then
            in_ctl sprigctl --endpoint "$s://$leader.sprigdb-controller-pods.$ns.svc:7480" ${tls[@]+"${tls[@]}"} "$@" 2>&1
            return
        fi
        # The previous layout's controllers have no names other pods
        # resolve: try the Service again until it lands on the leader.
        local i
        for i in $(seq 1 12); do
            out=$(in_ctl sprigctl --endpoint "$s://$ctl_svc:7480" "$@" 2>&1) && { printf '%s\n' "$out"; return 0; }
            grep -q 'send the request to the leader' <<<"$out" || break
        done
    fi
    printf '%s\n' "$out"
    return 1
}
pw_file=$work/app-password

for b in docker "$KIND" "$HELM" "$KUBECTL" openssl jq git; do
    command -v "$b" >/dev/null || die "$b is not on PATH"
done

# --- lint -------------------------------------------------------------------------

phase_lint() {
    say "helm lint and helm template"
    local all=$work/all-on.yaml out
    cat >"$all" <<'EOF'
gateway:
  listeners:
    mongo: {enabled: true, port: 27017}
    redis: {enabled: true, port: 6379}
    bolt: {enabled: true, port: 7687}
    kafka: {enabled: true, port: 9092}
    amqp: {enabled: true, port: 5672}
    amqps: {enabled: true, port: 5671}
  clientTls:
    secretName: client-tls
    clientCert:
      default: required
      listeners: {http: optional, amqp: "off"}
    clientCa: {secretName: client-ca}
monitoring: {serviceMonitor: true}
controller: {adminApi: {tlsSecret: admin-tls}}
EOF
    if out=$("$HELM" lint --strict "$chart" 2>&1); then pass "helm lint --strict (defaults)"; else bad "helm lint --strict (defaults)" "$out"; fi
    if out=$("$HELM" lint --strict "$chart" -f "$all" 2>&1); then pass "helm lint --strict (all listeners, clientTls)"; else bad "helm lint --strict (all listeners, clientTls)" "$out"; fi
    if out=$("$HELM" lint --strict "$chart" -f "$values" 2>&1); then pass "helm lint --strict (values-smoke.yaml)"; else bad "helm lint --strict (values-smoke.yaml)" "$out"; fi
    if "$HELM" template t "$chart" -n "$ns" >"$work/default.out" 2>&1; then
        pass "helm template (defaults)" "$(grep -c '^kind:' "$work/default.out") objects"
    else bad "helm template (defaults)" "$(cat "$work/default.out")"; fi
    if "$HELM" template t "$chart" -n "$ns" -f "$all" >"$work/all-on.out" 2>&1; then
        local ports n
        ports=$(awk '/^kind: Service$/{s=1} s&&/name: sprigdb-gateway$/{g=1} g&&/port:/{print} /^---/{s=0;g=0}' "$work/all-on.out" | grep -c port)
        n=$(grep -c 'client_cert' "$work/all-on.out")
        if [[ $ports == 10 && $n == 10 ]]; then pass "helm template (all listeners, clientTls)" "10 Service ports, 10 client_cert settings"
        else bad "helm template (all listeners, clientTls)" "Service ports $ports, client_cert settings $n (want 10, 10)"; fi
    else bad "helm template (all listeners, clientTls)" "$(cat "$work/all-on.out")"; fi
    if out=$("$HELM" template t "$chart" -n "$ns" --set tls.enabled=false 2>&1); then
        bad "refuses plain Admin API" "rendered without allowPlain"
    elif grep -q allowPlain <<<"$out"; then pass "refuses plain Admin API" "names controller.adminApi.allowPlain"
    else bad "refuses plain Admin API" "$out"; fi
    if out=$("$HELM" template t "$chart" -n "$ns" --set gateway.clientTls.clientCert.default=required 2>&1); then
        bad "refuses client certificates without a CA" "rendered"
    elif grep -q clientCa <<<"$out"; then pass "refuses client certificates without a CA" "names gateway.clientTls.clientCa"
    else bad "refuses client certificates without a CA" "$out"; fi
    # A bare YAML off (false) is off.
    out=$(printf 'gateway: {clientTls: {clientCert: {default: off}}}\n' | "$HELM" template t "$chart" -n "$ns" -f - 2>&1)
    if [[ $? == 0 ]] && ! grep -q client_cert <<<"$out"; then pass "a bare off renders as off" "no client_cert settings without clientCa"
    else bad "a bare off renders as off" "$(tail -3 <<<"$out")"; fi
}

# --- install ----------------------------------------------------------------------

# One certificate signed by the smoke CA: <out prefix> <subject> <SANs>.
sign() {
    local p=$1 subj=$2 sans=$3
    openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$p.key" 2>/dev/null
    openssl req -new -key "$p.key" -subj "$subj" -out "$p.csr" 2>/dev/null
    printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\nbasicConstraints=critical,CA:FALSE\n' "$sans" >"$p.ext"
    openssl x509 -req -in "$p.csr" -CA "$work/ca.crt" -CAkey "$work/ca.key" -CAcreateserial \
        -days 30 -sha256 -extfile "$p.ext" -out "$p.crt" 2>/dev/null
}

tls_secret() { # <name> <cert prefix> [ca file]
    k create secret generic "$1" --type=kubernetes.io/tls --from-file=tls.crt="$2.crt" \
        --from-file=tls.key="$2.key" --from-file=ca.crt="${3:-$work/ca.crt}" \
        --dry-run=client -o yaml | k apply -f - >/dev/null
}

# The Secrets cert-manager would make for the chart's Certificates, from
# the rendered Certificates of <chart dir> (secret name, subject, DNS names).
certificates_for() {
    local dir=$1 rendered
    rendered=$("$HELM" template sprigdb "$dir" -n "$ns" -f "$values" --show-only templates/certificates.yaml) || die "render certificates"
    k create --dry-run=client -o json -f - <<<"$rendered" | jq -r '
        (if .kind == "List" then .items[] else . end) | .spec as $s |
        [$s.secretName,
         ([("/O=" + ($s.subject.organizations // [] | .[])), ("/OU=" + ($s.subject.organizationalUnits // [] | .[]))] | join("")) + "/CN=" + $s.commonName,
         ($s.dnsNames | map("DNS:" + .) | join(","))] | @tsv' >"$work/certs.tsv" || die "read certificates"
    local name subj sans
    while IFS=$'\t' read -r name subj sans; do
        sign "$work/$name" "$subj" "$sans"
        tls_secret "$name" "$work/$name"
    done <"$work/certs.tsv"
}

ensure_image() {
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        [[ ${SMOKE_BUILD:-0} == 1 ]] || die "image $image is missing (set SMOKE_BUILD=1 to build it)"
        "$repo/deploy/compose-vm/build-image.sh" "$image" "$repo" || die "image build failed"
    fi
    local i
    for i in mariadb:11.8 quay.io/coreos/etcd:v3.6.5 node:22-bookworm-slim; do
        docker image inspect "$i" >/dev/null 2>&1 || docker pull -q "$i" >/dev/null || die "pull $i"
    done
}

test_pods() {
    k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: smoke-ctl
  # The chart's name label: the NetworkPolicies admit it like a SprigDB pod.
  labels: {app.kubernetes.io/name: sprigdb, app.kubernetes.io/component: smoke}
spec:
  enableServiceLinks: false
  containers:
    - name: ctl
      image: $image
      imagePullPolicy: Never
      command: [sleep, infinity]
      env: [{name: SPRIGCTL_TOKEN_FILE, value: /admin/token}]
      volumeMounts:
        - {name: ca, mountPath: /ca}
        - {name: admin, mountPath: /admin}
  volumes:
    - {name: ca, configMap: {name: sprigdb-ca-bundle}}
    - {name: admin, secret: {secretName: sprigdb-admin-token}}
---
apiVersion: v1
kind: Pod
metadata:
  name: smoke-node
  labels: {app.kubernetes.io/name: sprigdb, app.kubernetes.io/component: smoke}
spec:
  enableServiceLinks: false
  containers:
    - name: node
      image: node:22-bookworm-slim
      imagePullPolicy: Never
      command: [sleep, infinity]
      volumeMounts:
        - {name: ca, mountPath: /ca}
        - {name: js, mountPath: /js}
  volumes:
    - {name: ca, configMap: {name: sprigdb-ca-bundle}}
    - {name: js, configMap: {name: smoke-js}}
---
apiVersion: v1
kind: Pod
metadata:
  # No chart labels: a client outside SprigDB.
  name: smoke-sql
spec:
  enableServiceLinks: false
  containers:
    - name: sql
      image: mariadb:11.8
      imagePullPolicy: Never
      command: [sleep, infinity]
      volumeMounts:
        - {name: ca, mountPath: /ca}
        - {name: cc, mountPath: /client}
  volumes:
    - {name: ca, configMap: {name: sprigdb-ca-bundle}}
    - {name: cc, secret: {secretName: smoke-client-cert}}
EOF
    k wait --for=condition=Ready pod/smoke-ctl pod/smoke-node pod/smoke-sql --timeout=180s >/dev/null || die "test pods not ready"
}

phase_install() {
    say "kind cluster $cluster"
    ensure_image
    if "$KIND" get clusters 2>/dev/null | grep -qx "$cluster"; then
        "$KIND" get kubeconfig --name "$cluster" >"$kcfg"
    else
        cat >"$work/kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    # The chart's etcd spreads over zones (DoNotSchedule).
    labels: {topology.kubernetes.io/zone: kind-a}
EOF
        "$KIND" create cluster --name "$cluster" --image "$node_image" --config "$work/kind.yaml" \
            --kubeconfig "$kcfg" --wait 180s || die "kind create cluster"
        created_cluster=1
        docker update --memory "${SMOKE_NODE_MEMORY:-6g}" --memory-swap "${SMOKE_NODE_MEMORY:-6g}" \
            "$cluster-control-plane" >/dev/null
    fi
    local i
    for i in "$image" mariadb:11.8 quay.io/coreos/etcd:v3.6.5 node:22-bookworm-slim; do
        "$KIND" load docker-image --name "$cluster" "$i" >/dev/null || die "kind load $i"
    done
    # The chart's image reference must name the image as loaded.
    image_values=(--set "image.repository=${image%:*}" --set "image.tag=${image##*:}")

    say "namespace, stub CRDs, certificates"
    "$KUBECTL" --kubeconfig "$kcfg" create namespace "$ns" --dry-run=client -o yaml |
        "$KUBECTL" --kubeconfig "$kcfg" apply -f - >/dev/null
    "$KUBECTL" --kubeconfig "$kcfg" apply -f "$here/stub-crds.yaml" >/dev/null
    "$KUBECTL" --kubeconfig "$kcfg" apply -f "$chart/crds" >/dev/null
    "$KUBECTL" --kubeconfig "$kcfg" wait --for=condition=Established crd --all --timeout=60s >/dev/null
    if [[ ! -f $work/ca.crt ]]; then
        openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$work/ca.key" 2>/dev/null
        openssl req -x509 -new -key "$work/ca.key" -subj "/O=sprigdb smoke/CN=sprigdb smoke CA" -days 30 \
            -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
            -out "$work/ca.crt" 2>/dev/null
        # A second CA signs client certificates only.
        openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$work/client-ca.key" 2>/dev/null
        openssl req -x509 -new -key "$work/client-ca.key" -subj "/O=sprigdb smoke/CN=sprigdb smoke client CA" -days 30 \
            -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
            -out "$work/client-ca.crt" 2>/dev/null
        openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$work/client.key" 2>/dev/null
        openssl req -new -key "$work/client.key" -subj "/O=sprigdb smoke/CN=$login" -out "$work/client.csr" 2>/dev/null
        printf 'extendedKeyUsage=clientAuth\nbasicConstraints=critical,CA:FALSE\n' >"$work/client.ext"
        openssl x509 -req -in "$work/client.csr" -CA "$work/client-ca.crt" -CAkey "$work/client-ca.key" \
            -CAcreateserial -days 30 -sha256 -extfile "$work/client.ext" -out "$work/client.crt" 2>/dev/null
        # A client certificate from a CA the gateway doesn't trust.
        sign "$work/stranger" "/CN=stranger" "DNS:stranger"
        openssl rand -hex 16 >"$pw_file"
    fi
    k create configmap sprigdb-ca-bundle --from-file=ca.crt="$work/ca.crt" --dry-run=client -o yaml | k apply -f - >/dev/null
    certificates_for "$chart"
    sign "$work/gateway-public" "/O=sprigdb smoke/CN=$gw" \
        "DNS:sprigdb-gateway,DNS:$gw,DNS:$gw.cluster.local,DNS:*.sprigdb-gateway-pods.$ns.svc,DNS:localhost,IP:127.0.0.1"
    tls_secret smoke-gateway-public-tls "$work/gateway-public"
    sign "$work/mariadb" "/CN=smoke-mariadb" "DNS:smoke-mariadb,DNS:smoke-mariadb.$ns.svc"
    tls_secret smoke-mariadb-tls "$work/mariadb"
    k create secret generic smoke-client-ca --from-file=ca.crt="$work/client-ca.crt" --dry-run=client -o yaml | k apply -f - >/dev/null
    k create secret generic smoke-client-cert --from-file=tls.crt="$work/client.crt" --from-file=tls.key="$work/client.key" \
        --from-file=stranger.crt="$work/stranger.crt" --from-file=stranger.key="$work/stranger.key" \
        --dry-run=client -o yaml | k apply -f - >/dev/null
    k create configmap smoke-js --from-file=smoke.mjs="$repo/deploy/compose-vm/smoke/smoke.mjs" \
        --from-file=amqp-plain.mjs="$here/amqp-plain.mjs" --dry-run=client -o yaml | k apply -f - >/dev/null

    say "test MariaDB (external single-node cluster)"
    if ! k get secret smoke-mariadb-pw >/dev/null 2>&1; then
        k create secret generic smoke-mariadb-pw --from-literal=root="$(openssl rand -hex 16)" \
            --from-literal=ctl="$(openssl rand -hex 16)" --from-literal=repl="$(openssl rand -hex 16)" >/dev/null
    fi
    local ctlpw replpw
    ctlpw=$(k get secret smoke-mariadb-pw -o jsonpath='{.data.ctl}' | base64 -d)
    replpw=$(k get secret smoke-mariadb-pw -o jsonpath='{.data.repl}' | base64 -d)
    # The controllers, workers and gateways mount this Secret at
    # /etc/sprigdb/creds (the operator writes it for clusters it makes).
    printf '[sprig_ctl]\nuser = "sprig_ctl"\npassword = "%s"\ntls = "verify"\nca_file = "/etc/sprigdb/ca/ca.crt"\n[sprig_repl]\npassword = "%s"\ntls = "verify"\nca_file = "/etc/sprigdb/ca/ca.crt"\n' \
        "$ctlpw" "$replpw" >"$work/smoke1.toml"
    k create secret generic sprigdb-cluster-credentials --from-file=smoke1.toml="$work/smoke1.toml" \
        --dry-run=client -o yaml | k apply -f - >/dev/null
    k apply -f "$here/mariadb.yaml" >/dev/null
    k rollout status statefulset/smoke-mariadb --timeout=300s >/dev/null || die "test MariaDB not ready"

    say "helm install"
    local t0=$SECONDS
    if h upgrade --install sprigdb "$chart" -f "$values" "${image_values[@]}" --wait --timeout 10m >"$work/install.log" 2>&1; then
        pass "helm install" "$((SECONDS - t0)) s, settings Job done"
    else
        bad "helm install" "$(tail -5 "$work/install.log")"
        k get pods -o wide
        return 1
    fi
    test_pods
}

# --- admin ------------------------------------------------------------------------

phase_admin() {
    say "controllers: StatefulSet, headless Service, Admin API over HTTPS"
    local kind svc
    kind=$(k get statefulset sprigdb-controller -o jsonpath='{.spec.serviceName}/{.status.readyReplicas}/{.spec.replicas}' 2>&1)
    if [[ $kind == sprigdb-controller-pods/2/2 ]]; then pass "controller StatefulSet" "serviceName sprigdb-controller-pods, 2/2 ready"
    else bad "controller StatefulSet" "$kind"; fi
    if k get deployment sprigdb-controller >/dev/null 2>&1; then bad "no controller Deployment" "still there"; else pass "no controller Deployment"; fi
    svc=$(k get svc sprigdb-controller-pods -o jsonpath='{.spec.clusterIP} {.spec.publishNotReadyAddresses} {.spec.ports[*].port}')
    if [[ $svc == "None true 7480 9180" ]]; then pass "headless Service sprigdb-controller-pods" "$svc"; else bad "headless Service sprigdb-controller-pods" "$svc"; fi

    # Each controller's registration in the DC tier.
    local etcd=(k exec sprigdb-etcd-dc-0 -- etcdctl --endpoints="https://sprigdb-etcd-dc-0.sprigdb-etcd-dc.$ns.svc:2379"
        --cacert=/etc/etcd/ca/ca.crt --cert=/etc/etcd/tls/tls.crt --key=/etc/etcd/tls/tls.key)
    local regs i want got
    regs=$("${etcd[@]}" get --prefix "" 2>&1 | grep -a -A1 'controllers/sprigdb-controller-' | grep -a -v '^--$')
    printf '%s\n' "$regs" >"$work/controller-registrations.txt"
    for i in 0 1; do
        want="sprigdb-controller-$i.sprigdb-controller-pods.$ns.svc:7480"
        got=$(grep -a -A1 "controllers/sprigdb-controller-$i\$" <<<"$regs" | tail -1)
        if grep -q "$want" <<<"$got"; then pass "controller-$i registers its Admin address" "$want"
        else bad "controller-$i registers its Admin address" "want $want, got ${got:0:200}"; fi
        # The name resolves inside the cluster and the certificate covers it.
        if out=$(in_ctl sprigctl --endpoint "https://$want" --ca-file /ca/ca.crt dc list 2>&1); then
            pass "Admin API at controller-$i's address" "HTTPS, certificate verified"
        else bad "Admin API at controller-$i's address" "${out:0:300}"; fi
    done

    local out
    if out=$(sprigctl dc list 2>&1) && grep -q "$dc" <<<"$out"; then pass "sprigctl --ca-file dc list" "$(tr -s ' \n' ' ' <<<"$out" | cut -c1-80)"
    else bad "sprigctl --ca-file dc list" "${out:0:300}"; fi
    if out=$(in_ctl sprigctl --endpoint "https://$ctl_svc:7480" dc list 2>&1); then
        bad "sprigctl without the CA is refused" "it connected: ${out:0:200}"
    else pass "sprigctl without the CA is refused" "$(head -1 <<<"$out" | cut -c1-90)"; fi
    if out=$(in_ctl sprigctl --endpoint "http://$ctl_svc:7480" dc list 2>&1); then
        bad "plain HTTP to the Admin API is refused" "it answered"
    else pass "plain HTTP to the Admin API is refused"; fi
    # NetworkPolicy: a pod without the chart's labels can't reach 7480.
    if in_sql timeout 5 bash -c "</dev/tcp/$ctl_svc/7480" 2>/dev/null; then
        bad "NetworkPolicy closes 7480 to other pods" "an unlabelled pod connected (CNI without NetworkPolicy?)"
    else pass "NetworkPolicy closes 7480 to other pods"; fi
}

# --- clients ----------------------------------------------------------------------

sql() { # <port> [mariadb client options...] -e <sql>
    local port=$1
    shift
    in_sql mariadb -h "$gw" -P "$port" -u "$login" "-p$(cat "$pw_file")" --ssl --ssl-ca=/ca/ca.crt \
        --ssl-verify-server-cert -N -B "$@" 2>&1
}

phase_clients() {
    say "cluster, tenant, logins"
    local out
    if ! sprigctl -o json cluster list 2>/dev/null | jq -e '.clusters[]? | select(.name == "smoke1")' >/dev/null; then
        out=$(sprigctl -o json cluster register smoke1 --dc "$dc" --topology single \
            --node "smoke-mariadb.$ns.svc:3306" --credentials file:/etc/sprigdb/creds/smoke1.toml \
            --memory-gb 1 --storage-gb 2 --yes 2>&1)
        if jq -e '.cluster.cluster_id' <<<"$out" >/dev/null 2>&1; then pass "cluster register (TLS verified)" "$(jq -r '.cluster.cluster_id' <<<"$out")"
        else bad "cluster register (TLS verified)" "${out:0:400}"; return 1; fi
    fi
    out=$(sprigctl tenant create "$tenant" --yes) || grep -qi 'exists' <<<"$out" || { bad "tenant create" "${out:0:300}"; return 1; }
    out=$(sprigctl tenant user add "$tenant" "$user" --password "$(cat "$pw_file")" --if-not-exists --yes) ||
        { bad "tenant user add" "${out:0:300}"; return 1; }
    pass "tenant $tenant, user $login"

    out=$(sql 3306 -e "CREATE DATABASE IF NOT EXISTS smoke_db; CREATE TABLE IF NOT EXISTS smoke_db.t (id INT PRIMARY KEY, v VARCHAR(20)); REPLACE INTO smoke_db.t VALUES (1,'mysql'),(2,'persona'); SELECT COUNT(*) FROM smoke_db.t; SELECT @@version")
    if [[ $(sed -n 1p <<<"$out") == 2 ]]; then pass "MySQL login (3306, TLS)" "2 rows, version $(sed -n 2p <<<"$out")"
    else bad "MySQL login (3306, TLS)" "${out:0:300}"; fi
    out=$(sql 3307 -e "SELECT GROUP_CONCAT(v ORDER BY id) FROM smoke_db.t; SELECT @@version")
    if [[ $(sed -n 1p <<<"$out") == mysql,persona ]]; then pass "MariaDB login (3307, TLS)" "read back, version $(sed -n 2p <<<"$out")"
    else bad "MariaDB login (3307, TLS)" "${out:0:300}"; fi
    # Bolt's default graph and AMQP's vhost / live in these databases.
    sql 3306 -e "CREATE DATABASE IF NOT EXISTS graph; CREATE DATABASE IF NOT EXISTS mq" >/dev/null

    say "gateway Service ports and the other protocols"
    out=$(k get svc sprigdb-gateway -o jsonpath='{range .spec.ports[*]}{.name}={.port} {end}')
    if [[ $out == "mysql=3306 mariadb=3307 http=8080 https=8443 mongo=27017 redis=6379 bolt=7687 kafka=9092 amqp=5672 amqps=5671 " ]]; then
        pass "gateway Service publishes every toggle" "$out"
    else bad "gateway Service publishes every toggle" "$out"; fi
    out=$(k get deploy sprigdb-gateway -o jsonpath='{range .spec.template.spec.containers[0].ports[*]}{.name}={.containerPort} {end}')
    [[ $out == *"amqps=5671"* && $out == *"bolt=7687"* ]] && pass "gateway pod ports" "$out" || bad "gateway pod ports" "$out"
    out=$(in_node node /js/smoke.mjs "$gw" /ca/ca.crt "$login" "$(cat "$pw_file")" \
        '{"https":8443,"redis":6379,"bolt":7687,"kafka":9092,"amqps":5671}' 2>&1)
    printf '%s\n' "$out" | sed 's/^/      /'
    grep -q '^FAIL' <<<"$out" && fail=1
    grep -q '^PASS  Bolt' <<<"$out" && grep -q '^PASS  Kafka' <<<"$out" && grep -q '^PASS  AMQP' <<<"$out" &&
        pass "Bolt, Kafka and AMQP (TLS) clients" || bad "Bolt, Kafka and AMQP (TLS) clients"
    out=$(in_node node /js/amqp-plain.mjs "$gw" 5672 "$login" "$(cat "$pw_file")" 2>&1)
    if [[ $? == 0 ]]; then pass "AMQP 0-9-1 (plain, 5672)" "$out"; else bad "AMQP 0-9-1 (plain, 5672)" "$out"; fi
    gateway_addresses
}

# Each gateway registers <pod IP, dashed>.sprigdb-gateway-pods.<ns>.svc:
# Kafka's metadata names it, and the other gateways reach its peer service
# there (a KILL from another gateway).
gateway_addresses() {
    say "gateway addresses: Kafka brokers and the peer service"
    local out host ips a b id i
    out=$(in_node node /js/smoke.mjs "$gw" /ca/ca.crt "$login" "$(cat "$pw_file")" '{"kafka":9092}' 2>&1 | grep 'Kafka')
    host=$(sed -n 's/.*brokers [0-9]*@\([^:,]*\):9092.*/\1/p' <<<"$out")
    if [[ $host == *.sprigdb-gateway-pods.$ns.svc ]]; then pass "Kafka advertises a resolvable name" "$host"
    else bad "Kafka advertises a resolvable name" "${out:0:200}"; fi
    out=$(in_node node -e "const s=require('tls').connect({host:'$host',port:9092,ca:require('fs').readFileSync('/ca/ca.crt'),servername:'$host'},()=>{console.log(s.authorized?'verified':'not verified: '+s.authorizationError);s.end()});s.on('error',e=>{console.log(String(e));process.exit(1)})" 2>&1)
    [[ $out == verified ]] && pass "Kafka broker address: TLS, name verified" "$host:9092" || bad "Kafka broker address: TLS, name verified" "$out"
    read -r -a ips <<<"$(k get pods -l app.kubernetes.io/component=gateway -o jsonpath='{.items[*].status.podIP}')"
    if [[ ${#ips[@]} -lt 2 ]]; then bad "KILL across gateways" "needs two gateways, found ${#ips[@]}"; return; fi
    a=$(tr '.:' '--' <<<"${ips[0]}").sprigdb-gateway-pods.$ns.svc
    b=$(tr '.:' '--' <<<"${ips[1]}").sprigdb-gateway-pods.$ns.svc
    # A statement on gateway A, killed from gateway B.
    in_sql sh -c "rm -f /tmp/kill.out; nohup mariadb -h $a -P 3307 -u $login -p$(cat "$pw_file") --ssl --ssl-ca=/ca/ca.crt --ssl-verify-server-cert -N -B -n -e 'SELECT CONNECTION_ID(); SELECT SLEEP(60)' >/tmp/kill.out 2>&1 &"
    for i in $(seq 1 20); do id=$(in_sql sh -c 'head -1 /tmp/kill.out 2>/dev/null'); [[ $id =~ ^[0-9]+$ ]] && break; sleep 0.5; done
    [[ $id =~ ^[0-9]+$ ]] || { bad "KILL across gateways" "no connection id on gateway A: $(in_sql cat /tmp/kill.out)"; return; }
    sleep 1
    out=$(in_sql mariadb -h "$b" -P 3307 -u "$login" "-p$(cat "$pw_file")" --ssl --ssl-ca=/ca/ca.crt --ssl-verify-server-cert -e "KILL QUERY $id" 2>&1)
    for i in $(seq 1 20); do [[ $(in_sql sh -c 'wc -l </tmp/kill.out') -ge 2 ]] && break; sleep 0.5; done
    if [[ $(in_sql sh -c 'wc -l </tmp/kill.out') -ge 2 ]]; then
        pass "KILL across gateways (peer service)" "connection $id on $a ended by KILL QUERY through $b"
    else bad "KILL across gateways (peer service)" "KILL: ${out:0:200}; A still running: $(in_sql cat /tmp/kill.out | tr '\n' ' ')"; fi
}

# --- mtls -------------------------------------------------------------------------

setting() { # <key>: the effective value at the DC's scope
    sprigctl -o json settings get "$1" --scope "dc:$dc" 2>/dev/null | jq -r '.value // .effective.value // empty' | tr -d '"'
}

phase_mtls() {
    say "client certificates"
    local out v
    # A value set by hand while the chart has no clientCa.
    sprigctl settings set listener.mariadb.client_cert optional --scope "dc:$dc" --reason smoke --yes >/dev/null 2>&1
    h upgrade sprigdb "$chart" -f "$values" "${image_values[@]}" --wait --timeout 10m >"$work/upgrade-noca.log" 2>&1 ||
        bad "helm upgrade without clientCa" "$(tail -3 "$work/upgrade-noca.log")"
    v=$(setting listener.mariadb.client_cert)
    if [[ $v == optional ]]; then pass "settings Job keeps a hand-set client_cert" "listener.mariadb.client_cert = optional after an upgrade"
    else bad "settings Job keeps a hand-set client_cert" "listener.mariadb.client_cert = '$v' (set optional by hand)"; fi
    sprigctl settings set listener.mariadb.client_cert off --scope "dc:$dc" --reason smoke --yes >/dev/null 2>&1

    h upgrade sprigdb "$chart" -f "$values" "${image_values[@]}" \
        --set gateway.clientTls.clientCa.secretName=smoke-client-ca \
        --set gateway.clientTls.clientCert.listeners.mysql=required \
        --wait --timeout 10m >"$work/upgrade-mtls.log" 2>&1 || { bad "helm upgrade with clientCa" "$(tail -3 "$work/upgrade-mtls.log")"; return 1; }
    pass "helm upgrade with clientCa, mysql required"
    for kv in "tls.bundles.default.client_ca_path=/etc/sprigdb/client-ca/ca.crt" listener.mysql.client_cert=required \
        listener.mariadb.client_cert=off listener.https.client_cert=off; do
        v=$(setting "${kv%%=*}")
        [[ $v == "${kv#*=}" ]] && pass "settings Job set ${kv%%=*}" "$v" || bad "settings Job set ${kv%%=*}" "got '$v', want ${kv#*=}"
    done
    # The gateways pick the settings up live; give them a few seconds.
    local i ok=0
    for i in $(seq 1 20); do
        out=$(sql 3306 --ssl-cert=/client/tls.crt --ssl-key=/client/tls.key -e "SELECT COUNT(*) FROM smoke_db.t")
        [[ $out == 2 ]] && { ok=1; break; }
        sleep 3
    done
    [[ $ok == 1 ]] && pass "3306 with a client certificate" "login works" || bad "3306 with a client certificate" "${out:0:300}"
    out=$(sql 3306 -e "SELECT 1")
    if [[ $out == 1 ]]; then bad "3306 without a client certificate" "login worked"
    else pass "3306 without a client certificate" "refused: $(grep -m1 ERROR <<<"$out" | cut -c1-90)"; fi
    out=$(sql 3306 --ssl-cert=/client/stranger.crt --ssl-key=/client/stranger.key -e "SELECT 1")
    if [[ $out == 1 ]]; then bad "3306 with an untrusted client certificate" "login worked"
    else pass "3306 with an untrusted client certificate" "refused: $(grep -m1 ERROR <<<"$out" | cut -c1-90)"; fi
    out=$(sql 3307 -e "SELECT 1")
    [[ $out == 1 ]] && pass "3307 (off) without a client certificate" "login works" || bad "3307 (off) without a client certificate" "${out:0:300}"

    # Back to no client certificates: modes off, CA kept for this upgrade.
    h upgrade sprigdb "$chart" -f "$values" "${image_values[@]}" \
        --set gateway.clientTls.clientCa.secretName=smoke-client-ca \
        --wait --timeout 10m >"$work/upgrade-off.log" 2>&1 || bad "helm upgrade, modes off" "$(tail -3 "$work/upgrade-off.log")"
    v=$(setting listener.mysql.client_cert)
    [[ $v == off ]] && pass "modes off with clientCa kept" "listener.mysql.client_cert = off" || bad "modes off with clientCa kept" "got '$v'"
}

# --- metrics ----------------------------------------------------------------------

metric_get() { # <ip> -> "HTTP <status>" and the /metrics body (from the labelled node pod)
    in_node node -e "fetch('http://$1:9180/metrics').then(async r=>{const t=await r.text();process.stdout.write('HTTP '+r.status+'\\n'+t)}).catch(e=>{console.error(String(e));process.exit(1)})"
}

phase_metrics() {
    say "telemetry Services and /metrics"
    local svc ips ip body n
    for svc in sprigdb-controller-pods sprigdb-gateway-health sprigdb-operator-telemetry sprigdb-worker-telemetry; do
        if ! k get svc "$svc" >/dev/null 2>&1; then bad "Service $svc" "missing"; continue; fi
        ips=$(k get endpointslices -l "kubernetes.io/service-name=$svc" -o jsonpath='{range .items[*].endpoints[*]}{.addresses[0]} {end}')
        n=0
        for ip in $ips; do
            body=$(metric_get "$ip" 2>&1)
            # An operator with no SprigCluster yet has recorded nothing:
            # an empty 200 is still an answer.
            if [[ $(head -1 <<<"$body") == "HTTP 200" ]]; then n=$((n + 1)); else bad "$svc $ip /metrics" "${body:0:200}"; fi
        done
        [[ $n -gt 0 ]] && pass "$svc /metrics" "$n pod(s) answer 200, $(grep -c '^# TYPE sprig_' <<<"$body") sprig_ families on the last" || bad "$svc /metrics" "no endpoint answered"
    done
    # With monitoring.serviceMonitor off, only SprigDB's own pods reach a
    # controller's telemetry port.
    ip=$(k get pod sprigdb-controller-0 -o jsonpath='{.status.podIP}')
    if in_sql timeout 5 bash -c "</dev/tcp/$ip/9180" 2>/dev/null; then
        bad "controller 9180 closed to other pods (no ServiceMonitor)" "connected"
    else pass "controller 9180 closed to other pods (no ServiceMonitor)"; fi
}

# --- backup -----------------------------------------------------------------------

phase_backup() {
    say "backup metric (stand-in backup Job)"
    local cid uid out i
    cid=$(sprigctl -o json cluster list 2>/dev/null | jq -r '.clusters[]? | select(.name == "smoke1") | .cluster_id')
    [[ -n $cid ]] || { bad "backup metric" "no cluster smoke1"; return 1; }
    # A SprigCluster for the registered cluster, and the PhysicalBackup
    # (stub CRD) that owns its backup Jobs, as mariadb-operator names it.
    k apply -f - >/dev/null <<EOF
apiVersion: sprigdb.io/v1alpha1
kind: SprigCluster
metadata: {name: smoke-bk}
spec: {topology: single, nodes: 1, mariadb: {image: "mariadb:11.8"}, storage: {size: 1Gi}}
---
apiVersion: k8s.mariadb.com/v1alpha1
kind: PhysicalBackup
metadata: {name: smoke-bk-backup}
spec: {}
EOF
    k patch sprigcluster smoke-bk --subresource=status --type=merge -p "{\"status\":{\"clusterId\":\"$cid\"}}" >/dev/null ||
        { bad "backup metric" "can't set the SprigCluster's status"; return 1; }
    uid=$(k get physicalbackup smoke-bk-backup -o jsonpath='{.metadata.uid}')
    k delete job smoke-bk-backup-1 --ignore-not-found >/dev/null
    k apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: smoke-bk-backup-1
  ownerReferences:
    - {apiVersion: k8s.mariadb.com/v1alpha1, kind: PhysicalBackup, name: smoke-bk-backup, uid: $uid, controller: true}
spec:
  template:
    spec:
      restartPolicy: Never
      containers: [{name: backup, image: node:22-bookworm-slim, imagePullPolicy: Never, command: ["true"]}]
EOF
    k wait --for=condition=Complete job/smoke-bk-backup-1 --timeout=120s >/dev/null || { bad "backup Job" "didn't complete"; return 1; }
    local done_at want
    done_at=$(k get job smoke-bk-backup-1 -o jsonpath='{.status.completionTime}')
    want=$(date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$done_at" +%s 2>/dev/null || date -u -d "$done_at" +%s)
    # The leading operator reads the Jobs once a minute.
    for i in $(seq 1 30); do
        for ip in $(k get pods -l app.kubernetes.io/component=operator -o jsonpath='{.items[*].status.podIP}'); do
            out=$(metric_get "$ip" 2>/dev/null | grep "^sprig_backup_last_success_timestamp_seconds{" | grep "cluster=\"$cid\"")
            [[ -n $out ]] && break 2
        done
        sleep 5
    done
    if [[ -n $out ]] && awk -v w="$want" '{exit !($NF+0 == w+0)}' <<<"$out"; then
        pass "sprig_backup_last_success_timestamp_seconds" "$out"
    else bad "sprig_backup_last_success_timestamp_seconds" "want $want for $cid, got '${out:-nothing}'"; fi
    k delete job smoke-bk-backup-1 --ignore-not-found >/dev/null
    k delete physicalbackup smoke-bk-backup --ignore-not-found >/dev/null
    k delete sprigcluster smoke-bk --ignore-not-found --wait=false >/dev/null
    k patch sprigcluster smoke-bk --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
}

# --- upgrade ----------------------------------------------------------------------

phase_upgrade() {
    say "upgrade from the previous chart layout ($old_ref: controllers as a Deployment)"
    local old=$work/oldchart out
    rm -rf "$old" && mkdir -p "$old"
    git -C "$repo" archive "$old_ref" charts/sprigdb | tar -x -C "$old" || { bad "previous chart" "git archive $old_ref"; return 1; }
    old=$old/charts/sprigdb
    grep -q '^kind: Deployment' "$old/templates/controller.yaml" || { bad "previous chart" "$old_ref has no controller Deployment"; return 1; }

    # A fresh deployment: no release, no etcd data, an empty MariaDB.
    h uninstall sprigdb --wait --timeout 5m >/dev/null 2>&1
    k delete pod smoke-ctl smoke-node smoke-sql --ignore-not-found --wait=true >/dev/null
    k delete pvc data-sprigdb-etcd-dc-0 --ignore-not-found --wait=true >/dev/null
    k delete statefulset smoke-mariadb --wait=true >/dev/null
    k delete pvc data-smoke-mariadb-0 --ignore-not-found --wait=true >/dev/null
    k delete job -l app.kubernetes.io/name=sprigdb --ignore-not-found >/dev/null
    k apply -f "$here/mariadb.yaml" >/dev/null
    k rollout status statefulset/smoke-mariadb --timeout=300s >/dev/null || { bad "test MariaDB" "not ready"; return 1; }
    certificates_for "$old"

    # --no-hooks: the previous settings Job can't finish here (its pods
    # lack the labels the controller NetworkPolicy admits, and its calls
    # aren't sent on to the leader); this chart's Job runs at the upgrade.
    local t0=$SECONDS
    if h install sprigdb "$old" -f "$values" "${image_values[@]}" --no-hooks --wait --timeout 10m >"$work/old-install.log" 2>&1; then
        pass "helm install $old_ref" "$((SECONDS - t0)) s"
    else bad "helm install $old_ref" "$(tail -5 "$work/old-install.log")"; return 1; fi
    test_pods
    # The previous Admin API is plain HTTP inside the cluster.
    out=$(SMOKE_ADMIN_SCHEME=http sprigctl -o json cluster register smoke1 --dc "$dc" --topology single \
        --node "smoke-mariadb.$ns.svc:3306" --credentials file:/etc/sprigdb/creds/smoke1.toml \
        --memory-gb 1 --storage-gb 2 --yes) || { bad "register on $old_ref" "${out:0:300}"; return 1; }
    SMOKE_ADMIN_SCHEME=http sprigctl tenant create "$tenant" --yes >/dev/null
    SMOKE_ADMIN_SCHEME=http sprigctl tenant user add "$tenant" "$user" --password "$(cat "$pw_file")" --if-not-exists --yes >/dev/null
    # The previous chart mounted no client certificate: an operator set the
    # TLS listeners' files by hand (here, the gateways' own certificate).
    SMOKE_ADMIN_SCHEME=http sprigctl settings set tls.bundles.default.cert_path /etc/sprigdb/tls/tls.crt --scope "dc:$dc" --reason smoke >/dev/null
    SMOKE_ADMIN_SCHEME=http sprigctl settings set tls.bundles.default.key_path /etc/sprigdb/tls/tls.key --scope "dc:$dc" --reason smoke >/dev/null
    local i
    for i in $(seq 1 20); do
        out=$(sql 3306 -e "CREATE DATABASE IF NOT EXISTS up_db; CREATE TABLE IF NOT EXISTS up_db.t (id INT PRIMARY KEY); REPLACE INTO up_db.t VALUES (1),(2),(3); SELECT COUNT(*) FROM up_db.t")
        [[ $out == 3 ]] && break
        sleep 3
    done
    [[ $out == 3 ]] && pass "data on $old_ref" "3 rows" || { bad "data on $old_ref" "${out:0:300}"; return 1; }
    k get pods -l app.kubernetes.io/component=controller -o name | sed 's|pod/||' >"$work/old-controllers.txt"

    # This chart's Certificates name more hosts (the controllers' pod
    # names): cert-manager reissues them; here the script does.
    certificates_for "$chart"
    t0=$SECONDS
    if h upgrade sprigdb "$chart" -f "$values" "${image_values[@]}" --wait --timeout 10m >"$work/upgrade.log" 2>&1; then
        pass "helm upgrade to this chart" "$((SECONDS - t0)) s, settings Job done"
    else bad "helm upgrade to this chart" "$(tail -5 "$work/upgrade.log")"; k get pods; return 1; fi
    if k get deployment sprigdb-controller >/dev/null 2>&1; then bad "Deployment removed" "still there"; else pass "Deployment removed"; fi
    out=$(k get statefulset sprigdb-controller -o jsonpath='{.status.readyReplicas}/{.spec.replicas}')
    [[ $out == 2/2 ]] && pass "StatefulSet ready" "$out" || bad "StatefulSet ready" "$out"
    out=$(sql 3306 -e "SELECT COUNT(*) FROM up_db.t")
    [[ $out == 3 ]] && pass "data after the upgrade" "3 rows" || bad "data after the upgrade" "${out:0:300}"
    out=$(sprigctl dc list 2>&1) && pass "Admin API over HTTPS after the upgrade" || bad "Admin API over HTTPS after the upgrade" "${out:0:300}"
    # The Deployment's controllers registered under their pod names; their
    # records go when their etcd leases lapse.
    local keys left t1=$SECONDS
    for i in $(seq 1 40); do
        keys=$(k exec sprigdb-etcd-dc-0 -- etcdctl --endpoints="https://sprigdb-etcd-dc-0.sprigdb-etcd-dc.$ns.svc:2379" \
            --cacert=/etc/etcd/ca/ca.crt --cert=/etc/etcd/tls/tls.crt --key=/etc/etcd/tls/tls.key \
            get --prefix "" --keys-only 2>/dev/null | grep -a 'controllers/')
        left=$(grep -c -f "$work/old-controllers.txt" <<<"$keys")
        [[ $left == 0 ]] && break
        sleep 3
    done
    if [[ $left == 0 ]]; then pass "old controllers' registrations gone" "within $((SECONDS - t1)) s of the upgrade finishing; now $(tr '\n' ' ' <<<"$keys" | sed 's|[^ ]*/controllers/||g')"
    else bad "old controllers' registrations gone" "$left still registered after $((SECONDS - t1)) s"; fi
    out=$(sql 3306 -e "DROP DATABASE up_db" 2>&1)
}

# --- run --------------------------------------------------------------------------

cleanup() {
    if [[ $created_cluster == 1 && ${SMOKE_KEEP:-0} != 1 ]]; then
        "$KIND" delete cluster --name "$cluster" >/dev/null 2>&1
    fi
}
trap cleanup EXIT

printf 'smoke: cluster %s, image %s, work %s\n' "$cluster" "$image" "$work"
image_values=(--set "image.repository=${image%:*}" --set "image.tag=${image##*:}")
if [[ " $phases " != *" install "* ]] && [[ $phases != lint ]]; then
    "$KIND" get kubeconfig --name "$cluster" >"$kcfg" 2>/dev/null || die "no kind cluster $cluster (run the install phase)"
fi
for p in lint install admin clients mtls metrics backup upgrade; do
    want "$p" || continue
    "phase_$p" || fail=1
done
say "result: $([[ $fail == 0 ]] && echo PASS || echo FAIL)"
exit $fail
