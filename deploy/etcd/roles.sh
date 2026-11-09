#!/bin/sh
# etcd users and roles of chapter 23 ("Security notes"): one user per role,
# named as the role's certificate common name, each limited to its key
# prefixes. Run once per etcd, as etcd's root user, before `auth enable`.
#
#   roles.sh dc <dc_id>            # a DC tier's etcd
#   roles.sh global <dc_id>...     # the global tier's etcd, every DC named
#
# ETCDCTL is the etcdctl command (default `etcdctl`; for a container,
# ETCDCTL="docker exec <name> etcdctl"). With mTLS (production) users have
# no password: etcd takes the user from the client certificate's CN. Set
# SPRIG_ETCD_TEST_PASSWORD to give every user that password instead (tests
# without certificates).
#
# Until every global write goes through the global leader's `Global`
# service (chapter 23 step 10: R2 has PromotionDone only), DC leaders still
# write the directory, global settings and dc records themselves, and
# gateways and workers read the global tier: `sprig-controller` therefore
# also gets the `sprig-global` role and `sprig-gateway`, `sprig-worker` the
# `sprig-global-read` role in the global etcd. Drop those grants once the
# `Global` service carries those writes and the relay carries the reads.
set -eu

ETCDCTL=${ETCDCTL:-etcdctl}
tier=${1:?"usage: roles.sh dc <dc_id> | global <dc_id>..."}
shift

e() { $ETCDCTL "$@" >/dev/null; }

user() {
    if [ -n "${SPRIG_ETCD_TEST_PASSWORD:-}" ]; then
        e user add "$1:$SPRIG_ETCD_TEST_PASSWORD" 2>/dev/null || true
    else
        e user add "$1" --no-password 2>/dev/null || true
    fi
}

role() { e role add "$1" 2>/dev/null || true; }
grant() { e role grant-permission "$1" "$2" "$3" --prefix=true; }
bind() { e user grant-role "$1" "$2"; }

e user add "root:${SPRIG_ETCD_ROOT_PASSWORD:-${SPRIG_ETCD_TEST_PASSWORD:-root}}" 2>/dev/null || true
e user grant-role root root || true

case "$tier" in
dc)
    dc=${1:?"roles.sh dc <dc_id>"}
    p="/sprig/dc/$dc/"
    role sprig-gateway
    grant sprig-gateway read "$p"
    grant sprig-gateway readwrite "${p}gateways/"
    grant sprig-gateway readwrite "${p}gateway-slots/"
    role sprig-worker
    grant sprig-worker read "$p"
    grant sprig-worker readwrite "${p}jobs/"
    grant sprig-worker readwrite "${p}workers/"
    role sprig-agent
    grant sprig-agent read "${p}clusters/"
    grant sprig-agent read "${p}controllers/"
    grant sprig-agent readwrite "${p}leases/writer/"
    grant sprig-agent readwrite "${p}agents/"
    role sprig-controller
    grant sprig-controller readwrite "$p"
    for u in sprig-gateway sprig-worker sprig-agent sprig-controller; do
        user "$u"
        bind "$u" "$u"
    done
    ;;
global)
    [ $# -gt 0 ] || { echo "roles.sh global <dc_id>..." >&2; exit 2; }
    g=/sprig/g/
    role sprig-global
    grant sprig-global readwrite "$g"
    role sprig-global-read
    grant sprig-global-read read "$g"
    for dc in "$@"; do
        r="sprig-dc-$dc"
        role "$r"
        grant "$r" read "$g"
        e role grant-permission "$r" readwrite "${g}dc-leases/$dc"
        e role grant-permission "$r" readwrite "${g}dc-load/$dc"
        e role grant-permission "$r" readwrite "${g}dc-observe/$dc"
        grant "$r" readwrite "${g}latency/$dc/"
        grant "$r" readwrite "${g}token-keys/$dc/"
        user "$r"
        bind "$r" "$r"
    done
    # Global candidates hold sprig-global; until the Global service carries
    # every global write, every controller does (see the header).
    user sprig-controller
    bind sprig-controller sprig-global
    for u in sprig-gateway sprig-worker; do
        user "$u"
        bind "$u" sprig-global-read
    done
    ;;
*)
    echo "roles.sh dc <dc_id> | global <dc_id>..." >&2
    exit 2
    ;;
esac
echo "roles for the $tier tier written; enable with: $ETCDCTL auth enable"
