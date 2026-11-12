#!/bin/bash
# Starts one node of the dev Galera cluster (compose.yaml, services galera-1
# to galera-3; chapter 27):
#
#   sprig-galera-start.sh first|join [mariadbd options...]
#
# 1. After a crash (the laptop rebooted, Docker stopped, an OOM kill),
#    grastate.dat says seqno -1 and the server doesn't know its position.
#    Started like that, every node offers 00000000-...:-1 and the cluster
#    that Galera's pc.recovery restores from gvwstate.dat has an all-zero
#    state UUID ("Quorum: No node with complete state"), which no node can
#    join again later (decision HD1; bugfix-41). So first ask InnoDB for the
#    position, as Galera's galera_recovery script does (mariadbd
#    --wsrep-recover), and start with --wsrep-start-position: the nodes then
#    agree on the real UUID and the most advanced node's position.
# 2. Bootstraps a new primary component only when it should: a node that
#    Galera marked safe_to_bootstrap: 1 (the last to leave a cleanly stopped
#    cluster, whichever node that was), or the first node on a fresh volume
#    (no grastate.dat). Otherwise it joins: a restarted or recreated node
#    rejoins instead of splitting the cluster, and after a crash of every
#    node pc.recovery brings the primary component back by itself.
set -u
role=${1:-join}
shift || true
g=/var/lib/mysql/grastate.dat
say() { echo "sprig-galera-start: $*" >&2; }

if [ -s "$g" ] && grep -q '^seqno: *-1' "$g"; then
    rec=/tmp/sprig-wsrep-recover.log
    rm -f "$rec"
    gosu mysql mariadbd "$@" --wsrep-recover --log-error="$rec" >/dev/null 2>&1 || true
    pos=$(sed -n 's/.*WSREP: Recovered position: *\([0-9a-f-]*:-\{0,1\}[0-9]*\).*/\1/p' "$rec" 2>/dev/null | tail -n 1)
    case $pos in
    '' | *:-1 | 00000000-0000-0000-0000-000000000000:*)
        say "no usable position recovered (${pos:-none}); starting without one"
        ;;
    *)
        say "recovered position $pos after an unclean stop"
        set -- --wsrep-start-position="$pos" "$@"
        ;;
    esac
fi

if grep -qs '^safe_to_bootstrap: *1' "$g" || { [ "$role" = first ] && [ ! -s "$g" ]; }; then
    say "bootstrapping a new primary component"
    set -- --wsrep-new-cluster "$@"
fi

exec docker-entrypoint.sh mariadbd "$@"
