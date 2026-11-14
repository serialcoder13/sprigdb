#!/bin/bash
# Starts, and health-checks, one node of the dev Galera cluster
# (compose.yaml, services galera-1 to galera-3):
#
#   sprig-galera-start.sh first|join [mariadbd options...]   # entrypoint
#   sprig-galera-start.sh health                             # healthcheck
#
# A full restart of the three nodes (the laptop rebooting, Docker Desktop
# restarting, all three stopped at once) must bring the cluster back by
# itself, with its state UUID. Two things went wrong before:
#
# 1. After a crash every node's grastate.dat says seqno -1 and the server
#    doesn't know its position. Started like that, every node offered
#    00000000-...:-1, and the primary component that Galera's pc.recovery
#    restored from gvwstate.dat got an all-zero state UUID ("Quorum: No node
#    with complete state"), which no node can join again later. So the
#    position is first recovered from InnoDB, as Galera's galera_recovery
#    script does (mariadbd --wsrep-recover), and the server starts with
#    --wsrep-start-position: the nodes agree on the real UUID.
# 2. When all three leave cleanly at the same time, none is marked
#    safe_to_bootstrap and gvwstate.dat is gone, so nobody formed a primary
#    component and every node aborted after pc.wait_prim_timeout, over and
#    over. Now each healthy node records its last committed position in
#    the shared volume /sprig-galera (the healthcheck writes
#    /sprig-galera/<node> while it is in the primary component), and the
#    first node bootstraps only when every other node has a record, of the
#    same cluster, written no more than 60 s before its own last one, and
#    none of them is ahead of its own position. Nodes that leave together
#    have the same position (replication is synchronous). A node that left
#    earlier is behind, and then the last one to leave is marked
#    safe_to_bootstrap and bootstraps instead. A record that is missing,
#    of another cluster, stale or ahead means this node may lack commits:
#    it doesn't bootstrap, and waits (pc.wait_prim_timeout, then restarts)
#    for another node or an operator (deploy/compose-vm/RUNBOOK.md, "Galera:
#    a node that can't rejoin").
#
# The shared volume works only because the three nodes run on one Docker
# host: this is for the dev stack (deploy/compose) only. On separate hosts
# the safe procedure is galera_recovery on each node, then a bootstrap of
# the most advanced node chosen by an operator or the controller.
#
# Bootstrap rules, in order: safe_to_bootstrap: 1 bootstraps; a
# gvwstate.dat (a crash of every node) joins and lets pc.recovery restore
# the primary component; the first node bootstraps on a fresh volume, or by
# rule 2; everything else joins.
#
# Testing: `decide` (as the first node) and `decide-join` print the
# decision without starting anything, with
# SPRIG_GALERA_DATADIR, SPRIG_GALERA_SHARED, SPRIG_GALERA_NODE and (for a
# seqno -1) SPRIG_GALERA_RECOVERED standing in for the container's
# (galera-start-test.sh).
set -u
role=${1:-join}
shift || true
dir=${SPRIG_GALERA_DATADIR:-/var/lib/mysql}
g=$dir/grastate.dat
shared=${SPRIG_GALERA_SHARED:-/sprig-galera}
me=${SPRIG_GALERA_NODE:-$(hostname)}
nodes=${SPRIG_GALERA_NODES:-galera-1 galera-2 galera-3}
nil=00000000-0000-0000-0000-000000000000
say() { echo "sprig-galera-start: $*" >&2; }

if [ "$role" = health ]; then
    s=$(mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" -Nse \
        "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS
         WHERE VARIABLE_NAME IN ('wsrep_local_state', 'wsrep_cluster_status',
                                 'wsrep_cluster_state_uuid', 'wsrep_last_committed')" 2>/dev/null) || exit 1
    v() { echo "$s" | awk -v k="$1" 'toupper($1) == k {print $2}'; }
    state=$(v WSREP_LOCAL_STATE)
    status=$(v WSREP_CLUSTER_STATUS)
    uuid=$(v WSREP_CLUSTER_STATE_UUID)
    seqno=$(v WSREP_LAST_COMMITTED)
    # Recorded while in the primary component (a donor or a desynced node
    # still commits), never an all-zero UUID.
    if [ "$status" = Primary ] && [ -d "$shared" ] && [ -n "$uuid" ] && [ "$uuid" != "$nil" ] &&
        [ -n "$seqno" ]; then
        printf '%s %s\n' "$uuid" "$seqno" > "$shared/.$me.tmp" && mv -f "$shared/.$me.tmp" "$shared/$me"
    fi
    [ "$state" = 4 ]
    exit
fi

# The node's position: grastate.dat after a clean stop, InnoDB after a crash.
uuid=$(sed -n 's/^uuid: *//p' "$g" 2>/dev/null)
seqno=$(sed -n 's/^seqno: *//p' "$g" 2>/dev/null)
if [ -s "$g" ] && [ "$seqno" = -1 ]; then
    if [ "$role" = decide ] || [ "$role" = decide-join ]; then
        pos=${SPRIG_GALERA_RECOVERED:-}
    else
        rec=/tmp/sprig-wsrep-recover.log
        rm -f "$rec"
        gosu mysql mariadbd "$@" --wsrep-recover --log-error="$rec" >/dev/null 2>&1 || true
        pos=$(sed -n 's/.*WSREP: Recovered position: *\([0-9a-f-]*:-\{0,1\}[0-9]*\).*/\1/p' "$rec" 2>/dev/null | tail -n 1)
    fi
    case $pos in
    '' | *:-1 | "$nil":*)
        say "no usable position recovered (${pos:-none}); starting without one"
        ;;
    *)
        say "recovered position $pos after an unclean stop"
        uuid=${pos%%:*}
        seqno=${pos##*:}
        set -- --wsrep-start-position="$pos" "$@"
        ;;
    esac
fi

mtime() { stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null; }

# Whether this node may bootstrap by rule 2 (see the top): its own record
# and every other node's exist, are of its cluster, were written within
# 60 s of each other, and none is ahead of its position.
not_behind() {
    case $uuid in '' | "$nil") say "no cluster UUID: not bootstrapping"; return 1 ;; esac
    [ "${seqno:--1}" -ge 0 ] 2>/dev/null || { say "no position: not bootstrapping"; return 1; }
    local mine n f u s t
    [ -f "$shared/$me" ] || { say "no record of this node: not bootstrapping"; return 1; }
    mine=$(mtime "$shared/$me")
    for n in $nodes; do
        [ "$n" = "$me" ] && continue
        f=$shared/$n
        [ -f "$f" ] || { say "no record of $n: not bootstrapping"; return 1; }
        read -r u s < "$f" || { say "unreadable record of $n: not bootstrapping"; return 1; }
        [ "$u" = "$uuid" ] || { say "$n was last in cluster $u: not bootstrapping"; return 1; }
        t=$(mtime "$f")
        [ "$t" -ge $((mine - 60)) ] || { say "$n's record is $((mine - t)) s older than this node's: not bootstrapping"; return 1; }
        [ "$s" -le "$seqno" ] 2>/dev/null || { say "$n was last at $s, past this node's $seqno: not bootstrapping"; return 1; }
    done
    return 0
}

boot=
if grep -qs '^safe_to_bootstrap: *1' "$g"; then
    boot="marked safe_to_bootstrap"
elif [ -s "$dir/gvwstate.dat" ]; then
    say "every node crashed: pc.recovery restores the primary component"
elif { [ "$role" = first ] || [ "$role" = decide ]; } && [ ! -s "$g" ]; then
    boot="fresh volume"
elif { [ "$role" = first ] || [ "$role" = decide ]; } && not_behind; then
    boot="no node is marked safe_to_bootstrap and this node ($uuid:$seqno) is not behind any other"
    # Galera refuses --wsrep-new-cluster without the mark.
    [ "$role" = first ] && gosu mysql sed -i 's/^safe_to_bootstrap: *0/safe_to_bootstrap: 1/' "$g"
fi
case $role in decide | decide-join)
    if [ -n "$boot" ]; then echo "bootstrap: $boot"; else echo join; fi
    exit 0
    ;;
esac
if [ -n "$boot" ]; then
    say "bootstrapping a new primary component: $boot"
    set -- --wsrep-new-cluster "$@"
fi

exec docker-entrypoint.sh mariadbd "$@"
