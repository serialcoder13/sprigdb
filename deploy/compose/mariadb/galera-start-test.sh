#!/bin/bash
# Tests galera-start.sh's bootstrap decision (bugfix-41) without Docker:
#
#   deploy/compose/mariadb/galera-start-test.sh
#
# Each case writes a grastate.dat, maybe a gvwstate.dat, and the nodes'
# position records into a temporary directory, runs the script's `decide`
# mode as galera-1 (or another node), and compares the first word of the
# answer (bootstrap or join).
set -u
here=$(cd "$(dirname "$0")" && pwd)
U=7b303cab-27d6-418a-afba-d9b4cd9764f0
V=11111111-2222-3333-4444-555555555555
fails=0
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# touch -d isn't portable: ages in seconds via perl's utime.
age() { perl -e 'utime(time - $ARGV[0], time - $ARGV[0], $ARGV[1]) or die' "$2" "$1"; }

# case <name> <want> <role> <node> <grastate: uuid seqno safe> [gvw] -- [record node uuid seqno age]...
case_() {
    local name=$1 want=$2 role=$3 node=$4 gu=$5 gs=$6 safe=$7
    shift 7
    local d=$tmp/$name
    mkdir -p "$d/data" "$d/shared"
    if [ "$gu" != none ]; then
        printf '# GALERA saved state\nversion: 2.1\nuuid:    %s\nseqno:   %s\nsafe_to_bootstrap: %s\n' \
            "$gu" "$gs" "$safe" > "$d/data/grastate.dat"
    fi
    if [ "${1:-}" = gvw ]; then
        echo "my_uuid: x" > "$d/data/gvwstate.dat"
        shift
    fi
    [ "${1:-}" = -- ] && shift
    while [ $# -ge 4 ]; do
        echo "$2 $3" > "$d/shared/$1"
        age "$d/shared/$1" "$4"
        shift 4
    done
    local got
    got=$(SPRIG_GALERA_DATADIR=$d/data SPRIG_GALERA_SHARED=$d/shared SPRIG_GALERA_NODE=$node \
        SPRIG_GALERA_RECOVERED=${RECOVERED:-} bash "$here/galera-start.sh" "$role" 2>"$d/log")
    if [ "${got%%:*}" = "$want" ]; then
        echo "ok   $name: $got"
    else
        echo "FAIL $name: want $want, got '$got'"
        sed 's/^/     /' "$d/log"
        fails=$((fails + 1))
    fi
}

# Three nodes stopped together cleanly: equal positions, fresh records.
case_ clean_together bootstrap decide galera-1 $U 26 0 -- \
    galera-1 $U 26 5 galera-2 $U 26 5 galera-3 $U 26 5
# The same as a joining node: never bootstraps by itself.
case_ joiner_never join decide-join galera-2 $U 26 0 -- \
    galera-1 $U 26 5 galera-2 $U 26 5 galera-3 $U 26 5
# Another node's record is ahead: it may hold commits this one lacks.
case_ peer_ahead join decide galera-1 $U 26 0 -- \
    galera-1 $U 26 5 galera-2 $U 27 5 galera-3 $U 26 5
# A record is missing: unknown position.
case_ peer_missing join decide galera-1 $U 26 0 -- \
    galera-1 $U 26 5 galera-2 $U 26 5
# A record is stale (written long before this node's last one).
case_ peer_stale join decide galera-1 $U 26 0 -- \
    galera-1 $U 26 5 galera-2 $U 20 3600 galera-3 $U 26 5
# A record of another cluster.
case_ peer_other_cluster join decide galera-1 $U 26 0 -- \
    galera-1 $U 26 5 galera-2 $V 30 5 galera-3 $U 26 5
# No record of this node itself.
case_ own_missing join decide galera-1 $U 26 0 -- \
    galera-2 $U 26 5 galera-3 $U 26 5
# The all-zero UUID never bootstraps by rule 2 (HD1's procedure instead).
case_ nil_uuid join decide galera-1 00000000-0000-0000-0000-000000000000 26 0 -- \
    galera-1 $U 26 5 galera-2 $U 26 5 galera-3 $U 26 5
# Marked safe_to_bootstrap (the last to leave): bootstraps, whatever the node.
case_ marked_safe bootstrap decide galera-3 $U 30 1 -- \
    galera-1 $U 26 600 galera-3 $U 30 5
# Every node crashed: pc.recovery, never a bootstrap.
case_ all_crashed join decide galera-1 $U -1 0 gvw -- \
    galera-1 $U 26 5 galera-2 $U 26 5 galera-3 $U 26 5
# A crash without gvwstate.dat: the recovered position decides.
RECOVERED=$U:26 case_ crash_recovered bootstrap decide galera-1 $U -1 0 -- \
    galera-1 $U 25 5 galera-2 $U 26 5 galera-3 $U 26 5
RECOVERED=$U:24 case_ crash_recovered_behind join decide galera-1 $U -1 0 -- \
    galera-1 $U 24 5 galera-2 $U 26 5 galera-3 $U 26 5
# A fresh volume: the first node bootstraps, the others join.
case_ fresh_first bootstrap decide galera-1 none - -
case_ fresh_join join decide-join galera-2 none - -

if [ "$fails" -gt 0 ]; then
    echo "$fails case(s) failed"
    exit 1
fi
echo "all cases passed"
