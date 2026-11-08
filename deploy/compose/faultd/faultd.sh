#!/bin/sh
# faultd commands. <pid> is the target container's PID in the host PID
# namespace (`docker inspect -f '{{.State.Pid}}' <container>`); every rule
# carries the comment sprig-faultd so `heal` removes exactly faultd's own.
#
#   faultd.sh idle                          keep the container running
#   faultd.sh partition <pid> <peer-ip>     drop traffic to and from peer
#   faultd.sh delay <pid> <ms> [jitter-ms] [loss-%]   netem on eth0
#   faultd.sh heal <pid>                    remove faultd's rules and netem
#
# No clock skew: containers share the Docker VM's wall clock, and time
# namespaces don't virtualise CLOCK_REALTIME, so faultd can't move one
# container's clock. sprig-testkit skews throwaway containers with
# libfaketime instead (sprig_testkit::faults::faketime).
set -eu

in_ns() {
    pid=$1
    shift
    nsenter -t "$pid" -n "$@"
}

case "${1:-idle}" in
    idle)
        trap 'exit 0' TERM INT
        while :; do sleep 3600 & wait $!; done
        ;;
    partition)
        in_ns "$2" iptables -I INPUT -s "$3" -m comment --comment sprig-faultd -j DROP
        in_ns "$2" iptables -I OUTPUT -d "$3" -m comment --comment sprig-faultd -j DROP
        ;;
    delay)
        in_ns "$2" tc qdisc replace dev eth0 root netem delay "${3}ms" "${4:-0}ms" loss "${5:-0}%"
        ;;
    heal)
        in_ns "$2" sh -c '
            for chain in INPUT OUTPUT; do
                while n=$(iptables -L "$chain" --line-numbers -n | awk "/sprig-faultd/ { print \$1; exit }"); [ -n "$n" ]; do
                    iptables -D "$chain" "$n"
                done
            done'
        in_ns "$2" tc qdisc del dev eth0 root 2>/dev/null || true
        ;;
    *)
        echo "faultd: unknown command $1" >&2
        exit 2
        ;;
esac
