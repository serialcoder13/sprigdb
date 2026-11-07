#!/bin/sh
# Inter-DC latency for the three-DC layout (compose.3dc.yaml; chapter 27,
# "Development setup"; chapter 46, "Integration with Docker Compose").
#
# Runs in the Docker host's network namespace (network_mode: host) with
# NET_ADMIN. Every container on the layout's bridge has a veth on the host
# side; this script gives each veth the same qdisc tree:
#
#   prio 1: (everything -> band 1:1, no delay)
#     1:2 netem DELAY_MUM1_FRA1_MS   u32: src mum1 & dst fra1, or the reverse
#     1:3 netem DELAY_MUM1_IAD1_MS   u32: src mum1 & dst iad1, or the reverse
#     1:4 netem DELAY_FRA1_IAD1_MS   u32: src fra1 & dst iad1, or the reverse
#
# A veth's egress is the traffic going into its container, so a packet
# between two DCs is delayed once, on the way into the receiver: the delay is
# one-way and a ping between DCs shows twice the value. Traffic inside a DC
# is untouched. The script re-checks every second, so containers that start
# or restart later are shaped too, and removes its qdiscs when stopped.
set -u

BRIDGE=${BRIDGE:-sprig3dc0}
MUM1=${NET_MUM1:-10.10.1.0/24}
FRA1=${NET_FRA1:-10.10.2.0/24}
IAD1=${NET_IAD1:-10.10.3.0/24}
D_MUM1_FRA1=${DELAY_MUM1_FRA1_MS:-30}
D_MUM1_IAD1=${DELAY_MUM1_IAD1_MS:-80}
D_FRA1_IAD1=${DELAY_FRA1_IAD1_MS:-60}
JITTER=${JITTER_MS:-0}

veths() {
    ip -o link show master "$BRIDGE" 2>/dev/null | awk -F': ' '{ print $2 }' | cut -d@ -f1
}

netem_args() {
    if [ "$JITTER" -gt 0 ]; then
        echo "delay ${1}ms ${JITTER}ms distribution normal limit 100000"
    else
        echo "delay ${1}ms limit 100000"
    fi
}

pair() { # dev net-a net-b class
    tc filter add dev "$1" parent 1: protocol ip prio 1 u32 \
        match ip src "$2" match ip dst "$3" flowid "$4" &&
    tc filter add dev "$1" parent 1: protocol ip prio 1 u32 \
        match ip src "$3" match ip dst "$2" flowid "$4"
}

# netem_args is meant to split into words.
# shellcheck disable=SC2046
shape() { # dev
    tc qdisc replace dev "$1" root handle 1: prio bands 4 \
        priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 &&
    tc qdisc add dev "$1" parent 1:2 handle 12: netem $(netem_args "$D_MUM1_FRA1") &&
    tc qdisc add dev "$1" parent 1:3 handle 13: netem $(netem_args "$D_MUM1_IAD1") &&
    tc qdisc add dev "$1" parent 1:4 handle 14: netem $(netem_args "$D_FRA1_IAD1") &&
    pair "$1" "$MUM1" "$FRA1" 1:2 &&
    pair "$1" "$MUM1" "$IAD1" 1:3 &&
    pair "$1" "$FRA1" "$IAD1" 1:4
}

cleanup() {
    for dev in $(veths); do
        tc qdisc del dev "$dev" root 2>/dev/null
    done
    rm -f /tmp/netem-ready
    echo "netem: removed shaping from bridge $BRIDGE"
    exit 0
}
trap cleanup TERM INT

echo "netem: bridge $BRIDGE; one-way delays mum1-fra1 ${D_MUM1_FRA1} ms," \
    "mum1-iad1 ${D_MUM1_IAD1} ms, fra1-iad1 ${D_FRA1_IAD1} ms, jitter ${JITTER} ms"
while :; do
    if ip link show "$BRIDGE" >/dev/null 2>&1; then
        for dev in $(veths); do
            if ! tc qdisc show dev "$dev" | grep -q 'qdisc prio 1:'; then
                if shape "$dev"; then
                    echo "netem: shaped $dev"
                else
                    echo "netem: failed to shape $dev" >&2
                    tc qdisc del dev "$dev" root 2>/dev/null
                fi
            fi
        done
        touch /tmp/netem-ready
    else
        echo "netem: waiting for bridge $BRIDGE" >&2
    fi
    sleep 1 &
    wait $!
done
