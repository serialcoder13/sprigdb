#!/bin/sh
# Runs `docker compose` on the three-DC layout (compose.3dc.yaml) under its
# own project name, so it never touches the single-DC project (sprigdb-dev)
# that `.env` names for compose.yaml.
#
#   ./3dc.sh up -d --build --wait
#   ./3dc.sh latency              # ping matrix between DC subnets
#   ./3dc.sh down -v
#
# Any other arguments go to `docker compose` unchanged.
set -eu

dir=$(cd "$(dirname "$0")" && pwd)
project=${SPRIG3DC_PROJECT:-sprigdb-3dc}

compose() {
    docker compose -p "$project" -f "$dir/compose.3dc.yaml" "$@"
}

# Pings from a probe container with an address in one DC's subnet to the
# etcd of another DC, so the netem filters match; prints the average RTT
# next to the RTT the configured one-way delays add. The probe waits 2 s
# first: netem.sh shapes a new container's veth within a second.
latency() {
    net="${project}_wan"
    image=sprigdb-netem:dev
    count=${COUNT:-5}
    printf '%-12s %10s %12s\n' "path" "added RTT" "measured avg"
    for spec in \
        "mum1 10.10.1.250 mum1 10.10.1.10 0" \
        "mum1 10.10.1.250 fra1 10.10.2.10 ${SPRIG3DC_DELAY_MUM1_FRA1_MS:-30}" \
        "mum1 10.10.1.250 iad1 10.10.3.10 ${SPRIG3DC_DELAY_MUM1_IAD1_MS:-80}" \
        "fra1 10.10.2.250 iad1 10.10.3.10 ${SPRIG3DC_DELAY_FRA1_IAD1_MS:-60}"; do
        # shellcheck disable=SC2086 # split the spec into fields
        set -- $spec
        avg=$(docker run --rm --network "$net" --ip "$2" --entrypoint sh "$image" \
            -c "sleep 2 && ping -c $count -q $4" | awk -F'/' '/min\/avg\/max/ { print $4 }')
        printf '%-12s %7s ms %9s ms\n' "$1-$3" "$(($5 * 2))" "${avg:-?}"
    done
}

case "${1:-}" in
    latency) latency ;;
    *) compose "$@" ;;
esac
