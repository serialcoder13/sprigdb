#!/usr/bin/env bash
# Builds the sprigdb image for the Compose-on-VM bundle.
#
#   deploy/compose-vm/build-image.sh <tag> [source dir]
#
# Two steps, so the compile's memory is capped (a release build of the
# workspace needs about 5-6 GB; BuildKit ignores `docker build --memory`):
#   1. compile in rust:1.94.0-slim-bookworm with `docker run -m`, into a
#      cache volume (sprigdb-build-target) and out/ next to this script;
#   2. `docker build` a debian:bookworm-slim image that copies the two
#      binaries (Dockerfile.runtime).
# On a CI runner with enough memory, deploy/compose-vm/Dockerfile does both
# in one `docker build`.
#
# Environment: SPRIG_BUILD_MEM (default 6g), SPRIG_BUILD_JOBS (default 2),
# SPRIG_BUILD_CGU (codegen units, default 16),
# SPRIG_BUILD_TARGET_VOLUME, SPRIG_BUILD_CARGO_VOLUME (cache volume names).
set -euo pipefail
tag=${1:?usage: build-image.sh <tag> [source dir]}
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
src=$(cd "${2:-$here/../..}" && pwd)
mem=${SPRIG_BUILD_MEM:-6g}
jobs=${SPRIG_BUILD_JOBS:-2}
# The workspace's release profile has codegen-units = 1 and thin LTO; with one
# codegen unit, sprig-gateway alone needs more than 6 GB to compile (killed
# at 6 GB on 11 Nov). 16 units keep thin LTO and fit in 6 GB. Set
# SPRIG_BUILD_CGU=1 on a builder with 12 GB or more for the exact profile.
cgu=${SPRIG_BUILD_CGU:-16}
tvol=${SPRIG_BUILD_TARGET_VOLUME:-sprigdb-build-target}
cvol=${SPRIG_BUILD_CARGO_VOLUME:-sprigdb-build-cargo}
name=sprigdb-build-$$
out=$here/out/$(printf %s "$tag" | tr ':/' '__')
mkdir -p "$out"

echo "build-image: compiling $src in a container (memory $mem, $jobs jobs)"
docker run --rm --name "$name" -m "$mem" --memory-swap "$mem" \
    -v "$src":/src:ro -v "$tvol":/target -v "$cvol":/usr/local/cargo/registry \
    -v "$out":/out -e CARGO_TARGET_DIR=/target -e CARGO_PROFILE_RELEASE_DEBUG=false \
    -e CARGO_INCREMENTAL=0 -e CARGO_PROFILE_RELEASE_CODEGEN_UNITS="$cgu" -w /src \
    "${RUST_IMAGE:-rust:1.94.0-slim-bookworm}" bash -c "
        set -e
        [ -e /usr/include/google/protobuf/descriptor.proto ] || { apt-get update -qq && apt-get install -y -qq --no-install-recommends protobuf-compiler libprotobuf-dev make pkg-config >/dev/null; }
        # The cache volume is shared between source trees mounted at the same
        # path, and cargo judges freshness by file times: a tree whose files
        # are older than the last build would reuse that build's crates. Forget
        # the workspace's own crates (all named sprig*); dependencies stay cached.
        rm -rf /target/release/.fingerprint/sprig*
        cargo build --release --locked -j $jobs -p sprigdb -p sprigctl
        install -m 0755 /target/release/sprigdb /target/release/sprigctl /out/"

echo "build-image: packing $tag"
docker build -q -f "$here/Dockerfile.runtime" --build-arg SPRIGDB_VERSION="$tag" -t "$tag" "$out"
docker image inspect -f '{{.Id}} {{.Size}}' "$tag"
