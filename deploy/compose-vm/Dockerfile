# syntax=docker/dockerfile:1
# The sprigdb image for the Compose-on-VM bundle (deploy/compose-vm): one
# image runs every role (gateway, controller, worker); the command picks it.
#
#   docker build -f deploy/compose-vm/Dockerfile -t sprigdb:<version> .
#
# Build from the repository root. The build stage needs about 6 GB of memory
# for a release build of the workspace; set it with `docker build --memory`
# on a builder that allows it, or build on the CI runner. protoc comes from
# Debian's protobuf-compiler and libprotobuf-dev (BSD-3-Clause; the latter
# for the well-known .proto files); etcd-client's build script needs
# it (decision R12 used a stand-in; this replaces it).
ARG RUST_IMAGE=rust:1.94.0-slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev make pkg-config \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
ENV CARGO_PROFILE_RELEASE_DEBUG=false CARGO_TARGET_DIR=/target
ARG CARGO_BUILD_JOBS=4
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/target \
    cargo build --release --locked -j "${CARGO_BUILD_JOBS}" -p sprigdb -p sprigctl \
 && install -D -m 0755 /target/release/sprigdb /out/sprigdb \
 && install -D -m 0755 /target/release/sprigctl /out/sprigctl

FROM ${RUNTIME_IMAGE}
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 sprigdb \
 && useradd --system --uid 10001 --gid sprigdb --home-dir /var/lib/sprigdb --create-home sprigdb \
 && install -d -o sprigdb -g sprigdb /etc/sprigdb /var/lib/sprigdb /var/log/sprigdb
COPY --from=build /out/sprigdb /out/sprigctl /usr/bin/
ARG SPRIGDB_VERSION=dev
LABEL org.opencontainers.image.title="sprigdb" \
      org.opencontainers.image.version="${SPRIGDB_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"
USER sprigdb
WORKDIR /var/lib/sprigdb
# compose.yaml sets `init: true` (Docker's own init reaps and forwards signals).
ENTRYPOINT ["/usr/bin/sprigdb"]
