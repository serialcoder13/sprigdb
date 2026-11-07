# SprigDB

SprigDB is an open-source, multi-model, multi-data-center database built as a Rust layer over MariaDB. It speaks the MySQL, MariaDB, MongoDB, Redis, Neo4j Bolt and Kafka protocols, plus REST and gRPC, and runs without a DBA: placement, sharding, indexing and failover are automatic.

The design lives in [serialcoder13/sprigdb-plan](https://github.com/serialcoder13/sprigdb-plan): a high-level design book and a low-level design book with build steps for every component.

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE).

## Development

You need the Rust toolchain pinned in `rust-toolchain.toml` (rustup installs it on first use) and, for the local environment, Docker with Compose v2.

### Build and test

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
cargo deny check          # needs `cargo install cargo-deny`
```

CI (`.github/workflows/ci.yml`) runs the same checks on every push.

### Run a role

The server is one binary; the role is a subcommand (`gateway`, `controller`, `worker`, `agent`, `operator`). Each role serves `/healthz`, `/readyz` and `/metrics` on port 9180 and drains on SIGTERM. The roles are skeletons for now: they start, report ready and shut down cleanly.

```sh
SPRIGDB_NODE_DC=dc-a SPRIGDB_TELEMETRY_LOG_FORMAT=pretty cargo run -p sprigdb -- gateway
curl localhost:9180/readyz
```

Configuration comes from `sprigdb.toml` (`--config`, or `./sprigdb.toml` if present) with `SPRIGDB_<SECTION>_<FIELD>` environment overrides. A minimal file:

```toml
[node]
dc = "dc-a"

[telemetry]
log_format = "pretty"            # json (default) or pretty
bootstrap_addr = "127.0.0.1:9180"
```

### Local environment

`deploy/compose/` runs etcd and MariaDB with the binlog settings SprigDB requires. Pick a topology with a profile:

```sh
cd deploy/compose
docker compose --profile single up -d --wait        # one node
docker compose --profile replication up -d --wait   # primary + 2 replicas, semi-sync AFTER_SYNC
docker compose --profile galera up -d --wait        # 3-node Galera
docker compose --profile all down -v                # stop and delete the data
```

| Service | Host port |
|---|---|
| etcd | 2379 |
| single | 13301 |
| primary, replica-1, replica-2 | 13311, 13312, 13313 |
| galera-1, galera-2, galera-3 | 13321, 13322, 13323 |

MariaDB 11.8 is the default; use `MARIADB_IMAGE=mariadb:12.3 docker compose ...` for 12.3. Development credentials are in `deploy/compose/.env` (root password `sprig-dev-root`); never reuse them.

### Three data centers

`deploy/compose/compose.3dc.yaml` simulates three DCs on one machine, each with its own etcd (the DC tier) and a MariaDB cluster in a different topology, plus a three-member global etcd tier with one member per DC. A `netem` service adds latency between the DCs' subnets with `tc netem`. Run it through `3dc.sh`, which always uses the project name `sprigdb-3dc` (the `.env` file's project name belongs to the single-DC setup):

```sh
cd deploy/compose
./3dc.sh up -d --build --wait     # about a minute; builds the small netem image first
./3dc.sh latency                  # ping between the DCs
./3dc.sh down -v                  # stop and delete the data
```

| DC | Subnet | MariaDB cluster | Ports: DC etcd, global etcd, MariaDB |
|---|---|---|---|
| mum1 | 10.10.1.0/24 | `mum1-c1`: primary + 2 replicas, semi-sync AFTER_SYNC | 34179, 34180, 34106–34108 |
| fra1 | 10.10.2.0/24 | `fra1-c1`: 3-node Galera | 34279, 34280, 34206–34208 |
| iad1 | 10.10.3.0/24 | `iad1-c1`: single node | 34379, 34380, 34306 |

One-way delays follow chapter 46: mum1–fra1 30 ms, mum1–iad1 80 ms, fra1–iad1 60 ms, so a ping shows twice that. Change them with `SPRIG3DC_DELAY_MUM1_FRA1_MS`, `SPRIG3DC_DELAY_MUM1_IAD1_MS`, `SPRIG3DC_DELAY_FRA1_IAD1_MS` and `SPRIG3DC_JITTER_MS`. Traffic from the host through published ports isn't delayed. The layout can run next to the single-DC one but only once per Docker host.

### Integration tests

Integration tests use `sprig-testkit` and skip themselves, with a message, unless the variable naming their service is set. Point them at the single-DC environment with:

```sh
export SPRIG_TEST_ETCD=http://127.0.0.1:2379
export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
export SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,mysql://root:sprig-dev-root@127.0.0.1:13312,mysql://root:sprig-dev-root@127.0.0.1:13313
export SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321,mysql://root:sprig-dev-root@127.0.0.1:13322,mysql://root:sprig-dev-root@127.0.0.1:13323
export SPRIG_TEST_DOCKER=1        # tests that start their own Compose projects
cargo test --workspace
```

A list holds the writer first. `SPRIG_TEST_ENV_FILE=deploy/compose/env.3dc.toml` describes the three-DC layout instead (`sprig_testkit::Env`). `SPRIG_TEST_STRICT=1` turns a skip into a failure, as in CI; `SPRIG_TEST_KEEP=1` leaves Compose projects that tests start running after the test. CI runs the stages of chapter 46: S0 (format, lint, unit tests, `cargo deny`) on every push, S1 (integration tests against Compose) on pull requests and `main`, and S2 (the three-DC layout on MariaDB 11.8 and 12.3) nightly or on demand.
