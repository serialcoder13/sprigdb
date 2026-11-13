---
title: How SprigDB works
audience: [user, dba, operator]
sidebar:
  order: 1
---

# How SprigDB works

SprigDB is a database layer in front of MariaDB. Applications connect to it with the clients they already use: MySQL and MariaDB drivers, MongoDB drivers, Redis clients, Neo4j drivers, Kafka and AMQP clients, or plain HTTP. SprigDB stores everything in MariaDB and decides where it lives.

It does four jobs that otherwise need a DBA and an operations team:

- **Placement.** It decides which MariaDB cluster, and which data centre, holds each tenant's data.
- **Sharding.** It splits data across clusters as it grows.
- **Indexing.** It creates indexes from the queries it sees.
- **Failover.** It moves writes to a healthy copy when a node or a whole data centre fails.

## The pieces

```text
  MySQL    MariaDB   MongoDB   Redis   Bolt   Kafka   AMQP   HTTP (REST, gRPC)
    │         │         │        │      │      │       │         │
┌───▼─────────▼─────────▼────────▼──────▼──────▼───────▼─────────▼──────────┐
│ Gateways   one front end per protocol: TLS, login, the tenant, translation│
│            into one request shape                                         │
│            logic: functions, validations, triggers (KIRun, JS, Lua, SQL)  │
│            execution: routing to shards, merging, joins across models,    │
│            transactions across shards                                     │
└───────────────┬──────────────────────────────────────┬────────────────────┘
                │ SQL                                  │ binlog
┌───────────────▼────────────────┐   ┌─────────────────▼─────────────────────┐
│ MariaDB clusters, any topology │   │ Change feed: moves, search indexing,  │
│ shards of many tenants         │   │ triggers, streams, subscriptions      │
└────────────────────────────────┘   └───────────────────────────────────────┘
 Control plane (etcd): clusters, shard maps, leases, settings, jobs.
 One tier per data centre, plus a small global tier.
```

**The `sprigdb` binary** runs in one of five roles, chosen by a subcommand:

| Role | Where it runs | What it does |
|---|---|---|
| `sprigdb gateway` | Every data centre, as many as you need; stateless | All client listeners, logins, translation, routing, short logic, workload capture |
| `sprigdb controller` | Every data centre; one leader per data centre | Probing clusters, placement, the sharding advisor, automatic indexing decisions, failover, scheduling jobs |
| `sprigdb worker` | Every data centre, as many as you need | Background jobs: moving shards, building and validating indexes, search indexing, asynchronous triggers and schedules |
| `sprigdb agent` | Beside MariaDB nodes that SprigDB installs on VMs | Installing MariaDB, health, self-fencing |
| `sprigdb operator` | One per Kubernetes cluster | Turns SprigDB resources into MariaDB resources |

`sprigctl` is the operator's command-line tool, and `sprig` is the shell for everyone. [Studio](../tools/studio.md) is the browser workspace, served by every gateway.

The only other moving parts are **MariaDB** (11.8 or 12.3) and **etcd**.

## Ports

| Port | Listener | On by default | TLS by default |
|---|---|---|---|
| 3306 | MySQL 8.4 persona | yes | required |
| 3307 | MariaDB persona | yes | required |
| 8080 | HTTP: REST, gRPC and Studio | yes | off |
| 8443 | HTTPS: the same, with TLS | yes | required |
| 27017 | MongoDB | no | required |
| 6379 | Redis | no | required |
| 7687 | Neo4j Bolt | no | required |
| 9092 | Kafka | no | required |
| 5672 | AMQP 0-9-1 | no | off |
| 5671 | AMQP 0-9-1 over TLS | no | required |
| 9180 | `/metrics`, `/healthz`, `/readyz` | yes | off |

Turn a listener on with its `listener.<name>.enabled` setting. Each listener also has its own bind address (`listener.<name>.bind`) and TLS mode (`listener.<name>.tls`: `off`, `optional` or `required`), and changes apply live, without a restart. See [Listeners and TLS](../security/tls.md).

## How data is laid out

A **tenant** owns one or more **databases**. Inside a database, objects of every model are grouped into **table groups** that share a **distribution key**: tables, collections, key-value namespaces and Redis databases. A table group is cut into **shards**, and each shard is one MariaDB schema on one cluster.

```text
tenant "acme"
├── catalog schema  t42_sprig                              on cluster A1
└── database "shop"
    ├── table group 3   key: customer id
    │   customers, orders (tables), events (collection), sessions (Redis)
    │   ├── shard s0   schema t42_d1_g3_s0                   on cluster A1
    │   └── shard s1   schema t42_d1_g3_s1                   on cluster A2
    ├── table group 4   key: product id
    │   products, product_embeddings (vectors)
    │   └── shard s0   schema t42_d1_g4_s0                   on cluster A3
    └── unsharded: countries, currencies    one copy, on one cluster
```

What follows from this layout:

- **Your names don't change.** You see `shop.orders`; MariaDB sees `t42_d1_g3_s1.orders`. SprigDB translates schema names only, never table or column names.
- **Related data lives together.** A customer's orders, events and session keys share the customer id. They land on the same shard, so they join and commit together inside one MariaDB server.
- **Every model is MariaDB rows.** A collection is a table with `_id`, `doc` (JSON) and `_types`. A Redis hash is one row per field. Search indexes are the one structure kept outside MariaDB, and they can always be rebuilt from it.
- **Every tenant starts as one shard per group,** with exactly MariaDB's behaviour, and splits only when size or load call for it. See [Shards and table groups](shards-and-table-groups.md).
- **Isolation is MariaDB's.** A tenant's backend accounts are granted only on its own schemas, so MariaDB itself enforces tenant isolation.

## What happens to a request

1. The client connects to a gateway in its data centre, on its protocol's port.
2. The gateway logs the user in. The login names the tenant: `acme.app` is user `app` of tenant `acme`. See [Tenants and databases](tenants-and-databases.md).
3. The front end translates the request into one request shape: a SQL statement, a MongoDB `find`, a Redis `HGET` or a REST call.
4. Functions, validations and computed fields run, inside MariaDB when they compile to SQL and in the gateway's engine otherwise.
5. The gateway finds the shards. Most requests touch one shard and become one SQL statement. Others are split across shards, run, and merged.
6. The result goes back in the client's protocol. The commit reaches the change feed, which updates search indexes, fires triggers and notifies subscribers.

After a write, the gateway remembers its position in the replication stream (the GTID). A later read in the same session may go to a replica only once that replica has caught up, so you read your own writes without hints.

When a statement can't run with exact semantics across shards, it fails with `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` instead of returning a wrong answer.

## What runs in the background

| Loop | Watches | Does |
|---|---|---|
| [Automatic indexing](automatic-indexing.md) | Repeated slow queries | Designs indexes, validates them on a replica, builds them shard by shard, and drops them if they stop helping |
| Sharding advisor | Foreign keys, joins, transactions | Chooses table groups and distribution keys before the first split |
| Balancer | Cluster load and shard sizes | Splits, merges and moves shards, and moves tenants between clusters |
| Data-centre placement | Traffic per data centre, measured latency | Moves tenants toward their users |
| Failover | Health, leases | Promotes a copy and fences the old writer, within the tenant's recovery point objective |
| Read routing | Replica lag | Sends reads to replicas that have caught up |
| Search indexing | The change feed | Keeps search indexes up to date |
| Logic | Schedules, the change feed | Runs triggers and scheduled code |

Every loop can be set to act on its own, wait for approval, only recommend, or stay off. See [Approvals](approvals.md). Every decision is recorded, and `sprigctl explain` says why it was made.

## Where to go next

- [Tenants and databases](tenants-and-databases.md): who owns what, and how logins work.
- [Clusters and data centres](clusters-and-data-centres.md): where data lives physically.
- [Quick start](../get-started/quick-start.md): run it on your machine.
