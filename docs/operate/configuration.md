---
title: Configuration
audience: [operator]
sidebar:
  order: 4
---

# Configuration

SprigDB has two kinds of configuration. The **process file**, `sprigdb.toml`, holds only what a process needs before it can reach the control plane: its data centre and id, the etcd seeds, its own certificate, file paths for secrets, and thread counts. Everything else is a **live setting**, stored in etcd or in a tenant's catalog, changed with `sprigctl settings` or, for tenant scopes, SQL such as `ALTER TABLE orders SPRIG SET (...)`, and applied without a restart.

This page documents every section and field of `sprigdb.toml`, the `SPRIGDB_*` environment overrides, and how the two kinds relate.

## Where the file comes from

Every role of the `sprigdb` binary reads the same file:

```sh
sprigdb --config /etc/sprigdb/sprigdb.toml gateway
```

| Source | Used when |
|---|---|
| `--config <path>` (or `-c`) | Given. A missing or unreadable file is an error. |
| `SPRIGDB_CONFIG` | The same as `--config`, from the environment. |
| `./sprigdb.toml` | No `--config`, and the file exists in the working directory. |
| `SPRIGDB_*` variables only | No file at all. `[node] dc` must then come from `SPRIGDB_NODE_DC`. |

Unknown sections and unknown fields are refused: the process exits at start with `ER_SPRIG_CONFIG_INVALID` (39998) and names the field. That catches typos before they silently fall back to a default.

A minimal file for a production gateway:

```toml
[node]
dc = "fra1"
id = "gw-1"

[meta]
dc_seeds = ["https://etcd-fra1-0:2379", "https://etcd-fra1-1:2379", "https://etcd-fra1-2:2379"]
global_seeds = ["https://etcd-g-fra1:2379", "https://etcd-g-ams1:2379", "https://etcd-g-mum1:2379"]
deployment_id = "prod-eu"

[tls]
ca = "/etc/sprigdb/tls/ca.pem"
cert = "/etc/sprigdb/tls/node.pem"
key = "/etc/sprigdb/tls/node-key.pem"

[telemetry]
log_format = "json"
bootstrap_addr = "0.0.0.0:9180"
```

## Environment overrides

Any field of these sections can be overridden by an environment variable named `SPRIGDB_<SECTION>_<FIELD>`, in upper case: `node`, `meta`, `tls`, `gateway`, `controller`, `worker`, `agent`, `operator`, `telemetry` and `audit`.

```sh
SPRIGDB_NODE_DC=fra1
SPRIGDB_NODE_ID=gw-2
SPRIGDB_TELEMETRY_BOOTSTRAP_LOG_LEVEL='info,sprig_meta=debug'
SPRIGDB_CONTROLLER_ADMIN_ADDR=0.0.0.0:7480
SPRIGDB_META_DC_SEEDS='["https://etcd-fra1-0:2379", "https://etcd-fra1-1:2379"]'
SPRIGDB_GATEWAY_LISTEN_OVERRIDE='{ mysql = "127.0.0.1:23306" }'
```

Rules:

- The variable wins over the file.
- The value is read as a TOML value when it parses as one (`0`, `true`, `["a", "b"]`, `{ k = "v" }`), and as a string otherwise. So a list or a table must be written in TOML syntax.
- The section is everything up to the first underscore after `SPRIGDB_`; the rest is the field. `SPRIGDB_CONTROLLER_ADMIN_TOKEN_FILE` sets `[controller] admin_token_file`.
- A nested table such as `[agent.rebuild]` is replaced whole by `SPRIGDB_AGENT_REBUILD='{ datadir = "/data/mysql" }'`; fields you leave out take their defaults.
- `[recovery]` has no environment override. Set it in the file.
- Variables for any other section are ignored.

## Sections and fields

Paths may be relative; they are relative to the process's working directory. Run from a fixed directory, or use absolute paths.

### `[node]`

Who this process is. Required.

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `dc` | text | none (required) | `SPRIGDB_NODE_DC` | The data centre this process belongs to, such as `fra1`. A lowercase DNS label. |
| `id` | text | the hostname | `SPRIGDB_NODE_ID` | Unique within the data centre. Gateways are drained and listed by it (`sprigctl gateway drain <id>`). |

### `[meta]`

The etcd seeds. Members beyond the seeds are discovered and followed while the process runs.

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `dc_seeds` | list of URLs | `[]` | `SPRIGDB_META_DC_SEEDS` | Endpoints of this data centre's etcd tier. |
| `global_seeds` | list of URLs | `[]` | `SPRIGDB_META_GLOBAL_SEEDS` | Endpoints of the global etcd tier. Empty, or equal to `dc_seeds`, means **single-etcd mode**: one etcd serves both tiers. |
| `deployment_id` | text | `""` | `SPRIGDB_META_DEPLOYMENT_ID` | Must match the deployment's `system.deployment_id`. A process pointed at another deployment's etcd refuses to start. Empty means not checked. |

Single-etcd mode is right for one data centre and for development. With several data centres, give each its own DC tier and share a global tier; see [Data centres](data-centres.md).

### `[tls]`

This process's identity for mutual TLS between SprigDB processes. Optional; when present all three fields are required.

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `ca` | path | none | `SPRIGDB_TLS_CA` | CA bundle (PEM) that peers' certificates are checked against. |
| `cert` | path | none | `SPRIGDB_TLS_CERT` | This process's certificate chain (PEM). |
| `key` | path | none | `SPRIGDB_TLS_KEY` | Its private key (PEM). |

`[tls]` is used for calls to controllers that serve their internal port over mutual TLS (`[controller] internal_tls`), for the gateways' internal peer service, and as the gateway's client certificate towards an HTTPS Admin API. It isn't the certificate clients see: client listeners use the `tls.bundles.*` and `listener.<name>.cert` live settings (see [TLS](../security/tls.md)).

### `[gateway]`

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `worker_threads` | integer | `0` | `SPRIGDB_GATEWAY_WORKER_THREADS` | Runtime threads; `0` means one per CPU. |
| `login_mode` | `catalog` or `accounts_file` | `accounts_file` when `accounts_file` is set, else `catalog` | `SPRIGDB_GATEWAY_LOGIN_MODE` | Where logins come from. `catalog`: tenants and their users, with logical database names. `accounts_file`: a fixed list of logins on raw clusters, names unchanged (development and testing). |
| `accounts_file` | path | none | `SPRIGDB_GATEWAY_ACCOUNTS_FILE` | In `accounts_file` mode, the TOML file of logins. Re-read within a second of a change. Missing means every login fails with 1045. |
| `backends_file` | path | none | `SPRIGDB_GATEWAY_BACKENDS_FILE` | In `catalog` mode, the TOML file naming the MariaDB account tenant sessions run as: a `[per_user]` secret from which each user's own backend account password is derived, or a `[default]` account, or `[tenants.<name>]` entries. Read at start. A verified login with no matching entry fails with 39998. |
| `listen_override` | table | `{}` | `SPRIGDB_GATEWAY_LISTEN_OVERRIDE` | Per listener name, an address that replaces the data centre's `listener.<name>.bind` for this process only, such as `{ mysql = "127.0.0.1:23306" }`. For a second gateway on one host. |
| `workload_key_file` | path | none | `SPRIGDB_GATEWAY_WORKLOAD_KEY_FILE` | The file of versioned master secrets (`<version> <64 hex digits>` per line) that sealed query-parameter samples are encrypted under. Unset: no parameter samples are captured; statistics continue. Use the same file as the controllers. |
| `workload_capture_off` | bool | `false` | `SPRIGDB_GATEWAY_WORKLOAD_CAPTURE_OFF` | Turns workload capture off on this gateway. A kill switch; automatic indexing then sees nothing from it. |
| `advertise_addr` | text | the hostname | `SPRIGDB_GATEWAY_ADVERTISE_ADDR` | The host other gateways, workers, load balancers and `sprigctl` reach this gateway at. Published in its registration. |
| `controller_token_file` | path | none | `SPRIGDB_GATEWAY_CONTROLLER_TOKEN_FILE` | The credential this gateway presents to the controllers' Admin API for calls it makes on its own behalf (Studio's indexing and workload views). Must match the controller's `gateway_token_file`, a `service_token_files` entry, or `admin_token_file`. |
| `operator_token_file` | path | none | `SPRIGDB_GATEWAY_OPERATOR_TOKEN_FILE` | The Admin API token Studio's operator mode uses for logins listed in `studio.operator_logins`. Unset: operator mode answers 503. |
| `controller_ca_file` | path | `[tls] ca` | `SPRIGDB_GATEWAY_CONTROLLER_CA_FILE` | CA (PEM) for a controller's HTTPS Admin API. |
| `kafka_advertise_host` | text | `kafka.advertised_host`, else `advertise_addr` | `SPRIGDB_GATEWAY_KAFKA_ADVERTISE_HOST` | The host Kafka clients are told to reach this gateway at, in Kafka metadata. Set one per gateway when each has its own public name. |

### `[controller]`

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `worker_threads` | integer | `0` | `SPRIGDB_CONTROLLER_WORKER_THREADS` | Runtime threads; `0` means one per CPU. |
| `bootstrap` | bool | `false` | `SPRIGDB_CONTROLLER_BOOTSTRAP` | When the global tier has no deployment yet, write the first data centre's records (`system.deployment_id`, the DC record as `active`, the tenant id counter). Later starts only verify the id. Set it on the first data centre's controllers. |
| `global_candidate` | bool | `false` | `SPRIGDB_CONTROLLER_GLOBAL_CANDIDATE` | Also campaign for global leader, which runs the DC failover decision and the DC lifecycle. Set it on controllers in data centres that hold global etcd voters. |
| `admin_addr` | socket address | `127.0.0.1:7480` | `SPRIGDB_CONTROLLER_ADMIN_ADDR` | Where the Admin API (REST under `/admin/v1/`) is served. Port `0` picks a free port. |
| `admin_advertise` | text | the bound address | `SPRIGDB_CONTROLLER_ADMIN_ADVERTISE` | The address gateways and `sprigctl` reach the Admin API at, when it differs from the bound one. |
| `admin_token_file` | path | none | `SPRIGDB_CONTROLLER_ADMIN_TOKEN_FILE` | The bearer token Admin API calls must present. Required when `admin_addr` isn't a loopback address. Once named operators exist, this token is refused with 39310 unless `admin_token_break_glass` is on. |
| `admin_token_break_glass` | bool | `false` | `SPRIGDB_CONTROLLER_ADMIN_TOKEN_BREAK_GLASS` | Accept `admin_token_file` again after named operators are in use. Every call made with it is audited as `breakglass.admin_token`. |
| `admin_tls_cert` | path | none | `SPRIGDB_CONTROLLER_ADMIN_TLS_CERT` | Serve the Admin API over HTTPS with this certificate chain (PEM). Unset: plain HTTP. |
| `admin_tls_key` | path | none | `SPRIGDB_CONTROLLER_ADMIN_TLS_KEY` | The private key of `admin_tls_cert`. |
| `admin_tls_client_ca` | path | none | `SPRIGDB_CONTROLLER_ADMIN_TLS_CLIENT_CA` | With `admin_tls_cert`: require client certificates signed by this CA (mutual TLS). |
| `gateway_token_file` | path | none | `SPRIGDB_CONTROLLER_GATEWAY_TOKEN_FILE` | The credential gateways present for their own calls. Accepted only on the tenant routes Studio reads and acts through (indexes, recommendations, explanations, decisions, workload). It is the service identity named `gateway`. |
| `service_token_files` | table | `{}` | `SPRIGDB_CONTROLLER_SERVICE_TOKEN_FILES` | Named service identities, each a file holding the secret one gateway presents: `{ gw-fra1 = "/etc/sprigdb/gw-fra1.token" }`. The name is recorded in the audit. |
| `dc_operator_token_file` | path | none | `SPRIGDB_CONTROLLER_DC_OPERATOR_TOKEN_FILE` | The token of this data centre's operators, for approvals at the `dc` level. |
| `internal_addr` | socket address | `127.0.0.1:0` | `SPRIGDB_CONTROLLER_INTERNAL_ADDR` | Where the controller serves its internal services to gateways, agents, workers and other data centres' controllers: permits, peer pings, failover observations and promotion calls. Port `0` picks a free port. In a real deployment, bind a reachable address. |
| `internal_advertise` | text | the bound address | `SPRIGDB_CONTROLLER_INTERNAL_ADVERTISE` | The address others reach `internal_addr` at, when it differs (a published port, a proxy). |
| `internal_tls` | bool | `false` | `SPRIGDB_CONTROLLER_INTERNAL_TLS` | Serve the internal services over mutual TLS with `[tls]`; every client must present a certificate `[tls] ca` signed. Needs `[tls]`. |
| `workload_key_file` | path | none | `SPRIGDB_CONTROLLER_WORKLOAD_KEY_FILE` | The same master-secret file as the gateways'. Automatic-index validation opens the parameter samples it replays with it; without it validation falls back to estimates and a canary. |
| `autoindex_test_clock_file` | path | none | `SPRIGDB_CONTROLLER_AUTOINDEX_TEST_CLOCK_FILE` | Testing only: a file of whole seconds by which the automatic-indexing clock runs ahead. Leave unset in production. |
| `smtp_password_file` | path | none | `SPRIGDB_CONTROLLER_SMTP_PASSWORD_FILE` | Password for `approvals.smtp_username`, for approval e-mails. |
| `smtp_ca_file` | path | none | `SPRIGDB_CONTROLLER_SMTP_CA_FILE` | CA (PEM) for the SMTP server's certificate instead of the built-in roots. |
| `smtp_client_cert_file` | path | none | `SPRIGDB_CONTROLLER_SMTP_CLIENT_CERT_FILE` | Client certificate chain (PEM) offered to the SMTP server. |
| `smtp_client_key_file` | path | none | `SPRIGDB_CONTROLLER_SMTP_CLIENT_KEY_FILE` | Its private key (PEM). |
| `approvals_webhook_secret_file` | path | none | `SPRIGDB_CONTROLLER_APPROVALS_WEBHOOK_SECRET_FILE` | The secret that signs events sent to `approvals.webhook_url`. |

### `[worker]` and `[operator]`

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `worker_threads` | integer | `0` | `SPRIGDB_WORKER_WORKER_THREADS`, `SPRIGDB_OPERATOR_WORKER_THREADS` | Runtime threads; `0` means one per CPU. |

### `[agent]`

The node agent runs beside a MariaDB node that SprigDB manages on a VM. See [Deploying on VMs](deploy-vms.md).

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `worker_threads` | integer | `0` | `SPRIGDB_AGENT_WORKER_THREADS` | Runtime threads; `0` means one per CPU. |
| `cluster` | text | none | `SPRIGDB_AGENT_CLUSTER` | The cluster the node belongs to, such as `fra1-c1`. Unset: the agent fences nothing. |
| `node` | text | the node whose address is `mysql_addr` | `SPRIGDB_AGENT_NODE` | The node's id in the cluster record. |
| `mysql_addr` | text | the node's address in the cluster record | `SPRIGDB_AGENT_MYSQL_ADDR` | `host:port` of the local MariaDB. |
| `credentials` | text | the cluster's `sprig_ctl` reference | `SPRIGDB_AGENT_CREDENTIALS` | Secret reference of the account the agent fences with, `file:<path>#<table>`. It needs `READ_ONLY ADMIN` and `CONNECTION ADMIN`. |
| `internal_addr` | socket address | none | `SPRIGDB_AGENT_INTERNAL_ADDR` | Where the agent's internal service listens: the backup stream and rebuild calls. Unset: the node can neither be rebuilt nor be a rebuild's source. |
| `internal_advertise` | text | `http://` and the bound address | `SPRIGDB_AGENT_INTERNAL_ADVERTISE` | The URL other agents and workers reach the service at. |

### `[agent.rebuild]`

How the agent stops, starts and restores its node during `sprigctl cluster rebuild`. Every command runs after `exec_prefix`, so a node in a container is reached with `exec_prefix = ["docker", "exec", "-i", "<container>"]`.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `exec_prefix` | list of text | `[]` | Put before every command. Empty: run on the agent's host. |
| `stop_command` | list of text | `["systemctl", "stop", "mariadb"]` | Stops mysqld and returns once it has stopped. |
| `start_command` | list of text | `["systemctl", "start", "mariadb"]` | Starts mysqld (it boots `read_only`). |
| `datadir` | text | `/var/lib/mysql` | MariaDB's data directory. |
| `staging_dir` | text | `/var/lib/sprigdb/rebuild` | Where backups are received and prepared. Needs room for one copy of the data. |
| `owner` | text | `mysql:mysql` | `user:group` that owns the restored files. |
| `backup_bin` | text | `mariadb-backup` | The `mariadb-backup` binary. |
| `mbstream_bin` | text | `mbstream` | The `mbstream` binary. |

### `[telemetry]`

What a process uses before its live settings arrive.

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `bootstrap_log_level` | text | `info` | `SPRIGDB_TELEMETRY_BOOTSTRAP_LOG_LEVEL` | Log filter in `tracing` directive syntax: `info`, or `info,sprig_meta=debug`. |
| `log_format` | `json` or `pretty` | `json` | `SPRIGDB_TELEMETRY_LOG_FORMAT` | JSON lines for production; `pretty` for a terminal. |
| `bootstrap_addr` | socket address | `0.0.0.0:9180` | `SPRIGDB_TELEMETRY_BOOTSTRAP_ADDR` | Where `/metrics`, `/healthz` and `/readyz` are served. Two processes on one host need different ports. |

See [Monitoring](monitoring.md).

### `[audit]`

| Field | Type | Default | Environment | Meaning |
|---|---|---|---|---|
| `spool_dir` | path | none | `SPRIGDB_AUDIT_SPOOL_DIR` | A local spool directory for audit events, one per process. Unset: audit events go only to the log, under the target `sprig_audit`. |
| `sink_dir` | path | none | `SPRIGDB_AUDIT_SINK_DIR` | Where the shipper writes audit files, as `audit/<stream>/<date>/...`. Required with `spool_dir`: a spool without a sink stops the process at start. |

See [Audit](../security/audit.md).

### `[recovery]`

Used by workers that extract a diverged former writer's transactions (`sprigctl cluster extract`). No environment override.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `extract_dir` | path | none | The local directory sealed extracts are written under. |
| `extract_key_file` | path | none | The master keyring file (`<version> <64 hex digits>` per line). Each tenant's extract key is derived from it, so one tenant's extract opens with nothing else. |

Without both, extract jobs fail with the reason, and rebuilding a diverged node waits for an operator (or `--no-extract`). See [Failover](failover.md).

## Secret files

Every secret in `sprigdb.toml` is a path to a file, never the secret itself: tokens, passwords, keys and keyrings. Keep these files readable only by the user SprigDB runs as. Credentials for MariaDB's system accounts are not in `sprigdb.toml` at all: cluster records name them by reference (`file:<path>` or `env:<name>`), and only controllers and workers resolve them. See [Clusters](clusters.md).

## Process settings and live settings

| | Process file | Live settings |
|---|---|---|
| Holds | Identity, etcd seeds, own certificate, secret file paths, thread counts | Everything else: listeners, TLS bundles, limits, timeouts, every automatic loop's mode |
| Stored in | `sprigdb.toml` and the environment, per process | etcd (global, dc and cluster scope) and tenant catalogs (tenant, database and object scope) |
| Changed with | Editing the file, then a rolling restart | `sprigctl settings set`, `ALTER ... SPRIG SET` (tenant, database and object scopes), Studio |
| Takes effect | At the next start | Without a restart |

Every live setting is declared once in code, with its type, default, scopes and hard bounds. The [settings reference](../reference/settings.md) lists all of them. Each setting says how it applies:

- **live**: at once, in every process;
- **rebind**: the listener is rebound to the new address, TLS mode or certificate without a restart; sessions on a retired or stricter listener get `listener.drain_seconds` to finish;
- **new sessions**: sessions opened after the change.

The nearest scope wins: object, database, tenant (and its ancestors), cluster, dc, then global. Wider scopes can set bounds and locks that narrower ones can't cross. See [Settings and scopes](../concepts/settings-and-scopes.md).

```sh
sprigctl settings get listener.mysql.tls --scope dc:fra1
sprigctl settings set listener.mongo.enabled true --scope dc --dc fra1 --reason "MongoDB clients"
sprigctl settings set gateway.max_connections 40000 --scope global --reason "bigger gateways"
```

### Config drift

A process reads `sprigdb.toml` once, at start. If the file on disk changes afterwards, `/readyz?verbose` reports it, and readiness stays up:

```text
[-]config drift: /etc/sprigdb/sprigdb.toml differs from the running configuration; it applies at the next rolling restart
```

Use this after a configuration push to find processes that still need a restart. See [Upgrades](upgrades.md) for restarting without losing writes.

## Example files

The repository ships working examples:

| File | What it shows |
|---|---|
| `deploy/packaging/sprigdb.example.toml` | The smallest production-shaped file, installed by the packages. |
| `deploy/compose/sprigdb.dev.toml` | A development controller in single-etcd mode, with `bootstrap = true`. Development only. |
| `deploy/compose/sprigdb.gateway.dev.toml` | A development gateway next to it, in `catalog` login mode. Development only. |

## Related

- [Settings reference](../reference/settings.md)
- [Settings and scopes](../concepts/settings-and-scopes.md)
- [Data centres](data-centres.md)
- [Monitoring](monitoring.md)
- [sprigctl](../tools/sprigctl.md)
