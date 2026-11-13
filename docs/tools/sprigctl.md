---
title: sprigctl
audience: [operator, dba]
sidebar:
  order: 3
---

# sprigctl

`sprigctl` is the operator's command-line tool. It manages data centres, clusters, tenants, settings, automatic indexes, approvals, shards and moves, failover, logic, sinks and the rest of SprigDB through the controllers' Admin API. It never talks to etcd directly, except for the few platform-connection commands that say so.

This page explains how to connect and sign in, the conventions every command shares, and what each command group does, with examples. Every flag of every command is in the [sprigctl reference](../reference/sprigctl.md), generated from `sprigctl --help`.

## Connect

`sprigctl` calls the Admin API of a controller:

```sh
sprigctl --endpoint https://ctl.fra1.internal:7480 --ca-file /etc/sprigdb/tls/ca.pem dc list
```

| Option | Environment | Default | Meaning |
|---|---|---|---|
| `--endpoint <url>` | `SPRIGCTL_ENDPOINT` | `http://127.0.0.1:7480` | The Admin API, served by every controller on `[controller] admin_addr` |
| `--ca-file <pem>` | `SPRIGCTL_CA_FILE` | | For `https://`: the CA the controller's certificate is checked against |
| `--client-cert <pem>`, `--client-key <pem>` | `SPRIGCTL_CLIENT_CERT`, `SPRIGCTL_CLIENT_KEY` | | A client certificate, when the controller requires one (`admin_tls_client_ca`) |
| `--token-file <file>` | `SPRIGCTL_TOKEN_FILE` | | A file holding a bearer token. Without it, `sprigctl` uses your login session for the endpoint. |
| `--dc <dc>` | | | The data centre to act in, where a command needs one |
| `-o`, `--output` | | `table` | `table`, `json` or `yaml` (YAML is printed as JSON, which every YAML 1.2 parser reads) |
| `--dry-run` | | | Show what would change, without changing it |
| `--yes` | | | Don't ask for confirmation |

Each controller answers for its own data centre. Point `--endpoint` at a controller of the DC you are working on. Calls that write cluster records run on the DC leader; any other controller answers 39504 `ER_SPRIG_NOT_LEADER` and names the leader. Moves and failovers say which DC to call below.

## Sign in

People sign in as **named operators**, with a password and a second factor (TOTP):

```sh
sprigctl --endpoint https://ctl.fra1.internal:7480 login --name alice
sprigctl whoami
sprigctl logout
```

| `login` option | Meaning |
|---|---|
| `--name` | Your operator name (default `$USER`, or `SPRIGCTL_OPERATOR`) |
| `--password-file` | A file holding the password; else `SPRIGCTL_PASSWORD`; else a prompt |
| `--code` | A TOTP code; else a prompt |
| `--recovery-code` | A recovery code instead of a TOTP code |

The first login enrols the second factor: it prints an `otpauth://` URI for your authenticator app, asks for the first code, and prints ten recovery codes once. Keep them.

The session is kept per endpoint in `$SPRIGCTL_SESSIONS` (default `~/.config/sprigctl/sessions.json`, mode 0600) and used by every later command that has no `--token-file`. Sessions last `operators.session_hours` (12). After `operators.lockout_failures` (5) failed logins in a row, the operator is locked for `operators.lockout_minutes` (15). `operators.mfa_required` (on) makes the second factor mandatory.

### The first operator

A new deployment has only the bootstrap token, `[controller] admin_token_file`. Use it once to create the first platform operator:

```sh
sprigctl --endpoint https://ctl.fra1.internal:7480 --token-file /etc/sprigdb/admin-token \
    operator create alice --role platform --display-name "Alice" --email alice@example.com
```

From the first operator login on, the bootstrap token is refused with 39310 `ER_SPRIG_OPERATOR_LOGIN_REQUIRED`. If every operator is locked out, set `[controller] admin_token_break_glass = true`, restart the controller, fix the accounts with the bootstrap token, and remove the setting again. Every call made that way is audited as `breakglass.admin_token`.

### Operators

```sh
sprigctl operator create bob --role dc --dcs fra1,ams1 --reason "on-call for EU"
sprigctl operator list
sprigctl operator show bob
sprigctl operator disable bob --reason "left the team"     # their sessions and tokens end
sprigctl operator enable bob --reason "back"
sprigctl operator reset-factor bob --reason "lost phone"   # the next login enrols a new one
sprigctl operator set-password alice --current-file ~/old.pass --password-file ~/new.pass
```

| Role | May |
|---|---|
| `platform` | Everything |
| `dc` | Read everything on its DCs' controllers, and change clusters, mover and other jobs, in-doubt transactions, recommendations at its level, its DCs' drains, and settings at `dc` and `cluster` scope |

A DC operator calling another DC's controller, or a call its role doesn't allow, gets 39313 `ER_SPRIG_OPERATOR_FORBIDDEN` with the reason.

### Tokens for scripts

Scripts and services use tokens, issued from a login session:

```sh
sprigctl token create --scope clusters:read --scope tenants:read --days 30 --label "dashboard"
sprigctl token create --service deploy-bot --kind ci --scope '*:write' --never-expires --reason "CI deploys"
sprigctl token list [--owner bob | --all]
sprigctl token revoke <key_id> --reason "rotated"
```

- `--scope <area>:read` or `<area>:write` narrows what the token may do. The area is the first part of the Admin API path, such as `clusters`, `tenants`, `settings`, `dcs`, `jobs`, `mover`, `txns` or `recommendations`; `*` means every area. A token never has more rights than its owner.
- `--days` (default 30) is the lifetime, at most `operators.token_max_days` (90). `--dcs` limits it to some DCs.
- `--service <name>` issues a service identity (`sprigsvc_...`) instead of a personal token; `--kind` is `ci` (default) or `gateway`, and only a service identity may be `--never-expires`.

The token is printed once. Save it in a file and pass it with `--token-file`.

## Conventions

- **Confirmations.** `cluster drain`, `cluster retire`, `tenant delete` and `index drop` ask you to type the object's name. Without a terminal they fail unless you pass `--yes`.
- **Dry runs.** `--dry-run` checks and prints the plan for most writes, including registrations, drains, moves and failovers.
- **Reasons and actors.** Writes that are logged as decisions take `--reason`, and record who made them as `op:<actor>`, from `--actor`, `SPRIGCTL_ACTOR` or `$USER`.
- **Tenant default.** Commands with `--tenant` read `SPRIGCTL_TENANT` when it is missing.
- **Errors.** A failed call prints `sprigctl <group> <command>: <code> <name>: <message>` on standard error. The codes are in [Error codes](../reference/errors.md).
- **Exit status.** 0 on success, 1 on any failure, including a failed precheck, a refused confirmation, a failed `check`, and a command group that isn't built yet.

## Command groups

| Group | What it does |
|---|---|
| `dc` | Register, list, show, drain data centres; the latency matrix |
| `cluster` | Register, probe, drain, retire MariaDB clusters; extract and rebuild nodes |
| `tenant` | Create, list, show, delete tenants; users, export and import, settings, pins, moves between DCs, failover, standbys, residency |
| `settings` (alias `config`) | See and change settings at every scope |
| `index` | Automatic indexes and their recommendations |
| `recommendations` | The approvals inbox and its policies |
| `explain` | Why an index was built, a decision, a tenant's placement |
| `shard` | List, split, merge, re-key and move shards |
| `job` | Mover jobs |
| `advisor` | The sharding advisor |
| `gateway` | The DC's gateways, and draining one |
| `check` | Operational checks |
| `logic` | Compile, deploy and roll back logic; logs and dead letters |
| `space` | Spaces and shares |
| `inherit` | Inherited tables, shares, tree-wide ids and delete blockers |
| `txn` | In-doubt distributed transactions |
| `sink` | Sinks, their dead letters and connections |
| `graph` | Graphs and their partitioning |
| `search` | Search indexes on workers |
| `login`, `logout`, `whoami`, `operator`, `token` | Operators and their credentials (above) |

### dc

```sh
sprigctl dc register ams1 --tags country=NL,jurisdiction=EU --kind full --global-voter
sprigctl dc list
sprigctl dc show fra1                 # includes the current DC leader
sprigctl dc latency                   # p50, p90, p99 between every pair
sprigctl dc drain ams1                # no new tenants; retired once empty
sprigctl dc undrain ams1
```

`register` writes the DC as `joining`; the global leader activates it once it holds its lease and latency is measured. `--kind witness` registers a site that runs no tenants. See [Data centres](../operate/data-centres.md).

### cluster

```sh
sprigctl cluster register fra1-main --topology primary_replica \
    --node db1:3306 --node db2:3306 --node db3:3306 --credentials file:/etc/sprigdb/creds/fra1-main.toml
sprigctl cluster register -f fra1-ext1.toml --dry-run      # prechecks only
sprigctl cluster list
sprigctl cluster show fra1-c1                              # nodes, health, capability profile, gaps
sprigctl cluster probe fra1-c1 --full                      # probe now and publish what changed
sprigctl cluster set-nodes fra1-c1 --node n2=db2-new:3306 [--remove n3] [--only]
sprigctl cluster drain fra1-c1                             # stop placing; move its shards away
sprigctl cluster retire fra1-c1                            # a drained cluster's record; probes stop
sprigctl cluster extract fra1-c1 n1                        # a diverged former writer's transactions, sealed
sprigctl cluster extract-open --key-file /etc/sprigdb/keys/extract.keys n1-t42.extract
sprigctl cluster rebuild fra1-c1 n1 [--from n3] [--no-extract]
```

`register` prechecks the cluster and fails with 39511 listing every missing item. Topologies are `single`, `primary_replica`, `galera` and `external`. See [Clusters](../operate/clusters.md) and [Failover](../operate/failover.md).

### tenant

```sh
sprigctl tenant create acme [--under platform] [--cluster fra1-c1]   # returns once active
sprigctl tenant list
sprigctl tenant show acme              # state, ancestry, catalog cluster, versions
sprigctl tenant delete acme            # a leaf tenant; drops its catalog
sprigctl tenant user add acme admin --password 's3cret-Passw0rd' [--host '%'] [--native] [--if-not-exists]
sprigctl tenant settings acme [--all]  # its own setting rows; --all adds its ancestors'
sprigctl tenant suspend acme --reason "invoice overdue"   # it and its subtree: logins 39303, sessions closed
sprigctl tenant resume acme
```

- `create` without `--cluster` homes the tenant in the least busy DC its parent's residency allows; with `--cluster`, on that cluster of the controller's DC.
- `user add` creates a user in the tenant's catalog; the user logs in as `acme.admin`. `--native` also stores a `mysql_native_password` verifier, used where the tenant's `auth.mysql_native_password` is on. Inside a tenant, administrators can also use `CREATE USER`; see [Accounts and logins](../security/accounts-and-logins.md).
- `delete` refuses a tenant with children (39362).
- `suspend` needs a reason. Logins to the tenant and every tenant below it fail with 39303, open sessions get 39306 at their next statement, and triggers, sinks and scheduled runs wait until `resume`; housekeeping such as TTL expiry goes on. See [Tenant administration](../security/tenant-administration.md#suspension).

Export and import:

```sh
export SPRIGCTL_GATEWAY_PASSWORD='s3cret-Passw0rd'
sprigctl tenant export acme --to /backups/acme --gateway gw.fra1.internal:3306 --user acme.admin
sprigctl tenant import /backups/acme --into acme2 --create --gateway gw.fra1.internal:3306 --user acme2.admin
sprigctl tenant import /backups/acme --into acme --verify-only --gateway gw.fra1.internal:3306 --user acme.admin
```

See [Backups and moves](../operate/backups-and-moves.md).

Placement, moves and failover:

```sh
sprigctl tenant pin acme --reason "contract requires fra1"   # the balancer and DC moves leave it alone
sprigctl tenant unpin acme
sprigctl tenant traffic acme                                 # traffic by client DC, and the move it suggests
sprigctl --dry-run tenant move acme --to ams1
sprigctl tenant move acme --to ams1 [--cluster ams1-c1] [--max-pause-ms 3000] --wait
sprigctl tenant move acme --status
sprigctl tenant standby acme --dc ams1 --cluster ams1-c1     # record a replica-cluster standby
sprigctl tenant failover acme --to ams1 [--accept-data-loss] [--old-home-fenced]
sprigctl tenant failover --all-from fra1 --to ams1
sprigctl tenant failover acme --errant                        # failovers and diverged transactions
```

Run `tenant move` against a controller of the tenant's home DC, and `tenant failover` against one of the new home. See [Data centres](../operate/data-centres.md) and [Failover](../operate/failover.md).

Residency:

```sh
sprigctl tenant residency acme                                        # show
sprigctl tenant residency acme --policy 'jurisdiction = "EU"'         # the plan, for the tenant and every descendant
sprigctl tenant residency acme --policy 'jurisdiction = "EU"' --apply
sprigctl tenant residency acme --deny-remote-access true --apply
sprigctl tenant residency acme --policy '' --apply                    # remove it
```

Nothing changes without `--apply`. See [Residency](../security/residency.md).

### settings

```sh
sprigctl settings list --prefix auto_index. [--tenant-settable | --operator-only] [--accepts tenant]
sprigctl settings describe auto_index.max_per_table          # type, default, scopes, bounds, who may set it
sprigctl settings get auto_index.mode --tenant acme --db shop --object orders
sprigctl settings set catalog.cache_max_mb 1024 --scope dc --dc fra1 --reason "bigger caches"
sprigctl settings set auto_index.mode recommend --scope object:acme.shop.orders
sprigctl settings reset auto_index.mode --scope tenant:acme [--bounds | --all]
sprigctl settings bounds auto_index.max_per_table --scope global --max 8
sprigctl settings bounds auto_index.mode --scope tenant:acme --allowed off,recommend
sprigctl settings lock auto_index.mode --scope tenant:acme
sprigctl settings unlock auto_index.mode --scope tenant:acme
```

`--scope` is `global`, `dc`, `cluster`, `tenant`, `database` or `object` with `--dc`, `--cluster`, `--tenant`, `--db` and `--object`, or one word: `dc:<id>`, `cluster:<id>`, `tenant:<name>`, `db:<tenant>.<db>`, `object:<tenant>.<db>.<object>`. `get` (alias `explain`) shows the effective value, where it comes from, and each scope's row and bounds; `reset` (alias `unset`) removes a value so narrower scopes inherit. Refusals: a value outside the bounds (39080), an unknown key (39081), a scope the key doesn't accept (39082), a key locked by a wider scope (39083), a value of the wrong type (39084). See [Settings and scopes](../concepts/settings-and-scopes.md) and the [settings reference](../reference/settings.md).

### index and explain

```sh
sprigctl index list --tenant acme [--db shop] [--table orders] [--state active] [--recommended] [--all]
sprigctl index show orders.sprig_auto_3f9a1c --tenant acme       # definition, evidence, validations, history
sprigctl index recommendations --tenant acme
sprigctl index approve orders.sprig_auto_3f9a1c --tenant acme --reason "checked the plan"
sprigctl index reject orders.sprig_auto_3f9a1c --tenant acme --reason "write-heavy table"
sprigctl index drop orders.sprig_auto_3f9a1c --tenant acme --yes
sprigctl index pin orders.sprig_auto_3f9a1c --tenant acme        # freeze it in its state
sprigctl index unpin orders.sprig_auto_3f9a1c --tenant acme
sprigctl index restore shop.orders.idx_status --tenant acme      # bring back a retired or dropped index, pinned
sprigctl explain query 9f1c2a7be03d4410 --tenant acme            # why an index was or wasn't built
sprigctl explain decision <id> --tenant acme
sprigctl explain placement acme                                  # where its shards are and what the balancer plans
```

An index is named by its id from `index list`, by `[<db>.][<table>.]sprig_auto_<hex>`, or, for your own index, by `[<db>.][<table>.]<name>`. States are `validating`, `recommended`, `rolling_out`, `observing`, `active`, `retiring`, `dropping`, `dropped`, `rejected`, `failed` and `frozen`. See [Automatic indexing](../performance/automatic-indexing.md).

### recommendations

The approvals inbox, for everything a loop set to `approve` or `recommend` is waiting on:

```sh
sprigctl recommendations list [--tenant acme] [--level tenant] [--kind index] [--state open] [--all]
sprigctl recommendations show tacme.42
sprigctl recommendations approve tacme.42 --reason "ok" [--window 2026-11-14T01:00:00Z/2026-11-14T04:00:00Z]
sprigctl recommendations ignore tacme.42 --reason "not now" [--days 14 | --forever]
sprigctl recommendations snooze tacme.42 --hours 24
sprigctl recommendations reopen tacme.42
sprigctl recommendations comment tacme.42 "Waiting for the release"
sprigctl recommendations policy --tenant acme [--db shop] [--object orders]
```

Ids are `t<tenant>.<n>`, `dc.<dc>.<n>` or `g.<n>`. Levels are `table`, `database`, `tenant`, `dc` and `platform`. An approved item is applied on the deciding loop's next cycle, inside `--window` if given (RFC 3339, UTC, at most 30 days). See [Approvals](../concepts/approvals.md).

### shard, job and advisor

```sh
sprigctl shard list acme
sprigctl shard move acme --to fra1-c2 [--manual] [--verify full|off|sample:10] [--wait] [--pin]
sprigctl shard move acme 3 4 --to fra1-c2 --wait                 # shard 4 of table group 3
sprigctl shard split acme 3 4 [--at 0x9000000000000000 | --ways 3]
sprigctl shard merge acme 3 6 7
sprigctl shard rekey acme 3 --key customer_id [--member events=cust_id] [--shards 4] [--to fra1-c2]
sprigctl job list [--tenant acme]
sprigctl job show <id>
sprigctl job cutover <id>          # let a --manual move cut over
sprigctl job cancel <id>           # before its cutover commits
sprigctl advisor run acme          # for a tenant without table groups
sprigctl advisor show acme [run]
sprigctl advisor apply acme <run>
```

See [Backups and moves](../operate/backups-and-moves.md) and the [sharding advisor](../performance/sharding-advisor.md).

### gateway

```sh
sprigctl gateway list
sprigctl gateway show gw-1
sprigctl gateway drain gw-1        # readiness off, listeners closed, work finished, exit
```

See [Upgrades](../operate/upgrades.md).

### check

```sh
sprigctl check no-restart --telemetry http://gw-1:9180 --gateway gw-1:3306 --user acme.app \
    --set gateway.max_connections=30000@dc:fra1 --listener mysql=0.0.0.0:3316
```

Changes settings, listener addresses and certificates under load, and proves there was no restart and no failed statement. It exits 1 when a check fails. See [Upgrades](../operate/upgrades.md#check-that-live-changes-need-no-restart).

### logic

```sh
sprigctl logic compile discount.json [--table orders:id=int,total=float]   # IR, placement and SQL, offline
sprigctl logic deploy discount.json --db shop --gateway mysql://acme.admin:PASSWORD@gw:3306 [--tier auto|mariadb|engine] [--definer]
sprigctl logic rollback discount --db shop [--to-version 3] [--procedure] --gateway mysql://acme.admin:PASSWORD@gw:3306
sprigctl logic list --catalog mysql://sprig_ctl:PASSWORD@db1:3306/t42_sprig
sprigctl logic show discount --catalog mysql://...@db1:3306/t42_sprig
sprigctl logic logs [discount] [--limit 50] [-f] --catalog mysql://...@db1:3306/t42_sprig
sprigctl logic dead-letters list [--binding b] [--state dead] --catalog mysql://...@db1:3306/t42_sprig
sprigctl logic dead-letters replay --all [--binding b] --catalog ...
sprigctl logic dead-letters discard 17 18 --catalog ...
```

`deploy` and `rollback` send `CREATE OR REPLACE PROCEDURE ... LANGUAGE kirun` and `ALTER FUNCTION ... ROLLBACK` through a gateway as the user in the URL. `list`, `show`, `logs` and `dead-letters` read the tenant's catalog schema directly over an operator's MariaDB connection. See [Logic](../data-models/logic.md).

### space and inherit

```sh
sprigctl space list --catalog mysql://...@db1:3306/t42_sprig
sprigctl space show [eu] --catalog mysql://...@db1:3306/t42_sprig
sprigctl space create eu [--under root] --gateway mysql://acme.admin:PASSWORD@gw:3306
sprigctl space share products-eu --table shop.products --to all [--where "region = 'EU'"] [--columns id,name] [--reshare] --gateway ...
sprigctl space unshare products-eu --gateway ...
sprigctl inherit tables --gateway mysql://acme.admin:PASSWORD@gw:3306 [-d shop]
sprigctl inherit share products-eu --gateway ...
sprigctl inherit check shop.products --gateway ...            # ids held by more than one tenant of the tree
sprigctl inherit blockers shop.products --where "id = 7" --gateway ...
```

The gateway commands run SQL (`CREATE SPACE`, `CREATE SHARE`, `SHOW INHERITABLE TABLES`, `SPRIG CHECK IDS`, `SPRIG CHECK REFERENCES`) as the user in the URL, so that user's privileges decide what is shown. See [Inheritance](../data-models/inheritance.md).

### txn

```sh
sprigctl txn list [--tenant acme]      # in-doubt transactions on this DC's clusters, oldest first
sprigctl txn show <dtid>               # its branches and its decision
sprigctl txn resolve <dtid>            # finish it now, by its recorded decision
```

A transaction is in doubt while one of its XA branches is prepared. The controller's resolver finishes such branches on its own; `resolve` does the same at once. The recorded decision commits it; with none recorded it is aborted. You never pick the outcome.

### sink

```sh
sprigctl sink list --gateway mysql://acme.admin:PASSWORD@gw:3306/shop [--db shop]
sprigctl sink show shop.orders_to_kafka --gateway ...
sprigctl sink pause shop.orders_to_kafka --gateway ...
sprigctl sink resume shop.orders_to_kafka --gateway ...
sprigctl sink reset shop.orders_to_kafka --to earliest --gateway ...   # latest, earliest, snapshot or a time
sprigctl sink test shop.orders_to_kafka [--sample 5] --gateway ...
sprigctl sink connections list --gateway ... [--db shop]
sprigctl sink connections test shop.kafka_main [--platform] [--write [--to topic]] --gateway ...
sprigctl sink dead-letters list shop.orders_to_kafka --catalog mysql://...@db1:3306/t42_sprig
sprigctl sink dead-letters replay shop.orders_to_kafka --all --catalog ...
sprigctl sink platform-connections list --etcd https://etcd-g-fra1:2379
```

Most commands run the SQL statements through a gateway (`--gateway`, or `SPRIGCTL_GATEWAY`). Dead letters are read from the tenant's catalog. Platform connections, shared with tenants by the operator, live in the global etcd tier (`--etcd`, or `SPRIGCTL_ETCD`); `register` takes secrets only as `file:` references. See [Sinks](../data-models/sinks.md).

### graph and search

```sh
sprigctl graph list acme
sprigctl graph show acme social.friends          # labels, types, indexes, shards, row counts
sprigctl graph partition acme social.friends     # the share of relationships that cross shards
sprigctl search list acme                        # search indexes on workers and their copies
sprigctl search show acme shop.product_text
sprigctl search rebuild acme shop.product_text   # a new generation, built from MariaDB
```

See [Graphs](../data-models/graphs.md) and [Search](../data-models/search.md).

## Not supported yet

- Reading the change feed (its consumers and lag) from `sprigctl`. Watch the `sprig_feed_*` metrics instead; see the [metrics reference](../reference/metrics.md).
- Importing a MySQL dump or MongoDB collections with `sprigctl`. Load data through the protocols instead (for example `mysql < dump.sql`, or inserts from a MongoDB driver), or move a whole tenant with `sprigctl tenant export` and `tenant import`.

## Related

- [sprigctl reference](../reference/sprigctl.md)
- [Configuration](../operate/configuration.md)
- [Clusters](../operate/clusters.md)
- [Failover](../operate/failover.md)
- [Error codes](../reference/errors.md)
