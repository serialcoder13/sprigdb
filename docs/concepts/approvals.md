---
title: Approvals
audience: [dba, operator]
sidebar:
  order: 6
---

# Approvals

Every automatic decision SprigDB makes, such as building an index, splitting a shard or moving a tenant, can act on its own, wait for a person to approve it, only recommend, or stay off. Read this page to choose a mode for each kind of decision, and to learn how held decisions reach the right people and what they can do with them.

## Modes

Each kind of decision is governed by one mode setting. The mode has four values:

| Mode | What happens when SprigDB wants to act |
|---|---|
| `on` | It acts. Nothing waits; the decision is recorded in the decision log |
| `approve` | It raises a **recommendation** and waits. The change runs only after someone approves it, or a timer you set approves it |
| `recommend` | It raises a recommendation as advice and doesn't act. A person may still approve it |
| `off` | It does nothing |

Some mode keys keep their component's own spellings: `auto` means `on`; `balancer.mode` also takes `dry_run`, and `failover.mode` takes `manual`. Both of those keep the component's own dry-run behaviour and raise no recommendation.

Modes are ordinary settings, so they resolve through [scopes](settings-and-scopes.md): you can set `approve` for one sensitive table and leave the rest of the tenant `on`.

```sql test
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20));
ALTER TABLE orders SPRIG SET (auto_index.mode = 'approve');
SHOW SPRIG RECOMMENDATIONS FOR TABLE orders;
```

## Kinds of decision

| Kind | Decision | Mode setting | Narrowest level |
|---|---|---|---|
| `index.create` | Create an index (B-tree, vector, spatial or a document field column) | `auto_index.mode` | table |
| `index.drop` | Drop or retire an index | `auto_index.mode` | table |
| `vector.ann` | Build an approximate-nearest-neighbour (HNSW) graph for a vector index | `vector.auto_ann` | table |
| `graph.partition` | Partition a graph | `graph.auto_partition` | table |
| `shard.split` | Split a shard | `sharding.mode` | database |
| `shard.merge` | Merge shards | `sharding.mode` | database |
| `shard.move` | Move a shard to another cluster in the DC | `sharding.mode` | database |
| `advisor.design` | Choose table groups and distribution keys | `advisor.mode` | database |
| `advisor.rekey` | Re-key a table group | `advisor.mode` | database |
| `advisor.reference` | Mark a table as a reference table (today it stays unsharded) | `advisor.mode` | database |
| `advisor.lookup` | Create a global lookup index | `advisor.mode` | database |
| `balancer.move` | Move a tenant to another cluster | `balancer.mode` | tenant |
| `dc.move` | Move a tenant to another data centre | `migration.mode` | tenant |
| `standby.create` | Create a standby in another data centre | `failover.standby_mode` | tenant |
| `standby.drop` | Drop a standby | `failover.standby_mode` | tenant |
| `failover.cluster` | Fail a cluster's writer over | `failover.mode` | dc |
| `failover.dc` | Fail tenants over to another data centre | `failover.mode` | dc |

A kind's **family** is the part before the dot (`index`, `shard`, `advisor`, …). Filters and settings that take a list of kinds accept families too.

The mode settings of the advisor, sharding, the balancer, migration, standbys and failover are operator-only. Tenant users set `auto_index.mode`, `vector.auto_ann` and `graph.auto_partition`, at the tenant, a database or a table. Each setting's default is in the [settings reference](../reference/settings.md); `auto_index.mode`, `sharding.mode` and `advisor.mode` default to `on`, `balancer.mode`, `migration.mode` and `failover.mode` to `auto`.

## Levels and who approves

Every recommendation sits at a **level**, which says who owns it:

| Level | Who may approve |
|---|---|
| `table` | Users with `ALTER` on the table (or `INDEX`, for index kinds), and everyone at the levels below |
| `database` | Users with `ALTER` on the database (or `INDEX`, for index kinds), and everyone below |
| `tenant` | The tenant's administrators (`SPRIG_TENANT_ADMIN`), and administrators of any ancestor tenant |
| `dc` | The data centre's operators |
| `platform` | Platform operators |

Platform operators may act on anything. A user who can see an item but not act on it, for example someone with `SELECT` on the table, sees it in the inbox as view-only.

A new recommendation's level is, by default, the scope its mode was set at: if `auto_index.mode = 'approve'` is set on `orders`, its index recommendations sit at the table level and the table's owners decide. A kind never sits below its narrowest level (see the table of kinds). `approvals.start_level` fixes a level instead: `table`, `database`, `tenant`, `dc` or `platform`.

## The life of a recommendation

```text
            ┌──── snooze ───► snoozed ──(time up)──┐
            │                                      ▼
raise ─► open ─── approve ─► approved ─► applying ─► applied
            │                                  └───► failed
            ├──── ignore ──► ignored
            ├──── (condition went away) ──► expired
            └──── (newer advice) ─────────► superseded
```

| State | Meaning |
|---|---|
| `open` | Waiting for a person, or a timer |
| `approved` | Approved; the deciding component runs it on its next cycle |
| `applying` | The change is running |
| `applied` | Done |
| `failed` | The change failed. An item stuck in `applying` for `approvals.applying_timeout_hours` (24) also becomes `failed` |
| `ignored` | Someone said no |
| `snoozed` | Out of the inbox until the snooze ends |
| `superseded` | Newer advice for the same target replaced it |
| `expired` | The condition went away (the component stopped proposing it), or re-checking before the change showed it no longer holds |

There is at most one live item per decision: when the component proposes the same change again, the open item is refreshed with the new evidence instead of a new item appearing. An item the component stops proposing expires: after 3 hours for shard, balancer, move and standby kinds, 24 hours for advisor and graph kinds, 3 days for index kinds, and 1 hour for failovers.

`reopen` brings an `ignored`, `expired`, `failed` or `snoozed` item back to `open`.

### Approve

Approving moves the item to `approved`, and the component applies it on its next cycle. You can give a reason, which is recorded with the decision. You can also approve **in a window**: the change runs only between a start and an end time (UTC). If the window passes before the change could run, the item expires.

### Ignore

Ignoring says no. Three forms:

| Form | Suppresses the same recommendation |
|---|---|
| Ignore once | For `approvals.ignore_once_days` (1) |
| Ignore for N days | For N days |
| Ignore forever | Until someone reopens it |

An ignored recommendation comes back early, as `recommendation.reraised`, if its evidence grows past `approvals.reraise_factor` (2) times what it was when you ignored it. A slow query that got twice as costly deserves another look.

### Snooze

Snoozing takes an item out of the inbox for N hours, after which it reopens. Use it for "not now".

### Comment

Anyone who may see an item may comment on it. The last 100 comments and 50 state changes are kept on each item.

### Emergencies

Some decisions can't wait: a failover when a writer is gone, for example. While `approvals.allow_emergency` is on (the default), a decision the component marks as an emergency is applied at once, even in `approve` mode, and raises a `recommendation.emergency` event so people see what happened.

## Timers: escalation and auto-apply

Two timers act on items nobody has decided:

| Setting | Default | Meaning |
|---|---|---|
| `approvals.escalate_after` | `off` | After this long, notify the next level up as well |
| `approvals.auto_apply_after` | `off` | After this long, approve the item automatically |
| `approvals.auto_apply_forbidden` | (empty) | Operator-only: kinds or families that never auto-apply, as a comma list |

Both take `off`, one duration for every kind (`90m`, `24h`, `3d`), or per-kind durations with a fallback:

```sql test
ALTER TABLE orders SPRIG SET (approvals.escalate_after = 'index.create=24h,*=3d');
ALTER TABLE orders SPRIG SET (approvals.auto_apply_after = '7d');
```

Escalation widens who is told, one step at a time: table owners, then database owners, then the tenant's administrators, then each ancestor tenant's administrators from the nearest up, then platform operators. The level of the item, and so who may approve it, doesn't change: escalation only brings more people in. Advice (`recommend` mode) never escalates or auto-applies.

## Notifications

Every event (`recommendation.new`, `.escalated`, `.approved`, `.auto_approved`, `.ignored`, `.snoozed`, `.reopened`, `.applying`, `.applied`, `.failed`, `.expired`, `.superseded`, `.reraised`, `.emergency`, `.commented`) goes to these channels, all carrying the same JSON:

| Channel | How to set it up |
|---|---|
| **Email** | Each user subscribes their own address in Studio (**Performance › Approval policy**, **My address**), with delivery `immediate`, `digest` or `off`, for every kind or a list of kinds. Immediate emails go out for new, escalated, applied, failed and emergency items. The digest of waiting items is sent daily at `approvals.digest_hour_utc` (8); `approvals.reminder = 'off'` turns it off. Operators set the SMTP server (`approvals.smtp_url`, `approvals.smtp_from`, `approvals.smtp_username`) and the operators' addresses for dc and platform items (`approvals.operator_emails`) |
| **Tenant webhook** | `approvals.webhook_connection`: a webhook connection, `<database>.<connection>` or `platform:<name>`, that receives the tenant's events. See [Streams, queues and sinks](../sql/streams-queues-sinks.md) |
| **Platform webhook** | Operator-only `approvals.webhook_url`: every event, signed with the controller's webhook secret, retried up to six times |
| **Stream** | `approvals.stream_database`: a database whose `sprig_recommendations` stream receives the tenant's events, for your own consumers |

Links in emails and events point at Studio when an operator sets `approvals.studio_url`. Notifications never slow or block a decision.

## The inbox

The same inbox is in Studio, the `sprig` shell, `sprigctl`, SQL and the REST API. Each shows only what you may see, and acts as you.

Recommendation ids name where an item lives: `t42.17` is item 17 of tenant 42, `dc.mum1.3` a data centre's, and `g.5` a global one. Inside a tenant, SQL and Studio use the plain number.

### Studio

Open **Performance › Approvals** (`/studio/t/<tenant>/inbox`). A badge on the activity bar shows how many items wait.

- Filter by **Kind** and **State**, and select an item to see its **Evidence**, history and comments.
- Act with **Approve**, **Approve in a window**, **Ignore**, **Snooze** and **Comment**.
- Tick several items, or **Select all**, and act on them together, for example **Snooze selected**.
- **Approval policy** (`/studio/t/<tenant>/inbox/policy`) shows every kind's mode, level and timers at the tenant, a database or a table, and holds your email subscription.

### SQL

```sql
SHOW SPRIG RECOMMENDATIONS [ALL] [FOR DATABASE db | FOR TABLE [db.]table];
SPRIG APPROVE RECOMMENDATION 17 [REASON 'text'];
SPRIG IGNORE RECOMMENDATION 17 [FOR 30 DAYS | FOREVER] [REASON 'text'];
SPRIG SNOOZE RECOMMENDATION 17 FOR 48 HOURS;
```

`ALL` includes closed items. See [EXPLAIN and workload](../sql/explain-and-workload.md).

### The sprig shell

```sh
sprig approvals list            # open items you may act on; --all for closed ones
sprig approvals show t42.17
sprig approvals approve 17 --reason 'checked with the orders team'
sprig approvals approve 17 --window 2026-11-14T01:00:00Z/2026-11-14T03:00:00Z
sprig approvals ignore 17 --days 30 --reason 'table is being redesigned'
sprig approvals snooze 17 --hours 48
sprig approvals comment 17 'waiting for the release'
```

### sprigctl

Operators see every tenant's items, and the dc and platform ones:

```sh
sprigctl recommendations list --tenant acme --kind index --state open
sprigctl recommendations list --level dc
sprigctl recommendations show t42.17
sprigctl recommendations approve t42.17 --reason 'reviewed' --window 2026-11-14T01:00:00Z/2026-11-14T03:00:00Z
sprigctl recommendations ignore t42.17 --reason 'not needed' --forever
sprigctl recommendations snooze t42.17 --hours 24
sprigctl recommendations reopen t42.17 --reason 'try again'
sprigctl recommendations policy --tenant acme --db shop --object orders
```

Actions are recorded as `op:<name>`, from `--actor`, `SPRIGCTL_ACTOR` or `$USER`.

### REST

| Method and path | Does |
|---|---|
| `GET /v1/recommendations` | The items you may see; filters `kind`, `state`, `level`, `all`, `db`, `table` |
| `GET /v1/recommendations/{id}` | One item with its evidence, history and comments |
| `POST /v1/recommendations/{id}:approve` (`:ignore`, `:snooze`, `:reopen`, `:comment`) | Act on one item |
| `POST /v1/recommendations:bulk` | One action on up to 200 items |
| `GET /v1/recommendations:policy` | Every kind's mode, level and timers |
| `GET`, `POST /v1/recommendations:subscriptions` | Email subscriptions; you set only your own |

See the [REST API reference](../reference/api.md).

## Housekeeping

Closed items are purged after `approvals.retention_days` (180). The approvals loop runs every `approvals.tick_seconds` (30) in each data centre's leader controller. Metrics are under `sprig_approvals_*`; see [Metrics](../reference/metrics.md).

## Related

- [Automatic indexing](automatic-indexing.md)
- [Shards and table groups](shards-and-table-groups.md)
- [Settings and scopes](settings-and-scopes.md)
- [Studio](../tools/studio.md)
- [Settings reference](../reference/settings.md#approvals)
