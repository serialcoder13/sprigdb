---
title: Residency
audience: [operator, dba]
sidebar:
  order: 8
---

# Residency

A residency policy keeps a tenant's data, and its credentials, inside a chosen set of data centres: those in one jurisdiction, one country, or a named list. SprigDB checks the policy at every point where the tenant's data could move to another data centre, so a home move, a standby, a shard move, a failover or a sink can't take it outside. Use it for customers bound by data-protection or sovereignty rules.

## Data-centre tags

Residency is a condition over **tags** on data centres. Operators tag a data centre when they register it:

```sh
sprigctl dc register dc-fra --tags jurisdiction=EU,country=DE,provider=hetzner
sprigctl dc register dc-ams --tags jurisdiction=EU,country=NL
sprigctl dc register dc-iad --tags jurisdiction=US,country=US
```

Every data centre also has the pseudo-tag `dc`, its id.

## Policies

A policy is a condition over tags:

```text
policy   := or_expr                         (an empty policy allows every data centre)
or_expr  := and_expr { or and_expr }
and_expr := unary { and unary }
unary    := not unary | ( or_expr ) | pred
pred     := name = "value" | name != "value" | name in ("value", ...)
name     := dc | a tag key ([a-z][a-z0-9_]{0,31})
```

- `name = "v"` needs the tag present and equal; `name != "v"` holds when the tag is missing or different; `name in (...)` needs the tag present and listed.
- Keywords (`and`, `or`, `not`, `in`) are lower case. Strings are double-quoted, with `\"` and `\\` escapes.

Examples:

| Policy | Allows |
|---|---|
| `jurisdiction = "EU"` | Every data centre tagged `jurisdiction=EU` |
| `country in ("DE", "NL")` | Germany and the Netherlands |
| `jurisdiction = "EU" and provider != "acme-cloud"` | EU data centres not run by one provider |
| `dc in ("dc-fra", "dc-ams")` | Two named data centres |

A policy that doesn't parse fails with `ER_SPRIG_RESIDENCY_INVALID` (39506).

### Inheritance down the tenant tree

A tenant's allowed data centres are its own policy's set intersected with its parent's:

```text
allowed(T) = resolve(policy(T)) ∩ allowed(parent(T))      (a root uses its own set)
no policy on T or any ancestor → unrestricted
```

A descendant can narrow its ancestors' residency but never widen it. A new child tenant's home must lie in its parent's set (39507).

## Setting a tenant's residency

Operators set residency with `sprigctl`. Without `--apply`, nothing changes: the plan for the tenant and every descendant is printed.

```sh
sprigctl tenant residency acme --policy 'jurisdiction = "EU"'
```

```text
tenant:              acme (42)
policy:              jurisdiction = "EU"
allowed DCs:         dc-ams, dc-fra
deny remote access:  false
state:               compliant
plan for acme (home dc-iad):
  - ...
plan for pune (home dc-fra): nothing to do
dry run: nothing changed (add --apply)
```

Then apply it:

```sh
sprigctl tenant residency acme --policy 'jurisdiction = "EU"' --apply
sprigctl tenant residency acme                                   # show it
sprigctl tenant residency acme --deny-remote-access true --apply
sprigctl tenant residency acme --policy '' --apply               # remove the tenant's own policy
```

Setting a policy:

1. parses it (39506 if invalid);
2. resolves it through the tree; an empty result fails with `ER_SPRIG_RESIDENCY_UNSATISFIABLE` (39507), naming the ancestor that rules out the rest;
3. warns about data centres admitted only because a tag is missing (with `!=`);
4. plans the changes for the tenant and every descendant; with `--apply`, a step that has no way to comply fails with 39507;
5. writes the policy, its allowed set and `deny_remote_access` to the tenant's directory entry, readable by every data centre, and the allowed set to every descendant.

Residency lives in the global directory rather than in tenant settings, so every data centre can read it, even one that holds none of the tenant's data.

## What residency limits

Each path that could put a tenant's data in another data centre checks the allowed set it commits against, so a policy changed at the same moment stops it at its commit point:

| Path | Check |
|---|---|
| Moving the tenant's home to another data centre | The target must be allowed, at the start and again at commit (`ER_SPRIG_TENANT_MOVE_REFUSED`, 39550) |
| Placement and the balancer | No cluster in a data centre outside the set |
| Standbys | No standby is created outside the set, and existing ones there are dropped |
| Automatic data-centre failover | Standbys outside the set are never promoted |
| Manual failover | The target must be allowed (`ER_SPRIG_FAILOVER_REFUSED`, 39505) |
| New child tenants | The child's home must be in the parent's set (39507) |
| Logins | Credentials stay in allowed data centres (below) |
| Change feeds | A feed can't be read in a data centre outside the set (39507) |
| Sinks | A destination must carry tags the policy admits (`ER_SPRIG_SINK_RESIDENCY`, 39214) |
| Inherited data and shares | Copies and fan-in readers outside the set are refused (`ER_SPRIG_INHERIT_RESIDENCY`, 39243; `ER_SPRIG_SHARE_RESIDENCY_CONFLICT`, 39889) |

A missing or malformed allowed set in a tenant's residency allows nothing: the checks fail closed. If the global directory can't be reached, every reader keeps the last set it saw.

### Sinks

A sink's destination host must carry residency tags, either declared on its connection or known from the provider's region in the host name, and they must satisfy the tenant's policy. A destination with no tags is refused when the tenant has a policy: "`<host>` has no residency tags, and the tenant has a residency policy". See [Sinks](../data-models/sinks.md).

### Messages between tenants

With `residency.cross_tenant_messaging_strict = on` (global, operator only), a pinned tenant can't publish into another tenant's streams or queues stored in a data centre its residency doesn't allow (`ER_SPRIG_STREAM_RESIDENCY`, 39949, or 39243). It is `off` by default.

## Converging after a change

A controller in each data centre runs a residency loop for the tenants homed there, every 10 seconds while a tenant isn't compliant, and again whenever the directory or a data centre's record changes:

1. re-resolve the allowed set (data-centre tags may have changed);
2. drop standbys outside the set;
3. start a move of the tenant's home if it is outside the set, whatever `migration.mode` says;
4. pause change-feed subscriptions read in data centres outside the set;
5. move shards off clusters in data centres that left the set, through the balancer.

`sprigctl tenant residency <tenant>` shows the state:

| State | Meaning |
|---|---|
| `compliant` | Everything is inside the allowed set |
| `converging` | Moves are under way; `pending` lists them |
| `violated` | Something is outside the set and can't be moved yet; `pending` says what |

| Metric | Meaning |
|---|---|
| `sprig_controller_residency_tenants{state}` | Tenants by state |
| `sprig_controller_residency_changes_total` | Policy changes applied |
| `sprig_controller_residency_violations_total{point}` | Corrections the residency loop made, by kind: `standby` dropped, `home` move started, `feed` paused |
| `sprig_controller_residency_remote_traffic_total` | Times the best data centre for a tenant's traffic was outside its residency, so the tenant wasn't moved there |

## Logins from other data centres

Gateways outside a pinned tenant's allowed data centres never load its credentials. When a client of a pinned tenant connects to such a gateway:

- by default, the gateway **hands the login off** to a gateway in the nearest allowed data centre over the internal peer port (7403, mutual TLS). The allowed data centre checks the password with its own login service, lockout counters and audit, and returns only the result. The client must log in over TLS, because the password has to be checked in full there (the fast path isn't available). Each handoff costs one round trip between data centres and is audited as `auth.handoff`;
- with `--deny-remote-access true`, the gateway refuses the login with `ER_SPRIG_REMOTE_ACCESS_DENIED` (39302) instead.

Handoffs cover the MySQL, MariaDB and HTTP logins. A handoff carries whether the client presented a verified certificate, with its subject and issuer, so accounts with `REQUIRE X509`, `REQUIRE SUBJECT` or `REQUIRE ISSUER` are checked in the allowed data centre as on a direct login (see [Client certificates](tls.md#client-certificates)). A gateway of an earlier release hands off no subject or issuer, so `REQUIRE SUBJECT` and `REQUIRE ISSUER` accounts are refused through it until it is upgraded. After a login, statements go to the tenant's clusters in its home data centre as for any tenant.

## What happens on failover

Residency is part of every failover decision:

- **Automatic data-centre failover** considers only standbys in allowed data centres. A tenant whose only up-to-date standby is outside the set is not failed over: it stays fenced in the lost data centre until that comes back, or an operator acts. The failover's decision records "`<dc>`: outside the tenant's residency".
- **The promotion's commit point** re-reads the residency in force, so a policy changed during a failover is respected.
- **Manual failover** to a data centre outside the set is refused with `ER_SPRIG_FAILOVER_REFUSED` (39505).

Plan standbys accordingly: a tenant pinned to the EU needs its standby in another EU data centre to survive the loss of its home. See [Failover](../operate/failover.md).

## Settings and errors

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `residency.cross_tenant_messaging_strict` | text | `off` | global (operator only) | `on`: a pinned tenant's messages into another tenant's streams and queues are refused outside its residency |

| Code | Name | Meaning |
|---|---|---|
| 39302 | `ER_SPRIG_REMOTE_ACCESS_DENIED` | A login at a gateway outside the allowed data centres, with remote access denied |
| 39505 | `ER_SPRIG_FAILOVER_REFUSED` | A manual failover to a data centre outside the set |
| 39506 | `ER_SPRIG_RESIDENCY_INVALID` | The policy doesn't parse |
| 39507 | `ER_SPRIG_RESIDENCY_UNSATISFIABLE` | No data centre satisfies the policy and its ancestors', a step can't comply, or a feed is read outside the set |
| 39550 | `ER_SPRIG_TENANT_MOVE_REFUSED` | A home move to a data centre outside the set |
| 39214 | `ER_SPRIG_SINK_RESIDENCY` | A sink destination outside the policy |
| 39243 | `ER_SPRIG_INHERIT_RESIDENCY` | Inherited data or a message would land outside the set |
| 39889 | `ER_SPRIG_SHARE_RESIDENCY_CONFLICT` | A share conflicts with a residency policy |
| 39949 | `ER_SPRIG_STREAM_RESIDENCY` | A message into another tenant's stream outside the publisher's residency |

## Related

- [Clusters and data centres](../concepts/clusters-and-data-centres.md)
- [Data centres](../operate/data-centres.md)
- [Failover](../operate/failover.md)
- [Tenant administration](tenant-administration.md)
- [Audit](audit.md)
