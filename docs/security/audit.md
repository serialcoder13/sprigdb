---
title: Audit
audience: [dba, operator]
sidebar:
  order: 7
---

# Audit

SprigDB records security events: every login decision, every account change, operator actions, impersonation and setting changes. Events never contain a secret: no password, scramble, verifier, key or token is ever written. This page lists what is recorded, where it goes, how the pipeline stays durable, and the settings that control it.

## Where audit events go

Every audit event goes to two places:

1. **The process log**, as one JSON line through the log target `sprig_audit`, so a log shipper can separate audit lines from the rest. Each line carries a `stream` field: `tenant-<id>` for a tenant's events, or `deployment` and `platform` for platform events.
2. **The audit pipeline**, when it is configured: a durable local spool on each process, shipped every second to the audit sink.

Configure the pipeline in each process's configuration file:

```toml
[audit]
spool_dir = "/var/lib/sprigdb/audit-spool"   # local to this process
sink_dir = "/mnt/audit"                       # required with spool_dir
```

The sink is a directory (a mounted volume or object-storage mount) that receives files in this layout:

```text
/mnt/audit/audit/<stream>/<date>/<instance>-<first seq>-<last seq>.jsonl
```

- `<stream>` is `tenant-<tenant id>` for a tenant's events and `platform` for operators, settings and platform decisions. Keeping tenants in separate streams lets you hand a tenant its own audit trail.
- Each line is one record with the process instance, a sequence number, the event name and its body.

Without `[audit]`, events are only in the process log.

### Durability

```text
emit(event):
  append to the spool; fsync every 100 ms or 64 events
ship, every second:
  from the last acknowledged sequence number, write to the sink (durable before acknowledged)
  trim the spool
```

- A restart reloads whatever was spooled but not yet shipped, so nothing is lost.
- A record may reach the sink twice after a crash; deduplicate on `(instance, seq)`.

### When the spool is full: failing closed

If the sink is unreachable and the spool grows past `audit.spool_max_mb` (512), SprigDB stops making changes it can't audit:

- **Account statements** (`CREATE USER`, `ALTER USER`, `DROP USER`, `GRANT`, `REVOKE`, role statements) and **Admin API changes** (`sprigctl` writes, settings changes, operator actions) fail with `ER_SPRIG_AUDIT_UNAVAILABLE` (39970), which is retryable.
- **Logins continue**, so applications keep working. Their events are kept in memory, up to 10,000, then dropped and counted in `sprig_audit_dropped_total`.

Fix the sink, and the spool drains within seconds.

## What is audited

### Logins and accounts (tenant streams)

Recorded by every gateway for every protocol that logs in with a password or key (MySQL, MariaDB, HTTP Basic and API keys, and the others through the same login service), and by account changes from any client:

| Event | When |
|---|---|
| `auth.login_succeeded` | A login succeeded |
| `auth.login_failed` | A login was refused; `reason` says why (`bad_credentials`, `locked`, `temp_locked`, `suspended`, `expired`, `tls_required`, ...) |
| `auth.rate_limited` | An address was refused for too many failures |
| `auth.account_locked`, `auth.account_unlocked` | `ACCOUNT LOCK` or `UNLOCK`, or a temporary lock after failed logins and its end |
| `auth.password_changed` | A password was set or changed |
| `auth.password_rejected` | A new password failed the password policy |
| `auth.user_created`, `auth.user_dropped` | `CREATE USER`, `DROP USER`, `sprigctl tenant user add` |
| `auth.account_changed` | Other account changes: expiry, password and lockout clauses |
| `authz.privileges_granted`, `authz.privileges_revoked` | `GRANT` and `REVOKE` of privileges, including `REVOKE ALL PRIVILEGES, GRANT OPTION`; `detail` is the statement as parsed |
| `authz.role_created`, `authz.role_dropped` | `CREATE ROLE`, `DROP ROLE` |
| `authz.roles_granted`, `authz.roles_revoked` | `GRANT role TO ...`, `REVOKE role FROM ...` |
| `authz.default_roles_set` | `SET DEFAULT ROLE` |
| `auth.verifiers_added` | SCRAM verifiers were added at a login |
| `auth.handoff` | A login was decided in another data centre because of the tenant's [residency](residency.md) |

Each event carries:

| Field | Meaning |
|---|---|
| `event`, `at_ms` | The event and its time (UTC milliseconds) |
| `tenant`, `tenant_name` | The tenant the event is about |
| `user`, `host`, `user_id` | The account; for an unknown account, the login name as sent |
| `actor` | The administrator who acted, when not the user, with their tenant |
| `client_ip`, `gateway` | Where the request came from, and which gateway decided |
| `protocol` | `mysql`, `mariadb`, `http`, or `admin` for account changes |
| `mechanism` | `caching_sha2_password` (fast or full path), `native`, `basic`, `api_key` |
| `outcome`, `reason`, `detail` | `ok` or `refused`, why, and more (never a secret) |

A change made by an administrator of an ancestor tenant is written to both tenants' streams: the target tenant sees who changed its account, and the ancestor sees what its administrator did.

```json
{"event":"auth.login_failed","at_ms":1763023412345,"tenant":42,"tenant_name":"acme","user":"app","host":"%","client_ip":"10.20.1.7","gateway":"gw-a-1","protocol":"mysql","mechanism":"caching_sha2_password","outcome":"refused","reason":"bad_credentials"}
```

### Sessions, keys, factors and impersonation (process log)

Gateways also write these events to the `sprig_audit` log target:

| Event | When |
|---|---|
| `studio.session_started`, `studio.session_ended` | A token session (Studio or API) began or ended |
| `auth.session_revoked` | A session was revoked (logout, refresh-token reuse, account change) |
| `auth.api_key_created`, `auth.api_key_revoked` | An API key was created or revoked |
| `auth.factor_enrolled`, `auth.factor_reset` | A second factor was enrolled, or a user's factors were reset |
| `auth.impersonation_started` | An administrator began impersonating a user, with both identities, the reason and the duration |
| `studio.view_as` | The row-level security tester ran a statement as a user (caller, user, tenant, row count and a hash of the statement) |
| `studio.export` | A result was exported from Studio |
| `studio.operator_mode_entered`, `studio.operator_action`, `studio.operator_session` | Operator mode in Studio |

Collect them from the gateway logs by their `sprig_audit` target.

### Operators and the platform (platform stream)

Recorded by controllers:

| Event | When |
|---|---|
| `operator.login`, `operator.login_failed`, `operator.locked` | Operator logins |
| `operator.created`, `operator.disabled`, `operator.enabled` | Operator accounts |
| `operator.password_changed`, `operator.factor_enrolled`, `operator.factor_reset` | Operator credentials |
| `operator.token_issued`, `operator.service_identity_issued`, `operator.token_revoked` | Operator tokens |
| `operator.switched` | Named operators took over from the bootstrap token |
| `admin.denied` | An Admin API call refused for lack of rights |
| `breakglass.admin_token` | The bootstrap token was used under break-glass |
| `settings.changed` | A setting changed through the Admin API (`sprigctl settings`), with key, scope, old and new value, who and why |
| `admin.tenant_failover`, `admin.dc_state` | A manual tenant failover, a data centre's state changed |
| `admin.tenant_suspended`, `admin.tenant_resumed` | `sprigctl tenant suspend` and `resume`, with the reason and the tenants below; also written to the tenant's stream |
| `admin.accounts_exported`, `admin.accounts_imported` | Accounts copied out of or into a tenant |

Setting changes made with SQL (`SPRIG SET`, `ALTER ... SPRIG SET`) are logged by the gateway as `settings.set` with key, scope, old and new value, who and why, in the regular log.

## Reading the audit trail

The sink holds plain JSON lines, so any log tool reads them. For example, failed logins for tenant 42 on one day:

```sh
cat /mnt/audit/audit/tenant-42/2026-11-12/*.jsonl \
  | jq -c 'select(.event == "auth.login_failed") | {at_ms, user, client_ip, reason}'
```

## Settings and metrics

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `audit.spool_max_mb` | int | 512 | dc, global (operator only) | Local spool size before account and admin changes fail closed |
| `audit.retention_days` | int, 1 to 36500 | 365 | dc, global (operator only); a tenant value is accepted but not read | Days of audit files kept in the sink |

Each process that writes to the sink runs a retention pass a minute after it starts and then hourly. The pass removes whole day directories (`audit/<stream>/<date>`) older than `audit.retention_days`, oldest first and at most 100 per pass, coming back after 10 seconds while more are due, so a record is kept at least that many days. The value applies to every stream alike, read at the process's data centre (or global) scope. `sprig_audit_retention_removed_total` counts the day directories removed. If your sink storage has its own lifecycle rules, keep them no shorter than this setting.

| Metric | Meaning |
|---|---|
| `sprig_audit_events_total` | Events recorded by the pipeline |
| `sprig_audit_spool_bytes`, `sprig_audit_spool_max_bytes` | Spool use and its limit; alert well before they meet |
| `sprig_audit_ship_failures_total` | Failed writes to the sink |
| `sprig_audit_dropped_total` | Login events dropped while the spool was full |
| `sprig_auth_audit_events_total{event}` | Login and account events by name |

## Not supported yet

- Tenant-level `audit.retention_days`: the setting accepts a tenant value, but retention reads only the data-centre and global scopes, so every tenant's stream is kept for the same time.
- An S3 or other object-storage sink; the sink is a directory.
- Auditing of data access (statements that read or change rows).

## Related

- [Accounts and logins](accounts-and-logins.md)
- [Tenant administration](tenant-administration.md)
- [Residency](residency.md)
- [Monitoring](../operate/monitoring.md)
- [Configuration](../operate/configuration.md)
