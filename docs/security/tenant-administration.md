---
title: Tenant administration
audience: [dba, operator]
sidebar:
  order: 3
---

# Tenant administration

Tenants form a tree. A reseller, a business unit or a SaaS platform can own a tenant with descendant tenants beneath it, one per customer or team, and administer them from above: set their settings and the bounds they must stay within, manage their approvals, and impersonate their users to help them. This page covers the tenant administrator's privilege, what it reaches in descendant tenants, impersonation, suspension, and how tenant administrators differ from platform operators.

For tenants and databases in general, see [Tenants and databases](../concepts/tenants-and-databases.md).

## The tenant tree

```text
acme                     depth 0, a root tenant
├── acme-eu              depth 1
│   └── pune             depth 2
└── acme-us              depth 1
```

Each tenant has its own catalog, accounts, databases and settings. A tenant's settings inherit from its ancestors unless it sets its own, and ancestors can bound what descendants may set. The tree is at most `tenant.max_depth` (64) deep, with the root at depth 0; deeper tenants are refused with `ER_SPRIG_TENANT_DEPTH_EXCEEDED` (39508).

### Creating and deleting tenants

Tenants are created and deleted by platform operators:

```sh
sprigctl tenant create acme
sprigctl tenant create pune --under acme-eu
sprigctl tenant delete pune
```

`create` bootstraps the tenant's catalog and returns once the tenant is active. A tenant name is a lowercase DNS label. A parent that is suspended, or whose tree is being changed, takes no children (39303, or `ER_SPRIG_TENANT_TREE_BUSY` 39509, which is retryable). `delete` removes a leaf tenant and drops its catalog; it asks for the name unless you pass `--yes`. A tenant with children can't be deleted (`ER_SPRIG_TENANT_HAS_CHILDREN`, 39362). A deleted tenant's name becomes free again once its catalog has been cleaned up.

## The SPRIG_TENANT_ADMIN privilege

`SPRIG_TENANT_ADMIN` makes a user an administrator of their tenant and of every descendant tenant. It allows every account statement (`CREATE USER`, `GRANT`, `CREATE ROLE`, `SHOW GRANTS FOR` anyone), every settings statement, and the descendant actions below. It doesn't by itself grant access to data: that comes from ordinary privileges. The first user of a new tenant holds `ALL` on `*.*` and `SPRIG_TENANT_ADMIN`.

```sql
GRANT SPRIG_TENANT_ADMIN ON *.* TO 'ops'@'%';
```

It is granted only at `*.*`, only by name (`ALL` never includes it), and only by someone who holds it with `GRANT OPTION`. Hold it on as few accounts as you can, and consider `auth.mfa_required = 'admins'` so that every holder needs a second factor for HTTP logins.

### What it reaches in descendant tenants

| Action in a descendant | How |
|---|---|
| Set the descendant's settings | `ALTER TENANT <name> SPRIG SET (...)`, `ALTER TENANT <name> SPRIG RESET (...)` |
| Bound the descendant's settings | `ALTER TENANT SPRIG SET BOUNDS (...)` on your own tenant: bounds apply to every descendant |
| See and act on the descendant's recommendations | The approvals inbox, Studio's tenant switcher |
| Impersonate a user of the descendant | `POST /v1/auth/impersonate`, Studio |
| Reset a user's second factor, test row-level security as a user | `POST /v1/auth/factors:reset`, `POST /v1/policies:testAs` |
| Write row-level security policies over the subtree | `SPRIG_TENANT_MANAGED()` and `SPRIG_TENANT_IN_SUBTREE()` in policies (see [Row-level security](row-level-security.md)) |

Data views and SQL sessions always use your home tenant: logging in as `acme.ops` doesn't give you a SQL session in `pune`. To see what a descendant's user sees, impersonate them.

`ALTER TENANT <name>` writes only to a descendant of your tenant. For any other name, including one that doesn't exist, it fails with error 1227, so the answer never reveals other tenants:

```sql test-error
ALTER TENANT no_such_tenant SPRIG SET (auth.password_lock_minutes = 30);
```

Changes made from above are recorded in both tenants' audit streams, with the acting administrator; see [Audit](audit.md).

### Bounds on descendants

Bounds are limits an ancestor puts on a setting: a minimum, a maximum, an allowed list, or a lock that stops anyone below from changing it. They apply to narrower scopes of the same tenant and to every descendant.

```sql test
ALTER TENANT SPRIG SET BOUNDS (auto_index.max_per_table MAX 6,
                               auto_index.mode ALLOWED ('on', 'recommend'));
SHOW SPRIG SETTINGS LIKE 'auto_index.m%';
ALTER TENANT SPRIG RESET BOUNDS (auto_index.max_per_table, auto_index.mode);
```

```sql
-- Nobody below acme may turn off impersonation or change its limit.
ALTER TENANT SPRIG SET BOUNDS (auth.impersonation LOCKED, auth.impersonation_max_minutes MAX 30);
```

A value outside a bound fails with `ER_SPRIG_SETTING_OUT_OF_BOUNDS` (39080); changing a locked setting fails with `ER_SPRIG_SETTING_LOCKED` (39083). Tightening a bound never fails: values already set below it are clamped or ignored, and the statement answers with one warning (39080) per affected value, shown by `SHOW WARNINGS`. Account clauses, such as `FAILED_LOGIN_ATTEMPTS`, must also lie within the bounds. Setting bounds needs `SPRIG_SETTINGS_ADMIN` (or `SPRIG_TENANT_ADMIN`). See [Settings and scopes](../concepts/settings-and-scopes.md).

Each descendant holds a copy of its ancestors' settings, refreshed when an ancestor changes them. The operator setting `tenant.ancestry_mode` chooses how; only copying is built, so use `auto` (the default) or `copy`, which behave the same at any subtree size.

## Impersonation

An administrator can open a session as another user to see exactly what they see, for support or to check permissions. Every impersonation is audited with both identities.

```sh
curl -s https://gw.example.com:8443/v1/auth/impersonate \
  -H 'Authorization: Bearer <your-session-token>' -H 'Content-Type: application/json' \
  -d '{"tenant": "pune", "user": "app", "host": "%", "reason": "ticket 4411: missing orders", "minutes": 15}'
```

In Studio, open **Access control**, **Users and access**, choose **Impersonate** next to the user, and give a reason and a duration. Every statement then runs as that user, and the audit log records you both.

The rules:

| Rule | Detail |
|---|---|
| Caller | A signed-in session (not HTTP Basic or an API key) of a user with `SPRIG_TENANT_ADMIN` over the target tenant: their own tenant or a descendant |
| Target | Any user of that tenant who doesn't administer the caller's tenant |
| Tenant setting | `auth.impersonation = on` at the target tenant (an ancestor can lock it off with a bound) |
| Reason | Required |
| Duration | 1 to `auth.impersonation_max_minutes` (60) minutes; 15 if you don't say |
| Second factor | When `auth.mfa_required` covers the caller, the caller's session must have been signed in with a second factor (else `ER_SPRIG_MFA_REQUIRED`, 39304) |

A refused impersonation fails with `ER_SPRIG_IMPERSONATION_DENIED` (39305) and says why.

An impersonation session:

- carries the administrator in its token's `act` claim, so every action is attributed to both;
- has no refresh token and ends when its access token expires, at the end of the duration;
- can't impersonate again, change passwords or second factors, or create API keys.

Impersonation is logged as `auth.impersonation_started` and counted by `sprig_auth_impersonations_total`.

To turn impersonation off for your tenant and everything below it:

```sql
SPRIG SET auth.impersonation = 'off';
```

## Suspension

Platform operators suspend and resume tenants with `sprigctl`. A reason is required; it is shown by `sprigctl tenant show` and kept in the audit log.

```sh
sprigctl tenant suspend acme --reason "invoice 2026-118 overdue"
sprigctl tenant resume acme
```

The commands call the Admin API: `POST /admin/v1/tenants/{name}:suspend` with `{"reason": "..."}`, and `POST /admin/v1/tenants/{name}:resume`. The reason may be up to 1,024 characters; an empty one fails with 39792 `ER_SPRIG_ADMIN_REQUEST_INVALID`. Suspending a tenant that is already suspended, or resuming one that isn't, changes nothing and says so.

A suspension covers the tenant and every tenant below it:

- Every new login fails with `ER_SPRIG_TENANT_SUSPENDED` (39303; HTTP 403). The error is returned only after the password has checked out, so it never reveals to a stranger that the tenant exists.
- Open sessions get `ER_SPRIG_SESSION_REVOKED` (39306) at their next statement, while `auth.close_sessions_on_revoke` is on (the default). When a descendant's sessions can't be reached this way (for example, it is homed in another data centre), the command lists it; those sessions end when they next reconnect.
- Triggers, sinks and scheduled runs of the tenant wait, and start again at `resume`.
- Housekeeping keeps running: shard moves, index builds, search copies, key-value expiry and document TTL expiry.
- The tenant takes no new child tenants (39303).

Both calls are audited, as `admin.tenant_suspended` and `admin.tenant_resumed`, in the tenant's stream and the platform stream. While the audit spool is full they fail with 39970 `ER_SPRIG_AUDIT_UNAVAILABLE`. See [Audit](audit.md).

## Operators and tenant administrators

| | Platform operator | Tenant administrator |
|---|---|---|
| Who | People who run the SprigDB platform | Users of a tenant with `SPRIG_TENANT_ADMIN` |
| Account | A named operator in the platform's directory, not in any tenant | A user in the tenant's catalog |
| Logs in with | `sprigctl login`, `sprig`, Studio operator mode | Any client, as `tenant.user` |
| Reaches | Clusters, data centres, placement, jobs, every tenant's settings, tenant creation and deletion, failover | Their tenant and its descendants |
| Second factor | Required by default (`operators.mfa_required`) | Optional (`auth.mfa_required`) |
| Rights | Role `platform_operator` (everything) or `dc_operator` of named data centres | MySQL grants |

### Operator accounts

Operators log in as themselves with a password and a TOTP code; the first login enrols the factor and prints the recovery codes once:

```sh
sprigctl login --name priya
sprigctl whoami
sprigctl operator create sam --role dc --dcs dc-a
sprigctl operator list
sprigctl operator disable sam
sprigctl logout
```

The roles:

- `platform_operator` may do everything.
- `dc_operator` acts only on its data centres' controllers, and its writes are limited to clusters, jobs, moves, in-doubt transactions, recommendations, drains of its data centres, and data-centre and cluster settings.

A login session lasts `operators.session_hours` (12). After `operators.lockout_failures` (5) failed logins in a row, an operator is locked for `operators.lockout_minutes` (15). Operators issue scoped tokens for automation with `sprigctl token create`; see [API keys and tokens](api-keys-and-tokens.md).

Until the first named operator logs in, the controller's bootstrap token (`[controller] admin_token_file`) works; after that it is refused with `ER_SPRIG_OPERATOR_LOGIN_REQUIRED` (39310) unless `[controller] admin_token_break_glass` is set.

### Operator mode in Studio

Studio offers an operator mode to the logins listed in `studio.operator_logins` (a comma-separated list of `tenant.user`). It is controlled by:

| Setting | Default | Meaning |
|---|---|---|
| `studio.operator_logins` | empty | Logins holding platform operator rights in Studio |
| `studio.operator_mode` | `on` | Offer operator mode at all |
| `studio.operator_require_mfa` | false | A second factor is needed to enter operator mode |
| `studio.operator_mtls` | `off` | `required`: a verified client certificate is needed for operator mode. Set `listener.https.client_cert` and `tls.bundles.default.client_ca_path` so the gateway asks for one; see [TLS](tls.md#operator-mode) |

Refusals use `ER_SPRIG_STUDIO_OPERATOR_MFA_REQUIRED` (39926), `ER_SPRIG_STUDIO_OPERATOR_CERT_REQUIRED` (39927) and `ER_SPRIG_STUDIO_OPERATOR_MODE_DISABLED` (39928). Entering operator mode and every action taken in it are audited.

## Settings

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `auth.impersonation` | text | `on` | tenant, dc, global | Allow administrators above to impersonate users here |
| `auth.impersonation_max_minutes` | int | 60 | tenant, dc, global | Longest impersonation |
| `auth.mfa_required` | text | `off` | tenant, dc, global | `off`, `admins` or `all` |
| `tenant.max_depth` | int | 64 | global (operator only) | Deepest tenant tree |
| `tenant.ancestry_mode` | text | `auto` | tenant, global (operator only) | How descendants hold their ancestors' settings |
| `operators.mfa_required` | bool | true | global (operator only) | Operators log in with a second factor |
| `operators.session_hours` | int | 12 | global (operator only) | Operator login session |
| `operators.lockout_failures` | int | 5 | global (operator only) | Failures before an operator is locked; 0 never |
| `operators.lockout_minutes` | int | 15 | global (operator only) | How long a locked operator waits |
| `operators.token_max_days` | int | 90 | global (operator only) | Longest life of an operator's token |

## Not supported yet

- `USE TENANT` or any other way for an administrator to open a SQL session in a descendant tenant without impersonating a user.
- Suspending or resuming a tenant through SQL or Studio; use `sprigctl tenant suspend` and `resume`, or the Admin API.
- Moving a subtree to another parent.
- `tenant.ancestry_mode = shared` or `reference`. With either, descendants' copies of their ancestors' settings stop being refreshed.

## Related

- [Grants and roles](grants-and-roles.md)
- [Accounts and logins](accounts-and-logins.md)
- [Audit](audit.md)
- [Tenants and databases](../concepts/tenants-and-databases.md)
- [Settings and scopes](../concepts/settings-and-scopes.md)
