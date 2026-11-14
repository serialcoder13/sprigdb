---
title: Accounts and logins
audience: [dba, operator]
sidebar:
  order: 1
---

# Accounts and logins

Every user of SprigDB belongs to a tenant and logs in with a password, an API key or a SprigDB token. One password works on every protocol: SprigDB stores a verifier for each login mechanism when the password is set. This page covers users and passwords, how each protocol logs in, password policies, lockout and login rate limits, and second factors for HTTP logins.

For the account statements themselves (`CREATE USER`, `GRANT`), see [Tenants and accounts in SQL](../sql/tenants-and-accounts.md); for privileges, [Grants and roles](grants-and-roles.md).

## Login names

A login names its tenant and user: `acme.app` is user `app` of tenant `acme`. The tenant is the part before the first dot. Accounts are matched by user and client host, as in MySQL (`'app'@'%'`, `'app'@'10.0.%'`), with the most specific host first.

Two other ways name the tenant without a prefix:

- **Tenant endpoints.** With `listener.tenant_sni_suffixes` set to `db.example.com`, a client that connects over TLS to `acme.db.example.com` logs in to tenant `acme` with the plain user name `app`. There the whole login name is the user name, so `globex.app` at `acme.db.example.com` doesn't reach `globex`: it fails with 1045. See [Tenant endpoints by server name](tls.md#tenant-endpoints-by-server-name).
- **Bound listeners.** The Redis and AMQP listeners can take plain user names for one tenant: set `listener.redis.tenant` or `listener.amqp.tenant` to that tenant's name. A tenant endpoint wins over a bound listener.

## Users and passwords

Create users and set passwords with MySQL's statements, from any MySQL or MariaDB client logged in as a user with `CREATE USER` (or `SPRIG_TENANT_ADMIN`):

```sql
CREATE USER 'report'@'%' IDENTIFIED BY '<password>' PASSWORD EXPIRE INTERVAL 90 DAY;
ALTER USER 'report'@'%' IDENTIFIED BY '<new-password>' RETAIN CURRENT PASSWORD;
ALTER USER 'report'@'%' DISCARD OLD PASSWORD;
DROP USER 'report'@'%';
```

The full form:

```text
CREATE USER [IF NOT EXISTS] acct [auth] {, acct [auth]} [DEFAULT ROLE roles]
    [REQUIRE NONE | SSL | X509 | tls_names] [password options] [ACCOUNT LOCK | UNLOCK]
    [COMMENT 'text' | ATTRIBUTE 'json']
ALTER USER [IF EXISTS] acct [auth [RETAIN CURRENT PASSWORD]] {, ...} [DEFAULT ROLE roles]
    [REQUIRE ...] [password options] [ACCOUNT LOCK | UNLOCK] [DISCARD OLD PASSWORD]
    [COMMENT 'text' | ATTRIBUTE 'json']
DROP USER [IF EXISTS] acct {, acct}
auth := IDENTIFIED BY 'password' | IDENTIFIED WITH plugin [BY 'password']
tls_names := SUBJECT 'name' [AND] ISSUER 'name' (either, or both in any order)
```

`ATTRIBUTE` stores a JSON object on the account, and `COMMENT 'text'` stores `{"comment": "text"}`; on `ALTER USER` the object is merged into what the account holds, and a key set to `null` is removed. Row-level security policies read these values with `SPRIG_USER_ATTR('key')`; see [Row-level security](row-level-security.md). A value that isn't a JSON object fails with 3934.

Not accepted (error 1235): `IDENTIFIED ... AS 'hash'` and `IDENTIFIED BY PASSWORD 'hash'`, `RENAME USER`, `REQUIRE CIPHER`, and resource limits (`WITH MAX_...`).

Operators can also create a user with `sprigctl tenant user add <tenant> <user> --password ...`. In Studio, **Access control**, then **Users and access**, generates the same statements and shows them before running them; passwords are sent through `POST /v1/users:setPassword` so they never appear in statement text or history.

### Changing your own password

Any user can change their own password, even when it has expired:

```sql
SET PASSWORD = '<new-password>';
ALTER USER USER() IDENTIFIED BY '<new-password>' REPLACE '<current-password>';
```

`REPLACE` names the current password. It is required when the account has `PASSWORD REQUIRE CURRENT` (or `auth.password_require_current` is on), on the MySQL listener only: MariaDB has no `REPLACE` clause, so the rule doesn't apply on 3307.

### Dual passwords

`RETAIN CURRENT PASSWORD` keeps the old password working beside the new one, so you can roll a password out to many application instances without downtime. Both passwords log in until `ALTER USER ... DISCARD OLD PASSWORD`. Without `RETAIN`, the old password moves to the history.

### Verifiers per mechanism

SprigDB never stores a password. When one is set, it stores one verifier per mechanism, so the same password works over every protocol:

| Verifier | Used by | Format and cost |
|---|---|---|
| `caching_sha2_password` | MySQL and MariaDB listeners, HTTP Basic, Redis `AUTH`, Bolt basic, AMQP and Kafka `PLAIN` | SHA-256 crypt, `auth.caching_sha2_rounds` (5000) rounds; the same format MySQL 8.4 stores |
| `mysql_native_password` | Old MySQL clients | `SHA1(SHA1(password))`; stored and accepted only with `auth.mysql_native_password = true` |
| SCRAM-SHA-256 | MongoDB, Kafka | PBKDF2, `auth.scram_sha256_iterations` (15000); the password is normalized with SASLprep |
| SCRAM-SHA-512 | Kafka | PBKDF2, `auth.scram_sha512_iterations` (15000) |

A password that SASLprep refuses (control characters, unassigned code points, invalid UTF-8) gets no SCRAM-SHA-256 verifier and can't be used over MongoDB.

Accounts created before SCRAM verifiers existed, or imported with only a MySQL hash, get their SCRAM verifiers at the next login that sees the clear password (any login over TLS that takes the full path), while `auth.backfill_verifiers` is on (the default).

Verifiers live in the tenant's catalog, which no tenant account can read. Changing the cost settings affects only passwords set afterwards.

## How each protocol logs in

| Protocol | Mechanisms |
|---|---|
| MySQL (3306), MariaDB (3307) | `caching_sha2_password` (default), `mysql_native_password` when enabled, `mysql_clear_password` over TLS only |
| HTTP (8080, 8443) | `Authorization: Basic`, `Bearer <api-key>`, `Bearer <sprig token>`; the browser login at `POST /v1/auth/login` |
| MongoDB (27017) | SCRAM-SHA-256 |
| Redis (6379) | `AUTH user password` |
| Bolt (7687) | The `basic` scheme |
| Kafka (9092) | SASL SCRAM-SHA-512, SCRAM-SHA-256, and `PLAIN` over TLS only |
| AMQP (5672, 5671) | SASL `PLAIN` and `AMQPLAIN`; `EXTERNAL` (a client-certificate login) for accounts that require a certificate, on connections that presented a verified one |

### caching_sha2_password needs TLS for a full login

`caching_sha2_password` has two paths. The fast path checks a scramble against a cache of recent successful logins and needs no TLS. The full path, used for the first login of an account on a gateway, sends the password and needs TLS: SprigDB doesn't support the RSA key exchange that MySQL offers on plain connections. A full login without TLS fails with error 1045 and the hint "caching_sha2_password needs TLS for a full authentication (RSA key exchange is not supported): connect with TLS". The MySQL listeners require TLS by default, so this only matters when you turn TLS off; see [TLS](tls.md).

### Unknown accounts

A login for an account that doesn't exist takes exactly the same packets and the same hashing work as a real one that misses the fast cache, then fails with 1045. An attacker can't tell which accounts exist from timing or replies.

## Password policies

Set policies for the whole tenant with settings, or per account with clauses on `CREATE USER` and `ALTER USER`. An account clause wins over the setting.

| Rule | Setting (default) | Account clause |
|---|---|---|
| Strength | `auth.password_policy` (`off`) | none |
| History: may not repeat the last n passwords | `auth.password_history` (0) | `PASSWORD HISTORY n \| DEFAULT` |
| Reuse interval: may not repeat a password used in the last n days | `auth.password_reuse_interval_days` (0) | `PASSWORD REUSE INTERVAL n DAY \| DEFAULT` |
| Lifetime | `auth.default_password_lifetime_days` (0, never expires) | `PASSWORD EXPIRE INTERVAL n DAY \| NEVER \| DEFAULT` |
| Change must name the current password | `auth.password_require_current` (false) | `PASSWORD REQUIRE CURRENT [OPTIONAL \| DEFAULT]` |
| Expire now | none | `PASSWORD EXPIRE` |

Strength levels follow MySQL's `validate_password`:

| Level | A new password must |
|---|---|
| `off` | Nothing |
| `low` | Have at least 8 characters, and not be the user name or the user name reversed |
| `medium` | `low`, plus a digit, a lower-case letter, an upper-case letter and a special character |
| `strong` | `medium`, plus not contain the user name (when it has 4 or more characters) |

```sql test
SPRIG SET (auth.password_policy = 'medium', auth.password_history = 5, auth.default_password_lifetime_days = 180);
SHOW SPRIG SETTINGS LIKE 'auth.password%';
SPRIG RESET (auth.password_policy, auth.password_history, auth.default_password_lifetime_days);
```

Tenant-scope settings need `SPRIG_SETTINGS_ADMIN`. An account clause must also lie within the bounds the tenant's ancestors set, else `ER_SPRIG_SETTING_OUT_OF_BOUNDS` (39080).

A weak password fails with error 1819. A reused password fails with 3638 on 3306 and 1819 on 3307. A missing or wrong `REPLACE` fails with 3892 or 3891.

### Expired passwords

When a password expires (by `PASSWORD EXPIRE` or its lifetime), a client that declares it can handle expired passwords logs in to **sandbox mode**, where it may only change its password (`ALTER USER ... IDENTIFIED BY`, `SET PASSWORD`) and run `SET`. Other clients get error 1862.

## Lockout and login rate limits

### Temporary lockout

After a number of wrong passwords in a row, an account is locked for a while, as MySQL's `FAILED_LOGIN_ATTEMPTS` and `PASSWORD_LOCK_TIME` do. Tracking is on when both are non-zero:

| Setting | Default | Account clause |
|---|---|---|
| `auth.failed_login_attempts` | 0 (off) | `FAILED_LOGIN_ATTEMPTS n` |
| `auth.password_lock_minutes` | 0 (off); -1 locks until unlocked | `PASSWORD_LOCK_TIME n \| UNBOUNDED` |

```sql
ALTER USER 'report'@'%' FAILED_LOGIN_ATTEMPTS 5 PASSWORD_LOCK_TIME 15;
```

Only wrong passwords for an existing account count. A locked account fails with error 3955 on 3306 and 4150 on 3307, even with the right password. The lock ends by itself when its time passes, or at `ALTER USER ... ACCOUNT UNLOCK`. Unlike MySQL, the count and the lock are kept in the tenant's catalog, so they survive restarts and apply on every gateway at once.

`ACCOUNT LOCK` is a different, permanent lock that an administrator sets; a login to such an account fails with 3118 on 3306 and 4151 on 3307.

### Rate limits

Two limits slow down password guessing on each gateway:

| Scope | Rule | Settings |
|---|---|---|
| Source address | More than `auth.ip_failures_per_minute` (30) failed logins in 60 seconds: the address is refused for `auth.ip_block_seconds` (300) with `ER_SPRIG_LOGIN_RATE_LIMITED` (39300) before the greeting; HTTP gets 429 with `Retry-After` | data centre or global |
| Account and address | After `auth.delay_after_failures` (3) failures in a row, each further failure is delayed by `auth.failure_delay_ms` (1000), doubling up to `auth.failure_delay_max_ms` (10000) | tenant, data centre or global |

Delays are keyed by account and address together, so an attacker can't slow down a real user's logins from somewhere else, and unknown accounts are delayed exactly like real ones. These limiters live in each gateway's memory; an attacker spreading guesses over many gateways still meets the account lockout, which is shared.

Password hashing runs on a bounded pool of CPU slots. A login that can't get a slot within 2 seconds fails with `ER_SPRIG_AUTH_UNAVAILABLE` (39301), as does a login whose tenant catalog can't be read.

## Requiring TLS

`auth.require_tls = true` refuses every login of the tenant that didn't arrive over TLS. Per account:

| Clause | The login needs |
|---|---|
| `REQUIRE SSL` | TLS |
| `REQUIRE X509` | TLS with a client certificate the listener verified |
| `REQUIRE SUBJECT '...'`, `REQUIRE ISSUER '...'` | A verified client certificate whose subject (and issuer) is exactly the name given, such as `/C=GB/O=Acme/CN=billing` |

The certificate is checked in addition to the password, not instead of it; the one exception is AMQP's SASL `EXTERNAL`, which logs an account with a certificate requirement in by its certificate alone (see [AMQP](../connect/amqp.md#log-in-with-a-client-certificate-external)). A refused login gets error 1045 on MySQL, or the protocol's own wrong-password answer elsewhere.

```sql
ALTER USER 'billing'@'%' REQUIRE SUBJECT '/C=GB/O=Acme/CN=billing' AND ISSUER '/C=GB/O=Acme/CN=Acme client CA';
```

Certificate requirements need a listener that asks for client certificates (`listener.<name>.client_cert`). Every protocol checks them: MySQL, MariaDB, MongoDB, Redis, Kafka, Bolt, AMQP, and HTTP Basic, API keys and the password login of `POST /v1/auth/login`. A login handed to another data centre for the tenant's [residency](residency.md) carries the certificate's subject and issuer, so it is checked there too. See [Client certificates](tls.md#client-certificates).

To see what an account requires, run `SHOW CREATE USER`, which prints the `REQUIRE` clause (never the password hash):

```sql
SHOW CREATE USER 'billing'@'%';
```

See [Tenants and accounts](../sql/tenants-and-accounts.md#show-create-user) for its output on each port.

## Sessions after account changes

Every account change commits with a new version of the tenant's credentials, which every gateway follows within moments. What happens to open sessions:

| Change | Open sessions |
|---|---|
| `DROP USER` | Closed at their next safe point |
| `ACCOUNT LOCK` | Closed |
| Password login removed from the account | Closed |
| Temporary lock after failed logins | Kept: an attacker's failures shouldn't end a real user's sessions |
| Password changed | Kept, as in MySQL |
| `PASSWORD EXPIRE` | Kept, as in MySQL |

A closed session's next statement gets `ER_SPRIG_SESSION_REVOKED` (39306). Set `auth.close_sessions_on_revoke = false` to keep sessions open until they end by themselves. Token sessions behave the same way, and a token session also ends when the account's password changes; see [API keys and tokens](api-keys-and-tokens.md).

## Second factors for HTTP logins

Users who log in over HTTP, in Studio or with `POST /v1/auth/login`, can add a TOTP second factor (any authenticator app). Second factors don't apply to MySQL, MongoDB or other wire-protocol logins, or to API keys.

| Setting | Default | Meaning |
|---|---|---|
| `auth.mfa_required` | `off` | `off`, `admins` (users with `SPRIG_TENANT_ADMIN`) or `all` |
| `auth.mfa_challenge_seconds` | 300 | Life of a login's second-factor challenge, and of an enrolment ticket |

### Enrolling

1. Sign in, then call `POST /v1/auth/factors:enrol` (in Studio: **Access control**, **Security**). The reply shows a new TOTP seed once, as base32 and as an `otpauth://` URI for a QR code.
2. Add it to your authenticator app and confirm with a current code: `POST /v1/auth/factors:confirm` with `{"code": "123456"}`.
3. The factor becomes active and 10 recovery codes are shown, once. Store them safely.

When `auth.mfa_required` covers a user who has no factor yet, the password login returns an enrolment ticket instead of a session, and confirming the factor with that ticket also signs the user in.

### Logging in with a factor

`POST /v1/auth/login` with the password returns `{challenge, methods, expires_in}` and no tokens. Complete it with `POST /v1/auth/mfa` and `{"challenge": "...", "code": "123456"}`, using a current TOTP code or a recovery code. A TOTP code is 6 digits, valid for 30 seconds with one step of clock drift either way, and works only once. A recovery code works once and is then deleted.

### Losing a factor

`POST /v1/auth/factors:reset` removes every factor of an account and ends all its sessions. A user resets their own with a current code or a recovery code; an administrator (`SPRIG_TENANT_ADMIN` or `CREATE USER`) resets another user's with `{"user": "report", "host": "%"}`. `GET /v1/auth/factors` shows your factors, how many recovery codes are left, and whether a factor is required.

TOTP seeds are encrypted with a per-tenant key derived from the gateway's key file (`[gateway] workload_key_file`). Without that file, second factors are unavailable.

Operators have their own second factor, required by default; see [Tenant administration](tenant-administration.md).

## Errors at login

| Error | Meaning |
|---|---|
| 1045 | Wrong user, host, password or key; or TLS required and missing |
| 1862 | Password expired, and the client can't use sandbox mode |
| 3118 (3306), 4151 (3307) | The account is locked (`ACCOUNT LOCK`) |
| 3955 (3306), 4150 (3307) | Temporarily locked after failed logins |
| 3159 | `mysql_clear_password` without TLS |
| 39300 `ER_SPRIG_LOGIN_RATE_LIMITED` | Too many failures from this address |
| 39301 `ER_SPRIG_AUTH_UNAVAILABLE` | Credentials can't be checked right now; retry |
| 39302 `ER_SPRIG_REMOTE_ACCESS_DENIED` | The tenant can't be reached from this data centre (see [Residency](residency.md)) |
| 39303 `ER_SPRIG_TENANT_SUSPENDED` | The tenant or an ancestor is suspended; reported only after the password checked out |
| 39306 `ER_SPRIG_SESSION_REVOKED` | The session was closed by an account change |

## Settings

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `auth.caching_sha2_rounds` | int | 5000 | SHA-256 crypt rounds of new `caching_sha2_password` verifiers |
| `auth.mysql_native_password` | bool | false | Store and accept `mysql_native_password` verifiers |
| `auth.scram_sha256_iterations` | int | 15000 | PBKDF2 iterations of new SCRAM-SHA-256 verifiers |
| `auth.scram_sha512_iterations` | int | 15000 | PBKDF2 iterations of new SCRAM-SHA-512 verifiers |
| `auth.backfill_verifiers` | bool | true | Add missing SCRAM verifiers at logins that see the clear password |
| `auth.password_policy` | text | `off` | `off`, `low`, `medium` or `strong` |
| `auth.password_history` | int | 0 | Earlier passwords a new one may not repeat |
| `auth.password_reuse_interval_days` | int | 0 | Days within which a password may not be reused |
| `auth.default_password_lifetime_days` | int | 0 | Password lifetime; 0 never expires |
| `auth.password_require_current` | bool | false | Password changes must name the current password |
| `auth.failed_login_attempts` | int | 0 | Failures in a row before a temporary lock; 0 off |
| `auth.password_lock_minutes` | int | 0 | Temporary lock length; -1 until unlocked; 0 off |
| `auth.delay_after_failures` | int | 3 | Failures before delays start; 0 never |
| `auth.failure_delay_ms` | int | 1000 | First delay, doubling per failure |
| `auth.failure_delay_max_ms` | int | 10000 | Longest delay |
| `auth.ip_failures_per_minute` | int | 30 | Failures per address per minute before it is refused; 0 never |
| `auth.ip_block_seconds` | int | 300 | How long a refused address waits |
| `auth.require_tls` | bool | false | Refuse logins without TLS |
| `auth.close_sessions_on_revoke` | bool | true | Close sessions of dropped or locked users |
| `auth.mfa_required` | text | `off` | `off`, `admins` or `all`, for HTTP logins |
| `auth.mfa_challenge_seconds` | int | 300 | Life of a second-factor challenge |

All accept the tenant, data-centre and global scopes, except `auth.ip_failures_per_minute` and `auth.ip_block_seconds` (data centre and global).

## Not supported yet

- WebAuthn and passkeys as a second factor (TOTP only).
- SCRAM-SHA-1 for MongoDB clients (SCRAM-SHA-256 only).
- The RSA key exchange of `caching_sha2_password` on connections without TLS.
- MySQL resource limits on accounts (`MAX_QUERIES_PER_HOUR` and the like).

## Related

- [Grants and roles](grants-and-roles.md)
- [API keys and tokens](api-keys-and-tokens.md)
- [TLS](tls.md)
- [Audit](audit.md)
- [Tenants and accounts in SQL](../sql/tenants-and-accounts.md)
