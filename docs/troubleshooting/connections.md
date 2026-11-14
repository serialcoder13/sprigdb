---
title: Connection problems
audience: [user, dba, operator]
sidebar:
  order: 2
---

# Connection problems

This page covers clients that can't connect or can't log in. Work through it in the order a connection does: the TCP connection, then TLS, then the login name, then the credentials, then the tenant and the limits.

A rule to keep in mind: SprigDB doesn't tell a stranger whether a tenant or user exists. A login that names no tenant, a tenant that can't take logins, an unknown user and a wrong password all get the same "Access denied" (1045), after the same amount of work. The detail is in the gateway's log and metrics, not in the client's error.

## Quick checks

| Check | How |
|---|---|
| Is the port open? | `nc -vz gw1.example.internal 3306` |
| Is the gateway ready? | `curl -s http://gw1.example.internal:9180/readyz?verbose` |
| Which listeners are on, and where? | `sprigctl settings get listener.mysql.enabled`, `... listener.mysql.bind`, `... listener.mysql.tls` |
| Are logins failing, and why? | `sprig_auth_logins_total` by `outcome`, `mechanism` and `protocol` |
| Are connections refused before login? | `sprig_gateway_connections_rejected_total` by `listener` and `reason` |
| Is TLS failing? | `sprig_gateway_tls_handshakes_total` by `listener`, `outcome` and `version` |

## TLS errors

Every listener has a TLS mode, `listener.<name>.tls`: `required`, `optional` or `off`. The defaults are `required` for 3306, 3307, 8443, 27017, 6379, 7687, 9092 and 5671, and `off` for 8080 and 5672.

### "Connections using insecure transport are prohibited" (3159)

**Cause.** The client connected without TLS to a listener whose mode is `required`. The gateway refuses before it reads the user name. MySQL clients see 3159 with SQLSTATE `HY000`; MariaDB clients on 3307 see 3159 with `08004`. The same error is sent when a client offers `mysql_clear_password` without TLS.

**Confirm.** `sprig_mysql_plaintext_refused_total{reason="listener_required"}` rises, and `sprigctl settings get listener.mysql.tls` shows `required`.

**Fix.** Turn TLS on in the client. For the `mysql` client:

```sh
mysql -h gw1.example.internal -P 3306 -u acme.app -p --ssl-mode=REQUIRED
```

In drivers, set the equivalent option (`sslMode=REQUIRED` in JDBC, `tls=true` in Go's driver, `ssl: {}` in Node's `mysql2`). Change the listener to `optional` only on a trusted network.

### "Bad handshake" when the client asks for TLS

**Cause.** The client sent a TLS request to a listener whose mode is `off`.

**Confirm.** `sprig_mysql_plaintext_refused_total{reason="listener_off"}` rises.

**Fix.** Either connect without TLS (`--ssl-mode=DISABLED`) or set the listener's mode to `optional` or `required`.

### The TLS handshake fails

**Cause.** One of these:

- The client doesn't trust the gateway's certificate, or the host name doesn't match it.
- The client offers only TLS 1.0 or 1.1. The gateway accepts TLS 1.2 and later, or 1.3 only when `tls.min_version` is `1.3`.
- The certificate expired.
- The client took longer than `gateway.handshake_timeout_ms` (10 seconds) to finish the PROXY header, TLS and login.

**Confirm.** `sprig_gateway_tls_handshakes_total` by `outcome` shows the failures, and `version` shows what clients negotiate. `sprig_gateway_cert_expiry_seconds` shows how long each certificate bundle has left. Test from the client host:

```sh
openssl s_client -connect gw1.example.internal:3306 -starttls mysql -servername gw1.example.internal
```

**Fix.** Give clients the CA that signed the gateway's certificate, and connect by a name the certificate covers. The default bundle is read from `tls.bundles.default.cert_path` and `tls.bundles.default.key_path`; the gateway watches both files, so replacing them takes effect without a restart. See [TLS](../security/tls.md).

### The handshake fails only with a client certificate, or only without one

**Cause.** The listener asks for client certificates (`listener.<name>.client_cert`):

- With `required`, a client that sends no certificate fails the TLS handshake.
- With `optional` or `required`, a certificate that doesn't chain to a CA in `tls.bundles.default.client_ca_path` fails the handshake.

**Confirm.** `sprigctl settings get listener.mysql.client_cert` and `... tls.bundles.default.client_ca_path`. `sprig_gateway_tls_handshakes_total{outcome=...}` counts the failures. Check the client's certificate against the CA with `openssl verify -CAfile client-ca.pem client.pem`.

**Fix.** Issue the client a certificate from the configured CA, or use `optional` so clients without one still connect. Gateways reload the CA file within a few seconds of it changing, with no restart; if a changed file doesn't load, the gateway log shows `gateway.client_ca_rejected` and the old CAs stay in use. While you rotate a CA, keep both the old and the new one in the file.

### "Access denied" only when connecting without TLS

**Cause.** The account requires TLS. Either `auth.require_tls` is on for the tenant, or the user was created with `REQUIRE SSL`. SprigDB checks this after the password and answers 1045.

**Fix.** Connect with TLS. To check the tenant's setting: `SHOW SPRIG SETTINGS LIKE 'auth.require_tls';`.

### "Access denied" for an account that requires a client certificate

**Cause.** The account was created with `REQUIRE X509`, `REQUIRE SUBJECT` or `REQUIRE ISSUER`, and the login didn't meet it. Any of these:

- The client sent no certificate, or the listener doesn't ask for one (`listener.<name>.client_cert` is `off`, the default).
- The certificate's subject or issuer isn't exactly the name the account requires. Names are compared as text in the form `/C=GB/O=Acme/CN=billing`, in certificate order.
- The certificate was signed by a CA the gateway no longer trusts, after the CA file changed.
- The login was handed to another data centre for the tenant's residency by a gateway of an earlier release, which carries no subject or issuer.

Every protocol checks the requirement: MySQL and MariaDB answer 1045, MongoDB 18 `AuthenticationFailed`, Redis `WRONGPASS`, Kafka `SASL_AUTHENTICATION_FAILED`, Bolt `Neo.ClientError.Security.Unauthorized`, AMQP 403 `ACCESS_REFUSED` and HTTP 401. The audit event's `reason` is `tls_required`. The same applies to an AMQP `EXTERNAL` login, which is also refused for an account with no certificate requirement.

**Confirm.** `SHOW CREATE USER billing;` shows the account's `REQUIRE` clause.

**Fix.** Compare the account's requirement with `openssl x509 -in client.pem -noout -subject -issuer -nameopt compat`, set the listener's `client_cert` to `optional`, and send the certificate with the login. See [Client certificates](../security/tls.md#client-certificates).

## caching_sha2_password fails on the first login

**Symptom.** A client that doesn't use TLS gets "Access denied", although the password is right:

```text
ERROR 1045 (28000): Access denied for user 'app'@'10.0.4.17' (using password: YES);
caching_sha2_password needs TLS for a full authentication (RSA key exchange is not supported): connect with TLS
```

The same client may log in fine at other times, or through another gateway.

**Cause.** `caching_sha2_password`, the default plugin on both personas, has two paths:

- The **fast path** checks a scramble against a cache in the gateway. It works without TLS, but only for an account that has logged in through that gateway before.
- The **full path** sends the password itself. Without TLS, MySQL would protect it with the server's RSA key; SprigDB doesn't support the RSA exchange, so it refuses the full path without TLS.

The first login of an account on each gateway, and the first after the gateway restarts or the password changes, takes the full path.

**Confirm.** The error carries the hint above. `sprig_auth_fast_cache_total` by `result` shows misses on that gateway.

**Fix.** Use one of these, in order of preference:

1. Connect with TLS. This also protects every later statement.
2. If a client can't use TLS and runs only on a trusted network, let the tenant use `mysql_native_password`, which needs no full path:

   ```sql
   ALTER TENANT SPRIG SET (auth.mysql_native_password = ON);
   ```

   Then create or reset the user's password so a native verifier is stored, and configure the client to use `mysql_native_password`. With `sprigctl`, `sprigctl tenant user add acme app --password '<password>' --native` stores both verifiers.

Don't configure the client with `get-server-public-key` or an RSA key file; SprigDB refuses that exchange too.

## Wrong login name format

**Symptom.** "Access denied" (1045) with a password you know is right, from the first attempt.

**Cause.** The login name must name the tenant. SprigDB splits the name at the **first** dot: `acme.app` is user `app` of tenant `acme`. Common mistakes:

| Login name | What SprigDB reads | Result |
|---|---|---|
| `app` | No tenant | Denied |
| `ACME.app` | Tenant `ACME`, which isn't a valid name (tenant names are lower case) | Denied |
| `app@acme` | No dot: no tenant | Denied |
| `acme.app.reports` | Tenant `acme`, user `app.reports` | Works if that user exists |
| `acme.` | Tenant `acme`, empty user | Denied |

Other rules:

- A tenant name is 2 to 63 characters: a lower-case letter, then lower-case letters, digits and hyphens, not ending with a hyphen.
- The user part is limited to 32 characters on the MySQL persona (3306) and 80 on the MariaDB persona (3307). The whole login name is limited to 255.
- The user part is case-sensitive, as in MySQL.
- A tenant can take logins while it is `active`, `migrating` or `failing_over`. While it is `creating`, `fenced` or `deleting`, its logins are denied like any unknown name.
- Connection attributes never select a tenant.

On a tenant endpoint (a host name under `listener.tenant_sni_suffixes`, such as `acme.db.example.com`), the tenant comes from the host name instead, and the whole login name is the user name: log in as `app`, not `acme.app`. See [DNS and tenant endpoints](#dns-and-tenant-endpoints).

Other protocols use the same form: MongoDB's `username`, Redis `AUTH acme.app <password>`, and HTTP Basic authentication. Over HTTP you can instead send the tenant in the `x-sprig-tenant` header with the plain user name. API keys (`sprig_<tenant>_...`) carry their tenant. On the Redis and AMQP listeners, `listener.redis.tenant` and `listener.amqp.tenant` name a tenant for plain user names.

**Confirm.** `sprigctl tenant show acme` shows that the tenant exists and its state. `sprig_auth_logins_total{outcome=...}` counts denials, and the gateway log records the reason for each refused login.

**Fix.** Use `<tenant>.<user>`, all of it in the user-name field of the client or connection string:

```text
mysql://acme.app:<password>@gw1.example.internal:3306/shop
```

### Accounts that are locked or expired

These look similar but carry their own error numbers:

| Error | Cause | Fix |
|---|---|---|
| 3955 | Temporarily locked after `auth.failed_login_attempts` failures in a row (off by default) | Wait `auth.password_lock_minutes`, or have an administrator unlock the account |
| 3118 (4151 on 3307) | The account was locked with `ACCOUNT LOCK` | `ALTER USER ... ACCOUNT UNLOCK` as an administrator |
| 1862 | The password expired, and the client can't handle sandbox mode | Change the password from a client that can, or have an administrator reset it |

Logins also slow down after failures: from the `auth.delay_after_failures`th failure in a row (3 by default) for an account and address, each failure is delayed by `auth.failure_delay_ms` (1 second), doubling up to `auth.failure_delay_max_ms` (10 seconds). A client that looks hung during login may be waiting out this delay.

## Tenant suspended

**Symptom.** `ER_SPRIG_TENANT_SUSPENDED` (39303): `tenant acme is suspended`. Over HTTP it is a 403.

**Cause.** The tenant, or a tenant above it in the tree, is suspended. SprigDB checks this only after the password verified, so only someone with valid credentials learns it. Sessions already open when a tenant is suspended are closed at the next safe point with `ER_SPRIG_SESSION_REVOKED` (39306), when `auth.close_sessions_on_revoke` is on.

**Confirm.** `sprigctl tenant show acme` and `sprigctl tenant show` for each parent tenant.

**Fix.** Nothing on the client side helps. A platform operator lifts the suspension with `sprigctl tenant resume acme`; `sprigctl tenant show acme` shows the reason it was suspended. Until then, every login of the tenant and its children is refused, its triggers, sinks and scheduled runs wait, and no child tenants can be created under it.

## Address blocked after failed logins

**Symptom.** `ER_SPRIG_LOGIN_RATE_LIMITED` (39300), before the server greeting:

```text
ERROR 39300 (HY000): Host '203.0.113.7' is blocked for 287 s because of too many failed logins
```

Over HTTP it is a 429 with a `Retry-After` header.

**Cause.** More than `auth.ip_failures_per_minute` (30) logins failed from that address within a minute on this gateway, so the gateway refuses the address for `auth.ip_block_seconds` (300). The counter is per gateway and per address, across all protocols, and every login of every tenant from that address counts.

**Confirm.** `sprig_auth_rate_limited_total{scope="ip"}` rises. The gateway log names the address. Look for the client that is failing: a deployment with an old password, a health check that logs in with wrong credentials, or a password-guessing attack.

**Fix.**

1. Find and fix the client that fails. The block ends on its own after the time in the message.
2. If many clients reach the gateway through a load balancer or proxy, they all share its address, and one bad client blocks everyone. Have the load balancer send the PROXY protocol header so the gateway sees real client addresses:

   ```sh
   sprigctl settings set gateway.proxy_trusted_cidrs 10.0.0.0/24 --scope dc --dc fra1
   sprigctl settings set gateway.proxy_protocol optional --scope dc --dc fra1
   ```

   Only peers in `gateway.proxy_trusted_cidrs` (here, the load balancers) may send the header. Use `required` once every load balancer sends it.
3. Raise `auth.ip_failures_per_minute` only if legitimate traffic really fails that often; 0 turns the limit off.

## Connection limits

### "Too many connections" (1040)

**Cause.** The gateway already has `gateway.max_connections` (20,000) client connections. MySQL clients get 1040 in place of the greeting; plain HTTP gets a 503.

**Confirm.** `sprig_gateway_connections_rejected_total{reason="max_connections"}` and `sprig_gateway_connections` by `listener` and `state`.

**Fix.** Add gateways, or reduce idle connections in client pools. Raise `gateway.max_connections` only if the gateway has memory and file descriptors to spare.

### ER_SPRIG_TENANT_CONNECTION_LIMIT (39001)

**Cause.** The tenant already has `limits.tenant_connections` (2,000) connections on this gateway. Over HTTP, the same code means the tenant has `api.txn.max_open_per_tenant` open REST or gRPC transactions on this gateway.

**Confirm.** `sprig_gateway_connections_rejected_total{reason="tenant_limit"}`. The message names the limit.

**Fix.** Shrink the application's pool, or close leaked connections. The limit is per gateway, so spreading clients over more gateways also helps. An operator can raise `limits.tenant_connections` for the tenant.

### Other refusals at connect time

| Error | Cause | What to do |
|---|---|---|
| `ER_SPRIG_GATEWAY_DRAINING` (39000) | The gateway is shutting down or draining | Reconnect; the load balancer should send you elsewhere |
| `ER_SPRIG_ADMISSION_BACKOFF` (39531) | The data centre is returning from an outage and admits new connections in steps | Retry after the delay in the message (1 to 5 seconds); see [Failover problems](failover.md#fencing-errors) |
| `ER_SPRIG_AUTH_UNAVAILABLE` (39301) | The tenant's credentials can't be loaded, or password hashing is saturated | Retry; if it lasts, check the tenant's home cluster |
| `ER_SPRIG_REMOTE_ACCESS_DENIED` (39302) | The tenant's residency doesn't allow logins in this data centre | Connect to a gateway in an allowed data centre |
| `ER_SPRIG_CATALOG_UNAVAILABLE` (39070) | The tenant's catalog can't be reached | See the tenant's home cluster with `sprigctl tenant show` |

## Listener disabled or moved

**Symptom.** "Connection refused" on a port, or a session that was working closes with `ER_SPRIG_LISTENER_RETIRED` (39004).

**Cause.** Listeners are switched and placed by settings, and changes apply live:

| Setting | Meaning |
|---|---|
| `listener.<name>.enabled` | Whether the listener runs. MongoDB, Redis, Bolt, Kafka and AMQP are off by default |
| `listener.<name>.bind` | Address and port, such as `0.0.0.0:3306` |
| `listener.<name>.tls` | `required`, `optional` or `off` |
| `listener.drain_seconds` | Grace (600 seconds) before sessions of a retired or stricter listener are closed |

When a listener is disabled, moved or tightened, the gateway closes its existing sessions at a safe point between statements, after `listener.drain_seconds`, with 39004. New connections go to the new address.

**Confirm.**

```sh
sprigctl settings get listener.mongo.enabled
sprigctl settings get listener.mongo.bind
```

`sprig_gateway_listener_generation` increases each time a listener's configuration changes.

**Fix.** Enable the listener, or point the client at the address in `listener.<name>.bind`:

```sh
sprigctl settings set listener.mongo.enabled true --scope dc --dc fra1
```

See [Listeners and TLS](../security/tls.md) for every listener's name and default port.

## DNS and tenant endpoints

Clients reach gateways by a DNS name that your load balancer or DNS serves; SprigDB doesn't manage DNS. If the name doesn't resolve or points at the wrong gateways, fix it in your DNS. Use a name the gateway's certificate covers, or TLS verification fails.

With tenant endpoints, the host name names the tenant: with `listener.tenant_sni_suffixes` set to `db.example.com`, user `app` connecting over TLS to `acme.db.example.com` logs in to tenant `acme`. If such logins fail with 1045:

| Check | What to look for |
|---|---|
| The setting | `sprigctl settings get listener.tenant_sni_suffixes` lists the suffix the client uses |
| TLS | Tenant endpoints need TLS; a plain-text connection carries no server name, so `app` names no tenant |
| SNI | The client sends the host name as the TLS server name. Most clients do; some MySQL clients don't unless configured to |
| The name | Exactly one label before the suffix, and a valid tenant name: `x.acme.db.example.com` names no tenant |
| The login name | The plain user name. On a tenant endpoint `acme.app` is a user named `acme.app` in `acme`, and `globex.app` doesn't reach `globex` |
| DNS and certificate | `*.db.example.com` resolves to the gateways, and the gateway certificate covers it |

See [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name).

## Related

- [Accounts and logins](../security/accounts-and-logins.md)
- [TLS](../security/tls.md)
- [Tenant administration](../security/tenant-administration.md)
- [Connect overview](../connect/overview.md)
- [Reading SprigDB errors](reading-errors.md)
