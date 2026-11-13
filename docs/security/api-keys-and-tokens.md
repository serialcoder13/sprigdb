---
title: API keys and tokens
audience: [user, dba, operator]
sidebar:
  order: 6
---

# API keys and tokens

Programs that call SprigDB over HTTP (REST and gRPC on 8080 and 8443) authenticate with an API key or a SprigDB token instead of sending a password with every request. This page explains how to create, use and revoke API keys, how token sessions and refresh work, the lifetimes involved, how SprigDB signs its tokens, and the tokens operators issue for automation.

| Credential | Looks like | For | Lifetime |
|---|---|---|---|
| API key | `sprig_acme_<id>_<secret>` | Servers, scripts and integrations calling the HTTP API | Until it expires or is revoked |
| Access token | `sprigat_<signed token>` | Browser sessions (Studio) and API clients that log in with a password | `auth.token_ttl_seconds` (15 minutes) |
| Refresh token | `acme.<session>.<random>` | Getting new access tokens for a session | The session's idle and maximum lifetimes |
| Operator token | `sprigop_<id>_<secret>`, `sprigsvc_<id>_<secret>` | Platform operators and their automation, on the Admin API | Up to `operators.token_max_days` (90 days) |

All of them are sent the same way:

```text
Authorization: Bearer <api-key or access token>
```

API keys and tokens work on the HTTP listeners only. MySQL, MongoDB, Redis, Bolt, Kafka and AMQP clients log in with a password; see [Accounts and logins](accounts-and-logins.md).

## API keys

An API key belongs to one user and acts with that user's privileges. Its format is `sprig_<tenant>_<key id>_<secret>`: the tenant name (a visible prefix, so secret scanners can find leaked keys), a 12-character key id, and a 256-bit secret as 64 hex digits. SprigDB stores only the SHA-256 of the secret, so a key is shown exactly once, when it is created.

### Create a key

```sh
curl -s https://gw.example.com:8443/v1/api-keys \
  -H 'Authorization: Bearer <access-token>' -H 'Content-Type: application/json' \
  -d '{"name": "nightly-export", "expires_days": 90, "allowed_cidrs": ["10.20.0.0/16"]}'
```

```json
{ "key_id": "k3j8q2m5x7wa", "key": "sprig_acme_k3j8q2m5x7wa_<secret>", "name": "nightly-export" }
```

| Field | Default | Meaning |
|---|---|---|
| `name` | required | A name you choose, to recognise the key |
| `user`, `host` | the caller, `%` | The account the key acts as; another user's key needs `CREATE USER` or `SPRIG_TENANT_ADMIN` |
| `expires_days` | none (never expires) | Days until the key stops working, at most 3650 |
| `allowed_cidrs` | none (any address) | Client addresses allowed to use the key |

Creating a key also enables the API-key login method on the account. An impersonation session can't create keys.

In Studio, open **Access control**, **Users and access**, and use the API keys section; the new key is shown once with a button to copy it.

### Use a key

```sh
curl -s https://gw.example.com:8443/v1/databases/shop/sql:query \
  -H 'Authorization: Bearer sprig_acme_k3j8q2m5x7wa_<secret>' \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT id, status FROM orders WHERE customer_id = 42"}'
```

Each request checks, in constant time, that the secret matches, that the key isn't revoked or expired, that the client's address is within `allowed_cidrs` when set, and that the account is still usable: not dropped, not locked (permanently or after failed logins), TLS present if the account requires it, and the tenant not suspended. The tenant comes from the key's prefix, so no tenant header is needed.

A refused key gets HTTP 401 with the MySQL error in the body (1045 for a bad, revoked or expired key). Logins with keys go through the same rate limits and audit as password logins.

### List and revoke keys

```sh
curl -s https://gw.example.com:8443/v1/api-keys -H 'Authorization: Bearer <access-token>'
curl -s -X POST https://gw.example.com:8443/v1/api-keys/k3j8q2m5x7wa:revoke \
  -H 'Authorization: Bearer <access-token>'
```

The list shows each key's id, name, user, allowed addresses, expiry, revocation and creation time, never its secret. Users see their own keys; holders of `CREATE USER` or `SPRIG_TENANT_ADMIN` see and revoke everyone's.

Revoking a key is an account change, so every gateway refuses it within moments. Key creation and revocation are audited (`auth.api_key_created`, `auth.api_key_revoked`).

Rotate a key by creating a new one, deploying it, then revoking the old one.

## Token sessions

A program or a browser can log in with a password and get a short-lived access token and a refresh token. This is how Studio signs in.

### Log in

```sh
curl -s https://gw.example.com:8443/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"login": "acme.app", "password": "<password>", "refresh_transport": "body", "client": "api"}'
```

```json
{
  "access_token": "sprigat_eyJhbGciOiJFZERTQSIs...",
  "token_type": "Bearer",
  "expires_in": 900,
  "session_id": "...",
  "refresh_token": "acme...."
}
```

| Field | Meaning |
|---|---|
| `login` | `tenant.user` |
| `password` | The account's password |
| `refresh_transport` | `body` returns the refresh token in the reply; anything else (the default) sets it as a cookie, for browsers |
| `client` | `api` for programs; Studio's own logins are `studio` |

If the account has a second factor, or `auth.mfa_required` covers it, the reply is a challenge or an enrolment ticket instead of tokens; complete it with `POST /v1/auth/mfa`. See [Accounts and logins](accounts-and-logins.md#second-factors-for-http-logins).

Password logins over plain HTTP from another host are refused with `ER_SPRIG_STUDIO_INSECURE_TRANSPORT` (39921) unless `studio.allow_insecure_http` is on: use 8443.

### Refresh and log out

```sh
curl -s https://gw.example.com:8443/v1/auth/refresh \
  -H 'Content-Type: application/json' -d '{"refresh_token": "<refresh-token>"}'

curl -s -X POST https://gw.example.com:8443/v1/auth/logout \
  -H 'Authorization: Bearer <access-token>'
```

`POST /v1/auth/refresh` returns a new access token and a new refresh token; the old refresh token stops working. If an old refresh token is ever presented again, SprigDB treats it as stolen and revokes the whole session, on every gateway. Keep only the newest refresh token.

`GET /v1/auth/me` returns the principal, the session, the tenant tree, the caller's grants and the `studio.*` settings that apply.

### Browser sessions and cookies

With the cookie transport, the refresh token is an `HttpOnly`, `SameSite=Strict` cookie scoped to `/v1/auth` (`__Secure-sprig-refresh` over HTTPS, `sprig-refresh` on a loopback HTTP page). JavaScript can't read it. Cookie-based login, refresh and logout require:

- the `X-Sprig-CSRF` header on the request (any value; a cross-site page can't send it), else `ER_SPRIG_STUDIO_CSRF_HEADER_MISSING` (39923);
- the same origin, or one listed in `studio.public_origins`, else `ER_SPRIG_STUDIO_ORIGIN_REJECTED` (39922).

### Lifetimes

| Setting | Default | Meaning |
|---|---|---|
| `auth.token_ttl_seconds` | 900 | Access-token lifetime (60 to 86400) |
| `studio.session_idle_minutes` | 30 | A session not refreshed for this long ends (`ER_SPRIG_STUDIO_SESSION_IDLE`, 39924) |
| `studio.session_max_hours` | 12 | Longest session, whatever its activity (`ER_SPRIG_STUDIO_SESSION_EXPIRED`, 39925) |
| `auth.refresh_ttl_hours` | 168 | Upper limit on a session's lifetime; the shorter of this and `studio.session_max_hours` applies |

All accept the tenant, data-centre and global scopes. A session also ends when:

- the user logs out;
- the account is dropped or locked, or its password changes;
- an administrator resets the user's second factors;
- the refresh token is reused.

Ending a session is an account change: every gateway refuses its access tokens within moments, without waiting for them to expire.

## How SprigDB signs tokens

An access token is a compact JWS signed with Ed25519 (`EdDSA`), prefixed with `sprigat_`. Its claims name the issuer, audience, user, tenant, session, authentication methods (`amr`: `pwd`, plus `otp` or `rec` after a second factor), client, issue time and expiry; an impersonation token also carries the administrator in `act`.

- **Keys.** Each gateway makes its own Ed25519 key pair in memory when it starts and publishes only the public key to the control plane. Private keys never leave the process that made them.
- **Verification.** Any gateway in any data centre verifies any token: it looks up the signing key by the token's `kid`, checks the signature, audience and expiry, then checks that the session is live and the account still usable. Only `EdDSA` is accepted; a token naming any other algorithm (such as `none` or `HS256`) is refused before any key is looked up.
- **Rotation.** Each gateway makes a new key pair every `auth.token_key_rotation_days` (30), publishing it before signing with it. Old public keys stay published long enough to verify every token they signed. A gateway restart makes a new pair; tokens signed by the old one keep working, so restarts sign nobody out.
- **Revoking a key.** Deleting a key's record from the control plane makes every token it signed fail within 10 minutes.

Studio's export tickets (`POST /v1/exports:ticket`, valid `studio.export_ticket_seconds`, 60) and second-factor challenge tickets are signed the same way.

Tokens from external identity providers (OIDC and other JWT issuers) aren't accepted yet.

## Operator tokens

Platform operators log in with `sprigctl login` (password and TOTP) and get a login session token, valid `operators.session_hours` (12). From that session they issue scoped tokens for automation:

```sh
sprigctl token create --scope clusters:read --scope recommendations:write --days 30 --label "dashboards"
sprigctl token create --service ci-deploy --kind ci --scope settings:write --days 90
sprigctl token list
sprigctl token revoke <key_id>
```

- People's tokens look like `sprigop_<key id>_<secret>`; service identities, which only platform operators issue, look like `sprigsvc_<key id>_<secret>`. Only the SHA-256 of the secret is stored.
- A scope is `<area>:read` or `<area>:write`, where the area is the first part of the Admin API path (`clusters`, `settings`, `recommendations`, ...) and `*` means every area. `write` includes `read`.
- `--dcs` narrows a token to some data centres.
- A token expires within `operators.token_max_days` (90); a service identity may be issued with `--never-expires`.
- Issuing and revoking tokens are audited (`operator.token_issued`, `operator.service_identity_issued`, `operator.token_revoked`).

An expired operator token fails with `ER_SPRIG_OPERATOR_TOKEN_EXPIRED` (39316). See [Tenant administration](tenant-administration.md#operators-and-tenant-administrators).

## Settings

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `auth.token_ttl_seconds` | int | 900 | tenant, dc, global | Access-token lifetime |
| `auth.refresh_ttl_hours` | int | 168 | tenant, dc, global | Upper limit on a token session |
| `auth.token_key_rotation_days` | int | 30 | dc, global (operator only) | Signing-key rotation |
| `studio.session_idle_minutes` | int | 30 | tenant, dc, global | Idle time before a session ends |
| `studio.session_max_hours` | int | 12 | tenant, dc, global | Longest session |
| `studio.allow_insecure_http` | bool | false | dc, global | Allow password logins on plain HTTP from other hosts |
| `studio.public_origins` | text | empty | dc, global | Extra origins for the origin check |
| `operators.session_hours` | int | 12 | global (operator only) | Operator login session |
| `operators.token_max_days` | int | 90 | global (operator only) | Longest operator token |

## Not supported yet

- Scopes on API keys narrower than the user's own privileges, and creating keys with SQL statements.
- API keys and tokens on the MySQL, MongoDB, Redis, Bolt, Kafka and AMQP listeners.
- External JWT and OIDC tokens.

## Related

- [Accounts and logins](accounts-and-logins.md)
- [Tenant administration](tenant-administration.md)
- [REST and gRPC](../connect/rest-and-grpc.md)
- [REST API reference](../reference/api.md)
- [Audit](audit.md)
