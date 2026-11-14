---
title: TLS
audience: [operator]
sidebar:
  order: 5
---

# TLS

Every gateway listener has its own TLS mode and certificate, and every TLS setting changes live: new certificates, new modes and new ports take effect without restarting a gateway or dropping the connections that are still allowed. Internal traffic between SprigDB processes uses mutual TLS. This page covers the client listeners and their modes, certificates and rotation, client certificates, tenant endpoints by server name, the internal ports, and Studio's operator mode.

## Listeners and their TLS modes

| Listener | Port | On by default | TLS by default |
|---|---|---|---|
| `mysql` | 3306 | yes | `required` |
| `mariadb` | 3307 | yes | `required` |
| `http` | 8080 | yes | `off` |
| `https` | 8443 | yes | `required` |
| `mongo` | 27017 | no | `required` |
| `redis` | 6379 | no | `required` |
| `bolt` | 7687 | no | `required` |
| `kafka` | 9092 | no | `required` |
| `amqp` | 5672 | no | `off` |
| `amqps` | 5671 | no | `required` |

Each listener has five settings:

| Setting | Meaning |
|---|---|
| `listener.<name>.enabled` | Serve this listener |
| `listener.<name>.bind` | Address and port, such as `0.0.0.0:3306` |
| `listener.<name>.tls` | `required`, `optional` or `off` |
| `listener.<name>.cert` | The certificate bundle; only `default` exists today |
| `listener.<name>.client_cert` | Client certificates: `off` (the default), `optional` or `required`. See [Client certificates](#client-certificates) |

The modes:

| Mode | Behaviour |
|---|---|
| `required` | Clients must use TLS. MySQL-family clients that don't ask for it are refused at login; on other protocols a connection that doesn't start a TLS handshake is closed |
| `optional` | Clients may use TLS or plain text. MySQL-family listeners offer TLS in the protocol; on other protocols the gateway detects a TLS `ClientHello` from the first byte |
| `off` | Plain text only |

`tls.min_version` (`1.2` or `1.3`, default `1.2`) is the lowest TLS version every client listener accepts.

These settings take the data-centre and global scopes, and operators set them:

```sh
sprigctl settings set --scope global listener.mongo.enabled true --reason "enable MongoDB"
sprigctl settings set --scope dc:dc-a listener.redis.bind 10.0.4.12:6379
sprigctl settings set --scope global tls.min_version 1.3
```

### Plain text on public addresses

A listener with TLS `optional` or `off` bound to an address outside private ranges logs a warning (`gateway.plaintext_public`) and sets `sprig_gateway_listener_plaintext_public` to 1 for that listener. The `http` listener is plain text by default; bind it to a private address, or put it behind a TLS-terminating load balancer.

Logins can require TLS whatever the listener allows: `auth.require_tls` for a whole tenant, or `REQUIRE SSL` on an account. `caching_sha2_password` needs TLS for a first login on a gateway, and Kafka's `PLAIN` and MySQL's `mysql_clear_password` are refused without TLS. See [Accounts and logins](accounts-and-logins.md).

## Certificates

Listeners take their certificate from a **bundle**: a PEM certificate chain (leaf first) and its PEM private key, named by two settings:

| Setting | Meaning |
|---|---|
| `tls.bundles.default.cert_path` | PEM certificate chain of the default bundle, leaf first |
| `tls.bundles.default.key_path` | PEM private key of the default bundle |

Both are empty by default. A listener whose mode isn't `off` needs a certificate: until both paths are set, such a listener isn't started, and the gateway reports "TLS needs a certificate: set tls.bundles.default.cert_path and tls.bundles.default.key_path".

```sh
sprigctl settings set --scope dc:dc-a tls.bundles.default.cert_path /etc/sprigdb/tls/gateway.pem
sprigctl settings set --scope dc:dc-a tls.bundles.default.key_path /etc/sprigdb/tls/gateway-key.pem
```

The files must be readable by the gateway process on every gateway host. Use a certificate whose names cover every name clients connect to (the load balancer's name and any per-gateway names).

Only the `default` bundle can be configured today; naming any other bundle in `listener.<name>.cert` is rejected ("only the default bundle can be configured yet"). All listeners therefore share one certificate.

### Rotating certificates without a restart

Gateways poll the bundle's files and compare their contents. When a change is seen on two polls in a row, the new pair is loaded, validated (the key must match the certificate) and swapped in atomically:

- new handshakes use the new certificate;
- open connections keep the one they negotiated;
- a bad pair (unreadable, mismatched, no certificate) is rejected and the old one kept.

Because the contents are compared rather than modification times, this also works for Kubernetes Secrets mounted as volumes, which update by swapping a symbolic link. To rotate, replace the files in place (write the key first, then the certificate). Changing the *paths* is a settings change and loads a new bundle the same way.

`sprig_gateway_cert_expiry_seconds{bundle}` shows the time left until the loaded certificate expires; alert on it.

To prove a rotation causes no restart and no failed statement, run `sprigctl check no-restart` with `--rotate-key` and `--rotate-cert` against a gateway under load.

## Live listener changes

Every `listener.*` and `tls.*` change applies live:

- A changed bind address is bound first, and only then is the old socket closed. If the new address can't be bound, the old listener keeps running and the change is reported as not applied.
- New connections get the new settings; open connections keep the settings they were accepted with.
- A **stricter** change (TLS made `required`, the minimum version raised, the listener moved or disabled) closes the open connections the new settings no longer allow, after `listener.drain_seconds` (600): plain-text connections when TLS becomes required, TLS 1.2 connections when the minimum becomes 1.3, every connection when the listener moves or is disabled. They are closed at a safe point between statements first, and forcibly at twice the drain time. Such a session gets `ER_SPRIG_LISTENER_RETIRED` (39004), which is retryable: reconnect.
- A looser change closes nothing.

| Setting | Default | Meaning |
|---|---|---|
| `listener.drain_seconds` | 600 | Grace before sessions of a stricter or retired listener close |
| `gateway.handshake_timeout_ms` | 10000 | Longest connection phase: PROXY header, TLS handshake and login |
| `tls.min_version` | `1.2` | Lowest TLS version on client listeners |

### Behind a load balancer

If a TCP load balancer sits in front of the gateways, let it pass the client's address with the PROXY protocol (versions 1 and 2), so logins, rate limits and audit see the real client:

| Setting | Default | Meaning |
|---|---|---|
| `gateway.proxy_protocol` | `off` | `off`, `optional` or `required` |
| `gateway.proxy_trusted_cidrs` | empty | Comma-separated CIDRs allowed to send a PROXY header |

A header is accepted only from a trusted peer; from anyone else the bytes are left to the protocol. With `required`, a trusted peer's connection without a valid header is closed. TLS from the client still ends at the gateway: use TCP (layer 4) load balancing, not TLS termination, for the database protocols.

## Client certificates

A listener can ask TLS clients for a certificate, signed by a CA you name. Accounts can then require one (`REQUIRE X509`) or a particular one (`REQUIRE SUBJECT` and `REQUIRE ISSUER`), and Studio's operator mode can require one.

| Setting | Default | Meaning |
|---|---|---|
| `listener.<name>.client_cert` | `off` | `off`: no certificate is asked for. `optional`: the gateway asks for one and verifies it when the client sends it; clients without one still connect. `required`: a TLS handshake without a valid certificate fails |
| `tls.bundles.default.client_ca_path` | empty | PEM file with one or more CA certificates that client certificates must chain to |

Both take the data-centre and global scopes. The listener's `tls` mode must not be `off`, and the CA file must be set and readable; otherwise the listener change is rejected, the listener keeps running with its previous settings, and the change is reported as not applied, with a reason such as "listener.mysql.client_cert needs a CA: set tls.bundles.default.client_ca_path". A certificate that doesn't chain to the CA fails the TLS handshake, in `optional` mode too.

To let accounts on the MySQL listener log in with certificates from your client CA (the other listeners work the same way, with their own `listener.<name>.client_cert`):

```sh
sprigctl settings set --scope global tls.bundles.default.client_ca_path /etc/sprigdb/tls/client-ca.pem --reason "client certificates"
sprigctl settings set --scope global listener.mysql.client_cert optional --reason "client certificates"
```

Then, in the tenant, require a certificate for an account:

```sql
CREATE USER 'billing'@'%' IDENTIFIED BY 's3cret-Passw0rd'
    REQUIRE SUBJECT '/C=GB/O=Acme/CN=billing' AND ISSUER '/C=GB/O=Acme/CN=Acme client CA';
ALTER USER 'app'@'%' REQUIRE X509;
```

The client connects with its certificate and key, for example `mysql -h db.example.com -u acme.billing -p --ssl-mode=VERIFY_IDENTITY --ssl-ca=ca.pem --ssl-cert=billing.pem --ssl-key=billing-key.pem`. The password is still checked: a certificate is required in addition to it, not instead of it. The one exception is AMQP's `EXTERNAL` login, below.

`REQUIRE SUBJECT` and `REQUIRE ISSUER` compare the verified certificate's names exactly, in the one-line form MySQL and OpenSSL use: each attribute in certificate order, as `/C=GB/O=Acme/CN=billing`. To see a certificate's names in that form, run `openssl x509 -in billing.pem -noout -subject -issuer -nameopt compat`. A login that doesn't meet the account's requirement fails with 1045, like a wrong password.

The `optional` mode is usually the right one: accounts without a certificate requirement keep logging in without one. Use `required` only on a listener where every client has a certificate.

### Where certificate requirements are checked

Every listener checks `REQUIRE X509`, `REQUIRE SUBJECT` and `REQUIRE ISSUER` against the certificate the connection presented:

| Listener | Logins checked | A login that doesn't meet the requirement gets |
|---|---|---|
| `mysql`, `mariadb` | MySQL-protocol logins | 1045 |
| `mongo` | SCRAM logins | 18 `AuthenticationFailed` |
| `redis` | `AUTH` | `WRONGPASS` |
| `kafka` | SASL logins | `SASL_AUTHENTICATION_FAILED` |
| `bolt` | `basic` logins | `Neo.ClientError.Security.Unauthorized` |
| `amqp`, `amqps` | `PLAIN`, `AMQPLAIN` and `EXTERNAL` logins | 403 `ACCESS_REFUSED` |
| `http`, `https` | Basic, API keys, and the password login at `POST /v1/auth/login` | 401 |

`REQUIRE X509` needs a certificate the listener verified; `REQUIRE SUBJECT` and `REQUIRE ISSUER` also compare its names. The refusal looks like a wrong password, so it doesn't tell a client which part failed.

The certificate adds a check to the account's usual login (password, SCRAM, `AUTH` or API key). The one login by certificate alone is AMQP's SASL `EXTERNAL`, for accounts that require a certificate; see [AMQP](../connect/amqp.md#log-in-with-a-client-certificate-external). MongoDB's `MONGODB-X509` and Kafka mutual TLS without SASL aren't supported.

A login handed to another data centre because of the tenant's [residency](residency.md) carries whether a certificate was verified and its subject and issuer, so `REQUIRE SUBJECT` and `REQUIRE ISSUER` are checked in the deciding data centre as well. A handoff from a gateway of an earlier release carries no names, and accounts with `REQUIRE SUBJECT` or `REQUIRE ISSUER` are refused through it, so upgrade every data centre's gateways before relying on them.

`SHOW CREATE USER` prints an account's requirement as MySQL does, such as `REQUIRE SUBJECT '/C=GB/O=Acme/CN=billing' ISSUER '/C=GB/O=Acme/CN=Acme client CA'`. See [Tenants and accounts](../sql/tenants-and-accounts.md#show-create-user).

### Changing client certificate settings

`listener.<name>.client_cert` applies like any listener change: new connections get the new mode, and open connections keep the one they were accepted with. Making client certificates `required` doesn't close open connections.

Gateways poll the CA file every second, as they poll the server certificate, and compare its contents. When a change is seen on two polls in a row, the new CAs are loaded and used for new handshakes, within a few seconds and without a restart; connections already open keep running. A file that doesn't load (unreadable, malformed, or with no certificate in it) is rejected, logged as `gateway.client_ca_rejected`, and the old CAs stay in use; a successful reload is logged as `gateway.client_ca_reloaded`. To rotate a client CA without refusing anyone, put both the old and the new CA in the file until every client has a certificate from the new one.

## Tenant endpoints by server name

Users normally put their tenant in the login name: `acme.app`. With tenant endpoints, the host name a client connects to names the tenant instead, so `app` connecting to `acme.db.example.com` logs in to tenant `acme`. This helps applications that can't change their user names.

| Setting | Default | Scopes | Meaning |
|---|---|---|---|
| `listener.tenant_sni_suffixes` | empty | dc, global | DNS suffixes under which a TLS server name names a tenant: `<tenant>.<suffix>`. A comma-separated list (`db.example.com, sprig.example.net`), or a JSON array of strings. Empty: no tenant endpoints |

To set it up:

1. Point a wildcard DNS name, `*.db.example.com`, at your gateways or their load balancer.
2. Give the gateways a certificate that covers `*.db.example.com` (see [Certificates](#certificates)), so clients can verify the host name.
3. Set the suffix:

   ```sh
   sprigctl settings set --scope global listener.tenant_sni_suffixes db.example.com --reason "tenant endpoints"
   ```

4. Connect with TLS to the tenant's name, logging in with the plain user name:

   ```sh
   mongosh "mongodb://app:<password>@acme.db.example.com:27017/shop?authSource=admin&tls=true"
   ```

The rules:

- The server name comes from TLS (SNI), so tenant endpoints work only on TLS connections. Over HTTP, Basic authentication also takes the tenant from the `Host` header under a suffix.
- Exactly one label in front of the suffix names the tenant, and it must be a valid tenant name. `x.acme.db.example.com` names no tenant. Server names are compared in lower case.
- On a tenant endpoint the whole login name is the user name. Don't add the tenant prefix there: `acme.app` at `acme.db.example.com` looks for a user named `acme.app` in tenant `acme`, and `globex.app` there doesn't reach tenant `globex`. Both fail with 1045 unless such a user exists in `acme`.
- A tenant endpoint wins over `listener.redis.tenant` and `listener.amqp.tenant`.
- Each suffix must be a DNS name (labels of `a`–`z`, `0`–`9` and `-`, not starting or ending with `-`, up to 63 characters each, 253 in all), listed once. Any other value is refused when you set it, with 39080 `ER_SPRIG_SETTING_OUT_OF_BOUNDS`.
- The setting applies live: the next login uses the new list. Open sessions are unaffected.

Clients must send the server name. Most TLS clients send the host name they connect to; some MySQL clients don't unless configured to. If `app` is refused at a tenant endpoint while `acme.app` works on the shared name, check that the client sends SNI.

## Internal ports: mutual TLS

SprigDB's own processes talk to each other over mutual TLS, using a process identity issued by your internal CA, set in each process's configuration file:

```toml
[tls]
ca = "/etc/sprigdb/internal/ca.pem"     # the internal CA
cert = "/etc/sprigdb/internal/node.pem" # this process's certificate chain
key = "/etc/sprigdb/internal/node-key.pem"
```

| Port | Service | Transport |
|---|---|---|
| 7403 (`listener.peer.bind`) | Gateway to gateway: `KILL` across gateways, session lists, drains, transaction-handle forwarding, queue and pub/sub relays, login handoffs between data centres | With `[tls]`: mutual TLS, every client must present a certificate signed by `[tls] ca`. This isn't a setting. Without `[tls]` (development only), plain text and only on a loopback address; a non-loopback `listener.peer.bind` without `[tls]` leaves the peer service off |
| Controllers' internal address (`[controller] internal_addr`) | Gateways, workers, agents and other controllers calling a controller | Mutual TLS with the `[tls]` identity when `[controller] internal_tls = true` |
| Admin API (`[controller] admin_addr`, default `127.0.0.1:7480`) | `sprigctl`, Studio's operator mode, automation | HTTPS with `[controller] admin_tls_cert` and `admin_tls_key`; mutual TLS when `admin_tls_client_ca` is also set |

`listener.peer.bind` (default `0.0.0.0:7403`) is read when the gateway starts.

Connect `sprigctl` to an Admin API served over HTTPS with:

```sh
sprigctl --endpoint https://ctl.dc-a.example.com:7480 \
  --ca-file /etc/sprigdb/admin-ca.pem \
  --client-cert ~/.sprigctl/me.pem --client-key ~/.sprigctl/me-key.pem \
  dc list
```

The same options come from `SPRIGCTL_ENDPOINT`, `SPRIGCTL_CA_FILE`, `SPRIGCTL_CLIENT_CERT` and `SPRIGCTL_CLIENT_KEY`.

## Studio over HTTPS

Studio is served on the `http` and `https` listeners (`studio.listeners`, default `http,https`) under `studio.path` (`/studio`). Logins on plain HTTP are refused from any address other than loopback, with `ER_SPRIG_STUDIO_INSECURE_TRANSPORT` (39921), unless `studio.allow_insecure_http = true`. Behind a reverse proxy that terminates TLS, list the proxy's public origins in `studio.public_origins` so Studio's origin check accepts them.

### Operator mode

Operator mode in Studio (for the logins in `studio.operator_logins`) can be hardened with:

| Setting | Default | Meaning |
|---|---|---|
| `studio.operator_mode` | `on` | `off` hides operator mode everywhere |
| `studio.operator_require_mfa` | false | A second factor is needed to enter operator mode (else 39926) |
| `studio.operator_mtls` | `off` | `required`: a verified client certificate is needed for operator mode and the operator API routes (else `ER_SPRIG_STUDIO_OPERATOR_CERT_REQUIRED`, 39927) |

With `studio.operator_mtls = required`, set `listener.https.client_cert` to `optional` (or `required`) and `tls.bundles.default.client_ca_path` to the CA that signs your operators' certificates, and install each operator's certificate in their browser. Without a client certificate setting on the listener, no connection carries a verified certificate, and operator mode is refused on that gateway. Operators can always use `sprigctl` against the Admin API, which supports mutual TLS of its own.

## Watch TLS

| Metric | Labels | Meaning |
|---|---|---|
| `sprig_gateway_tls_handshakes_total` | `listener`, `version`, `outcome` | TLS handshakes by version and result |
| `sprig_gateway_cert_expiry_seconds` | `bundle` | Time until the loaded certificate expires |
| `sprig_gateway_listener_generation` | `listener` | Bumps at every applied listener change |
| `sprig_gateway_listener_plaintext_public` | `listener` | 1 when a listener accepts plain text on a public address |
| `sprig_gateway_connections_rejected_total` | `listener`, `reason` | Connections refused |

## Not supported yet

- Certificate bundles other than `default`, so per-listener certificates and per-listener client CAs.
- Logins by certificate alone other than AMQP `EXTERNAL`: MongoDB `MONGODB-X509`, and Kafka mutual TLS without SASL.
- `REQUIRE CIPHER` (error 1235).

## Related

- [Accounts and logins](accounts-and-logins.md)
- [How SprigDB works](../concepts/overview.md)
- [Configuration](../operate/configuration.md)
- [Deploy on Kubernetes](../operate/deploy-kubernetes.md)
- [Monitoring](../operate/monitoring.md)
