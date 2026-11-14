---
title: AMQP
audience: [user, dba]
sidebar:
  order: 7
---

# AMQP

SprigDB speaks AMQP 0-9-1, the protocol of RabbitMQ, and AMQP 1.0. RabbitMQ clients can declare exchanges and queues, publish with confirms and consume with acknowledgements, and every message is stored durably in MariaDB. Use it when your application already uses RabbitMQ, or when you want work queues that can also be written and acknowledged inside SQL transactions.

This page covers connecting. For the queue model, arguments, dead-lettering, delays and SQL access, see [Queues](../data-models/queues.md).

## Enable the listeners

There are two listeners, both off by default. Both serve AMQP 0-9-1 and 1.0; the client's protocol header picks the version.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `listener.amqp.enabled` | bool | `false` | Serve AMQP on 5672 |
| `listener.amqp.bind` | text | `0.0.0.0:5672` | Address and port |
| `listener.amqp.tls` | text | `off` | `required`, `optional` or `off` |
| `listener.amqp.cert` | text | `default` | Certificate bundle |
| `listener.amqps.enabled` | bool | `false` | Serve AMQP over TLS on 5671 |
| `listener.amqps.bind` | text | `0.0.0.0:5671` | Address and port |
| `listener.amqps.tls` | text | `required` | `required`, `optional` or `off` |
| `listener.amqps.cert` | text | `default` | Certificate bundle |
| `listener.amqps.client_cert` | text | `off` | Client certificates: `off`, `optional` or `required` (see [Client certificates](../security/tls.md#client-certificates)). Needed for `EXTERNAL` and for accounts that require a certificate |
| `listener.amqp.tenant` | text | empty | Tenant of user names without a dot, on both listeners |

5672 is plaintext by default, as RabbitMQ's port is. Prefer 5671: with PLAIN authentication the password crosses the network as it is. TLS starts with the first byte on a TLS listener; AMQP has no in-protocol upgrade.

```sh
sprigctl settings set listener.amqps.enabled true --scope global --reason "RabbitMQ clients"
```

Connection settings:

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `amqp.sasl_mechanisms` | text | `PLAIN,AMQPLAIN,EXTERNAL` | Mechanisms offered. `EXTERNAL` is offered only on a connection that presented a verified client certificate |
| `amqp.cert_login_field` | text | `common_name` | For `EXTERNAL`, the certificate field that names the user: `common_name` (the subject's last `CN`) or `distinguished_name` (the whole subject, as RabbitMQ prints it: `CN=acme.app,O=Acme`) |
| `amqp.frame_max` | int | `131072` | Largest frame, negotiated at connection |
| `amqp.channel_max` | int | `2047` | Channels per connection |
| `amqp.heartbeat_seconds` | int | `60` | Heartbeat the server proposes; the connection closes after two missed intervals |
| `amqp.default_vhost_database` | text | `mq` | Database of the virtual host `/` |
| `amqp.max_message_bytes` | int | `16777216` | Largest message |
| `amqp.publish_timeout_ms` | int | `10000` | A publish not confirmed in this time is nacked |
| `amqp.consumer_timeout_ms` | int | `1800000` | Longest a delivery may stay unacknowledged; 0 for no limit |
| `amqp.max_unacked_per_consumer` | int | `65535` | Unacknowledged deliveries per consumer with prefetch 0 |
| `amqp.quota_publish_bytes_per_sec` | int | `0` | Tenant's publish quota, bytes per second; 0 for none |

## Virtual hosts are databases

A virtual host is a database of your tenant:

| Virtual host | Database |
|---|---|
| `/` | The database `amqp.default_vhost_database` names, `mq` by default |
| `shop` | The database `shop` |

The database must exist. Opening a virtual host whose database your tenant doesn't have fails with 530 `NOT_ALLOWED` and `ER_SPRIG_MQ_NOT_FOUND` (39250) in the reply text. Create it first, for example from the MySQL port:

```sql
CREATE DATABASE mq;
```

Exchanges, queues and bindings belong to their virtual host, so two tenants (or two databases) can each have a queue named `billing`.

## Log in

| Mechanism | Supported | Notes |
|---|---|---|
| `PLAIN` | yes | 0-9-1 and 1.0 |
| `AMQPLAIN` | yes | 0-9-1 only |
| `EXTERNAL` | yes | 0-9-1 and 1.0. Offered only when the connection presented a client certificate the listener verified; the certificate names the user. See below |
| `ANONYMOUS` | no | |

The tenant comes from the user name: `acme.app` is user `app` of tenant `acme`. If `listener.amqp.tenant` names a tenant, a plain user name such as `app` logs in to that tenant, so applications written for RabbitMQ keep their user names. A dotted name wins over the bound tenant. On a tenant endpoint (a TLS host name such as `acme.db.example.com` under `listener.tenant_sni_suffixes`), the host name names the tenant and wins over both; see [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name).

Client-certificate requirements are checked on every AMQP login, against the certificate the connection presented. With `PLAIN` or `AMQPLAIN`, an account with `REQUIRE X509` needs a certificate the listener verified, and one with `REQUIRE SUBJECT` or `REQUIRE ISSUER` needs a certificate whose subject or issuer is exactly the name given, in addition to its password.

A refused login gets `connection.close` with 403 `ACCESS_REFUSED` and `ER_SPRIG_AMQP_SASL_FAILED` (39269) when the client announces `authentication_failure_close`, as current RabbitMQ clients do; otherwise the socket closes. The login runs the same checks as every other protocol: account locks, expired passwords, `REQUIRE SSL`, `auth.require_tls`, failure delays and the per-address limit. `connection.update-secret` checks the new secret as a fresh login.

To use an exchange or queue you need grants on the virtual host's database, or on the exchange or queue name. See [Queues](../data-models/queues.md#privileges).

### Log in with a client certificate (EXTERNAL)

SASL `EXTERNAL` logs in with the client certificate alone, as RabbitMQ's certificate login does. To use it:

1. Have the listener ask for certificates: set `tls.bundles.default.client_ca_path` and `listener.amqps.client_cert` to `optional` or `required`. See [Client certificates](../security/tls.md#client-certificates).
2. Issue the client a certificate whose common name is its login name, such as `CN=acme.billing`. The user name follows the usual rules: `acme.billing` names tenant `acme`, and a plain `billing` works on a bound listener (`listener.amqp.tenant`) or a tenant endpoint. `amqp.cert_login_field = distinguished_name` uses the whole subject instead.
3. Give the account a certificate requirement. Only an account with `REQUIRE X509`, `REQUIRE SUBJECT` or `REQUIRE ISSUER` may log in without a password, and its `SUBJECT` and `ISSUER` are still checked:

   ```sql
   CREATE USER 'billing'@'%' IDENTIFIED BY 's3cret-Passw0rd'
       REQUIRE SUBJECT '/O=Acme/CN=acme.billing' AND ISSUER '/O=Acme/CN=Acme client CA';
   ```

4. Connect with the certificate and the `EXTERNAL` mechanism, for example in Python with pika: `pika.credentials.ExternalCredentials()` and `ssl_options` holding the certificate and key.

`EXTERNAL` for an account without a certificate requirement, or with a certificate that doesn't meet it, is refused like a wrong password. An authorization identity, if the client sends one, must be the same name the certificate gives. The other login checks (locks, expired passwords, `auth.require_tls`, failure delays) apply as for a password login. A connection without a verified certificate isn't offered `EXTERNAL` at all.

## Connect

An AMQP URI names the user, the host and the virtual host. URL-encode `/` as `%2F`:

```text
amqps://acme.app:<password>@db.example.com:5671/%2F
amqps://acme.app:<password>@db.example.com:5671/shop
```

Python, with pika:

```python
import pika, ssl

ctx = ssl.create_default_context(cafile="ca.pem")
params = pika.ConnectionParameters(
    host="db.example.com", port=5671, virtual_host="/",
    credentials=pika.PlainCredentials("acme.app", os.environ["MQ_PASSWORD"]),
    ssl_options=pika.SSLOptions(ctx, "db.example.com"))
conn = pika.BlockingConnection(params)
ch = conn.channel()

ch.queue_declare("billing", durable=True, arguments={"x-queue-type": "quorum"})
ch.confirm_delivery()
ch.basic_publish("", "billing", b'{"order_id": 7001}',
                 pika.BasicProperties(content_type="application/json", delivery_mode=2))
```

Java, with the RabbitMQ client:

```java
ConnectionFactory f = new ConnectionFactory();
f.setUri("amqps://db.example.com:5671/%2F");
f.setUsername("acme.app");
f.setPassword(System.getenv("MQ_PASSWORD"));
f.useSslProtocol(sslContext);
try (Connection c = f.newConnection(); Channel ch = c.createChannel()) {
    ch.basicQos(50);
    ch.basicConsume("billing", false, (tag, d) -> {
        // do the work
        ch.basicAck(d.getEnvelope().getDeliveryTag(), false);
    }, tag -> {});
}
```

Node.js, with amqplib:

```js
const amqp = require('amqplib');
const conn = await amqp.connect('amqps://acme.app:' + encodeURIComponent(process.env.MQ_PASSWORD)
  + '@db.example.com:5671/%2F', { ca: [fs.readFileSync('ca.pem')] });
const ch = await conn.createConfirmChannel();
await ch.assertExchange('orders', 'topic', { durable: true });
ch.publish('orders', 'order.paid.eu', Buffer.from('{"order_id":7001}'));
await ch.waitForConfirms();
```

Go, with amqp091-go:

```go
conn, err := amqp.DialTLS("amqps://acme.app:"+url.QueryEscape(pw)+"@db.example.com:5671/%2F", tlsConfig)
ch, err := conn.Channel()
q, err := ch.QueueDeclare("billing", true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
```

## What works

### Connection and server

The server announces product `SprigDB`, version `4.1.0` (the RabbitMQ level clients compare against), and these capabilities: `publisher_confirms`, `basic.nack`, `consumer_cancel_notify`, `exchange_exchange_bindings`, `connection.blocked`, `consumer_priorities`, `authentication_failure_close`, `per_consumer_qos` and `direct_reply_to`.

### Methods

| Class | Methods | Support |
|---|---|---|
| `connection` | `start`, `tune`, `open`, `close`, `blocked`, `unblocked`, `update-secret` | yes |
| `channel` | `open`, `close`, `flow` | yes; `flow` with `active = false` answers 540 |
| `exchange` | `declare`, `delete`, `bind`, `unbind` | yes; types `direct`, `fanout`, `topic`, `headers` and `x-delayed-message`; alternate and internal exchanges |
| `queue` | `declare`, `bind`, `unbind`, `purge`, `delete` | yes; classic and quorum queues, exclusive, auto-delete and server-named queues |
| `basic` | `qos`, `consume`, `cancel`, `publish`, `return`, `deliver`, `get`, `ack`, `nack`, `reject`, `recover` | yes; `immediate`, `prefetch_size` other than 0 and `recover` without requeue answer 540 |
| `confirm` | `select` | yes; one confirm per message, after the commit |
| `tx` | `select`, `commit`, `rollback` | yes, but `tx.commit` isn't one atomic transaction across queues |

Queue arguments, such as `x-message-ttl`, `x-expires`, `x-max-length`, `x-overflow`, `x-dead-letter-exchange`, `x-delivery-limit`, `x-single-active-consumer` and `x-max-priority`, behave as in RabbitMQ. `x-queue-type = stream` isn't supported (540); use SprigDB's [event streams](../data-models/streams.md) over Kafka instead. See [Queues](../data-models/queues.md#queue-arguments) for the full list.

### Delivery guarantees

- **A confirm means committed.** `basic.ack` after `confirm.select` comes once the message is committed on MariaDB; with `queue.confirm_mode = replica`, once a replica has also applied it. A publish not committed within `amqp.publish_timeout_ms` is nacked (39261); republish it.
- **Messages are always on disk**, whatever `delivery_mode` says, except in temporary queues (see [Queues](../data-models/queues.md#how-queues-are-stored)).
- **Unacknowledged deliveries return to their queue** when the channel or connection closes, marked `redelivered`.
- **A stuck consumer is cancelled.** A delivery unacknowledged longer than `amqp.consumer_timeout_ms` (30 minutes) is requeued and the consumer gets `basic.cancel` (39262).
- **Publish quotas** send `connection.blocked` to the virtual host's connections for the rest of the second.

### AMQP 1.0

AMQP 1.0 clients use the same exchanges and queues. Targets are `/exchanges/<exchange>/<key>`, `/exchanges/<exchange>`, `/queues/<queue>`, or the anonymous terminus with `to` set on each message; sources are `/queues/<queue>`. The virtual host comes from `open.hostname` as `vhost:<name>`. Messages convert between 0-9-1 and 1.0 as RabbitMQ 4 converts them. AMQP 1.0 transactions and link recovery aren't supported. See [Queues](../data-models/queues.md#over-amqp-10).

## Not supported

| Feature | What happens |
|---|---|
| Stream queues and super streams | 540 `NOT_IMPLEMENTED` |
| `immediate` publishes, `prefetch_size`, `basic.recover` without requeue, `channel.flow` off | 540 |
| SASL `ANONYMOUS` | Not offered |
| The RabbitMQ management HTTP API (port 15672) and its web UI | Use `GET /v1/databases/{database}/mq`, `sprig.mq_info`, or [Studio](../tools/studio.md) |
| MQTT and STOMP | No listener |
| Atomic `tx.commit` across queues | Publish and acknowledge from SQL inside a database transaction instead |

## Errors you may see

AMQP clients receive RabbitMQ's reply codes, with SprigDB's number and name in the reply text:

```text
NOT_FOUND - no queue 'billing' in vhost '/' (39250 ER_SPRIG_MQ_NOT_FOUND)
```

| Reply | SprigDB code | When |
|---|---|---|
| 403 `ACCESS_REFUSED` | 39269, 39253 | A refused login, or no permission on the exchange, queue or virtual host |
| 404 `NOT_FOUND` | 39250 | An unknown exchange or queue |
| 405 `RESOURCE_LOCKED` | 39252 | An exclusive queue or consumer held by another connection |
| 406 `PRECONDITION_FAILED` | 39251, 39257, 39264 | A redeclare with other arguments, an unknown delivery tag, a message over `amqp.max_message_bytes`, an `x-delay` above `queue.delay_max_ms` |
| 312 `NO_ROUTE` | | A `mandatory` publish that reached no queue (`basic.return`) |
| 530 `NOT_ALLOWED` | 39250, 39271 | A virtual host whose database doesn't exist; more channels than `channel_max`; a reused consumer tag |
| 540 `NOT_IMPLEMENTED` | 39254 | An unsupported method, flag or argument |
| 501 to 505 | 39268 | A malformed frame, or a method on a channel that isn't open |
| `basic.nack` | 39256, 39260, 39261 | A queue full under `reject-publish`, an unreachable queue owner (retry), or no commit in time |
| `basic.cancel` from the server | 39262 | A consumer timed out, or its queue was deleted |
| Connection closed after `AMQP 0 0 9 1` | 39270 | An unknown protocol header |

See [Queues](../data-models/queues.md#errors) for every queue error, and [Error codes](../reference/errors.md).

## Related

- [Queues](../data-models/queues.md)
- [Connect to SprigDB](overview.md)
- [Streams, queues and sinks in SQL](../sql/streams-queues-sinks.md)
- [Accounts and logins](../security/accounts-and-logins.md)
- [Connection problems](../troubleshooting/connections.md)
