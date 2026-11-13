---
title: Queues
audience: [user, dba]
sidebar:
  order: 10
---

# Queues

A queue holds messages until a consumer takes and acknowledges them, and gives each message to one consumer. Use queues for work that must be done once ("send the welcome mail", "resize this image"), with retries and dead letters when it fails. If every reader should see every record, use an [event stream](streams.md) instead.

SprigDB's queues follow RabbitMQ's model: exchanges route messages to queues through bindings. Existing RabbitMQ applications connect over AMQP 0-9-1 (or AMQP 1.0) without changes. From SQL and logic code you can publish and acknowledge inside a database transaction, so a message is sent only if your data change commits, and a job is marked done only if its results commit.

## How queues are stored

| RabbitMQ | SprigDB |
|---|---|
| Virtual host | A database of the tenant. `/` is the database named by `amqp.default_vhost_database` (`mq` by default); any other virtual host is the database of the same name |
| Exchange, queue, binding | Catalog objects of that database |
| User | A SprigDB user of the tenant; the login names the tenant (`acme.app`) |
| Permissions | Grants on the database or on the exchange or queue name |
| Node | A gateway |

The database must exist: opening a virtual host whose database the tenant doesn't have fails with 530 `NOT_ALLOWED` and `ER_SPRIG_MQ_NOT_FOUND` (39250) in the reply text. Create the database first, for example `CREATE DATABASE mq;` for virtual host `/`.

Durable messages are rows in internal tables in the virtual host database's first shard schema, on the tenant's cluster: messages, per-queue state, an outbox for transactional publishes and dead letters, and delayed messages. They are written in batches (group commit), and a publisher confirm means the batch committed on the MariaDB primary. Messages are always on disk, whatever `delivery_mode` says, except in temporary queues (below).

Each queue has one **owner**: a gateway that holds the queue's lease, keeps its head in memory and decides every delivery. Clients can connect to any gateway; a gateway that doesn't own a queue relays to the owner. The owner moves to the gateway where most of a queue's consumers are, and if the owner's gateway stops, another gateway takes the queue over once the lease expires (at most about 10 seconds).

**Temporary queues** (exclusive, auto-delete or server-named) live only in the memory of the gateway that declared them, messages included. They aren't written to MariaDB and are lost if that gateway stops, as they would be when the connection drops. Other gateways can still route to them.

## Exchanges, queues and bindings

### Exchange types

| Type | Routes by |
|---|---|
| `direct` | Exact routing key |
| `fanout` | Every bound queue |
| `topic` | Routing key patterns with `*` (one word) and `#` (zero or more words) |
| `headers` | Message headers, with the binding's `x-match` |
| `x-delayed-message` | Holds each message for its `x-delay` header, then routes it by the exchange's `x-delayed-type` (`direct` by default) |

Every virtual host has RabbitMQ's built-in exchanges: the default exchange `""` (routing key = queue name), `amq.direct`, `amq.fanout`, `amq.topic`, `amq.headers` and `amq.match`. Exchange-to-exchange bindings, alternate exchanges (`alternate-exchange`) and internal exchanges work as in RabbitMQ.

### Queue types

`x-queue-type` is `classic` or `quorum`; without it, `queue.default_type` (`classic`) applies. Both types use the same storage and engine, and MariaDB replicates both. The type only selects which arguments and rules apply:

| | Classic | Quorum |
|---|---|---|
| Durable, auto-delete, exclusive | Any | Durable only, never exclusive (406) |
| Priorities | With `x-max-priority`, 1 to 255 levels | Always on, 32 levels; a message without a priority counts as 4 |
| Delivery limit | With `x-delivery-limit` | `x-delivery-limit`, else `queue.delivery_limit_default` (20) |
| `x-overflow` | `drop-head`, `reject-publish`, `reject-publish-dlx` | `drop-head`, `reject-publish` (`reject-publish-dlx` acts as `reject-publish`) |

`x-queue-type = stream` is refused with 540 (`NOT_IMPLEMENTED`); use [event streams](streams.md) over Kafka or SQL.

### Queue arguments

| Argument | Meaning |
|---|---|
| `x-message-ttl` | Message lifetime in milliseconds. A message's own `expiration` property applies when smaller |
| `x-expires` | Delete the queue after it has been unused for this many milliseconds |
| `x-max-length`, `x-max-length-bytes` | Limits on ready messages |
| `x-overflow` | What happens at a length limit: `drop-head` (default) drops or dead-letters the oldest, `reject-publish` refuses the new message, `reject-publish-dlx` refuses and dead-letters it |
| `x-dead-letter-exchange`, `x-dead-letter-routing-key` | Where dead letters go |
| `x-delivery-limit` | Deliveries before a message is dead-lettered |
| `x-max-priority` | Priority levels of a classic queue |
| `x-single-active-consumer` | Only one consumer receives at a time; the others wait as standbys |
| `x-consumer-timeout` | Per-queue consumer timeout in milliseconds |

Redeclaring a queue with different `durable`, `auto_delete` or arguments (`x-queue-type`, `x-message-ttl`, `x-max-length`, `x-max-length-bytes`, `x-overflow`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-max-priority`, `x-single-active-consumer`) fails with 406 `PRECONDITION_FAILED`, as in RabbitMQ.

Names of exchanges and queues are up to 255 bytes. Server-named queues are `amq.gen-` followed by 22 characters.

### Declaring topology

Applications declare exchanges, queues and bindings over AMQP, as they do with RabbitMQ, and the declarations are stored in the tenant's catalog. In SQL, the `CREATE EXCHANGE`, `CREATE QUEUE` and `BIND` statements declare per-tenant definitions only (see [Queues for every tenant](#queues-for-every-tenant)); ordinary exchanges and queues are declared from AMQP clients.

To see a virtual host's topology, call `GET /v1/databases/{database}/mq` over REST. It returns the exchanges, queues and bindings. See the [REST API](../reference/api.md).

## Use queues over AMQP 0-9-1

The AMQP listeners are off by default. Turn on `listener.amqp.enabled` for 5672 (plaintext by default, `listener.amqp.tls = off`) and `listener.amqps.enabled` for 5671 (TLS from the first byte). Both serve AMQP 0-9-1 and 1.0; the protocol header picks the version. See [AMQP](../connect/amqp.md).

For example, with `pika` for Python:

```python
import pika

params = pika.ConnectionParameters(
    host="gateway.example.com", port=5672, virtual_host="/",
    credentials=pika.PlainCredentials("acme.app", "<password>"))
conn = pika.BlockingConnection(params)
ch = conn.channel()

ch.exchange_declare("orders", exchange_type="topic", durable=True)
ch.queue_declare("billing", durable=True,
                 arguments={"x-queue-type": "quorum",
                            "x-dead-letter-exchange": "dlx"})
ch.queue_bind("billing", "orders", routing_key="order.paid.*")

ch.confirm_delivery()
ch.basic_publish("orders", "order.paid.eu", b'{"order_id": 7001}',
                 pika.BasicProperties(content_type="application/json",
                                      delivery_mode=2))

ch.basic_qos(prefetch_count=50)
def handle(ch, method, props, body):
    ...  # do the work
    ch.basic_ack(method.delivery_tag)
ch.basic_consume("billing", handle)
ch.start_consuming()
```

### Logging in

- The tenant comes from a dotted user name (`acme.app`), else from `listener.amqp.tenant`, which binds the listener to one tenant so that unchanged applications can keep plain user names such as `app`. A dotted name wins over the bound tenant.
- SASL mechanisms are `PLAIN` and, on 0-9-1, `AMQPLAIN` (`amqp.sasl_mechanisms`). `ANONYMOUS` is never accepted.
- A refused login gets `connection.close` with 403 when the client announced `authentication_failure_close`; otherwise the socket closes.
- `connection.update-secret` checks the new secret as a fresh login of the same user.

### Methods served

All the AMQP 0-9-1 classes are served: `connection`, `channel`, `exchange` (declare, delete, bind, unbind), `queue` (declare, bind, unbind, purge, delete), `basic` (qos, consume, cancel, publish, get, ack, nack, reject, recover), `confirm` and `tx`. The server announces product `SprigDB`, version `4.1.0` (the RabbitMQ level clients compare against), and these capabilities:

| Capability | Behaviour |
|---|---|
| `publisher_confirms` | `confirm.select`; `basic.ack` after the commit, `basic.nack` on failure |
| `basic.nack` | Nack with `multiple` and `requeue` |
| `consumer_cancel_notify` | `basic.cancel` from the server when a queue is deleted or a consumer times out |
| `exchange_exchange_bindings` | `exchange.bind` and `exchange.unbind` |
| `connection.blocked` | `connection.blocked` and `unblocked` when a publish quota is reached |
| `consumer_priorities` | `x-priority` on `basic.consume` |
| `authentication_failure_close` | `connection.close` 403 before closing the socket |
| `per_consumer_qos` | `basic.qos` with `global = false` per consumer, `global = true` per channel |
| `direct_reply_to` | The `amq.rabbitmq.reply-to` pseudo-queue for RPC |

Reply codes follow RabbitMQ: an unknown delivery tag is 406, a reused consumer tag 530, a method on a channel that isn't open 504, and malformed frames 501 to 505. SprigDB's error number and name are in the reply text, for example `NOT_FOUND - no queue 'q' in vhost '/' (39250 ER_SPRIG_MQ_NOT_FOUND)`.

### Publishing and confirms

- A confirm means the message is committed on the shard's MariaDB primary. With `queue.confirm_mode = replica`, the confirm also waits until a replica has applied it; on a single node or Galera, the commit is the confirm.
- A publish not confirmed within `amqp.publish_timeout_ms` (10 s) is nacked (`ER_SPRIG_MQ_CONFIRM_TIMEOUT`, 39261). Republish nacked messages.
- Confirms come one per message, never with `multiple`, and a `basic.return` comes before its confirm.
- `mandatory` returns unroutable messages with 312 `NO_ROUTE`.
- A message above `amqp.max_message_bytes` (16 MiB) is refused (`ER_SPRIG_MQ_MESSAGE_TOO_LARGE`, 39257).
- When the tenant's publishes pass `amqp.quota_publish_bytes_per_sec`, the virtual host's connections get `connection.blocked` for the rest of that second.
- A `basic.get` first commits the channel's waiting publishes, so a channel reads its own publish.

### Consuming, acknowledgements and prefetch

- **Credit.** `basic.qos` `prefetch_count` limits unacknowledged deliveries per consumer (`global = false`) or per channel (`global = true`). With prefetch 0, a consumer may hold up to `amqp.max_unacked_per_consumer` (65,535). `prefetch_size` must be 0.
- **Dispatch.** Competing consumers on a queue share its messages; higher `x-priority` consumers are served first; with `x-single-active-consumer`, one consumer receives at a time. An exclusive consumer is the only one allowed on its queue.
- **Settlement.** `basic.ack` deletes a message. `basic.nack` and `basic.reject` with `requeue = true` return it to its original position, marked `redelivered`; without requeue it is dead-lettered or dropped. `basic.recover` with `requeue = true` requeues the channel's unacknowledged deliveries.
- **Delivery count.** A failed delivery raises the header `x-delivery-count`: a `basic.reject` with requeue, or a delivery returned because its channel or connection closed. `basic.nack` with requeue and `basic.recover` return the message without counting. When the count passes the queue's delivery limit, the message is dead-lettered with reason `delivery_limit`.
- **Consumer timeout.** A delivery left unacknowledged longer than `x-consumer-timeout`, or `amqp.consumer_timeout_ms` (30 minutes; 0 disables it), is requeued and the consumer is cancelled (`ER_SPRIG_MQ_CONSUMER_TIMEOUT`, 39262).
- **Lost connections.** When a connection closes, its unacknowledged deliveries return to their queues, marked `redelivered`.
- **Delivery tokens.** Every delivery carries the header `x-sprig-delivery`: a token that SQL and logic can use to acknowledge that delivery inside a database transaction (below).

### Transactions on a channel

`tx.select`, `tx.commit` and `tx.rollback` work. Acknowledgements are checked at once, applied at commit and restored on rollback. `tx.commit` publishes the channel's messages, waits for every confirm, then applies its acknowledgements. It isn't one atomic MariaDB transaction across queues: for atomic work with your data, publish and acknowledge from SQL inside a database transaction.

### RPC and direct reply-to

Requests with `reply_to` and `correlation_id` work unchanged, with an exclusive reply queue or with direct reply-to: consume `amq.rabbitmq.reply-to` with `no_ack`, and publish requests with `reply_to = amq.rabbitmq.reply-to`. The server rewrites the name into a signed address that a responder on any gateway of the data centre can reply to. A forged or other tenant's address is unroutable. Misuse, such as manual acknowledgement on the pseudo-queue, is 406 (`ER_SPRIG_MQ_DIRECT_REPLY_TO`, 39275).

## Over AMQP 1.0

AMQP 1.0 clients use the same queues. Targets are `/exchanges/<exchange>/<key>`, `/exchanges/<exchange>`, `/queues/<queue>`, or the anonymous terminus with `to` set on each message; sources are `/queues/<queue>`. Virtual hosts come from `open.hostname` as `vhost:<name>`. Consumer outcomes map to settlement: `accepted` acknowledges, `rejected` dead-letters, `released` requeues, and `modified` requeues (or dead-letters with `undeliverable-here`). Publishing links get `amqp10.link_credit` (256) credit. Messages convert between 0-9-1 and 1.0 as RabbitMQ 4 converts them, so either protocol can read the other's messages. See [AMQP](../connect/amqp.md).

## Use queues from SQL

SQL sessions publish, take and acknowledge messages with `CALL sprig.mq_*` statements. The arguments are evaluated by your session, so any expression works, such as `JSON_OBJECT(...)` or a column of a subquery.

### Publish inside a transaction

```sql
START TRANSACTION;
UPDATE orders SET status = 'paid' WHERE order_id = 7001;
CALL sprig.mq_publish('orders', 'order.paid.eu',
     JSON_OBJECT('order_id', 7001, 'amount', 49.90),
     JSON_OBJECT('content_type', 'application/json', 'message_id', 'o7001-paid'));
COMMIT;
```

`CALL sprig.mq_publish(exchange, routing_key, body, properties [, mandatory [, vhost]])` returns one row, `routed`: the number of queues the message reaches.

- **Inside a transaction**, the message is routed at once (so `routed` and `mandatory` are known) and written to the virtual host's outbox in your transaction. If the transaction rolls back, nothing is sent. After the commit, the message is forwarded to its queues, exactly once and in commit order across transactions (with `queue.forwarder = feed`, the default).
- **Outside a transaction**, it is published at once, and the call returns after the queues committed it.
- `properties` is a JSON object with RabbitMQ's property names: `content_type`, `content_encoding`, `headers` (an object), `delivery_mode`, `priority`, `correlation_id`, `reply_to`, `expiration`, `message_id`, `timestamp`, `type`, `user_id`, `app_id`. `NULL` means none. Messages are persistent unless `delivery_mode` is 1.
- With `mandatory = 1`, a message that reaches no queue is an error (`ER_SPRIG_MQ_UNROUTABLE`, 39255).
- `vhost` is a virtual host name; `NULL` or absent means `/`.

The example above needs the exchange `orders`, declared by an AMQP client. You can try the call in any database without one: the default exchange `""` routes to the queue named by the routing key, and here no queue `billing` exists yet, so the message reaches none. `DATABASE()` names the current database as the virtual host.

```sql test
CALL sprig.mq_publish('', 'billing', 'hello', NULL, 0, DATABASE());
```

```text output
routed
0
```

With `mandatory` set to 1, the same publish fails:

```sql test-error
CALL sprig.mq_publish('', 'billing', 'hello', NULL, 1, DATABASE());  -- ER_SPRIG_MQ_UNROUTABLE (39255)
```

### Take and acknowledge inside a transaction

```sql
CALL sprig.mq_get('billing', 10, 30000);   -- up to 10 messages, held for 30 s

START TRANSACTION;
INSERT INTO invoices (order_id, amount) VALUES (7001, 49.90);
CALL sprig.mq_ack('<delivery_token>');     -- from mq_get's first column
COMMIT;
```

`CALL sprig.mq_get(queue [, max [, lease_ms [, vhost [, lease_tag]]]])` takes up to `max` messages (default 1, at most 1,000) and holds them for `lease_ms` (default 30,000). It returns the columns `delivery_token`, `body`, `properties` (JSON), `routing_key`, `exchange`, `redelivered` and `delivery_count`. Messages not acknowledged before the lease ends, or before the session ends, go back to the queue.

`CALL sprig.mq_ack(token)` deletes the message in your transaction. If the transaction commits, the message is gone for good; if it rolls back, the message comes back `redelivered`. Acknowledging a delivery that was already settled, or whose lease ran out, fails with `ER_SPRIG_MQ_ALREADY_SETTLED` (39258) and rolls the transaction back, so your writes never commit twice for one message. A malformed or forged token is `ER_SPRIG_MQ_DELIVERY_TOKEN_INVALID` (39259):

```sql test-error
CALL sprig.mq_ack('not-a-delivery-token');  -- ER_SPRIG_MQ_DELIVERY_TOKEN_INVALID (39259)
```

`CALL sprig.mq_nack(token [, requeue])` returns the delivery to the queue (`requeue` 1, the default) or dead-letters it (0).

The token works for any delivery, not only SQL ones: a consumer that received a message over AMQP can acknowledge it from its database transaction with the `x-sprig-delivery` header's value.

### Inspect and manage queues

| Call | Returns | Does |
|---|---|---|
| `sprig.mq_info(queue [, vhost])` | `ready`, `unacked`, `consumers` | Counts of one queue |
| `sprig.mq_info()` or `sprig.mq_info(NULL, vhost)` | `queue`, `ready`, `unacked`, `consumers` | One row per durable queue you may read |
| `sprig.mq_browse(queue [, max [, vhost]])` | `seq`, `body`, `properties`, `routing_key`, `exchange`, `delivery_count` | Up to `max` stored messages (default 100, at most 1,000) in delivery order, without consuming or locking them; includes delivered, unacknowledged messages |
| `sprig.mq_purge(queue [, vhost])` | `purged` | Removes ready messages, as `queue.purge` does |
| `sprig.mq_republish(queue, exchange [, routing_key [, max [, vhost]]])` | `moved` | Moves up to `max` messages (default 100, at most 10,000) to an exchange, for example out of a dead-letter queue; `routing_key` `NULL` keeps each message's own key. Delivery is at least once: a crash mid-move can leave a copy in both queues |
| `sprig.mq_release(queue, lease_tag [, vhost])` | OK | Returns every message that `mq_get` took under `lease_tag` to the queue at once, marked `redelivered` |

### Over REST and in logic

The same calls run over REST through the SQL endpoint, `POST /v1/databases/{database}/sql:execute`, and inside a REST transaction when you pass its handle. Logic code calls `db.mq.publish(exchange, routingKey, body, props?)`, `db.mq.get(queue, {max, leaseMs})`, `db.mq.ack(token)` and `db.mq.nack(token, requeue)` in JavaScript and Lua, or `Sprig.Mq.Publish`, `Get`, `Ack` and `Nack` in KIRun, through the caller's session and transaction, in virtual host `/`. A trigger `ON QUEUE` runs code for each message and acknowledges it in the same transaction as the code's writes. See [Logic](logic.md).

## Dead letters, TTL and delays

### Dead-lettering

A message is dead-lettered when it is rejected without requeue, expires, is dropped by a length limit, or passes its delivery limit. If its queue has `x-dead-letter-exchange`, the message is routed there with `x-dead-letter-routing-key` (or its own key) and an `x-death` header, as in RabbitMQ: one entry per queue and reason (`rejected`, `expired`, `maxlen`, `delivery_limit`). Without a dead-letter exchange, it is dropped.

Dead-lettering is always at least once: the delete from the old queue and the write to the outbox commit together, so a crash can't lose a message between queues. `x-dead-letter-strategy` is accepted and doesn't change this. The message's `expiration` is removed, so it doesn't expire again at once. A message that would loop back into a queue it died in, for a reason other than `rejected`, is dropped (`ER_SPRIG_MQ_DEAD_LETTER_CYCLE`, 39277, logged).

### Time to live

- **Message TTL** (`x-message-ttl`, or the smaller `expiration` per message) applies at the head of the queue: a message expires when it reaches the head, and expired heads are also swept from queues without consumers.
- **Queue TTL** (`x-expires`) deletes a queue unused for that long.

### Delayed messages

Declare an exchange of type `x-delayed-message`, with `x-delayed-type` naming the routing type, and publish with the header `x-delay` in milliseconds:

```python
ch.exchange_declare("later", exchange_type="x-delayed-message", durable=True,
                    arguments={"x-delayed-type": "direct"})
ch.basic_publish("later", "billing", b"retry",
                 pika.BasicProperties(headers={"x-delay": 60000}))
```

The message is stored in MariaDB and routed when it is due, so bindings added in the meantime count. A missing, zero or negative `x-delay` routes at once. An `x-delay` above `queue.delay_max_ms` (4,294,967,295) is refused (`ER_SPRIG_MQ_DELAY_OUT_OF_RANGE`, 39264). `mandatory` is accepted but can't return a delayed message, because its route isn't known when it is published. From SQL, publish to the delayed exchange with `headers` containing `x-delay`.

## Ordering and delivery guarantees

- **Per queue, messages are delivered in the order they were enqueued**, except that higher priorities go first and a requeued message returns to its original position.
- One channel's publishes to one queue keep their order. Transactional publishes from SQL and logic are forwarded in commit order across transactions.
- With several consumers, each sees its share in order, but processing can overlap. Use `x-single-active-consumer` when processing must be strictly ordered.
- **At least once.** A message that a consumer received but didn't acknowledge is delivered again after a crash, a lost connection or an owner takeover, always marked `redelivered`. A message is never delivered twice without the flag.
- **Exactly-once effects** come from acknowledging inside the database transaction that does the work (`sprig.mq_ack`, or a trigger `ON QUEUE`).
- **Owner failover.** When a queue's owner gateway stops, deliveries pause until another gateway takes the queue over: up to the 10-second lease, plus `queue.takeover_grace_ms` (2,000 ms) during which consumers that re-attach keep the deliveries they hold. Publishes waiting on an unreachable owner fail with `ER_SPRIG_MQ_OWNER_MOVED` (39260, retryable); republish them.
- **Shard moves.** When the tenant moves between clusters, queue tables move with their shard. During the brief hold, some publishes may be nacked; republish them.

## Changes of a table to an exchange

To publish every committed change of a table to an exchange, set `queue.change_exchange` on the table:

```sql
ALTER TABLE orders SPRIG SET (queue.change_exchange = 'orders.changes');
```

Each change is published to that exchange, in the table's database, with routing key `<db>.<table>.<op>` (for example `shop.orders.u`) and the [change topic](streams.md) envelope as a JSON body. Changes are published as the user who ran the `ALTER TABLE`, so that user's privileges and row-level security decide what is sent: an update that hides or reveals a row goes out as a delete or an insert. Delivery is at least once, in commit order. Reset the setting to stop. Consumers that need replay should read a change topic instead. See [Change feed](../concepts/change-feed.md).

## Queues for every tenant

A tenant can declare exchanges, queues and bindings that every member tenant gets its own instance of, in its own database and storage:

```sql
CREATE EXCHANGE jobs.work TYPE topic FOR EACH TENANT;
CREATE QUEUE jobs.welcome_mail FOR EACH TENANT WITH (x_queue_type = 'quorum');
BIND QUEUE jobs.welcome_mail TO EXCHANGE jobs.work KEY 'mail.*' FOR EACH TENANT;
```

- Arguments in `WITH (...)` use `_` for `-` (`x_queue_type` is `x-queue-type`); classic and quorum queues are allowed.
- In a member, the names are local: its AMQP clients declare, publish and consume them as their own queues. Deleting an inherited name is 403. A member can override the policy-like arguments (TTL, length limits, overflow, delivery limit, dead-lettering, consumer timeout) with `ALTER INHERITED QUEUE`.
- An ancestor publishes into one member's instance with the header `x-sprig-tenant: <tenant>`, or with the routing key `@<tenant>:<key>` for clients that can't set headers; `*` as the tenant broadcasts to every member. Deliveries carry `x-sprig-tenant` and `x-sprig-origin-tenant`.
- A holder of `SPRIG_FAN_IN` in the ancestor consumes every member's instance at once with `basic.consume` argument `x-sprig-fan-in = true`. Order holds per member only.

See [Inheritance](inheritance.md) for definitions, overrides and fan-in.

## Privileges

RabbitMQ's permissions map to grants on the virtual host's database, or on the exchange or queue name:

| RabbitMQ | SprigDB grant |
|---|---|
| Configure (declare, delete, bind) | `CREATE`, `DROP` or `ALTER` |
| Write (publish to an exchange) | `INSERT` |
| Read (consume, get, purge) | `SELECT`, and `DELETE` for queues |

A user with no right on a virtual host's database, or on any of its exchanges and queues, can't open the virtual host. A refused operation is 403 `ACCESS_REFUSED` (`ER_SPRIG_MQ_ACCESS_REFUSED`, 39253). From SQL, a publish inside a transaction writes the outbox and needs `INSERT` on the database, and an acknowledgement inside a transaction deletes the message row and needs `DELETE` on the database.

## Limits

| Limit | Default | Setting | When exceeded |
|---|---|---|---|
| Message size | 16 MiB (at most 512 MiB) | `amqp.max_message_bytes` | 406, 39257 |
| Frame size | 131,072 bytes | `amqp.frame_max` | Negotiated at connection |
| Channels per connection | 2,047 | `amqp.channel_max` | 530, 39271 |
| Heartbeat proposed | 60 s | `amqp.heartbeat_seconds` | Connection closed after two missed intervals |
| Unacknowledged per consumer with prefetch 0 | 65,535 | `amqp.max_unacked_per_consumer` | Deliveries wait |
| Consumer timeout | 30 minutes | `amqp.consumer_timeout_ms` | Requeue and cancel, 39262 |
| Confirm wait | 10 s | `amqp.publish_timeout_ms` | Nack, 39261 |
| Longest delay | 4,294,967,295 ms | `queue.delay_max_ms` | 406, 39264 |
| Publish rate per tenant | none | `amqp.quota_publish_bytes_per_sec` | `connection.blocked` |
| Exchange and queue names | 255 bytes | | |
| `mq_get` and `mq_browse` per call | 1,000 messages | | |
| `mq_republish` per call | 10,000 messages | | |

Sharding: all of a virtual host's queues live in its database's first shard, on one MariaDB primary, and each queue is served by one owner gateway at a time. A queue doesn't spread across shards. For more throughput, use more queues, possibly in different databases.

## Settings

| Setting | Default | Scopes | Meaning |
|---|---|---|---|
| `listener.amqp.enabled` | false | dc, global | Serve AMQP on `listener.amqp.bind` (`0.0.0.0:5672`) |
| `listener.amqp.tls` | off | dc, global | TLS mode of the 5672 listener |
| `listener.amqps.enabled` | false | dc, global | Serve AMQP with TLS on `listener.amqps.bind` (`0.0.0.0:5671`) |
| `listener.amqp.tenant` | empty | dc, global | Tenant of plain user names on both AMQP listeners |
| `amqp.default_vhost_database` | `mq` | tenant, global | Database of virtual host `/` |
| `amqp.sasl_mechanisms` | `PLAIN,AMQPLAIN,EXTERNAL` | tenant, global | Offered SASL mechanisms |
| `amqp.heartbeat_seconds` | 60 | tenant, global | Heartbeat the server proposes |
| `amqp.frame_max` | 131072 | dc, global | Largest frame |
| `amqp.channel_max` | 2047 | dc, global | Channels per connection |
| `amqp.max_message_bytes` | 16777216 | object, database, tenant, cluster, dc, global | Largest message |
| `amqp.publish_timeout_ms` | 10000 | tenant, global | Nack an unconfirmed publish after this |
| `amqp.consumer_timeout_ms` | 1800000 | object, database, tenant, cluster, dc, global | Unacknowledged delivery limit; 0 is none |
| `amqp.max_unacked_per_consumer` | 65535 | tenant, global | Cap for prefetch 0 |
| `amqp.quota_publish_bytes_per_sec` | 0 | tenant, global | Publish quota; 0 is none |
| `amqp10.link_credit` | 256 | tenant, global | Credit granted to AMQP 1.0 publishing links |
| `amqp10.legacy_addresses` | false | object, database, tenant, global | Accept bare queue names and AMQP address v1 forms |
| `queue.default_type` | `classic` | object, database, tenant, global | Queue type when none is declared |
| `queue.delivery_limit_default` | 20 | object, database, tenant, global | Quorum queues' delivery limit |
| `queue.confirm_mode` | `commit` | object, database, tenant, global | Confirm after the commit, or after a replica applied it (`replica`) |
| `queue.delay_max_ms` | 4294967295 | tenant, global | Longest `x-delay` |
| `queue.change_exchange` | empty | object | Publish the table's changes to this exchange |
| `queue.forwarder` | `feed` | object, database, tenant, global | Forward transactional publishes from the change feed in commit order (`feed`), or by polling in insert order (`poll`) |
| `queue.forward_fallback_ms` | 2000 | object, database, tenant, global | Age after which a polling fallback forwards a transactional publish |
| `queue.owner_cache_mb` | 64 | object, database, tenant, cluster, dc, global | Ready messages a queue owner keeps in memory |
| `queue.write_batch_max_rows` | 500 | dc, global | Rows per owner write batch |
| `queue.takeover_grace_ms` | 2000 | dc, global | Re-attachment window after an owner takeover |
| `queue.owner_move_after_seconds` | 60 | dc, global | Consumers on another gateway this long move the owner there |
| `queue.owner_move_cooldown_seconds` | 300 | dc, global | Least time between two moves of one queue's owner |

See [Settings](../reference/settings.md) for bounds, and [Metrics](../reference/metrics.md) for the `sprig_queue_*` and `sprig_amqp_*` metrics.

## Errors

AMQP clients receive the AMQP reply code (0-9-1) or condition (1.0), with SprigDB's number and name in the text. SQL and REST callers receive the number.

| Code | Name | AMQP | When |
|---|---|---|---|
| 39250 | `ER_SPRIG_MQ_NOT_FOUND` | 404 | Unknown virtual host, exchange or queue |
| 39251 | `ER_SPRIG_MQ_PRECONDITION` | 406 | Redeclare with other arguments, unknown delivery tag, bad properties |
| 39252 | `ER_SPRIG_MQ_RESOURCE_LOCKED` | 405 | An exclusive queue or consumer held by another connection |
| 39253 | `ER_SPRIG_MQ_ACCESS_REFUSED` | 403 | Permission denied |
| 39254 | `ER_SPRIG_MQ_NOT_IMPLEMENTED` | 540 | An unsupported method, flag or argument |
| 39255 | `ER_SPRIG_MQ_UNROUTABLE` | 312 | A mandatory publish from SQL or logic reached no queue |
| 39256 | `ER_SPRIG_MQ_QUEUE_FULL` | nack | `reject-publish` overflow |
| 39257 | `ER_SPRIG_MQ_MESSAGE_TOO_LARGE` | 406 | Above `amqp.max_message_bytes` |
| 39258 | `ER_SPRIG_MQ_ALREADY_SETTLED` | | An acknowledgement in a transaction of a message already settled or whose lease ended; the transaction rolls back |
| 39259 | `ER_SPRIG_MQ_DELIVERY_TOKEN_INVALID` | | A malformed, forged or other tenant's delivery token |
| 39260 | `ER_SPRIG_MQ_OWNER_MOVED` | nack | The queue's owner can't be reached; retryable (SQLSTATE `40001`) |
| 39261 | `ER_SPRIG_MQ_CONFIRM_TIMEOUT` | nack | Not committed within `amqp.publish_timeout_ms` |
| 39262 | `ER_SPRIG_MQ_CONSUMER_TIMEOUT` | cancel | Unacknowledged past the consumer timeout |
| 39264 | `ER_SPRIG_MQ_DELAY_OUT_OF_RANGE` | 406 | `x-delay` above `queue.delay_max_ms` |
| 39266 | `ER_SPRIG_MQ_QUOTA` | `connection.blocked` | Over the tenant's publish quota |
| 39268 | `ER_SPRIG_AMQP_FRAME_ERROR` | 501 to 505 | A malformed frame or command, or a method on a channel that isn't open |
| 39269 | `ER_SPRIG_AMQP_SASL_FAILED` | 403 | Authentication failed |
| 39270 | `ER_SPRIG_AMQP_PROTOCOL_HEADER` | | An unknown protocol header; the server answers `AMQP 0 0 9 1` and closes |
| 39271 | `ER_SPRIG_AMQP_CHANNEL_LIMIT` | 530 | Above `channel_max` |
| 39275 | `ER_SPRIG_MQ_DIRECT_REPLY_TO` | 406 | Direct reply-to misuse |
| 39277 | `ER_SPRIG_MQ_DEAD_LETTER_CYCLE` | | A message dropped in a dead-letter cycle (logged) |

## Not supported

- **Stream queues and super streams** (`x-queue-type = stream`, `x-stream-offset`): 540. Use [event streams](streams.md).
- **The RabbitMQ management HTTP API** (port 15672) and its web UI. Inspect queues with `sprig.mq_info` and `sprig.mq_browse`, `GET /v1/databases/{database}/mq`, or [Studio](../tools/studio.md).
- **`CREATE EXCHANGE`, `CREATE QUEUE` and `BIND` in SQL for ordinary objects.** These statements declare per-tenant definitions only; declare ordinary topology from an AMQP client.
- **SASL `EXTERNAL`** (client certificate logins), although it is listed in `amqp.sasl_mechanisms`: the AMQP listeners don't pass client certificates to the login, so it is never offered, and accounts with `REQUIRE X509`, `SUBJECT` or `ISSUER` can't log in over AMQP.
- **`immediate` publishes**, `prefetch_size` other than 0, `basic.recover` without requeue and `channel.flow` with `active = false`: 540, as in RabbitMQ.
- **Atomic `tx.commit` across queues.** Use SQL transactions for atomic publish and acknowledge.
- **AMQP 1.0 transactions**, link recovery and the `second` receiver settle mode, and the 1.0 delivery-time annotations (`x-opt-delivery-time`, `x-opt-delivery-delay`).
- **Lanes** (`x-sprig-lanes`): accepted and stored, but every queue is served as one lane.
- **Publishing up the tenant tree.** A descendant's own sessions can't publish into an ancestor's exchange, although `queue.descendant_publish` and `queue.descendant_key` are registered settings.
- **Publishing from MongoDB** clients.

## Related

- [Event streams](streams.md): events that every reader sees
- [Sinks](sinks.md): deliver queue messages to external systems
- [Logic](logic.md): triggers on queues and `db.mq.*`
- [AMQP](../connect/amqp.md): connecting RabbitMQ clients
- [Streams, queues and sinks in SQL](../sql/streams-queues-sinks.md): statement reference
