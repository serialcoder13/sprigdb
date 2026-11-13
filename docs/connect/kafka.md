---
title: Kafka
audience: [user, dba]
sidebar:
  order: 6
---

# Kafka

SprigDB speaks the Kafka protocol, so Kafka producers, consumers and admin tools can write and read SprigDB's event streams. Every gateway acts as a broker. Use it when your application already uses a Kafka client, or when you want a durable event log that SQL can also write and read.

This page covers connecting. For streams themselves (partitions, retention, compaction, change topics, exactly-once processing with SQL), see [Event streams](../data-models/streams.md).

## Enable the listener

The Kafka listener is off by default.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `listener.kafka.enabled` | bool | `false` | Serve Kafka |
| `listener.kafka.bind` | text | `0.0.0.0:9092` | Address and port |
| `listener.kafka.tls` | text | `required` | `required`, `optional` or `off` |
| `listener.kafka.cert` | text | `default` | Certificate bundle |
| `listener.kafka.client_cert` | text | `off` | Client certificates: `off`, `optional` or `required` (see [Client certificates](../security/tls.md#client-certificates)) |

```sh
sprigctl settings set listener.kafka.enabled true --scope global --reason "Kafka clients"
```

Kafka settings:

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `kafka.sasl_mechanisms` | text | `SCRAM-SHA-512,SCRAM-SHA-256,PLAIN` | Mechanisms offered, in order |
| `kafka.advertised_host` | text | empty | Host this gateway gives in Metadata; empty for its own address |
| `kafka.fetch_max_wait_ms` | int | `500` | Cap on a fetch's wait for new records |
| `kafka.group_initial_rebalance_delay_ms` | int | `3000` | Delay of an empty group's first rebalance |
| `kafka.group_session_timeout_max_ms` | int | `300000` | Largest session timeout a group member may ask for |
| `stream.produce_wait_ms` | int | `2000` | How long a produce waits for its records' offsets |

### Brokers and addresses

Clients see every ready gateway in the data centre that runs the Kafka listener as a broker. Each gateway's broker id is a small integer that is unique among the data centre's live gateways. Each partition's leader is one of those gateways, chosen by hashing, but any gateway can serve any request.

Metadata gives each broker's address. If clients reach the gateways through other names (a load balancer, NAT, or a container network), set `kafka.advertised_host` so the addresses in Metadata are ones the client can reach; otherwise the client connects to the bootstrap server and then fails to reach the brokers it was told about.

## Log in

Authentication is required. Before it, a client may send only ApiVersions, SaslHandshake and SaslAuthenticate; anything else closes the connection.

| Mechanism | Supported | Notes |
|---|---|---|
| `SCRAM-SHA-512` | yes | The first offered |
| `SCRAM-SHA-256` | yes | |
| `PLAIN` | yes | Only over TLS, unless the listener's TLS is `off` |
| `OAUTHBEARER`, `GSSAPI` | no | `UNSUPPORTED_SASL_MECHANISM` |
| Mutual TLS without SASL | no | The listener can ask for client certificates (`listener.kafka.client_cert`), but a SASL login is still needed |

Use your SprigDB account. The user name names the tenant: `acme.app` is user `app` of tenant `acme`. On a tenant endpoint (a TLS host name such as `acme.db.example.com` under `listener.tenant_sni_suffixes`), log in as plain `app`; see [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name). With TLS, the security protocol is `SASL_SSL`; with the listener's TLS off, `SASL_PLAINTEXT`.

A failed login answers `SASL_AUTHENTICATION_FAILED` with `Authentication failed (...)` and closes the connection. The login runs the same checks as every other protocol: account locks, expired passwords, `REQUIRE SSL`, `auth.require_tls`, failure delays and the per-address limit. An account with `REQUIRE X509` logs in when the connection presented a client certificate the listener verified (set `ssl.keystore.location` in the client, and `listener.kafka.client_cert` on the gateway). Accounts with `REQUIRE SUBJECT` or `REQUIRE ISSUER` can't log in over Kafka.

SCRAM needs the account's SCRAM verifiers. Every password set in SprigDB stores them. An account whose password was set before that gets them at its next successful login that sees the password, for example a `PLAIN` login over TLS. See [Connect to SprigDB](overview.md#one-user-every-protocol).

## Topics are streams

A topic is a stream, named `<database>.<stream>`: stream `order_events` in database `shop` is topic `shop.order_events`. The name is split at the first dot. A topic name without a database part fails with `INVALID_TOPIC_EXCEPTION`, "a topic is named <database>.<stream>".

Topics aren't created by producing to them. Create a stream with SQL, or with CreateTopics from a Kafka admin client, which runs the same statement as you:

```sql
CREATE STREAM shop.order_events PARTITIONS 12;
```

```sh
kafka-topics.sh --bootstrap-server db.example.com:9092 --command-config client.properties \
  --create --topic shop.order_events --partitions 12
```

Producing needs `INSERT` on the stream or its database, and consuming needs `SELECT`. A topic you can neither read nor write is invisible to you. A table's change topic, such as `shop.orders.changes`, can be consumed but not produced to.

## Connect

`kcat`:

```sh
kcat -b db.example.com:9092 -X security.protocol=SASL_SSL -X ssl.ca.location=ca.pem \
  -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username=acme.app -X sasl.password='<password>' \
  -P -t shop.order_events -k 7001 <<< '{"type":"order.paid"}'

kcat -b db.example.com:9092 -X security.protocol=SASL_SSL -X ssl.ca.location=ca.pem \
  -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username=acme.app -X sasl.password='<password>' \
  -G billing shop.order_events
```

Java client and the Kafka command-line tools (`client.properties`):

```text
bootstrap.servers=db.example.com:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="acme.app" password="<password>";
ssl.truststore.location=/etc/sprig/truststore.jks
ssl.truststore.password=<truststore-password>
enable.idempotence=true
acks=all
```

Python, with `confluent-kafka`:

```python
from confluent_kafka import Consumer, Producer

conf = {
    "bootstrap.servers": "db.example.com:9092",
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "ca.pem",
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": "acme.app",
    "sasl.password": os.environ["DB_PASSWORD"],
}
p = Producer({**conf, "enable.idempotence": True, "acks": "all"})
p.produce("shop.order_events", key="7001", value='{"type":"order.paid"}')
p.flush()

c = Consumer({**conf, "group.id": "billing", "auto.offset.reset": "earliest"})
c.subscribe(["shop.order_events"])
msg = c.poll(5.0)
```

Go, with franz-go:

```go
client, err := kgo.NewClient(
    kgo.SeedBrokers("db.example.com:9092"),
    kgo.DialTLSConfig(&tls.Config{RootCAs: pool}),
    kgo.SASL(scram.Auth{User: "acme.app", Pass: os.Getenv("DB_PASSWORD")}.AsSha512Mechanism()),
    kgo.ConsumerGroup("billing"),
    kgo.ConsumeTopics("shop.order_events"),
)
```

Node.js, with KafkaJS:

```js
const kafka = new Kafka({
  brokers: ['db.example.com:9092'],
  ssl: { ca: [fs.readFileSync('ca.pem')] },
  sasl: { mechanism: 'scram-sha-512', username: 'acme.app', password: process.env.DB_PASSWORD },
});
```

## What works

| API | Versions | Support |
|---|---|---|
| Produce | 3 to 9 | yes; `acks=1` and `acks=all`, idempotent producers, gzip, snappy, LZ4 and zstd |
| Fetch | 4 to 13 | yes; long poll, both isolation levels; no fetch sessions (clients fall back to full fetches) |
| ListOffsets | 1 to 7 | yes; earliest, latest and by timestamp |
| Metadata | 0 to 12 | yes |
| OffsetCommit, OffsetFetch | 0 to 8 | yes; offsets are stored in MariaDB |
| FindCoordinator | 0 to 4 | Group coordinators only |
| JoinGroup, SyncGroup, Heartbeat, LeaveGroup | 0 to 9, 0 to 5, 0 to 4, 0 to 5 | Kafka's classic group protocol, with static membership |
| DescribeGroups, ListGroups, DeleteGroups | 0 to 5, 0 to 5, 0 to 2 | yes |
| SaslHandshake, SaslAuthenticate | 0 to 1, 0 to 2 | yes |
| ApiVersions | 0 to 4 | yes |
| CreateTopics, DeleteTopics, CreatePartitions | 0 to 7, 0 to 6, 0 to 3 | yes, as stream DDL |
| DescribeConfigs, IncrementalAlterConfigs | 0 to 4, 0 to 1 | Topic configs `retention.ms`, `retention.bytes`, `max.message.bytes`, `cleanup.policy` |
| InitProducerId | 0 to 4 | Idempotent producers only |
| OffsetForLeaderEpoch | 2 to 4 | yes |
| Transactions: AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit | | no; a `transactional.id` is refused |
| ConsumerGroupHeartbeat (the KIP-848 group protocol) | | no; use the classic protocol (`group.protocol=classic`) |
| DeleteRecords, OffsetDelete, AlterConfigs, DescribeCluster, ACL and quota APIs, log-dir APIs | | no |

A request for an API or version that isn't served closes the connection, as a Kafka broker does. ApiVersions advertises exactly the table above, so clients pick versions that work. Client-side partition assignors (range, round-robin, sticky and cooperative-sticky) all work, because the coordinator only relays the leader's assignment.

## How it differs from a Kafka cluster

- **Every acknowledged record is committed in MariaDB.** `acks=1` and `acks=all` mean the same. `acks=0` is refused, because SprigDB always acknowledges after the commit and such a producer reads no answer.
- **No Kafka transactions.** Exactly-once processing works differently: consume, write to tables and commit the offset in one SQL transaction. See [Event streams](../data-models/streams.md#exactly-once-consume-and-write).
- **`read_committed` and `read_uncommitted` read the same records**, since the log holds only committed records.
- **Replication factor and segment settings are ignored.** MariaDB replicates the log; tools that send `min.insync.replicas` or `segment.*` keep working.
- **One replica per partition** appears in Metadata.
- **Records can also come from SQL.** `INSERT` into a stream, or a publish inside a SQL transaction, appears to Kafka consumers once the transaction commits. Keys partition the same way as Kafka's default partitioner, so SQL and Kafka producers put a key on the same partition.

## Errors you may see

SprigDB answers with Kafka's error codes. Where a SprigDB error lies behind one, the error message starts with its number, for example `39940 ER_SPRIG_STREAM_NOT_FOUND: no stream shop.order_events`.

| Kafka error | When |
|---|---|
| `SASL_AUTHENTICATION_FAILED` | Wrong credentials, unknown tenant, or any other refused login |
| `UNSUPPORTED_SASL_MECHANISM` | A mechanism not in `kafka.sasl_mechanisms`, or `PLAIN` without TLS |
| `UNKNOWN_TOPIC_OR_PARTITION` | The stream doesn't exist, or you have no privilege on it (39940) |
| `INVALID_TOPIC_EXCEPTION` | A topic name without a database, or a produce to a change topic (39941) |
| `TOPIC_ALREADY_EXISTS` | CreateTopics for an existing stream (39942) |
| `INVALID_PARTITIONS` | CreatePartitions with fewer partitions (39943) |
| `TOPIC_AUTHORIZATION_FAILED` | Topic administration without the SQL privileges it needs |
| `POLICY_VIOLATION` | Deleting a change topic |
| `INVALID_CONFIG` | A topic config SprigDB doesn't know |
| `MESSAGE_TOO_LARGE` | A record over `stream.max_message_bytes` (39944) |
| `INVALID_RECORD` | A keyless record on a compacted stream (39955), or a batch SprigDB doesn't accept |
| `UNSUPPORTED_COMPRESSION_TYPE` | An unknown compression codec |
| `INVALID_REQUIRED_ACKS` | `acks` other than 0, 1 or -1 |
| `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` | A `transactional.id`, or a transactional batch (39951) |
| `REQUEST_TIMED_OUT` | Offsets weren't assigned within `stream.produce_wait_ms` (39946); the records are committed, and an idempotent retry returns their offsets |
| `OFFSET_OUT_OF_RANGE` | A fetch offset outside the log (39947) |
| `NOT_COORDINATOR` | The group's coordinator moved to another gateway; the client finds it again |
| `KAFKA_STORAGE_ERROR` | A retryable SprigDB error, such as MariaDB being unreachable |
| Connection closed | `acks=0` (39950), an API before logging in, or an API or version that isn't served |

## Related

- [Event streams](../data-models/streams.md)
- [Connect to SprigDB](overview.md)
- [Streams, queues and sinks in SQL](../sql/streams-queues-sinks.md)
- [Accounts and logins](../security/accounts-and-logins.md)
- [Connection problems](../troubleshooting/connections.md)
