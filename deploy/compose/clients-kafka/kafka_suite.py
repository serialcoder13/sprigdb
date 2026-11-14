# Kafka client checks with kafka-python: produce, groups, commits, seeks,
# timestamps, headers, gzip. Prints one JSON object of observations; run.sh
# runs it against an Apache Kafka reference and SprigDB and
# deploy/compose/ci/outcomes.py compares (keys ending in _seconds are
# timings, not compared).
# Usage: python kafka_suite.py bootstrap user password topic group
# (user "-": PLAINTEXT, no SASL, for the reference).
import json, sys, time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

boot, user, pw, topic, group = sys.argv[1:6]
sasl = dict(api_version=(2, 5, 0))
if user != "-":
    sasl.update(security_protocol='SASL_PLAINTEXT', sasl_mechanism='SCRAM-SHA-512',
                sasl_plain_username=user, sasl_plain_password=pw)
out = {}
N = 300
p = KafkaProducer(bootstrap_servers=boot, acks='all', compression_type='gzip', linger_ms=20, **sasl)
t = time.time()
futs = [p.send(topic, key=f"k{i % 30}".encode(), value=f"v{i}".encode(),
               headers=[("h", f"{i}".encode())]) for i in range(N)]
meta = [f.get(timeout=60) for f in futs]
p.flush()
out["produce_seconds"] = round(time.time() - t, 2)
out["produced"] = len(meta)
out["offsets_known"] = all(m.offset >= 0 for m in meta)
# Same key, same partition, increasing offsets.
by_key = {}
for i, m in enumerate(meta):
    by_key.setdefault(f"k{i % 30}", []).append((m.partition, m.offset))
out["key_partition_stable"] = all(len({p for p, _ in v}) == 1 for v in by_key.values())
out["key_offsets_increase"] = all([o for _, o in v] == sorted(o for _, o in v) for v in by_key.values())
p.close()

def consume(group, want, secs=60, reset='earliest'):
    c = KafkaConsumer(topic, group_id=group, bootstrap_servers=boot, auto_offset_reset=reset,
                      enable_auto_commit=False, consumer_timeout_ms=3000, **sasl)
    got = []
    end = time.time() + secs
    while len(got) < want and time.time() < end:
        for tp, recs in c.poll(timeout_ms=1000).items():
            got.extend(recs)
    c.commit()
    c.close()
    return got

t = time.time()
got = consume(group, N)
out["group_read"] = len(got)
out["group_read_seconds"] = round(time.time() - t, 2)
vals = {}
for r in got:
    vals.setdefault(r.key.decode(), []).append(int(r.value.decode()[1:]))
out["per_key_order"] = all(v == sorted(v) for v in vals.values())
out["headers_kept"] = all(dict(r.headers).get("h") == r.value[1:] for r in got)
out["timestamps_present"] = all(r.timestamp > 0 for r in got)
out["second_run_reads"] = len(consume(group, 1, secs=6))
# Assign, seek, end offsets, offsets for times.
c = KafkaConsumer(bootstrap_servers=boot, enable_auto_commit=False, consumer_timeout_ms=3000, **sasl)
parts = sorted(c.partitions_for_topic(topic) or [])
out["partitions"] = len(parts)
tps = [TopicPartition(topic, x) for x in parts]
c.assign(tps)
ends = c.end_offsets(tps)
begins = c.beginning_offsets(tps)
out["end_minus_begin"] = sum(ends[tp] - begins[tp] for tp in tps)
c.seek_to_beginning()
n = 0
end = time.time() + 30
while n < N and time.time() < end:
    n += sum(len(v) for v in c.poll(timeout_ms=1000).values())
out["seek_to_beginning_reads"] = n
ts = int(time.time() * 1000) - 3600_000
oft = c.offsets_for_times({tp: ts for tp in tps})
out["offsets_for_times_start"] = all(oft[tp] is not None and oft[tp].offset == begins[tp] for tp in tps)
c.close()
print(json.dumps(out, sort_keys=True))
