"""Turns the Apache Kafka image's JVM tools' output (run.sh jvm) into one
JSON object of observations for deploy/compose/ci/outcomes.py.

    jvm_summary.py <raw-dir>

Reads verifiable-producer.log and verifiable-consumer.log (one JSON event per
line), producer-perf.log and groups.log (kafka-consumer-groups --describe).
"""

import json
import os
import re
import sys


def events(path):
    out = []
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("{"):
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out


def text(path):
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def main():
    raw = sys.argv[1]
    prod = events(os.path.join(raw, "verifiable-producer.log"))
    cons = events(os.path.join(raw, "verifiable-consumer.log"))
    out = {
        "producer_acked": sum(1 for e in prod if e.get("name") == "producer_send_success"),
        "producer_errors": sum(1 for e in prod if e.get("name") == "producer_send_error"),
        "consumer_records": sum(e.get("count", 0) for e in cons if e.get("name") == "records_consumed"),
        "consumer_commits_ok": all(e.get("success") for e in cons if e.get("name") == "offsets_committed"),
        "consumer_assigned": any(e.get("name") == "partitions_assigned" and e.get("partitions") for e in cons),
    }
    perf = text(os.path.join(raw, "producer-perf.log"))
    m = re.findall(r"(\d+) records sent, ([0-9.]+) records/sec", perf)
    out["perf_records_sent"] = int(m[-1][0]) if m else 0
    out["perf_rate"] = float(m[-1][1]) if m else 0.0
    lags = []
    for line in text(os.path.join(raw, "groups.log")).splitlines():
        cols = line.split()
        if len(cols) >= 6 and cols[1].startswith("kt.") and cols[5].isdigit():
            lags.append(int(cols[5]))
    out["group_partitions_described"] = len(lags)
    out["group_lag_total"] = sum(lags)
    print(json.dumps(out, sort_keys=True))


if __name__ == "__main__":
    main()
