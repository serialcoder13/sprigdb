// Kafka scenarios with franz-go's kgo client (pinned in go.mod): keyed
// produce with headers, a consumer group read with commits, a second read
// of the same group, per-key order. Prints one JSON object of observations;
// deploy/compose/ci/outcomes.py compares the reference's with SprigDB's
// (keys ending in _seconds are timings).
//
//	go run . <bootstrap> <user|-> <password> <topic> <group>
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const n = 300

func opts(boot, user, pass string) []kgo.Opt {
	o := []kgo.Opt{kgo.SeedBrokers(boot)}
	if user != "-" {
		o = append(o, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()))
	}
	return o
}

func main() {
	boot, user, pass, topic, group := os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5]
	out := map[string]any{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	p, err := kgo.NewClient(append(opts(boot, user, pass), kgo.DefaultProduceTopic(topic), kgo.RequiredAcks(kgo.AllISRAcks()))...)
	if err != nil {
		fail(out, "client", err)
	}
	start := time.Now()
	parts := map[string]map[int32]bool{}
	offs := map[string][]int64{}
	produced := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%d", i%30)
		r := &kgo.Record{Key: []byte(key), Value: []byte(fmt.Sprintf("v%d", i)),
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte(strconv.Itoa(i))}}}
		res := p.ProduceSync(ctx, r)
		if res.FirstErr() != nil {
			out["produce_error"] = res.FirstErr().Error()
			break
		}
		produced++
		if parts[key] == nil {
			parts[key] = map[int32]bool{}
		}
		parts[key][r.Partition] = true
		offs[key] = append(offs[key], r.Offset)
	}
	p.Close()
	out["produce_seconds"] = time.Since(start).Seconds()
	out["produced"] = produced
	stable, increasing := true, true
	for k := range parts {
		stable = stable && len(parts[k]) == 1
		increasing = increasing && sort.SliceIsSorted(offs[k], func(a, b int) bool { return offs[k][a] < offs[k][b] })
	}
	out["key_partition_stable"] = stable
	out["key_offsets_increase"] = increasing

	read := func(want int, wait time.Duration) []*kgo.Record {
		c, err := kgo.NewClient(append(opts(boot, user, pass), kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())...)
		if err != nil {
			fail(out, "client", err)
		}
		defer c.Close()
		var got []*kgo.Record
		end := time.Now().Add(wait)
		for len(got) < want && time.Now().Before(end) {
			pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
			fs := c.PollFetches(pctx)
			pcancel()
			fs.EachRecord(func(r *kgo.Record) { got = append(got, r) })
		}
		if err := c.CommitUncommittedOffsets(ctx); err != nil {
			out["commit_error"] = err.Error()
		}
		return got
	}
	start = time.Now()
	got := read(n, 90*time.Second)
	out["group_read_seconds"] = time.Since(start).Seconds()
	out["group_read"] = len(got)
	order, headers := true, true
	last := map[string]int{}
	for _, r := range got {
		v, _ := strconv.Atoi(string(r.Value[1:]))
		if prev, ok := last[string(r.Key)]; ok && v < prev {
			order = false
		}
		last[string(r.Key)] = v
		headers = headers && len(r.Headers) == 1 && string(r.Headers[0].Value) == string(r.Value[1:])
	}
	out["per_key_order"] = order
	out["headers_kept"] = headers
	out["second_run_reads"] = len(read(1, 8*time.Second))
	emit(out)
}

func fail(out map[string]any, what string, err error) {
	out[what+"_error"] = err.Error()
	emit(out)
	os.Exit(0)
}

func emit(out map[string]any) {
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}
