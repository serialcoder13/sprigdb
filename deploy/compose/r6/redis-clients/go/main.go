// go-redis v9.7.0 against SprigDB and a reference Redis: the same scenario
// on both (go-redis speaks RESP3 by default), every result compared.
// Usage: go run . REF SPRIG USER PASSWORD
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

type rec struct {
	name string
	val  string
}

func scenario(ctx context.Context, r, other *redis.Client, p string) []rec {
	var out []rec
	add := func(name string, v interface{}, err error) {
		if err != nil && err != redis.Nil {
			v = "ERR " + err.Error()
		}
		b, _ := json.Marshal(v)
		out = append(out, rec{name, string(b)})
	}
	if ks, _ := r.Keys(ctx, p+"*").Result(); len(ks) > 0 {
		r.Del(ctx, ks...)
	}
	v1, e := r.Set(ctx, p+"s", "hello", 0).Result()
	add("set", v1, e)
	v2, e := r.Get(ctx, p+"s").Result()
	add("get", v2, e)
	v3, e := r.SetEx(ctx, p+"t", "v", 100*time.Second).Result()
	add("setex", v3, e)
	ttl, e := r.TTL(ctx, p+"t").Result()
	add("ttl_pos", ttl > 90*time.Second, e)
	n, e := r.IncrBy(ctx, p+"n", 42).Result()
	add("incrby", n, e)
	f, e := r.IncrByFloat(ctx, p+"f", 1.5).Result()
	add("incrbyfloat", f, e)
	mg, e := r.MGet(ctx, p+"s", p+"missing").Result()
	add("mget", mg, e)
	h, e := r.HSet(ctx, p+"h", "f1", "v1", "f2", "v2").Result()
	add("hset", h, e)
	ha, e := r.HGetAll(ctx, p+"h").Result()
	add("hgetall", ha, e)
	l, e := r.RPush(ctx, p+"l", "a", "b", "c").Result()
	add("rpush", l, e)
	lr, e := r.LRange(ctx, p+"l", 0, -1).Result()
	add("lrange", lr, e)
	sa, e := r.SAdd(ctx, p+"x", "1", "2", "3").Result()
	add("sadd", sa, e)
	sm, e := r.SMembers(ctx, p+"x").Result()
	sort.Strings(sm)
	add("smembers", sm, e)
	za, e := r.ZAdd(ctx, p+"z", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2.5, Member: "b"}).Result()
	add("zadd", za, e)
	zr, e := r.ZRangeWithScores(ctx, p+"z", 0, -1).Result()
	add("zrange_withscores", zr, e)
	zs, e := r.ZScore(ctx, p+"z", "b").Result()
	add("zscore", zs, e)
	ty, e := r.Type(ctx, p+"z").Result()
	add("type", ty, e)
	ex, e := r.Expire(ctx, p+"s", 100*time.Second).Result()
	add("expire", ex, e)
	var cursor uint64
	var seen []string
	for {
		ks, c, err := r.Scan(ctx, cursor, p+"*", 3).Result()
		if err != nil {
			add("scan", nil, err)
			break
		}
		seen = append(seen, ks...)
		cursor = c
		if c == 0 {
			break
		}
	}
	uniq := map[string]bool{}
	for _, k := range seen {
		uniq[k] = true
	}
	keys := make([]string, 0, len(uniq))
	for k := range uniq {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	add("scan", keys, nil)
	cmds, e := r.Pipelined(ctx, func(pl redis.Pipeliner) error {
		for i := 0; i < 5; i++ {
			pl.Set(ctx, fmt.Sprintf("%spl%d", p, i), i, 0)
			pl.Get(ctx, fmt.Sprintf("%spl%d", p, i))
		}
		return nil
	})
	var pv []string
	for _, c := range cmds {
		pv = append(pv, c.String())
	}
	add("pipeline", pv, e)
	tx, e := r.TxPipelined(ctx, func(pl redis.Pipeliner) error {
		pl.Incr(ctx, p+"tx")
		pl.Incr(ctx, p+"tx")
		return nil
	})
	var tv []string
	for _, c := range tx {
		tv = append(tv, c.String())
	}
	add("multi_exec", tv, e)
	// WATCH: another client changes the key first.
	werr := r.Watch(ctx, func(t *redis.Tx) error {
		other.Set(ctx, p+"w", "changed", 0)
		_, err := t.TxPipelined(ctx, func(pl redis.Pipeliner) error {
			pl.Set(ctx, p+"w", "mine", 0)
			return nil
		})
		return err
	}, p+"w")
	add("watch", werr == redis.TxFailedErr, nil)
	ev, e := r.Eval(ctx, "return {KEYS[1], ARGV[1], redis.call('INCR', KEYS[1])}", []string{p + "e"}, "arg").Result()
	add("eval", ev, e)
	sc := redis.NewScript("return redis.call('GET', KEYS[1])")
	sv, e := sc.Run(ctx, r, []string{p + "s"}).Result()
	add("script_run", sv, e)
	ga, e := r.GeoAdd(ctx, p+"g", &redis.GeoLocation{Name: "Palermo", Longitude: 13.361389, Latitude: 38.115556},
		&redis.GeoLocation{Name: "Catania", Longitude: 15.087269, Latitude: 37.502669}).Result()
	add("geoadd", ga, e)
	gd, e := r.GeoDist(ctx, p+"g", "Palermo", "Catania", "km").Result()
	add("geodist", gd, e)
	// Pub/sub.
	ps := r.Subscribe(ctx, p+"chan")
	_, e = ps.Receive(ctx)
	add("subscribe", e == nil, nil)
	pn, e := other.Publish(ctx, p+"chan", "hi").Result()
	add("publish", pn, e)
	m, e := ps.ReceiveMessage(ctx)
	if e == nil {
		add("message", []string{m.Channel, m.Payload}, nil)
	} else {
		add("message", nil, e)
	}
	ps.Close()
	// A blocked pop woken by another connection.
	go func() {
		time.Sleep(300 * time.Millisecond)
		other.RPush(ctx, p+"q", "job")
	}()
	t := time.Now()
	bp, e := r.BLPop(ctx, 5*time.Second, p+"q").Result()
	add("blpop", bp, e)
	add("blpop_fast", time.Since(t) < 3*time.Second, nil)
	if ks, _ := r.Keys(ctx, p+"*").Result(); len(ks) > 0 {
		r.Del(ctx, ks...)
	}
	return out
}

func main() {
	ref, sprig, user, pw := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	ctx := context.Background()
	p := "r6c:go:"
	a := redis.NewClient(&redis.Options{Addr: ref})
	a2 := redis.NewClient(&redis.Options{Addr: ref})
	b := redis.NewClient(&redis.Options{Addr: sprig, Username: user, Password: pw})
	b2 := redis.NewClient(&redis.Options{Addr: sprig, Username: user, Password: pw})
	want := scenario(ctx, a, a2, p)
	got := scenario(ctx, b, b2, p)
	diffs := 0
	for i, w := range want {
		if i >= len(got) || got[i].val != w.val {
			diffs++
			g := ""
			if i < len(got) {
				g = got[i].val
			}
			fmt.Printf("DIFF %s: redis %s sprigdb %s\n", w.name, w.val, g)
		}
	}
	fmt.Printf("go-redis v9.7.0 (RESP3): %d checks, %d differences\n", len(want), diffs)
	if diffs > 0 || len(want) != len(got) {
		os.Exit(1)
	}
}
