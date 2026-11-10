"""redis-py 5.2.1 against SprigDB and a reference Redis: the same scenario on
both, every result compared. Usage: python suite_py.py REF_HOST:PORT SPRIG_HOST:PORT USER PASSWORD"""
import json
import sys
import threading
import time

import redis


def scenario(r, other, p):
    out = []

    def rec(name, v):
        if isinstance(v, bytes):
            v = v.decode("utf-8", "replace")
        out.append((name, json.dumps(v, sort_keys=True, default=str)))

    keys = list(r.scan_iter(match=p + "*", count=100))
    if keys:
        r.delete(*keys)
    rec("set", r.set(p + "s", "hello"))
    rec("get", r.get(p + "s"))
    rec("set_ex", r.set(p + "t", "v", ex=100))
    rec("ttl_pos", r.ttl(p + "t") > 90)
    rec("incr", r.incr(p + "n"))
    rec("incrby", r.incrby(p + "n", 41))
    rec("incrbyfloat", r.incrbyfloat(p + "f", 1.5))
    rec("mset", r.mset({p + "a": "1", p + "b": "2"}))
    rec("mget", r.mget(p + "a", p + "b", p + "missing"))
    rec("append", r.append(p + "s", " world"))
    rec("getrange", r.getrange(p + "s", 0, 4))
    rec("setnx", r.setnx(p + "s", "x"))
    rec("hset", r.hset(p + "h", mapping={"f1": "v1", "f2": "v2", "n": "5"}))
    rec("hget", r.hget(p + "h", "f1"))
    rec("hgetall", r.hgetall(p + "h"))
    rec("hincrby", r.hincrby(p + "h", "n", 3))
    rec("hdel", r.hdel(p + "h", "f2"))
    rec("hlen", r.hlen(p + "h"))
    rec("rpush", r.rpush(p + "l", "a", "b", "c"))
    rec("lpush", r.lpush(p + "l", "z"))
    rec("lrange", r.lrange(p + "l", 0, -1))
    rec("lpop", r.lpop(p + "l"))
    rec("lindex", r.lindex(p + "l", 1))
    rec("llen", r.llen(p + "l"))
    rec("sadd", r.sadd(p + "x", "1", "2", "3"))
    rec("sadd2", r.sadd(p + "y", "2", "3", "4"))
    rec("smembers", sorted(m for m in r.smembers(p + "x")))
    rec("sinter", sorted(m for m in r.sinter(p + "x", p + "y")))
    rec("sismember", r.sismember(p + "x", "2"))
    rec("zadd", r.zadd(p + "z", {"a": 1, "b": 2.5, "c": -1}))
    rec("zrange", r.zrange(p + "z", 0, -1, withscores=True))
    rec("zrank", r.zrank(p + "z", "b"))
    rec("zincrby", r.zincrby(p + "z", 2, "a"))
    rec("zrangebyscore", r.zrangebyscore(p + "z", 0, 10))
    rec("zscore", r.zscore(p + "z", "b"))
    rec("type", [r.type(p + k) for k in ["s", "h", "l", "x", "z"]])
    rec("expire", r.expire(p + "a", 100))
    rec("persist", r.persist(p + "a"))
    rec("rename", r.rename(p + "b", p + "b2"))
    rec("exists", r.exists(p + "b", p + "b2"))
    rec("scan", sorted(k for k in r.scan_iter(match=p + "*", count=3)))
    pipe = r.pipeline(transaction=False)
    for i in range(10):
        pipe.set(p + f"pl{i}", i)
    for i in range(10):
        pipe.get(p + f"pl{i}")
    rec("pipeline", pipe.execute())
    tx = r.pipeline(transaction=True)
    tx.incr(p + "tx")
    tx.incr(p + "tx")
    tx.lpush(p + "txl", "q")
    rec("multi_exec", tx.execute())
    # WATCH conflict: another client changes the key.
    with r.pipeline() as w:
        w.watch(p + "w")
        other.set(p + "w", "changed")
        w.multi()
        w.set(p + "w", "mine")
        try:
            w.execute()
            rec("watch", "committed")
        except redis.WatchError:
            rec("watch", "aborted")
    rec("eval", r.eval("return {KEYS[1], ARGV[1], redis.call('INCR', KEYS[1])}", 1, p + "e", "arg"))
    script = r.register_script("return redis.call('GET', KEYS[1])")
    rec("evalsha", script(keys=[p + "s"]))
    rec("geoadd", r.geoadd(p + "g", (13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania")))
    rec("geodist", r.geodist(p + "g", "Palermo", "Catania", unit="km"))
    # Pub/sub across connections.
    ps = r.pubsub()
    ps.subscribe(p + "chan")
    first = ps.get_message(timeout=2)
    rec("subscribe", first["type"] if first else None)
    rec("publish", other.publish(p + "chan", "hi"))
    msg = None
    for _ in range(20):
        msg = ps.get_message(timeout=0.5)
        if msg and msg["type"] == "message":
            break
    rec("message", msg["data"] if msg else None)
    ps.close()
    # A blocked pop woken by another connection.
    threading.Timer(0.3, lambda: other.rpush(p + "q", "job")).start()
    t = time.time()
    rec("blpop", r.blpop([p + "q"], timeout=5))
    rec("blpop_fast", time.time() - t < 3)
    keys = list(r.scan_iter(match=p + "*", count=100))
    if keys:
        r.delete(*keys)
    return out


def client(addr, user=None, password=None, protocol=2):
    host, port = addr.rsplit(":", 1)
    return redis.Redis(host=host, port=int(port), username=user, password=password, decode_responses=True, protocol=protocol)


def main():
    ref, sprig, user, pw = sys.argv[1:5]
    p = "r6c:py:"
    bad = False
    for proto in (2, 3):
        want = scenario(client(ref, protocol=proto), client(ref, protocol=proto), p)
        got = scenario(client(sprig, user, pw, proto), client(sprig, user, pw, proto), p)
        diffs = [(a, b) for a, b in zip(want, got) if a != b]
        for a, b in diffs:
            print(f"DIFF RESP{proto} {a[0]}: redis {a[1]} sprigdb {b[1]}")
        print(f"redis-py {redis.__version__} RESP{proto}: {len(want)} checks, {len(diffs)} differences")
        bad = bad or bool(diffs) or len(want) != len(got)
    sys.exit(1 if bad else 0)


main()
