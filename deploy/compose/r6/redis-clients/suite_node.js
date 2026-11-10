// ioredis 5.4.2 against SprigDB and a reference Redis: the same scenario on
// both, every result compared. Usage:
//   NODE_PATH=<clients/node/node_modules> node suite_node.js REF SPRIG USER PASSWORD
const Redis = require("ioredis");

async function scenario(r, other, p) {
  const out = [];
  const rec = (name, v) => out.push([name, JSON.stringify(v)]);
  const keys = await r.keys(p + "*");
  if (keys.length) await r.del(...keys);
  rec("set", await r.set(p + "s", "hello"));
  rec("get", await r.get(p + "s"));
  rec("set_ex", await r.set(p + "t", "v", "EX", 100));
  rec("ttl_pos", (await r.ttl(p + "t")) > 90);
  rec("incr", await r.incr(p + "n"));
  rec("incrbyfloat", await r.incrbyfloat(p + "f", 1.5));
  rec("mset", await r.mset(p + "a", "1", p + "b", "2"));
  rec("mget", await r.mget(p + "a", p + "b", p + "missing"));
  rec("append", await r.append(p + "s", " world"));
  rec("hset", await r.hset(p + "h", { f1: "v1", f2: "v2", n: "5" }));
  // Field order is unspecified in Redis (SprigDB orders by a hash).
  rec("hgetall", Object.fromEntries(Object.entries(await r.hgetall(p + "h")).sort()));
  rec("hincrby", await r.hincrby(p + "h", "n", 3));
  rec("hdel", await r.hdel(p + "h", "f2"));
  rec("rpush", await r.rpush(p + "l", "a", "b", "c"));
  rec("lrange", await r.lrange(p + "l", 0, -1));
  rec("lpop", await r.lpop(p + "l"));
  rec("sadd", await r.sadd(p + "x", "1", "2", "3"));
  rec("smembers", (await r.smembers(p + "x")).sort());
  rec("zadd", await r.zadd(p + "z", 1, "a", 2.5, "b", -1, "c"));
  rec("zrange", await r.zrange(p + "z", 0, -1, "WITHSCORES"));
  rec("zrevrank", await r.zrevrank(p + "z", "a"));
  rec("type", await Promise.all(["s", "h", "l", "x", "z"].map((k) => r.type(p + k))));
  rec("expire", await r.expire(p + "a", 100));
  rec("exists", await r.exists(p + "a", p + "nope"));
  // SCAN with a stream.
  const seen = [];
  await new Promise((resolve) => {
    const st = r.scanStream({ match: p + "*", count: 3 });
    st.on("data", (ks) => seen.push(...ks));
    st.on("end", resolve);
  });
  rec("scan", [...new Set(seen)].sort());
  // Auto-pipelined and explicit pipelines, MULTI/EXEC.
  const pl = r.pipeline();
  for (let i = 0; i < 10; i++) pl.set(p + "pl" + i, String(i));
  for (let i = 0; i < 10; i++) pl.get(p + "pl" + i);
  rec("pipeline", await pl.exec());
  rec("multi", await r.multi().incr(p + "tx").incr(p + "tx").lpush(p + "txl", "q").exec());
  rec("eval", await r.eval("return {KEYS[1], ARGV[1], redis.call('INCR', KEYS[1])}", 1, p + "e", "arg"));
  r.defineCommand("getit", { numberOfKeys: 1, lua: "return redis.call('GET', KEYS[1])" });
  rec("defined_command", await r.getit(p + "s"));
  rec("geoadd", await r.geoadd(p + "g", 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania"));
  rec("geodist", await r.geodist(p + "g", "Palermo", "Catania", "km"));
  // Pub/sub on a duplicate connection.
  const sub = r.duplicate();
  const got = new Promise((resolve) => sub.on("message", (ch, m) => resolve([ch, m])));
  rec("subscribe", await sub.subscribe(p + "chan"));
  rec("publish", await other.publish(p + "chan", "hi"));
  rec("message", await got);
  sub.disconnect();
  // A blocked pop woken by another connection.
  const blocker = r.duplicate();
  setTimeout(() => other.rpush(p + "q", "job"), 300);
  const t = Date.now();
  rec("blpop", await blocker.blpop(p + "q", 5));
  rec("blpop_fast", Date.now() - t < 3000);
  blocker.disconnect();
  const left = await r.keys(p + "*");
  if (left.length) await r.del(...left);
  return out;
}

function client(addr, user, password) {
  const [host, port] = addr.split(":");
  return new Redis({ host, port: Number(port), username: user, password, lazyConnect: false, maxRetriesPerRequest: 1 });
}

(async () => {
  const [ref, sprig, user, pw] = process.argv.slice(2);
  const p = "r6c:node:";
  const a = [client(ref), client(ref)];
  const b = [client(sprig, user, pw), client(sprig, user, pw)];
  const want = await scenario(a[0], a[1], p);
  const got = await scenario(b[0], b[1], p);
  let diffs = 0;
  want.forEach((w, i) => {
    if (!got[i] || got[i][1] !== w[1]) {
      diffs++;
      console.log(`DIFF ${w[0]}: redis ${w[1]} sprigdb ${got[i] && got[i][1]}`);
    }
  });
  console.log(`ioredis ${require("ioredis/package.json").version}: ${want.length} checks, ${diffs} differences`);
  [...a, ...b].forEach((c) => c.disconnect());
  process.exit(diffs || want.length !== got.length ? 1 : 0);
})().catch((e) => {
  console.error(e);
  process.exit(2);
});
