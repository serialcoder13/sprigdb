// Jedis 5.2.0 and Lettuce 6.5.1 against SprigDB and a reference Redis: the
// same scenario on both, every result compared. Jedis speaks RESP2 here,
// Lettuce RESP3 (its default). Usage: java -cp 'lib/*:.' Suite REF SPRIG USER PASSWORD
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import java.util.*;
import java.util.concurrent.*;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

public class Suite {
    static List<String[]> jedis(String addr, String user, String pw, String p) throws Exception {
        List<String[]> out = new ArrayList<>();
        String[] hp = addr.split(":");
        DefaultJedisClientConfig.Builder b = DefaultJedisClientConfig.builder();
        if (user != null) b.user(user).password(pw);
        JedisClientConfig cfg = b.build();
        HostAndPort h = new HostAndPort(hp[0], Integer.parseInt(hp[1]));
        try (Jedis r = new Jedis(h, cfg); Jedis other = new Jedis(h, cfg)) {
            Set<String> old = r.keys(p + "*");
            if (!old.isEmpty()) r.del(old.toArray(new String[0]));
            BiConsumer add = (n, v) -> out.add(new String[] {n, String.valueOf(v)});
            add.accept("set", r.set(p + "s", "hello"));
            add.accept("get", r.get(p + "s"));
            add.accept("incr", r.incrBy(p + "n", 42));
            add.accept("incrbyfloat", r.incrByFloat(p + "f", 1.5));
            add.accept("mget", r.mget(p + "s", p + "nope"));
            add.accept("hset", r.hset(p + "h", Map.of("f1", "v1")));
            add.accept("hgetall", new TreeMap<>(r.hgetAll(p + "h")));
            add.accept("rpush", r.rpush(p + "l", "a", "b", "c"));
            add.accept("lrange", r.lrange(p + "l", 0, -1));
            add.accept("sadd", r.sadd(p + "x", "1", "2"));
            add.accept("smembers", new TreeSet<>(r.smembers(p + "x")));
            add.accept("zadd", r.zadd(p + "z", 2.5, "b"));
            add.accept("zincrby", r.zincrby(p + "z", 1, "b"));
            add.accept("zrange", r.zrangeWithScores(p + "z", 0, -1).toString());
            add.accept("type", r.type(p + "z"));
            add.accept("ttl", r.ttl(p + "s"));
            String cur = ScanParams.SCAN_POINTER_START;
            TreeSet<String> seen = new TreeSet<>();
            do {
                ScanResult<String> sr = r.scan(cur, new ScanParams().match(p + "*").count(2));
                seen.addAll(sr.getResult());
                cur = sr.getCursor();
            } while (!cur.equals("0"));
            add.accept("scan", seen);
            Pipeline pl = r.pipelined();
            for (int i = 0; i < 5; i++) pl.set(p + "pl" + i, "" + i);
            Response<String> g = pl.get(p + "pl3");
            pl.sync();
            add.accept("pipeline", g.get());
            Transaction t = r.multi();
            t.incr(p + "tx");
            t.incr(p + "tx");
            add.accept("multi_exec", t.exec());
            r.watch(p + "w");
            other.set(p + "w", "changed");
            Transaction w = r.multi();
            w.set(p + "w", "mine");
            add.accept("watch", w.exec());
            add.accept("eval", r.eval("return {KEYS[1], ARGV[1]}", 1, p + "e", "arg"));
            String sha = r.scriptLoad("return redis.call('GET', KEYS[1])");
            add.accept("evalsha", r.evalsha(sha, 1, p + "s"));
            add.accept("geoadd", r.geoadd(p + "g", 13.361389, 38.115556, "Palermo"));
            // Pub/sub on its own connection.
            BlockingQueue<String> got = new LinkedBlockingQueue<>();
            JedisPubSub sub = new JedisPubSub() {
                @Override public void onMessage(String c, String m) { got.add(c + "=" + m); unsubscribe(); }
                @Override public void onSubscribe(String c, int n) { got.add("subscribed " + n); }
            };
            Thread st = new Thread(() -> { try (Jedis s = new Jedis(h, cfg)) { s.subscribe(sub, p + "chan"); } });
            st.start();
            add.accept("subscribe", got.poll(5, TimeUnit.SECONDS));
            add.accept("publish", other.publish(p + "chan", "hi"));
            add.accept("message", got.poll(5, TimeUnit.SECONDS));
            st.join(5000);
            // A blocked pop woken by another connection.
            new Thread(() -> { try { Thread.sleep(300); try (Jedis o = new Jedis(h, cfg)) { o.rpush(p + "q", "job"); } } catch (Exception e) { } }).start();
            long t0 = System.currentTimeMillis();
            add.accept("blpop", r.blpop(5, p + "q"));
            add.accept("blpop_fast", System.currentTimeMillis() - t0 < 3000);
            Set<String> left = r.keys(p + "*");
            if (!left.isEmpty()) r.del(left.toArray(new String[0]));
        }
        return out;
    }

    interface BiConsumer { void accept(String n, Object v); }

    static List<String[]> lettuce(String addr, String user, String pw, String p) throws Exception {
        List<String[]> out = new ArrayList<>();
        String[] hp = addr.split(":");
        RedisURI.Builder ub = RedisURI.builder().withHost(hp[0]).withPort(Integer.parseInt(hp[1]));
        if (user != null) ub.withAuthentication(user, pw.toCharArray());
        RedisClient client = RedisClient.create(ub.build());
        try (var c1 = client.connect(); var c2 = client.connect(); var ps = client.connectPubSub()) {
            RedisCommands<String, String> r = c1.sync();
            RedisCommands<String, String> other = c2.sync();
            List<String> old = r.keys(p + "*");
            if (!old.isEmpty()) r.del(old.toArray(new String[0]));
            BiConsumer add = (n, v) -> out.add(new String[] {n, String.valueOf(v)});
            add.accept("set", r.set(p + "s", "hello"));
            add.accept("get", r.get(p + "s"));
            add.accept("incr", r.incrby(p + "n", 42));
            add.accept("hset", r.hset(p + "h", Map.of("f1", "v1", "f2", "v2")));
            add.accept("hgetall", new TreeMap<>(r.hgetall(p + "h")));
            add.accept("rpush", r.rpush(p + "l", "a", "b", "c"));
            add.accept("lrange", r.lrange(p + "l", 0, -1));
            add.accept("sadd", r.sadd(p + "x", "1", "2"));
            add.accept("smembers", new TreeSet<>(r.smembers(p + "x")));
            add.accept("zadd", r.zadd(p + "z", 2.5, "b"));
            add.accept("zscore", r.zscore(p + "z", "b"));
            add.accept("zrange", r.zrangeWithScores(p + "z", 0, -1).toString());
            add.accept("type", r.type(p + "h"));
            TreeSet<String> seen = new TreeSet<>();
            ScanIterator<String> it = ScanIterator.scan(r, ScanArgs.Builder.matches(p + "*").limit(2));
            while (it.hasNext()) seen.add(it.next());
            add.accept("scan", seen);
            r.multi();
            r.incr(p + "tx");
            r.incr(p + "tx");
            add.accept("multi_exec", r.exec().stream().toList());
            add.accept("eval", r.eval("return {KEYS[1], ARGV[1]}", ScriptOutputType.MULTI, new String[] {p + "e"}, "arg"));
            String sha = r.scriptLoad("return redis.call('GET', KEYS[1])");
            add.accept("evalsha", r.evalsha(sha, ScriptOutputType.VALUE, p + "s"));
            BlockingQueue<String> got = new LinkedBlockingQueue<>();
            ps.addListener(new RedisPubSubAdapter<>() {
                @Override public void message(String ch, String m) { got.add(ch + "=" + m); }
            });
            ps.sync().subscribe(p + "chan");
            add.accept("publish", other.publish(p + "chan", "hi"));
            add.accept("message", got.poll(5, TimeUnit.SECONDS));
            new Thread(() -> { try { Thread.sleep(300); other.rpush(p + "q", "job"); } catch (Exception e) { } }).start();
            long t0 = System.currentTimeMillis();
            var kv = r.blpop(5, p + "q");
            add.accept("blpop", kv == null ? null : kv.getKey() + "=" + kv.getValue());
            add.accept("blpop_fast", System.currentTimeMillis() - t0 < 3000);
            List<String> left = r.keys(p + "*");
            if (!left.isEmpty()) r.del(left.toArray(new String[0]));
        } finally {
            client.shutdown();
        }
        return out;
    }

    static int compare(String name, List<String[]> want, List<String[]> got) {
        int diffs = 0;
        for (int i = 0; i < want.size(); i++) {
            String g = i < got.size() ? got.get(i)[1] : null;
            if (!want.get(i)[1].equals(g)) {
                diffs++;
                System.out.println("DIFF " + name + " " + want.get(i)[0] + ": redis " + want.get(i)[1] + " sprigdb " + g);
            }
        }
        System.out.println(name + ": " + want.size() + " checks, " + diffs + " differences");
        return diffs + (want.size() == got.size() ? 0 : 1);
    }

    public static void main(String[] a) throws Exception {
        int bad = compare("Jedis 5.2.0 (RESP2)", jedis(a[0], null, null, "r6c:jedis:"), jedis(a[1], a[2], a[3], "r6c:jedis:"));
        bad += compare("Lettuce 6.5.1 (RESP3)", lettuce(a[0], null, null, "r6c:lettuce:"), lettuce(a[1], a[2], a[3], "r6c:lettuce:"));
        System.exit(bad == 0 ? 0 : 1);
    }
}
