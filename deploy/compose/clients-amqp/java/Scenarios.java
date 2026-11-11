// AMQP 0-9-1 scenarios with RabbitMQ's Java client (amqp-client, pinned in
// pom.xml), run as a single-file program: java -cp 'libs/*' Scenarios.java
// <host> <port> <user> <password> <vhost> <prefix>. Prints one JSON object,
// one key per scenario; deploy/compose/ci/outcomes.py compares the reference's
// object with SprigDB's (chapter 50, "Testing").
import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class Scenarios {
    static ConnectionFactory cf = new ConnectionFactory();
    static String pre;
    static final Map<String, Object> out = new TreeMap<>();

    interface Body { Object run(Connection c, Channel ch) throws Exception; }

    static String q(String n) { return pre + "." + n; }

    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    static String s(byte[] b) { return new String(b, StandardCharsets.UTF_8); }

    static void scenario(String name, Body body) {
        try (Connection c = cf.newConnection()) {
            Channel ch = c.createChannel();
            out.put(name, body.run(c, ch));
        } catch (Exception e) {
            out.put(name, Map.of("error", code(e)));
        }
    }

    static Object code(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ShutdownSignalException sse && sse.getReason() instanceof AMQP.Channel.Close cl) {
                return cl.getReplyCode();
            }
        }
        return e.getClass().getSimpleName();
    }

    static List<String> drain(Channel ch, String queue, long waitMs) throws Exception {
        List<String> got = new ArrayList<>();
        long end = System.currentTimeMillis() + waitMs;
        while (true) {
            GetResponse r = ch.basicGet(queue, true);
            if (r == null) {
                if (System.currentTimeMillis() < end) { Thread.sleep(100); continue; }
                return got;
            }
            got.add(s(r.getBody()));
        }
    }

    public static void main(String[] a) throws Exception {
        cf.setHost(a[0]);
        cf.setPort(Integer.parseInt(a[1]));
        cf.setUsername(a[2]);
        cf.setPassword(a[3]);
        cf.setVirtualHost(a[4]);
        cf.setAutomaticRecoveryEnabled(false);
        pre = a[5];

        scenario("confirmed_publish_get", (c, ch) -> {
            ch.confirmSelect();
            ch.queueDeclare(q("a"), true, false, false, null);
            for (int i = 0; i < 5; i++) {
                ch.basicPublish("", q("a"), MessageProperties.PERSISTENT_TEXT_PLAIN, b("m" + i));
            }
            boolean confirmed = ch.waitForConfirms(10_000);
            List<Object> got = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                GetResponse r = ch.basicGet(q("a"), true);
                got.add(r == null ? null : List.of(s(r.getBody()), r.getMessageCount()));
            }
            return List.of(confirmed, got);
        });
        scenario("nack_requeue", (c, ch) -> {
            ch.queueDeclare(q("b"), false, false, false, null);
            ch.basicPublish("", q("b"), null, b("x"));
            Thread.sleep(200);
            GetResponse r = ch.basicGet(q("b"), false);
            ch.basicNack(r.getEnvelope().getDeliveryTag(), false, true);
            GetResponse r2 = ch.basicGet(q("b"), true);
            return List.of(r.getEnvelope().isRedeliver(), r2.getEnvelope().isRedeliver(), s(r2.getBody()));
        });
        scenario("reject_to_dlx", (c, ch) -> {
            ch.exchangeDeclare(q("dlx"), "fanout");
            ch.queueDeclare(q("dead"), false, false, false, null);
            ch.queueBind(q("dead"), q("dlx"), "");
            ch.queueDeclare(q("c"), false, false, false, Map.of("x-dead-letter-exchange", q("dlx")));
            ch.basicPublish("", q("c"), null, b("r"));
            Thread.sleep(200);
            GetResponse r = ch.basicGet(q("c"), false);
            ch.basicReject(r.getEnvelope().getDeliveryTag(), false);
            return drain(ch, q("dead"), 3000);
        });
        scenario("topic_routing", (c, ch) -> {
            ch.exchangeDeclare(q("topic"), "topic");
            ch.queueDeclare(q("t1"), false, false, false, null);
            ch.queueBind(q("t1"), q("topic"), "a.*");
            ch.queueDeclare(q("t2"), false, false, false, null);
            ch.queueBind(q("t2"), q("topic"), "#.z");
            for (String k : List.of("a.b", "a.z", "x.y.z", "b.c")) ch.basicPublish(q("topic"), k, null, b(k));
            Thread.sleep(300);
            List<String> t1 = drain(ch, q("t1"), 0), t2 = drain(ch, q("t2"), 0);
            Collections.sort(t1);
            Collections.sort(t2);
            return List.of(t1, t2);
        });
        scenario("consume_prefetch", (c, ch) -> {
            ch.queueDeclare(q("pf"), false, false, false, null);
            for (int i = 0; i < 5; i++) ch.basicPublish("", q("pf"), null, b("" + i));
            Thread.sleep(300);
            ch.basicQos(2);
            BlockingQueue<Long> tags = new LinkedBlockingQueue<>();
            ch.basicConsume(q("pf"), false, (tag, d) -> tags.add(d.getEnvelope().getDeliveryTag()), tag -> {});
            Thread.sleep(1000);
            int first = tags.size();
            for (Long t : new ArrayList<>(tags)) ch.basicAck(t, false);
            Thread.sleep(1000);
            return List.of(first, tags.size());
        });
        scenario("tx_rollback_commit", (c, ch) -> {
            ch.queueDeclare(q("tx"), false, false, false, null);
            ch.txSelect();
            ch.basicPublish("", q("tx"), null, b("rolled"));
            ch.txRollback();
            ch.basicPublish("", q("tx"), null, b("committed"));
            ch.txCommit();
            return drain(ch, q("tx"), 1000);
        });
        scenario("passive_missing", (c, ch) -> {
            ch.queueDeclarePassive(q("missing"));
            return "declared";
        });
        scenario("properties_round_trip", (c, ch) -> {
            ch.queueDeclare(q("props"), false, false, false, null);
            AMQP.BasicProperties p = new AMQP.BasicProperties.Builder().contentType("application/json")
                .correlationId("c-1").replyTo("r").messageId("m-1").headers(Map.of("k", "v"))
                .deliveryMode(2).type("t").appId("a").build();
            ch.basicPublish("", q("props"), p, b("{}"));
            Thread.sleep(200);
            AMQP.BasicProperties g = ch.basicGet(q("props"), true).getProps();
            Object k = g.getHeaders() == null ? null : String.valueOf(g.getHeaders().get("k"));
            return Arrays.asList(g.getContentType(), g.getCorrelationId(), g.getReplyTo(), g.getMessageId(),
                String.valueOf(k), g.getDeliveryMode(), g.getType(), g.getAppId());
        });
        scenario("cleanup", (c, ch) -> {
            for (String n : List.of("a", "b", "c", "dead", "t1", "t2", "pf", "tx", "props")) {
                try { ch.queueDelete(q(n)); } catch (Exception e) { ch = c.createChannel(); }
            }
            for (String n : List.of("dlx", "topic")) {
                try { ch.exchangeDelete(q(n)); } catch (Exception e) { ch = c.createChannel(); }
            }
            return "ok";
        });
        System.out.println(json(out));
    }

    static String json(Object o) {
        if (o == null) return "null";
        if (o instanceof String str) {
            StringBuilder sb = new StringBuilder("\"");
            for (char ch : str.toCharArray()) {
                if (ch == '"' || ch == '\\') sb.append('\\').append(ch);
                else if (ch < 0x20) sb.append(String.format("\\u%04x", (int) ch));
                else sb.append(ch);
            }
            return sb.append('"').toString();
        }
        if (o instanceof Map<?, ?> m) {
            StringJoiner j = new StringJoiner(", ", "{", "}");
            new TreeMap<>(m).forEach((k, v) -> j.add(json(String.valueOf(k)) + ": " + json(v)));
            return j.toString();
        }
        if (o instanceof Collection<?> l) {
            StringJoiner j = new StringJoiner(", ", "[", "]");
            for (Object v : l) j.add(json(v));
            return j.toString();
        }
        return String.valueOf(o);
    }
}
