# AMQP 0-9-1 differential scenarios (r9final, 10 November 2026; committed for
# CI by r4three): runs the same steps against one endpoint and prints one JSON
# object, one key per scenario, with what pika observed. run.sh runs it against
# a RabbitMQ reference and SprigDB and deploy/compose/ci/outcomes.py compares.
# Usage: python amqp_diff.py host port user password vhost prefix
import glob, json, sys, time
sys.path[:0] = glob.glob('/wheels/pika-*.whl')
import pika

host, port, user, pw, vhost, pre = sys.argv[1:7]
params = pika.ConnectionParameters(host=host, port=int(port), virtual_host=vhost,
    credentials=pika.PlainCredentials(user, pw), heartbeat=0, blocked_connection_timeout=30)
out = {}

def conn():
    return pika.BlockingConnection(params)

def q(n):
    return f"{pre}.{n}"

def drain(ch, name, n=100, wait=0.0):
    got = []
    end = time.time() + wait
    while len(got) < n:
        m, p, b = ch.basic_get(name, auto_ack=True)
        if m is None:
            if time.time() < end:
                time.sleep(0.1); continue
            break
        got.append(b.decode())
    return got

def err(e):
    a = getattr(e, 'args', ())
    return a[0] if a and isinstance(a[0], int) else type(e).__name__

def scenario(name):
    def deco(f):
        c = conn()
        try:
            ch = c.channel()
            out[name] = f(c, ch)
        except Exception as e:
            out[name] = {"error": err(e)}
        finally:
            try: c.close()
            except Exception: pass
        return f
    return deco

@scenario("basic_publish_get")
def _(c, ch):
    ch.confirm_delivery()
    ch.queue_declare(q("a"), durable=True)
    for i in range(5):
        ch.basic_publish('', q("a"), f"m{i}".encode(), pika.BasicProperties(delivery_mode=2))
    res = []
    for _ in range(6):
        m, p, b = ch.basic_get(q("a"), auto_ack=True)
        res.append(None if m is None else [b.decode(), m.redelivered, m.message_count])
    return res

@scenario("nack_requeue")
def _(c, ch):
    ch.confirm_delivery()
    ch.queue_declare(q("b"))
    ch.basic_publish('', q("b"), b"x")
    m, p, b = ch.basic_get(q("b"))
    ch.basic_nack(m.delivery_tag, requeue=True)
    m2, p2, b2 = ch.basic_get(q("b"), auto_ack=True)
    return [m.redelivered, m2.redelivered, b2.decode()]

@scenario("reject_to_dlx")
def _(c, ch):
    ch.confirm_delivery()
    ch.exchange_declare(q("dlx"), 'fanout')
    ch.queue_declare(q("dead"))
    ch.queue_bind(q("dead"), q("dlx"))
    ch.queue_declare(q("c"), arguments={"x-dead-letter-exchange": q("dlx")})
    ch.basic_publish('', q("c"), b"r")
    m, p, b = ch.basic_get(q("c"))
    ch.basic_reject(m.delivery_tag, requeue=False)
    time.sleep(0.5)
    m, p, b = ch.basic_get(q("dead"), auto_ack=True)
    d = (p.headers or {}).get("x-death", [{}])[0]
    return [b.decode(), d.get("reason"), d.get("count"), d.get("queue")]

@scenario("ttl_expiry_to_dlx")
def _(c, ch):
    ch.confirm_delivery()
    ch.exchange_declare(q("dlx2"), 'fanout')
    ch.queue_declare(q("dead2"))
    ch.queue_bind(q("dead2"), q("dlx2"))
    ch.queue_declare(q("d"), arguments={"x-message-ttl": 200, "x-dead-letter-exchange": q("dlx2")})
    ch.basic_publish('', q("d"), b"t")
    time.sleep(1.0)
    m, p, b = ch.basic_get(q("d"), auto_ack=True)
    got = drain(ch, q("dead2"), 1, wait=3)
    return [m is None, got]

@scenario("max_length_drop_head")
def _(c, ch):
    ch.confirm_delivery()
    ch.queue_declare(q("e"), arguments={"x-max-length": 2})
    for i in range(3):
        ch.basic_publish('', q("e"), f"l{i}".encode())
    return drain(ch, q("e"))

@scenario("topic_routing")
def _(c, ch):
    ch.confirm_delivery()
    ch.exchange_declare(q("topic"), 'topic')
    for n, k in (("t1", "a.*"), ("t2", "#.z")):
        ch.queue_declare(q(n)); ch.queue_bind(q(n), q("topic"), k)
    for k in ("a.b", "a.z", "x.y.z", "b.c"):
        ch.basic_publish(q("topic"), k, k.encode())
    return [sorted(drain(ch, q("t1"))), sorted(drain(ch, q("t2")))]

@scenario("fanout")
def _(c, ch):
    ch.confirm_delivery()
    ch.exchange_declare(q("fan"), 'fanout')
    for n in ("f1", "f2"):
        ch.queue_declare(q(n)); ch.queue_bind(q(n), q("fan"))
    ch.basic_publish(q("fan"), 'ignored', b"f")
    return [drain(ch, q("f1")), drain(ch, q("f2"))]

@scenario("headers_exchange")
def _(c, ch):
    ch.confirm_delivery()
    ch.exchange_declare(q("hdr"), 'headers')
    ch.queue_declare(q("hall")); ch.queue_bind(q("hall"), q("hdr"), arguments={"x-match": "all", "a": "1", "b": "2"})
    ch.queue_declare(q("hany")); ch.queue_bind(q("hany"), q("hdr"), arguments={"x-match": "any", "a": "1", "b": "2"})
    for name, h in (("both", {"a": "1", "b": "2"}), ("onlya", {"a": "1"}), ("none", {"c": "3"})):
        ch.basic_publish(q("hdr"), '', name.encode(), pika.BasicProperties(headers=h))
    return [sorted(drain(ch, q("hall"))), sorted(drain(ch, q("hany")))]

@scenario("priority")
def _(c, ch):
    ch.confirm_delivery()
    ch.queue_declare(q("p"), arguments={"x-max-priority": 5})
    for body, pr in (("low", 1), ("high", 5), ("mid", 3), ("none", None)):
        ch.basic_publish('', q("p"), body.encode(), pika.BasicProperties(priority=pr))
    return drain(ch, q("p"))

@scenario("mandatory_return")
def _(c, ch):
    returned = []
    ch.add_on_return_callback(lambda ch_, m, p, b: returned.append([m.reply_code, b.decode()]))
    ch.exchange_declare(q("nowhere"), 'direct')
    ch.basic_publish(q("nowhere"), 'nokey', b"lost", mandatory=True)
    c.process_data_events(time_limit=1)
    return returned

@scenario("passive_missing")
def _(c, ch):
    ch.queue_declare(q("missing"), passive=True)
    return "declared"

@scenario("redeclare_different_args")
def _(c, ch):
    ch.queue_declare(q("g"), arguments={"x-max-length": 5})
    ch.queue_declare(q("g"), arguments={"x-max-length": 6})
    return "accepted"

@scenario("purge")
def _(c, ch):
    ch.confirm_delivery()
    ch.queue_declare(q("h"))
    for i in range(4):
        ch.basic_publish('', q("h"), b"p")
    r = ch.queue_purge(q("h"))
    return [r.method.message_count, drain(ch, q("h"))]

@scenario("tx_rollback_commit")
def _(c, ch):
    ch.queue_declare(q("tx"))
    ch.tx_select()
    ch.basic_publish('', q("tx"), b"rolled")
    ch.tx_rollback()
    ch.basic_publish('', q("tx"), b"committed")
    ch.tx_commit()
    return drain(ch, q("tx"), wait=1)

@scenario("prefetch")
def _(c, ch):
    ch.confirm_delivery()
    ch.queue_declare(q("pf"))
    for i in range(5):
        ch.basic_publish('', q("pf"), f"{i}".encode())
    ch.basic_qos(prefetch_count=2)
    got = []
    ch.basic_consume(q("pf"), lambda ch_, m, p, b: got.append((m.delivery_tag, b.decode())))
    c.process_data_events(time_limit=1.0)
    first = len(got)
    for t, _ in list(got):
        ch.basic_ack(t)
    c.process_data_events(time_limit=1.0)
    return [first, len(got)]

@scenario("exclusive_elsewhere")
def _(c, ch):
    ch.queue_declare(q("ex"), exclusive=True)
    c2 = conn()
    try:
        c2.channel().queue_declare(q("ex"), passive=True)
        return "visible"
    except Exception as e:
        return {"other": err(e)}
    finally:
        try: c2.close()
        except Exception: pass

@scenario("delete_if_empty")
def _(c, ch):
    ch.confirm_delivery()
    ch.queue_declare(q("del"))
    ch.basic_publish('', q("del"), b"x")
    ch.queue_delete(q("del"), if_empty=True)
    return "deleted"

@scenario("alternate_exchange")
def _(c, ch):
    ch.confirm_delivery()
    ch.exchange_declare(q("alt"), 'fanout')
    ch.queue_declare(q("altq")); ch.queue_bind(q("altq"), q("alt"))
    ch.exchange_declare(q("main"), 'direct', arguments={"alternate-exchange": q("alt")})
    ch.basic_publish(q("main"), 'unbound', b"to-alt")
    return drain(ch, q("altq"), wait=1)

@scenario("properties_round_trip")
def _(c, ch):
    ch.confirm_delivery()
    ch.queue_declare(q("props"))
    ch.basic_publish('', q("props"), b"{}", pika.BasicProperties(
        content_type="application/json", correlation_id="c-1", reply_to="r", message_id="m-1",
        headers={"k": "v", "n": 5}, delivery_mode=2, type="t", app_id="a"))
    m, p, b = ch.basic_get(q("props"), auto_ack=True)
    h = {k: v for k, v in (p.headers or {}).items() if not k.startswith("x-sprig")}
    return [p.content_type, p.correlation_id, p.reply_to, p.message_id, h, p.delivery_mode, p.type, p.app_id]

@scenario("direct_reply_to")
def _(c, ch):
    ch.queue_declare(q("rpc"))
    replies = []
    ch.basic_consume('amq.rabbitmq.reply-to', lambda ch_, m, p, b: replies.append(b.decode()), auto_ack=True)
    ch.basic_publish('', q("rpc"), b"ping", pika.BasicProperties(reply_to='amq.rabbitmq.reply-to'))
    c2 = conn(); ch2 = c2.channel()
    m, p, b = ch2.basic_get(q("rpc"), auto_ack=True)
    ch2.basic_publish('', p.reply_to, b"pong")
    c.process_data_events(time_limit=2)
    c2.close()
    return [b.decode(), replies]

@scenario("cleanup")
def _(c, ch):
    for n in ("a","b","c","dead","d","dead2","e","t1","t2","f1","f2","hall","hany","p","g","h","tx","pf","altq","props","rpc"):
        try: ch.queue_delete(q(n))
        except Exception: ch = c.channel()
    for n in ("dlx","dlx2","topic","fan","hdr","nowhere","alt","main"):
        try: ch.exchange_delete(q(n))
        except Exception: ch = c.channel()
    return "ok"

print(json.dumps(out, sort_keys=True))
