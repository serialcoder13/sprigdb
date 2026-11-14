// AMQP 0-9-1 scenarios with RabbitMQ.Client (pinned in Scenarios.csproj).
// Usage: dotnet run -- <host> <port> <user> <password> <vhost> <prefix>.
// Prints one JSON object, one key per scenario; deploy/compose/ci/outcomes.py
// compares the reference's object with SprigDB's.
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

var factory = new ConnectionFactory
{
    HostName = args[0],
    Port = int.Parse(args[1]),
    UserName = args[2],
    Password = args[3],
    VirtualHost = args[4],
    AutomaticRecoveryEnabled = false,
};
var pre = args[5];
var output = new SortedDictionary<string, object?>();
string Q(string n) => $"{pre}.{n}";
byte[] B(string s) => Encoding.UTF8.GetBytes(s);
string S(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);

List<string> Drain(IModel ch, string queue, int waitMs)
{
    var got = new List<string>();
    var end = DateTime.UtcNow.AddMilliseconds(waitMs);
    while (true)
    {
        var r = ch.BasicGet(queue, true);
        if (r == null)
        {
            if (DateTime.UtcNow < end) { Thread.Sleep(100); continue; }
            return got;
        }
        got.Add(S(r.Body));
    }
}

void Scenario(string name, Func<IConnection, IModel, object?> body)
{
    try
    {
        using var c = factory.CreateConnection();
        var ch = c.CreateModel();
        output[name] = body(c, ch);
    }
    catch (OperationInterruptedException e) when (e.ShutdownReason != null)
    {
        output[name] = new Dictionary<string, object> { ["error"] = (int)e.ShutdownReason.ReplyCode };
    }
    catch (Exception e)
    {
        output[name] = new Dictionary<string, object> { ["error"] = e.GetType().Name };
    }
}

Scenario("confirmed_publish_get", (c, ch) =>
{
    ch.ConfirmSelect();
    ch.QueueDeclare(Q("a"), true, false, false);
    for (var i = 0; i < 5; i++)
    {
        var p = ch.CreateBasicProperties();
        p.Persistent = true;
        ch.BasicPublish("", Q("a"), p, B($"m{i}"));
    }
    var confirmed = ch.WaitForConfirms(TimeSpan.FromSeconds(10));
    var got = new List<object?>();
    for (var i = 0; i < 6; i++)
    {
        var r = ch.BasicGet(Q("a"), true);
        got.Add(r == null ? null : new object[] { S(r.Body), r.MessageCount });
    }
    return new object[] { confirmed, got };
});
Scenario("nack_requeue", (c, ch) =>
{
    ch.QueueDeclare(Q("b"), false, false, false);
    ch.BasicPublish("", Q("b"), null, B("x"));
    Thread.Sleep(200);
    var r = ch.BasicGet(Q("b"), false);
    ch.BasicNack(r.DeliveryTag, false, true);
    var r2 = ch.BasicGet(Q("b"), true);
    return new object[] { r.Redelivered, r2.Redelivered, S(r2.Body) };
});
Scenario("reject_to_dlx", (c, ch) =>
{
    ch.ExchangeDeclare(Q("dlx"), "fanout");
    ch.QueueDeclare(Q("dead"), false, false, false);
    ch.QueueBind(Q("dead"), Q("dlx"), "");
    ch.QueueDeclare(Q("c"), false, false, false, new Dictionary<string, object> { ["x-dead-letter-exchange"] = Q("dlx") });
    ch.BasicPublish("", Q("c"), null, B("r"));
    Thread.Sleep(200);
    var r = ch.BasicGet(Q("c"), false);
    ch.BasicReject(r.DeliveryTag, false);
    return Drain(ch, Q("dead"), 3000);
});
Scenario("topic_routing", (c, ch) =>
{
    ch.ExchangeDeclare(Q("topic"), "topic");
    ch.QueueDeclare(Q("t1"), false, false, false);
    ch.QueueBind(Q("t1"), Q("topic"), "a.*");
    ch.QueueDeclare(Q("t2"), false, false, false);
    ch.QueueBind(Q("t2"), Q("topic"), "#.z");
    foreach (var k in new[] { "a.b", "a.z", "x.y.z", "b.c" }) ch.BasicPublish(Q("topic"), k, null, B(k));
    Thread.Sleep(300);
    var t1 = Drain(ch, Q("t1"), 0);
    var t2 = Drain(ch, Q("t2"), 0);
    t1.Sort(StringComparer.Ordinal);
    t2.Sort(StringComparer.Ordinal);
    return new object[] { t1, t2 };
});
Scenario("tx_rollback_commit", (c, ch) =>
{
    ch.QueueDeclare(Q("tx"), false, false, false);
    ch.TxSelect();
    ch.BasicPublish("", Q("tx"), null, B("rolled"));
    ch.TxRollback();
    ch.BasicPublish("", Q("tx"), null, B("committed"));
    ch.TxCommit();
    return Drain(ch, Q("tx"), 1000);
});
Scenario("passive_missing", (c, ch) =>
{
    ch.QueueDeclarePassive(Q("missing"));
    return "declared";
});
Scenario("cleanup", (c, ch) =>
{
    foreach (var n in new[] { "a", "b", "c", "dead", "t1", "t2", "tx" })
    {
        try { ch.QueueDelete(Q(n)); } catch (Exception) { ch = c.CreateModel(); }
    }
    foreach (var n in new[] { "dlx", "topic" })
    {
        try { ch.ExchangeDelete(Q(n)); } catch (Exception) { ch = c.CreateModel(); }
    }
    return "ok";
});
Console.WriteLine(JsonSerializer.Serialize(output));
