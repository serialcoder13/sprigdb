// Kafka scenarios with Confluent.Kafka (pinned in Scenarios.csproj): keyed
// produce with headers, a consumer group read with commits, a second read of
// the same group, per-key order. Prints one JSON object of observations;
// deploy/compose/ci/outcomes.py compares the reference's with SprigDB's
// (keys ending in _seconds are timings).
// Usage: dotnet run -- <bootstrap> <user|-> <password> <topic> <group>
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

var (boot, user, pass, topic, group) = (args[0], args[1], args[2], args[3], args[4]);
const int N = 300;
var output = new SortedDictionary<string, object?>();

T Secure<T>(T c) where T : ClientConfig
{
    c.BootstrapServers = boot;
    if (user != "-")
    {
        c.SecurityProtocol = SecurityProtocol.SaslPlaintext;
        c.SaslMechanism = SaslMechanism.ScramSha512;
        c.SaslUsername = user;
        c.SaslPassword = pass;
    }
    return c;
}

var sw = Stopwatch.StartNew();
var parts = new Dictionary<string, HashSet<int>>();
var offs = new Dictionary<string, List<long>>();
var produced = 0;
try
{
    using var p = new ProducerBuilder<string, string>(Secure(new ProducerConfig { Acks = Acks.All, LingerMs = 20 })).Build();
    for (var i = 0; i < N; i++)
    {
        var key = $"k{i % 30}";
        var msg = new Message<string, string> { Key = key, Value = $"v{i}", Headers = new Headers { { "h", Encoding.UTF8.GetBytes(i.ToString()) } } };
        var r = await p.ProduceAsync(topic, msg);
        produced++;
        if (!parts.ContainsKey(key)) { parts[key] = new HashSet<int>(); offs[key] = new List<long>(); }
        parts[key].Add(r.Partition.Value);
        offs[key].Add(r.Offset.Value);
    }
}
catch (Exception e)
{
    output["produce_error"] = e.GetType().Name;
}
output["produce_seconds"] = sw.Elapsed.TotalSeconds;
output["produced"] = produced;
output["key_partition_stable"] = parts.Values.All(s => s.Count == 1);
output["key_offsets_increase"] = offs.Values.All(l => l.SequenceEqual(l.OrderBy(x => x)));

List<ConsumeResult<string, string>> Read(int want, TimeSpan wait)
{
    var got = new List<ConsumeResult<string, string>>();
    var cfg = Secure(new ConsumerConfig { GroupId = group, AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false });
    using var c = new ConsumerBuilder<string, string>(cfg).Build();
    c.Subscribe(topic);
    var end = DateTime.UtcNow + wait;
    while (got.Count < want && DateTime.UtcNow < end)
    {
        var r = c.Consume(TimeSpan.FromSeconds(1));
        if (r != null && !r.IsPartitionEOF) got.Add(r);
    }
    try { if (got.Count > 0) c.Commit(); } catch (KafkaException e) { output["commit_error"] = e.Error.Code.ToString(); }
    c.Close();
    return got;
}

sw.Restart();
List<ConsumeResult<string, string>> got;
try { got = Read(N, TimeSpan.FromSeconds(90)); }
catch (Exception e) { output["consume_error"] = e.GetType().Name; got = new(); }
output["group_read_seconds"] = sw.Elapsed.TotalSeconds;
output["group_read"] = got.Count;
output["per_key_order"] = got.GroupBy(r => r.Message.Key).All(g =>
{
    var v = g.Select(r => int.Parse(r.Message.Value[1..])).ToList();
    return v.SequenceEqual(v.OrderBy(x => x));
});
output["headers_kept"] = got.All(r => r.Message.Headers.Count == 1
    && Encoding.UTF8.GetString(r.Message.Headers[0].GetValueBytes()) == r.Message.Value[1..]);
try { output["second_run_reads"] = Read(1, TimeSpan.FromSeconds(8)).Count; }
catch (Exception e) { output["second_run_reads"] = e.GetType().Name; }
Console.WriteLine(JsonSerializer.Serialize(output));
