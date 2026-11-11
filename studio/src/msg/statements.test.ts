import { describe, expect, it } from 'vitest';
import { alterInherited, alterSink, bindingsGraph, createStream, getMessages, publishTest, readRecords, settle } from './statements';

describe('stream, queue, sink and inherited statements (steps 28–31)', () => {
  it('streams', () => {
    expect(readRecords('order_events', 3, 1200, 100)).toBe(
      'SELECT partition_no, offset_no, ts, `key`, `value`, headers FROM `order_events` WHERE partition_no = 3 AND offset_no >= 1200 ORDER BY offset_no LIMIT 100',
    );
    expect(readRecords('s', 0, 0, 99999)).toContain('LIMIT 1000');
    expect(() => readRecords('s', -1, 0, 1)).toThrow();
    expect(() => readRecords('s', 0, 1.5, 1)).toThrow();
    expect(createStream('e`v', 12, '7 days', 'delete')).toBe("CREATE STREAM `e``v` PARTITIONS 12 RETENTION '7 days' CLEANUP delete");
    expect(() => createStream('x', 0, '', 'compact')).toThrow();
  });

  it('queues', () => {
    expect(publishTest('orders', 'order.paid', '{"a":1}', '/')).toBe(`CALL sprig.mq_publish('orders', 'order.paid', '{"a":1}', '{}')`);
    expect(publishTest('x', 'k', "it's", 'mq2')).toBe("CALL sprig.mq_publish('x', 'k', 'it''s', '{}', FALSE, 'mq2')");
    expect(getMessages('billing', 500)).toBe("CALL sprig.mq_get('billing', 100, 30000)");
    expect(settle('q1.AA', true)).toBe("CALL sprig.mq_nack('q1.AA', TRUE)");
    expect(settle('q1.AA', false)).toBe("CALL sprig.mq_ack('q1.AA')");
    const g = bindingsGraph({
      vhost: '/',
      exchanges: [{ id: 1, name: 'orders', type: 'topic', durable: true, internal: false, arguments: null }],
      queues: [{ id: 2, name: 'billing', type: 'classic', durable: true, arguments: null, lanes: 1, state: 'active' }],
      bindings: [{ source: 'orders', destination: 'billing', destination_kind: 'queue', binding_key: 'order.*', arguments: null }],
    });
    expect(g.nodes.size).toBe(2);
    expect([...g.edges.values()][0]).toMatchObject({ start: 'x:orders', end: 'q:billing', type: 'order.*' });
  });

  it('sinks and inherited objects', () => {
    expect(alterSink('out', 'pause')).toBe('ALTER SINK `out` PAUSE');
    expect(alterSink('out', 'earliest')).toBe('ALTER SINK `out` RESET TO EARLIEST');
    expect(alterInherited('trigger', 'shop.sync_users', 'disable')).toBe('ALTER INHERITED TRIGGER `shop`.`sync_users` DISABLE');
    expect(() => alterInherited('table', 't', 'reset')).toThrow();
  });
});

describe('bodies', () => {
  it('shows UTF-8 as text and anything else as hex', async () => {
    const { binaryText } = await import('./body');
    expect(binaryText('eyJuIjogM30=')).toBe('{"n": 3}');
    expect(binaryText('//4=')).toBe('0xfffe');
    expect(binaryText(null)).toBeNull();
  });
});

describe('connections and script sinks (round 5)', () => {
  it('writes CREATE CONNECTION with masked credentials for display', async () => {
    const { createConnection, MASK } = await import('./statements');
    const opts = [
      { key: 'url', value: 'https://hooks.example.com/in' },
      { key: 'auth', value: 'bearer' },
      { key: '', value: 'ignored' },
    ];
    const creds = [{ key: 'token', value: "s3cr'et" }];
    expect(createConnection('crm', 'webhook', opts, creds)).toBe(
      "CREATE CONNECTION `crm` TYPE webhook WITH (url = 'https://hooks.example.com/in', auth = 'bearer') CREDENTIALS (token = 's3cr''et')",
    );
    const shown = createConnection('crm', 'webhook', opts, creds, true);
    expect(shown).toContain(`token = ${MASK}`);
    expect(shown).not.toContain('s3cr');
    expect(() => createConnection('crm', 'webhook', [], [])).toThrow('needs url');
    expect(() => createConnection('b', 's3', [{ key: 'endpoint', value: 'https://s3.example.com' }], [])).toThrow('needs bucket');
    expect(() => createConnection('k', 'kafka', [{ key: 'bootstrap', value: 'k:9092' }], [{ key: 'token', value: 'x' }])).toThrow('takes no credential token');
    expect(() => createConnection('k', 'kafka', [{ key: 'bootstrap', value: 'k:9092' }, { key: 'boot strap', value: 'x' }], [])).toThrow("isn't an option name");
  });
  it('sets credentials and drops connections', async () => {
    const { setCredentials, dropConnection } = await import('./statements');
    expect(setCredentials('crm', [{ key: 'TOKEN', value: 't2' }])).toBe("ALTER CONNECTION `crm` SET CREDENTIALS (token = 't2')");
    expect(setCredentials('crm', [{ key: 'token', value: 't2' }], true)).toBe("ALTER CONNECTION `crm` SET CREDENTIALS (token = '********')");
    expect(() => setCredentials('crm', [{ key: 'token', value: '' }])).toThrow('at least one');
    expect(dropConnection('c`1')).toBe('DROP CONNECTION `c``1`');
  });
  it('quotes script bodies with a tag the body does not hold', async () => {
    const { dollarQuote } = await import('./statements');
    expect(dollarQuote('return 1')).toBe('$$return 1$$');
    expect(dollarQuote('a $$ b')).toBe('$sprig$a $$ b$sprig$');
    expect(dollarQuote('a $$ b $sprig$')).toBe('$sprig2$a $$ b $sprig$$sprig2$');
  });
  it('writes script sinks and reads them back from SHOW CREATE SINK', async () => {
    const { createScriptSink, setScript, scriptOf, SCRIPT_TEMPLATES } = await import('./statements');
    const body = 'export function deliver(b) { return {ok: true}; }';
    const sql = createScriptSink('s1', { kind: 'stream', name: 'events' }, 'javascript', ['crm', ' '], body);
    expect(sql).toBe('CREATE SINK `s1` FROM STREAM `events` TO SCRIPT LANGUAGE javascript USING CONNECTIONS (`crm`) AS $$' + body + '$$');
    expect(scriptOf(sql)).toEqual({ language: 'javascript', body });
    expect(createScriptSink('s2', { kind: 'changes', name: 'a, b' }, 'lua', [], 'x')).toBe(
      'CREATE SINK `s2` FROM CHANGES OF `a`, `b` TO SCRIPT LANGUAGE lua AS $$x$$',
    );
    expect(setScript('s1', 'a $$ b')).toBe('ALTER SINK `s1` SET SCRIPT AS $sprig$a $$ b$sprig$');
    expect(scriptOf('CREATE SINK `x` FROM STREAM `e` TO SCRIPT LANGUAGE js AS $t$  y $t$ WITH (a = 1)')).toEqual({ language: 'javascript', body: ' y' });
    expect(scriptOf('CREATE SINK `x` FROM STREAM `e` TO WEBHOOK CONNECTION `c`')).toBeNull();
    expect(scriptOf('CREATE SINK `x` FROM STREAM `e` TO SCRIPT FUNCTION `f`')).toBeNull();
    expect(() => createScriptSink('s', { kind: 'queue', name: '' }, 'lua', [], 'x')).toThrow('name the source');
    expect((JSON.parse(SCRIPT_TEMPLATES.kirun) as { parameters: { batch: { parameterName: string } } }).parameters.batch.parameterName).toBe('batch');
  });
});

describe('connection option lines', () => {
  it('reads key = value lines', async () => {
    const { parsePairs } = await import('./SinkForms');
    expect(parsePairs("auth = bearer\n# note\n\ntimeout_ms = '5000'")).toEqual([
      { key: 'auth', value: 'bearer' },
      { key: 'timeout_ms', value: '5000' },
    ]);
    expect(() => parsePairs('nonsense')).toThrow('key = value');
  });
});
