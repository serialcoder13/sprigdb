import { describe, expect, it } from 'vitest';
import { createSink, mqInfoAll, queueInfos, testNewConnection, testNewSink, type SinkSpec } from './statements';

const SPEC: SinkSpec = {
  name: 'out7',
  from: { kind: 'stream', name: 'orders' },
  where: '',
  columns: '',
  transform: '',
  target: 'webhook',
  connection: 'hook',
  where_to: '',
  key: '',
  guarantee: 'at least once',
  start: 'latest',
  options: [],
};

describe('Studio round 7: messaging statements', () => {
  it('asks for every queue in one mq_info call (ST66)', () => {
    expect(mqInfoAll()).toBe('CALL sprig.mq_info(NULL)');
    expect(mqInfoAll('/')).toBe('CALL sprig.mq_info(NULL)');
    expect(mqInfoAll('shop')).toBe("CALL sprig.mq_info(NULL, 'shop')");
    const m = queueInfos(
      ['queue', 'ready', 'unacked', 'consumers'],
      [
        ['a', '2', '1', '0'],
        ['b', '0', '0', '3'],
        [null, '9', '9', '9'],
      ],
    );
    expect([...m.keys()]).toEqual(['a', 'b']);
    expect(m.get('a')).toEqual({ ready: 2, unacked: 1, consumers: 0 });
    expect(m.get('b')).toEqual({ ready: 0, unacked: 0, consumers: 3 });
  });
  it('tests an unsaved connection with TEST CREATE CONNECTION, masked on screen (ST68)', () => {
    const opts = [{ key: 'url', value: 'https://h.example.com/in' }];
    const creds = [{ key: 'token', value: 's3cret' }];
    expect(testNewConnection('crm', 'webhook', opts, creds, true)).toBe(
      "TEST CREATE CONNECTION `crm` TYPE webhook WITH (url = 'https://h.example.com/in') CREDENTIALS (token = '********')",
    );
    expect(testNewConnection('crm', 'webhook', opts, creds)).toContain("token = 's3cret'");
    expect(() => testNewConnection('crm', 'webhook', [], creds)).toThrow(/needs url/);
  });
  it('writes CREATE SINK for each target, and its dry run (ST69, ST70)', () => {
    expect(createSink(SPEC)).toBe('CREATE SINK `out7` FROM STREAM `orders` TO WEBHOOK CONNECTION `hook`');
    expect(
      createSink({
        ...SPEC,
        where: 'amount > 10',
        columns: 'id, amount',
        transform: 'enrich',
        target: 'kafka',
        where_to: 'orders-out',
        key: 'id',
        guarantee: 'exactly once',
        start: 'earliest',
        options: [{ key: 'batch_max_records', value: '5' }],
      }),
    ).toBe(
      "CREATE SINK `out7` FROM STREAM `orders` WHERE amount > 10 COLUMNS (`id`, `amount`) TRANSFORM `enrich` TO KAFKA CONNECTION `hook` TOPIC 'orders-out' KEY 'id' GUARANTEE EXACTLY ONCE START AT EARLIEST WITH (batch_max_records = '5')",
    );
    expect(createSink({ ...SPEC, target: 'webhook', where_to: '/in' })).toContain("TO WEBHOOK CONNECTION `hook` PATH '/in'");
    expect(createSink({ ...SPEC, target: 's3', where_to: 'raw/' })).toContain("TO S3 CONNECTION `hook` PREFIX 'raw/'");
    expect(createSink({ ...SPEC, from: { kind: 'changes', name: 'a, b' }, target: 'database', where_to: 'copy' })).toBe(
      "CREATE SINK `out7` FROM CHANGES OF `a`, `b` TO DATABASE CONNECTION `hook` TABLE 'copy'",
    );
    expect(() => createSink({ ...SPEC, target: 'kafka' })).toThrow(/topic/);
    expect(() => createSink({ ...SPEC, connection: ' ' })).toThrow(/connection/);
    expect(testNewSink(createSink(SPEC), 25)).toBe('TEST CREATE SINK `out7` FROM STREAM `orders` TO WEBHOOK CONNECTION `hook` WITH SAMPLE 25');
    expect(() => testNewSink('DROP SINK x')).toThrow();
    expect(() => testNewSink(createSink(SPEC), 0)).toThrow();
  });
});
