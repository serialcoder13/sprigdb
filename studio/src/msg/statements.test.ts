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
