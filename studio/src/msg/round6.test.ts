import { describe, expect, it } from 'vitest';
import { mqBrowse, mqInfo, mqPurge, mqRepublish, outcomes, queueInfo, scratchName, stepRows, testConnection, testSink } from './statements';

describe('queue pages (round 6, R9 item 89)', () => {
  it('calls take the vhost last, and leave it out for /', () => {
    expect(mqInfo('orders')).toBe("CALL sprig.mq_info('orders')");
    expect(mqInfo('orders', '/')).toBe("CALL sprig.mq_info('orders')");
    expect(mqInfo("o'q", 'shop')).toBe("CALL sprig.mq_info('o''q', 'shop')");
    expect(mqBrowse('q', 50, 'shop')).toBe("CALL sprig.mq_browse('q', 50, 'shop')");
    expect(mqBrowse('q', 5000)).toBe("CALL sprig.mq_browse('q', 1000)");
    expect(mqBrowse('q', Number.NaN)).toBe("CALL sprig.mq_browse('q', 50)");
    expect(mqPurge('q', 'shop')).toBe("CALL sprig.mq_purge('q', 'shop')");
  });
  it('republish keeps each routing key when none is given', () => {
    expect(mqRepublish('dlq', 'orders', '', 100)).toBe("CALL sprig.mq_republish('dlq', 'orders', NULL, 100)");
    expect(mqRepublish('dlq', '', ' a.b ', 5, 'shop')).toBe("CALL sprig.mq_republish('dlq', '', 'a.b', 5, 'shop')");
    expect(() => mqRepublish('dlq', 'x', '', 0)).toThrow();
    expect(() => mqRepublish('dlq', 'x', '', 10_001)).toThrow();
    expect(() => mqRepublish('dlq', 'x', '', 1.5)).toThrow();
  });
  it('reads mq_info as numbers', () => {
    expect(queueInfo(['ready', 'unacked', 'consumers'], ['2', '1', '0'])).toEqual({ ready: 2, unacked: 1, consumers: 0 });
    expect(queueInfo(['ready'], undefined)).toBeNull();
  });
});

describe('dry runs (round 6, R9 items 104–105)', () => {
  it('builds TEST CONNECTION and TEST SINK', () => {
    expect(testConnection('crm')).toBe('TEST CONNECTION `crm`');
    expect(testConnection('k', { to: 'audit' })).toBe("TEST CONNECTION `k` WRITE TO 'audit'");
    expect(testConnection('h', { to: ' ' })).toBe('TEST CONNECTION `h` WRITE');
    expect(() => testConnection(' ')).toThrow();
    expect(testSink('s')).toBe('TEST SINK `s` WITH SAMPLE 10');
    expect(testSink('s', 1000)).toBe('TEST SINK `s` WITH SAMPLE 1000');
    expect(() => testSink('s', 0)).toThrow();
    expect(() => testSink('s', 1001)).toThrow();
  });
  it('scratch names are distinct from the real one and safe as identifiers', () => {
    expect(scratchName('crm-api', () => 0.5)).toBe('crm_api_studiotest_7fffff');
    expect(scratchName('x'.repeat(80), () => 0)).toMatch(/^x{40}_studiotest_000000$/);
  });
  it('reads the step and record rows', () => {
    const steps = stepRows(
      ['Step', 'Result', 'Detail', 'Elapsed ms'],
      [
        ['credentials', 'ok', 'opened', '1'],
        ['query', 'failed', 'password authentication failed for user postgres', '12'],
      ],
    );
    expect(steps).toEqual([
      { step: 'credentials', ok: true, detail: 'opened', ms: 1 },
      { step: 'query', ok: false, detail: 'password authentication failed for user postgres', ms: 12 },
    ]);
    expect(outcomes(['Position', 'Outcome'], [['0:1', 'deliver'], ['0:2', 'filtered'], ['0:3', 'deliver']])).toEqual({ deliver: 2, filtered: 1 });
  });
});
