import { describe, expect, it } from 'vitest';
import { autoRollback } from './api';
import { declaration, dependentsOf, dropStatement, rolloutStatement } from './declaration';
import { argValue, budgetLines, jsonField, kirunParams, mocks, parseParams, snapshotRows } from './testrun';

describe('test runs (round 6, RC13–RC17)', () => {
  it('reads parameters from the form and from a KIRun definition', () => {
    expect(parseParams('a BIGINT, n DECIMAL(10,2), IN `s` VARCHAR(40)')).toEqual([
      { name: 'a', type: 'BIGINT' },
      { name: 'n', type: 'DECIMAL(10,2)' },
      { name: 's', type: 'VARCHAR(40)' },
    ]);
    expect(parseParams('  ')).toEqual([]);
    expect(kirunParams('{"parameters": {"a": {"schema": {"type": "INTEGER"}}, "b": {"schema": {"type": ["STRING"]}}}}')).toEqual([
      { name: 'a', type: 'INTEGER' },
      { name: 'b', type: 'STRING' },
    ]);
    expect(kirunParams('not json')).toEqual([]);
  });
  it('types arguments by parameter type', () => {
    expect(argValue('BIGINT', '42')).toBe(42);
    expect(argValue('INTEGER', '-1.5')).toBe(-1.5);
    expect(argValue('DECIMAL(10,2)', ' ')).toBeNull();
    expect(() => argValue('int', 'x')).toThrow();
    expect(argValue('BOOLEAN', 'true')).toBe(true);
    expect(argValue('JSON', '{"a": 1}')).toEqual({ a: 1 });
    expect(() => argValue('JSON', '{')).toThrow();
    expect(argValue('VARCHAR(10)', 'null')).toBeNull();
    expect(argValue('VARCHAR(10)', ' x ')).toBe(' x ');
    expect(argValue('ANY', 'plain')).toBe('plain');
  });
  it('checks snapshot rows and builds mocks', () => {
    expect(snapshotRows('')).toBeUndefined();
    expect(snapshotRows('{"products": [{"id": 1}]}')).toEqual({ products: [{ id: 1 }] });
    expect(() => snapshotRows('[1]')).toThrow(/object/);
    expect(() => snapshotRows('{"t": [1]}')).toThrow(/row/);
    expect(() => jsonField('Row', '{')).toThrow(/^Row: /);
    expect(
      mocks([
        { method: 'any', url: 'https://rates.example.com/*', status: '', body: '{"rate": 2}' },
        { method: 'POST', url: 'https://x.example.com/a', status: '503', body: 'busy' },
        { method: 'GET', url: ' ', status: '200', body: '' },
      ]),
    ).toEqual([
      { method: '', url: 'https://rates.example.com/*', status: 200, body: { rate: 2 } },
      { method: 'POST', url: 'https://x.example.com/a', status: 503, body: 'busy' },
    ]);
    expect(() => mocks([{ method: 'GET', url: 'u', status: '99', body: '' }])).toThrow();
  });
  it('pairs each budget with its limit', () => {
    expect(budgetLines({ limits: { wallMs: 5000, dataOps: 1000, memoryBytes: 64 }, used: { wallMs: 3, dataOps: 2, peakBytes: 10 } })).toEqual([
      { key: 'wallMs', label: 'Wall time (ms)', used: 3, limit: 5000 },
      { key: 'dataOps', label: 'Data operations', used: 2, limit: 1000 },
      { key: 'peakBytes', label: 'Peak memory (bytes)', used: 10, limit: 64 },
    ]);
    expect(budgetLines(null)).toEqual([]);
  });
});

describe('rollout and drop (round 6, RC18–RC22)', () => {
  const form = { kind: 'function' as const, name: 'f', runtime: 'javascript' as const, params: 'a BIGINT', returns: 'BIGINT', security: 'invoker' as const, tier: 'auto' as const, body: 'return a;' };
  it('deploys as a stage', () => {
    expect(declaration('shop', { ...form, rollout: { pct: 10, by: 'user' } })).toContain("WITH (SPRIG_TIER = 'auto', SPRIG_ROLLOUT = 10) AS");
    expect(declaration('shop', { ...form, rollout: { pct: 50, by: 'tenant' } })).toContain("SPRIG_ROLLOUT = 50, SPRIG_ROLLOUT_BY = 'tenant')");
    expect(() => declaration('shop', { ...form, rollout: { pct: 100, by: 'user' } })).toThrow();
    expect(declaration('shop', form)).toContain("WITH (SPRIG_TIER = 'auto') AS");
  });
  it('moves, promotes and aborts', () => {
    expect(rolloutStatement('shop', 'function', 'f', { to: 50 })).toBe('ALTER FUNCTION `shop`.`f` ROLLOUT TO 50');
    expect(rolloutStatement('shop', 'procedure', 'p', { to: 5, by: 'tenant' })).toBe('ALTER PROCEDURE `shop`.`p` ROLLOUT TO 5 BY TENANT');
    expect(rolloutStatement('shop', 'function', 'f', 'promote')).toBe('ALTER FUNCTION `shop`.`f` ROLLOUT PROMOTE');
    expect(rolloutStatement('shop', 'function', 'f', 'abort')).toBe('ALTER FUNCTION `shop`.`f` ROLLOUT ABORT');
    expect(() => rolloutStatement('shop', 'function', 'f', { to: 0 })).toThrow();
  });
  it('drops with RESTRICT, then CASCADE, and reads 39875', () => {
    expect(dropStatement('shop', 'function', 'f', false)).toBe('DROP FUNCTION `shop`.`f` RESTRICT');
    expect(dropStatement('shop', 'trigger', 't', true)).toBe('DROP TRIGGER `shop`.`t` CASCADE');
    expect(() => dropStatement('shop', 'binding', 'b', false)).toThrow();
    expect(
      dependentsOf(
        'function f is in use: schedules nightly, hourly; sinks s1 (TRANSFORM); computed columns orders.total; code that calls it: function g, procedure h. Drop or change them first, or DROP FUNCTION f CASCADE',
      ),
    ).toEqual({ schedules: ['nightly', 'hourly'], sinks: ['s1'], columns: ['orders.total'], callers: ['function g', 'procedure h'] });
    expect(dependentsOf("function f is the TRANSFORM of sinks a, b: ALTER SINK ... or DROP SINK first (CASCADE doesn't drop sinks)")).toEqual({
      schedules: [],
      sinks: ['a', 'b'],
      columns: [],
      callers: [],
    });
    expect(dependentsOf('something else')).toBeNull();
  });
  it('finds the newest automatic rollback in the logs', () => {
    const row = (log_id: number, message: string) => ({ log_id, module_id: 1, version: 2, at: '', level: 'warn', source: 'rollout', message });
    expect(autoRollback([row(1, 'x'), row(3, 'rollout of version 2 rolled back by itself (error 39870): ...'), row(2, 'rolled back by itself')])?.log_id).toBe(3);
    expect(autoRollback([row(1, 'deployed')])).toBeNull();
  });
});
