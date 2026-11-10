import { describe, expect, it } from 'vitest';
import { confirmTyped, moveLag } from './ops';

describe('operator mode', () => {
  it('asks for the target name and a reason', () => {
    const answers = (a: (string | null)[]) => () => a.shift() ?? null;
    expect(confirmTyped('acme', 'Run', answers(['acme', 'hot shard']))).toBe('hot shard');
    expect(confirmTyped('acme', 'Run', answers(['acm', 'x']))).toBeNull();
    expect(confirmTyped('acme', 'Run', answers(['acme', '  ']))).toBeNull();
    expect(confirmTyped('acme', 'Run', answers([null]))).toBeNull();
  });
  it('shows lag in ms or seconds', () => {
    const j = { id: 'x', kind: 'move', tenant: 1, state: 'copying', source: 'a', target: 'b' };
    expect(moveLag({ ...j, progress: { lag_ms: 48 } })).toBe('48 ms');
    expect(moveLag({ ...j, progress: { lag_ms: 2500 } })).toBe('2.5 s');
    expect(moveLag({ ...j, progress: null })).toBe('');
  });
});
