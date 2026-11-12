import { describe, expect, it } from 'vitest';
import { dependentsFromRows, noDependents, showDependents } from './declaration';

describe('Studio round 7: SHOW DEPENDENTS (ST67)', () => {
  it('asks only of functions and procedures', () => {
    expect(showDependents('shop', 'function', 'rate')).toBe('SHOW DEPENDENTS OF FUNCTION `shop`.`rate`');
    expect(showDependents('shop', 'PROCEDURE', 'nightly')).toBe('SHOW DEPENDENTS OF PROCEDURE `shop`.`nightly`');
    expect(showDependents('shop', 'trigger', 't')).toBeNull();
  });
  it('reads the rows by kind', () => {
    const d = dependentsFromRows(
      ['Kind', 'Name', 'Cascade', 'Detail'],
      [
        ['schedule', 'nightly', 'dropped', '…'],
        ['sink', 'out', 'refused', '…'],
        ['column', 'orders.total', 'kept', '…'],
        ['caller', 'function other', 'kept', '…'],
        ['something new', 'x', 'kept', '…'],
      ],
    );
    expect(d).toEqual({ schedules: ['nightly'], sinks: ['out'], columns: ['orders.total'], callers: ['function other'] });
    expect(noDependents(d)).toBe(false);
    expect(noDependents(dependentsFromRows(['Kind', 'Name', 'Cascade', 'Detail'], []))).toBe(true);
  });
});
