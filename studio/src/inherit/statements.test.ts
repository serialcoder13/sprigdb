import { describe, expect, it } from 'vitest';
import { checkIds, checkReferences, origin, sharesByName, showCreateShare, tableName } from './statements';

describe('inheritance statements (chapter 42, plan step 7)', () => {
  it('builds the statements', () => {
    expect(tableName('shop.t2')).toBe('`shop`.`t2`');
    expect(() => tableName(' ')).toThrow();
    expect(showCreateShare('s`1')).toBe('SHOW CREATE SHARE `s``1`');
    expect(checkReferences('t2', 'id = 5')).toBe('SPRIG CHECK REFERENCES `t2` WHERE id = 5');
    expect(checkReferences('shop.t2', '  ')).toBe('SPRIG CHECK REFERENCES `shop`.`t2`');
    expect(checkIds('t9')).toBe('SPRIG CHECK IDS `t9`');
  });

  it('describes origins and groups shares', () => {
    expect(origin({ Levels_up: '0', Origin: 'a' })).toBe('declared here');
    expect(origin({ Levels_up: '2', Origin: 'acme' })).toBe('inherited from acme (2 levels up)');
    const g = sharesByName([
      { Share: 's1', Role: 'granted', Object: 'shop.t2', From: 'a', To: 'ALL CHILDREN' },
      { Share: 's1', Role: 'granted', Object: 'shop.t1', From: 'a', To: 'ALL CHILDREN' },
      { Share: 's1', Role: 'received', Object: 'shop.t1', From: 'a', To: 'b' },
    ]);
    // Tables in name order whatever order SHOW SHARES gave; received shares say where from.
    expect(g).toEqual([
      { name: 's1', role: 'granted', tables: ['shop.t1', 'shop.t2'], from: 'a', to: 'ALL CHILDREN' },
      { name: 's1', role: 'received', tables: ['shop.t1'], from: 'a', to: 'b' },
    ]);
  });
});
