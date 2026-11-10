import { describe, expect, it } from 'vitest';
import { account, classify, ident, literal, needsConfirmation, splitStatements, statementAt } from './sql';

describe('quoting', () => {
  it('quotes identifiers and literals', () => {
    expect(ident('a`b')).toBe('`a``b`');
    expect(literal("it's")).toBe("'it''s'");
    expect(literal('a\\b')).toBe("'a\\\\b'");
    expect(account('bob', '10.%')).toBe("'bob'@'10.%'");
  });
});

describe('splitting', () => {
  it('splits at semicolons outside quotes and comments', () => {
    const sql = "select ';' as a; -- x; y\nselect `a;b` from t /* ; */;\n\n# c;\nselect \"x\\\";\"";
    const s = splitStatements(sql);
    expect(s.map((x) => x.text)).toEqual([
      "select ';' as a",
      "-- x; y\nselect `a;b` from t /* ; */",
      '# c;\nselect "x\\";"',
    ]);
    for (const x of s) expect(sql.slice(x.start, x.end)).toBe(x.text);
  });
  it('drops empty and comment-only statements', () => {
    expect(splitStatements(' ; -- only\n ;')).toEqual([]);
  });
  it('finds the statement under the cursor', () => {
    const sql = 'select 1;\nselect 2;\n';
    expect(statementAt(sql, 0)?.text).toBe('select 1');
    expect(statementAt(sql, 12)?.text).toBe('select 2');
    expect(statementAt(sql, sql.length)?.text).toBe('select 2');
  });
});

describe('classify', () => {
  it.each([
    ['SELECT 1', 'read'],
    ['  /* hi */ show tables', 'read'],
    ['with x as (select 1) select * from x', 'read'],
    ['insert into t values (1)', 'write'],
    ['create table t (a int)', 'ddl'],
    ['ALTER TABLE t SPRIG SET (auto_index.mode = off)', 'ddl'],
    ['create user bob', 'account'],
    ["grant select on *.* to 'a'", 'account'],
    ['begin', 'session'],
    ['frobnicate', 'other'],
  ])('%s → %s', (sql, cls) => {
    expect(classify(sql)).toBe(cls);
  });
  it('confirm_writes modes', () => {
    expect(needsConfirmation('write', 'ddl')).toBe(false);
    expect(needsConfirmation('ddl', 'ddl')).toBe(true);
    expect(needsConfirmation('write', 'all')).toBe(true);
    expect(needsConfirmation('ddl', 'off')).toBe(false);
  });
});
