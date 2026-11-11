import { describe, expect, it } from 'vitest';
import { argLiteral, callStatement, declaration, lineOf, paramsText, rollbackStatement, sqlString, stepOf, template } from './declaration';
import { SPRIG_FUNCTIONS } from './sprigFunctions';

describe('logic declarations (step 26)', () => {
  it('builds the CREATE statements the parser reads', () => {
    expect(
      declaration('shop', { kind: 'function', name: 'total', runtime: 'javascript', params: 'id BIGINT', returns: 'DECIMAL(12,2)', security: 'invoker', tier: 'auto', body: 'return id * 2;' }),
    ).toBe("CREATE OR REPLACE FUNCTION `shop`.`total`(id BIGINT) RETURNS DECIMAL(12,2) LANGUAGE javascript SQL SECURITY INVOKER WITH (SPRIG_TIER = 'auto') AS $$\nreturn id * 2;\n$$");
    expect(
      declaration('shop', { kind: 'procedure', name: 'q`x', runtime: 'kirun', params: '', returns: '', security: 'definer', tier: 'mariadb', body: '{"name":"it\'s \\\\n"}' }),
    ).toBe("CREATE OR REPLACE PROCEDURE `shop`.`q``x` LANGUAGE kirun SQL SECURITY DEFINER WITH (SPRIG_TIER = 'mariadb') AS '{\"name\":\"it''s \\\\\\\\n\"}'");
    expect(() => declaration('d', { kind: 'function', name: 'f', runtime: 'lua', params: '', returns: '', security: 'invoker', tier: 'auto', body: 'x $$ y' })).toThrow();
    expect(sqlString("a'b\\c")).toBe("'a''b\\\\c'");
  });

  it('test runs are CALLs with literals by type', () => {
    const params = [
      { name: 'id', type: 'int' },
      { name: 'note', type: 'str', sql: 'VARCHAR(40)' },
      { name: 'ok', type: 'bool' },
    ];
    expect(callStatement('shop', 'f', params, { id: '7', note: "it's", ok: 'true' })).toBe("CALL `shop`.`f`(7, 'it''s', TRUE)");
    expect(callStatement('shop', 'f', params, {})).toBe('CALL `shop`.`f`(NULL, NULL, NULL)');
    expect(() => argLiteral('int', '7; DROP TABLE x')).toThrow();
    expect(argLiteral('DECIMAL(10,2)', '1.5')).toBe('1.5');
    expect(argLiteral('json', '{"a":1}')).toBe('\'{"a":1}\'');
  });

  it('rollback, parameters from a signature, reason locations', () => {
    expect(rollbackStatement('shop', 'function', 'f', 3)).toBe('ALTER FUNCTION `shop`.`f` ROLLBACK TO VERSION 3');
    expect(rollbackStatement('shop', 'procedure', 'p')).toBe('ALTER PROCEDURE `shop`.`p` ROLLBACK');
    expect(paramsText({ params: [{ name: 'a', type: 'int', sql: 'bigint' }, { name: 'b', type: 'str' }] })).toBe('a bigint, b LONGTEXT');
    expect(lineOf({ code: 'js.x', at: 'line 3, column 2', detail: '' })).toBe(3);
    expect(stepOf({ code: 'kirun.f', at: 'step item, parameter sql', detail: '' })).toBe('item');
    expect(stepOf({ code: 'x', at: 'line 1', detail: '' })).toBeNull();
  });

  it('templates parse as a KIRun definition and the Sprig functions are known', () => {
    const k = template('kirun', 'procedure', 'double');
    expect((JSON.parse(k.body) as { name: string }).name).toBe('double');
    expect(SPRIG_FUNCTIONS.map((f) => `${f.ns}.${f.name}`)).toContain('Sprig.Db.QueryOne');
  });
});
