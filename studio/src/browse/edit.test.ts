import { describe, expect, it } from 'vitest';
import { pageQuery, readOnlyColumns, rereadFor, statementFor, type TableRef } from './edit';

const t: TableRef = {
  db: 'shop',
  table: 'orders',
  columns: [
    { name: 'id', type: 'LONG', mysql_type: 3 },
    { name: 'status', type: 'VAR_STRING', mysql_type: 253 },
    { name: 'total', type: 'DOUBLE', mysql_type: 5 },
    { name: 'note', type: 'BLOB', mysql_type: 252 },
  ],
  key: [0],
  readOnly: new Set(),
};

describe('optimistic edit statements', () => {
  it('updates with every column read as the condition', () => {
    const s = statementFor(t, {
      kind: 'update',
      row: 0,
      original: ['17', 'open', '9.5', null],
      values: new Map([[1, 'paid']]),
    });
    expect(s).toBe(
      "UPDATE `shop`.`orders` SET `status` = 'paid' WHERE `id` <=> '17' AND `status` <=> 'open' AND CAST(`total` AS CHAR) <=> '9.5' AND `note` <=> NULL LIMIT 1",
    );
  });
  it('deletes and inserts', () => {
    expect(statementFor(t, { kind: 'delete', row: 0, original: ['1', "it's", null, null] })).toContain("`status` <=> 'it''s'");
    expect(statementFor(t, { kind: 'insert', row: 0, values: new Map([[0, '2'], [3, null]]) })).toBe(
      "INSERT INTO `shop`.`orders` (`id`, `note`) VALUES ('2', NULL)",
    );
  });
  it('never writes read-only columns', () => {
    const ro = { ...t, readOnly: new Set([1]) };
    expect(statementFor(ro, { kind: 'update', row: 0, original: ['1', 'a', '1', null], values: new Map([[1, 'x'], [3, 'y']]) })).toContain(
      "SET `note` = 'y'",
    );
    expect(readOnlyColumns([{ name: '_sprig_v', type: 'x', mysql_type: 3 }, { name: 'b', type: 'x', mysql_type: 252, binary: true }, { name: 'g', type: 'x', mysql_type: 3 }], new Set(['g']))).toEqual(
      new Set([0, 1, 2]),
    );
  });
  it('re-reads by key and pages by keyset', () => {
    expect(rereadFor(t, ['17', 'x', null, null])).toBe("SELECT * FROM `shop`.`orders` WHERE `id` <=> '17'");
    expect(pageQuery({ db: 'shop', table: 'orders', keyNames: ['id'] }, null, 101)).toBe(
      'SELECT * FROM `shop`.`orders` ORDER BY `id` LIMIT 101',
    );
    expect(pageQuery({ db: 's', table: 't', keyNames: ['a', 'b'] }, ['1', 'x'], 11)).toBe(
      "SELECT * FROM `s`.`t` WHERE (`a`, `b`) > ('1', 'x') ORDER BY `a`, `b` LIMIT 11",
    );
  });
});
