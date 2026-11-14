import { describe, expect, it } from 'vitest';
import { filterSql } from './filter';
import { pageQuery } from './edit';

describe('filter bar SQL', () => {
  it('joins conditions with AND and quotes names and values', () => {
    expect(
      filterSql([
        { col: 'status', op: '=', value: "o'pen" },
        { col: 'total', op: '>=', value: '100' },
      ]),
    ).toBe("`status` = 'o''pen' AND `total` >= '100'");
  });
  it('handles != , IN, LIKE and the NULL tests', () => {
    expect(filterSql([{ col: 'a', op: '!=', value: 'x' }])).toBe("`a` <> 'x'");
    expect(filterSql([{ col: 'a', op: 'IN', value: ' x, y ,, z' }])).toBe("`a` IN ('x', 'y', 'z')");
    expect(filterSql([{ col: 'a', op: 'IN', value: ' , ' }])).toBe('FALSE');
    expect(filterSql([{ col: 'a', op: 'NOT LIKE', value: '%q%' }])).toBe("`a` NOT LIKE '%q%'");
    expect(filterSql([{ col: 'a', op: 'IS NULL', value: 'ignored' }])).toBe('`a` IS NULL');
  });
  it('is null with no conditions', () => {
    expect(filterSql([])).toBeNull();
  });
  it('pages within the filter, after the key', () => {
    const t = { db: 'shop', table: 'orders', keyNames: ['id'] };
    expect(pageQuery(t, null, 101, "`status` = 'open'")).toBe(
      "SELECT * FROM `shop`.`orders` WHERE (`status` = 'open') ORDER BY `id` LIMIT 101",
    );
    expect(pageQuery(t, ['17'], 101, "`status` = 'open'")).toBe(
      "SELECT * FROM `shop`.`orders` WHERE (`status` = 'open') AND `id` > '17' ORDER BY `id` LIMIT 101",
    );
    expect(pageQuery({ ...t, keyNames: [] }, null, 5, 'a > 1')).toBe('SELECT * FROM `shop`.`orders` WHERE (a > 1) LIMIT 5');
  });
});
