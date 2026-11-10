import { describe, expect, it } from 'vitest';
import { boundsStatement, parseBounds, precheck, resetStatement, setStatement, showStatement, settingValue } from './settings';

describe('settings statements', () => {
  it('builds statements per scope', () => {
    expect(showStatement({ kind: 'tenant', tenant: null }, 'studio.%')).toBe("SHOW SPRIG SETTINGS LIKE 'studio.%' FOR TENANT");
    expect(setStatement({ kind: 'table', db: 'shop', table: 'orders' }, 'auto_index.mode', 'recommend')).toBe(
      "ALTER TABLE `shop`.`orders` SPRIG SET (auto_index.mode = 'recommend')",
    );
    expect(setStatement({ kind: 'database', db: 'shop' }, 'studio.max_result_rows', '500')).toBe(
      'ALTER DATABASE `shop` SPRIG SET (studio.max_result_rows = 500)',
    );
    expect(resetStatement({ kind: 'tenant', tenant: 'pune' }, 'auth.password_lock_minutes')).toBe(
      'ALTER TENANT `pune` SPRIG RESET (auth.password_lock_minutes)',
    );
    expect(boundsStatement({ kind: 'tenant', tenant: null }, 'a.b', { max: '6', allowed: 'on, recommend', locked: true })).toBe(
      "ALTER TENANT SPRIG SET BOUNDS (a.b MAX 6 ALLOWED ('on', 'recommend') LOCKED)",
    );
    expect(settingValue('ON')).toBe('on');
    expect(settingValue("it's")).toBe("'it''s'");
  });
  it('pre-checks bounds', () => {
    const b = parseBounds("min 1, max 6, allowed ('on', 'off')");
    expect(b).toEqual({ min: 1, max: 6, allowed: ['on', 'off'], locked: false });
    expect(precheck('on', b)).toBeNull();
    expect(precheck('x', b)).toMatch(/Allowed/);
    expect(precheck('9', { locked: false, max: 6 })).toMatch(/At most 6/);
    expect(precheck('1', parseBounds('locked'))).toMatch(/Locked/);
    expect(parseBounds('none')).toEqual({ locked: false });
  });
});
