import { describe, expect, it } from 'vitest';
import { createPolicy, createRole, grantRole, grantToRole } from './policies';

describe('roles and policies (step 18)', () => {
  it('builds role statements', () => {
    expect(createRole('auditors')).toBe("CREATE ROLE 'auditors'@'%'");
    expect(grantToRole('select, insert', 'shop.*', 'auditors')).toBe("GRANT SELECT, INSERT ON `shop`.* TO 'auditors'@'%'");
    expect(grantToRole('SELECT', 'shop.orders', "o'k")).toBe("GRANT SELECT ON `shop`.`orders` TO 'o''k'@'%'");
    expect(() => grantToRole('SELECT; DROP', 'shop.*', 'r')).toThrow();
    expect(() => grantToRole('SELECT', 'shop', 'r')).toThrow();
    expect(grantRole('auditors', 'bob@localhost')).toBe("GRANT 'auditors'@'%' TO 'bob'@'localhost'");
  });

  it('builds CREATE POLICY as chapter 29 writes it', () => {
    expect(
      createPolicy({ name: 'own', db: 'shop', table: 'orders', kind: 'PERMISSIVE', command: 'SELECT', to: 'app_user', using: "owner_id = SPRIG_CLAIM('sub')", check: '' }),
    ).toBe("CREATE POLICY `own` ON `shop`.`orders` AS PERMISSIVE FOR SELECT TO `app_user` USING (owner_id = SPRIG_CLAIM('sub'))");
    expect(
      createPolicy({ name: 'ins', db: 'shop', table: 'orders', kind: 'PERMISSIVE', command: 'INSERT', to: '', using: '', check: "owner_id = SPRIG_CLAIM('sub')" }),
    ).toBe("CREATE POLICY `ins` ON `shop`.`orders` AS PERMISSIVE FOR INSERT TO PUBLIC WITH CHECK (owner_id = SPRIG_CLAIM('sub'))");
    expect(() => createPolicy({ name: 'x', db: 'd', table: 't', kind: 'PERMISSIVE', command: 'SELECT', to: '', using: '', check: '' })).toThrow();
  });
});
