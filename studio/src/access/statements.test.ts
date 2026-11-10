import { describe, expect, it } from 'vitest';
import { createUser, grant, grantTarget, revoke } from './statements';

describe('account statements', () => {
  it('never carry a password', () => {
    expect(createUser('bob', '%')).toEqual(["CREATE USER 'bob'@'%' ACCOUNT LOCK"]);
  });
  it('grants and revokes', () => {
    expect(grantTarget('', '')).toBe('*.*');
    expect(grant(['SELECT', 'INSERT'], 'shop', '', "o'neil", '%', false)).toBe(
      "GRANT SELECT, INSERT ON `shop`.* TO 'o''neil'@'%'",
    );
    expect(revoke(['INDEX'], 'shop', 'orders', 'a', '10.%')).toBe("REVOKE INDEX ON `shop`.`orders` FROM 'a'@'10.%'");
    expect(grant(['ALL PRIVILEGES'], '*', '', 'a', '%', true)).toBe("GRANT ALL PRIVILEGES ON *.* TO 'a'@'%' WITH GRANT OPTION");
  });
});
