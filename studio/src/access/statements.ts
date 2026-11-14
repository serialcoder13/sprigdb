// Account statements Studio's forms generate: the statements a SQL client
// would run, shown before they run. Passwords
// never appear here: they go through Users.SetPassword.

import { account, ident } from '../lib/sql';

export const PRIVILEGES = [
  'SELECT',
  'INSERT',
  'UPDATE',
  'DELETE',
  'CREATE',
  'DROP',
  'ALTER',
  'INDEX',
  'CREATE VIEW',
  'SHOW VIEW',
  'CREATE ROUTINE',
  'ALTER ROUTINE',
  'EXECUTE',
  'TRIGGER',
  'REFERENCES',
  'ALL PRIVILEGES',
] as const;

export function grantTarget(db: string, table: string): string {
  if (!db || db === '*') return '*.*';
  return `${ident(db)}.${table && table !== '*' ? ident(table) : '*'}`;
}

/** CREATE USER, locked until its password is set. */
export function createUser(user: string, host: string): string[] {
  return [`CREATE USER ${account(user, host)} ACCOUNT LOCK`];
}

export function unlockUser(user: string, host: string): string {
  return `ALTER USER ${account(user, host)} ACCOUNT UNLOCK`;
}

export function lockUser(user: string, host: string): string {
  return `ALTER USER ${account(user, host)} ACCOUNT LOCK`;
}

export function dropUser(user: string, host: string): string {
  return `DROP USER ${account(user, host)}`;
}

export function grant(privs: string[], db: string, table: string, user: string, host: string, withGrant: boolean): string {
  return `GRANT ${privs.join(', ')} ON ${grantTarget(db, table)} TO ${account(user, host)}${withGrant ? ' WITH GRANT OPTION' : ''}`;
}

export function revoke(privs: string[], db: string, table: string, user: string, host: string): string {
  return `REVOKE ${privs.join(', ')} ON ${grantTarget(db, table)} FROM ${account(user, host)}`;
}

export function showGrants(user: string, host: string): string {
  return `SHOW GRANTS FOR ${account(user, host)}`;
}
