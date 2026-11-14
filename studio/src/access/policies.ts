// Roles and row-level security policies as statements: built by the forms,
// shown before they run.

const ident = (s: string) => `\`${s.replace(/`/g, '``')}\``;
const account = (s: string) => {
  const [u, h] = s.split('@');
  if (!u) throw new Error('name the account');
  return `'${u.replace(/'/g, "''")}'@'${(h ?? '%').replace(/'/g, "''")}'`;
};

/** The session functions a policy may call. */
export const POLICY_FUNCTIONS = [
  'CURRENT_USER()',
  'SPRIG_USER_ID()',
  'SPRIG_TENANT_ID()',
  'SPRIG_HOME_TENANT_ID()',
  "SPRIG_CLAIM('sub')",
  "SPRIG_USER_ATTR('k')",
  'SPRIG_SESSION_SPACE()',
];

/** `ALTER TABLE ... ENABLE | DISABLE | FORCE | NO FORCE ROW LEVEL SECURITY`. */
export function rowSecurity(db: string, table: string, mode: 'ENABLE' | 'DISABLE' | 'FORCE' | 'NO FORCE'): string {
  if (!table) throw new Error('choose the table');
  return `ALTER TABLE ${ident(db)}.${ident(table)} ${mode} ROW LEVEL SECURITY`;
}

/** `DROP POLICY name ON db.table`. */
export function dropPolicy(name: string, db: string, table: string): string {
  if (!name || !table) throw new Error('name the policy and its table');
  return `DROP POLICY ${ident(name)} ON ${ident(db)}.${ident(table)}`;
}

export function createRole(role: string): string {
  return `CREATE ROLE ${account(role)}`;
}

const PRIVS = /^(ALL( PRIVILEGES)?|SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|INDEX|EXECUTE|CREATE ROUTINE|ALTER ROUTINE|CREATE VIEW|SHOW VIEW|TRIGGER|EVENT|REFERENCES|LOCK TABLES|CREATE TEMPORARY TABLES)$/i;

/** `GRANT privs ON db.obj TO role`. */
export function grantToRole(privs: string, on: string, role: string): string {
  const list = privs
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);
  if (!list.length || !list.every((p) => PRIVS.test(p))) throw new Error('privileges are names such as SELECT, INSERT, ALL');
  const m = /^([^.]+)\.([^.]+)$/.exec(on.trim());
  if (!m) throw new Error('grant on db.* or db.table');
  const part = (x: string) => (x === '*' ? '*' : ident(x));
  return `GRANT ${list.map((p) => p.toUpperCase()).join(', ')} ON ${part(m[1]!)}.${part(m[2]!)} TO ${account(role)}`;
}

export function grantRole(role: string, user: string): string {
  return `GRANT ${account(role)} TO ${account(user)}`;
}

export interface PolicyForm {
  name: string;
  db: string;
  table: string;
  kind: 'PERMISSIVE' | 'RESTRICTIVE';
  command: 'ALL' | 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE';
  to: string;
  using: string;
  check: string;
}

/** `CREATE POLICY ...` in its canonical form. */
export function createPolicy(f: PolicyForm): string {
  if (!f.name || !f.table) throw new Error('name the policy and its table');
  const to = f.to.trim() ? f.to.split(',').map((x) => (x.trim().toUpperCase() === 'PUBLIC' ? 'PUBLIC' : ident(x.trim()))).join(', ') : 'PUBLIC';
  if (f.command === 'INSERT' && f.using.trim()) throw new Error('an INSERT policy has only WITH CHECK');
  if (f.command !== 'INSERT' && !f.using.trim()) throw new Error('USING is the rows the policy admits');
  const using = f.command !== 'INSERT' ? ` USING (${f.using.trim()})` : '';
  const checkText = f.check.trim() || (f.command === 'INSERT' ? '' : '');
  const check = checkText ? ` WITH CHECK (${checkText})` : '';
  if (f.command === 'INSERT' && !check) throw new Error('an INSERT policy needs WITH CHECK');
  return `CREATE POLICY ${ident(f.name)} ON ${ident(f.db)}.${ident(f.table)} AS ${f.kind} FOR ${f.command} TO ${to}${using}${check}`;
}
