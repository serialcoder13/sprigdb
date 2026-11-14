// SQL text helpers for the editor and the forms. The gateway parses and
// decides everything; these only split text into statements, find the
// one under the cursor, classify it for confirmations (studio.confirm_writes)
// and quote names and literals in the statements forms generate.

/** `name` as a MySQL identifier: backquoted, inner backquotes doubled. */
export function ident(name: string): string {
  return '`' + name.replaceAll('`', '``') + '`';
}

/** `db`.`name`, or `name` alone without a database. */
export function qualified(db: string | null | undefined, name: string): string {
  return db ? `${ident(db)}.${ident(name)}` : ident(name);
}

/** A MySQL string literal: quotes doubled, backslashes and NUL escaped. */
export function literal(value: string): string {
  return (
    "'" +
    value.replaceAll('\\', '\\\\').replaceAll("'", "''").replaceAll('\0', '\\0') +
    "'"
  );
}

/** A value for SQL: NULL, or a string literal (MySQL converts as needed). */
export function sqlValue(v: string | null): string {
  return v === null ? 'NULL' : literal(v);
}

/** `'user'@'host'`. */
export function account(user: string, host = '%'): string {
  return `${literal(user)}@${literal(host)}`;
}

export interface Statement {
  text: string;
  start: number;
  end: number;
}

/**
 * Splits text into statements at semicolons outside quotes, backquotes and
 * comments. Empty statements are dropped; offsets point into `sql`.
 */
export function splitStatements(sql: string): Statement[] {
  const out: Statement[] = [];
  let start = 0;
  let i = 0;
  const n = sql.length;
  const push = (end: number) => {
    const raw = sql.slice(start, end);
    const lead = raw.length - raw.trimStart().length;
    const text = raw.trim();
    if (text && !/^(--[^\n]*\n?|#[^\n]*\n?|\/\*[\s\S]*?\*\/|\s)*$/.test(text)) {
      out.push({ text, start: start + lead, end: start + lead + text.length });
    }
  };
  while (i < n) {
    const c = sql[i];
    const next = sql[i + 1];
    if (c === "'" || c === '"' || c === '`') {
      i++;
      while (i < n && sql[i] !== c) {
        if (sql[i] === '\\' && c !== '`') i++;
        i++;
      }
      i++;
    } else if ((c === '-' && next === '-' && /\s/.test(sql[i + 2] ?? ' ')) || c === '#') {
      while (i < n && sql[i] !== '\n') i++;
    } else if (c === '/' && next === '*') {
      const close = sql.indexOf('*/', i + 2);
      i = close < 0 ? n : close + 2;
    } else if (c === ';') {
      push(i);
      i++;
      start = i;
    } else {
      i++;
    }
  }
  push(n);
  return out;
}

/** The statement containing `offset`, or the last one before it. */
export function statementAt(sql: string, offset: number): Statement | null {
  const all = splitStatements(sql);
  let best: Statement | null = null;
  for (const s of all) {
    if (offset >= s.start && offset <= s.end + 1) return s;
    if (s.end < offset) best = s;
  }
  return best ?? all[0] ?? null;
}

export type StatementClass = 'read' | 'write' | 'ddl' | 'account' | 'session' | 'other';

function firstWords(stmt: string): string[] {
  const stripped = stmt
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/(^|\n)\s*(--|#)[^\n]*/g, ' ')
    .trim()
    .replace(/^\(+/, '');
  return stripped.split(/\s+/, 4).map((w) => w.toUpperCase());
}

/** What a statement does, for confirmations; the server decides rights. */
export function classify(stmt: string): StatementClass {
  const [a = '', b = '', c = ''] = firstWords(stmt);
  switch (a) {
    case 'SELECT':
    case 'SHOW':
    case 'DESCRIBE':
    case 'DESC':
    case 'EXPLAIN':
    case 'VALUES':
    case 'TABLE':
    case 'HELP':
      return 'read';
    case 'WITH':
      return /\b(UPDATE|DELETE|INSERT)\b/i.test(stmt) ? 'write' : 'read';
    case 'INSERT':
    case 'UPDATE':
    case 'DELETE':
    case 'REPLACE':
    case 'LOAD':
    case 'CALL':
    case 'DO':
      return 'write';
    case 'CREATE':
    case 'ALTER':
    case 'DROP':
      if (b === 'USER' || b === 'ROLE' || (b === 'TENANT' && c !== 'SPRIG')) return 'account';
      return 'ddl';
    case 'TRUNCATE':
    case 'RENAME':
    case 'SPRIG':
      return 'ddl';
    case 'GRANT':
    case 'REVOKE':
      return 'account';
    case 'SET':
    case 'USE':
    case 'BEGIN':
    case 'START':
    case 'COMMIT':
    case 'ROLLBACK':
      return 'session';
    default:
      return 'other';
  }
}

/** Whether studio.confirm_writes asks before running a statement. */
export function needsConfirmation(cls: StatementClass, mode: string): boolean {
  if (mode === 'off') return false;
  if (mode === 'all') return cls !== 'read' && cls !== 'session';
  return cls === 'ddl' || cls === 'account' || cls === 'other';
}
