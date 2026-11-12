// Logic declarations as the statements a SQL client would run (chapter 41,
// "Declaring code"): Studio builds them from the form and the editor, shows
// them, and runs them with Sql.Execute, so there is no second path.

import type { Reason, Signature } from './api';

export type Runtime = 'kirun' | 'javascript' | 'lua';
export type LogicKind = 'function' | 'procedure';

export interface DeclForm {
  kind: LogicKind;
  name: string;
  runtime: Runtime;
  /** Parameters as written: `id BIGINT, note VARCHAR(40)` (scripts only). */
  params: string;
  /** `RETURNS` type (script functions only). */
  returns: string;
  security: 'invoker' | 'definer';
  tier: 'auto' | 'mariadb' | 'engine';
  body: string;
  /** Deploy as a staged rollout (round 6, RC18): the candidate's share and key. */
  rollout?: { pct: number; by: RolloutBy } | null;
}

export type RolloutBy = 'user' | 'tenant';

export const ident = (s: string) => `\`${s.replace(/`/g, '``')}\``;

/** A string literal as the declaration parser reads it. */
export function sqlString(s: string): string {
  return `'${s.replace(/\\/g, '\\\\').replace(/'/g, "''")}'`;
}

/** `CREATE OR REPLACE ...` for the form. */
export function declaration(db: string, f: DeclForm): string {
  const head = `CREATE OR REPLACE ${f.kind.toUpperCase()} ${ident(db)}.${ident(f.name)}`;
  let staged = '';
  if (f.rollout) {
    const n = f.rollout.pct;
    if (!Number.isInteger(n) || n < 1 || n > 99) throw new Error('a rollout stage is 1 to 99 percent');
    staged = `, SPRIG_ROLLOUT = ${n}${f.rollout.by === 'tenant' ? ", SPRIG_ROLLOUT_BY = 'tenant'" : ''}`;
  }
  const opts = `SQL SECURITY ${f.security.toUpperCase()} WITH (SPRIG_TIER = '${f.tier}'${staged})`;
  if (f.runtime === 'kirun') return `${head} LANGUAGE kirun ${opts} AS ${sqlString(f.body)}`;
  if (f.body.includes('$$')) throw new Error('the body may not contain $$');
  const params = `(${f.params.trim()})`;
  const returns = f.kind === 'function' && f.returns.trim() ? ` RETURNS ${f.returns.trim()}` : '';
  return `${head}${params}${returns} LANGUAGE ${f.runtime} ${opts} AS $$\n${f.body}\n$$`;
}

/** Parameters as written, from a version's signature (scripts keep their SQL types). */
export function paramsText(sig: Signature | null | undefined): string {
  return (sig?.params ?? []).map((p) => `${p.name} ${p.sql ?? sqlTypeOf(p.type)}`).join(', ');
}

function sqlTypeOf(t: string): string {
  switch (t) {
    case 'int':
      return 'BIGINT';
    case 'float':
      return 'DOUBLE';
    case 'bool':
      return 'BOOLEAN';
    case 'json':
      return 'JSON';
    default:
      return 'LONGTEXT';
  }
}

/** A test run's argument as a SQL literal, by the parameter's type. */
export function argLiteral(type: string, text: string): string {
  const t = text.trim();
  if (t === '' || /^null$/i.test(t)) return 'NULL';
  const lower = (type || '').toLowerCase();
  if (/^(int|bigint|integer|smallint|tinyint|mediumint|float|double|decimal|numeric|real)/.test(lower)) {
    if (!/^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/.test(t)) throw new Error(`'${t}' isn't a number`);
    return t;
  }
  if (lower === 'bool' || lower === 'boolean') {
    if (/^(true|1)$/i.test(t)) return 'TRUE';
    if (/^(false|0)$/i.test(t)) return 'FALSE';
    throw new Error(`'${t}' isn't true or false`);
  }
  return sqlString(text);
}

/** `CALL db.name(args)` for a test run. */
export function callStatement(db: string, name: string, params: { name: string; type: string; sql?: string }[], values: Record<string, string>): string {
  const args = params.map((p) => argLiteral(p.sql ?? p.type, values[p.name] ?? ''));
  return `CALL ${ident(db)}.${ident(name)}(${args.join(', ')})`;
}

/** `ALTER ... ROLLBACK [TO VERSION n]`. */
export function rollbackStatement(db: string, kind: LogicKind, name: string, version?: number): string {
  return `ALTER ${kind.toUpperCase()} ${ident(db)}.${ident(name)} ROLLBACK${version ? ` TO VERSION ${version}` : ''}`;
}

/** The line a reason points at (`line 3`, `line 3, column 5`), if any. */
export function lineOf(r: Reason): number | null {
  const m = /line (\d+)/.exec(r.at);
  return m ? Number(m[1]) : null;
}

/** The KIRun step a reason points at (`step item`, `step item, parameter sql`). */
export function stepOf(r: Reason): string | null {
  const m = /^step ([^,]+)/.exec(r.at);
  return m ? m[1]!.trim() : null;
}

/** Templates for a new module. */
export function template(runtime: Runtime, kind: LogicKind, name: string): Pick<DeclForm, 'params' | 'returns' | 'body'> {
  if (runtime === 'kirun') {
    return {
      params: '',
      returns: '',
      body: JSON.stringify(
        {
          name,
          namespace: 'App',
          parameters: { a: { parameterName: 'a', schema: { name: 'a', type: 'INTEGER' } } },
          events: { output: { name: 'output', parameters: { result: { name: 'result', type: 'INTEGER' } } } },
          steps: {
            done: {
              statementName: 'done',
              namespace: 'System',
              name: 'GenerateEvent',
              parameterMap: {
                eventName: { one: { key: 'one', type: 'VALUE', value: 'output' } },
                results: {
                  one: { key: 'one', type: 'VALUE', value: { name: 'result', value: { isExpression: true, value: 'Arguments.a * 2' } } },
                },
              },
            },
          },
        },
        null,
        2,
      ),
    };
  }
  if (runtime === 'lua') return { params: 'a BIGINT', returns: kind === 'function' ? 'BIGINT' : '', body: 'return a * 2' };
  return { params: 'a BIGINT', returns: kind === 'function' ? 'BIGINT' : '', body: 'return a * 2;' };
}

export type RolloutAction = { to: number; by?: RolloutBy } | 'promote' | 'abort';

/** `ALTER FUNCTION | PROCEDURE db.f ROLLOUT TO n [BY USER | TENANT] | PROMOTE | ABORT` (RC19). */
export function rolloutStatement(db: string, kind: LogicKind, name: string, a: RolloutAction): string {
  const head = `ALTER ${kind.toUpperCase()} ${ident(db)}.${ident(name)} ROLLOUT`;
  if (a === 'promote') return `${head} PROMOTE`;
  if (a === 'abort') return `${head} ABORT`;
  if (!Number.isInteger(a.to) || a.to < 1 || a.to > 99) throw new Error('a rollout stage is 1 to 99 percent (100 promotes, 0 aborts)');
  return `${head} TO ${a.to}${a.by ? ` BY ${a.by.toUpperCase()}` : ''}`;
}

/** The kinds `DROP` takes for a logic module. */
export const DROP_KINDS = ['function', 'procedure', 'validation', 'trigger', 'schedule'] as const;

/** `DROP {FUNCTION | ...} db.name RESTRICT | CASCADE` (RC22). */
export function dropStatement(db: string, kind: string, name: string, cascade: boolean): string {
  const k = kind.toLowerCase();
  if (!(DROP_KINDS as readonly string[]).includes(k)) throw new Error(`a ${kind} isn't dropped with DROP`);
  return `DROP ${k.toUpperCase()} ${ident(db)}.${ident(name)} ${cascade ? 'CASCADE' : 'RESTRICT'}`;
}

export interface Dependents {
  schedules: string[];
  sinks: string[];
  columns: string[];
  callers: string[];
}

/** `SHOW DEPENDENTS OF FUNCTION | PROCEDURE db.name` (ST67): what `DROP ... RESTRICT` would refuse, as rows. */
export function showDependents(db: string, kind: string, name: string): string | null {
  const k = kind.toLowerCase();
  if (k !== 'function' && k !== 'procedure') return null;
  return `SHOW DEPENDENTS OF ${k.toUpperCase()} ${ident(db)}.${ident(name)}`;
}

/** `SHOW DEPENDENTS`' rows (`Kind`, `Name`, `Cascade`, `Detail`) as lists by kind. */
export function dependentsFromRows(columns: readonly string[], rows: readonly (readonly (string | null)[])[]): Dependents {
  const at = (n: string) => columns.findIndex((c) => c.toLowerCase() === n);
  const [k, n] = [at('kind'), at('name')];
  const d: Dependents = { schedules: [], sinks: [], columns: [], callers: [] };
  const into: Record<string, keyof Dependents> = { schedule: 'schedules', sink: 'sinks', column: 'columns', caller: 'callers' };
  for (const row of rows) {
    const key = into[(row[k] ?? '').toLowerCase()];
    const name = row[n];
    if (key && name) d[key].push(name);
  }
  return d;
}

/** Whether nothing uses the module. */
export function noDependents(d: Dependents): boolean {
  return !d.schedules.length && !d.sinks.length && !d.columns.length && !d.callers.length;
}

/** What 39875 says still uses the module (`... is in use: schedules a, b; sinks s (TRANSFORM); computed columns t.c; code that calls it: function g. Drop or ...`). */
export function dependentsOf(message: string): Dependents | null {
  const m = /is in use: (.*?)\. Drop or change them first/s.exec(message);
  const sink = /is the TRANSFORM of sinks (.*?):/s.exec(message);
  if (!m && !sink) return null;
  const d: Dependents = { schedules: [], sinks: [], columns: [], callers: [] };
  if (sink) d.sinks = sink[1]!.split(',').map((x) => x.trim()).filter(Boolean);
  for (const part of (m?.[1] ?? '').split('; ')) {
    const list = (rest: string) => rest.split(',').map((x) => x.trim()).filter(Boolean);
    if (part.startsWith('schedules ')) d.schedules = list(part.slice('schedules '.length));
    else if (part.startsWith('sinks ')) d.sinks = list(part.slice('sinks '.length).replace(/ \(TRANSFORM\)$/, ''));
    else if (part.startsWith('computed columns ')) d.columns = list(part.slice('computed columns '.length));
    else if (part.startsWith('code that calls it: ')) d.callers = list(part.slice('code that calls it: '.length));
  }
  return d;
}
