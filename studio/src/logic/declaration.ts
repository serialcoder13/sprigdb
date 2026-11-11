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
}

export const ident = (s: string) => `\`${s.replace(/`/g, '``')}\``;

/** A string literal as the declaration parser reads it. */
export function sqlString(s: string): string {
  return `'${s.replace(/\\/g, '\\\\').replace(/'/g, "''")}'`;
}

/** `CREATE OR REPLACE ...` for the form. */
export function declaration(db: string, f: DeclForm): string {
  const head = `CREATE OR REPLACE ${f.kind.toUpperCase()} ${ident(db)}.${ident(f.name)}`;
  const opts = `SQL SECURITY ${f.security.toUpperCase()} WITH (SPRIG_TIER = '${f.tier}')`;
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
