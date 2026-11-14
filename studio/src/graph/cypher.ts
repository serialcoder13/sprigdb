// Cypher and GQL text in the editor: splitting on `;`
// outside strings, quoted names and comments (`//`, `/* */`), and a class
// for studio.confirm_writes. The gateway decides every right; the class only
// chooses whether to ask first.

import type { Statement, StatementClass } from '../lib/sql';

/** Statements of a Cypher or GQL text with their UTF-16 offsets. */
export function splitCypher(text: string): Statement[] {
  const out: Statement[] = [];
  let start = 0;
  let i = 0;
  const n = text.length;
  const push = (end: number) => {
    const raw = text.slice(start, end);
    const lead = raw.length - raw.trimStart().length;
    const body = raw.trim();
    if (body && !/^(\/\/[^\n]*\n?\s*|\/\*[\s\S]*?\*\/\s*)+$/.test(body)) {
      out.push({ text: body, start: start + lead, end: start + lead + body.length });
    }
  };
  while (i < n) {
    const c = text[i]!;
    const d = text[i + 1];
    if (c === '/' && d === '/') {
      const nl = text.indexOf('\n', i);
      i = nl < 0 ? n : nl + 1;
    } else if (c === '/' && d === '*') {
      const e = text.indexOf('*/', i + 2);
      i = e < 0 ? n : e + 2;
    } else if (c === "'" || c === '"' || c === '`') {
      i++;
      while (i < n && text[i] !== c) {
        if (text[i] === '\\' && c !== '`') i++;
        i++;
      }
      i++;
    } else if (c === ';') {
      push(i);
      i++;
      start = i;
    } else i++;
  }
  push(n);
  return out;
}

/** The statement under offset `at`, or the last one before it. */
export function cypherAt(text: string, at: number): Statement | null {
  const all = splitCypher(text);
  let best: Statement | null = null;
  for (const s of all) {
    if (s.start <= at) best = s;
    if (at >= s.start && at <= s.end + 1) return s;
  }
  return best ?? all[0] ?? null;
}

/** Text with strings, quoted names and comments blanked, upper-cased. */
function words(text: string): string {
  return text
    .replace(/\/\/[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"|`[^`]*`/g, ' ')
    .toUpperCase();
}

/** read, write or ddl, for the confirmation of studio.confirm_writes. */
export function classifyCypher(text: string): StatementClass {
  const w = words(text);
  if (/\b(CREATE|DROP)\s+(OR\s+REPLACE\s+)?(GRAPH|PROPERTY\s+GRAPH|INDEX|CONSTRAINT|RANGE\s+INDEX|TEXT\s+INDEX)\b/.test(w)) return 'ddl';
  if (/\b(CREATE|MERGE|SET|DELETE|REMOVE|INSERT|DETACH|FOREACH)\b/.test(w)) return 'write';
  return 'read';
}

/** Cypher's keywords for completion and the tokenizer. */
export const CYPHER_KEYWORDS = [
  'MATCH', 'OPTIONAL', 'WHERE', 'RETURN', 'WITH', 'UNWIND', 'ORDER', 'BY', 'SKIP', 'LIMIT', 'DISTINCT',
  'UNION', 'ALL', 'CREATE', 'MERGE', 'ON', 'SET', 'DELETE', 'DETACH', 'REMOVE', 'CALL', 'YIELD', 'AS',
  'AND', 'OR', 'XOR', 'NOT', 'IN', 'IS', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'EXISTS', 'COUNT', 'COLLECT', 'FOREACH', 'EXPLAIN', 'PROFILE', 'ASC', 'DESC', 'STARTS', 'ENDS', 'CONTAINS',
  'SHORTEST', 'ANY', 'FILTER', 'LET', 'FOR', 'NEXT', 'INSERT', 'USE', 'GRAPH', 'INDEX', 'CONSTRAINT',
  'shortestPath', 'allShortestPaths', 'id', 'elementId', 'labels', 'type', 'properties', 'keys', 'size',
  'length', 'nodes', 'relationships', 'coalesce', 'toInteger', 'toString', 'toFloat', 'sum', 'avg', 'min', 'max',
];
