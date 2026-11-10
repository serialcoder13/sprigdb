// Import parsing (chapter 44, "Import"): RFC 4180 CSV with a header row, or
// NDJSON (one object per line). Parsing is incremental, so a large file is
// read in slices.

import type { Cell } from '../api/types';

/** Parses CSV text into rows; `\N` and empty unquoted fields can mean NULL. */
export function parseCsv(text: string, emptyIsNull = true): Cell[][] {
  const rows: Cell[][] = [];
  let row: Cell[] = [];
  let field = '';
  let quoted = false;
  let wasQuoted = false;
  let i = 0;
  const endField = () => {
    row.push(!wasQuoted && ((emptyIsNull && field === '') || field === '\\N') ? null : field);
    field = '';
    wasQuoted = false;
  };
  while (i < text.length) {
    const c = text[i]!;
    if (quoted) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else quoted = false;
      } else field += c;
    } else if (c === '"' && field === '') {
      quoted = true;
      wasQuoted = true;
    } else if (c === ',') endField();
    else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      endField();
      rows.push(row);
      row = [];
    } else field += c;
    i++;
  }
  if (field !== '' || wasQuoted || row.length) {
    endField();
    rows.push(row);
  }
  return rows;
}

/** NDJSON objects as rows over `columns` (absent keys are NULL). */
export function parseNdjson(text: string): { columns: string[]; rows: Cell[][] } {
  const objs = text
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => JSON.parse(l) as Record<string, unknown>);
  const columns: string[] = [];
  for (const o of objs) for (const k of Object.keys(o)) if (!columns.includes(k)) columns.push(k);
  const rows = objs.map((o) =>
    columns.map((k) => {
      const v = o[k];
      if (v === null || v === undefined) return null;
      return typeof v === 'string' ? v : JSON.stringify(v);
    }),
  );
  return { columns, rows };
}
