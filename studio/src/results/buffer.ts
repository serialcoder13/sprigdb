// A result held in the browser (chapter 44, "Result buffer"): rows go into
// column arrays and stop at studio.max_result_rows or studio.max_result_mb,
// whichever comes first.

import type { Cell, Column, Row } from '../api/types';

export class ResultBuffer {
  readonly columns: Column[];
  /** One array per column. */
  readonly cols: Cell[][];
  rowCount = 0;
  /** Estimated bytes held (UTF-16 strings). */
  bytes = 0;
  truncated = false;

  constructor(
    columns: Column[],
    readonly maxRows: number,
    readonly maxBytes: number,
  ) {
    this.columns = columns;
    this.cols = columns.map(() => []);
  }

  /** Whether either cap is reached. */
  get full(): boolean {
    return this.rowCount >= this.maxRows || this.bytes >= this.maxBytes;
  }

  /** Adds rows until full; false once full (the caller cancels the stream). */
  push(rows: Row[]): boolean {
    for (const row of rows) {
      if (this.full) {
        this.truncated = true;
        return false;
      }
      for (let c = 0; c < this.cols.length; c++) {
        const v = row[c] ?? null;
        this.cols[c]!.push(v);
        this.bytes += v === null ? 8 : 16 + v.length * 2;
      }
      this.rowCount++;
    }
    return !this.full;
  }

  cell(row: number, col: number): Cell {
    return this.cols[col]?.[row] ?? null;
  }

  row(row: number): Row {
    return this.cols.map((c) => c[row] ?? null);
  }

  /** Whether a column holds numbers (MySQL numeric types). */
  numeric(col: number): boolean {
    const t = this.columns[col]?.mysql_type ?? -1;
    return [0, 1, 2, 3, 4, 5, 8, 9, 13, 246].includes(t);
  }
}
