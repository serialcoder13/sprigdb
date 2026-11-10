// Exports (chapter 44, "Exports"): a ticket from Exports.CreateTicket, then
// a hidden form POST /v1/exports. The browser's download manager handles any
// size, and no ticket appears in a URL.

import { json } from '../api/client';
import type { ExportTicket } from '../api/types';

export type ExportFormat = 'csv' | 'ndjson';

export async function exportAll(
  db: string | null,
  sql: string,
  format: ExportFormat,
  name = 'export',
): Promise<void> {
  const t = await json<ExportTicket>('/v1/exports:ticket', {
    body: { database: db ?? '-', sql, format, name },
  });
  const form = document.createElement('form');
  form.method = 'POST';
  form.action = '/v1/exports';
  form.style.display = 'none';
  const input = document.createElement('input');
  input.type = 'hidden';
  input.name = 'ticket';
  input.value = t.ticket;
  form.appendChild(input);
  document.body.appendChild(form);
  form.submit();
  form.remove();
}
