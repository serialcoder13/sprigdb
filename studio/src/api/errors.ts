// Errors from the API (chapter 37, "Errors"): SprigDB's JSON body
// {code, name, message, sqlstate, trace_id} with the HTTP status.

export interface ErrorBody {
  code?: number;
  name?: string;
  message?: string;
  sqlstate?: string;
  trace_id?: string;
  retryable?: boolean;
}

export class ApiError extends Error {
  readonly status: number;
  readonly code: number;
  readonly errorName: string;
  readonly sqlstate: string;
  readonly traceId: string;

  constructor(status: number, body: ErrorBody, traceId: string) {
    super(body.message ?? `HTTP ${status}`);
    this.name = 'ApiError';
    this.status = status;
    this.code = body.code ?? 0;
    this.errorName = body.name ?? '';
    this.sqlstate = body.sqlstate ?? '';
    this.traceId = body.trace_id ?? traceId;
  }

  /** One line for people: code, name, message. */
  describe(): string {
    const head = this.code ? `${this.code}${this.errorName ? ` ${this.errorName}` : ''}: ` : '';
    return head + this.message;
  }
}

export function isApiError(e: unknown): e is ApiError {
  return e instanceof ApiError;
}

/** A message for any thrown value. */
export function errorText(e: unknown): string {
  if (isApiError(e)) return e.describe();
  if (e instanceof Error) return e.message;
  return String(e);
}

/** Session-ending refusals (chapter 44, "Failure handling"). */
export const SESSION_ENDED_CODES = new Set([1045, 39924, 39925]);
