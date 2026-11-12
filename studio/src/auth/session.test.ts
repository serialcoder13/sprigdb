import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { checkIdle, insecureLoginBlocked, login, logout, refresh, useSession } from './session';
import type { WhoAmI } from '../api/types';

const me: WhoAmI = {
  principal: { tenant: 'acme', tenant_id: '7', user: 'app', host: '%', login: 'acme.app' },
  amr: ['pwd'],
  session: { id: 's', client: 'studio', created_at_ms: '0', idle_minutes: 30, max_hours: 12, access_ttl_seconds: 900 },
  tenants: [{ name: 'acme', id: '7', path: 'acme', relation: 'home' }],
  grants: [],
  privileges: { tenant_admin: false, settings_admin: false, create_user: false, grant_option: false, index_all: false, index_dbs: [] },
  platform_roles: [],
  studio: {
    session_idle_minutes: 30,
    session_max_hours: 12,
    persist_drafts: false,
    history_max: 500,
    max_result_rows: 10,
    max_result_mb: 1,
    statement_timeout_ms: 1000,
    confirm_writes: 'ddl',
    export_ticket_seconds: 60,
    import_batch_rows: 10,
    import_max_mb: 1,
    feed_view_max_events: 10,
    graph_max_elements: 10,
    map_tile_url: '',
    view_as: 'on',
    operator_mode: 'on',
  },
};

function reply(status: number, body: unknown) {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
}

describe('session', () => {
  let calls: { url: string; init?: RequestInit }[];
  beforeEach(() => {
    calls = [];
    useSession.setState({ status: 'signed_out', token: null, me: null, idleWarning: false, allowInsecureHttp: false });
  });
  afterEach(() => vi.restoreAllMocks());

  it('logs in with the cookie transport and CSRF header, keeping the token in memory', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation((input, init) => {
      const url = input as string;
      calls.push({ url, init });
      if (url === '/v1/auth/login') return Promise.resolve(reply(200, { access_token: 'sprigat_a', token_type: 'Bearer', expires_in: 900, session_id: 's' }));
      if (url === '/v1/auth/me') return Promise.resolve(reply(200, me));
      return Promise.resolve(reply(404, {}));
    });
    await login('acme.app', 'pw');
    const s = useSession.getState();
    expect(s.status).toBe('active');
    expect(s.token).toBe('sprigat_a');
    const loginCall = calls[0]!;
    expect((loginCall.init?.headers as Record<string, string>)['x-sprig-csrf']).toBe('1');
    expect(JSON.parse(loginCall.init?.body as string)).toMatchObject({ refresh_transport: 'cookie' });
    const meCall = calls[1]!;
    expect((meCall.init?.headers as Record<string, string>).authorization).toBe('Bearer sprigat_a');
    // Nothing lands in storage.
    expect(document.cookie).toBe('');
  });

  it('shares one refresh between concurrent callers', async () => {
    let n = 0;
    vi.spyOn(globalThis, 'fetch').mockImplementation((input) => {
      if ((input as string) === '/v1/auth/refresh') {
        n++;
        return Promise.resolve(reply(200, { access_token: `sprigat_${n}`, token_type: 'Bearer', expires_in: 900, session_id: 's' }));
      }
      return Promise.resolve(reply(404, {}));
    });
    const [a, b] = await Promise.all([refresh(), refresh()]);
    expect(n).toBe(1);
    expect(a).toBe(b);
  });

  it('warns before the idle limit and signs out at it', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(reply(200, { ended: true }));
    const t0 = Date.now();
    useSession.setState({ status: 'active', me, lastActivity: t0, token: 'x' });
    checkIdle(t0 + 27 * 60_000);
    expect(useSession.getState().idleWarning).toBe(false);
    checkIdle(t0 + 28.5 * 60_000);
    expect(useSession.getState().idleWarning).toBe(true);
    checkIdle(t0 + 30 * 60_000);
    await vi.waitFor(() => expect(useSession.getState().status).toBe('signed_out'));
    await logout();
  });

  it('refuses a plain-HTTP password from another host', () => {
    const at = (href: string) => new URL(href) as unknown as Location;
    expect(insecureLoginBlocked(at('http://10.0.0.5:8080/studio/login'))).toBe(true);
    expect(insecureLoginBlocked(at('http://127.0.0.1:8080/studio/login'))).toBe(false);
    expect(insecureLoginBlocked(at('http://localhost:8080/studio/login'))).toBe(false);
    expect(insecureLoginBlocked(at('https://db.example.com/studio/login'))).toBe(false);
    useSession.setState({ allowInsecureHttp: true });
    expect(insecureLoginBlocked(at('http://10.0.0.5:8080/studio/login'))).toBe(false);
  });
});
