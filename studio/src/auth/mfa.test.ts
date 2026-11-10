import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { completeMfa, confirmEnrolment, impersonate, login, stopImpersonating, useSession } from './session';
import type { WhoAmI } from '../api/types';

const me = (login: string, impersonator: string | null = null) =>
  ({
    principal: { tenant: 'acme', tenant_id: '7', user: login.split('.')[1], host: '%', login },
    amr: ['pwd', 'otp'],
    session: { id: 's', client: 'studio', created_at_ms: '0', idle_minutes: 30, max_hours: 12, access_ttl_seconds: 900 },
    tenants: [],
    grants: [],
    privileges: { tenant_admin: true, settings_admin: true, create_user: true, grant_option: true, index_all: true, index_dbs: [] },
    platform_roles: [],
    studio: {},
    impersonator,
  }) as unknown as WhoAmI;

function reply(status: number, body: unknown) {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
}

describe('second factors and impersonation', () => {
  let calls: { url: string; init?: RequestInit }[];
  beforeEach(() => {
    calls = [];
    useSession.setState({ status: 'signed_out', token: null, me: null, impersonating: null, allowInsecureHttp: false });
  });
  afterEach(() => vi.restoreAllMocks());

  it('a challenge stops the login until a code completes it', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation((input, init) => {
      const url = input as string;
      calls.push({ url, init });
      if (url === '/v1/auth/login') return Promise.resolve(reply(200, { challenge: 'c1', methods: ['totp', 'recovery'], expires_in: 300 }));
      if (url === '/v1/auth/mfa') return Promise.resolve(reply(200, { access_token: 'sprigat_m', token_type: 'Bearer', expires_in: 900, session_id: 's' }));
      if (url === '/v1/auth/me') return Promise.resolve(reply(200, me('acme.app')));
      return Promise.resolve(reply(404, {}));
    });
    const step = await login('acme.app', 'pw');
    expect(step).toEqual({ kind: 'challenge', challenge: 'c1', expiresIn: 300 });
    expect(useSession.getState().token).toBeNull();
    await completeMfa('c1', ' 123456 ');
    const mfa = calls.find((c) => c.url === '/v1/auth/mfa');
    expect(JSON.parse(mfa?.init?.body as string)).toEqual({ challenge: 'c1', code: '123456', refresh_transport: 'cookie' });
    expect((mfa?.init?.headers as Record<string, string>)['x-sprig-csrf']).toBe('1');
    expect(useSession.getState()).toMatchObject({ status: 'active', token: 'sprigat_m' });
  });

  it('an enrolment ticket signs in only after the recovery codes were shown', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation((input) => {
      const url = input as string;
      if (url === '/v1/auth/login') return Promise.resolve(reply(200, { enrol: 'e1', expires_in: 300 }));
      if (url === '/v1/auth/factors:confirm')
        return Promise.resolve(
          reply(200, { access_token: 'sprigat_e', token_type: 'Bearer', expires_in: 900, session_id: 's', recovery_codes: ['aaaaa-bbbbb'] }),
        );
      if (url === '/v1/auth/me') return Promise.resolve(reply(200, me('acme.app')));
      return Promise.resolve(reply(404, {}));
    });
    expect(await login('acme.app', 'pw')).toMatchObject({ kind: 'enrol', ticket: 'e1' });
    const r = await confirmEnrolment('123456', 'e1');
    expect(r.codes).toEqual(['aaaaa-bbbbb']);
    expect(useSession.getState().status).toBe('signed_out');
    await r.finish();
    expect(useSession.getState()).toMatchObject({ status: 'active', token: 'sprigat_e' });
  });

  it('impersonation swaps the tab token and stopping restores one\'s own without the cookie', async () => {
    useSession.setState({ status: 'active', token: 'sprigat_admin', me: me('acme.admin') });
    vi.spyOn(globalThis, 'fetch').mockImplementation((input, init) => {
      const url = input as string;
      calls.push({ url, init });
      const auth = (init?.headers as Record<string, string> | undefined)?.authorization ?? '';
      if (url === '/v1/auth/impersonate')
        return Promise.resolve(
          reply(200, { access_token: 'sprigat_imp', token_type: 'Bearer', expires_in: 600, session_id: 'i', impersonating: 'acme.bob', by: 'acme.admin' }),
        );
      if (url === '/v1/auth/me')
        return Promise.resolve(reply(200, auth.includes('imp') ? me('acme.bob', 'acme.admin') : me('acme.admin')));
      if (url === '/v1/auth/logout') return Promise.resolve(reply(200, { ended: true }));
      if (url === '/v1/auth/refresh') return Promise.resolve(reply(200, { access_token: 'sprigat_admin2', token_type: 'Bearer', expires_in: 900, session_id: 's' }));
      return Promise.resolve(reply(404, {}));
    });
    await impersonate({ user: 'bob', reason: 'ticket 42', minutes: 10 });
    expect(useSession.getState().impersonating).toMatchObject({ user: 'acme.bob', by: 'acme.admin' });
    expect(useSession.getState().me?.impersonator).toBe('acme.admin');
    await stopImpersonating();
    const out = calls.find((c) => c.url === '/v1/auth/logout');
    expect(out?.init?.credentials).toBe('omit');
    expect((out?.init?.headers as Record<string, string>).authorization).toBe('Bearer sprigat_imp');
    expect(useSession.getState()).toMatchObject({ impersonating: null, token: 'sprigat_admin2' });
    expect(useSession.getState().me?.principal.login).toBe('acme.admin');
  });
});
