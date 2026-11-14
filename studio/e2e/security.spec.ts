// Security pass: headers and CSP, cookie
// endpoints refusing cross-site use, refresh-token reuse, token storage.
import { expect, test } from '@playwright/test';
import { LOGIN, PASSWORD, signIn } from './fixtures';

test('every Studio response carries the security headers and CSP', async ({ request }) => {
  for (const path of ['/studio/', '/studio/login', '/studio/version.json', '/studio/nope.js']) {
    const r = await request.get(path);
    const h = r.headers();
    expect(h['content-security-policy'], path).toContain("script-src 'self'");
    expect(h['content-security-policy']).toContain("frame-ancestors 'none'");
    expect(h['content-security-policy']).not.toContain('unsafe-eval');
    expect(h['x-content-type-options']).toBe('nosniff');
    expect(h['x-frame-options']).toBe('DENY');
    expect(h['referrer-policy']).toBe('no-referrer');
    expect(h['cross-origin-opener-policy']).toBe('same-origin');
    expect(h['permissions-policy']).toContain('camera=()');
  }
  const index = await request.get('/studio/t/acme/q/1');
  expect(index.headers()['cache-control']).toBe('no-store');
  const html = await index.text();
  const asset = /src="(\/studio\/assets\/[^"]+\.js)"/.exec(html)?.[1];
  expect(asset).toBeTruthy();
  const a = await request.get(asset!, { headers: { 'accept-encoding': 'br' } });
  expect(a.headers()['cache-control']).toContain('immutable');
  expect(a.headers()['content-encoding']).toBe('br');
  expect((await request.get('/studio/%2e%2e/%2e%2e/etc/passwd')).status()).toBe(404);
});

test('cookie endpoints refuse other sites and missing CSRF headers', async ({ request, baseURL }) => {
  const login = await request.post('/v1/auth/login', {
    data: { login: LOGIN, password: PASSWORD, refresh_transport: 'cookie' },
    headers: { 'x-sprig-csrf': '1', origin: baseURL! },
  });
  expect(login.status()).toBe(200);
  const cookie = login.headers()['set-cookie'] ?? '';
  expect(cookie).toContain('HttpOnly');
  expect(cookie).toContain('SameSite=Strict');
  expect(cookie).toContain('Path=/v1/auth');
  // No CSRF header: 39923.
  let r = await request.post('/v1/auth/refresh', { headers: { origin: baseURL! } });
  expect(r.status()).toBe(403);
  expect(((await r.json()) as { code: number }).code).toBe(39923);
  // A foreign origin: 39922.
  r = await request.post('/v1/auth/refresh', { headers: { 'x-sprig-csrf': '1', origin: 'https://evil.example' } });
  expect(((await r.json()) as { code: number }).code).toBe(39922);
  r = await request.post('/v1/auth/refresh', { headers: { 'x-sprig-csrf': '1', 'sec-fetch-site': 'cross-site' } });
  expect(((await r.json()) as { code: number }).code).toBe(39922);
  // Same origin with the header: rotated.
  r = await request.post('/v1/auth/refresh', { headers: { 'x-sprig-csrf': '1', origin: baseURL! } });
  expect(r.status()).toBe(200);
});

test('a reused refresh token revokes the session', async ({ playwright, baseURL }) => {
  // A context of its own: no cookie from other tests.
  const request = await playwright.request.newContext({ baseURL });
  const r = await request.post('/v1/auth/login', {
    data: { login: LOGIN, password: PASSWORD, refresh_transport: 'body' },
  });
  const first = (await r.json()) as { refresh_token: string; access_token: string };
  const second = await request.post('/v1/auth/refresh', { data: { refresh_token: first.refresh_token } });
  expect(second.status()).toBe(200);
  const fresh = (await second.json()) as { access_token: string };
  // The old token again: theft. Everything of the session ends.
  const again = await request.post('/v1/auth/refresh', { data: { refresh_token: first.refresh_token } });
  expect(again.status()).toBe(401);
  const me = await request.get('/v1/auth/me', { headers: { authorization: `Bearer ${fresh.access_token}` } });
  expect(me.status()).toBe(401);
  await request.dispose();
});

test('a wrong password is refused like any login', async ({ request }) => {
  const r = await request.post('/v1/auth/login', {
    data: { login: LOGIN, password: 'wrong-password', refresh_transport: 'body' },
  });
  expect(r.status()).toBe(401);
  expect(((await r.json()) as { code: number }).code).toBe(1045);
});

test('tokens stay out of storage and the refresh cookie out of scripts', async ({ page }) => {
  await signIn(page);
  const found = await page.evaluate(async () => {
    const dbs = (await indexedDB.databases?.()) ?? [];
    return {
      cookie: document.cookie,
      local: JSON.stringify({ ...localStorage }),
      session: JSON.stringify({ ...sessionStorage }),
      idb: dbs.map((d) => d.name),
    };
  });
  expect(found.cookie).not.toContain('sprig-refresh');
  expect(found.local).not.toContain('sprigat_');
  expect(found.session).not.toContain('sprigat_');
  // Reload: the HttpOnly cookie signs the tab back in.
  await page.reload();
  await expect(page.getByTestId('whoami')).toHaveText(LOGIN);
  // Sign out: the cookie no longer works.
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByRole('button', { name: 'Sign in' })).toBeVisible();
  await page.reload();
  await expect(page.getByRole('button', { name: 'Sign in' })).toBeVisible();
});

