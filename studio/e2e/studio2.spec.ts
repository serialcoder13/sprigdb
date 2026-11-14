// Studio end to end: cursors past the row cap, the shards view, second
// factors and impersonation, operator mode.
// STUDIO_URL=... STUDIO_METRICS=http://127.0.0.1:<telemetry>/metrics pnpm e2e
// STUDIO_BACKEND=<host>:<port>:<user>:<password>:<backend account> also counts
// the tenant's backend connections with the mysql client.
import { expect, test, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { execFileSync } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { apiToken, SHOTS, signIn, sql, typeSql, openPage } from './fixtures';

const DB2 = 'studio2_e2e';
const METRICS = process.env.STUDIO_METRICS ?? '';
const BACKEND = process.env.STUDIO_BACKEND ?? '';

async function axe(page: Page, name: string) {
  const r = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  const bad = r.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical');
  expect(bad.map((v) => `${name}: ${v.id} ${v.help} (${v.nodes.map((n) => n.target.join(" ")).join(", ")})`)).toEqual([]);
}

async function shot(page: Page, name: string) {
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.screenshot({ path: `${SHOTS}/${name}-${scheme}.png`, fullPage: false });
  }
  await page.emulateMedia({ colorScheme: 'light' });
}

/** Open cursors on the gateway (its sprig_studio_cursors gauge). */
async function openCursors(): Promise<number> {
  if (!METRICS) return -1;
  const text = await (await fetch(METRICS)).text();
  const m = /^sprig_studio_cursors (\d+)/m.exec(text);
  return m ? Number(m[1]) : 0;
}

/** The tenant's backend connections running a statement (not idle in the
 * gateway's shared pool), when STUDIO_BACKEND says where. */
function backendConnections(): number {
  if (!BACKEND) return -1;
  const [host, port, user, password, account] = BACKEND.split(':');
  const out = execFileSync('mysql', [
    `-h${host}`, `-P${port}`, `-u${user}`, `-p${password}`, '-Nse',
    `SELECT COUNT(*) FROM information_schema.processlist WHERE user = '${account}' AND command <> 'Sleep'`,
  ], { stdio: ['ignore', 'pipe', 'ignore'] });
  return Number(out.toString().trim());
}

/** RFC 6238 (SHA-1, 6 digits, 30 s) for a base32 secret. */
function totp(secret: string, offset = 0): string {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = '';
  for (const ch of secret) bits += alphabet.indexOf(ch).toString(2).padStart(5, '0');
  const bytes = Buffer.from((bits.match(/.{8}/g) ?? []).map((b) => parseInt(b, 2)));
  const step = Math.floor(Date.now() / 30_000) + offset;
  const msg = Buffer.alloc(8);
  msg.writeBigUInt64BE(BigInt(step));
  const h = createHmac('sha1', bytes).update(msg).digest();
  const o = h[h.length - 1]! & 15;
  return String((h.readUInt32BE(o) & 0x7fffffff) % 1_000_000).padStart(6, '0');
}

/** Waits for the next TOTP step, so a code isn't one already used. */
async function nextStep(page: Page) {
  await page.waitForTimeout(30_500 - (Date.now() % 30_000));
}

async function hideTab(page: Page) {
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', { value: 'hidden', configurable: true });
    document.dispatchEvent(new Event('visibilitychange'));
  });
}

async function showTab(page: Page) {
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', { value: 'visible', configurable: true });
    document.dispatchEvent(new Event('visibilitychange'));
  });
}

test.describe.serial('Studio round 3', () => {
  let token = '';
  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB2}`);
    await sql(request, token, '-', `CREATE DATABASE ${DB2}`);
    await sql(request, token, DB2, 'CREATE TABLE big (id INT PRIMARY KEY, note VARCHAR(40))');
    await sql(
      request,
      token,
      DB2,
      "INSERT INTO big WITH RECURSIVE d(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM d WHERE i < 149) SELECT a.i * 150 + b.i + 1, CONCAT('row ', a.i * 150 + b.i + 1) FROM d a, d b",
    );
  });
  test.afterAll(async ({ request }) => {
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB2}`);
  });

  test('cursors: fetch past the cap, a sliding window, nothing held by a hidden tab', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Query');
    await page.getByTestId('db-select').selectOption(DB2);
    // 506 million rows: the backend is still sending while the cursor waits.
    await typeSql(page, 'SELECT a.id, b.note FROM big a CROSS JOIN big b');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('truncated')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId('result-0')).toContainText('10,000 rows');
    // Fetch more: a cursor from row 10,001, pages of 1,000, a window of 10,000.
    await page.getByTestId('fetch-more').click();
    await expect(page.getByTestId('window')).toContainText('Rows 1,001–11,000');
    await page.getByTestId('fetch-more').click();
    await expect(page.getByTestId('window')).toContainText('Rows 2,001–12,000');
    // The grid numbers rows by the result: its last row is 12,000.
    await expect(page.getByRole('grid', { name: 'Result 1' })).toHaveAttribute('aria-rowcount', '12001');
    if (METRICS) expect(await openCursors()).toBe(1);
    if (BACKEND) expect(backendConnections()).toBeGreaterThanOrEqual(1);
    await axe(page, 'query-cursor');
    await shot(page, 'query-cursor');
    // The tab is hidden: Studio closes its cursor at once, and the backend
    // statement and connection go with it.
    await hideTab(page);
    await expect.poll(openCursors, { timeout: 5_000 }).toBeLessThanOrEqual(0);
    if (BACKEND) await expect.poll(backendConnections, { timeout: 5_000 }).toBe(0);
    await showTab(page);
    // Fetch more again reopens after the rows read, and says so.
    await page.getByTestId('fetch-more').click();
    await expect(page.getByTestId('window')).toContainText('Rows 3,001–13,000');
    await expect(page.getByTestId('reopened')).toContainText('row 12,001');
    // Left alone past studio.cursor_idle_seconds, the gateway closes it too.
    if (METRICS) {
      expect(await openCursors()).toBe(1);
      await expect.poll(openCursors, { timeout: 70_000, intervals: [1_000] }).toBe(0);
      if (BACKEND) expect(backendConnections()).toBe(0);
    }
  });

  test('cursors: paging to the end of a result', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Query');
    await page.getByTestId('db-select').selectOption(DB2);
    await typeSql(page, 'SELECT id, note FROM big ORDER BY id');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('truncated')).toBeVisible({ timeout: 30_000 });
    // 22,500 rows in all: 10,000 shown, then pages of 1,000.
    for (let i = 0; i < 14; i++) {
      if (await page.getByTestId('all-fetched').isVisible()) break;
      await page.getByTestId('fetch-more').click();
      // One locator for "done or ready for the next page": the last page
      // replaces the button with "all fetched" in one render, and asking
      // the two apart raced it (isEnabled then waited for a button that
      // had gone; a WebKit flake).
      await expect(page.getByTestId('all-fetched').or(page.locator('[data-testid="fetch-more"]:enabled'))).toBeVisible({ timeout: 30_000 });
    }
    await expect(page.getByTestId('all-fetched')).toBeVisible();
    await expect(page.getByTestId('window')).toContainText('Rows 12,501–22,500');
    await expect(page.getByRole('grid', { name: 'Result 1' })).toHaveAttribute('aria-rowcount', '22501');
  });

  test('shards view: groups, keys, maps and advisor runs', async ({ page, playwright, baseURL }) => {
    // studio2_shop was sharded by the advisor on the test stack beforehand.
    await signIn(page);
    await openPage(page, 'Shards');
    const g = page.getByTestId('group-customers');
    await expect(g).toBeVisible({ timeout: 15_000 });
    await expect(g.getByTestId('key')).toHaveText('customer_id (numeric)');
    await expect(g.getByRole('table', { name: 'Members of customers' })).toContainText('orders');
    await expect(g.getByTestId('shard-map')).toContainText('dc-a-c1');
    await axe(page, 'shards');
    await shot(page, 'shards');
    await page.getByRole('tab', { name: 'Advisor runs' }).click();
    const runs = page.getByTestId('advisor-runs');
    await expect(runs).toContainText('applied');
    await runs.getByRole('row').filter({ hasText: 'applied' }).first().getByRole('button').click();
    await expect(page.getByTestId('run-detail')).toContainText('"design"');
    await axe(page, 'shards-advisor');
    await shot(page, 'shards-advisor');
    // A user with grants on another database sees no groups, and no runs.
    const api = await playwright.request.newContext({ baseURL });
    const tok = await apiToken(api);
    await sql(api, tok, '-', "DROP USER IF EXISTS 'studio2_viewer'@'%'");
    await sql(api, tok, '-', "CREATE USER 'studio2_viewer'@'%' IDENTIFIED BY 'sprig-dev-app'");
    await sql(api, tok, '-', `GRANT SELECT ON ${DB2}.* TO 'studio2_viewer'@'%'`);
    try {
      const r = await api.post('/v1/auth/login', {
        data: { login: 'acme.studio2_viewer', password: 'sprig-dev-app', refresh_transport: 'body', client: 'api' },
      });
      const vt = ((await r.json()) as { access_token: string }).access_token;
      const groups = (await (await api.get('/v1/shards', { headers: { authorization: `Bearer ${vt}` } })).json()) as {
        groups: unknown[];
        all_databases: boolean;
      };
      expect(groups).toMatchObject({ groups: [], all_databases: false });
      const runsReply = await api.get('/v1/shards/advisor', { headers: { authorization: `Bearer ${vt}` } });
      expect(runsReply.status()).toBe(403);
      expect(((await runsReply.json()) as { code: number }).code).toBe(1227);
    } finally {
      await sql(api, tok, '-', "DROP USER IF EXISTS 'studio2_viewer'@'%'");
      await api.dispose();
    }
  });

  test('second factor: enrol, sign in with a code and a recovery code, impersonate', async ({ page, request }) => {
    test.setTimeout(180_000);
    const user = 'studio2_ui_mfa';
    await sql(request, token, '-', `DROP USER IF EXISTS '${user}'@'%'`);
    await sql(request, token, '-', `DROP USER IF EXISTS 'studio2_ui_bob'@'%'`);
    await sql(request, token, '-', `CREATE USER '${user}'@'%' IDENTIFIED BY 'sprig-dev-mfa'`);
    await sql(request, token, '-', `CREATE USER 'studio2_ui_bob'@'%' IDENTIFIED BY 'sprig-dev-bob'`);
    await sql(request, token, '-', `GRANT SELECT ON ${DB2}.* TO 'studio2_ui_bob'@'%'`);
    try {
      // Enrol from the Security page.
      await signIn(page, `acme.${user}`, 'sprig-dev-mfa');
      await openPage(page, 'Security');
      await expect(page.getByTestId('no-factor')).toBeVisible();
      await page.getByRole('button', { name: 'Set up an authenticator app' }).click();
      await expect(page.getByTestId('totp-secret')).toHaveText(/^[A-Z2-7]{32}$/);
      const secret = (await page.getByTestId('totp-secret').textContent())!.trim();
      expect(secret).toHaveLength(32);
      await axe(page, 'security-enrol');
      await page.getByLabel('Code', { exact: true }).fill(totp(secret));
      await page.getByRole('button', { name: 'Confirm' }).click();
      await expect(page.getByTestId('recovery-codes').locator('li')).toHaveCount(10);
      const codes = (await page.getByTestId('recovery-codes').locator('li').allTextContents()).map((c) => c.trim());
      expect(codes).toHaveLength(10);
      await shot(page, 'security-recovery-codes');
      await page.getByRole('button', { name: 'I have saved them' }).click();
      await expect(page.getByTestId('factor-active')).toBeVisible();
      await expect(page.getByTestId('recovery-left')).toHaveText('10');
      await axe(page, 'security');
      await shot(page, 'security');
      await page.getByRole('button', { name: 'Sign out' }).click();
      // The logout request must finish before navigating, or the cookie stays.
      await expect(page.getByRole('button', { name: 'Sign in' })).toBeVisible();
      // Sign in: password, then the code.
      await page.goto('/studio/login');
      await page.getByLabel('User', { exact: true }).fill(`acme.${user}`);
      await page.getByLabel('Password').fill('sprig-dev-mfa');
      await page.getByRole('button', { name: 'Sign in' }).click();
      await expect(page.getByRole('heading', { name: 'Second factor' })).toBeVisible();
      await axe(page, 'login-mfa');
      await shot(page, 'login-mfa');
      await page.getByLabel('Code', { exact: true }).fill('000000');
      await page.getByRole('button', { name: 'Verify' }).click();
      await expect(page.getByTestId('mfa-error')).toContainText('1045');
      await nextStep(page);
      await page.getByLabel('Code', { exact: true }).fill(totp(secret));
      await page.getByRole('button', { name: 'Verify' }).click();
      await expect(page.getByTestId('whoami')).toHaveText(`acme.${user}`);
      await openPage(page, 'Security');
      await expect(page.getByTestId('amr')).toHaveText('pwd, otp');
      await page.getByRole('button', { name: 'Sign out' }).click();
      // The logout request must finish before navigating, or the cookie stays.
      await expect(page.getByRole('button', { name: 'Sign in' })).toBeVisible();
      // A recovery code instead.
      await page.goto('/studio/login');
      await page.getByLabel('User', { exact: true }).fill(`acme.${user}`);
      await page.getByLabel('Password').fill('sprig-dev-mfa');
      await page.getByRole('button', { name: 'Sign in' }).click();
      await page.getByLabel('Code', { exact: true }).fill(codes[0]!);
      await page.getByRole('button', { name: 'Verify' }).click();
      await expect(page.getByTestId('whoami')).toHaveText(`acme.${user}`);
      await openPage(page, 'Security');
      await expect(page.getByTestId('recovery-left')).toHaveText('9');
      await page.getByRole('button', { name: 'Sign out' }).click();
      // The logout request must finish before navigating, or the cookie stays.
      await expect(page.getByRole('button', { name: 'Sign in' })).toBeVisible();
      // Impersonation by the tenant administrator.
      await signIn(page);
      await page.goto('/studio/t/acme/access/users');
      const row = page.getByTestId('users-table').getByRole('row').filter({ hasText: 'studio2_ui_bob' });
      await row.getByRole('button', { name: 'Impersonate' }).click();
      await page.getByLabel('Reason (recorded in the audit log)').fill('e2e: checking what bob sees');
      await page.getByLabel('Minutes').fill('5');
      await page.getByRole('button', { name: 'Start impersonating' }).click();
      await expect(page.getByTestId('impersonation-banner')).toContainText('acme.studio2_ui_bob, impersonated by acme.app');
      await expect(page.getByTestId('whoami')).toHaveText('acme.studio2_ui_bob');
      await axe(page, 'impersonation');
      await shot(page, 'impersonation');
      await page.getByRole('button', { name: 'Stop impersonating' }).click();
      await expect(page.getByTestId('impersonation-banner')).toHaveCount(0);
      await expect(page.getByTestId('whoami')).toHaveText('acme.app');
    } finally {
      await sql(request, token, '-', `DROP USER IF EXISTS '${user}'@'%'`);
      await sql(request, token, '-', `DROP USER IF EXISTS 'studio2_ui_bob'@'%'`);
    }
  });

  test('operator mode: clusters, placement, moves; refused to others', async ({ page, playwright, baseURL }) => {
    // acme.app is in studio.operator_logins on the test stack (global scope).
    await signIn(page);
    await page.getByTestId('nav-operator').click();
    await expect(page.getByTestId('ops-clusters')).toContainText('dc-a-c1');
    await axe(page, 'ops-clusters');
    await shot(page, 'ops-clusters');
    await page.getByRole('link', { name: 'Moves and splits' }).click();
    await expect(page.getByTestId('ops-moves')).toContainText('split_shard');
    await axe(page, 'ops-moves');
    await shot(page, 'ops-moves');
    await page.getByRole('link', { name: 'Tenants and placement' }).click();
    await page.getByTestId('ops-tenants').getByRole('button', { name: 'acme', exact: true }).click();
    await expect(page.getByTestId('ops-placement')).toContainText('dc-a-c1');
    // An action asks for the target's name, then a reason.
    const answers = ['acme', 'e2e: checking the advisor from Studio'];
    page.on('dialog', (d) => void d.accept(answers.shift() ?? ''));
    await page.getByTestId('ops-tenants').getByRole('row').filter({ has: page.getByRole('button', { name: 'acme', exact: true }) }).getByRole('button', { name: 'Run the sharding advisor' }).click();
    // The controller's own answer comes back: done, or (once the tenant has
    // table groups) 39471, as sprigctl advisor run says.
    await expect(page.getByTestId('ops-tenants').getByRole('status')).toHaveText(/Done\.|39471/);
    await axe(page, 'ops-tenants');
    await shot(page, 'ops-tenants');
    await page.getByRole('link', { name: 'Data centers' }).click();
    await expect(page.getByTestId('ops-dcs')).toContainText('dc-a');
    // Another tenant's user isn't an operator.
    const api = await playwright.request.newContext({ baseURL });
    try {
      const r = await api.post('/v1/auth/login', {
        data: { login: 'globex.app', password: 'sprig-dev-app', refresh_transport: 'body', client: 'studio' },
      });
      const t = ((await r.json()) as { access_token: string }).access_token;
      const me = (await (await api.get('/v1/auth/me', { headers: { authorization: `Bearer ${t}` } })).json()) as {
        platform_roles: string[];
      };
      expect(me.platform_roles).toEqual([]);
      const x = await api.get('/v1/ops/clusters', { headers: { authorization: `Bearer ${t}` } });
      expect(x.status()).toBe(403);
      expect(((await x.json()) as { code: number }).code).toBe(39928);
    } finally {
      await api.dispose();
    }
  });
});
