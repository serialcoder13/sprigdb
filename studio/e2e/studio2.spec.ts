// Studio round 3 end to end (chapter 44 steps 9, 15–17): cursors past the
// row cap, the shards view, second factors and impersonation, operator mode.
// STUDIO_URL=... STUDIO_METRICS=http://127.0.0.1:<telemetry>/metrics pnpm e2e
// STUDIO_BACKEND=<host>:<port>:<user>:<password>:<backend account> also counts
// the tenant's backend connections with the mysql client.
import { expect, test, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { execFileSync } from 'node:child_process';
import { apiToken, SHOTS, signIn, sql, typeSql } from './fixtures';

const DB2 = 'studio2_e2e';
const METRICS = process.env.STUDIO_METRICS ?? '';
const BACKEND = process.env.STUDIO_BACKEND ?? '';

async function axe(page: Page, name: string) {
  const r = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  const bad = r.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical');
  expect(bad.map((v) => `${name}: ${v.id} ${v.help} (${v.nodes.length})`)).toEqual([]);
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
    await page.getByRole('link', { name: 'Query' }).click();
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
    await page.getByRole('link', { name: 'Query' }).click();
    await page.getByTestId('db-select').selectOption(DB2);
    await typeSql(page, 'SELECT id, note FROM big ORDER BY id');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('truncated')).toBeVisible({ timeout: 30_000 });
    // 22,500 rows in all: 10,000 shown, then pages of 1,000.
    for (let i = 0; i < 14; i++) {
      if (await page.getByTestId('all-fetched').isVisible()) break;
      await page.getByTestId('fetch-more').click();
      await expect
        .poll(async () => (await page.getByTestId('all-fetched').isVisible()) || (await page.getByTestId('fetch-more').isEnabled()))
        .toBe(true);
    }
    await expect(page.getByTestId('all-fetched')).toBeVisible();
    await expect(page.getByTestId('window')).toContainText('Rows 12,501–22,500');
    await expect(page.getByRole('grid', { name: 'Result 1' })).toHaveAttribute('aria-rowcount', '22501');
  });
});
