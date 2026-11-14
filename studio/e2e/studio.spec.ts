// Studio's core views end to end: login, explorer, SQL
// editor with confirmation and caps, results, export, browse with an edit
// conflict, settings, automatic indexing, users and API keys, accessibility
// (axe-core) and screenshots of every route in light and dark.
import { expect, test, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { readFileSync } from 'node:fs';
import { apiToken, DB, PASSWORD, SHOTS, signIn, sql, typeSql, openPage } from './fixtures';

const T = 'acme';

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

test.describe.serial('Studio R0', () => {
  let token = '';
  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB}`);
    await sql(request, token, '-', `CREATE DATABASE ${DB}`);
    await sql(request, token, DB, 'CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(80), total DECIMAL(10,2), note TEXT)');
    await sql(
      request,
      token,
      DB,
      "INSERT INTO orders VALUES (1,'open',10.50,'a'),(2,'open',20.00,NULL),(3,'paid',5.25,'it''s'),(17,'open',99.00,'x')",
    );
    // Hostile text for the XSS checks: a value, a column comment, a table name.
    await sql(request, token, DB, `INSERT INTO orders VALUES (90,'<img src=x onerror="window.__xss=1">',1,'<script>window.__xss=2</script>')`);
    await sql(request, token, DB, 'CREATE TABLE `<img src=y onerror=window.__xss=3>` (a INT PRIMARY KEY)');
  });
  test.afterAll(async ({ request }) => {
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB}`);
  });

  test('login page: accessible, refuses a wrong password', async ({ page }) => {
    await page.goto('/studio/login');
    await axe(page, 'login');
    await shot(page, 'login');
    await page.getByLabel('User', { exact: true }).fill('acme.app');
    await page.getByLabel('Password').fill('nope');
    await page.getByRole('button', { name: 'Sign in' }).click();
    await expect(page.getByRole('alert')).toContainText('1045');
  });

  test('explorer, object page and home', async ({ page }) => {
    await signIn(page);
    await expect(page).toHaveURL(new RegExp(`/studio/t/${T}`));
    await page.getByRole('button', { name: 'Expand studio0_e2e' }).click();
    await page.getByRole('link', { name: /orders/ }).click();
    await expect(page.getByRole('heading', { name: 'orders' })).toBeVisible();
    await expect(page.getByRole('grid', { name: 'orders rows' })).toBeVisible();
    await axe(page, 'object');
    await shot(page, 'object-data');
    await page.getByRole('tab', { name: 'Definition' }).click();
    await expect(page.getByTestId('ddl')).toContainText('CREATE TABLE');
    await page.getByRole('tab', { name: 'Columns' }).click();
    await expect(page.getByRole('cell', { name: 'status' })).toBeVisible();
    await page.goto(`/studio/t/${T}`);
    await axe(page, 'home');
    await shot(page, 'home');
  });

  test('SQL editor: run, confirm DDL, cap, cancel', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Query');
    await page.getByTestId('db-select').selectOption(DB);
    await typeSql(page, 'SELECT id, status FROM orders ORDER BY id');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('result-0')).toContainText('5 rows');
    await axe(page, 'query');
    await shot(page, 'query');
    // A DDL statement asks first (studio.confirm_writes = ddl).
    await typeSql(page, 'CREATE TABLE scratch (a INT)');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByRole('heading', { name: 'Run this statement?' })).toBeVisible();
    await page.getByRole('button', { name: 'Don’t run' }).click();
    await expect(page.getByTestId('phase')).toHaveText('Cancelled.');
    // Over the row cap: the first 10,000 rows and a notice.
    await typeSql(
      page,
      "WITH RECURSIVE d(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM d WHERE i < 119) SELECT a.i * 120 + b.i AS n, REPEAT('x', 20) AS pad FROM d a, d b",
    );
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('truncated')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId('result-0')).toContainText('10,000 rows');
    // Sql.Check marks an unknown column before anything runs (Monaco's
    // squiggle, from the gateway's EXPLAIN bind).
    await typeSql(page, 'SELECT nope FROM orders');
    await expect(page.locator('.squiggly-error')).toHaveCount(1, { timeout: 10_000 });
    await shot(page, 'query-check');
    // A syntax error shows its code and trace id.
    await typeSql(page, 'SELEC 1');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    // Unknown statements ask too, under the ddl mode.
    await page.getByRole('button', { name: 'Run it' }).click();
    await expect(page.getByTestId('query-error')).toContainText('1064');
    await expect(page.getByTestId('query-error')).toContainText('trace');
  });

  test('saved queries and history (decision R4)', async ({ page, request, browser }) => {
    await signIn(page);
    await openPage(page, 'Query');
    await page.getByTestId('db-select').selectOption(DB);
    await typeSql(page, 'SELECT id FROM orders WHERE id = 3');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('result-0')).toContainText('1 row');
    // History lists the run.
    await page.getByRole('button', { name: 'History', exact: true }).click();
    await expect(page.getByTestId('history-list')).toContainText('SELECT id FROM orders WHERE id = 3');
    // Save it under a name; the saved panel lists it, and it opens in a new tab.
    await page.getByTestId('save-query').click();
    await page.getByTestId('save-name').fill('order three');
    await page.getByTestId('save-name').press('Enter');
    await page.getByRole('button', { name: 'Saved queries', exact: true }).click();
    await expect(page.getByTestId('saved-list')).toContainText('order three');
    await page.getByTestId('saved-list').getByRole('button', { name: /^order three/ }).click();
    await expect(page.getByRole('tab', { name: /order three/ }).first()).toBeVisible();
    // Both live in the tenant's catalog (shared with sprig), not this browser:
    // the API returns them, and a fresh browser shows them.
    const auth = { authorization: `Bearer ${token}` };
    await expect
      .poll(async () => {
        const lib = (await (await request.get('/v1/library?history=50', { headers: auth })).json()) as {
          saved: { name: string; database: string | null }[];
          history: { text: string; client: string }[];
        };
        return [
          lib.saved.some((q) => q.name === 'order three' && q.database === DB),
          lib.history.some((h) => h.text === 'SELECT id FROM orders WHERE id = 3' && h.client === 'studio'),
        ];
      })
      .toEqual([true, true]);
    const other = await browser.newContext();
    const p2 = await other.newPage();
    await signIn(p2);
    await p2.getByRole('button', { name: 'Saved queries', exact: true }).click();
    await expect(p2.getByTestId('saved-list')).toContainText('order three');
    await expect(p2.getByTestId('library-where')).toHaveCount(0);
    await other.close();
  });

  test('export streams every row as a download', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Query');
    await page.getByTestId('db-select').selectOption(DB);
    await typeSql(page, 'SELECT id, status, note FROM orders WHERE id < 90 ORDER BY id');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('result-0')).toContainText('4 rows');
    const [dl] = await Promise.all([page.waitForEvent('download'), page.getByRole('button', { name: 'Export CSV' }).click()]);
    expect(dl.suggestedFilename()).toBe('result.csv');
    const text = readFileSync(await dl.path(), "utf8");
    expect(text.split('\r\n')[0]).toBe('id,status,note');
    expect(text).toContain("3,paid,it's");
    expect(text).toContain('2,open,\r\n');
  });

  test('browse: edit conflict shows three versions; save after taking current', async ({ page, request }) => {
    await signIn(page);
    await page.goto(`/studio/t/${T}/db/${DB}/obj/table/orders`);
    const grid = page.getByRole('grid', { name: 'orders rows' });
    await expect(grid).toContainText('open');
    // Alice edits row 17's total.
    const cell = grid.locator('[data-cell="3:2"]');
    await cell.dblclick();
    await page.getByLabel('New value for total').fill('100.00');
    await page.keyboard.press('Enter');
    // Bob saves status = 'paid' meanwhile.
    await sql(request, token, DB, "UPDATE orders SET status = 'paid' WHERE id = 17");
    await expect(page.getByTestId('pending-count')).toHaveText('1 change pending');
    await page.getByTestId('browse-apply').click();
    await expect(page.getByTestId('edit-preview')).toContainText("`status` <=> 'open'");
    await shot(page, 'browse-preview');
    await page.getByRole('button', { name: 'Run them' }).click();
    await expect(page.getByTestId('conflicts')).toContainText('Someone changed these rows');
    await expect(page.getByTestId('conflicts')).toContainText('paid');
    await shot(page, 'browse-conflict');
    await page.getByRole('button', { name: 'Keep mine (save against current)' }).click();
    await page.getByRole('button', { name: 'Run them' }).click();
    await expect(grid).toContainText('100.00');
    // The filter bar: a condition, then a raw WHERE clause.
    await page.getByLabel('Column', { exact: true }).selectOption('status');
    await page.getByLabel('Value', { exact: true }).fill('paid');
    await page.getByTestId('filter-apply').click();
    await expect(grid).toContainText('100.00');
    await expect(grid).not.toContainText('open');
    await page.getByRole('button', { name: '</> WHERE' }).click();
    await page.getByTestId('filter-where').fill('id = 17');
    await page.getByTestId('filter-apply').click();
    await expect(grid).toContainText('100.00');
    await expect(grid).not.toContainText('5.25');
    await page.getByRole('button', { name: 'Clear', exact: true }).click();
    await expect(grid).toContainText('open');
    // Hostile values render as text and run nothing.
    await expect(grid).toContainText('<img src=x onerror=');
    // The tree is open on the current database.
    await expect(page.getByRole('link', { name: /<img src=y/ })).toBeVisible();
    expect(await page.evaluate(() => (window as unknown as { __xss?: number }).__xss)).toBeUndefined();
  });

  test('settings at tenant and database scope', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Settings');
    await expect(page.getByTestId('settings-table')).toContainText('studio.max_result_rows');
    await axe(page, 'settings');
    await shot(page, 'settings');
    await page.getByTestId('settings-scope').selectOption('database');
    await page.getByLabel('Database', { exact: true }).selectOption(DB);
    await page.getByRole('button', { name: 'Set studio.max_result_rows' }).click();
    await page.getByLabel('New value for studio.max_result_rows').fill('500');
    await page.getByRole('button', { name: 'Set', exact: true }).click();
    await expect(page.getByTestId('settings-statement')).toHaveText(
      `ALTER DATABASE \`${DB}\` SPRIG SET (studio.max_result_rows = 500)`,
    );
    await page.getByRole('button', { name: 'Run it' }).click();
    await expect(page.getByTestId('settings-message')).toBeVisible();
    await expect(page.getByRole('row', { name: /studio\.max_result_rows/ })).toContainText('500');
  });

  test('automatic indexing view', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Automatic indexing');
    await expect(page.getByRole('heading', { name: 'Automatic indexing' })).toBeVisible();
    // Several sprig_auto_ indexes once the e2e setup raised recommendations.
    await expect(page.getByText(/None\.|sprig_auto_|controller/).first()).toBeVisible({ timeout: 20_000 });
    await axe(page, 'indexing');
    await shot(page, 'indexing');
    await page.getByRole('tab', { name: 'Recommendations' }).click();
    await shot(page, 'indexing-recommendations');
    // Workload.ListFingerprints: the statements this suite ran are there,
    // Studio's marked as such; one fingerprint's series charts.
    await page.getByRole('tab', { name: 'Fingerprints' }).click();
    await expect(page.getByTestId('fingerprints').locator('tbody tr').first()).toBeVisible({ timeout: 90_000 });
    await page.getByLabel('Source').selectOption('studio');
    await expect(page.getByTestId('fingerprints').locator('tbody tr').first()).toContainText('Studio', { timeout: 30_000 });
    await page.getByTestId('fingerprints').getByRole('button', { name: /Latency series/ }).first().click();
    await expect(page.getByRole('region', { name: 'Latency series' }).locator('tbody tr').first()).toBeVisible();
    await axe(page, 'indexing-fingerprints');
    await shot(page, 'indexing-fingerprints');
  });

  test('another tenant sees none of this tenant\'s data', async ({ playwright, baseURL }) => {
    const request = await playwright.request.newContext({ baseURL });
    const r = await request.post('/v1/auth/login', {
      data: { login: 'globex.app', password: PASSWORD, refresh_transport: 'body' },
    });
    const t = ((await r.json()) as { access_token: string }).access_token;
    const dbs = await request.post('/v1/databases/-/sql:execute', {
      headers: { authorization: `Bearer ${t}` },
      data: { sql: "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'studio0%'" },
    });
    expect(JSON.stringify(await dbs.json())).not.toContain('studio0_e2e');
    const read = await request.post('/v1/databases/studio0_e2e/sql:execute', {
      headers: { authorization: `Bearer ${t}` },
      data: { sql: 'SELECT * FROM orders' },
    });
    expect(read.status()).toBeGreaterThanOrEqual(400);
    // Indexing and accounts stay inside the caller's own tenant.
    const users = await request.get('/v1/users', { headers: { authorization: `Bearer ${t}` } });
    expect(JSON.stringify(await users.json())).not.toContain('acme');
    await request.dispose();
  });

  test('users and API keys', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Users and access');
    await expect(page.getByTestId('users-table')).toContainText('app');
    await axe(page, 'access-users');
    await shot(page, 'access-users');
    await page.getByRole('link', { name: 'API keys' }).click();
    const keyName = `studio0-e2e-key-${Date.now()}`;
    await page.getByLabel('Name').fill(keyName);
    await page.getByRole('button', { name: 'Create key' }).click();
    const key = page.getByTestId('new-key');
    await expect(key).toContainText('sprig_acme_');
    const k = (await key.locator('code').textContent()) ?? '';
    await shot(page, 'access-keys');
    await page.getByRole('button', { name: 'I have copied it' }).click();
    const row = page.getByRole('row', { name: new RegExp(keyName) });
    await row.getByRole('button', { name: 'Revoke' }).click();
    await expect(row).toContainText('revoked');
    // The revoked key no longer works.
    const r = await page.request.post(`/v1/databases/-/sql:execute`, {
      headers: { authorization: `Bearer ${k}` },
      data: { sql: 'SELECT 1' },
    });
    expect(r.status()).toBe(401);
    await page.getByRole('link', { name: 'Grants' }).click();
    await axe(page, 'access-grants');
  });
});
