// Screenshots for the public documentation (docs/img/studio/*.png), used by
// docs/get-started/studio-tour.md and docs/tools/studio.md. Skipped unless
// DOCS_SHOTS=1, so the normal e2e suite never runs it.
//
// Run it against a gateway (HTTP listener on, tenant user acme.app), for
// example through Vite's preview server with the API proxied:
//
//   cd studio
//   pnpm build
//   STUDIO_API=http://127.0.0.1:25080 pnpm exec vite preview --port 25173 --strictPort &
//   DOCS_SHOTS=1 STUDIO_URL=http://localhost:25173 STUDIO_OUT=/tmp/docs-shots-out \
//     pnpm exec playwright test e2e/docs-screenshots.spec.ts --project chromium
//
// The spec seeds its own data: a database `retail` with fictitious
// `customers` (40), `orders` (200, over the last 90 days) and `order_items`,
// and a JavaScript function `order_total`. It drops `retail` when it ends
// (set DOCS_KEEP=1 to keep it while iterating). It fails at the start if
// `retail` already exists, so it never overwrites someone else's database.
// The saved query it creates is deleted again. Running queries adds entries
// to the user's history, as any use of Studio does.
//
// Shots are light theme, 1280×800, device scale factor 1. DOCS_IMG sets the
// output folder (default ../docs/img/studio).
import { expect, test, type Page } from '@playwright/test';
import { apiToken, signIn, sql, typeSql, openPage, LOGIN } from './fixtures';

const T = 'acme';
const DB = 'retail';
const OUT = process.env.DOCS_IMG ?? '../docs/img/studio';

test.skip(process.env.DOCS_SHOTS !== '1', 'docs screenshots run only with DOCS_SHOTS=1');
test.use({ viewport: { width: 1280, height: 800 }, deviceScaleFactor: 1, colorScheme: 'light' });

// ---- Sample data: deterministic, clearly fictitious. ----

function rng(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const FIRST = [
  'Amelia', 'Tomas', 'Priya', 'Lena', 'Oskar', 'Maya', 'Jonas', 'Chloe', 'Rafael', 'Ines',
  'Noah', 'Freya', 'Mateo', 'Hana', 'Elias', 'Sofia', 'Arjun', 'Clara', 'Felix', 'Zara',
];
const LAST = [
  'Hartley', 'Brennan', 'Okafor', 'Lindqvist', 'Moreau', 'Castell', 'Varga', 'Nakamura', 'Doyle', 'Ferreira',
  'Holm', 'Ashby', 'Kowalski', 'Ramires', 'Whitlow', 'Sandoval', 'Pemberton', 'Iyer', 'Kessler', 'Marlow',
];
const COUNTRIES = ['GB', 'GB', 'GB', 'DE', 'DE', 'FR', 'NL', 'IE', 'ES', 'SE', 'US', 'US', 'CA', 'AU', 'IN'];
const PRODUCTS: [string, number][] = [
  ['Ceramic mug', 9.5],
  ['Linen tea towel', 7.25],
  ['Oak serving board', 34.0],
  ['Enamel kettle', 42.0],
  ['Cotton apron', 18.0],
  ['Glass storage jar', 6.75],
  ['Cast-iron pan', 55.0],
  ['Bamboo utensil set', 14.5],
  ['Stoneware bowl', 12.0],
  ['Copper measuring spoons', 21.0],
  ['Wool table runner', 29.0],
  ['Beeswax wraps (3)', 11.0],
];
const STATUSES = ['delivered', 'delivered', 'delivered', 'shipped', 'shipped', 'paid', 'paid', 'open', 'cancelled', 'refunded'];

function stamp(msAgo: number): string {
  return new Date(Date.now() - msAgo).toISOString().slice(0, 19).replace('T', ' ');
}
const DAY = 86_400_000;

function seedStatements(): string[] {
  const r = rng(20261113);
  const pick = <T,>(xs: T[]): T => xs[Math.floor(r() * xs.length)]!;
  const customers: string[] = [];
  const used = new Set<string>();
  for (let id = 1; id <= 40; id++) {
    let first: string;
    let last: string;
    do {
      first = pick(FIRST);
      last = pick(LAST);
    } while (used.has(first + last));
    used.add(first + last);
    const email = `${first}.${last}@example.com`.toLowerCase();
    const created = stamp(Math.floor((120 + r() * 600) * DAY));
    customers.push(`(${id}, '${first} ${last}', '${email}', '${pick(COUNTRIES)}', '${created}')`);
  }
  const orders: string[] = [];
  const items: string[] = [];
  // Orders in time order: ids rise with created_at.
  const ages = Array.from({ length: 200 }, () => Math.floor(r() * 90 * DAY)).sort((a, b) => b - a);
  ages.forEach((age, i) => {
    const id = 1001 + i;
    const lines = 1 + Math.floor(r() * 4);
    let total = 0;
    const seen = new Set<string>();
    for (let line = 1; line <= lines; line++) {
      let p = pick(PRODUCTS);
      while (seen.has(p[0])) p = pick(PRODUCTS);
      seen.add(p[0]);
      const qty = 1 + Math.floor(r() * 3);
      total += qty * p[1];
      items.push(`(${id}, ${line}, '${p[0]}', ${qty}, ${p[1].toFixed(2)})`);
    }
    // Recent orders are still open or paid; older ones have moved on.
    const status = age < 3 * DAY ? pick(['open', 'paid', 'paid']) : pick(STATUSES);
    orders.push(`(${id}, ${1 + Math.floor(r() * 40)}, '${status}', ${total.toFixed(2)}, '${stamp(age)}')`);
  });
  return [
    `CREATE TABLE customers (
       id BIGINT PRIMARY KEY,
       name VARCHAR(100) NOT NULL,
       email VARCHAR(200) NOT NULL,
       country CHAR(2) NOT NULL,
       created_at DATETIME NOT NULL
     )`,
    `CREATE TABLE orders (
       id BIGINT PRIMARY KEY,
       customer_id BIGINT NOT NULL,
       status VARCHAR(20) NOT NULL,
       total DECIMAL(10,2) NOT NULL,
       created_at DATETIME NOT NULL,
       KEY idx_orders_customer (customer_id)
     )`,
    `CREATE TABLE order_items (
       order_id BIGINT NOT NULL,
       line INT NOT NULL,
       product VARCHAR(100) NOT NULL,
       quantity INT NOT NULL,
       unit_price DECIMAL(10,2) NOT NULL,
       PRIMARY KEY (order_id, line)
     )`,
    `INSERT INTO customers VALUES ${customers.join(',\n')}`,
    `INSERT INTO orders VALUES ${orders.join(',\n')}`,
    `INSERT INTO order_items VALUES ${items.join(',\n')}`,
  ];
}

const ORDER_TOTAL = `CREATE OR REPLACE FUNCTION order_total(orderId BIGINT) RETURNS DOUBLE LANGUAGE javascript AS $$import { db } from 'sprig:db';

// The sum of an order's lines, from order_items.
export default function (orderId) {
  const r = db.sqlOne(
    'SELECT SUM(quantity * unit_price) AS total ' +
      'FROM order_items WHERE order_id = ?',
    [orderId],
  );
  return r === null || r.total === null ? 0 : Number(r.total);
}
$$`;

// No leading spaces: Monaco keeps the previous line's indentation as text
// is typed, so indented lines would drift right.
const BY_COUNTRY = `SELECT c.country, COUNT(o.id) AS orders, ROUND(AVG(o.total), 2) AS avg_order
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.status NOT IN ('cancelled', 'refunded')
GROUP BY c.country
ORDER BY orders DESC;`;

const SAVED = 'Orders by country';

/** An application's statements, so the workload view has something to show.
 * Enough runs that they outrank the seeding INSERTs by total time. */
function workload(): string[] {
  const out: string[] = [];
  for (let i = 0; i < 120; i++) {
    out.push(`SELECT id, status, total FROM orders WHERE customer_id = ${1 + (i % 40)} ORDER BY created_at DESC`);
    out.push(`SELECT id, total FROM orders WHERE status = 'open' ORDER BY created_at DESC LIMIT 20`);
    out.push('SELECT product, SUM(quantity) AS sold FROM order_items GROUP BY product ORDER BY sold DESC');
    out.push(`SELECT name, email FROM customers WHERE country = '${['GB', 'DE', 'FR', 'US'][i % 4]}'`);
  }
  return out;
}

// ---- Helpers ----

async function shot(page: Page, name: string, clip?: { x: number; y: number; width: number; height: number }): Promise<void> {
  // Let fonts, lazy chunks and transitions settle.
  await page.evaluate(() => document.fonts.ready);
  await page.waitForTimeout(400);
  await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: false, animations: 'disabled', caret: 'hide', clip });
}

/** The tree in the side panel, with `retail` open. */
async function expandRetail(page: Page): Promise<void> {
  const expand = page.getByRole('button', { name: `Expand ${DB}` });
  const orders = page.getByRole('link', { name: /^orders/ }).first();
  await expect(expand.or(orders).first()).toBeVisible({ timeout: 15_000 });
  if (await expand.isVisible()) await expand.click();
  await expect(orders).toBeVisible();
}

test.describe.serial('Docs screenshots', () => {
  let token = '';

  test.beforeAll(async ({ request }) => {
    test.setTimeout(120_000);
    token = await apiToken(request);
    const dbs = (await sql(request, token, '-', `SHOW DATABASES LIKE '${DB}'`)) as { results: { rows: unknown[][] }[] };
    if (dbs.results[0]?.rows.length && process.env.DOCS_KEEP !== '1') {
      throw new Error(`database ${DB} exists already; drop it first (this spec creates and drops it)`);
    }
    if (!dbs.results[0]?.rows.length) {
      await sql(request, token, '-', `CREATE DATABASE ${DB}`);
      for (const s of seedStatements()) await sql(request, token, DB, s);
      await sql(request, token, DB, ORDER_TOTAL);
    }
  });

  test.afterAll(async ({ request }) => {
    if (process.env.DOCS_KEEP === '1') return;
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB}`);
  });

  test('sign-in', async ({ page }) => {
    await page.goto('/studio/login');
    await page.getByLabel('User', { exact: true }).fill(LOGIN);
    await shot(page, 'sign-in', { x: 320, y: 50, width: 640, height: 320 });
  });

  test('home, explorer and table pages', async ({ page }) => {
    await signIn(page);
    await expect(page).toHaveURL(new RegExp(`/studio/t/${T}`));
    await shot(page, 'home');

    // The database page, with the tree open on retail.
    await page.goto(`/studio/t/${T}/db/${DB}`);
    await expandRetail(page);
    await expect(page.getByRole('cell', { name: 'order_items' }).first()).toBeVisible();
    await shot(page, 'explorer');

    // A table's Data tab.
    await page.getByRole('link', { name: /^orders/ }).first().click();
    const grid = page.getByRole('grid', { name: 'orders rows' });
    await expect(grid).toContainText('delivered');
    await page.getByRole('group', { name: 'Rows' }).scrollIntoViewIfNeeded();
    await shot(page, 'table-data');

    // Staged edits: a deleted row and two changed cells, not yet applied.
    // Delete row acts on the row that has the focus.
    await grid.locator('[data-cell="5:2"]').click();
    await page.getByRole('button', { name: 'Delete row' }).click();
    await grid.locator('[data-cell="1:2"]').dblclick();
    await page.getByLabel('New value for status').fill('shipped');
    await page.keyboard.press('Enter');
    await grid.locator('[data-cell="3:3"]').dblclick();
    await page.getByLabel('New value for total').fill('48.00');
    await page.keyboard.press('Enter');
    await expect(page.getByTestId('pending-count')).toHaveText('3 changes pending');
    await grid.locator('[data-cell="8:1"]').click();
    await shot(page, 'staged-edits');
    // Apply shows the statements first; don't run them.
    await page.getByTestId('browse-apply').click();
    await expect(page.getByTestId('edit-preview')).toContainText('UPDATE');
    await shot(page, 'staged-preview');
    const cancel = page.getByRole('button', { name: /^(Cancel|Don’t run)$/ });
    if (await cancel.first().isVisible()) await cancel.first().click();
    await page.getByRole('button', { name: 'Reset', exact: true }).click();
    await expect(page.getByTestId('pending-count')).toHaveText('0 changes pending');

    // Structure: the Columns tab.
    await page.getByRole('tab', { name: 'Columns' }).click();
    await expect(page.getByRole('cell', { name: 'customer_id' }).first()).toBeVisible();
    await shot(page, 'table-columns');
  });

  test('query, chart, saved queries and history', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Query');
    await page.getByTestId('db-select').selectOption(DB);
    await typeSql(page, BY_COUNTRY);
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('result-0')).toContainText('rows');
    await shot(page, 'query');

    await page.getByTestId('result-0').getByRole('tab', { name: 'Chart' }).click();
    await expect(page.getByTestId('result-0').locator('canvas, svg').first()).toBeVisible({ timeout: 20_000 });
    await page.waitForTimeout(1200);
    await shot(page, 'query-chart');

    // Save it, then show the saved list; the query is deleted again below.
    await page.getByTestId('result-0').getByRole('tab', { name: 'Grid' }).click();
    await page.getByTestId('save-query').click();
    await page.getByTestId('save-name').fill(SAVED);
    await page.getByTestId('save-name').press('Enter');
    await page.getByRole('button', { name: 'Saved queries', exact: true }).click();
    await expect(page.getByTestId('saved-list')).toContainText(SAVED);
    await shot(page, 'saved-queries');

    // The catalog write is fire-and-forget: wait for it before the page closes.
    const deleted = page.waitForResponse((r) => r.url().includes('/v1/library/saved:delete'));
    await page.getByTestId('saved-list').getByRole('button', { name: `Delete ${SAVED}` }).click();
    expect((await deleted).status()).toBe(200);
    await expect(page.getByTestId('saved-list')).not.toContainText(SAVED);
  });

  test('command palette', async ({ page }) => {
    await signIn(page);
    // The palette searches the objects of databases already loaded.
    await expandRetail(page);
    await page.keyboard.press('ControlOrMeta+k');
    const input = page.getByPlaceholder('Type to search tables, pages, settings and commands');
    await input.first().fill('ord');
    await page.waitForTimeout(300);
    await shot(page, 'palette');
    await page.keyboard.press('Escape');
  });

  test('logic editor', async ({ page }) => {
    await signIn(page);
    await page.goto(`/studio/t/${T}/logic/${DB}/function/order_total`);
    await expect(page.getByTestId('logic-module')).toBeVisible({ timeout: 20_000 });
    await expect(page.getByText('order_items').first()).toBeVisible({ timeout: 20_000 });
    await shot(page, 'logic');
  });

  test('settings', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Settings');
    await expect(page.getByTestId('settings-table')).toContainText('studio.max_result_rows');
    await page.getByTestId('settings-scope').selectOption('database');
    await page.getByLabel('Database', { exact: true }).selectOption(DB);
    await page.getByLabel('Filter keys').fill('studio.');
    await expect(page.getByTestId('settings-table')).not.toContainText('amqp.');
    await shot(page, 'settings');
  });

  test('workload', async ({ page, request }) => {
    test.setTimeout(240_000);
    for (const s of workload()) await sql(request, token, DB, s);
    // Fingerprints are aggregated in the background; wait until they show.
    await expect
      .poll(
        async () => {
          const r = await request.get('/v1/workload/fingerprints?hours=1&limit=100&source=client', {
            headers: { authorization: `Bearer ${token}` },
          });
          const body = (await r.json()) as { fingerprints: { db: string | null; stmt_type: string; runs: number }[] };
          return body.fingerprints.filter((f) => f.db === DB && f.stmt_type === 'SELECT' && f.runs >= 100).length;
        },
        { timeout: 120_000, intervals: [2_000] },
      )
      .toBeGreaterThanOrEqual(3);
    await signIn(page);
    await openPage(page, 'Automatic indexing');
    await page.getByRole('tab', { name: 'Fingerprints' }).click();
    await page.getByTestId('fingerprints').getByLabel('Window').selectOption('1');
    await page.getByTestId('fingerprints').getByLabel('Source').selectOption('client');
    await expect(page.getByTestId('fingerprints').locator('tbody tr').first()).toContainText(/orders|customers|order_items/, {
      timeout: 30_000,
    });
    await shot(page, 'workload');
  });
});
