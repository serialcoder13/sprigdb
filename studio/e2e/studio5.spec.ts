// Studio end to end, against a private deployment:
//   STUDIO_URL=http://127.0.0.1:24080 STUDIO_SHOTS=../target/agent-studio5/screenshots pnpm e2e studio5
// Graphs in the catalog snapshot (explorer, object page, the editor's graph
// picker and completion), exact BSON types from canonical Extended JSON,
// connection forms with write-only credentials, and the script-sink editor.
//
// The connection test needs `sink.destination_allowlist` to list
// hooks.example.com for the tenant (an operator setting):
//   sprigctl settings set sink.destination_allowlist '["hooks.example.com"]' --scope tenant:acme
import { expect, test, type APIRequestContext, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { apiToken, SHOTS, signIn, sql } from './fixtures';

/** Everything this suite creates, dropped by exact name afterwards. */
const DB5 = 'studio5_ui';
const RO = 'studio5_ro';
const RO_PASSWORD = 'studio5-ro-password';

async function axe(page: Page, name: string) {
  const r = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  const bad = r.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical');
  expect(bad.map((v) => `${name}: ${v.id} ${v.help} (${v.nodes.map((n) => n.target.join(' ')).join(', ')})`)).toEqual([]);
}

async function shot(page: Page, name: string) {
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.waitForTimeout(150);
    await page.screenshot({ path: `${SHOTS}/${name}-${scheme}.png`, fullPage: false });
  }
  await page.emulateMedia({ colorScheme: 'light' });
}

/** A statement in another language, as setup. */
async function lang(request: APIRequestContext, token: string, db: string, language: string, text: string): Promise<unknown> {
  const r = await request.post(`/v1/databases/${db}/sql:execute`, {
    headers: { authorization: `Bearer ${token}` },
    data: { sql: text, language },
  });
  const body: unknown = await r.json();
  if (r.status() !== 200) throw new Error(`${text}: ${JSON.stringify(body)}`);
  return body;
}

/** Replaces a Monaco editor's text (it keeps its own textarea). */
async function typeInto(page: Page, label: string, text: string) {
  const box = page.getByRole('textbox', { name: label, exact: true });
  await box.focus();
  await page.keyboard.press('ControlOrMeta+A');
  await page.keyboard.press('Delete');
  await page.keyboard.insertText(text);
}

test.describe.serial('Studio round 5', () => {
  let token = '';
  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB5}`);
    await sql(request, token, '-', `CREATE DATABASE ${DB5}`);
    await sql(request, token, DB5, 'CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)');
    await sql(request, token, DB5, 'CREATE GRAPH social5');
    await lang(request, token, `${DB5}.social5`, 'cypher', "CREATE (a:Person {name: 'Asha'})-[:KNOWS]->(b:Person {name: 'Ben'})-[:WROTE]->(n:Note {id: 1})");
    await sql(request, token, '-', `DROP USER IF EXISTS '${RO}'@'%'`);
    await sql(request, token, '-', `CREATE USER '${RO}'@'%' IDENTIFIED BY '${RO_PASSWORD}'`);
    await sql(request, token, '-', `GRANT SELECT ON ${DB5}.notes TO '${RO}'@'%'`);
    await sql(request, token, DB5, 'CREATE STREAM events5 PARTITIONS 1');
  });
  test.afterAll(async ({ request }) => {
    await sql(request, token, DB5, 'DROP SINK IF EXISTS deliver5').catch(() => undefined);
    await sql(request, token, DB5, 'DROP CONNECTION IF EXISTS crm5').catch(() => undefined);
    await sql(request, token, '-', `DROP USER IF EXISTS '${RO}'@'%'`);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB5}`);
  });

  test('graphs in the catalog: explorer, object page, picker; hidden without a grant', async ({ page, request }) => {
    await signIn(page);
    await page.goto(`/studio/t/acme/db/${DB5}`);
    const tree = page.getByRole('navigation', { name: 'Databases' });
    const link = tree.getByRole('link', { name: /social5/ });
    await expect(link).toBeVisible();
    await link.click();
    const obj = page.getByTestId('graph-object');
    await expect(obj.getByTestId('graph-labels')).toHaveText('Note, Person');
    await expect(obj.getByTestId('graph-types')).toHaveText('KNOWS, WROTE');
    await axe(page, 'graph-object');
    await shot(page, 'graph-object');
    // "Query" opens a Cypher tab on the graph; the picker lists it.
    await page.getByRole('button', { name: 'Query', exact: true }).click();
    await expect(page.getByTestId('graph-input')).toHaveValue('social5');
    await expect(page.getByTestId('graph-list').locator('option')).toHaveCount(1);
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('result-0')).toBeVisible();
    // A user with SELECT on one table doesn't see the graph.
    const r = await request.get(`/v1/databases/${DB5}/catalog`, {
      headers: { authorization: `Basic ${Buffer.from(`acme.${RO}:${RO_PASSWORD}`).toString('base64')}` },
    });
    expect(r.status()).toBe(200);
    const names = ((await r.json()) as { objects: { name: string; kind: string }[] }).objects.map((o) => `${o.kind}:${o.name}`);
    expect(names).toEqual(['table:notes']);
  });

  test('documents come back as canonical Extended JSON', async ({ request }) => {
    await lang(request, token, DB5, 'mongo', JSON.stringify({ insert: 'kinds', documents: [{ _id: 1, i: 1, l: { $numberLong: '2' }, f: { $numberDouble: '3.0' } }] }));
    const r = await request.post(`/v1/databases/${DB5}/sql:execute`, {
      headers: { authorization: `Bearer ${token}` },
      data: { sql: JSON.stringify({ find: 'kinds', filter: { _id: 1 } }), language: 'mongo', output: 'canonical_ejson' },
    });
    expect(r.status()).toBe(200);
    const cell = ((await r.json()) as { results: { rows: string[][] }[] }).results[0]?.rows[0]?.[0] ?? '';
    expect(JSON.parse(cell)).toEqual({ _id: { $numberInt: '1' }, i: { $numberInt: '1' }, l: { $numberLong: '2' }, f: { $numberDouble: '3.0' } });
    const bad = await request.post(`/v1/databases/${DB5}/sql:execute`, {
      headers: { authorization: `Bearer ${token}` },
      data: { sql: 'SELECT 1', output: 'canonical_ejson' },
    });
    expect(((await bad.json()) as { code: number }).code).toBe(39794);
  });

  test('connections: a form with write-only credentials', async ({ page }) => {
    await signIn(page);
    await page.goto(`/studio/t/acme/messaging?db=${DB5}&tab=sinks`);
    await page.getByTestId('conn-new-open').click();
    const form = page.getByTestId('conn-new');
    await form.getByTestId('conn-name').fill('crm5');
    await form.getByTestId('conn-opt-url').fill('https://hooks.example.com/in');
    await form.getByTestId('conn-more').fill('auth = bearer');
    await form.getByRole('button', { name: 'Add a credential' }).click();
    await form.getByTestId('conn-new-cred-0').fill('very-secret-token-1');
    await expect(form.getByTestId('conn-new-cred-0')).toHaveAttribute('type', 'password');
    await axe(page, 'connection-form');
    await form.getByTestId('conn-create').click();
    const shown = page.getByTestId('msg-confirm-sql');
    await expect(shown).toHaveText(
      "CREATE CONNECTION `crm5` TYPE webhook WITH (url = 'https://hooks.example.com/in', auth = 'bearer') CREDENTIALS (token = '********')",
    );
    await shot(page, 'connection-confirm');
    await page.getByTestId('msg-confirm').click();
    const list = page.getByTestId('msg-connections');
    await expect(list).toContainText('crm5');
    await expect(list).toContainText('token (v1)');
    await expect(page.locator('body')).not.toContainText('very-secret-token-1');
    // Set a new credential value: the statement on screen never holds it.
    await list.getByRole('button', { name: 'Open' }).first().click();
    const detail = page.getByTestId('conn-detail');
    await expect(detail.getByTestId('conn-def')).toContainText("token = '***'");
    await detail.getByRole('button', { name: 'Add a credential' }).click();
    await detail.getByTestId('conn-set-cred-0').fill('very-secret-token-2');
    await detail.getByTestId('conn-set-credentials').click();
    await expect(page.getByTestId('msg-confirm-sql').last()).toHaveText("ALTER CONNECTION `crm5` SET CREDENTIALS (token = '********')");
    await page.getByTestId('msg-confirm').last().click();
    await expect(list).toContainText('token (v2)');
    await expect(detail.getByTestId('conn-set-cred-0')).toHaveCount(0);
    await shot(page, 'connections');
  });

  test('script sinks: created from the editor, then edited', async ({ page }) => {
    await signIn(page);
    await page.goto(`/studio/t/acme/messaging?db=${DB5}&tab=sinks`);
    await page.getByTestId('script-sink-open').click();
    const form = page.getByTestId('script-sink-new');
    await form.getByTestId('script-sink-name').fill('deliver5');
    await form.getByTestId('script-sink-source').fill('events5');
    await form.getByTestId('script-sink-using').fill('crm5');
    await typeInto(page, 'Script sink code', 'export function deliver(batch, ctx) { return { ok: true }; }');
    await form.getByTestId('script-sink-create').click();
    await expect(page.getByTestId('msg-confirm-sql')).toHaveText(
      'CREATE SINK `deliver5` FROM STREAM `events5` TO SCRIPT LANGUAGE javascript USING CONNECTIONS (`crm5`) AS $$export function deliver(batch, ctx) { return { ok: true }; }$$',
    );
    await page.getByTestId('msg-confirm').click();
    const sinks = page.getByTestId('msg-sinks');
    await expect(sinks).toContainText('deliver5');
    await sinks.getByRole('button', { name: 'Open' }).first().click();
    const edit = page.getByTestId('script-sink-edit');
    await expect(edit).toContainText('Script (javascript)');
    await typeInto(page, 'Script of sink deliver5', 'export function deliver(batch) { return true; }');
    await edit.getByTestId('script-sink-save').click();
    await expect(page.getByTestId('msg-confirm-sql').last()).toHaveText('ALTER SINK `deliver5` SET SCRIPT AS $$export function deliver(batch) { return true; }$$');
    await page.getByTestId('msg-confirm').last().click();
    await expect(page.getByTestId('msg-sink-detail').locator('details pre')).toContainText('return true;');
    await axe(page, 'script-sink');
    await shot(page, 'script-sink');
  });
});
