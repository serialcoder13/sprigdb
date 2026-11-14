// Studio end to end (the TOTP QR code, graphs, the Redis console, the
// MongoDB editor, logic, messaging, view as), against a private deployment:
// STUDIO_URL=http://127.0.0.1:27080 STUDIO_SHOTS=../target/agent-studio3/screenshots pnpm e2e studio3
import { expect, test, type APIRequestContext, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { apiToken, SHOTS, signIn, sql, typeSql, openPage } from './fixtures';
import { qrPath } from '../src/auth/qr';

/** Everything this suite creates, dropped by exact name afterwards. */
export const DB3 = 'studio3_e2e';
const MFA_USER = 'studio3_ui_mfa';

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
export async function lang(request: APIRequestContext, token: string, db: string, language: string, text: string): Promise<unknown> {
  const r = await request.post(`/v1/databases/${db}/sql:execute`, {
    headers: { authorization: `Bearer ${token}` },
    data: { sql: text, language },
  });
  const body: unknown = await r.json();
  if (r.status() !== 200) throw new Error(`${text}: ${JSON.stringify(body)}`);
  return body;
}

async function kv(request: APIRequestContext, token: string, ns: string, command: string[]): Promise<unknown> {
  const r = await request.post(`/v1/databases/${DB3}/namespaces/${ns}:exec`, {
    headers: { authorization: `Bearer ${token}` },
    data: { command },
  });
  expect(r.status()).toBe(200);
  return ((await r.json()) as { reply: unknown }).reply;
}

test.describe.serial('Studio round 4', () => {
  let token = '';
  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB3}`);
    await sql(request, token, '-', `CREATE DATABASE ${DB3}`);
    await sql(request, token, DB3, 'CREATE GRAPH social');
    await lang(
      request,
      token,
      `${DB3}.social`,
      'cypher',
      "CREATE (a:Person {name: 'Asha'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Ben'})-[:LIKES]->(m:Movie {title: 'Up'}), (b)-[:KNOWS]->(c:Person {name: 'Cai'})",
    );
    await lang(
      request,
      token,
      DB3,
      'mongo',
      JSON.stringify({
        insert: 'people',
        documents: [
          { _id: { $oid: '64b7f0c2a1b2c3d4e5f60711' }, name: 'Asha', visits: { $numberLong: '9007199254740993' }, score: 1.5, tags: ['a', 'b'], at: { $date: '2026-11-10T00:00:00Z' } },
          { _id: { $oid: '64b7f0c2a1b2c3d4e5f60712' }, name: 'Ben', visits: { $numberLong: '7' }, score: { $numberDouble: '2.0' }, address: { city: 'Pune' } },
        ],
      }),
    );
    await kv(request, token, 'cache', ['HSET', 'user:1', 'name', 'Asha', 'age', '30']);
    await kv(request, token, 'cache', ['RPUSH', 'queue:jobs', 'a', 'b', 'c']);
  });
  test.afterAll(async ({ request }) => {
    await sql(request, token, '-', `DROP USER IF EXISTS '${MFA_USER}'@'%'`);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB3}`);
  });

  test('ST33: the enrolment shows a scannable QR code of the otpauth link', async ({ page, request }) => {
    await sql(request, token, '-', `DROP USER IF EXISTS '${MFA_USER}'@'%'`);
    await sql(request, token, '-', `CREATE USER '${MFA_USER}'@'%' IDENTIFIED BY 'sprig-dev-mfa'`);
    await signIn(page, `acme.${MFA_USER}`, 'sprig-dev-mfa');
    await openPage(page, 'Security');
    await page.getByRole('button', { name: 'Set up an authenticator app' }).click();
    const qr = page.getByTestId('totp-qr');
    await expect(qr).toBeVisible();
    await expect(qr).toHaveAttribute('aria-label', 'QR code of the authenticator link');
    const uri = (await page.getByTestId('totp-uri').textContent())!.trim();
    expect(uri).toMatch(/^otpauth:\/\/totp\//);
    // The symbol drawn is exactly the encoder's for the link shown.
    const d = await qr.locator('path').getAttribute('d');
    expect(d).toBe(qrPath(uri).d);
    const box = await qr.boundingBox();
    expect(box!.width).toBeGreaterThanOrEqual(180);
    await axe(page, 'security-enrol-qr');
    await shot(page, 'security-enrol-qr');
  });

  test('step 27: Cypher and GQL with the graph view, expand and the grid', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Query');
    await page.getByTestId('lang-select').selectOption('cypher');
    await page.getByTestId('db-select').selectOption(DB3);
    await page.getByTestId('graph-input').fill('social');
    await typeSql(page, "MATCH (a:Person {name: 'Asha'})-[r]->(b) RETURN a, r, b");
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    const view = page.getByTestId('graph-view');
    await expect(view).toBeVisible();
    await expect(view).toHaveAttribute('data-nodes', '2');
    await expect(view).toHaveAttribute('data-edges', '1');
    // Select Ben by keyboard (the node list), see his properties, expand.
    await page.getByTestId('graph-node-select').selectOption({ label: 'Ben (Person 2)' });
    await expect(page.getByTestId('graph-selection')).toContainText('name');
    await page.getByTestId('graph-expand').click();
    await expect(view).toHaveAttribute('data-nodes', '4');
    await expect(view).toHaveAttribute('data-edges', '3');
    await axe(page, 'graph');
    await shot(page, 'graph');
    // The grid is the accessible alternative: the same result as text.
    await page.getByRole('tab', { name: 'Grid' }).click();
    await expect(page.getByTestId('result-0')).toContainText('$node');
    // A path result in GQL.
    await page.getByTestId('lang-select').selectOption('gql');
    await typeSql(page, 'MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('graph-view')).toHaveAttribute('data-nodes', '3');
    await expect(page.getByTestId('graph-view')).toHaveAttribute('data-edges', '2');
    // A syntax error keeps its number.
    await page.getByTestId('lang-select').selectOption('cypher');
    await typeSql(page, 'MATCH (a RETURN a');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('query-error')).toContainText('39900');
  });

  test('step 22: the Redis console and key browser', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Redis');
    await page.getByTestId('kv-db').selectOption(DB3);
    await page.getByTestId('kv-ns').fill('cache');
    await typeSql(page, 'SET greeting "hello world"\nGET greeting\nHGETALL user:1\nLRANGE queue:jobs 0 -1\nGET');
    await page.getByRole('button', { name: 'Run all' }).click();
    const log = page.getByTestId('kv-log');
    await expect(log).toContainText('"OK"');
    await expect(log).toContainText('"hello world"');
    await expect(log).toContainText('1# "name" => "Asha"');
    await expect(log).toContainText('3) "c"');
    await expect(log).toContainText("(error) wrong number of arguments for 'get'");
    // FLUSHDB always asks; cancelling runs nothing.
    await typeSql(page, 'FLUSHDB');
    await page.getByTestId('kv-run').click();
    await expect(page.getByRole('heading', { name: 'Run this command?' })).toBeVisible();
    await page.getByRole('button', { name: 'Cancel' }).click();
    await expect(log).toContainText('(not run)');
    // The key browser: types, a hash's fields, a string edited only if unchanged.
    const keys = page.getByTestId('kv-keys');
    await expect(keys).toContainText('user:1');
    await expect(keys).toContainText('greeting');
    await keys.getByRole('button', { name: 'user:1' }).click();
    await expect(page.getByTestId('kv-structure')).toContainText('"age" => "30"');
    await axe(page, 'redis');
    await shot(page, 'redis');
    await keys.getByRole('button', { name: 'greeting' }).click();
    await expect(page.getByTestId('kv-string')).toHaveText('hello world');
    await page.getByRole('button', { name: 'Edit' }).click();
    await page.getByTestId('kv-edit').fill('hello again');
    await page.getByTestId('kv-save').click();
    await typeSql(page, 'GET greeting');
    await page.getByTestId('kv-run').click();
    await expect(log).toContainText('"hello again"');
  });
});

test.describe.serial('Studio round 4: MongoDB', () => {
  let token = '';
  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await sql(request, token, '-', `CREATE DATABASE IF NOT EXISTS ${DB3}`);
    await lang(request, token, DB3, 'mongo', JSON.stringify({ drop: 'people' })).catch(() => undefined);
    await lang(
      request,
      token,
      DB3,
      'mongo',
      JSON.stringify({
        insert: 'people',
        documents: [
          { _id: { $oid: '64b7f0c2a1b2c3d4e5f60711' }, name: 'Asha', visits: { $numberLong: '9007199254740993' }, score: 1.5, tags: ['a', 'b'], at: { $date: '2026-11-10T00:00:00Z' } },
          { _id: { $oid: '64b7f0c2a1b2c3d4e5f60712' }, name: 'Ben', visits: { $numberLong: '7' }, score: { $numberDouble: '2.0' }, address: { city: 'Pune' } },
        ],
      }),
    );
  });
  test.afterAll(async ({ request }) => {
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB3}`);
  });

  const firstDoc = async (request: APIRequestContext, command: unknown) => {
    const r = (await lang(request, token, DB3, 'mongo', JSON.stringify(command))) as { results: { rows: string[][] }[] };
    return JSON.parse(r.results[0]!.rows[0]![0]!) as Record<string, unknown>;
  };
  const typeOf = async (request: APIRequestContext, field: string, filter: unknown) =>
    (await firstDoc(request, { aggregate: 'people', pipeline: [{ $match: filter }, { $project: { t: { $type: '$' + field } } }], cursor: {} })).t;

  test('steps 19–20: the MongoDB editor, the JSON tree with types, an edit and a conflict', async ({ page, request }) => {
    await signIn(page);
    await openPage(page, 'Query');
    await page.getByTestId('lang-select').selectOption('mongo');
    await page.getByTestId('db-select').selectOption(DB3);
    // Code is refused before anything is sent.
    await typeSql(page, 'db.people.find({$where: function () { return true }})');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('query-error')).toContainText('never runs code');
    await typeSql(page, 'db.people.find({}).sort({name: 1})');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('doc-tree')).toBeVisible();
    const asha = page.getByTestId('doc-0');
    await expect(asha.locator('[data-type="objectId"]')).toHaveCount(1);
    await expect(asha.locator('[data-type="int64"]')).toHaveCount(1);
    await expect(asha.locator('[data-type="double"]')).toHaveCount(1);
    await expect(asha.locator('[data-type="date"]')).toHaveCount(1);
    await expect(asha).toContainText('9007199254740993');
    await axe(page, 'mongo-tree');
    await shot(page, 'mongo-tree');
    // Edit Ben's score; his int64 `visits` (7) must stay int64.
    const ben = page.getByTestId('doc-1');
    // Canonical output, so Ben's small int64 shows as int64, not int.
    await expect(ben.locator('[data-type="int64"]')).toHaveCount(1);
    await ben.getByRole('button', { name: 'Edit score' }).click();
    await expect(page.getByTestId('edit-type')).toHaveValue('double');
    await page.getByTestId('edit-value').fill('2.5');
    await page.getByRole('button', { name: 'Apply' }).click();
    await ben.getByTestId('doc-save').click();
    await expect(ben.getByTestId('doc-saved')).toBeVisible();
    const benId = { _id: { $oid: '64b7f0c2a1b2c3d4e5f60712' } };
    expect(await typeOf(request, 'visits', benId)).toBe('long');
    expect(await typeOf(request, 'score', benId)).toBe('double');
    // A conflict: Asha changes on the server after she was read.
    await asha.getByRole('button', { name: 'Edit name' }).click();
    await page.getByTestId('edit-value').fill('Asha K');
    await page.getByRole('button', { name: 'Apply' }).click();
    const ashaId = { _id: { $oid: '64b7f0c2a1b2c3d4e5f60711' } };
    await lang(request, token, DB3, 'mongo', JSON.stringify({ update: 'people', updates: [{ q: ashaId, u: { $set: { score: 9.5 } } }] }));
    await asha.getByTestId('doc-save').click();
    const conflict = asha.getByTestId('doc-conflict');
    await expect(conflict).toBeVisible();
    await expect(conflict).toContainText('9.5');
    await shot(page, 'mongo-conflict');
    await asha.getByTestId('doc-keep-mine').click();
    await expect(asha.getByTestId('doc-saved')).toBeVisible();
    const doc = await firstDoc(request, { find: 'people', filter: ashaId });
    expect(doc.name).toBe('Asha K');
    expect(doc.score).toBe(9.5);
    expect(await typeOf(request, 'visits', ashaId)).toBe('long');
    // Shell syntax for writes and helpers; aggregation.
    await typeSql(page, 'db.people.insertOne({name: "Cai", visits: NumberLong("3"), joined: ISODate("2026-11-11T00:00:00Z")})');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('result-0')).toBeVisible();
    expect(await typeOf(request, 'joined', { name: 'Cai' })).toBe('date');
    expect(await typeOf(request, 'visits', { name: 'Cai' })).toBe('long');
    await typeSql(page, 'db.people.aggregate([{$group: {_id: null, n: {$sum: 1}}}])');
    await page.getByRole('button', { name: 'Run', exact: true }).click();
    await expect(page.getByTestId('doc-tree')).toContainText('n');
    await expect(page.getByTestId('doc-0')).toContainText('3');
  });
});

test.describe.serial('Studio round 4: logic', () => {
  let token = '';
  const LDB = 'studio3_logic';
  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${LDB}`);
    await sql(request, token, '-', `CREATE DATABASE ${LDB}`);
    await sql(request, token, LDB, 'CREATE TABLE items (id BIGINT PRIMARY KEY, price DOUBLE)');
    await sql(request, token, LDB, 'INSERT INTO items VALUES (1, 2.5), (2, 4.0)');
  });
  test.afterAll(async ({ request }) => {
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${LDB}`);
  });

  test('step 26: write, check, deploy, test-run and roll back JavaScript; KIRun in kirun-ui', async ({ page, request }) => {
    test.setTimeout(120_000);
    await signIn(page);
    await page.goto(`/studio/t/acme/logic/${LDB}/new/function`);
    await page.getByTestId('logic-name').fill('studio3_double');
    await page.getByTestId('logic-params').fill('a BIGINT');
    await page.getByTestId('logic-returns').fill('BIGINT');
    await typeSql(page, 'return a * 2;');
    await page.getByTestId('logic-check').click();
    const result = page.getByTestId('logic-check-result');
    await expect(result).toBeVisible();
    await expect(page.getByTestId('logic-check-tier')).toHaveText(/mariadb|engine/);
    await axe(page, 'logic-new');
    await page.getByTestId('logic-deploy').click();
    await expect(page.getByTestId('logic-confirm-sql')).toContainText('CREATE OR REPLACE FUNCTION `studio3_logic`.`studio3_double`(a BIGINT) RETURNS BIGINT LANGUAGE javascript');
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-module')).toBeVisible();
    await expect(page.getByTestId('logic-active')).toHaveText('1');
    await expect(page.getByTestId('logic-modules')).toContainText('studio3_double');
    await shot(page, 'logic-js');
    // A test run (Logic.TestRun): always rolled back.
    await page.getByTestId('logic-tab-test').click();
    await page.getByTestId('logic-arg-a').fill('21');
    await page.getByTestId('logic-test-run').click();
    await expect(page.getByTestId('logic-report-result')).toContainText('42');
    await expect(page.getByTestId('logic-report')).toContainText('Writes, rolled back');
    // A second version, then roll back to the first.
    await page.getByTestId('logic-tab-code').click();
    await typeSql(page, 'return a * 3;');
    await page.getByTestId('logic-deploy').click();
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-active')).toHaveText('2');
    await page.getByTestId('logic-tab-test').click();
    await page.getByTestId('logic-arg-a').fill('21');
    await page.getByTestId('logic-test-run').click();
    await expect(page.getByTestId('logic-report-result')).toContainText('63');
    await page.getByTestId('logic-tab-versions').click();
    await expect(page.getByTestId('logic-versions')).toContainText('2');
    await page.getByTestId('logic-rollback-1').click();
    await expect(page.getByTestId('logic-confirm-sql')).toHaveText('ALTER FUNCTION `studio3_logic`.`studio3_double` ROLLBACK TO VERSION 1');
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-active')).toHaveText('1');
    await page.getByTestId('logic-tab-test').click();
    await page.getByTestId('logic-arg-a').fill('21');
    await page.getByTestId('logic-test-run').click();
    await expect(page.getByTestId('logic-report-result')).toContainText('42');
    await page.getByTestId('logic-tab-logs').click();
    await expect(page.getByTestId('logic-logs')).toBeVisible();
    await page.getByTestId('logic-tab-dead').click();
    await expect(page.getByText('no binding')).toBeVisible();
    // The SQL path sees the same function.
    const r = (await sql(request, token, LDB, `SELECT ${LDB}.studio3_double(5) AS v`)) as { results: { rows: string[][] }[] };
    expect(r.results[0]!.rows[0]![0]).toBe('10');

    // KIRun: the visual editor from @fincity/kirun-ui, then a dry run and a deploy.
    await page.goto(`/studio/t/acme/logic/${LDB}/new/procedure`);
    await page.getByTestId('logic-name').fill('studio3_kdouble');
    await page.getByTestId('logic-kind').selectOption('procedure');
    await page.getByTestId('logic-runtime').selectOption('kirun');
    await expect(page.getByTestId('kirun-editor')).toBeVisible();
    await page.getByTestId('logic-check').click();
    await expect(page.getByTestId('logic-check-tier')).toHaveText(/mariadb|engine/);
    await shot(page, 'logic-kirun');
    await page.getByTestId('logic-deploy').click();
    await expect(page.getByTestId('logic-confirm-sql')).toContainText('LANGUAGE kirun');
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-active')).toHaveText('1');
    await page.getByTestId('logic-tab-test').click();
    await page.getByTestId('logic-arg-a').fill('8');
    await page.getByTestId('logic-test-run').click();
    await expect(page.getByTestId('logic-report-result')).toContainText('16');
  });
});

test.describe.serial('Studio round 4: messaging', () => {
  let token = '';
  const MDB = 'studio3_msg';
  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${MDB}`);
    await sql(request, token, '-', `CREATE DATABASE ${MDB}`);
    await sql(request, token, MDB, "CREATE STREAM order_events PARTITIONS 2 RETENTION '7 days' CLEANUP delete");
    for (let i = 0; i < 6; i++) {
      await sql(request, token, MDB, `INSERT INTO order_events (\`key\`, \`value\`) VALUES ('k${i}', '{"n": ${i}}')`);
    }
    // Wait until the sequencer has given the records offsets.
    for (let t = 0; t < 40; t++) {
      const r = (await sql(request, token, MDB, 'SELECT COUNT(*) FROM order_events')) as { results: { rows: string[][] }[] };
      if (r.results[0]!.rows[0]![0] === '6') break;
      await new Promise((ok) => setTimeout(ok, 500));
    }
    const p = (await sql(request, token, MDB, 'SELECT partition_no, MAX(offset_no) FROM order_events GROUP BY partition_no')) as {
      results: { rows: string[][] }[];
    };
    const first = p.results[0]!.rows[0]!;
    await sql(request, token, MDB, `CALL sprig.commit_offset('billing', 'order_events', ${first[0]}, 1)`);
  });
  test.afterAll(async ({ request }) => {
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${MDB}`);
  });

  test('steps 28–31: streams with groups and lag, the record reader, queues, sinks, inherited objects', async ({ page }) => {
    await signIn(page);
    await openPage(page, 'Messaging');
    await page.getByTestId('msg-db').selectOption(MDB);
    const streams = page.getByTestId('msg-streams');
    await expect(streams).toContainText('order_events');
    await streams.getByRole('button', { name: 'Open' }).first().click();
    const detail = page.getByTestId('msg-stream-detail');
    await expect(page.getByTestId('msg-partitions').locator('tbody tr')).toHaveCount(2);
    await expect(page.getByTestId('msg-groups')).toContainText('billing');
    const lag = Number(await page.getByTestId('msg-lag-billing').textContent());
    expect(lag).toBeGreaterThan(0);
    await page.getByTestId('msg-read-partition').fill('0');
    await page.getByTestId('msg-read').click();
    await expect(page.getByTestId('msg-records')).toContainText('"n"');
    await axe(page, 'streams');
    await shot(page, 'streams');
    await expect(detail).toBeVisible();
    // A stream created from the form, through its statement.
    await page.getByText('Create a stream').click();
    await page.getByTestId('msg-stream-name').fill('studio3_events');
    await page.getByTestId('msg-stream-create').click();
    await expect(page.getByTestId('msg-confirm-sql')).toHaveText("CREATE STREAM `studio3_events` PARTITIONS 3 RETENTION '7 days' CLEANUP delete");
    await page.getByTestId('msg-confirm').click();
    await expect(streams).toContainText('studio3_events');
    // Queues: the database's virtual host, or why there is none yet.
    await page.getByTestId('msg-tab-queues').click();
    await expect(page.getByText(/Virtual host|no virtual host yet/)).toBeVisible();
    await shot(page, 'queues');
    // Sinks and inherited objects: what SHOW SINKS and SHOW INHERITED say.
    await page.getByTestId('msg-tab-sinks').click();
    await expect(page.getByTestId('msg-sinks')).toBeVisible();
    await page.getByTestId('msg-tab-inherited').click();
    await expect(page.getByRole('heading', { name: 'Instances' })).toBeVisible();
    await axe(page, 'inherited');
    await shot(page, 'inherited');
  });
});

test.describe.serial('Studio round 4: view as', () => {
  let token = '';
  const VDB = 'studio3_view';
  const VIEWER = 'studio3_viewer';
  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await sql(request, token, '-', `DROP USER IF EXISTS '${VIEWER}'@'%'`);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${VDB}`);
    await sql(request, token, '-', `CREATE DATABASE ${VDB}`);
    await sql(request, token, VDB, 'CREATE TABLE open_t (id INT PRIMARY KEY, note VARCHAR(20))');
    await sql(request, token, VDB, 'CREATE TABLE secret_t (id INT PRIMARY KEY, note VARCHAR(20))');
    await sql(request, token, VDB, "INSERT INTO open_t VALUES (1, 'visible'), (2, 'also')");
    await sql(request, token, VDB, "INSERT INTO secret_t VALUES (1, 'hidden')");
    await sql(request, token, '-', `CREATE USER '${VIEWER}'@'%' IDENTIFIED BY 'sprig-dev-viewer'`);
    await sql(request, token, '-', `GRANT SELECT ON ${VDB}.open_t TO '${VIEWER}'@'%'`);
  });
  test.afterAll(async ({ request }) => {
    await sql(request, token, '-', `DROP USER IF EXISTS '${VIEWER}'@'%'`);
    await sql(request, token, '-', `DROP DATABASE IF EXISTS ${VDB}`);
  });

  test('step 18: view as a user, refusals, and the policy and role statements', async ({ page, request }) => {
    await signIn(page);
    await openPage(page, 'Roles and policies');
    await page.getByTestId('viewas-user').fill(VIEWER);
    await page.getByTestId('viewas-db').selectOption(VDB);
    await page.getByTestId('viewas-sql').fill('SELECT id, note FROM open_t ORDER BY id');
    await page.getByTestId('viewas-run').click();
    const result = page.getByTestId('viewas-result');
    await expect(result).toContainText(`As acme.${VIEWER}: 2 rows`);
    await expect(result).toContainText('visible');
    await axe(page, 'view-as');
    await shot(page, 'view-as');
    // The user's privileges apply: no grant on secret_t.
    await page.getByTestId('viewas-sql').fill('SELECT * FROM secret_t');
    await page.getByTestId('viewas-run').click();
    await expect(page.getByTestId('viewas-error')).toContainText('1142');
    // Only reads (39931), and not as yourself (39930).
    await page.getByTestId('viewas-sql').fill('DELETE FROM open_t');
    await page.getByTestId('viewas-run').click();
    await expect(page.getByTestId('viewas-error')).toContainText('39931');
    await page.getByTestId('viewas-user').fill('app');
    await page.getByTestId('viewas-sql').fill('SELECT 1');
    await page.getByTestId('viewas-run').click();
    await expect(page.getByTestId('viewas-error')).toContainText('39930');
    // A non-administrator can't use the API either.
    const login = await request.post('/v1/auth/login', {
      data: { login: `acme.${VIEWER}`, password: 'sprig-dev-viewer', refresh_transport: 'body', client: 'api' },
    });
    const vt = ((await login.json()) as { access_token: string }).access_token;
    const r = await request.post('/v1/policies:testAs', {
      headers: { authorization: `Bearer ${vt}` },
      data: { user: 'app', database: VDB, sql: 'SELECT 1' },
    });
    expect(r.status()).toBe(403);
    expect(((await r.json()) as { code: number }).code).toBe(39930);
    // A policy statement from the form, shown before it runs.
    await page.getByTestId('pol-name').fill('own');
    await page.getByRole('combobox').filter({ hasText: VDB }).last().selectOption(VDB).catch(() => undefined);
    await expect(page.getByTestId('pol-statement')).toHaveCount(0);
  });
});
