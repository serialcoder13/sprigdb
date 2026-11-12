// Studio round 6 end to end (chapter 44 steps 26, 29–31; decisions ST54–),
// against studio6's private deployment, on Chromium, Firefox and WebKit:
//   STUDIO_URL=http://127.0.0.1:28080 STUDIO_AMQP=127.0.0.1:28682 \
//   STUDIO_HOOK_PORT=28881 STUDIO_CLOSED=127.0.0.1:28999 \
//   STUDIO_SHOTS=../target/agent-studio6/screenshots pnpm e2e studio6
//
// - queues: ready, unacknowledged and consumers (sprig.mq_info), browse
//   without taking (mq_browse), republish and purge with their
//   confirmations, and a user without queue rights refused;
// - sinks: TEST CONNECTION's steps (ok and failed), a test record written
//   on request, an unsaved connection tested (TEST CREATE CONNECTION), and
//   TEST SINK's dry run (nothing delivered);
// - logic: test runs of a draft and of the deployed version with
//   arguments, snapshot rows and fetch mocks, and the report; a staged
//   rollout moved, shown and promoted; DROP refused with its dependents
//   (39875), then CASCADE;
// - the Inheritance page (chapter 42; IG14) as a parent and a child tenant.
//
// Setup the operator does once (rule 15: every address explicit):
//   sprigctl settings set sink.destination_allowlist '["127.0.0.1:28881", "127.0.0.1:28999"]' --scope tenant:acme
//   sprigctl settings set logic.outbound_allowlist '["rates.example.com"]' --scope tenant:acme
//   sprigctl tenant create studio6kid --under acme; sprigctl tenant user add studio6kid app ...
//   GRANT SPRIG_SINK_ADMIN ON *.* TO app (as acme.app)
import { expect, test, type APIRequestContext, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { createServer, type Server } from 'node:http';
import { Amqp, amqpAddress } from './amqp';
import { apiToken, LOGIN, PASSWORD, SHOTS, signIn, sql } from './fixtures';

/** Everything this suite creates, dropped by exact name afterwards. */
const DB6 = 'studio6_ui';
const INH = 'studio6_inh';
const RO = 'studio6_ro';
const RO_PASSWORD = 'studio6-ro-password';
const KID = process.env.STUDIO_CHILD ?? 'studio6kid.app';
const HOOK_PORT = Number(process.env.STUDIO_HOOK_PORT ?? 0);
const CLOSED = process.env.STUDIO_CLOSED ?? '';

async function axe(page: Page, name: string) {
  const r = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  const bad = r.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical');
  expect(bad.map((v) => `${name}: ${v.id} ${v.help} (${v.nodes.map((n) => n.target.join(' ')).join(', ')})`)).toEqual([]);
}

async function shot(page: Page, name: string) {
  const browser = test.info().project.name;
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.waitForTimeout(150);
    await page.screenshot({ path: `${SHOTS}/${browser}/${name}-${scheme}.png`, fullPage: false });
  }
  await page.emulateMedia({ colorScheme: 'light' });
}

type Rows = { results: { rows?: (string | null)[][] }[] };
const first = (r: unknown) => (r as Rows).results.find((x) => x.rows)?.rows ?? [];

async function waitFor(what: string, f: () => Promise<boolean>, ms = 20_000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await f()) return;
    await new Promise((ok) => setTimeout(ok, 300));
  }
  throw new Error(`timed out waiting for ${what}`);
}

/** Replaces a Monaco editor's text, once Monaco has drawn its lines (a
 * select-all before then selects nothing). */
async function typeInto(page: Page, label: string, text: string) {
  // The page's only Monaco; `label` is checked on Chrome, whose Monaco
  // labels its EditContext element (Firefox's and WebKit's label a hidden
  // textarea Playwright's locators don't match the same way).
  const host = page.getByTestId('sql-editor').first();
  if (test.info().project.name === 'chromium') await expect(host.getByLabel(label, { exact: true })).toHaveCount(1);
  const lines = host.locator('.view-lines');
  await lines.click();
  for (let i = 0; i < 5 && (await lines.innerText()).trim() !== ''; i++) {
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.press('Backspace');
    await page.waitForTimeout(100);
  }
  await expect(lines).toHaveText('');
  // One line at a time: Firefox's Monaco doubles a multi-line insertText.
  const parts = text.split('\n');
  for (const [i, l] of parts.entries()) {
    await page.keyboard.insertText(l);
    if (i < parts.length - 1) await page.keyboard.press('Enter');
  }
  await expect(host.locator('.view-lines')).toContainText(text.split('\n')[0]!.slice(0, 12).replace(/ /g, '\u00a0'));
}

async function signOut(page: Page) {
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByLabel('User', { exact: true })).toBeVisible();
}

async function messaging(page: Page, tab: 'queues' | 'sinks') {
  await page.goto(`/studio/t/acme/messaging?db=${DB6}&tab=${tab}`);
}

test.describe.serial('Studio round 6', () => {
  let token = '';
  let amqp: Amqp | null = null;
  let consumer: Amqp | null = null;
  let hook: Server | null = null;
  const seen: string[] = [];

  test.beforeAll(async ({ request }) => {
    token = await apiToken(request);
    await cleanup(request, token);
    await sql(request, token, '-', `CREATE DATABASE ${DB6}`);
    await sql(request, token, DB6, 'CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR(40), price DOUBLE)');
    await sql(request, token, DB6, "INSERT INTO products VALUES (7, 'pen', 2.5), (8, 'ink', 4)");
    await sql(request, token, DB6, 'CREATE TABLE orders (id BIGINT AUTO_INCREMENT PRIMARY KEY, product BIGINT, total DOUBLE)');
    await sql(request, token, '-', `CREATE USER '${RO}'@'%' IDENTIFIED BY '${RO_PASSWORD}'`);
    await sql(request, token, '-', `GRANT SELECT ON ${DB6}.products TO '${RO}'@'%'`);
    if (HOOK_PORT) {
      hook = createServer((q, r) => {
        seen.push(`${q.method} ${q.url}`);
        q.resume();
        r.writeHead(q.method === 'OPTIONS' ? 204 : 200);
        r.end();
      });
      await new Promise<void>((ok) => hook!.listen(HOOK_PORT, '127.0.0.1', ok));
    }
  });

  test.afterAll(async ({ request }) => {
    await consumer?.close();
    await amqp?.close();
    await new Promise<void>((ok) => (hook ? hook.close(() => ok()) : ok()));
    await cleanup(request, token);
  });

  test('queues: counts and consumers, browse, republish, purge; refused without queue rights', async ({ page, request }) => {
    const at = amqpAddress();
    test.skip(!at, 'STUDIO_AMQP names the gateway AMQP listener (host:port)');
    amqp = await Amqp.open(at!.host, at!.port, LOGIN, PASSWORD, DB6);
    await amqp.exchangeDeclare('orders6', 'direct');
    await amqp.queueDeclare('orders6_q', { 'x-dead-letter-exchange': 'dlx6' });
    await amqp.queueDeclare('parking6');
    await amqp.queueBind('orders6_q', 'orders6', 'new');
    for (let i = 1; i <= 5; i++) await sql(request, token, DB6, `CALL sprig.mq_publish('orders6', 'new', '{"n": ${i}}', '{}', FALSE, '${DB6}')`);
    consumer = await Amqp.open(at!.host, at!.port, LOGIN, PASSWORD, DB6);
    await consumer.consume('orders6_q', 1);
    await waitFor('one unacknowledged delivery', async () => {
      const r = first(await sql(request, token, DB6, `CALL sprig.mq_info('orders6_q', '${DB6}')`));
      return r[0]?.join(',') === '4,1,1';
    });

    await signIn(page);
    await messaging(page, 'queues');
    await expect(page.getByText(`Virtual host ${DB6}`)).toBeVisible();
    await expect(page.getByTestId('mq-ready-orders6_q')).toHaveText('4');
    await expect(page.getByTestId('mq-unacked-orders6_q')).toHaveText('1');
    await expect(page.getByTestId('mq-consumers-orders6_q')).toHaveText('1');
    await expect(page.getByTestId('mq-ready-parking6')).toHaveText('0');
    await axe(page, 'queues');
    await shot(page, 'queues-counts');

    // Browse: every stored message, the delivered one too; nothing taken.
    await page.getByTestId('mq-open-orders6_q').click();
    await page.getByTestId('mq-browse').click();
    await expect(page.getByTestId('mq-browsed').locator('tbody tr')).toHaveCount(5);
    await expect(page.getByTestId('mq-browsed')).toContainText('"n": 3');
    await expect(page.getByTestId('mq-ready-orders6_q')).toHaveText('4');
    await axe(page, 'queue-browse');
    await shot(page, 'queue-browse');

    // Republish two to parking6 through the default exchange.
    await page.getByTestId('mq-republish-open').click();
    await page.getByTestId('mq-republish-exchange').selectOption('');
    await page.getByTestId('mq-republish-key').fill('parking6');
    await page.getByTestId('mq-republish-max').fill('2');
    await page.getByTestId('mq-republish').click();
    await expect(page.getByTestId('msg-confirm-sql')).toHaveText(`CALL sprig.mq_republish('orders6_q', '', 'parking6', 2, '${DB6}')`);
    await expect(page.getByTestId('msg-confirm-note')).toContainText('At least once');
    await shot(page, 'queue-republish-confirm');
    await page.getByTestId('msg-confirm').click();
    await expect(page.getByTestId('msg-outcome')).toHaveText('Moved 2 messages.');
    await expect(page.getByTestId('mq-ready-orders6_q')).toHaveText('2');
    await expect(page.getByTestId('mq-ready-parking6')).toHaveText('2');

    // Purge parking6: the dialog says how many go.
    await page.getByTestId('mq-open-parking6').click();
    await expect(page.getByTestId('mq-info')).toContainText('2');
    await page.getByTestId('mq-purge').click();
    await expect(page.getByTestId('msg-confirm-sql')).toHaveText(`CALL sprig.mq_purge('parking6', '${DB6}')`);
    await expect(page.getByTestId('msg-confirm-note')).toContainText('removes 2 ready messages of parking6');
    await axe(page, 'queue-purge-confirm');
    await shot(page, 'queue-purge-confirm');
    await page.getByTestId('msg-confirm').click();
    await expect(page.getByTestId('msg-outcome')).toHaveText('Purged 2 messages.');
    await expect(page.getByTestId('mq-ready-parking6')).toHaveText('0');
    // Cancelling runs nothing.
    await page.getByTestId('mq-open-orders6_q').click();
    await page.getByTestId('mq-purge').click();
    await page.getByRole('button', { name: 'Cancel' }).click();
    await expect(page.getByTestId('mq-ready-orders6_q')).toHaveText('2');

    // A user with SELECT on one table sees the topology, not the queues' contents.
    await signOut(page);
    await signIn(page, `acme.${RO}`, RO_PASSWORD);
    await messaging(page, 'queues');
    await expect(page.getByTestId('mq-ready-orders6_q')).toHaveText('?');
    await page.getByTestId('mq-open-orders6_q').click();
    await expect(page.getByTestId('mq-queue-detail').getByRole('alert').first()).toContainText('39253');
    await page.getByTestId('mq-purge').click();
    await page.getByTestId('msg-confirm').click();
    await expect(page.getByTestId('msg-outcome')).toContainText('refused');
    await shot(page, 'queue-refused');
    const r = first(await sql(request, token, DB6, `CALL sprig.mq_info('orders6_q', '${DB6}')`));
    expect(r[0]?.[0]).toBe('2');
    // Round 7 (ST66): every queue in one call, the same counts.
    const all = first(await sql(request, token, DB6, `CALL sprig.mq_info(NULL, '${DB6}')`));
    expect(all.find((x) => x[0] === 'orders6_q')?.slice(1)).toEqual([r[0]?.[0], r[0]?.[1], r[0]?.[2]]);
  });

  test('sinks: connection tests, a test before saving, a test record, the dry run', async ({ page, request }) => {
    test.skip(!HOOK_PORT || !CLOSED, 'STUDIO_HOOK_PORT and STUDIO_CLOSED name allowlisted addresses');
    await sql(request, token, DB6, 'CREATE STREAM events6 PARTITIONS 1');
    for (let i = 0; i < 6; i++) {
      await sql(request, token, DB6, `INSERT INTO events6 (\`key\`, \`value\`) VALUES ('k${i}', JSON_OBJECT('kind', '${i % 2 ? 'b' : 'a'}', 'n', ${i}))`);
    }
    await sql(request, token, DB6, `CREATE CONNECTION hook6 TYPE WEBHOOK WITH (url = 'http://127.0.0.1:${HOOK_PORT}/in') CREDENTIALS (signing_secret = 'studio6-signing')`);
    await sql(request, token, DB6, "CREATE SINK s6 FROM STREAM events6 WHERE value->>'$.kind' = 'a' TO WEBHOOK CONNECTION hook6 START AT LATEST");
    await sql(request, token, DB6, 'ALTER SINK s6 PAUSE');

    await signIn(page);
    await messaging(page, 'sinks');
    await page.getByTestId('msg-connections').getByRole('row', { name: /hook6/ }).getByRole('button', { name: 'Open' }).click();
    const before = seen.length;
    await page.getByTestId('conn-test-run').click();
    await expect(page.getByTestId('conn-test-steps-summary')).toHaveText(/All \d+ steps passed\./);
    await expect(page.getByTestId('conn-test-steps')).toContainText('OPTIONS answered HTTP 204');
    expect(seen.slice(before)).toEqual(['OPTIONS /in']);
    await axe(page, 'connection-test');
    await shot(page, 'connection-test');

    // A test record is a write: shown first.
    await page.getByTestId('conn-test-write').click();
    await expect(page.getByTestId('msg-confirm-sql')).toHaveText('TEST CONNECTION `hook6` WRITE');
    await expect(page.getByTestId('msg-confirm-note')).toContainText('writes one marked test record');
    await page.getByTestId('msg-confirm').click();
    await expect(page.getByTestId('conn-test-steps')).toBeVisible();
    await waitFor('the test record', async () => Promise.resolve(seen.slice(before).some((s) => s.startsWith('POST'))));

    // An unsaved connection, tested with TEST CREATE CONNECTION (ST68): nothing is created.
    await page.getByTestId('conn-new-open').click();
    await page.getByTestId('conn-name').fill('db6');
    await page.getByTestId('conn-kind').selectOption('mysql');
    const [host, port] = CLOSED.split(':');
    await page.getByTestId('conn-opt-host').fill(host!);
    await page.getByTestId('conn-opt-port').fill(port!);
    await page.getByTestId('conn-opt-database').fill('x');
    await page.getByTestId('conn-more').fill("tls = 'off'");
    const creds = page.getByTestId('conn-new-creds');
    await creds.getByRole('button', { name: 'Add a credential' }).click();
    await page.getByTestId('conn-new-cred-0').fill('studio6-user');
    await creds.getByRole('button', { name: 'Add a credential' }).click();
    await creds.getByRole('combobox').nth(1).selectOption('password');
    await page.getByTestId('conn-new-cred-1').fill('studio6-never-shown');
    await page.getByTestId('conn-test-first').click();
    const shown = page.getByTestId('msg-confirm-sql');
    await expect(shown).toContainText('TEST CREATE CONNECTION `db6` TYPE mysql');
    await expect(shown).toContainText("username = '********', password = '********'");
    await page.getByTestId('msg-confirm').click();
    await expect(page.getByTestId('conn-test-first-result')).toContainText(/1 of \d+ steps failed\./);
    await expect(page.getByTestId('conn-test-first-result')).toContainText('failed');
    // The value stays only in its password field (for "Create"), never in text on screen.
    expect(await page.locator('body').innerText()).not.toContain('studio6-never-shown');
    const conns = first(await sql(request, token, DB6, `SHOW CONNECTIONS FROM ${DB6}`));
    expect(conns.map((c) => c[1])).toEqual(['hook6']);
    await axe(page, 'connection-test-first');
    await shot(page, 'connection-test-first');

    // The sink's dry run: 3 delivered, 3 filtered, nothing sent.
    const posts = seen.filter((s) => s.startsWith('POST')).length;
    await page.getByTestId('msg-sinks').getByRole('row', { name: /s6/ }).getByRole('button', { name: 'Open' }).click();
    await page.getByTestId('sink-dry-run-go').click();
    await expect(page.getByTestId('sink-dry-summary')).toHaveText('3 deliver, 3 filtered');
    await expect(page.getByTestId('sink-dry-rows').locator('tbody tr')).toHaveCount(6);
    expect(seen.filter((s) => s.startsWith('POST')).length).toBe(posts);
    await page.getByTestId('sink-dry-sample').fill('0');
    await page.getByTestId('sink-dry-run-go').click();
    await expect(page.getByTestId('sink-dry-run').getByRole('alert')).toContainText('1 to 1,000');
    await axe(page, 'sink-dry-run');
    await shot(page, 'sink-dry-run');

    // Round 7: a new sink from the form (ST70), dry-run before it exists
    // (TEST CREATE SINK, ST69), then created.
    await page.getByTestId('sink-new-open').click();
    const form = page.getByTestId('sink-new');
    await form.getByTestId('sink-new-name').fill('s6b');
    await form.getByTestId('sink-new-source').fill('events6');
    await form.getByTestId('sink-new-where').fill("value->>'$.kind' = 'b'");
    await form.getByTestId('sink-new-connection').fill('hook6');
    await form.getByTestId('sink-new-dry-sample').fill('6');
    await form.getByTestId('sink-new-dry-run-go').click();
    await expect(form.getByTestId('sink-new-dry-summary')).toHaveText('3 deliver, 3 filtered');
    await expect(form.getByTestId('sink-new-dry-rows').locator('tbody tr')).toHaveCount(6);
    expect(first(await sql(request, token, DB6, `SHOW SINKS FROM ${DB6}`)).map((r) => r[1])).toEqual(['s6']);
    expect(seen.filter((s) => s.startsWith('POST')).length).toBe(posts);
    await axe(page, 'sink-new-dry-run');
    await shot(page, 'sink-new-dry-run');
    await form.getByTestId('sink-new-create').click();
    await expect(page.getByTestId('msg-confirm-sql')).toHaveText(
      "CREATE SINK `s6b` FROM STREAM `events6` WHERE value->>'$.kind' = 'b' TO WEBHOOK CONNECTION `hook6`",
    );
    await page.getByTestId('msg-confirm').click();
    await waitFor('the new sink', async () => first(await sql(request, token, DB6, `SHOW SINKS FROM ${DB6}`)).some((r) => r[1] === 's6b'));
    await sql(request, token, DB6, 'ALTER SINK s6b PAUSE');
  });

  test('logic: test runs with inputs, snapshot rows and fetch mocks; rollout; drop with dependents', async ({ page, request }) => {
    test.setTimeout(150_000);
    await signIn(page);
    // A draft, tested before it exists.
    await page.goto(`/studio/t/acme/logic/${DB6}/new/function`);
    await page.getByTestId('logic-name').fill('quote6');
    await page.getByTestId('logic-params').fill('productId BIGINT, qty INT');
    await page.getByTestId('logic-returns').fill('');
    await typeInto(
      page,
      'javascript source of quote6',
      [
        "const p = db.sqlOne('SELECT price FROM products WHERE id = ?', [productId]);",
        "if (p === null) { return sprig.event('error', {message: 'no product'}); }",
        "const r = fetch('https://rates.example.com/eur').json();",
        "console.log('rate ' + r.rate);",
        "db.exec('INSERT INTO orders (product, total) VALUES (?, ?)', [productId, p.price * qty * r.rate]);",
        'return {total: p.price * qty * r.rate};',
      ].join('\n'),
    );
    await page.getByTestId('logic-test-draft').click();
    const panel = page.getByTestId('logic-testrun');
    await panel.getByTestId('logic-arg-productId').fill('7');
    await panel.getByTestId('logic-arg-qty').fill('2');
    await panel.getByText('Snapshot rows').click();
    await panel.getByTestId('logic-test-rows').fill('{"products": [{"id": 7, "name": "pen", "price": 10}]}');
    await panel.getByText('Mocks for fetch').click();
    await panel.getByTestId('logic-mock-add').click();
    await panel.getByTestId('logic-mock-url-0').fill('https://rates.example.com/*');
    await panel.getByTestId('logic-mock-body-0').fill('{"rate": 1.5}');
    await panel.getByTestId('logic-test-run').click();
    const report = page.getByTestId('logic-report');
    await expect(report).toContainText('Draft of function quote6');
    await expect(page.getByTestId('logic-report-result')).toContainText('"total": 30');
    await expect(page.getByTestId('logic-report-statements').locator('tbody tr')).toHaveCount(2);
    await expect(page.getByTestId('logic-report-writes')).toContainText('orders (product, total) VALUES (7, 30)');
    await expect(page.getByTestId('logic-report-logs')).toHaveText('rate 1.5');
    await expect(page.getByTestId('logic-report-fetches')).toContainText('https://rates.example.com/eur');
    await expect(page.getByTestId('logic-report-table-products')).toContainText('pen');
    await expect(page.getByTestId('logic-report-budget')).toContainText('Data operations');
    await expect(page.getByTestId('logic-report-timing')).toContainText('ms');
    await axe(page, 'logic-test-draft');
    await report.scrollIntoViewIfNeeded();
    await shot(page, 'logic-test-draft');
    // Nothing was written, and the draft isn't deployed.
    expect(first(await sql(request, token, DB6, 'SELECT COUNT(*) FROM orders'))[0]![0]).toBe('0');
    expect(first(await sql(request, token, DB6, 'SELECT price FROM products WHERE id = 7'))[0]![0]).toBe('2.5');

    // Deploy, then a test run of the deployed version: no mock, so fetch fails and the report says why.
    await page.getByTestId('logic-deploy').click();
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-active')).toHaveText('1');
    await page.getByTestId('logic-tab-test').click();
    await page.getByTestId('logic-arg-productId').fill('99');
    await page.getByTestId('logic-arg-qty').fill('1');
    await page.getByTestId('logic-test-run').click();
    await expect(page.getByTestId('logic-report-event')).toContainText('error');
    await expect(page.getByTestId('logic-report-result')).toContainText('no product');
    await page.getByTestId('logic-arg-productId').fill('7');
    await page.getByTestId('logic-test-run').click();
    await expect(page.getByTestId('logic-report-error')).toBeVisible();
    await expect(page.getByTestId('logic-report-fetches')).toContainText('rates.example.com');
    await page.getByTestId('logic-report').scrollIntoViewIfNeeded();
    await shot(page, 'logic-test-deployed');

    // A staged rollout: version 2 to 30% of callers, moved to 60%, promoted.
    await page.getByTestId('logic-tab-code').click();
    await typeInto(page, 'javascript source of quote6', 'return {total: productId * qty};');
    await page.getByTestId('logic-staged').check();
    await page.getByTestId('logic-staged-pct').fill('30');
    await page.getByTestId('logic-deploy').click();
    await expect(page.getByTestId('logic-confirm-sql')).toContainText("WITH (SPRIG_TIER = 'auto', SPRIG_ROLLOUT = 30) AS");
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-notice')).toContainText('rollout to 30%');
    for (let i = 0; i < 3; i++) await sql(request, token, DB6, `CALL ${DB6}.quote6(99, ${i + 1})`).catch(() => undefined);
    await page.getByTestId('logic-tab-rollout').click();
    await expect(page.getByTestId('logic-rollout-pct')).toHaveText('30% of callers');
    await expect(page.getByTestId('logic-rollout-stage')).toContainText('v2');
    await page.getByTestId('logic-rollout-to').fill('60');
    await page.getByTestId('logic-rollout-move').click();
    await expect(page.getByTestId('logic-confirm-sql')).toHaveText(`ALTER FUNCTION \`${DB6}\`.\`quote6\` ROLLOUT TO 60`);
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-rollout-pct')).toHaveText('60% of callers');
    await expect(page.getByTestId('logic-rollout-stats')).toBeVisible();
    await axe(page, 'logic-rollout');
    await shot(page, 'logic-rollout');
    await page.getByTestId('logic-rollout-promote').click();
    await expect(page.getByTestId('logic-confirm-note')).toContainText('active version for every caller');
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-rollout-none')).toBeVisible();
    await expect(page.getByTestId('logic-active')).toHaveText('2');

    // Drop: what uses it is shown first (SHOW DEPENDENTS, ST67), since
    // RESTRICT would be refused (39875) while a schedule calls it; then CASCADE.
    await sql(request, token, DB6, `CREATE SCHEDULE quote6_nightly CRON '0 3 * * *' DO CALL quote6(7, 1)`);
    const deps = (await sql(request, token, DB6, `SHOW DEPENDENTS OF FUNCTION ${DB6}.quote6`)) as Rows;
    expect(first(deps)).toEqual([['schedule', 'quote6_nightly', 'dropped', 'the schedule calls it and is dropped with it']]);
    await page.getByTestId('logic-drop').click();
    await expect(page.getByTestId('logic-dependents')).toContainText('39875');
    await expect(page.getByTestId('logic-dep-schedules')).toContainText('quote6_nightly');
    await expect(page.getByTestId('logic-dep-schedules')).toContainText('dropped too');
    await axe(page, 'logic-drop-dependents');
    await shot(page, 'logic-drop-dependents');
    await page.getByTestId('logic-drop-cascade').click();
    await expect(page.getByTestId('logic-confirm-sql')).toHaveText(`DROP FUNCTION \`${DB6}\`.\`quote6\` CASCADE`);
    await page.getByTestId('logic-confirm').click();
    await expect(page.getByTestId('logic-modules')).not.toContainText('quote6');
  });

  test('inheritance page: tables, shares and why a delete is refused, as parent and child', async ({ page, request }) => {
    test.setTimeout(150_000);
    await sql(request, token, '-', `CREATE DATABASE ${INH}`);
    await sql(request, token, INH, 'CREATE TABLE t2 (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(40) NULL)');
    await sql(
      request,
      token,
      INH,
      'CREATE TABLE t1 (id INT AUTO_INCREMENT PRIMARY KEY, t2_id INT NULL, note VARCHAR(40) NULL, CONSTRAINT fk_t2 FOREIGN KEY (t2_id) REFERENCES t2 (id))',
    );
    await sql(request, token, INH, 'ALTER TABLE t1 SPRIG INHERITABLE KEY (id)');
    await sql(request, token, INH, 'CREATE SHARE inh6 ON t1 TO ALL CHILDREN WITH RESHARE');
    await sql(request, token, INH, "INSERT INTO t2 (name) VALUES ('k1'), ('k2')");
    await sql(request, token, INH, "INSERT INTO t1 (t2_id, note) VALUES (1, 'a')");
    // The child references the parent's k2 from a row of its own.
    const kid = await childToken(request);
    // A child's session finds a newly shared database only after it lists
    // its databases (SHOW DATABASES or SHOW INHERITABLE TABLES); until then
    // its name is 1049 (reported to chapter 42's owners, round 6).
    await waitFor('the child to see t1', async () => {
      try {
        await sql(request, kid, '-', 'SHOW DATABASES');
        return first(await sql(request, kid, INH, "SELECT COUNT(*) FROM t1 WHERE note = 'a'"))[0]?.[0] === '1';
      } catch {
        return false;
      }
    }, 30_000);
    await sql(request, kid, INH, "INSERT INTO t1 (t2_id, note) VALUES (2, 'kid row')");

    await signIn(page);
    await page.getByRole('link', { name: 'Inheritance' }).click();
    const tables = page.getByTestId('inh-tables');
    await expect(tables).toContainText(`${INH}.t1`);
    await expect(tables.getByRole('row', { name: new RegExp(`^${INH}\\.t1 id`) })).toContainText('declared here');
    await expect(tables).toContainText(`${INH}.t1, ${INH}.t2`);
    const shares = page.getByTestId('inh-shares');
    await expect(shares).toContainText('inh6');
    await shares.getByRole('button', { name: 'Definition' }).click();
    await expect(page.getByTestId('inh-share-def')).toContainText('CREATE SHARE `inh6` ON `studio6_inh`.`t1` TO ALL CHILDREN WITH RESHARE');
    await page.getByTestId('inh-db').selectOption(INH);
    await page.getByTestId('inh-table').fill('t2');
    await page.getByTestId('inh-where').fill('id = 2');
    await page.getByRole('button', { name: 'Find references' }).click();
    const blockers = page.getByTestId('inh-blockers');
    await expect(blockers).toContainText('studio6kid');
    await expect(blockers).toContainText('fk_t2');
    await page.getByTestId('inh-where').fill('id = 99');
    await page.getByRole('button', { name: 'Find references' }).click();
    await expect(page.getByTestId('inh-none')).toBeVisible();
    await page.getByRole('button', { name: 'Check ids' }).click();
    await expect(page.getByTestId('inh-none')).toContainText('No id is held by more than one tenant');
    await axe(page, 'inheritance');
    await shot(page, 'inheritance');

    // The child: the tables come from acme, one level up, and the share is received.
    await signOut(page);
    await signIn(page, KID, PASSWORD);
    await page.getByRole('link', { name: 'Inheritance' }).click();
    await expect(page.getByTestId('inh-tables').getByRole('row', { name: new RegExp(`^${INH}\\.t1 id`) })).toContainText('inherited from acme (1 level up)');
    await expect(page.getByTestId('inh-shares')).toContainText('received');
    await axe(page, 'inheritance-child');
    await shot(page, 'inheritance-child');
  });
});

async function childToken(request: APIRequestContext): Promise<string> {
  const r = await request.post('/v1/auth/login', { data: { login: KID, password: PASSWORD, refresh_transport: 'body', client: 'api' } });
  expect(r.status()).toBe(200);
  return ((await r.json()) as { access_token: string }).access_token;
}

/** Drops what this suite made, by exact name. */
async function cleanup(request: APIRequestContext, token: string) {
  const quiet = (p: Promise<unknown>) => p.catch(() => undefined);
  await quiet(sql(request, token, DB6, 'DROP SINK IF EXISTS s6'));
  await quiet(sql(request, token, DB6, 'DROP CONNECTION IF EXISTS hook6'));
  await quiet(sql(request, token, DB6, 'DROP CONNECTION IF EXISTS db6'));
  await quiet(sql(request, token, DB6, 'DROP SCHEDULE IF EXISTS quote6_nightly'));
  await quiet(sql(request, token, DB6, 'DROP FUNCTION IF EXISTS quote6 CASCADE'));
  await quiet(sql(request, token, INH, 'DROP SHARE IF EXISTS inh6'));
  await quiet(sql(request, token, '-', `DROP USER IF EXISTS '${RO}'@'%'`));
  await quiet(sql(request, token, '-', `DROP DATABASE IF EXISTS ${INH}`));
  // The child's own rows outlive the parent's database (chapter 42): the
  // child drops what is left of its instance once the parent's is gone.
  const kid = await childToken(request).catch(() => null);
  if (kid) await quiet(sql(request, kid, '-', `DROP DATABASE IF EXISTS ${INH}`));
  await quiet(sql(request, token, '-', `DROP DATABASE IF EXISTS ${DB6}`));
}
