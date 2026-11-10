// Chapter 52's approvals inbox end to end: the badge, filters, the item
// view with its evidence, comment, snooze, approve in a window, ignore,
// bulk actions and the policy page; accessibility with axe-core.
//
// Needs open recommendations in tenant acme (the live tests raise them:
// target/agent-approvals/live_*.sh) and STUDIO_URL of a gateway built
// with this Studio.
import { expect, test, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { SHOTS, signIn } from './fixtures';

const T = 'acme';

async function axe(page: Page, name: string) {
  const r = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  const bad = r.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical');
  expect(bad.map((v) => `${name}: ${v.id} ${v.help} (${v.nodes.length})`)).toEqual([]);
}

/** Answers the next prompts in order. */
function answer(page: Page, values: string[]) {
  const queue = [...values];
  const on = (d: import('@playwright/test').Dialog) => {
    const v = queue.shift();
    void d.accept(v ?? '');
    if (queue.length === 0) page.off('dialog', on);
  };
  page.on('dialog', on);
}

test.describe.serial('Approvals inbox (chapter 52)', () => {
  test('inbox: badge, filters, item, comment, snooze, window, ignore, bulk', async ({ page }) => {
    await signIn(page);
    await page.goto(`/studio/t/${T}/inbox`);
    await expect(page.getByRole('heading', { name: 'Approvals' })).toBeVisible();
    const table = page.getByTestId('inbox-table');
    await expect(table.locator('tbody tr').first()).toBeVisible();
    await expect(page.getByTestId('inbox-badge')).toBeVisible();
    await axe(page, 'inbox');
    await page.screenshot({ path: `${SHOTS}/approvals-inbox.png` });

    // Filter by kind: index items only.
    await page.getByLabel('Kind').selectOption('index');
    await expect(table.locator('tbody tr').first()).toContainText('index.');

    // Open the first open item: evidence and actions.
    await page.getByLabel('State').selectOption('open');
    const first = table.locator('tbody tr').first().getByRole('button');
    const id = (await first.textContent()) ?? '';
    await first.click();
    const item = page.getByTestId('inbox-item');
    await expect(item).toContainText(id);
    await expect(item.getByRole('heading', { name: 'Evidence' })).toBeVisible();
    await axe(page, 'inbox-item');
    await page.screenshot({ path: `${SHOTS}/approvals-item.png` });

    answer(page, ['checked with the app team']);
    await item.getByRole('button', { name: 'Comment' }).click();
    await expect(page.getByRole('status')).toContainText(id);
    await expect(item).toContainText('checked with the app team');

    answer(page, ['2']);
    await item.getByRole('button', { name: 'Snooze' }).click();
    await expect(page.getByRole('status')).toContainText(`${id} is snoozed`);

    // Snoozed items can be approved in a window.
    const start = new Date(Date.now() + 3_600_000).toISOString().replace(/\.\d+Z$/, 'Z');
    const end = new Date(Date.now() + 7_200_000).toISOString().replace(/\.\d+Z$/, 'Z');
    answer(page, [start, end]);
    await item.getByRole('button', { name: 'Approve in a window' }).click();
    await expect(page.getByRole('status')).toContainText(`${id} is approved`);
    await expect(item).toContainText('approved');

    // Ignore an approved item for 3 days.
    answer(page, ['not this week', '3']);
    await item.getByRole('button', { name: 'Ignore' }).click();
    await expect(page.getByRole('status')).toContainText(`${id} is ignored`);

    // Bulk: snooze every open item left.
    await page.getByLabel('State').selectOption('open');
    const boxes = table.locator('tbody input[type=checkbox]:not([disabled])');
    const n = await boxes.count();
    if (n > 0) {
      await page.getByLabel('Select all').check();
      answer(page, ['1']);
      await page.getByRole('button', { name: 'Snooze selected' }).click();
      await expect(page.getByRole('status')).toContainText(`${n} of ${n} done`);
    }
  });

  test('policy page and email subscription', async ({ page }) => {
    await signIn(page);
    await page.goto(`/studio/t/${T}/inbox/policy`);
    await expect(page.getByRole('heading', { name: 'Approval policy' })).toBeVisible();
    const t = page.getByTestId('policy-table');
    await expect(t).toContainText('index.create');
    await expect(t).toContainText('dc.move');
    await page.getByRole('textbox', { name: 'Database' }).fill('approvals_shop');
    await page.getByRole('textbox', { name: 'Table' }).fill('orders');
    await expect(t.locator('tr', { hasText: 'index.create' })).toContainText('approve');
    await page.getByLabel('My address').fill('acme-app@example.com');
    await page.getByLabel('Delivery').selectOption('digest');
    await page.getByRole('button', { name: 'Save' }).click();
    await expect(page.getByRole('status')).toContainText('Saved');
    await expect(page.getByText('acme-app@example.com')).toBeVisible();
    await axe(page, 'policy');
    await page.screenshot({ path: `${SHOTS}/approvals-policy.png` });
  });
});
