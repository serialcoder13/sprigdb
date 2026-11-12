// Operator sign-in end to end (chapter 14 "Operators", chapter 44 "Operator
// mode"; decisions OP10 and OP12): the first sign-in enrols TOTP from the
// QR code's link and shows the recovery codes once; a later sign-in takes a
// TOTP code, another a recovery code, which then can't be used again; the
// session token works on the gateway's own /admin/v1/operators routes;
// signing out ends it.
//
// Needs an operator with a password and no second factor yet:
//   sprigctl operator create <name> --role platform --password-file ...
//   (or `sprigctl operator reset-factor <name>` before a rerun)
// STUDIO_OPERATOR=<name> STUDIO_OPERATOR_PASSWORD=<password> pnpm e2e operators
// `{browser}` in STUDIO_OPERATOR stands for the Playwright project, so each
// browser enrols its own operator (deploy/compose/studio/e2e.sh setup makes
// studio-e2e-chromium, -firefox and -webkit, and run resets their factors).
import { createHmac } from 'node:crypto';
import { expect, test, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { SHOTS, signIn } from './fixtures';

const OPERATOR_NAME = process.env.STUDIO_OPERATOR ?? '';
/** This browser's operator. */
const op = () => OPERATOR_NAME.replace('{browser}', test.info().project.name);
const PASSWORD = process.env.STUDIO_OPERATOR_PASSWORD ?? '';
/** A Studio login that isn't in `studio.operator_logins` (the e2e setup
 * lists acme.app there for the older operator mode), so operator mode
 * asks for a named operator's sign-in. */
const STUDIO_LOGIN = process.env.STUDIO_OPERATOR_STUDIO_LOGIN ?? 'globex.app';

/** RFC 4648 base32 (no padding) to bytes. */
function base32(s: string): Buffer {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = 0;
  let value = 0;
  const out: number[] = [];
  for (const c of s.toUpperCase().replace(/=+$/, '')) {
    const i = alphabet.indexOf(c);
    if (i < 0) throw new Error(`not base32: ${c}`);
    value = (value << 5) | i;
    bits += 5;
    if (bits >= 8) {
      out.push((value >>> (bits - 8)) & 0xff);
      bits -= 8;
    }
  }
  return Buffer.from(out);
}

/** RFC 6238 TOTP (SHA-1, 6 digits, 30 s) at `offset` steps from now. */
function totp(secret: string, offset = 0): string {
  const step = Math.floor(Date.now() / 30_000) + offset;
  const msg = Buffer.alloc(8);
  msg.writeBigUInt64BE(BigInt(step));
  const h = createHmac('sha1', base32(secret)).update(msg).digest();
  const o = (h[h.length - 1] ?? 0) & 15;
  const n = (h.readUInt32BE(o) & 0x7fffffff) % 1_000_000;
  return String(n).padStart(6, '0');
}

async function axe(page: Page, name: string) {
  const r = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  const bad = r.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical');
  expect(bad.map((v) => `${name}: ${v.id} ${v.help} (${v.nodes.length})`)).toEqual([]);
}

async function signInForm(page: Page, password = PASSWORD) {
  const form = page.getByTestId('ops-signin');
  await expect(form).toBeVisible();
  await form.getByLabel('Operator').fill(op());
  await form.getByLabel('Password').fill(password);
  await form.getByRole('button', { name: 'Sign in' }).click();
  return form;
}

async function token(page: Page): Promise<string> {
  const raw = await page.evaluate(() => window.sessionStorage.getItem('sprig.operator'));
  return (JSON.parse(raw ?? '{}') as { token?: string }).token ?? '';
}

async function signOut(page: Page) {
  await page.getByTestId('ops-signout').click();
  await expect(page.getByTestId('ops-signin')).toBeVisible();
  expect(await token(page)).toBe('');
}

test.describe.serial('Operator sign-in (chapter 14, OP10, OP12)', () => {
  test.skip(!OPERATOR_NAME || !PASSWORD, 'STUDIO_OPERATOR and STUDIO_OPERATOR_PASSWORD name an operator without a factor');
  let secret = '';
  let recovery: string[] = [];

  test('first sign-in enrols TOTP and shows the recovery codes once', async ({ page }) => {
    await signIn(page, STUDIO_LOGIN);
    await page.goto('/studio/ops/clusters');
    await axe(page, 'operator sign-in');
    await page.screenshot({ path: `${SHOTS}/ops-signin.png` });

    // A wrong password is refused without saying which part was wrong.
    let form = await signInForm(page, `${PASSWORD}-wrong`);
    await expect(form.getByRole('alert')).toBeVisible();
    await expect(form.getByRole('alert')).toContainText('393');

    form = await signInForm(page);
    await expect(form.getByRole('heading', { name: 'Set up a second factor' })).toBeVisible();
    const qr = form.getByRole('img', { name: 'QR code of the authenticator link' });
    await expect(qr).toBeVisible();
    const uri = (await form.getByTestId('ops-otpauth').textContent()) ?? '';
    expect(uri).toMatch(/^otpauth:\/\/totp\//);
    secret = (await form.getByTestId('ops-secret').textContent()) ?? '';
    expect(new URL(uri).searchParams.get('secret')).toBe(secret);
    await axe(page, 'operator enrolment');
    await page.screenshot({ path: `${SHOTS}/ops-enrol.png` });

    // A wrong first code keeps the enrolment open.
    await form.getByLabel('Code', { exact: true }).fill(totp(secret, 5));
    await form.getByRole('button', { name: 'Confirm' }).click();
    await expect(form.getByRole('alert')).toBeVisible();
    await form.getByLabel('Code', { exact: true }).fill(totp(secret));
    await form.getByRole('button', { name: 'Confirm' }).click();

    const list = page.getByTestId('recovery-codes');
    await expect(list.locator('li')).toHaveCount(10);
    recovery = await list.locator('li').allTextContents();
    expect(recovery.every((c) => /^[a-z2-7]{5}-[a-z2-7]{5}$/.test(c))).toBe(true);
    await axe(page, 'operator recovery codes');
    await page.getByRole('button', { name: 'I have saved them' }).click();

    await expect(page.getByTestId('whoami')).toHaveText(`operator ${op()}`);
    await expect(page.getByTestId('ops-clusters')).toBeVisible();
    await expect(page.getByTestId('nav-operator')).toHaveCount(0); // operator mode has its own nav
    await axe(page, 'operator clusters');
    await page.screenshot({ path: `${SHOTS}/ops-signed-in.png` });

    // OP12: the session token works on the gateway's Admin API routes.
    const t = await token(page);
    expect(t).toMatch(/^sprigop_/);
    const who = await page.request.get('/admin/v1/operators:whoami', { headers: { authorization: `Bearer ${t}` } });
    expect(who.status()).toBe(200);
    expect(JSON.stringify(await who.json())).toContain(op());
    const me = await page.request.get(`/admin/v1/operators/${op()}`, { headers: { authorization: `Bearer ${t}` } });
    expect(me.status()).toBe(200);
    expect(((await me.json()) as { recovery_codes_left?: number }).recovery_codes_left).toBe(10);

    await signOut(page);
    const after = await page.request.get('/admin/v1/operators:whoami', { headers: { authorization: `Bearer ${t}` } });
    expect(after.status()).toBe(401);
  });

  test('a later sign-in takes a TOTP code, then a recovery code once', async ({ page }) => {
    test.skip(!secret, 'needs the enrolment of the previous test');
    await signIn(page, STUDIO_LOGIN);
    await page.goto('/studio/ops/dcs');

    // Password first; the code field appears when the controller asks.
    let form = await signInForm(page);
    await expect(form.getByLabel('Code', { exact: true })).toBeVisible();
    // The enrolment used this step: the next one is accepted (one step of drift).
    await form.getByLabel('Code', { exact: true }).fill(totp(secret, 1));
    await form.getByRole('button', { name: 'Sign in' }).click();
    await expect(page.getByTestId('whoami')).toHaveText(`operator ${op()}`);
    await expect(page.getByTestId('ops-dcs')).toBeVisible();
    await signOut(page);

    // A recovery code in the same field.
    form = await signInForm(page);
    await form.getByLabel('Code', { exact: true }).fill(recovery[0] ?? '');
    await form.getByRole('button', { name: 'Sign in' }).click();
    await expect(page.getByTestId('whoami')).toHaveText(`operator ${op()}`);
    const t = await token(page);
    const me = await page.request.get(`/admin/v1/operators/${op()}`, { headers: { authorization: `Bearer ${t}` } });
    expect(((await me.json()) as { recovery_codes_left?: number }).recovery_codes_left).toBe(9);
    await signOut(page);

    // The same recovery code again: refused.
    form = await signInForm(page);
    await form.getByLabel('Code', { exact: true }).fill(recovery[0] ?? '');
    await form.getByRole('button', { name: 'Sign in' }).click();
    await expect(form.getByRole('alert')).toBeVisible();
    await expect(page.getByTestId('ops-signout')).toHaveCount(0);
    await page.screenshot({ path: `${SHOTS}/ops-recovery-reused.png` });
  });
});
