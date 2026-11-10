import { expect, type APIRequestContext, type Page } from '@playwright/test';

export const LOGIN = process.env.STUDIO_LOGIN ?? 'acme.app';
export const PASSWORD = process.env.STUDIO_PASSWORD ?? 'sprig-dev-app';
/** Databases this suite creates and drops, by exact name. */
export const DB = 'studio0_e2e';
export const SHOTS = process.env.STUDIO_SHOTS ?? '../target/agent-studio1/screenshots';

export async function signIn(page: Page, login = LOGIN, password = PASSWORD): Promise<void> {
  await page.goto('/studio/login');
  await page.getByLabel('User', { exact: true }).fill(login);
  await page.getByLabel('Password').fill(password);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByTestId('whoami')).toHaveText(login);
}

/** An API token for setup statements (the same public API). */
export async function apiToken(request: APIRequestContext): Promise<string> {
  const r = await request.post('/v1/auth/login', {
    data: { login: LOGIN, password: PASSWORD, refresh_transport: 'body', client: 'api' },
  });
  expect(r.status()).toBe(200);
  return ((await r.json()) as { access_token: string }).access_token;
}

export async function sql(request: APIRequestContext, token: string, db: string, text: string): Promise<unknown> {
  const r = await request.post(`/v1/databases/${db}/sql:execute`, {
    headers: { authorization: `Bearer ${token}` },
    data: { sql: text },
  });
  const body: unknown = await r.json();
  if (r.status() !== 200) throw new Error(`${text}: ${JSON.stringify(body)}`);
  return body;
}

/** Types into Monaco (it keeps its own textarea). */
export async function typeSql(page: Page, text: string): Promise<void> {
  const ed = page.getByTestId('sql-editor');
  await ed.click();
  await page.keyboard.press('ControlOrMeta+A');
  await page.keyboard.press('Delete');
  await page.keyboard.insertText(text);
}
