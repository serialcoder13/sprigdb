// End-to-end tests (chapter 44 step 14) against a running gateway:
// STUDIO_URL=http://127.0.0.1:25080 pnpm e2e. Three projects (Studio round
// 6): `chromium` uses the locally installed Google Chrome (channel
// 'chrome'); `firefox` and `webkit` use Playwright's own builds in its cache
// (`npx playwright install firefox webkit`, approved 11 Nov). Pick one with
// `--project firefox`; without it, all three run.
import { defineConfig } from '@playwright/test';

const shots = process.env.STUDIO_SHOTS ?? '../target/agent-studio1/screenshots';
const viewport = { width: 1280, height: 860 };

export default defineConfig({
  testDir: 'e2e',
  timeout: 60_000,
  workers: 1,
  reporter: [['list']],
  outputDir: process.env.STUDIO_OUT ?? '../target/agent-studio1/playwright-results',
  use: {
    baseURL: process.env.STUDIO_URL ?? 'http://127.0.0.1:25080',
    headless: true,
    viewport,
    acceptDownloads: true,
    trace: 'retain-on-failure',
  },
  // Each browser keeps its own user agent (no device presets): Monaco picks
  // its keyboard (Cmd or Ctrl) from it, and Playwright's ControlOrMeta
  // follows the machine.
  projects: [
    { name: 'chromium', use: { browserName: 'chromium', channel: 'chrome' } },
    { name: 'firefox', use: { browserName: 'firefox' } },
    { name: 'webkit', use: { browserName: 'webkit' } },
  ],
  metadata: { shots },
});
