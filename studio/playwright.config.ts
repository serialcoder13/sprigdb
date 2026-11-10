// End-to-end tests (chapter 44 step 14) against a running gateway:
// STUDIO_URL=http://127.0.0.1:25080 pnpm e2e. Uses the locally installed
// Google Chrome (channel 'chrome'), so no browser bundle is downloaded.
import { defineConfig } from '@playwright/test';

const shots = process.env.STUDIO_SHOTS ?? '../target/agent-studio1/screenshots';

export default defineConfig({
  testDir: 'e2e',
  timeout: 60_000,
  workers: 1,
  reporter: [['list']],
  outputDir: '../target/agent-studio1/playwright-results',
  use: {
    baseURL: process.env.STUDIO_URL ?? 'http://127.0.0.1:25080',
    channel: 'chrome',
    headless: true,
    viewport: { width: 1280, height: 860 },
    acceptDownloads: true,
    trace: 'retain-on-failure',
  },
  metadata: { shots },
});
