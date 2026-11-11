/// <reference types="vitest/config" />
// Studio's build (chapter 44, "Serving assets"): everything under /studio/,
// hashed files under assets/, no inline scripts (the CSP has script-src
// 'self'), large libraries (Monaco, ECharts) split into chunks that load on
// first use.
import { defineConfig, type ProxyOptions } from 'vite';
import react from '@vitejs/plugin-react';

function apiProxy(): Record<string, ProxyOptions> {
  const target = process.env.STUDIO_API;
  return target
    ? {
        '/v1': { target, changeOrigin: false },
        '/admin/v1': { target, changeOrigin: false },
        '/studio/_': { target, changeOrigin: false },
      }
    : {};
}

export default defineConfig({
  base: '/studio/',
  plugins: [react()],
  // @fincity/kirun-ui imports the whole of Monaco ("monaco-editor", every
  // language and the TypeScript worker); it uses only the editor core's API
  // (create, themes, markers, a Monarch tokenizer), so it gets the core
  // Studio already loads.
  resolve: { alias: [{ find: /^monaco-editor$/, replacement: 'monaco-editor/editor' }] },
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
    sourcemap: false,
    // Monaco is about 3-4 MB; it loads only when an editor opens.
    chunkSizeWarningLimit: 5000,
    // No data: URIs for scripts or workers; small images may inline.
    assetsInlineLimit: 0,
    modulePreload: { polyfill: false },
  },
  worker: { format: 'es' },
  // `pnpm dev` or `vite preview` against a gateway: STUDIO_API=http://127.0.0.1:25080
  server: { proxy: apiProxy() },
  preview: { proxy: apiProxy() },
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.{ts,tsx}', 'scripts/**/*.test.mjs'],
    setupFiles: ['src/test/setup.ts'],
    css: false,
  },
});
