#!/usr/bin/env node
// Pre-compresses Studio's built files (chapter 44, "Serving assets"): next to
// each text file it writes `<file>.br` (brotli, quality 11) and `<file>.gz`
// (gzip, level 9) when they are smaller, and writes `studio-manifest.json`
// with the build version. sprig-studio's build script embeds the result, so
// the gateway never compresses at run time. Node's zlib does both formats:
// no extra dependency.
import { readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';
import { brotliCompressSync, constants, gzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';

const dist = process.argv[2] ?? 'dist';
const TEXT = /\.(html|js|mjs|css|json|svg|txt|map|ttf|wasm)$/;

function walk(dir) {
  return readdirSync(dir).flatMap((n) => {
    const p = join(dir, n);
    return statSync(p).isDirectory() ? walk(p) : [p];
  });
}

const files = walk(dist).filter((f) => !f.endsWith('.br') && !f.endsWith('.gz') && !f.endsWith('studio-manifest.json'));
const hash = createHash('sha256');
let raw = 0;
let br = 0;
for (const f of files.sort()) {
  const data = readFileSync(f);
  hash.update(relative(dist, f)).update(data);
  raw += data.length;
  if (!TEXT.test(f) || data.length < 512) continue;
  const b = brotliCompressSync(data, {
    params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: data.length },
  });
  const g = gzipSync(data, { level: 9 });
  if (b.length < data.length) writeFileSync(`${f}.br`, b);
  if (g.length < data.length) writeFileSync(`${f}.gz`, g);
  br += Math.min(b.length, data.length);
}
const version = hash.digest('hex').slice(0, 16);
// The first screen's budget (chapter 44: about 400 KB): index.html and the
// scripts and styles it names, as brotli.
const html = readFileSync(join(dist, 'index.html'), 'utf8');
if (/<script(?![^>]*\bsrc=)[^>]*>/i.test(html)) {
  console.error('index.html has an inline script: the CSP (script-src self) would block it');
  process.exit(1);
}
const firstFiles = ['index.html', ...[...html.matchAll(/(?:src|href)="\/studio\/([^"]+\.(?:js|css))"/g)].map((m) => m[1])];
let first = 0;
for (const f of firstFiles) {
  const p = join(dist, f);
  try {
    first += readFileSync(`${p}.br`).length;
  } catch {
    first += readFileSync(p).length;
  }
}
const BUDGET = 400 * 1024;
console.log(`first screen: ${(first / 1024).toFixed(0)} KB brotli (budget ${BUDGET / 1024} KB)`);
if (first > BUDGET) {
  console.error('the first screen is over budget');
  process.exit(1);
}
writeFileSync(
  join(dist, 'studio-manifest.json'),
  JSON.stringify({ build_version: version, min_api: 'v1', files: files.length }, null, 2),
);
console.log(`compressed ${files.length} files: ${(raw / 1e6).toFixed(2)} MB raw, ${(br / 1e6).toFixed(2)} MB brotli; build ${version}`);
