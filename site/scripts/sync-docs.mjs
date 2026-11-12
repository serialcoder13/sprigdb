// Copies ../docs into src/content/docs/docs for the website. The Markdown in
// ../docs is the one source; this only adapts it for Starlight:
// - README.md becomes the docs index;
// - a leading "# Title" is dropped (Starlight renders the front-matter title);
// - links between .md files lose their extension and become site paths.
import { mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const src = join(here, '..', '..', 'docs');
const out = join(here, '..', 'src', 'content', 'docs', 'docs');

rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });

function walk(dir) {
  return readdirSync(dir).flatMap((n) => {
    const p = join(dir, n);
    return statSync(p).isDirectory() ? walk(p) : [p];
  });
}

let pages = 0;
for (const file of walk(src)) {
  const rel = relative(src, file);
  if (!rel.endsWith('.md')) continue;
  let text = readFileSync(file, 'utf8');
  text = text.replace(/^(---\n[\s\S]*?\n---\n)\s*# [^\n]*\n/, '$1\n');
  text = text.replace(/\]\((?!https?:|#)([^)]+?)\.md(#[^)]*)?\)/g, (_, p, h) => `](../${p.replace(/^\.\//, '')}/${h ?? ''})`);
  const target = rel === 'README.md' ? 'index.md' : rel;
  mkdirSync(dirname(join(out, target)), { recursive: true });
  writeFileSync(join(out, target), text);
  pages++;
}
console.log(`sync-docs: ${pages} pages from ${src}`);
