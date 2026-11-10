#!/usr/bin/env node
// Licence and package allowlists (chapter 44, "Dependencies"; agent brief rule
// 14). Fails when:
//  - a run-time package's licence isn't MIT, BSD, ISC, Apache-2.0, Zlib,
//    0BSD or Unicode (an "A OR B" expression passes when one side does);
//  - a development-only package's licence isn't one of those, MPL-2.0, or a
//    reviewed exception below;
//  - any package is GPL, AGPL, LGPL, BSL, SSPL or "Commons Clause";
//  - any @fincity package other than @fincity/kirun-ui and @fincity/kirun-js,
//    or anything from Modlix's nocode-ui, is installed;
//  - a direct dependency isn't pinned to an exact version.
// Reads `pnpm licenses list --json`, so it sees every installed package,
// transitive ones included.
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');

export const RUNTIME_OK = ['MIT', 'ISC', 'BSD-2-Clause', 'BSD-3-Clause', '0BSD', 'Apache-2.0', 'Zlib', 'Unicode-3.0', 'Unicode-DFS-2016'];
export const DEV_OK = [...RUNTIME_OK, 'MIT-0', 'MPL-2.0', 'CC0-1.0'];
// Development-only packages under other permissive licences, reviewed one by
// one (decision ST3 in sprigdb-plan/decisions/studio-interim.md).
export const DEV_EXCEPTIONS = {
  argparse: 'Python-2.0', // js-yaml, under openapi-typescript's Redocly parser
  'caniuse-lite': 'CC-BY-4.0', // browser data under Babel (eslint-plugin-react-hooks)
  'lru-cache': 'BlueOak-1.0.0',
  minimatch: 'BlueOak-1.0.0',
};
const FORBIDDEN = /(^|[^A-Z])(A?GPL|LGPL|BSL|BUSL|SSPL|Commons[ -]Clause)/i;
const ALLOWED_FINCITY = new Set(['@fincity/kirun-ui', '@fincity/kirun-js']);

/** Whether an SPDX expression passes with `ok`. */
export function licenceOk(expr, ok) {
  const e = expr.replace(/[()]/g, '').trim();
  if (FORBIDDEN.test(e)) {
    // "MIT OR GPL-3.0" still passes on its permissive side.
    return e.split(/\s+OR\s+/).some((p) => !FORBIDDEN.test(p) && ok.includes(p.trim()));
  }
  if (/\s+OR\s+/.test(e)) return e.split(/\s+OR\s+/).some((p) => ok.includes(p.trim()));
  if (/\s+AND\s+/.test(e)) return e.split(/\s+AND\s+/).every((p) => ok.includes(p.trim()));
  return ok.includes(e);
}

/** Whether a package name is allowed at all. */
export function packageAllowed(name) {
  if (name.startsWith('@fincity/')) return ALLOWED_FINCITY.has(name);
  return !/nocode|modlix/i.test(name);
}

function list(prod) {
  const args = ['licenses', 'list', '--json'];
  if (prod) args.push('--prod');
  const out = execFileSync('pnpm', args, { cwd: root, encoding: 'utf8' });
  const byLicence = JSON.parse(out);
  const pkgs = [];
  for (const [licence, entries] of Object.entries(byLicence)) {
    for (const p of entries) pkgs.push({ name: p.name, versions: p.versions, licence });
  }
  return pkgs;
}

function main() {
  const problems = [];
  const prod = list(true);
  const prodNames = new Set(prod.map((p) => p.name));
  for (const p of prod) {
    if (!packageAllowed(p.name)) problems.push(`${p.name}: not an allowed package`);
    if (!licenceOk(p.licence, RUNTIME_OK)) problems.push(`${p.name}@${p.versions}: ${p.licence} is not allowed at run time`);
  }
  for (const p of list(false)) {
    if (prodNames.has(p.name)) continue;
    if (!packageAllowed(p.name)) problems.push(`${p.name}: not an allowed package`);
    if (licenceOk(p.licence, DEV_OK)) continue;
    if (DEV_EXCEPTIONS[p.name] === p.licence) continue;
    problems.push(`${p.name}@${p.versions}: ${p.licence} is not allowed (development only)`);
  }
  const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
  for (const sec of ['dependencies', 'devDependencies']) {
    for (const [name, v] of Object.entries(manifest[sec] ?? {})) {
      if (!/^\d+\.\d+\.\d+$/.test(v)) problems.push(`${name}: pin an exact version (has ${v})`);
    }
  }
  if (problems.length) {
    console.error(`Licence and package check failed:\n  ${problems.join('\n  ')}`);
    process.exit(1);
  }
  console.log(`Licences ok: ${prod.length} run-time packages, all packages checked.`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) main();
