// Connector check for Prisma ORM 6 (../README.md): its schema engine
// (`prisma db execute`, `prisma db pull`) and its query engine, both Rust
// MySQL clients of their own (mysql_async through quaint).
//
// Works in a database of its own, sprig_conn_prisma_<tls>, so that the
// introspection sees only this check's tables:
//
//   db_pull      CREATE the tables with `prisma db execute`, then
//                `prisma db pull`; the models, types and relation must come back
//   generate     `prisma generate` for the introspected schema
//   connect      a PrismaClient: TLS state as the run asks, server version
//   query        nested create, include, where, orderBy, update, aggregate,
//                count, $queryRaw with parameters, cascade delete
//   transaction  an interactive transaction rolled back, a batch committed
//
// Prints one JSON line per check on stdout.
'use strict';

const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const CONNECTOR = 'prisma';
const env = process.env;
const TLS = env.SPRIG_TLS;
const PERSONA = env.SPRIG_PERSONA || 'mariadb';
const SKIPPED = new Set((env.SPRIG_CONN_SKIP_CHECKS || '').split(/[ ,]+/).filter(Boolean));
const DB = `sprig_conn_prisma_${TLS}`;
const AUTHOR = 'author';
const BOOK = 'book';
const SCHEMA = '/app/prisma/schema.prisma';
const PRISMA = '/app/node_modules/.bin/prisma';

function emit(check, status, detail = '', ms = 0) {
  console.log(JSON.stringify({
    connector: CONNECTOR, tls: TLS, check, status, detail: String(detail).slice(0, 300), ms: Math.round(ms * 10) / 10,
  }));
}

function expect(cond, msg) {
  if (!cond) throw new Error(msg);
}

// Prisma's MySQL URL: TLS is used when an ssl* parameter asks for it.
function url(db) {
  const auth = `${encodeURIComponent(env.SPRIG_USER)}:${encodeURIComponent(env.SPRIG_PASSWORD)}`;
  const tls = TLS === 'required' ? 'sslaccept=accept_invalid_certs&sslmode=require' : 'sslmode=disable';
  return `mysql://${auth}@${env.SPRIG_HOST}:${env.SPRIG_PORT}/${db}?${tls}&connection_limit=3`;
}

function prisma(args, input, dbUrl = url(DB)) {
  try {
    return execFileSync(PRISMA, args, {
      input, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], timeout: 120000,
      env: { ...env, DATABASE_URL: dbUrl, CHECKPOINT_DISABLE: '1', PRISMA_HIDE_UPDATE_MESSAGE: '1' },
    });
  } catch (e) {
    const out = `${e.stderr || ''}${e.stdout || ''}`.replace(/\s+/g, ' ').replace(/Environment variables loaded[^.]*\./, '');
    throw new Error(`prisma ${args[0]} ${args[1] || ''}: ${out.slice(-240)}`);
  }
}

const DDL = `
CREATE TABLE ${AUTHOR} (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_author_name (name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE ${BOOK} (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  title VARCHAR(128) NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  published DATETIME(6) NULL,
  status ENUM('draft', 'published') NOT NULL DEFAULT 'draft',
  cover BLOB NULL,
  attrs JSON NULL,
  author_id BIGINT UNSIGNED NOT NULL,
  KEY ix_book_title (title),
  CONSTRAINT fk_book_author FOREIGN KEY (author_id) REFERENCES ${AUTHOR} (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
`;

let client = null;
let pulled = null;

function cDbPull() {
  const base = url(env.SPRIG_DATABASE);
  prisma(['db', 'execute', '--stdin', '--url', base], `DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB};`, base);
  prisma(['db', 'execute', '--stdin', '--url', url(DB)], DDL);
  fs.mkdirSync(path.dirname(SCHEMA), { recursive: true });
  fs.writeFileSync(SCHEMA, 'datasource db {\n  provider = "mysql"\n  url      = env("DATABASE_URL")\n}\n\n'
    + 'generator client {\n  provider = "prisma-client-js"\n}\n');
  prisma(['db', 'pull', '--schema', SCHEMA]);
  pulled = fs.readFileSync(SCHEMA, 'utf8');
  const field = (model, name) => {
    const m = pulled.match(new RegExp(`model ${model} \\{([\\s\\S]*?)\\n\\}`));
    const line = m && m[1].split('\n').find((l) => l.trim().startsWith(`${name} `));
    return line ? line.trim().replace(/\s+/g, ' ') : null;
  };
  const want = {
    'author.id': /^id BigInt @id @default\(autoincrement\(\)\) @db\.UnsignedBigInt$/,
    'author.name': /^name String @unique\(map: "uq_author_name"\) @db\.VarChar\(64\)$/,
    'book.price': /^price Decimal @db\.Decimal\(10, 2\)$/,
    'book.published': /^published DateTime\? @db\.DateTime\(6\)$/,
    'book.status': /^status book_status @default\(draft\)$/,
    'book.cover': /^cover Bytes\? @db\.Blob$/,
    // MySQL has a JSON type; MariaDB's JSON is LONGTEXT with a json_valid() CHECK.
    'book.attrs': PERSONA === 'mysql' ? /^attrs Json\?$/ : /^attrs (Json\?|String\? @db\.LongText)$/,
    'book.author': /^author author @relation\(fields: \[author_id\], references: \[id\], onDelete: Cascade, onUpdate: Restrict, map: "fk_book_author"\)$/,
  };
  const bad = Object.entries(want).filter(([k, re]) => !re.test(field(...k.split('.')) || ''))
    .map(([k]) => `${k}: ${field(...k.split('.'))}`);
  expect(bad.length === 0, `introspected ${bad.join('; ')}`);
  expect(/enum book_status \{\s*draft\s*published\s*\}/.test(pulled), 'enum book_status missing');
  expect(/@@index\(\[title\], map: "ix_book_title"\)/.test(pulled), 'index ix_book_title missing');
  return `2 models, enum, relation onDelete: Cascade; attrs as ${field('book', 'attrs').replace('attrs ', '')}`;
}

function cGenerate() {
  const out = prisma(['generate', '--schema', SCHEMA]);
  const v = (out.match(/Prisma Client \(v?([\d.]+)\)/) || [])[1];
  return `Prisma Client ${v || '?'} generated`;
}

async function cConnect() {
  // Loaded only now: the client is what `generate` just wrote.
  const { PrismaClient } = require('@prisma/client');
  client = new PrismaClient({ datasources: { db: { url: url(DB) } } });
  await client.$connect();
  const [st] = await client.$queryRawUnsafe("SHOW SESSION STATUS LIKE 'Ssl_cipher'");
  const [ver] = await client.$queryRawUnsafe('SELECT VERSION() AS v');
  const cipher = st.Value;
  if (TLS === 'required') expect(cipher, "TLS required but the session isn't encrypted");
  else expect(!cipher, `TLS disabled but the session is encrypted (${cipher})`);
  return `server ${ver.v}, cipher=${cipher || 'none'}`;
}

async function cQuery() {
  const a = client[AUTHOR];
  const b = client[BOOK];
  await b.deleteMany({});
  await a.deleteMany({});
  const created = await a.create({
    data: {
      name: 'Ünïcödé 🌱',
      book: {
        create: [
          { title: 'one', price: '9.99', status: 'published', cover: Buffer.from([0, 255, 128, 10]),
            published: new Date(Date.UTC(2024, 1, 29, 23, 59, 58, 123)), attrs: /attrs Json/.test(pulled) ? { a: 1, s: '🌱' } : '{"a": 1, "s": "🌱"}' },
          { title: 'two', price: '0.01' },
        ],
      },
    },
    include: { book: { orderBy: { id: 'asc' } } },
  });
  expect(typeof created.id === 'bigint' && created.book.length === 2, `created ${JSON.stringify(created, (k, v) => (typeof v === 'bigint' ? String(v) : v))}`);
  const one = await b.findFirst({ where: { title: 'one', author: { name: { startsWith: 'Ünï' } } }, include: { author: true } });
  expect(one && one.price.toString() === '9.99' && one.status === 'published' && one.author.name === 'Ünïcödé 🌱'
    && Buffer.compare(Buffer.from(one.cover), Buffer.from([0, 255, 128, 10])) === 0
    && one.published.toISOString() === '2024-02-29T23:59:58.123Z', `read back ${JSON.stringify(one, (k, v) => (typeof v === 'bigint' ? String(v) : v))}`);
  await b.update({ where: { id: one.id }, data: { price: '10.50', status: 'draft' } });
  const agg = await b.aggregate({ _sum: { price: true }, _count: true, where: { author_id: created.id } });
  expect(agg._count === 2 && agg._sum.price.toString() === '10.51', `aggregate ${JSON.stringify(agg)}`);
  const raw = await client.$queryRaw`SELECT COUNT(*) AS n FROM book WHERE price > ${1} AND title <> ${'x'}`;
  expect(Number(raw[0].n) === 1, `$queryRaw ${JSON.stringify(raw, (k, v) => (typeof v === 'bigint' ? String(v) : v))}`);
  await a.delete({ where: { id: created.id } });
  expect((await b.count()) === 0, 'ON DELETE CASCADE left books');
  return 'nested create, include, relation filter, update, aggregate, $queryRaw, cascade delete';
}

async function cTransaction() {
  const a = client[AUTHOR];
  await a.deleteMany({});
  try {
    await client.$transaction(async (tx) => {
      await tx[AUTHOR].create({ data: { name: 'rolled back' } });
      throw new Error('roll me back');
    });
    throw new Error('no exception');
  } catch (e) {
    expect(e.message === 'roll me back', `interactive transaction: ${e.message}`);
  }
  expect((await a.count()) === 0, 'row survived rollback');
  await client.$transaction([a.create({ data: { name: 'one' } }), a.create({ data: { name: 'two' } })]);
  expect((await a.count()) === 2, 'batch transaction not committed');
  await a.deleteMany({});
  return 'interactive transaction rolled back, batch transaction committed';
}

const CHECKS = [['db_pull', cDbPull], ['generate', cGenerate], ['connect', cConnect], ['query', cQuery],
  ['transaction', cTransaction]];

(async () => {
  let ok = true;
  for (const [name, fn] of CHECKS) {
    if (SKIPPED.has(name)) continue;
    if (!ok) {
      emit(name, 'fail', 'an earlier step failed');
      continue;
    }
    const t0 = performance.now();
    try {
      emit(name, 'pass', (await fn()) || '', performance.now() - t0);
    } catch (e) {
      emit(name, 'fail', `${e.code || e.name}: ${e.message}`.replace(/\s+/g, ' '), performance.now() - t0);
      if (name !== 'query' && name !== 'transaction') ok = false;
    }
  }
  if (client) await client.$disconnect().catch(() => {});
  try {
    prisma(['db', 'execute', '--stdin', '--url', url(env.SPRIG_DATABASE)], `DROP DATABASE IF EXISTS ${DB};`, url(env.SPRIG_DATABASE));
  } catch (e) {
    // best effort
  }
})();
