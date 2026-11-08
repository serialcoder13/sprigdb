// Connector check for Node mysql2.
//
// Runs the fixed script described in ../README.md and prints one JSON line
// per check on stdout. Configuration comes from SPRIG_* environment
// variables (see ../run.sh).
'use strict';

const crypto = require('node:crypto');
const util = require('node:util');
const mysql = require('mysql2/promise');

const CONNECTOR = 'mysql2';
const env = process.env;
const TLS = env.SPRIG_TLS;
const PREFIX = `t_mysql2_${TLS}`;

class NA extends Error {}
class Skip extends Error {}

function emit(check, status, detail = '', ms = 0) {
  console.log(JSON.stringify({
    connector: CONNECTOR, tls: TLS, check, status,
    detail: String(detail).slice(0, 300), ms: Math.round(ms * 10) / 10,
  }));
}

function expect(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function connect(extra = {}) {
  const c = await mysql.createConnection({
    host: env.SPRIG_HOST,
    port: Number(env.SPRIG_PORT),
    user: env.SPRIG_USER,
    password: env.SPRIG_PASSWORD,
    database: env.SPRIG_DATABASE,
    // Encrypt without verifying the self-signed certificate.
    ssl: TLS === 'required' ? { rejectUnauthorized: false } : undefined,
    charset: 'UTF8MB4_GENERAL_CI',
    multipleStatements: true,
    supportBigNumbers: true,
    bigNumberStrings: true,
    dateStrings: true, // JavaScript Dates stop at milliseconds; DATETIME(6) doesn't
    timezone: 'Z',
    ...extra,
  });
  await c.query("SET time_zone = '+00:00'");
  return c;
}

// --- the shared data set ---------------------------------------------------

const COLS = ['c_tiny', 'c_small', 'c_int', 'c_big', 'c_ubig', 'c_dec', 'c_float', 'c_double',
  'c_vchar', 'c_text', 'c_vbin', 'c_blob', 'c_date', 'c_dt', 'c_ts', 'c_time', 'c_json'];
const DDL = (t) => `CREATE TABLE ${t} (
  id INT PRIMARY KEY,
  c_tiny TINYINT, c_small SMALLINT, c_int INT, c_big BIGINT, c_ubig BIGINT UNSIGNED,
  c_dec DECIMAL(20,6), c_float FLOAT, c_double DOUBLE,
  c_vchar VARCHAR(64), c_text TEXT, c_vbin VARBINARY(16), c_blob BLOB,
  c_date DATE, c_dt DATETIME(6), c_ts TIMESTAMP NULL, c_time TIME, c_json JSON
) DEFAULT CHARSET = utf8mb4`;
const BLOB = Buffer.from([...Array(256).keys()]);
const VBIN = Buffer.from([0x00, 0xFF, 0x80, 0x7F, 0x0A, 0x0D, 0x27, 0x22, 0x5C]);
const VCHAR = 'héllo wörld 🌱';
const TEXT = 'Ünïcödé 日本語 🚀🌱 \'quoted\' "double" back\\slash';
const JSON_TEXT = '{"a": 1, "b": [true, null], "s": "🌱"}';
const EXPECTED = {
  c_tiny: '-128', c_small: '32767', c_int: '-2147483648',
  c_big: '9223372036854775807', c_ubig: '18446744073709551615',
  c_dec: '-12345678901234.567890', c_float: '1.5', c_double: '-0.00225',
  c_vchar: VCHAR, c_text: TEXT, c_vbin: VBIN.toString('hex'), c_blob: BLOB.toString('hex'),
  c_date: '2024-02-29', c_dt: '2024-02-29 23:59:58.123456',
  c_ts: '2024-02-29 12:34:56', c_time: '838:59:59', c_json: JSON_TEXT,
};

function canon(col, v) {
  if (v === null || v === undefined) return null;
  if (Buffer.isBuffer(v)) return (col === 'c_vbin' || col === 'c_blob') ? v.toString('hex') : v.toString('utf8');
  if (v instanceof Date) return v.toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
  if (typeof v === 'object') return JSON.stringify(v); // a JSON column mysql2 parsed
  return String(v);
}

function compareRow(row, nulls) {
  const bad = [];
  for (const col of COLS) {
    const got = canon(col, row[col]);
    const want = nulls ? null : EXPECTED[col];
    let ok = got === want;
    if (!ok && got !== null && want !== null) {
      if (col === 'c_json') ok = util.isDeepStrictEqual(JSON.parse(got), JSON.parse(want));
      if (col === 'c_float' || col === 'c_double') ok = Number(got) === Number(want);
    }
    if (!ok) bad.push(`${col}: got ${JSON.stringify(got)} want ${JSON.stringify(want)}`);
  }
  expect(bad.length === 0, bad.join('; '));
}

const esc = (s) => s.replace(/\\/g, '\\\\').replace(/'/g, "\\'");

// --- checks ------------------------------------------------------------------

let conn = null;

async function tlsState(c) {
  const [[row]] = await c.query("SHOW SESSION STATUS LIKE 'Ssl_cipher'");
  const cipher = row.Value;
  if (TLS === 'required') expect(cipher, "TLS required but the session isn't encrypted");
  else expect(!cipher, `TLS disabled but the session is encrypted (${cipher})`);
  return `cipher=${cipher || 'none'}`;
}

async function cConnect() {
  conn = await connect();
  return tlsState(conn);
}

async function cPlainQuery() {
  const [rows] = await conn.query("SELECT 1 + 1 AS n, 'sprig' AS s, VERSION() AS v");
  expect(Number(rows[0].n) === 2 && rows[0].s === 'sprig', `got ${JSON.stringify(rows)}`);
  return `server ${rows[0].v}`;
}

async function cTypes() {
  const t = `${PREFIX}_types`;
  await conn.query(`DROP TABLE IF EXISTS ${t}`);
  await conn.query(DDL(t));
  await conn.query(`INSERT INTO ${t} VALUES (1, -128, 32767, -2147483648, 9223372036854775807, `
    + `18446744073709551615, -12345678901234.567890, 1.5, -0.00225, '${esc(VCHAR)}', '${esc(TEXT)}', `
    + `X'${VBIN.toString('hex')}', X'${BLOB.toString('hex')}', '2024-02-29', '2024-02-29 23:59:58.123456', `
    + `'2024-02-29 12:34:56', '838:59:59', '${esc(JSON_TEXT)}'), (2${', NULL'.repeat(17)})`);
  // No parameters: the text protocol (COM_QUERY).
  const [rows] = await conn.query(`SELECT ${COLS.join(', ')} FROM ${t} ORDER BY id`);
  expect(rows.length === 2, `want 2 rows, got ${rows.length}`);
  compareRow(rows[0], false);
  compareRow(rows[1], true);
  await conn.query(`DROP TABLE ${t}`);
  return '17 columns, 2 rows';
}

async function cPrepared() {
  const t = `${PREFIX}_ps`;
  await conn.query(`DROP TABLE IF EXISTS ${t}`);
  await conn.query(DDL(t));
  // execute() is COM_STMT_PREPARE + COM_STMT_EXECUTE; mysql2 caches the
  // statement, so the second call re-executes it.
  const ins = `INSERT INTO ${t} VALUES (?${', ?'.repeat(COLS.length)})`;
  await conn.execute(ins, [10, -128, 32767, -2147483648, 9223372036854775807n, 18446744073709551615n,
    '-12345678901234.567890', 1.5, -0.00225, VCHAR, TEXT, VBIN, BLOB,
    new Date(Date.UTC(2024, 1, 29)), '2024-02-29 23:59:58.123456', new Date(Date.UTC(2024, 1, 29, 12, 34, 56)),
    '838:59:59', JSON_TEXT]);
  await conn.execute(ins, [11, ...COLS.map(() => null)]);
  const sel = `SELECT ${COLS.join(', ')} FROM ${t} WHERE id = ?`;
  for (const id of [10, 11]) {
    const [rows] = await conn.execute(sel, [id]);
    expect(rows.length === 1, `id ${id}: ${rows.length} rows`);
    try {
      compareRow(rows[0], id === 11);
    } catch (e) {
      throw new Error(`id ${id}: ${e.message}`);
    }
  }
  await conn.query(`DROP TABLE ${t}`);
  return 'server-side, 2 executions each';
}

async function cTransaction() {
  const t = `${PREFIX}_tx`;
  await conn.query(`DROP TABLE IF EXISTS ${t}`);
  await conn.query(`CREATE TABLE ${t} (id INT PRIMARY KEY) ENGINE = InnoDB`);
  await conn.beginTransaction();
  await conn.query(`INSERT INTO ${t} VALUES (1)`);
  await conn.rollback();
  const [[r1]] = await conn.query(`SELECT COUNT(*) AS n FROM ${t}`);
  expect(Number(r1.n) === 0, 'row survived rollback');
  await conn.beginTransaction();
  await conn.query(`INSERT INTO ${t} VALUES (2)`);
  await conn.commit();
  const other = await connect();
  const [[r2]] = await other.query(`SELECT COUNT(*) AS n FROM ${t} WHERE id = 2`);
  await other.end();
  expect(Number(r2.n) === 1, 'committed row not visible to another session');
  await conn.query(`DROP TABLE ${t}`);
}

async function cMultiStatement() {
  const [results] = await conn.query("SELECT 1 AS a; SELECT 2 AS b, 'two' AS c");
  expect(results.length === 2, `want 2 results, got ${results.length}`);
  expect(Number(results[0][0].a) === 1 && Number(results[1][0].b) === 2 && results[1][0].c === 'two',
    `got ${JSON.stringify(results)}`);
  return '2 resultsets';
}

async function cDuplicateKey() {
  const t = `${PREFIX}_dup`;
  await conn.query(`DROP TABLE IF EXISTS ${t}`);
  await conn.query(`CREATE TABLE ${t} (id INT PRIMARY KEY)`);
  await conn.query(`INSERT INTO ${t} VALUES (1)`);
  try {
    await conn.query(`INSERT INTO ${t} VALUES (1)`);
  } catch (e) {
    expect(e.errno === 1062 && e.sqlState === '23000', `got ${e.errno}/${e.sqlState}: ${e.message}`);
    await conn.query(`DROP TABLE ${t}`);
    return '1062/23000';
  }
  throw new Error('no error raised');
}

async function cLastInsertId() {
  const t = `${PREFIX}_ai`;
  await conn.query(`DROP TABLE IF EXISTS ${t}`);
  await conn.query(`CREATE TABLE ${t} (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT) AUTO_INCREMENT = 100`);
  const [r1] = await conn.query(`INSERT INTO ${t} (v) VALUES (1)`);
  const [r2] = await conn.query(`INSERT INTO ${t} (v) VALUES (2), (3), (4)`);
  const [[lid]] = await conn.query('SELECT LAST_INSERT_ID() AS id');
  expect(Number(r1.insertId) === 100, `first insert id ${r1.insertId}`);
  expect(Number(r2.insertId) === 101 && Number(lid.id) === 101,
    `multi-row insert id ${r2.insertId}, LAST_INSERT_ID() ${lid.id}`);
  await conn.query(`DROP TABLE ${t}`);
}

async function roundTrip(c, size, label) {
  const t = `${PREFIX}_${label}`;
  await c.query(`DROP TABLE IF EXISTS ${t}`);
  await c.query(`CREATE TABLE ${t} (id INT PRIMARY KEY, b LONGBLOB)`);
  const data = crypto.randomBytes(size);
  await c.execute(`INSERT INTO ${t} VALUES (1, ?)`, [data]);
  const [[row]] = await c.query(`SELECT b, SHA2(b, 256) AS sha FROM ${t} WHERE id = 1`);
  expect(row.sha === crypto.createHash('sha256').update(data).digest('hex'),
    'server-side hash differs: the upload was corrupted');
  expect(Buffer.compare(row.b, data) === 0, `download differs (${row.b.length} bytes, want ${size})`);
  await c.query(`DROP TABLE ${t}`);
  return `${size} bytes`;
}

async function cLargeRow() {
  return roundTrip(conn, 4 * 1024 * 1024, 'big');
}

async function cMultiPacket() {
  const [[row]] = await conn.query('SELECT @@max_allowed_packet AS m');
  const size = 16 * 1024 * 1024 + 1024;
  expect(Number(row.m) >= 2 * size + 1024, `server max_allowed_packet ${row.m} is too small for ${size} bytes`);
  return roundTrip(conn, size, 'mp');
}

async function cCompression() {
  const c = await connect({ compress: true });
  try {
    const [[st]] = await c.query("SHOW SESSION STATUS LIKE 'Compression'");
    expect(st.Value === 'ON', `Compression status ${st.Value}`);
    const [[r]] = await c.query("SELECT LENGTH(REPEAT('a', 1000000)) AS n");
    expect(Number(r.n) === 1000000, `got ${r.n}`);
    return `Compression=ON, ${await roundTrip(c, 1024 * 1024, 'zip')}`;
  } finally {
    await c.end();
  }
}

async function cConnAttrs() {
  const c = await connect({ connectAttributes: { sprig_check: 'mysql2' } });
  try {
    const [[ps]] = await c.query('SELECT @@performance_schema AS p');
    if (Number(ps.p) !== 1) throw new Skip('performance_schema is off on the server');
    const [rows] = await c.query('SELECT ATTR_VALUE AS v FROM performance_schema.session_connect_attrs '
      + "WHERE PROCESSLIST_ID = CONNECTION_ID() AND ATTR_NAME = 'sprig_check'");
    expect(rows.length === 1 && rows[0].v === 'mysql2', `sprig_check attribute: ${JSON.stringify(rows)}`);
    return 'sprig_check=mysql2';
  } finally {
    await c.end();
  }
}

const CHECKS = [
  ['connect', cConnect], ['plain_query', cPlainQuery], ['types', cTypes],
  ['prepared', cPrepared], ['transaction', cTransaction],
  ['multi_statement', cMultiStatement], ['duplicate_key', cDuplicateKey],
  ['last_insert_id', cLastInsertId], ['large_row', cLargeRow],
  ['multi_packet', cMultiPacket], ['compression', cCompression],
  ['conn_attrs', cConnAttrs],
];

(async () => {
  for (const [name, fn] of CHECKS) {
    if (!conn && name !== 'connect') {
      emit(name, 'fail', 'no connection');
      continue;
    }
    const t0 = performance.now();
    try {
      const detail = await fn();
      emit(name, 'pass', detail || '', performance.now() - t0);
    } catch (e) {
      if (e instanceof NA) emit(name, 'n/a', e.message);
      else if (e instanceof Skip) emit(name, 'skip', e.message);
      else emit(name, 'fail', `${e.code || e.name}: ${e.message}`, performance.now() - t0);
    }
  }
  if (conn) await conn.end().catch(() => {});
})();
