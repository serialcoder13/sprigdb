// Smoke test of the Compose-on-VM bundle's TLS listeners that have no
// command-line client on a typical host: REST, gRPC, Redis, Bolt, Kafka and
// AMQP. Node.js built-ins only (tls, https, http2). smoke.sh runs it after the
// MySQL, MariaDB and MongoDB checks.
//
//   node smoke.mjs <host> <ca.pem> <tenant.user> <password> <ports as JSON>
//
// Each check logs in as the tenant user over TLS (verifying the gateway's
// certificate against ca.pem) and does one real operation. Exit 1 if any fails.
import tls from "node:tls";
import https from "node:https";
import http2 from "node:http2";
import fs from "node:fs";

const [host, caFile, login, password, portsJson] = process.argv.slice(2);
const ca = fs.readFileSync(caFile);
const ports = JSON.parse(portsJson);
const basic = "Basic " + Buffer.from(`${login}:${password}`).toString("base64");
const results = [];
const servername = /^[0-9.]+$/.test(host) ? undefined : host;

function connectTls(port, alpn) {
  return new Promise((resolve, reject) => {
    const s = tls.connect({ host, port, ca, servername, ALPNProtocols: alpn }, () => {
      if (!s.authorized) reject(new Error("certificate not verified: " + s.authorizationError));
      else resolve(s);
    });
    s.once("error", reject);
    s.setTimeout(20000, () => s.destroy(new Error("timeout")));
  });
}

// A byte reader over a TLS socket: read(n) resolves with exactly n bytes.
function reader(sock) {
  let buf = Buffer.alloc(0);
  let wait = null;
  sock.on("data", (d) => { buf = Buffer.concat([buf, d]); if (wait) wait(); });
  return async function read(n) {
    while (buf.length < n) {
      await new Promise((res, rej) => { wait = res; sock.once("close", () => rej(new Error("connection closed"))); });
      wait = null;
    }
    const out = buf.subarray(0, n);
    buf = buf.subarray(n);
    return out;
  };
}

async function check(name, fn) {
  const t0 = Date.now();
  try {
    const detail = await fn();
    results.push({ name, ok: true, ms: Date.now() - t0, detail });
  } catch (e) {
    results.push({ name, ok: false, ms: Date.now() - t0, detail: String(e.message || e) });
  }
}

// --- REST ------------------------------------------------------------------
function rest() {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ sql: "SELECT 40 + 2 AS answer" });
    const req = https.request({ host, port: ports.https, ca, servername, method: "POST",
      path: "/v1/databases/-/sql:query",
      headers: { authorization: basic, "content-type": "application/json", "content-length": Buffer.byteLength(body) } },
      (res) => {
        let data = "";
        res.on("data", (d) => (data += d));
        res.on("end", () => res.statusCode === 200 && data.includes("42")
          ? resolve(`HTTP ${res.statusCode}, 42 returned`)
          : reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 200)}`)));
      });
    req.on("error", reject);
    req.end(body);
  });
}

// --- gRPC: sprig.v1.Sql/Query ---------------------------------------------
function pbString(field, s) {
  const b = Buffer.from(s);
  return Buffer.concat([Buffer.from([(field << 3) | 2, b.length]), b]);
}
function grpc() {
  return new Promise((resolve, reject) => {
    const client = http2.connect(`https://${host}:${ports.https}`, { ca, servername });
    client.on("error", reject);
    const msg = Buffer.concat([pbString(1, "-"), pbString(2, "SELECT 40 + 2 AS answer")]);
    const frame = Buffer.alloc(5 + msg.length);
    frame.writeUInt32BE(msg.length, 1);
    msg.copy(frame, 5);
    const req = client.request({ ":method": "POST", ":path": "/sprig.v1.Sql/Query",
      "content-type": "application/grpc", te: "trailers", authorization: basic });
    let data = Buffer.alloc(0);
    req.on("data", (d) => (data = Buffer.concat([data, d])));
    req.on("trailers", (t) => {
      client.close();
      const status = t["grpc-status"];
      if (status === "0" && data.includes(Buffer.from("42"))) resolve(`grpc-status 0, ${data.length} bytes, 42 returned`);
      else reject(new Error(`grpc-status ${status}: ${t["grpc-message"] || ""}`));
    });
    req.on("error", reject);
    req.end(frame);
  });
}

// --- Redis (RESP) -----------------------------------------------------------
function resp(...args) {
  return Buffer.from(`*${args.length}\r\n` + args.map((a) => `$${Buffer.byteLength(a)}\r\n${a}\r\n`).join(""));
}
async function redis() {
  const s = await connectTls(ports.redis);
  let data = "";
  s.on("data", (d) => (data += d));
  const until = async (n) => { for (let i = 0; i < 200 && data.split("\r\n").length <= n; i++) await new Promise((r) => setTimeout(r, 25)); };
  s.write(resp("AUTH", login, password)); await until(1);
  s.write(resp("SET", "smoke:key", "hello")); await until(2);
  s.write(resp("GET", "smoke:key")); await until(4);
  s.write(resp("DEL", "smoke:key")); await until(5);
  s.end();
  const lines = data.split("\r\n");
  if (lines[0] !== "+OK") throw new Error("AUTH: " + lines[0]);
  if (lines[1] !== "+OK") throw new Error("SET: " + lines[1]);
  if (lines[3] !== "hello") throw new Error("GET: " + lines.slice(2, 4).join(" "));
  return "AUTH, SET, GET, DEL ok";
}

// --- Bolt 5.x (PackStream) ---------------------------------------------------
function ps(v) {
  if (typeof v === "string") {
    const b = Buffer.from(v);
    return b.length < 16 ? Buffer.concat([Buffer.from([0x80 | b.length]), b]) : Buffer.concat([Buffer.from([0xd0, b.length]), b]);
  }
  if (typeof v === "number") return Buffer.from([v & 0xff]); // tiny ints only (-16..127)
  const keys = Object.keys(v);
  return Buffer.concat([Buffer.from([0xa0 | keys.length]), ...keys.flatMap((k) => [ps(k), ps(v[k])])]);
}
function boltMsg(tag, ...fields) {
  const body = Buffer.concat([Buffer.from([0xb0 | fields.length, tag]), ...fields.map(ps)]);
  const head = Buffer.alloc(2); head.writeUInt16BE(body.length);
  return Buffer.concat([head, body, Buffer.from([0, 0])]);
}
async function boltRead(read) {
  let msg = Buffer.alloc(0);
  for (;;) {
    const n = (await read(2)).readUInt16BE(0);
    if (n === 0) break;
    msg = Buffer.concat([msg, await read(n)]);
  }
  return msg; // msg[1] is the signature: 0x70 SUCCESS, 0x71 RECORD, 0x7f FAILURE
}
async function bolt() {
  const s = await connectTls(ports.bolt);
  const read = reader(s);
  s.write(Buffer.from([0x60, 0x60, 0xb0, 0x17, 0, 0, 4, 5, 0, 0, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0]));
  const v = await read(4);
  const version = `${v[3]}.${v[2]}`;
  const send = (m) => s.write(m);
  if (v[3] >= 5) {
    send(boltMsg(0x01, { user_agent: "sprig-smoke/1", bolt_agent: { product: "sprig-smoke/1" } }));
    let r = await boltRead(read);
    if (r[1] !== 0x70) throw new Error("HELLO failed: " + r.toString("latin1"));
    send(boltMsg(0x6a, { scheme: "basic", principal: login, credentials: password }));
    r = await boltRead(read);
    if (r[1] !== 0x70) throw new Error("LOGON failed: " + r.toString("latin1"));
  } else {
    send(boltMsg(0x01, { user_agent: "sprig-smoke/1", scheme: "basic", principal: login, credentials: password }));
    const r = await boltRead(read);
    if (r[1] !== 0x70) throw new Error("HELLO failed: " + r.toString("latin1"));
  }
  send(boltMsg(0x10, "RETURN 40 + 2 AS answer", {}, {}));
  const run = await boltRead(read);
  send(boltMsg(0x3f, { n: -1 }));
  const rec = await boltRead(read);
  s.end();
  if (run[1] !== 0x70) throw new Error(`Bolt ${version} RUN failed: ` + run.toString("latin1"));
  if (rec[1] !== 0x71 || rec[rec.length - 1] !== 42) throw new Error(`Bolt ${version} PULL: ` + rec.toString("latin1"));
  return `Bolt ${version}, logged in, RETURN 40 + 2 = 42`;
}

// --- Kafka ---------------------------------------------------------------------
async function kafka() {
  const s = await connectTls(ports.kafka);
  const read = reader(s);
  let corr = 0;
  const call = async (key, version, body) => {
    corr += 1;
    const client = Buffer.from("sprig-smoke");
    const head = Buffer.alloc(10); head.writeInt16BE(key, 0); head.writeInt16BE(version, 2); head.writeInt32BE(corr, 4); head.writeInt16BE(client.length, 8);
    const msg = Buffer.concat([head, client, body]);
    const size = Buffer.alloc(4); size.writeInt32BE(msg.length);
    s.write(Buffer.concat([size, msg]));
    const n = (await read(4)).readInt32BE(0);
    const res = await read(n);
    if (res.readInt32BE(0) !== corr) throw new Error("correlation id mismatch");
    return res.subarray(4);
  };
  const str = (x) => { const b = Buffer.from(x); const l = Buffer.alloc(2); l.writeInt16BE(b.length); return Buffer.concat([l, b]); };
  const av = await call(18, 0, Buffer.alloc(0));
  if (av.readInt16BE(0) !== 0) throw new Error("ApiVersions error " + av.readInt16BE(0));
  const apis = av.readInt32BE(2);
  const hs = await call(17, 1, str("PLAIN"));
  if (hs.readInt16BE(0) !== 0) throw new Error("SaslHandshake error " + hs.readInt16BE(0));
  const tok = Buffer.from(`\0${login}\0${password}`);
  const tl = Buffer.alloc(4); tl.writeInt32BE(tok.length);
  const au = await call(36, 1, Buffer.concat([tl, tok]));
  if (au.readInt16BE(0) !== 0) throw new Error("SaslAuthenticate error " + au.readInt16BE(0));
  const md = await call(3, 1, Buffer.from([0xff, 0xff, 0xff, 0xff]));
  const brokers = md.readInt32BE(0);
  let off = 4; const list = [];
  for (let i = 0; i < brokers; i++) {
    const id = md.readInt32BE(off); off += 4;
    const hl = md.readInt16BE(off); off += 2;
    const h = md.subarray(off, off + hl).toString(); off += hl;
    const p = md.readInt32BE(off); off += 4;
    const rl = md.readInt16BE(off); off += 2 + Math.max(rl, 0);
    list.push(`${id}@${h}:${p}`);
  }
  s.end();
  if (!list.some((b) => b.endsWith(`:${ports.kafka}`))) throw new Error("advertised brokers " + list.join(","));
  return `ApiVersions (${apis} APIs), SASL PLAIN ok, Metadata brokers ${list.join(",")}`;
}

// --- AMQP 0-9-1 -------------------------------------------------------------------
function amqpFrame(cls, method, args) {
  const payload = Buffer.concat([Buffer.from([cls >> 8, cls & 0xff, method >> 8, method & 0xff]), args]);
  const head = Buffer.alloc(7); head[0] = 1; head.writeUInt16BE(0, 1); head.writeUInt32BE(payload.length, 3);
  return Buffer.concat([head, payload, Buffer.from([0xce])]);
}
const shortstr = (x) => Buffer.concat([Buffer.from([Buffer.byteLength(x)]), Buffer.from(x)]);
const longstr = (b) => { const l = Buffer.alloc(4); l.writeUInt32BE(b.length); return Buffer.concat([l, b]); };
async function amqpRead(read) {
  const head = await read(7);
  const size = head.readUInt32BE(3);
  const payload = await read(size);
  await read(1);
  return { type: head[0], cls: payload.readUInt16BE(0), method: payload.readUInt16BE(2), args: payload.subarray(4) };
}
async function amqp() {
  const s = await connectTls(ports.amqps);
  const read = reader(s);
  s.write(Buffer.from("AMQP\x00\x00\x09\x01", "latin1"));
  const start = await amqpRead(read);
  if (start.cls !== 10 || start.method !== 10) throw new Error(`expected Connection.Start, got ${start.cls}.${start.method}`);
  s.write(amqpFrame(10, 11, Buffer.concat([Buffer.alloc(4), shortstr("PLAIN"), longstr(Buffer.from(`\0${login}\0${password}`)), shortstr("en_US")])));
  const tune = await amqpRead(read);
  if (tune.cls !== 10 || tune.method !== 30) throw new Error(`login refused (${tune.cls}.${tune.method}): ${tune.args.subarray(3, 3 + tune.args[2]).toString()}`);
  s.write(amqpFrame(10, 31, tune.args.subarray(0, 8)));
  s.write(amqpFrame(10, 40, Buffer.concat([shortstr("/"), shortstr(""), Buffer.from([0])])));
  const open = await amqpRead(read);
  s.end();
  if (open.cls !== 10 || open.method !== 41) throw new Error(`Connection.Open refused (${open.cls}.${open.method}): ${open.args.subarray(3, 3 + open.args[2]).toString()}`);
  return "Connection.Start, PLAIN login, Tune, Open-Ok";
}

await check("REST (HTTPS)", rest);
await check("gRPC (h2 over TLS)", grpc);
await check("Redis (TLS)", redis);
await check("Bolt (TLS)", bolt);
await check("Kafka (TLS, SASL PLAIN)", kafka);
await check("AMQP 0-9-1 (TLS)", amqp);
for (const r of results) console.log(`${r.ok ? "PASS" : "FAIL"}  ${r.name.padEnd(26)} ${String(r.ms).padStart(5)} ms  ${r.detail}`);
process.exit(results.every((r) => r.ok) ? 0 : 1);
