// A plain (no TLS) AMQP 0-9-1 login for deploy/kind/smoke.sh: protocol
// header, Connection.Start, PLAIN login, Tune, Open on vhost "/".
// Node.js built-ins only.
//
//   node amqp-plain.mjs <host> <port> <tenant.user> <password>
import net from "node:net";

const [host, port, login, password] = process.argv.slice(2);
const s = net.connect({ host, port: Number(port) });
s.setTimeout(20000, () => s.destroy(new Error("timeout")));
let buf = Buffer.alloc(0);
let wait = null;
s.on("data", (d) => { buf = Buffer.concat([buf, d]); if (wait) wait(); });
async function read(n) {
  while (buf.length < n) {
    await new Promise((res, rej) => { wait = res; s.once("close", () => rej(new Error("connection closed"))); });
    wait = null;
  }
  const out = buf.subarray(0, n);
  buf = buf.subarray(n);
  return out;
}
function frame(cls, method, args) {
  const payload = Buffer.concat([Buffer.from([cls >> 8, cls & 0xff, method >> 8, method & 0xff]), args]);
  const head = Buffer.alloc(7); head[0] = 1; head.writeUInt32BE(payload.length, 3);
  return Buffer.concat([head, payload, Buffer.from([0xce])]);
}
async function next() {
  const head = await read(7);
  const payload = await read(head.readUInt32BE(3));
  await read(1);
  return { cls: payload.readUInt16BE(0), method: payload.readUInt16BE(2), args: payload.subarray(4) };
}
const shortstr = (x) => Buffer.concat([Buffer.from([Buffer.byteLength(x)]), Buffer.from(x)]);
const longstr = (b) => { const l = Buffer.alloc(4); l.writeUInt32BE(b.length); return Buffer.concat([l, b]); };
const text = (f) => f.args.subarray(3, 3 + f.args[2]).toString();
try {
  await new Promise((res, rej) => { s.once("connect", res); s.once("error", rej); });
  s.write(Buffer.from("AMQP\x00\x00\x09\x01", "latin1"));
  const start = await next();
  if (start.cls !== 10 || start.method !== 10) throw new Error(`expected Connection.Start, got ${start.cls}.${start.method}`);
  s.write(frame(10, 11, Buffer.concat([Buffer.alloc(4), shortstr("PLAIN"), longstr(Buffer.from(`\0${login}\0${password}`)), shortstr("en_US")])));
  const tune = await next();
  if (tune.cls !== 10 || tune.method !== 30) throw new Error(`login refused (${tune.cls}.${tune.method}): ${text(tune)}`);
  s.write(frame(10, 31, tune.args.subarray(0, 8)));
  s.write(frame(10, 40, Buffer.concat([shortstr("/"), shortstr(""), Buffer.from([0])])));
  const open = await next();
  if (open.cls !== 10 || open.method !== 41) throw new Error(`Connection.Open refused (${open.cls}.${open.method}): ${text(open)}`);
  s.end();
  console.log("Connection.Start, PLAIN login, Tune, Open-Ok");
} catch (e) {
  console.log(String(e.message || e));
  s.destroy();
  process.exit(1);
}
