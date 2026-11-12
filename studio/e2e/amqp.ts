// A small AMQP 0-9-1 client for the end-to-end suite's setup (Studio round
// 6): queues and exchanges can only be declared over AMQP (chapter 50 has no
// SQL topology statements yet, decision ST45), and the queue pages need a
// live consumer to show one. It speaks just enough of the protocol: PLAIN
// login, one channel, exchange and queue declare, bind, qos, consume (no
// acks: deliveries stay unacknowledged until the connection closes) and
// close. Host and port are always given (rule 15); nothing here defaults
// to 5672.

import { connect, type Socket } from 'node:net';

const FRAME_END = 0xce;

class Writer {
  private parts: Buffer[] = [];
  u8(n: number) {
    this.parts.push(Buffer.from([n]));
    return this;
  }
  u16(n: number) {
    const b = Buffer.alloc(2);
    b.writeUInt16BE(n);
    this.parts.push(b);
    return this;
  }
  u32(n: number) {
    const b = Buffer.alloc(4);
    b.writeUInt32BE(n);
    this.parts.push(b);
    return this;
  }
  short(s: string) {
    const b = Buffer.from(s, 'utf8');
    if (b.length > 255) throw new Error('shortstr too long');
    return this.u8(b.length).raw(b);
  }
  long(b: Buffer) {
    return this.u32(b.length).raw(b);
  }
  /** A field table of string values ('S'). */
  table(t: Record<string, string> = {}) {
    const w = new Writer();
    for (const [k, v] of Object.entries(t)) w.short(k).u8(0x53).long(Buffer.from(v, 'utf8'));
    return this.long(w.done());
  }
  raw(b: Buffer) {
    this.parts.push(b);
    return this;
  }
  done(): Buffer {
    return Buffer.concat(this.parts);
  }
}

function method(cls: number, m: number, args: (w: Writer) => Writer = (w) => w): Buffer {
  return args(new Writer().u16(cls).u16(m)).done();
}

interface Frame {
  type: number;
  channel: number;
  payload: Buffer;
}

export class Amqp {
  private sock: Socket;
  private buf = Buffer.alloc(0);
  private waiters: { cls: number; m: number; ok: (p: Buffer) => void; fail: (e: Error) => void }[] = [];
  private failed: Error | null = null;
  /** Deliveries seen (body text), for consumers. */
  readonly delivered: string[] = [];

  private constructor(sock: Socket) {
    this.sock = sock;
    sock.on('data', (d: Buffer) => this.onData(d));
    sock.on('error', (e) => this.fail(e));
    sock.on('close', () => this.fail(new Error('AMQP connection closed')));
  }

  private fail(e: Error) {
    this.failed ??= e;
    for (const w of this.waiters.splice(0)) w.fail(e);
  }

  private onData(d: Buffer) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.buf.length < 7) return;
      const size = this.buf.readUInt32BE(3);
      if (this.buf.length < 8 + size) return;
      const f: Frame = { type: this.buf[0]!, channel: this.buf.readUInt16BE(1), payload: this.buf.subarray(7, 7 + size) };
      this.buf = this.buf.subarray(8 + size);
      this.onFrame(f);
    }
  }

  private onFrame(f: Frame) {
    if (f.type === 3) {
      this.delivered.push(f.payload.toString('utf8'));
      return;
    }
    if (f.type !== 1) return;
    const cls = f.payload.readUInt16BE(0);
    const m = f.payload.readUInt16BE(2);
    const args = f.payload.subarray(4);
    if ((cls === 10 && m === 50) || (cls === 20 && m === 40)) {
      const code = args.readUInt16BE(0);
      const text = args.subarray(3, 3 + args[2]!).toString('utf8');
      this.fail(new Error(`AMQP ${cls === 10 ? 'connection' : 'channel'} closed by the server: ${code} ${text}`));
      return;
    }
    const i = this.waiters.findIndex((w) => w.cls === cls && w.m === m);
    if (i >= 0) this.waiters.splice(i, 1)[0]!.ok(args);
  }

  private wait(cls: number, m: number): Promise<Buffer> {
    if (this.failed) return Promise.reject(this.failed);
    return new Promise((ok, fail) => this.waiters.push({ cls, m, ok, fail }));
  }

  private send(channel: number, payload: Buffer) {
    const head = Buffer.alloc(7);
    head[0] = 1;
    head.writeUInt16BE(channel, 1);
    head.writeUInt32BE(payload.length, 3);
    this.sock.write(Buffer.concat([head, payload, Buffer.from([FRAME_END])]));
  }

  private async call(channel: number, payload: Buffer, cls: number, m: number): Promise<Buffer> {
    const r = this.wait(cls, m);
    this.send(channel, payload);
    return r;
  }

  /** Opens a connection to `host:port` and channel 1 on `vhost` (a database name). */
  static async open(host: string, port: number, user: string, password: string, vhost: string): Promise<Amqp> {
    const sock = connect({ host, port });
    await new Promise<void>((ok, fail) => {
      sock.once('connect', ok);
      sock.once('error', fail);
    });
    const c = new Amqp(sock);
    const start = c.wait(10, 10);
    sock.write(Buffer.from('AMQP\x00\x00\x09\x01', 'latin1'));
    await start;
    const tune = c.wait(10, 30);
    c.send(
      0,
      method(10, 11, (w) => w.table({ product: 'studio-e2e' }).short('PLAIN').long(Buffer.from(`\0${user}\0${password}`, 'utf8')).short('en_US')),
    );
    const t = await tune;
    c.send(0, method(10, 31, (w) => w.u16(t.readUInt16BE(0) || 2047).u32(t.readUInt32BE(2) || 131072).u16(0)));
    await c.call(0, method(10, 40, (w) => w.short(vhost).short('').u8(0)), 10, 41);
    await c.call(1, method(20, 10, (w) => w.short('')), 20, 11);
    return c;
  }

  async exchangeDeclare(name: string, type: 'direct' | 'fanout' | 'topic' | 'headers', durable = true): Promise<void> {
    await this.call(1, method(40, 10, (w) => w.u16(0).short(name).short(type).u8(durable ? 0b10 : 0).table()), 40, 11);
  }

  async queueDeclare(name: string, args: Record<string, string> = {}, durable = true): Promise<{ messages: number; consumers: number }> {
    const r = await this.call(1, method(50, 10, (w) => w.u16(0).short(name).u8(durable ? 0b10 : 0).table(args)), 50, 11);
    const n = r[0]!;
    return { messages: r.readUInt32BE(1 + n), consumers: r.readUInt32BE(5 + n) };
  }

  async queueBind(queue: string, exchange: string, key: string): Promise<void> {
    await this.call(1, method(50, 20, (w) => w.u16(0).short(queue).short(exchange).short(key).u8(0).table()), 50, 21);
  }

  /** A consumer that holds up to `prefetch` messages unacknowledged. */
  async consume(queue: string, prefetch: number): Promise<void> {
    await this.call(1, method(60, 10, (w) => w.u32(0).u16(prefetch).u8(0)), 60, 11);
    await this.call(1, method(60, 20, (w) => w.u16(0).short(queue).short('').u8(0).table()), 60, 21);
  }

  async close(): Promise<void> {
    if (this.failed) {
      this.sock.destroy();
      return;
    }
    try {
      await Promise.race([this.call(0, method(10, 50, (w) => w.u16(200).short('bye').u16(0).u16(0)), 10, 51), new Promise((ok) => setTimeout(ok, 2000))]);
    } catch {
      // closing anyway
    }
    this.sock.destroy();
  }
}

/** `STUDIO_AMQP=127.0.0.1:28682`: required, never a default port. */
export function amqpAddress(): { host: string; port: number } | null {
  const a = process.env.STUDIO_AMQP;
  if (!a) return null;
  const i = a.lastIndexOf(':');
  return { host: a.slice(0, i), port: Number(a.slice(i + 1)) };
}
