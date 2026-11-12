import net from "node:net";
import os from "node:os";
import fs from "node:fs";
import path from "node:path";
import { RedisServer } from "../src/server";
import { createTcpServer } from "../src/network";
import { startReplication, type ReplicationLink } from "../src/replication";
import { defaults, type Config } from "../src/config";

export class ReplyError extends Error {}
export type Reply = string | number | null | ReplyError | Reply[];

/** Decodes one RESP reply starting at `pos`; returns null if incomplete. */
export function decodeReply(buf: Buffer, pos = 0): { value: Reply; next: number } | null {
  const end = buf.indexOf("\r\n", pos);
  if (end === -1) return null;
  const type = String.fromCharCode(buf[pos]);
  const line = buf.toString("utf8", pos + 1, end);
  const after = end + 2;
  switch (type) {
    case "+":
      return { value: line, next: after };
    case "-":
      return { value: new ReplyError(line), next: after };
    case ":":
      return { value: Number(line), next: after };
    case "$": {
      const len = Number(line);
      if (len === -1) return { value: null, next: after };
      if (buf.length < after + len + 2) return null;
      return { value: buf.toString("utf8", after, after + len), next: after + len + 2 };
    }
    case "*": {
      const n = Number(line);
      if (n === -1) return { value: null, next: after };
      const items: Reply[] = [];
      let p = after;
      for (let i = 0; i < n; i++) {
        const r = decodeReply(buf, p);
        if (!r) return null;
        items.push(r.value);
        p = r.next;
      }
      return { value: items, next: p };
    }
    default:
      throw new Error(`bad reply type ${type}`);
  }
}

export class TestClient {
  private buf = Buffer.alloc(0);
  private queue: Reply[] = [];
  private waiting: ((r: Reply) => void)[] = [];

  private constructor(readonly socket: net.Socket) {
    socket.on("data", (chunk) => {
      this.buf = Buffer.concat([this.buf, chunk]);
      for (;;) {
        const r = decodeReply(this.buf);
        if (!r) break;
        this.buf = this.buf.subarray(r.next);
        const w = this.waiting.shift();
        if (w) w(r.value);
        else this.queue.push(r.value);
      }
    });
  }

  static connect(port: number): Promise<TestClient> {
    return new Promise((resolve, reject) => {
      const s = net.connect(port, "127.0.0.1", () => resolve(new TestClient(s)));
      s.on("error", reject);
    });
  }

  next(timeoutMs = 2000): Promise<Reply> {
    if (this.queue.length) return Promise.resolve(this.queue.shift()!);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("timed out waiting for reply")), timeoutMs);
      this.waiting.push((r) => {
        clearTimeout(t);
        resolve(r);
      });
    });
  }

  /** Resolves true if no reply arrives within `ms` (used to assert blocking). */
  async silentFor(ms: number): Promise<boolean> {
    await sleep(ms);
    return this.queue.length === 0;
  }

  write(...args: string[]): void {
    this.socket.write(`*${args.length}\r\n` + args.map((a) => `$${Buffer.byteLength(a)}\r\n${a}\r\n`).join(""));
  }

  send(...args: string[]): Promise<Reply> {
    this.write(...args);
    return this.next();
  }

  close(): void {
    this.socket.destroy();
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error("condition not met in time");
    await sleep(20);
  }
}

export function tempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "redis-ts-test-"));
}

export interface Running {
  redis: RedisServer;
  port: number;
  connect(): Promise<TestClient>;
  close(): Promise<void>;
}

export async function startServer(overrides: Partial<Config> = {}): Promise<Running> {
  const config: Config = { ...defaults, port: 0, save: 0, dir: tempDir(), ...overrides };
  const redis = new RedisServer(config);
  if (redis.role === "master") redis.loadFromDisk();

  const tcp = createTcpServer(redis);
  const sockets = new Set<net.Socket>();
  tcp.on("connection", (s) => {
    sockets.add(s);
    s.on("close", () => sockets.delete(s));
  });
  await new Promise<void>((r) => tcp.listen(0, "127.0.0.1", r));
  const port = (tcp.address() as net.AddressInfo).port;
  config.port = port;

  let link: ReplicationLink | null = null;
  if (config.replicaof) link = startReplication(redis, config.replicaof.host, config.replicaof.port);

  const clients: TestClient[] = [];
  return {
    redis,
    port,
    async connect() {
      const c = await TestClient.connect(port);
      clients.push(c);
      return c;
    },
    async close() {
      link?.stop();
      clients.forEach((c) => c.close());
      sockets.forEach((s) => s.destroy());
      redis.store.stop();
      await new Promise((r) => tcp.close(r));
    },
  };
}
