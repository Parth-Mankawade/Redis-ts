import fs from "node:fs";
import path from "node:path";
import net from "node:net";
import { randomBytes } from "node:crypto";
import type { Config } from "./config";
import { Store } from "./store";
import { encodeRdb, decodeRdb } from "./rdb";
import { RedisError } from "./errors";
import { commands, type CommandSpec } from "./commands";
import * as R from "./resp";

export interface Client {
  id: number;
  socket: net.Socket;
  buffer: Buffer;
  /** Queued commands while inside MULTI, or null outside a transaction. */
  multi: string[][] | null;
  multiFailed: boolean;
  inExec: boolean;
  subscriptions: Set<string>;
  /** Set while the client waits on BLPOP or WAIT; its input is paused meanwhile. */
  blocked: { keys: string[]; timer: NodeJS.Timeout | null } | null;
  /** True for the replica's connection to its master. */
  fromMaster: boolean;
  /** True for a connection from a replica to this master (after PSYNC). */
  isReplica: boolean;
  ackOffset: number;
  closed: boolean;
}

interface PendingWait {
  client: Client;
  needed: number;
  target: number;
}

const SUBSCRIBED_ALLOWED = new Set(["SUBSCRIBE", "UNSUBSCRIBE", "PING", "QUIT"]);
const TX_CONTROL = new Set(["MULTI", "EXEC", "DISCARD"]);

export class RedisServer {
  readonly store = new Store();
  readonly startedAt = Date.now();

  // replication state
  replid = randomBytes(20).toString("hex");
  /** Master: bytes propagated to replicas. Replica: bytes processed from master. */
  replOffset = 0;
  readonly replicas = new Set<Client>();
  masterLinkUp = false;
  private waits: PendingWait[] = [];

  // pub/sub and blocking state
  readonly channels = new Map<string, Set<Client>>();
  private blockedOn = new Map<string, Client[]>();
  private readyKeys = new Set<string>();

  // persistence state
  dirty = 0;
  lastSave = Math.floor(Date.now() / 1000);
  bgsaveInProgress = false;

  private nextClientId = 1;

  constructor(readonly config: Config) {}

  get role(): "master" | "slave" {
    return this.config.replicaof ? "slave" : "master";
  }

  // ------------------------------------------------------------ clients

  newClient(socket: net.Socket, fromMaster = false): Client {
    return {
      id: this.nextClientId++,
      socket,
      buffer: Buffer.alloc(0),
      multi: null,
      multiFailed: false,
      inExec: false,
      subscriptions: new Set(),
      blocked: null,
      fromMaster,
      isReplica: false,
      ackOffset: 0,
      closed: false,
    };
  }

  onData(c: Client, chunk: Buffer): void {
    c.buffer = c.buffer.length ? Buffer.concat([c.buffer, chunk]) : chunk;
    this.processInput(c);
  }

  /** Parses and runs every complete command in the client's buffer. */
  processInput(c: Client): void {
    const out: string[] = [];
    const flush = () => {
      if (out.length && !c.closed) c.socket.write(out.join(""));
      out.length = 0;
    };
    try {
      this.processCommands(c, out, flush);
    } finally {
      flush();
    }
  }

  private processCommands(c: Client, out: string[], flush: () => void): void {
    while (!c.blocked && !c.closed) {
      let parsed: R.ParsedCommand | null;
      try {
        parsed = R.parseCommand(c.buffer);
      } catch (e) {
        flush();
        c.socket.end(R.error(`ERR Protocol error: ${(e as Error).message}`));
        c.closed = true;
        return;
      }
      if (!parsed) return;
      c.buffer = c.buffer.subarray(parsed.consumed);
      if (parsed.args.length === 0) continue;

      if (parsed.args[0].toUpperCase() === "QUIT") {
        flush();
        c.socket.end(R.simple("OK"));
        c.closed = true;
        return;
      }

      // PSYNC writes the RDB straight to the socket, so earlier batched replies go out first.
      if (parsed.args[0].toUpperCase() === "PSYNC") flush();
      const reply = this.execute(c, parsed.args);
      if (c.fromMaster) {
        // A replica stays silent towards its master, except to acknowledge offsets.
        const isGetAck = parsed.args[0].toUpperCase() === "REPLCONF" && parsed.args[1]?.toUpperCase() === "GETACK";
        if (isGetAck && reply !== null) c.socket.write(reply);
        this.replOffset += parsed.consumed;
      } else if (reply !== null) {
        out.push(reply);
      }
    }
  }

  removeClient(c: Client): void {
    c.closed = true;
    this.unblock(c);
    for (const ch of c.subscriptions) this.unsubscribe(c, ch);
    this.replicas.delete(c);
    this.waits = this.waits.filter((w) => w.client !== c);
  }

  // ------------------------------------------------------------ dispatch

  execute(c: Client, args: string[]): string | null {
    const name = args[0].toUpperCase();
    const spec = commands.get(name);

    if (c.subscriptions.size > 0 && !SUBSCRIBED_ALLOWED.has(name)) {
      return R.error(
        `ERR Can't execute '${args[0].toLowerCase()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`,
      );
    }

    if (c.multi && !TX_CONTROL.has(name)) {
      const problem = this.validate(args, spec);
      if (problem) {
        c.multiFailed = true;
        return problem;
      }
      c.multi.push(args);
      return R.simple("QUEUED");
    }

    return this.run(c, args);
  }

  private validate(args: string[], spec: CommandSpec | undefined): string | null {
    if (!spec) {
      const rest = args.slice(1).map((a) => `'${a}'`).join(" ");
      return R.error(`ERR unknown command '${args[0]}', with args beginning with: ${rest}`);
    }
    const n = args.length;
    if (spec.arity >= 0 ? n !== spec.arity : n < -spec.arity) {
      return R.error(`ERR wrong number of arguments for '${args[0].toLowerCase()}' command`);
    }
    return null;
  }

  run(c: Client, args: string[]): string | null {
    const spec = commands.get(args[0].toUpperCase());
    const problem = this.validate(args, spec);
    if (problem || !spec) return problem;

    if (spec.write && this.role === "slave" && !c.fromMaster) {
      return R.error("READONLY You can't write against a read only replica.");
    }

    let reply: string | null;
    try {
      reply = spec.handler(this, c, args.slice(1));
    } catch (e) {
      if (e instanceof RedisError) return R.error(e.message);
      console.error(`Unexpected error running ${args[0]}:`, e);
      return R.error("ERR internal error");
    }

    if (spec.write) {
      this.dirty++;
      if (spec.propagate !== false) this.propagate(args);
    }
    this.handleReadyKeys();
    return reply;
  }

  // ------------------------------------------------------------ replication

  /** Streams a write command to all connected replicas. */
  propagate(args: string[]): void {
    if (this.role !== "master") return;
    const payload = R.array(args);
    for (const r of this.replicas) r.socket.write(payload);
    this.replOffset += Buffer.byteLength(payload);
  }

  /** Starts a full resync: snapshot the dataset and send it as an RDB payload. */
  fullResync(c: Client): void {
    const rdb = encodeRdb(this.store.entries());
    c.socket.write(R.simple(`FULLRESYNC ${this.replid} ${this.replOffset}`));
    c.socket.write(Buffer.concat([Buffer.from(`$${rdb.length}\r\n`), rdb]));
    c.isReplica = true;
    c.ackOffset = this.replOffset;
    this.replicas.add(c);
  }

  /** Loads the RDB snapshot sent by our master during a full resync. */
  loadFromMaster(rdb: Buffer, replid: string, offset: number): void {
    this.store.flush();
    for (const [k, e] of decodeRdb(rdb)) this.store.set(k, e);
    this.replid = replid;
    this.replOffset = offset;
    this.masterLinkUp = true;
  }

  countAcked(target: number): number {
    let n = 0;
    for (const r of this.replicas) if (r.ackOffset >= target) n++;
    return n;
  }

  /** WAIT: block the client until `needed` replicas acknowledged all writes so far. */
  wait(c: Client, needed: number, timeoutMs: number): string | null {
    const target = this.replOffset;
    const acked = this.countAcked(target);
    if (acked >= needed || this.replicas.size === 0 || c.inExec) return R.integer(acked);

    this.propagate(["REPLCONF", "GETACK", "*"]);
    const pending: PendingWait = { client: c, needed, target };
    this.waits.push(pending);
    const timer =
      timeoutMs > 0
        ? setTimeout(() => this.finishWait(pending), timeoutMs)
        : null;
    c.blocked = { keys: [], timer };
    return null;
  }

  onAck(c: Client, offset: number): void {
    c.ackOffset = Math.max(c.ackOffset, offset);
    for (const w of [...this.waits]) {
      if (this.countAcked(w.target) >= w.needed) this.finishWait(w);
    }
  }

  private finishWait(w: PendingWait): void {
    if (!this.waits.includes(w)) return;
    this.waits = this.waits.filter((x) => x !== w);
    this.unblock(w.client);
    if (!w.client.closed) {
      w.client.socket.write(R.integer(this.countAcked(w.target)));
      this.processInput(w.client);
    }
  }

  // ------------------------------------------------------------ blocking lists

  block(c: Client, keys: string[], timeoutMs: number): void {
    const timer =
      timeoutMs > 0
        ? setTimeout(() => {
            this.unblock(c);
            if (!c.closed) {
              c.socket.write(R.nullArray());
              this.processInput(c);
            }
          }, timeoutMs)
        : null;
    c.blocked = { keys, timer };
    for (const k of keys) {
      const q = this.blockedOn.get(k) ?? [];
      q.push(c);
      this.blockedOn.set(k, q);
    }
  }

  unblock(c: Client): void {
    if (!c.blocked) return;
    if (c.blocked.timer) clearTimeout(c.blocked.timer);
    for (const k of c.blocked.keys) {
      const q = this.blockedOn.get(k)?.filter((x) => x !== c) ?? [];
      if (q.length) this.blockedOn.set(k, q);
      else this.blockedOn.delete(k);
    }
    c.blocked = null;
  }

  /** Called by list pushes; blocked clients are served after the push is propagated. */
  signalKeyAsReady(key: string): void {
    if (this.blockedOn.has(key)) this.readyKeys.add(key);
  }

  private handleReadyKeys(): void {
    while (this.readyKeys.size > 0) {
      const keys = [...this.readyKeys];
      this.readyKeys.clear();
      for (const key of keys) {
        for (;;) {
          const queue = this.blockedOn.get(key);
          const list = this.store.getList(key);
          if (!queue?.length || !list?.length) break;
          const client = queue[0];
          const value = list.popFront()!;
          this.store.dropIfEmpty(key);
          this.unblock(client);
          this.dirty++;
          this.propagate(["LPOP", key]);
          client.socket.write(R.array([key, value]));
          setImmediate(() => this.processInput(client));
        }
      }
    }
  }

  // ------------------------------------------------------------ pub/sub

  subscribe(c: Client, channel: string): void {
    c.subscriptions.add(channel);
    const subs = this.channels.get(channel) ?? new Set();
    subs.add(c);
    this.channels.set(channel, subs);
  }

  unsubscribe(c: Client, channel: string): void {
    c.subscriptions.delete(channel);
    const subs = this.channels.get(channel);
    subs?.delete(c);
    if (subs && subs.size === 0) this.channels.delete(channel);
  }

  publish(channel: string, message: string): number {
    const subs = this.channels.get(channel);
    if (!subs) return 0;
    const payload = R.array(["message", channel, message]);
    for (const s of subs) s.socket.write(payload);
    return subs.size;
  }

  // ------------------------------------------------------------ persistence

  rdbPath(): string {
    return path.join(this.config.dir, this.config.dbfilename);
  }

  loadFromDisk(): number {
    const file = this.rdbPath();
    if (!fs.existsSync(file)) return 0;
    const data = decodeRdb(fs.readFileSync(file));
    for (const [k, e] of data) this.store.set(k, e);
    return data.size;
  }

  saveSync(): void {
    const rdb = encodeRdb(this.store.entries());
    fs.mkdirSync(this.config.dir, { recursive: true });
    const tmp = path.join(this.config.dir, `temp-${process.pid}.rdb`);
    fs.writeFileSync(tmp, rdb);
    fs.renameSync(tmp, this.rdbPath()); // atomic replace: never leaves a half-written dump
    this.dirty = 0;
    this.lastSave = Math.floor(Date.now() / 1000);
  }

  /** Snapshots in memory synchronously, then writes to disk without blocking clients. */
  async saveInBackground(): Promise<void> {
    if (this.bgsaveInProgress) return;
    this.bgsaveInProgress = true;
    const rdb = encodeRdb(this.store.entries());
    const dirtyAtSnapshot = this.dirty;
    try {
      await fs.promises.mkdir(this.config.dir, { recursive: true });
      const tmp = path.join(this.config.dir, `temp-bg-${process.pid}.rdb`);
      await fs.promises.writeFile(tmp, rdb);
      await fs.promises.rename(tmp, this.rdbPath());
      this.dirty -= dirtyAtSnapshot;
      this.lastSave = Math.floor(Date.now() / 1000);
    } finally {
      this.bgsaveInProgress = false;
    }
  }

  // ------------------------------------------------------------ introspection

  info(section?: string): string {
    const sections: Record<string, string[]> = {
      server: [
        "redis_version:7.2.0",
        "redis_mode:standalone",
        `process_id:${process.pid}`,
        `tcp_port:${this.config.port}`,
        `uptime_in_seconds:${Math.floor((Date.now() - this.startedAt) / 1000)}`,
      ],
      replication: [`role:${this.role}`],
      persistence: [
        `rdb_changes_since_last_save:${this.dirty}`,
        `rdb_bgsave_in_progress:${this.bgsaveInProgress ? 1 : 0}`,
        `rdb_last_save_time:${this.lastSave}`,
      ],
      keyspace: [],
    };
    if (this.role === "master") {
      sections.replication.push(`connected_slaves:${this.replicas.size}`);
      let i = 0;
      for (const r of this.replicas) {
        sections.replication.push(`slave${i++}:ip=${r.socket.remoteAddress},offset=${r.ackOffset}`);
      }
    } else {
      sections.replication.push(
        `master_host:${this.config.replicaof!.host}`,
        `master_port:${this.config.replicaof!.port}`,
        `master_link_status:${this.masterLinkUp ? "up" : "down"}`,
      );
    }
    sections.replication.push(`master_replid:${this.replid}`, `master_repl_offset:${this.replOffset}`);
    const keys = this.store.size();
    if (keys > 0) sections.keyspace.push(`db0:keys=${keys},expires=${this.store.expiresCount()}`);

    const wanted = section?.toLowerCase();
    const names = !wanted || wanted === "all" || wanted === "default" ? Object.keys(sections) : [wanted];
    return names
      .filter((n) => sections[n])
      .map((n) => `# ${n[0].toUpperCase()}${n.slice(1)}\r\n${sections[n].join("\r\n")}\r\n`)
      .join("\r\n");
  }
}
