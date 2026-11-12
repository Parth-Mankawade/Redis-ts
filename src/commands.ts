import type { RedisServer, Client } from "./server";
import { RedisError, NOT_INTEGER, SYNTAX } from "./errors";
import * as R from "./resp";

type Handler = (s: RedisServer, c: Client, args: string[]) => string | null;

export interface CommandSpec {
  handler: Handler;
  /** Redis-style arity: N = exactly N args (incl. name), -N = at least N. */
  arity: number;
  /** Modifies the dataset: rejected on replicas, counted for persistence, propagated. */
  write?: boolean;
  /** Set to false when the handler propagates its own (rewritten) effect. */
  propagate?: boolean;
}

export const commands = new Map<string, CommandSpec>();
const def = (name: string, arity: number, handler: Handler, opts: Partial<CommandSpec> = {}) =>
  commands.set(name, { handler, arity, ...opts });

// ------------------------------------------------------------ helpers

function toInt(s: string | undefined): number {
  if (s === undefined || !/^-?\d+$/.test(s)) throw new RedisError(NOT_INTEGER);
  const n = Number(s);
  if (!Number.isSafeInteger(n)) throw new RedisError(NOT_INTEGER);
  return n;
}

function toPositiveTtl(s: string | undefined, cmd: string): number {
  const n = toInt(s);
  if (n <= 0) throw new RedisError(`ERR invalid expire time in '${cmd}' command`);
  return n;
}

// ------------------------------------------------------------ connection

def("PING", -1, (_s, c, a) => {
  if (c.subscriptions.size > 0) return R.array(["pong", a[0] ?? ""]);
  return a.length ? R.bulk(a[0]) : R.simple("PONG");
});
def("ECHO", 2, (_s, _c, a) => R.bulk(a[0]));
def("SELECT", 2, (_s, _c, a) => {
  if (toInt(a[0]) !== 0) throw new RedisError("ERR DB index is out of range");
  return R.simple("OK");
});
// redis-cli sends COMMAND DOCS on startup for hints; an empty reply is fine.
def("COMMAND", -1, () => R.rawArray([]));

// ------------------------------------------------------------ strings

def(
  "SET",
  -3,
  (s, _c, a) => {
    const [key, value, ...opts] = a;
    let expiresAt: number | null = null;
    let nx = false, xx = false, get = false, keepTtl = false, hasExpiry = false;
    for (let i = 0; i < opts.length; i++) {
      const o = opts[i].toUpperCase();
      if (o === "NX") nx = true;
      else if (o === "XX") xx = true;
      else if (o === "GET") get = true;
      else if (o === "KEEPTTL") keepTtl = true;
      else if (o === "EX" || o === "PX" || o === "EXAT" || o === "PXAT") {
        if (hasExpiry || i + 1 >= opts.length) throw new RedisError(SYNTAX);
        const n = toPositiveTtl(opts[++i], "set");
        expiresAt = o === "EX" ? Date.now() + n * 1000 : o === "PX" ? Date.now() + n : o === "EXAT" ? n * 1000 : n;
        hasExpiry = true;
      } else throw new RedisError(SYNTAX);
    }
    if ((nx && xx) || (keepTtl && hasExpiry)) throw new RedisError(SYNTAX);

    const old = get ? s.store.getString(key) : null; // throws WRONGTYPE for non-strings
    const exists = s.store.has(key);
    if ((nx && exists) || (xx && !exists)) return R.bulk(get ? old : null);
    if (keepTtl) expiresAt = s.store.get(key)?.expiresAt ?? null;
    s.store.setString(key, value, expiresAt);
    return get ? R.bulk(old) : R.simple("OK");
  },
  { write: true },
);

def("GET", 2, (s, _c, a) => R.bulk(s.store.getString(a[0])));

def("GETDEL", 2, (s, _c, a) => {
  const v = s.store.getString(a[0]);
  if (v !== null) s.store.delete(a[0]);
  return R.bulk(v);
}, { write: true });

def("MGET", -2, (s, _c, a) =>
  R.array(a.map((k) => {
    const e = s.store.get(k);
    return e?.type === "string" ? e.value : null;
  })),
);

def("MSET", -3, (s, _c, a) => {
  if (a.length % 2 !== 0) throw new RedisError("ERR wrong number of arguments for 'mset' command");
  for (let i = 0; i < a.length; i += 2) s.store.setString(a[i], a[i + 1]);
  return R.simple("OK");
}, { write: true });

function incrBy(s: RedisServer, key: string, delta: number): string {
  const current = s.store.getString(key);
  const n = current === null ? 0 : toInt(current);
  const next = n + delta;
  if (!Number.isSafeInteger(next)) throw new RedisError("ERR increment or decrement would overflow");
  s.store.setString(key, String(next), s.store.get(key)?.expiresAt ?? null); // INCR keeps the TTL
  return R.integer(next);
}

def("INCR", 2, (s, _c, a) => incrBy(s, a[0], 1), { write: true });
def("DECR", 2, (s, _c, a) => incrBy(s, a[0], -1), { write: true });
def("INCRBY", 3, (s, _c, a) => incrBy(s, a[0], toInt(a[1])), { write: true });
def("DECRBY", 3, (s, _c, a) => incrBy(s, a[0], -toInt(a[1])), { write: true });

def("APPEND", 3, (s, _c, a) => {
  const v = (s.store.getString(a[0]) ?? "") + a[1];
  s.store.setString(a[0], v, s.store.get(a[0])?.expiresAt ?? null);
  return R.integer(Buffer.byteLength(v));
}, { write: true });

def("STRLEN", 2, (s, _c, a) => R.integer(Buffer.byteLength(s.store.getString(a[0]) ?? "")));

// ------------------------------------------------------------ keys

def("DEL", -2, (s, _c, a) => R.integer(a.filter((k) => s.store.has(k) && s.store.delete(k)).length), { write: true });
def("EXISTS", -2, (s, _c, a) => R.integer(a.filter((k) => s.store.has(k)).length));
def("KEYS", 2, (s, _c, a) => R.array(s.store.keys(a[0])));
def("TYPE", 2, (s, _c, a) => R.simple(s.store.get(a[0])?.type ?? "none"));
def("DBSIZE", 1, (s) => R.integer(s.store.size()));

const flush: (s: RedisServer) => string = (s) => {
  s.store.flush();
  return R.simple("OK");
};
def("FLUSHALL", -1, flush, { write: true });
def("FLUSHDB", -1, flush, { write: true });

function expire(s: RedisServer, key: string, ms: number): string {
  return R.integer(s.store.setExpiry(key, Date.now() + ms) ? 1 : 0);
}
def("EXPIRE", 3, (s, _c, a) => expire(s, a[0], toInt(a[1]) * 1000), { write: true });
def("PEXPIRE", 3, (s, _c, a) => expire(s, a[0], toInt(a[1])), { write: true });
def("PERSIST", 2, (s, _c, a) => {
  const e = s.store.get(a[0]);
  if (!e || e.expiresAt === null) return R.integer(0);
  s.store.setExpiry(a[0], null);
  return R.integer(1);
}, { write: true });

function ttl(s: RedisServer, key: string, unitMs: number): string {
  const e = s.store.get(key);
  if (!e) return R.integer(-2);
  if (e.expiresAt === null) return R.integer(-1);
  return R.integer(Math.max(0, Math.round((e.expiresAt - Date.now()) / unitMs)));
}
def("TTL", 2, (s, _c, a) => ttl(s, a[0], 1000));
def("PTTL", 2, (s, _c, a) => ttl(s, a[0], 1));

def("RENAME", 3, (s, _c, a) => {
  const e = s.store.get(a[0]);
  if (!e) throw new RedisError("ERR no such key");
  s.store.delete(a[0]);
  s.store.set(a[1], e);
  return R.simple("OK");
}, { write: true });

// ------------------------------------------------------------ lists

function push(s: RedisServer, a: string[], left: boolean): string {
  const [key, ...values] = a;
  const list = s.store.getList(key, true)!;
  for (const v of values) left ? list.pushFront(v) : list.pushBack(v);
  s.signalKeyAsReady(key);
  return R.integer(list.length);
}
def("LPUSH", -3, (s, _c, a) => push(s, a, true), { write: true });
def("RPUSH", -3, (s, _c, a) => push(s, a, false), { write: true });

function pop(s: RedisServer, a: string[], left: boolean): string {
  const [key, countArg] = a;
  const count = countArg === undefined ? null : toInt(countArg);
  if (count !== null && count < 0) throw new RedisError("ERR value is out of range, must be positive");
  const list = s.store.getList(key);
  if (!list || list.length === 0) return count === null ? R.bulk(null) : R.nullArray();
  if (count === 0) return R.rawArray([]);
  const n = Math.min(count ?? 1, list.length);
  const items: string[] = [];
  for (let i = 0; i < n; i++) items.push((left ? list.popFront() : list.popBack())!);
  s.store.dropIfEmpty(key);
  return count === null ? R.bulk(items[0]) : R.array(items);
}
def("LPOP", -2, (s, _c, a) => pop(s, a, true), { write: true });
def("RPOP", -2, (s, _c, a) => pop(s, a, false), { write: true });

def("LLEN", 2, (s, _c, a) => R.integer(s.store.getList(a[0])?.length ?? 0));

def("LRANGE", 4, (s, _c, a) => {
  const list = s.store.getList(a[0]);
  if (!list) return R.rawArray([]);
  const len = list.length;
  let start = toInt(a[1]);
  let stop = toInt(a[2]);
  if (start < 0) start = Math.max(len + start, 0);
  if (stop < 0) stop = len + stop;
  if (stop >= len) stop = len - 1;
  if (start > stop || start >= len) return R.rawArray([]);
  return R.array(list.slice(start, stop + 1));
});

def("LINDEX", 3, (s, _c, a) => {
  const list = s.store.getList(a[0]);
  if (!list) return R.bulk(null);
  let i = toInt(a[1]);
  if (i < 0) i += list.length;
  return R.bulk(list.get(i) ?? null);
});

def("LSET", 4, (s, _c, a) => {
  const list = s.store.getList(a[0]);
  if (!list) throw new RedisError("ERR no such key");
  let i = toInt(a[1]);
  if (i < 0) i += list.length;
  if (i < 0 || i >= list.length) throw new RedisError("ERR index out of range");
  list.set(i, a[2]);
  return R.simple("OK");
}, { write: true });

def(
  "BLPOP",
  -3,
  (s, c, a) => {
    const keys = a.slice(0, -1);
    const timeout = Number(a[a.length - 1]);
    if (!Number.isFinite(timeout)) throw new RedisError("ERR timeout is not a float or out of range");
    if (timeout < 0) throw new RedisError("ERR timeout is negative");

    for (const key of keys) {
      const list = s.store.getList(key);
      if (list && list.length > 0) {
        const value = list.popFront()!;
        s.store.dropIfEmpty(key);
        s.propagate(["LPOP", key]); // replicas must not block, so send the effect
        return R.array([key, value]);
      }
    }
    if (c.inExec) return R.nullArray(); // never block inside a transaction
    s.block(c, keys, Math.round(timeout * 1000));
    return null;
  },
  { write: true, propagate: false },
);

// ------------------------------------------------------------ transactions

def("MULTI", 1, (_s, c) => {
  if (c.multi) throw new RedisError("ERR MULTI calls can not be nested");
  c.multi = [];
  c.multiFailed = false;
  return R.simple("OK");
});

def("DISCARD", 1, (_s, c) => {
  if (!c.multi) throw new RedisError("ERR DISCARD without MULTI");
  c.multi = null;
  return R.simple("OK");
});

def("EXEC", 1, (s, c) => {
  if (!c.multi) throw new RedisError("ERR EXEC without MULTI");
  const queued = c.multi;
  const failed = c.multiFailed;
  c.multi = null;
  c.multiFailed = false;
  if (failed) throw new RedisError("EXECABORT Transaction discarded because of previous errors.");

  // Node is single-threaded, so running the queue in one go is atomic:
  // no other client's command can interleave.
  c.inExec = true;
  try {
    return R.rawArray(queued.map((args) => s.run(c, args) ?? R.nullArray()));
  } finally {
    c.inExec = false;
  }
});

// ------------------------------------------------------------ pub/sub

def("SUBSCRIBE", -2, (s, c, a) =>
  a
    .map((ch) => {
      s.subscribe(c, ch);
      return R.rawArray([R.bulk("subscribe"), R.bulk(ch), R.integer(c.subscriptions.size)]);
    })
    .join(""),
);

def("UNSUBSCRIBE", -1, (s, c, a) => {
  const targets = a.length ? a : [...c.subscriptions];
  if (targets.length === 0) return R.rawArray([R.bulk("unsubscribe"), R.bulk(null), R.integer(0)]);
  return targets
    .map((ch) => {
      s.unsubscribe(c, ch);
      return R.rawArray([R.bulk("unsubscribe"), R.bulk(ch), R.integer(c.subscriptions.size)]);
    })
    .join("");
});

def("PUBLISH", 3, (s, _c, a) => R.integer(s.publish(a[0], a[1])));

// ------------------------------------------------------------ replication

def("REPLCONF", -2, (s, c, a) => {
  switch (a[0].toUpperCase()) {
    case "ACK":
      s.onAck(c, toInt(a[1]));
      return null; // ACKs are never answered
    case "GETACK":
      return R.array(["REPLCONF", "ACK", String(s.replOffset)]);
    default:
      return R.simple("OK"); // listening-port, capa, ...
  }
});

def("PSYNC", 3, (s, c) => {
  if (s.role !== "master") throw new RedisError("ERR this replica cannot serve PSYNC");
  s.fullResync(c);
  return null;
});

def("WAIT", 3, (s, c, a) => {
  const needed = toInt(a[0]);
  const timeout = toInt(a[1]);
  if (timeout < 0) throw new RedisError("ERR timeout is negative");
  return s.wait(c, needed, timeout);
});

// ------------------------------------------------------------ server / persistence

def("INFO", -1, (s, _c, a) => R.bulk(s.info(a[0])));

def("CONFIG", -2, (s, _c, a) => {
  if (a[0].toUpperCase() !== "GET") throw new RedisError(`ERR unknown subcommand '${a[0]}'. Only CONFIG GET is supported`);
  const values: Record<string, string> = {
    port: String(s.config.port),
    dir: s.config.dir,
    dbfilename: s.config.dbfilename,
    save: s.config.save ? `${s.config.save} 1` : "",
    replicaof: s.config.replicaof ? `${s.config.replicaof.host} ${s.config.replicaof.port}` : "",
  };
  const out: string[] = [];
  for (const pattern of a.slice(1)) {
    const re = new RegExp("^" + pattern.toLowerCase().replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*").replace(/\?/g, ".") + "$");
    for (const [k, v] of Object.entries(values)) if (re.test(k) && !out.includes(k)) out.push(k, v);
  }
  return R.array(out);
});

def("SAVE", 1, (s) => {
  if (s.bgsaveInProgress) throw new RedisError("ERR Background save already in progress");
  s.saveSync();
  return R.simple("OK");
});

def("BGSAVE", -1, (s) => {
  if (s.bgsaveInProgress) throw new RedisError("ERR Background save already in progress");
  s.saveInBackground().catch((e) => console.error("BGSAVE failed:", e));
  return R.simple("Background saving started");
});

def("LASTSAVE", 1, (s) => R.integer(s.lastSave));
