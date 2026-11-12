import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { startServer, sleep, ReplyError, type Running, type TestClient } from "./helpers";

let srv: Running;
let c: TestClient;

before(async () => {
  srv = await startServer();
  c = await srv.connect();
});
after(() => srv.close());

const isErr = (r: unknown, prefix: string) => r instanceof ReplyError && r.message.startsWith(prefix);

test("PING / ECHO", async () => {
  assert.equal(await c.send("PING"), "PONG");
  assert.equal(await c.send("PING", "hi"), "hi");
  assert.equal(await c.send("ECHO", "hello"), "hello");
});

test("SET / GET and options", async () => {
  assert.equal(await c.send("SET", "a", "1"), "OK");
  assert.equal(await c.send("GET", "a"), "1");
  assert.equal(await c.send("GET", "missing"), null);
  assert.equal(await c.send("SET", "a", "2", "NX"), null);
  assert.equal(await c.send("SET", "new", "x", "XX"), null);
  assert.equal(await c.send("SET", "a", "3", "XX", "GET"), "1");
  assert.equal(await c.send("GET", "a"), "3");
  assert.ok(isErr(await c.send("SET", "a", "1", "EX", "0"), "ERR invalid expire time"));
  assert.ok(isErr(await c.send("SET", "a", "1", "NX", "XX"), "ERR syntax error"));
});

test("key expiry with PX, EX, TTL, PTTL, PERSIST", async () => {
  await c.send("SET", "temp", "v", "PX", "100");
  assert.equal(await c.send("GET", "temp"), "v");
  const pttl = (await c.send("PTTL", "temp")) as number;
  assert.ok(pttl > 0 && pttl <= 100);
  await sleep(150);
  assert.equal(await c.send("GET", "temp"), null);
  assert.equal(await c.send("TTL", "temp"), -2);

  await c.send("SET", "p", "v", "EX", "100");
  assert.equal(await c.send("TTL", "p"), 100);
  assert.equal(await c.send("PERSIST", "p"), 1);
  assert.equal(await c.send("TTL", "p"), -1);
  assert.equal(await c.send("EXPIRE", "p", "100"), 1);
  assert.equal(await c.send("EXPIRE", "nokey", "100"), 0);
});

test("active expiry removes keys that are never read", async () => {
  for (let i = 0; i < 50; i++) await c.send("SET", `exp:${i}`, "v", "PX", "50");
  await sleep(400);
  // inspect the raw map size via the store (bypassing lazy expiry)
  const raw = (srv.redis.store as unknown as { data: Map<string, unknown> }).data;
  assert.equal([...raw.keys()].filter((k) => k.startsWith("exp:")).length, 0);
});

test("INCR family and integer validation", async () => {
  assert.equal(await c.send("INCR", "counter"), 1);
  assert.equal(await c.send("INCRBY", "counter", "10"), 11);
  assert.equal(await c.send("DECR", "counter"), 10);
  assert.equal(await c.send("DECRBY", "counter", "4"), 6);
  await c.send("SET", "str", "abc");
  assert.ok(isErr(await c.send("INCR", "str"), "ERR value is not an integer"));
});

test("INCR keeps an existing TTL", async () => {
  await c.send("SET", "ic", "1", "EX", "100");
  await c.send("INCR", "ic");
  assert.equal(await c.send("TTL", "ic"), 100);
});

test("APPEND, STRLEN, MSET, MGET, GETDEL", async () => {
  assert.equal(await c.send("APPEND", "ap", "hello"), 5);
  assert.equal(await c.send("APPEND", "ap", " world"), 11);
  assert.equal(await c.send("STRLEN", "ap"), 11);
  assert.equal(await c.send("MSET", "m1", "a", "m2", "b"), "OK");
  assert.deepEqual(await c.send("MGET", "m1", "nope", "m2"), ["a", null, "b"]);
  assert.equal(await c.send("GETDEL", "m1"), "a");
  assert.equal(await c.send("EXISTS", "m1"), 0);
});

test("DEL, EXISTS, TYPE, KEYS, RENAME, DBSIZE", async () => {
  await c.send("FLUSHALL");
  await c.send("MSET", "user:1", "a", "user:2", "b", "order:1", "c");
  await c.send("RPUSH", "queue", "x");
  assert.deepEqual(((await c.send("KEYS", "user:*")) as string[]).sort(), ["user:1", "user:2"]);
  assert.equal(await c.send("DBSIZE"), 4);
  assert.equal(await c.send("TYPE", "user:1"), "string");
  assert.equal(await c.send("TYPE", "queue"), "list");
  assert.equal(await c.send("TYPE", "nope"), "none");
  assert.equal(await c.send("EXISTS", "user:1", "user:2", "nope"), 2);
  assert.equal(await c.send("RENAME", "order:1", "order:2"), "OK");
  assert.equal(await c.send("GET", "order:2"), "c");
  assert.equal(await c.send("DEL", "user:1", "user:2", "nope"), 2);
});

test("lists: push, pop, range, index, set", async () => {
  await c.send("DEL", "l");
  assert.equal(await c.send("RPUSH", "l", "a", "b", "c"), 3);
  assert.equal(await c.send("LPUSH", "l", "y", "z"), 5);
  assert.deepEqual(await c.send("LRANGE", "l", "0", "-1"), ["z", "y", "a", "b", "c"]);
  assert.deepEqual(await c.send("LRANGE", "l", "-2", "100"), ["b", "c"]);
  assert.deepEqual(await c.send("LRANGE", "l", "5", "10"), []);
  assert.equal(await c.send("LINDEX", "l", "-1"), "c");
  assert.equal(await c.send("LSET", "l", "0", "Z"), "OK");
  assert.equal(await c.send("LPOP", "l"), "Z");
  assert.deepEqual(await c.send("LPOP", "l", "2"), ["y", "a"]);
  assert.deepEqual(await c.send("RPOP", "l", "5"), ["c", "b"]);
  assert.equal(await c.send("EXISTS", "l"), 0, "empty lists are deleted");
  assert.equal(await c.send("LPOP", "l"), null);
  assert.equal(await c.send("LLEN", "l"), 0);
});

test("WRONGTYPE errors", async () => {
  await c.send("SET", "s", "v");
  await c.send("RPUSH", "lst", "v");
  assert.ok(isErr(await c.send("LPUSH", "s", "x"), "WRONGTYPE"));
  assert.ok(isErr(await c.send("GET", "lst"), "WRONGTYPE"));
});

test("argument count and unknown commands", async () => {
  assert.ok(isErr(await c.send("GET"), "ERR wrong number of arguments for 'get'"));
  assert.ok(isErr(await c.send("NOPE", "x"), "ERR unknown command 'NOPE'"));
});

test("BLPOP returns immediately when data exists", async () => {
  await c.send("RPUSH", "bq", "ready");
  assert.deepEqual(await c.send("BLPOP", "bq", "1"), ["bq", "ready"]);
});

test("BLPOP blocks until another client pushes", async () => {
  const waiter = await srv.connect();
  const pusher = await srv.connect();
  waiter.write("BLPOP", "jobs", "0");
  assert.ok(await waiter.silentFor(100), "should be blocked");
  assert.equal(await pusher.send("RPUSH", "jobs", "job-1"), 1);
  assert.deepEqual(await waiter.next(), ["jobs", "job-1"]);
  assert.equal(await pusher.send("LLEN", "jobs"), 0);
});

test("BLPOP serves blocked clients in FIFO order", async () => {
  const a = await srv.connect();
  const b = await srv.connect();
  a.write("BLPOP", "fifo", "0");
  await a.silentFor(30);
  b.write("BLPOP", "fifo", "0");
  await b.silentFor(30);
  await c.send("RPUSH", "fifo", "1", "2");
  assert.deepEqual(await a.next(), ["fifo", "1"]);
  assert.deepEqual(await b.next(), ["fifo", "2"]);
});

test("BLPOP times out with a null reply and the client keeps working", async () => {
  const w = await srv.connect();
  const t0 = Date.now();
  w.write("BLPOP", "never", "0.2");
  w.write("PING"); // queued behind the blocked command
  assert.equal(await w.next(), null);
  assert.ok(Date.now() - t0 >= 190);
  assert.equal(await w.next(), "PONG");
});

test("MULTI / EXEC runs queued commands atomically", async () => {
  assert.equal(await c.send("MULTI"), "OK");
  assert.equal(await c.send("SET", "tx", "1"), "QUEUED");
  assert.equal(await c.send("INCR", "tx"), "QUEUED");
  assert.equal(await c.send("GET", "tx"), "QUEUED");
  assert.deepEqual(await c.send("EXEC"), ["OK", 2, "2"]);
});

test("EXEC returns per-command runtime errors", async () => {
  await c.send("SET", "txs", "abc");
  await c.send("MULTI");
  await c.send("INCR", "txs");
  await c.send("SET", "txs2", "ok");
  const res = (await c.send("EXEC")) as unknown[];
  assert.ok(isErr(res[0], "ERR value is not an integer"));
  assert.equal(res[1], "OK");
});

test("EXECABORT on queue-time errors, DISCARD, and misuse", async () => {
  await c.send("MULTI");
  assert.ok(isErr(await c.send("NOPE"), "ERR unknown command"));
  assert.ok(isErr(await c.send("EXEC"), "EXECABORT"));
  await c.send("MULTI");
  await c.send("SET", "discarded", "1");
  assert.equal(await c.send("DISCARD"), "OK");
  assert.equal(await c.send("GET", "discarded"), null);
  assert.ok(isErr(await c.send("EXEC"), "ERR EXEC without MULTI"));
  await c.send("MULTI");
  assert.ok(isErr(await c.send("MULTI"), "ERR MULTI calls can not be nested"));
  await c.send("DISCARD");
});

test("pub/sub delivers messages to subscribers", async () => {
  const sub1 = await srv.connect();
  const sub2 = await srv.connect();
  assert.deepEqual(await sub1.send("SUBSCRIBE", "news", "sports"), ["subscribe", "news", 1]);
  assert.deepEqual(await sub1.next(), ["subscribe", "sports", 2]);
  assert.deepEqual(await sub2.send("SUBSCRIBE", "news"), ["subscribe", "news", 1]);

  assert.equal(await c.send("PUBLISH", "news", "hello"), 2);
  assert.deepEqual(await sub1.next(), ["message", "news", "hello"]);
  assert.deepEqual(await sub2.next(), ["message", "news", "hello"]);
  assert.equal(await c.send("PUBLISH", "nobody", "x"), 0);

  assert.ok(isErr(await sub1.send("GET", "x"), "ERR Can't execute 'get'"));
  assert.deepEqual(await sub1.send("PING"), ["pong", ""]);
  assert.deepEqual(await sub2.send("UNSUBSCRIBE", "news"), ["unsubscribe", "news", 0]);
  assert.equal(await c.send("PUBLISH", "news", "again"), 1);
  assert.deepEqual(await sub1.next(), ["message", "news", "again"]);
});

test("pipelined commands are answered in order", async () => {
  const p = await srv.connect();
  const n = 500;
  for (let i = 0; i < n; i++) p.write("INCR", "pipe");
  let last = 0;
  for (let i = 0; i < n; i++) last = (await p.next()) as number;
  assert.equal(last, n);
});

test("INFO and CONFIG GET", async () => {
  const info = (await c.send("INFO", "replication")) as string;
  assert.match(info, /role:master/);
  assert.match(info, /master_replid:[0-9a-f]{40}/);
  const cfg = (await c.send("CONFIG", "GET", "dbfilename")) as string[];
  assert.deepEqual(cfg, ["dbfilename", "dump.rdb"]);
});
