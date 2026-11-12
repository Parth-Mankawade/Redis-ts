import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { startServer, tempDir, waitFor } from "./helpers";

test("SAVE writes an RDB file that is loaded on restart", async () => {
  const dir = tempDir();
  const first = await startServer({ dir });
  const c = await first.connect();
  await c.send("SET", "name", "parth");
  await c.send("SET", "session", "abc", "EX", "3600");
  await c.send("RPUSH", "queue", "a", "b");
  assert.equal(await c.send("SAVE"), "OK");
  assert.ok(fs.existsSync(path.join(dir, "dump.rdb")));
  await first.close();

  const second = await startServer({ dir });
  const c2 = await second.connect();
  assert.equal(await c2.send("GET", "name"), "parth");
  assert.deepEqual(await c2.send("LRANGE", "queue", "0", "-1"), ["a", "b"]);
  const ttl = (await c2.send("TTL", "session")) as number;
  assert.ok(ttl > 3500 && ttl <= 3600);
  await second.close();
});

test("BGSAVE saves without blocking and resets the dirty counter", async () => {
  const dir = tempDir();
  const srv = await startServer({ dir });
  const c = await srv.connect();
  await c.send("SET", "k", "v");
  assert.equal(srv.redis.dirty, 1);
  assert.equal(await c.send("BGSAVE"), "Background saving started");
  await waitFor(() => fs.existsSync(path.join(dir, "dump.rdb")) && !srv.redis.bgsaveInProgress);
  assert.equal(srv.redis.dirty, 0);
  assert.equal(typeof (await c.send("LASTSAVE")), "number");
  await srv.close();
});

test("uses a custom dir and dbfilename", async () => {
  const dir = tempDir();
  const srv = await startServer({ dir, dbfilename: "custom.rdb" });
  const c = await srv.connect();
  await c.send("SET", "x", "1");
  await c.send("SAVE");
  assert.ok(fs.existsSync(path.join(dir, "custom.rdb")));
  assert.deepEqual(await c.send("CONFIG", "GET", "dir"), ["dir", dir]);
  await srv.close();
});
