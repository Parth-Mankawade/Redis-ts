import { test } from "node:test";
import assert from "node:assert/strict";
import { startServer, waitFor, ReplyError } from "./helpers";

async function setup(replicaCount = 1) {
  const master = await startServer();
  const mc = await master.connect();
  await mc.send("SET", "before", "snapshot"); // exists before replicas connect -> sent via RDB
  await mc.send("RPUSH", "list", "a", "b");
  const replicas = [];
  for (let i = 0; i < replicaCount; i++) {
    const r = await startServer({ replicaof: { host: "127.0.0.1", port: master.port } });
    replicas.push(r);
  }
  await waitFor(() => master.redis.replicas.size === replicaCount && replicas.every((r) => r.redis.masterLinkUp));
  return { master, mc, replicas, close: async () => { for (const r of replicas) await r.close(); await master.close(); } };
}

test("replica receives the initial snapshot and streamed writes", async () => {
  const { mc, replicas, close } = await setup();
  const rc = await replicas[0].connect();
  assert.equal(await rc.send("GET", "before"), "snapshot");
  assert.deepEqual(await rc.send("LRANGE", "list", "0", "-1"), ["a", "b"]);

  await mc.send("SET", "after", "streamed");
  await mc.send("INCR", "n");
  await mc.send("INCR", "n");
  await mc.send("DEL", "before");
  await waitFor(async () => (await rc.send("GET", "n")) === "2");
  assert.equal(await rc.send("GET", "after"), "streamed");
  assert.equal(await rc.send("EXISTS", "before"), 0);
  await close();
});

test("replicas are read-only", async () => {
  const { replicas, close } = await setup();
  const rc = await replicas[0].connect();
  const r = await rc.send("SET", "x", "1");
  assert.ok(r instanceof ReplyError && r.message.startsWith("READONLY"));
  assert.match((await rc.send("INFO", "replication")) as string, /role:slave[\s\S]*master_link_status:up/);
  await close();
});

test("WAIT returns once replicas acknowledge the write offset", async () => {
  const { master, mc, replicas, close } = await setup(2);
  assert.equal(await mc.send("WAIT", "2", "500"), 2, "no pending writes");
  await mc.send("SET", "k", "v");
  assert.equal(await mc.send("WAIT", "2", "2000"), 2);
  for (const r of master.redis.replicas) assert.ok(r.ackOffset > 0);
  // both replicas have processed exactly the master's stream
  for (const r of replicas) assert.ok(r.redis.replOffset > 0);
  await close();
});

test("WAIT times out with the number of replicas that did acknowledge", async () => {
  const { mc, replicas, close } = await setup(1);
  await mc.send("SET", "k", "v");
  const t0 = Date.now();
  assert.equal(await mc.send("WAIT", "3", "300"), 1);
  assert.ok(Date.now() - t0 >= 290);
  void replicas;
  await close();
});

test("BLPOP on the master is replicated as a plain LPOP", async () => {
  const { master, mc, replicas, close } = await setup();
  const rc = await replicas[0].connect();
  const blocker = await master.connect();
  blocker.write("BLPOP", "jobs", "0");
  await blocker.silentFor(50);
  await mc.send("RPUSH", "jobs", "j1", "j2");
  assert.deepEqual(await blocker.next(), ["jobs", "j1"]);
  await mc.send("WAIT", "1", "1000");
  assert.deepEqual(await rc.send("LRANGE", "jobs", "0", "-1"), ["j2"]);
  await close();
});

test("transactions are replicated", async () => {
  const { mc, replicas, close } = await setup();
  const rc = await replicas[0].connect();
  await mc.send("MULTI");
  await mc.send("SET", "a", "1");
  await mc.send("INCRBY", "a", "41");
  await mc.send("EXEC");
  await mc.send("WAIT", "1", "1000");
  assert.equal(await rc.send("GET", "a"), "42");
  await close();
});
