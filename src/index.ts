#!/usr/bin/env node
import { parseArgs } from "./config";
import { RedisServer } from "./server";
import { createTcpServer } from "./network";
import { startReplication } from "./replication";

function main(): void {
  let config;
  try {
    config = parseArgs(process.argv.slice(2));
  } catch (e) {
    console.error((e as Error).message);
    process.exit(1);
  }

  const server = new RedisServer(config);

  if (server.role === "master") {
    try {
      const n = server.loadFromDisk();
      if (n > 0) console.log(`DB loaded from ${server.rdbPath()}: ${n} keys`);
    } catch (e) {
      console.error(`Failed to load ${server.rdbPath()}: ${(e as Error).message}`);
      process.exit(1);
    }
  }

  const tcp = createTcpServer(server);
  tcp.listen(config.port, () => {
    console.log(`redis-ts ${server.role} listening on port ${config.port}`);
    if (config.replicaof) {
      console.log(`[replica] connecting to master ${config.replicaof.host}:${config.replicaof.port}`);
      startReplication(server, config.replicaof.host, config.replicaof.port);
    }
  });

  // Periodic snapshots, like "save 60 1" in redis.conf.
  if (config.save > 0 && server.role === "master") {
    setInterval(() => {
      if (server.dirty > 0) server.saveInBackground().catch((e) => console.error("Auto-save failed:", e));
    }, config.save * 1000).unref();
  }

  const shutdown = () => {
    if (config.save > 0 && server.role === "master" && server.dirty > 0) {
      server.saveSync();
      console.log("DB saved on disk");
    }
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main();
