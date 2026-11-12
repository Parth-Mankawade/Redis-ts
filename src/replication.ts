import net from "node:net";
import type { RedisServer } from "./server";
import * as R from "./resp";

/**
 * Replica side of master-replica replication.
 *
 * Handshake:  PING -> REPLCONF listening-port -> REPLCONF capa psync2 -> PSYNC ? -1
 * The master answers +FULLRESYNC <replid> <offset>, then sends its dataset as
 * "$<len>\r\n<rdb bytes>" (no trailing CRLF), then streams every write command.
 * The replica applies those commands silently and tracks how many bytes it has
 * processed, which it reports back when the master asks with REPLCONF GETACK.
 */
export interface ReplicationLink {
  stop(): void;
}

export function startReplication(server: RedisServer, host: string, port: number): ReplicationLink {
  let stopped = false;
  let socket: net.Socket;

  const connect = () => {
    socket = net.connect(port, host);
    runLink(server, socket, () => {
      if (!stopped) setTimeout(connect, 1000); // reconnect after the link drops
    });
  };
  connect();

  return {
    stop() {
      stopped = true;
      socket.destroy();
    },
  };
}

function runLink(server: RedisServer, socket: net.Socket, onClose: () => void): void {
  const client = server.newClient(socket, true);
  type Phase = "PONG" | "PORT" | "CAPA" | "PSYNC" | "RDB" | "STREAM";
  let phase: Phase = "PONG";
  let buf = Buffer.alloc(0);

  const send = (...args: string[]) => socket.write(R.array(args));

  socket.on("connect", () => send("PING"));

  socket.on("data", (chunk) => {
    if (phase === "STREAM") return server.onData(client, chunk);
    buf = Buffer.concat([buf, chunk]);

    for (;;) {
      if (phase === "RDB") {
        const end = buf.indexOf("\r\n");
        if (end === -1) return;
        if (buf[0] !== 0x24 /* '$' */) return fail(`expected RDB payload, got '${buf.toString("utf8", 0, end)}'`);
        const len = Number(buf.toString("utf8", 1, end));
        if (buf.length < end + 2 + len) return; // wait for the full snapshot
        const rdb = buf.subarray(end + 2, end + 2 + len);
        buf = buf.subarray(end + 2 + len);
        try {
          server.loadFromMaster(rdb, pendingReplid, pendingOffset);
        } catch (e) {
          return fail(`could not load snapshot from master: ${(e as Error).message}`);
        }
        console.log(`[replica] full resync complete: ${server.store.size()} keys loaded from master`);
        phase = "STREAM";
        // Like real replicas, report our offset every second so the master can track lag.
        ackTimer = setInterval(() => send("REPLCONF", "ACK", String(server.replOffset)), 1000);
        ackTimer.unref();
        if (buf.length) server.onData(client, buf);
        return;
      }

      const end = buf.indexOf("\r\n");
      if (end === -1) return;
      const line = buf.toString("utf8", 0, end);
      buf = buf.subarray(end + 2);
      if (line.startsWith("-")) return fail(line.slice(1));

      switch (phase) {
        case "PONG":
          send("REPLCONF", "listening-port", String(server.config.port));
          phase = "PORT";
          break;
        case "PORT":
          send("REPLCONF", "capa", "psync2");
          phase = "CAPA";
          break;
        case "CAPA":
          send("PSYNC", "?", "-1");
          phase = "PSYNC";
          break;
        case "PSYNC": {
          const [kind, replid, offset] = line.slice(1).split(" ");
          if (kind !== "FULLRESYNC") return fail(`unsupported sync reply: ${line}`);
          pendingReplid = replid;
          pendingOffset = Number(offset);
          phase = "RDB";
          break;
        }
      }
    }
  });

  let ackTimer: NodeJS.Timeout | null = null;
  let pendingReplid = "";
  let pendingOffset = 0;

  function fail(reason: string): void {
    console.error(`[replica] replication failed: ${reason}`);
    socket.destroy();
  }

  socket.on("error", (e) => console.error(`[replica] master link error: ${e.message}`));
  socket.on("close", () => {
    if (ackTimer) clearInterval(ackTimer);
    server.masterLinkUp = false;
    server.removeClient(client);
    onClose();
  });
}
