import net from "node:net";
import type { RedisServer } from "./server";

/** Wires TCP connections to the server. Each socket gets its own client state and input buffer. */
export function createTcpServer(server: RedisServer): net.Server {
  return net.createServer((socket) => {
    socket.setNoDelay(true);
    const client = server.newClient(socket);
    socket.on("data", (chunk) => server.onData(client, chunk));
    socket.on("close", () => server.removeClient(client));
    socket.on("error", () => socket.destroy());
  });
}
