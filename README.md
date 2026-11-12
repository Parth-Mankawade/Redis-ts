# redis-ts

A Redis-compatible in-memory data store written from scratch in TypeScript on raw TCP sockets: no Redis libraries, no runtime dependencies.

It speaks the real Redis wire protocol, so the official `redis-cli`, `redis-benchmark` and client libraries work against it unchanged. It also **interoperates with real Redis**: real `redis-server` loads its snapshot files, real Redis replicas can sync from it, and it can run as a replica of a real Redis master.

## Features

| Area | What's implemented |
|---|---|
| **Protocol** | RESP2 parser that handles partial TCP reads, pipelining, binary-safe values and inline commands |
| **Strings** | `SET` (`EX` `PX` `EXAT` `PXAT` `NX` `XX` `GET` `KEEPTTL`), `GET`, `GETDEL`, `MSET`, `MGET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN` |
| **Keys & expiry** | `DEL`, `EXISTS`, `KEYS` (glob patterns), `TYPE`, `RENAME`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `DBSIZE`, `FLUSHALL` |
| **Lists** | `LPUSH`, `RPUSH`, `LPOP`, `RPOP` (with count), `LRANGE`, `LINDEX`, `LSET`, `LLEN`, and blocking `BLPOP` with timeouts and FIFO fairness |
| **Transactions** | `MULTI`, `EXEC`, `DISCARD`, with queue-time error detection (`EXECABORT`) |
| **Pub/Sub** | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, subscribed-mode command restrictions |
| **Persistence** | Binary RDB snapshots: `SAVE`, `BGSAVE`, `LASTSAVE`, periodic auto-save, save on shutdown, load on startup, atomic file replacement, CRC64 checksums |
| **Replication** | Master–replica with full resync (`PSYNC`), command propagation, offset tracking, `REPLCONF ACK/GETACK`, `WAIT`, read-only replicas, automatic reconnect |
| **Server** | `PING`, `ECHO`, `INFO`, `CONFIG GET`, `SELECT`, `COMMAND`, `QUIT` |

## Quick start

```bash
npm install
npm run build
npm start                 # listens on 6379

redis-cli SET user:1 parth EX 60
redis-cli GET user:1
redis-cli RPUSH jobs a b c
redis-cli LRANGE jobs 0 -1
```

Options:

```
--port N               TCP port (default 6379)
--dir PATH             directory for the RDB file (default: cwd)
--dbfilename FILE      RDB file name (default dump.rdb)
--save SECONDS         snapshot every N seconds if there were writes; 0 disables (default 60)
--replicaof "HOST PORT"  run as a replica of the given master
```

Run a master and a replica:

```bash
node dist/index.js --port 6379
node dist/index.js --port 6380 --replicaof "127.0.0.1 6379"

redis-cli -p 6379 SET greeting hello
redis-cli -p 6379 WAIT 1 1000       # -> 1 (replica acknowledged)
redis-cli -p 6380 GET greeting      # -> "hello"
redis-cli -p 6380 SET x 1           # -> READONLY error
```

Docker:

```bash
docker build -t redis-ts .
docker run -p 6379:6379 -v $PWD/data:/data redis-ts
```

## Tests

```bash
npm test
```

47 tests cover the protocol parser, every command family, expiry (lazy and active), blocking pops, transactions, pub/sub, persistence across restarts, and multi-replica replication with `WAIT`. Replication tests spin up real master and replica servers over TCP. The RDB tests include a `dump.rdb` produced by real `redis-server` 7.0.

## Compatibility with real Redis

Verified against Redis 7.0.15:

- `redis-cli` and `redis-benchmark` work unchanged.
- A `dump.rdb` written by redis-ts loads in real `redis-server`.
- A `dump.rdb` written by real Redis loads in redis-ts. This includes LZF-compressed strings, integer-encoded strings, and lists in Redis's compact `quicklist`/`listpack` encodings (and the older `ziplist` encoding from Redis 6 and earlier).
- A real Redis replica can sync from a redis-ts master, and redis-ts can replicate from a real Redis master (strings and lists; other data types are reported as unsupported instead of crashing).

## Benchmarks

`redis-benchmark -n 100000 -c 50` on a 2-core cloud VM:

| Command | redis-ts | real Redis 7.0 (same machine) |
|---|---|---|
| GET | ~108k ops/s | ~114k ops/s |
| SET | ~63k ops/s | ~95k ops/s |
| LPUSH / LPOP | ~77k / ~101k ops/s | — |
| GET, pipelined ×16 | ~406k ops/s | — |

## How it works

```
src/
  index.ts        CLI entry point: config, startup load, auto-save, graceful shutdown
  network.ts      TCP server: one Client state object per connection
  server.ts       core: command dispatch, transactions, blocking, pub/sub, replication, persistence
  commands.ts     command table (arity, write flag) and handlers
  resp.ts         RESP parser and reply encoders
  store.ts        keyspace with lazy and active expiry
  deque.ts        ring-buffer deque used for lists
  rdb.ts          RDB snapshot reader and writer
  encodings.ts    listpack / ziplist decoders (for files from real Redis)
  crc64.ts, lzf.ts  checksum and decompression used by RDB
  replication.ts  replica-side handshake and replication stream
```

### Key design decisions

**Partial reads and pipelining.** TCP is a byte stream, so one `data` event can hold half a command or fifty of them. Each connection keeps its own input buffer. The parser returns `null` until a full command has arrived and reports exactly how many bytes it consumed. Replies for a batch of pipelined commands are joined into a single socket write, which made pipelined throughput about 3× higher than writing each reply separately.

**Atomicity without locks.** Node runs JavaScript on one thread, so every command runs to completion before the next one starts. This is the same model Redis uses. `EXEC` runs its whole queue in a single synchronous pass, so no other client's command can interleave.

**Expiry.** Same two-part strategy as Redis:
- *Lazy:* every read checks the deadline and deletes the key if it has passed.
- *Active:* every 100 ms, a sweep samples 20 keys that have a TTL and deletes the expired ones. It repeats while more than 25% of a sample was expired, with a 25 ms time budget per tick. This reclaims memory for keys nobody reads again without ever scanning the whole keyspace.

**Lists as a ring-buffer deque.** `Array.unshift`/`shift` are O(n) in JavaScript. Using them made LPUSH/LPOP on a large list about 7× slower. A power-of-two ring buffer gives O(1) push/pop at both ends.

**Blocking commands.** A client running `BLPOP` is put in a per-key FIFO wait queue, and its input is paused so later pipelined commands wait their turn. When a push makes a key ready, waiters are served only after the push has been propagated to replicas. Real Redis does the same ("ready keys"). Otherwise a replica would receive the pop before the push.

**Replication.** The replica performs the standard handshake: `PING`, then `REPLCONF listening-port`, then `REPLCONF capa`, then `PSYNC ? -1`. The master answers `+FULLRESYNC <replid> <offset>`, then sends an RDB snapshot (`$<len>\r\n<bytes>`, with no trailing CRLF), then streams every write command.
- Both sides count bytes in the replication stream (the *offset*). `WAIT` sends `REPLCONF GETACK *` and blocks until enough replicas report an offset at least as large as the master's offset when `WAIT` was called.
- `BLPOP` is propagated as a plain `LPOP`, because a replica must never block.

**Persistence.** Snapshots are written to a temp file and then `rename`d over the old one, which is atomic, so a crash mid-save never leaves a corrupt `dump.rdb`. `BGSAVE` serialises the dataset in memory synchronously (a consistent point-in-time snapshot), then writes it to disk asynchronously without blocking clients. Files use RDB version 9, which every Redis release since 5.0 can load, and end with the CRC-64/Jones checksum Redis expects.

## Limitations

- Supports the string and list data types (no hashes, sets, sorted sets or streams yet).
- Replication supports full resync only; partial resync from a replication backlog is not implemented.
- Single database (`SELECT 0`).

## License

MIT
