import { crc64 } from "./crc64";
import { lzfDecompress } from "./lzf";
import { decodeListpack, decodeZiplist } from "./encodings";
import type { Entry } from "./store";
import { Deque } from "./deque";

/**
 * Reader/writer for Redis's binary RDB snapshot format.
 *
 * Files written here load in real redis-server, and string keys from real
 * Redis dumps (including int-encoded and LZF-compressed strings) load here.
 *
 * Layout:  "REDIS" + version | aux fields | SELECTDB | RESIZEDB |
 *          [expiry] type key value ... | EOF | CRC64 (little-endian)
 */

const RDB_VERSION = "0009"; // loadable by every Redis release since 5.0

const OP = {
  MODULE_AUX: 0xf7,
  IDLE: 0xf8,
  FREQ: 0xf9,
  AUX: 0xfa,
  RESIZEDB: 0xfb,
  EXPIRETIME_MS: 0xfc,
  EXPIRETIME: 0xfd,
  SELECTDB: 0xfe,
  EOF: 0xff,
} as const;

const TYPE_STRING = 0;
const TYPE_LIST = 1; // what we write: simple length-prefixed list
const TYPE_LIST_ZIPLIST = 10; // Redis 2.x/3.x
const TYPE_LIST_QUICKLIST = 14; // Redis 3.2 - 6.x: quicklist of ziplists
const TYPE_LIST_QUICKLIST_2 = 18; // Redis 7+: quicklist of listpacks
const QUICKLIST_NODE_PLAIN = 1;

// ---------------------------------------------------------------- writing

function encodeLength(n: number): Buffer {
  if (n < 1 << 6) return Buffer.from([n]);
  if (n < 1 << 14) return Buffer.from([0x40 | (n >> 8), n & 0xff]);
  const b = Buffer.alloc(5);
  b[0] = 0x80;
  b.writeUInt32BE(n, 1);
  return b;
}

function encodeString(s: string): Buffer {
  const data = Buffer.from(s, "utf8");
  return Buffer.concat([encodeLength(data.length), data]);
}

export function encodeRdb(entries: Iterable<[string, Entry]>): Buffer {
  const items = [...entries];
  const parts: Buffer[] = [Buffer.from("REDIS" + RDB_VERSION, "ascii")];

  const aux = (k: string, v: string) => parts.push(Buffer.from([OP.AUX]), encodeString(k), encodeString(v));
  aux("redis-ver", "7.0.0");
  aux("redis-bits", "64");
  aux("ctime", String(Math.floor(Date.now() / 1000)));

  parts.push(Buffer.from([OP.SELECTDB]), encodeLength(0));
  const withTtl = items.filter(([, e]) => e.expiresAt !== null).length;
  parts.push(Buffer.from([OP.RESIZEDB]), encodeLength(items.length), encodeLength(withTtl));

  for (const [key, e] of items) {
    if (e.expiresAt !== null) {
      const ts = Buffer.alloc(9);
      ts[0] = OP.EXPIRETIME_MS;
      ts.writeBigUInt64LE(BigInt(e.expiresAt), 1);
      parts.push(ts);
    }
    if (e.type === "string") {
      parts.push(Buffer.from([TYPE_STRING]), encodeString(key), encodeString(e.value));
    } else {
      parts.push(Buffer.from([TYPE_LIST]), encodeString(key), encodeLength(e.value.length));
      for (const item of e.value) parts.push(encodeString(item));
    }
  }

  parts.push(Buffer.from([OP.EOF]));
  const body = Buffer.concat(parts);
  const checksum = Buffer.alloc(8);
  checksum.writeBigUInt64LE(crc64(body));
  return Buffer.concat([body, checksum]);
}

// ---------------------------------------------------------------- reading

export class RdbError extends Error {}

class Reader {
  pos = 0;
  constructor(private buf: Buffer) {}

  byte(): number {
    if (this.pos >= this.buf.length) throw new RdbError("unexpected end of file");
    return this.buf[this.pos++];
  }

  bytes(n: number): Buffer {
    if (this.pos + n > this.buf.length) throw new RdbError("unexpected end of file");
    const b = this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return b;
  }

  /** Returns either a plain length or a special string encoding id. */
  length(): { len: number; special: number | null } {
    const first = this.byte();
    switch (first >> 6) {
      case 0:
        return { len: first & 0x3f, special: null };
      case 1:
        return { len: ((first & 0x3f) << 8) | this.byte(), special: null };
      case 2:
        if (first === 0x80) return { len: this.bytes(4).readUInt32BE(0), special: null };
        if (first === 0x81) return { len: Number(this.bytes(8).readBigUInt64BE(0)), special: null };
        throw new RdbError(`bad length encoding 0x${first.toString(16)}`);
      default:
        return { len: 0, special: first & 0x3f };
    }
  }

  plainLength(): number {
    const l = this.length();
    if (l.special !== null) throw new RdbError("expected length, got encoded value");
    return l.len;
  }

  /** Raw string bytes (handles int-encoded and LZF-compressed strings). */
  blob(): Buffer {
    const { len, special } = this.length();
    if (special === null) return this.bytes(len);
    switch (special) {
      case 0:
        return Buffer.from(String(this.bytes(1).readInt8(0)));
      case 1:
        return Buffer.from(String(this.bytes(2).readInt16LE(0)));
      case 2:
        return Buffer.from(String(this.bytes(4).readInt32LE(0)));
      case 3: {
        const clen = this.plainLength();
        const ulen = this.plainLength();
        return lzfDecompress(this.bytes(clen), ulen);
      }
      default:
        throw new RdbError(`unknown string encoding ${special}`);
    }
  }

  string(): string {
    return this.blob().toString("utf8");
  }
}

function readList(r: Reader, type: number): string[] {
  try {
    switch (type) {
      case TYPE_LIST: {
        const n = r.plainLength();
        const list: string[] = [];
        for (let i = 0; i < n; i++) list.push(r.string());
        return list;
      }
      case TYPE_LIST_ZIPLIST:
        return decodeZiplist(r.blob());
      case TYPE_LIST_QUICKLIST: {
        const nodes = r.plainLength();
        const list: string[] = [];
        for (let i = 0; i < nodes; i++) list.push(...decodeZiplist(r.blob()));
        return list;
      }
      default: {
        const nodes = r.plainLength();
        const list: string[] = [];
        for (let i = 0; i < nodes; i++) {
          const container = r.plainLength();
          const blob = r.blob();
          if (container === QUICKLIST_NODE_PLAIN) list.push(blob.toString("utf8"));
          else list.push(...decodeListpack(blob));
        }
        return list;
      }
    }
  } catch (e) {
    if (e instanceof RdbError) throw e;
    throw new RdbError(`corrupt list encoding: ${(e as Error).message}`);
  }
}

export function decodeRdb(buf: Buffer): Map<string, Entry> {
  if (buf.length < 9 || buf.toString("ascii", 0, 5) !== "REDIS") throw new RdbError("not an RDB file");
  const version = Number(buf.toString("ascii", 5, 9));
  const r = new Reader(buf);
  r.pos = 9;

  const out = new Map<string, Entry>();
  const now = Date.now();
  let expiresAt: number | null = null;

  for (;;) {
    const op = r.byte();
    switch (op) {
      case OP.EOF: {
        if (version >= 5 && r.pos + 8 <= buf.length) {
          const stored = buf.readBigUInt64LE(r.pos);
          if (stored !== 0n && stored !== crc64(buf.subarray(0, r.pos))) throw new RdbError("checksum mismatch");
        }
        return out;
      }
      case OP.AUX:
        r.string();
        r.string();
        continue;
      case OP.SELECTDB:
        r.plainLength();
        continue;
      case OP.RESIZEDB:
        r.plainLength();
        r.plainLength();
        continue;
      case OP.EXPIRETIME_MS:
        expiresAt = Number(r.bytes(8).readBigUInt64LE(0));
        continue;
      case OP.EXPIRETIME:
        expiresAt = r.bytes(4).readUInt32LE(0) * 1000;
        continue;
      case OP.IDLE:
        r.plainLength();
        continue;
      case OP.FREQ:
        r.byte();
        continue;
      case OP.MODULE_AUX:
        throw new RdbError("module data is not supported");
    }

    const key = r.string();
    let entry: Entry;
    if (op === TYPE_STRING) {
      entry = { type: "string", value: r.string(), expiresAt };
    } else if (op === TYPE_LIST || op === TYPE_LIST_ZIPLIST || op === TYPE_LIST_QUICKLIST || op === TYPE_LIST_QUICKLIST_2) {
      entry = { type: "list", value: new Deque(readList(r, op)), expiresAt };
    } else {
      throw new RdbError(`unsupported value type ${op} for key '${key}' (only strings and lists are supported)`);
    }
    if (expiresAt === null || expiresAt > now) out.set(key, entry);
    expiresAt = null;
  }
}
