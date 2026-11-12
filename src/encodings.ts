/**
 * Decoders for the compact list encodings real Redis uses inside RDB files:
 *  - listpack  (Redis 7+, inside quicklist v2 nodes)
 *  - ziplist   (Redis 3.2 - 6.x, inside quicklist v1 nodes)
 */

export class EncodingError extends Error {}

export function decodeListpack(buf: Buffer): string[] {
  const out: string[] = [];
  let p = 6; // skip total-bytes (u32) + num-elements (u16)
  for (;;) {
    if (p >= buf.length) throw new EncodingError("listpack: missing terminator");
    const b = buf[p];
    if (b === 0xff) return out;
    const start = p;
    let value: string;

    if ((b & 0x80) === 0) {
      value = String(b & 0x7f); // 7-bit unsigned int
      p += 1;
    } else if ((b & 0xc0) === 0x80) {
      const len = b & 0x3f; // 6-bit string length
      value = buf.toString("utf8", p + 1, p + 1 + len);
      p += 1 + len;
    } else if ((b & 0xe0) === 0xc0) {
      let v = ((b & 0x1f) << 8) | buf[p + 1]; // 13-bit signed int
      if (v >= 1 << 12) v -= 1 << 13;
      value = String(v);
      p += 2;
    } else if ((b & 0xf0) === 0xe0) {
      const len = ((b & 0x0f) << 8) | buf[p + 1]; // 12-bit string length
      value = buf.toString("utf8", p + 2, p + 2 + len);
      p += 2 + len;
    } else {
      switch (b) {
        case 0xf0: {
          const len = buf.readUInt32LE(p + 1);
          value = buf.toString("utf8", p + 5, p + 5 + len);
          p += 5 + len;
          break;
        }
        case 0xf1:
          value = String(buf.readInt16LE(p + 1));
          p += 3;
          break;
        case 0xf2:
          value = String(buf.readIntLE(p + 1, 3));
          p += 4;
          break;
        case 0xf3:
          value = String(buf.readInt32LE(p + 1));
          p += 5;
          break;
        case 0xf4:
          value = buf.readBigInt64LE(p + 1).toString();
          p += 9;
          break;
        default:
          throw new EncodingError(`listpack: unknown encoding byte 0x${b.toString(16)}`);
      }
    }

    // Every entry is followed by a "backlen" (its own size), 1-5 bytes long.
    const entryLen = p - start;
    p += entryLen < 128 ? 1 : entryLen < 16384 ? 2 : entryLen < 2097152 ? 3 : entryLen < 268435456 ? 4 : 5;
    out.push(value);
  }
}

export function decodeZiplist(buf: Buffer): string[] {
  const out: string[] = [];
  let p = 10; // zlbytes (u32) + zltail (u32) + zllen (u16)
  for (;;) {
    if (p >= buf.length) throw new EncodingError("ziplist: missing terminator");
    if (buf[p] === 0xff) return out;
    p += buf[p] === 0xfe ? 5 : 1; // previous-entry length
    const enc = buf[p];

    switch (enc >> 6) {
      case 0: {
        const len = enc & 0x3f;
        out.push(buf.toString("utf8", p + 1, p + 1 + len));
        p += 1 + len;
        continue;
      }
      case 1: {
        const len = ((enc & 0x3f) << 8) | buf[p + 1];
        out.push(buf.toString("utf8", p + 2, p + 2 + len));
        p += 2 + len;
        continue;
      }
      case 2: {
        const len = buf.readUInt32BE(p + 1);
        out.push(buf.toString("utf8", p + 5, p + 5 + len));
        p += 5 + len;
        continue;
      }
    }

    switch (enc) {
      case 0xc0:
        out.push(String(buf.readInt16LE(p + 1)));
        p += 3;
        break;
      case 0xd0:
        out.push(String(buf.readInt32LE(p + 1)));
        p += 5;
        break;
      case 0xe0:
        out.push(buf.readBigInt64LE(p + 1).toString());
        p += 9;
        break;
      case 0xf0:
        out.push(String(buf.readIntLE(p + 1, 3)));
        p += 4;
        break;
      case 0xfe:
        out.push(String(buf.readInt8(p + 1)));
        p += 2;
        break;
      default:
        if (enc >= 0xf1 && enc <= 0xfd) {
          out.push(String((enc & 0x0f) - 1)); // 4-bit immediate 0..12
          p += 1;
        } else throw new EncodingError(`ziplist: unknown encoding byte 0x${enc.toString(16)}`);
    }
  }
}
