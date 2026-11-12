/**
 * RESP2 (REdis Serialization Protocol) parsing and encoding.
 *
 * TCP is a byte stream: a single `data` event may contain half a command or
 * several pipelined commands. `parseCommand` therefore returns `null` when the
 * buffer does not yet hold a complete command, and reports how many bytes it
 * consumed so the caller can slice them off and keep the remainder.
 */

export interface ParsedCommand {
  args: string[];
  consumed: number;
}

const CRLF = "\r\n";
const MAX_BULK = 512 * 1024 * 1024;

export class ProtocolError extends Error {}

function readLine(buf: Buffer, start: number): { line: string; next: number } | null {
  const end = buf.indexOf(CRLF, start);
  if (end === -1) return null;
  return { line: buf.toString("utf8", start, end), next: end + 2 };
}

export function parseCommand(buf: Buffer): ParsedCommand | null {
  if (buf.length === 0) return null;

  // Inline commands ("PING\r\n"), used by telnet / netcat.
  if (buf[0] !== 0x2a /* '*' */) {
    const l = readLine(buf, 0);
    if (!l) return null;
    return { args: l.line.trim().split(/\s+/).filter(Boolean), consumed: l.next };
  }

  const header = readLine(buf, 0);
  if (!header) return null;
  const count = Number(header.line.slice(1));
  if (!Number.isInteger(count)) throw new ProtocolError("invalid multibulk length");

  let pos = header.next;
  const args: string[] = [];
  for (let i = 0; i < count; i++) {
    const lenLine = readLine(buf, pos);
    if (!lenLine) return null;
    if (lenLine.line[0] !== "$") throw new ProtocolError(`expected '$', got '${lenLine.line[0]}'`);
    const len = Number(lenLine.line.slice(1));
    if (!Number.isInteger(len) || len < 0 || len > MAX_BULK) throw new ProtocolError("invalid bulk length");
    const dataEnd = lenLine.next + len;
    if (buf.length < dataEnd + 2) return null; // wait for the rest of the payload
    args.push(buf.toString("utf8", lenLine.next, dataEnd));
    pos = dataEnd + 2;
  }
  return { args, consumed: pos };
}

// ---- Encoders: every reply is built as an already-encoded RESP string ----

export const simple = (s: string): string => `+${s}${CRLF}`;
export const error = (s: string): string => `-${s}${CRLF}`;
export const integer = (n: number): string => `:${n}${CRLF}`;
export const bulk = (s: string | null): string =>
  s === null ? `$-1${CRLF}` : `$${Buffer.byteLength(s)}${CRLF}${s}${CRLF}`;
export const nullArray = (): string => `*-1${CRLF}`;
export const array = (items: (string | null)[]): string =>
  `*${items.length}${CRLF}${items.map(bulk).join("")}`;
/** Array whose elements are already RESP-encoded (used for nested / mixed replies). */
export const rawArray = (encoded: string[]): string => `*${encoded.length}${CRLF}${encoded.join("")}`;
