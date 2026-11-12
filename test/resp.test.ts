import { test } from "node:test";
import assert from "node:assert/strict";
import { parseCommand, ProtocolError, array, bulk, rawArray, integer } from "../src/resp";

const enc = (...args: string[]) => Buffer.from(array(args));

test("parses a complete command", () => {
  const r = parseCommand(enc("SET", "key", "value"));
  assert.deepEqual(r?.args, ["SET", "key", "value"]);
  assert.equal(r?.consumed, enc("SET", "key", "value").length);
});

test("returns null for every incomplete prefix of a command", () => {
  const full = enc("SET", "key", "hello world");
  for (let i = 0; i < full.length; i++) assert.equal(parseCommand(full.subarray(0, i)), null, `prefix ${i}`);
});

test("parses pipelined commands one at a time", () => {
  let buf = Buffer.concat([enc("PING"), enc("ECHO", "hi"), enc("GET", "k")]);
  const seen: string[][] = [];
  let r;
  while ((r = parseCommand(buf))) {
    seen.push(r.args);
    buf = buf.subarray(r.consumed);
  }
  assert.deepEqual(seen, [["PING"], ["ECHO", "hi"], ["GET", "k"]]);
  assert.equal(buf.length, 0);
});

test("handles binary-safe values containing CRLF and multi-byte UTF-8", () => {
  const r = parseCommand(enc("SET", "k", "line1\r\nline2 — ✓"));
  assert.deepEqual(r?.args, ["SET", "k", "line1\r\nline2 — ✓"]);
});

test("parses inline commands", () => {
  assert.deepEqual(parseCommand(Buffer.from("PING\r\n"))?.args, ["PING"]);
  assert.deepEqual(parseCommand(Buffer.from("SET  a   b\r\n"))?.args, ["SET", "a", "b"]);
});

test("rejects malformed input", () => {
  assert.throws(() => parseCommand(Buffer.from("*x\r\n")), ProtocolError);
  assert.throws(() => parseCommand(Buffer.from("*1\r\n:5\r\n")), ProtocolError);
});

test("encodes replies", () => {
  assert.equal(bulk(null), "$-1\r\n");
  assert.equal(bulk("✓"), "$3\r\n✓\r\n");
  assert.equal(rawArray([integer(1), bulk("a")]), "*2\r\n:1\r\n$1\r\na\r\n");
});
