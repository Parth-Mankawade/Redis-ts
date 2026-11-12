import { test } from "node:test";
import assert from "node:assert/strict";
import { encodeRdb, decodeRdb, RdbError } from "../src/rdb";
import { crc64 } from "../src/crc64";
import { globToRegExp } from "../src/glob";
import type { Entry } from "../src/store";
import { Deque } from "../src/deque";

const plain = (m: Map<string, Entry>) =>
  [...m.entries()].map(([k, e]) => [k, { ...e, value: typeof e.value === "string" ? e.value : e.value.toArray() }]);
const listOf = (e: Entry | undefined) => (e && e.type === "list" ? e.value.toArray() : undefined);

test("crc64 matches the Redis (Jones) check value", () => {
  assert.equal(crc64(Buffer.from("123456789")), 0xe9c6d914c4b8d9can);
});

test("round-trips strings, lists and expiries", () => {
  const future = Date.now() + 60_000;
  const long = "x".repeat(20_000); // exercises the 32-bit length encoding
  const entries: [string, Entry][] = [
    ["s", { type: "string", value: "hello", expiresAt: null }],
    ["ttl", { type: "string", value: "soon", expiresAt: future }],
    ["l", { type: "list", value: new Deque(["a", "b", "c"]), expiresAt: null }],
    ["long", { type: "string", value: long, expiresAt: null }],
    ["mid", { type: "string", value: "y".repeat(100), expiresAt: null }],
  ];
  const back = decodeRdb(encodeRdb(entries));
  assert.deepEqual(plain(back), plain(new Map(entries)));
});

test("skips keys that already expired", () => {
  const rdb = encodeRdb([["old", { type: "string", value: "v", expiresAt: Date.now() - 1000 }]]);
  assert.equal(decodeRdb(rdb).size, 0);
});

test("detects corruption via checksum", () => {
  const rdb = encodeRdb([["k", { type: "string", value: "value", expiresAt: null }]]);
  const i = rdb.indexOf("value");
  rdb[i] = "X".charCodeAt(0);
  assert.throws(() => decodeRdb(rdb), /checksum/);
});

test("reads int-encoded strings and second-precision expiry", () => {
  // header, SELECTDB 0, EXPIRETIME (secs), string type, key "n", int16 value 1000, EOF, zero checksum
  const expSecs = Math.floor(Date.now() / 1000) + 3600;
  const exp = Buffer.alloc(4);
  exp.writeUInt32LE(expSecs);
  const buf = Buffer.concat([
    Buffer.from("REDIS0009"),
    Buffer.from([0xfe, 0x00, 0xfd]),
    exp,
    Buffer.from([0x00, 0x01]),
    Buffer.from("n"),
    Buffer.from([0xc1, 0xe8, 0x03]),
    Buffer.from([0xff]),
    Buffer.alloc(8),
  ]);
  const out = decodeRdb(buf);
  assert.deepEqual(out.get("n"), { type: "string", value: "1000", expiresAt: expSecs * 1000 });
});

test("rejects non-RDB input", () => {
  assert.throws(() => decodeRdb(Buffer.from("hello world")), RdbError);
});

test("glob patterns follow Redis KEYS semantics", () => {
  const m = (p: string, s: string) => globToRegExp(p).test(s);
  assert.ok(m("user:*", "user:42"));
  assert.ok(!m("user:*", "session:1"));
  assert.ok(m("h?llo", "hello"));
  assert.ok(m("h[ae]llo", "hallo") && !m("h[ae]llo", "hillo"));
  assert.ok(m("h[^e]llo", "hallo") && !m("h[^e]llo", "hello"));
  assert.ok(m("a\\*b", "a*b") && !m("a\\*b", "axb"));
});

// ---- Compatibility with files produced by real Redis ----

import fs from "node:fs";
import path from "node:path";
import { decodeZiplist } from "../src/encodings";

test("loads a dump.rdb produced by real Redis 7.0 (quicklist/listpack lists, LZF, int strings)", () => {
  // Fixture created with redis-server 7.0.15; see README "Compatibility".
  const data = decodeRdb(fs.readFileSync(path.join(__dirname, "fixtures", "redis-7.0.rdb")));
  const mixed = [
    "0", "7", "127", "128", "-1", "4095", "-4096", "4096", "32767", "-32768", "40000", "8388607", "-8388608",
    "2147483647", "-2147483648", "9007199254740991", "-9007199254740991", "hello", "", "✓ unicode",
    "a".repeat(63), "b".repeat(64), "c".repeat(4095), "d".repeat(5000),
  ];
  const big = Array.from({ length: 2000 }, (_, i) => `item-${i}-` + "x".repeat(40));

  assert.deepEqual(listOf(data.get("mixed")), mixed);
  assert.deepEqual(listOf(data.get("big")), big);
  assert.equal(data.get("str")?.value, "plain");
  assert.equal(data.get("int")?.value, "123456789");
  assert.equal(data.get("lzf")?.value, "abcabc".repeat(1000));
  assert.equal(data.get("ttl")?.expiresAt, 4102444800000);
});

test("decodes ziplists (Redis <= 6 list encoding)", () => {
  // entries: "hi", 5 (4-bit immediate), -2 (int8), 1000 (int16), 70000 (int24)
  const entries = Buffer.from([
    0x00, 0x02, 0x68, 0x69,
    0x04, 0xf6,
    0x01, 0xfe, 0xfe,
    0x02, 0xc0, 0xe8, 0x03,
    0x03, 0xf0, 0x70, 0x11, 0x01,
  ]);
  const header = Buffer.alloc(10);
  const zl = Buffer.concat([header, entries, Buffer.from([0xff])]);
  assert.deepEqual(decodeZiplist(zl), ["hi", "5", "-2", "1000", "70000"]);
});

test("deque supports O(1) operations at both ends and grows correctly", () => {
  const d = new Deque<number>();
  for (let i = 0; i < 100; i++) i % 2 ? d.pushBack(i) : d.pushFront(i);
  const expected = [...Array.from({ length: 50 }, (_, i) => 98 - 2 * i), ...Array.from({ length: 50 }, (_, i) => 2 * i + 1)];
  assert.deepEqual(d.toArray(), expected);
  assert.equal(d.popFront(), 98);
  assert.equal(d.popBack(), 99);
  assert.equal(d.get(0), 96);
  assert.deepEqual(d.slice(0, 3), [96, 94, 92]);
  d.set(0, -1);
  assert.equal(d.get(0), -1);
  assert.equal(d.length, 98);
  while (d.length) d.popBack();
  assert.equal(d.popFront(), undefined);
});
