/**
 * CRC-64/Jones (reflected), the checksum Redis appends to RDB files.
 * Check value: crc64("123456789") === 0xe9c6d914c4b8d9can
 */
const POLY = 0x95ac9329ac4bc9b5n; // reflected form of 0xad93d23594c935a9
const MASK = 0xffffffffffffffffn;

const TABLE: bigint[] = (() => {
  const t: bigint[] = [];
  for (let i = 0; i < 256; i++) {
    let crc = BigInt(i);
    for (let j = 0; j < 8; j++) crc = crc & 1n ? (crc >> 1n) ^ POLY : crc >> 1n;
    t.push(crc & MASK);
  }
  return t;
})();

export function crc64(data: Uint8Array, crc = 0n): bigint {
  for (const byte of data) crc = TABLE[Number((crc ^ BigInt(byte)) & 0xffn)] ^ (crc >> 8n);
  return crc & MASK;
}
