/** LZF decompression, used by Redis to compress long strings inside RDB files. */
export function lzfDecompress(input: Buffer, outLen: number): Buffer {
  const out = Buffer.alloc(outLen);
  let ip = 0;
  let op = 0;
  while (ip < input.length) {
    const ctrl = input[ip++];
    if (ctrl < 32) {
      // literal run of ctrl + 1 bytes
      const len = ctrl + 1;
      input.copy(out, op, ip, ip + len);
      ip += len;
      op += len;
    } else {
      // back-reference
      let len = ctrl >> 5;
      if (len === 7) len += input[ip++];
      let ref = op - ((ctrl & 0x1f) << 8) - 1 - input[ip++];
      len += 2;
      if (ref < 0) throw new Error("LZF: invalid back-reference");
      while (len--) out[op++] = out[ref++];
    }
  }
  if (op !== outLen) throw new Error("LZF: length mismatch");
  return out;
}
