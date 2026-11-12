/**
 * Double-ended queue backed by a growable ring buffer.
 *
 * Redis lists need O(1) push/pop at both ends (LPUSH, RPUSH, LPOP, RPOP, BLPOP).
 * A plain JS array gives O(1) only at the tail: `unshift`/`shift` copy the whole
 * array, which made LPUSH/LPOP on a large list ~7x slower before this was added.
 */
export class Deque<T> implements Iterable<T> {
  private buf: (T | undefined)[];
  private head = 0;
  private len = 0;

  constructor(items: Iterable<T> = []) {
    this.buf = new Array(16);
    for (const x of items) this.pushBack(x);
  }

  get length(): number {
    return this.len;
  }

  private get mask(): number {
    return this.buf.length - 1; // capacity is always a power of two
  }

  private grow(): void {
    const next = new Array<T | undefined>(this.buf.length * 2);
    for (let i = 0; i < this.len; i++) next[i] = this.buf[(this.head + i) & this.mask];
    this.buf = next;
    this.head = 0;
  }

  pushBack(x: T): void {
    if (this.len === this.buf.length) this.grow();
    this.buf[(this.head + this.len) & this.mask] = x;
    this.len++;
  }

  pushFront(x: T): void {
    if (this.len === this.buf.length) this.grow();
    this.head = (this.head - 1) & this.mask;
    this.buf[this.head] = x;
    this.len++;
  }

  popFront(): T | undefined {
    if (this.len === 0) return undefined;
    const x = this.buf[this.head];
    this.buf[this.head] = undefined; // let GC reclaim it
    this.head = (this.head + 1) & this.mask;
    this.len--;
    return x;
  }

  popBack(): T | undefined {
    if (this.len === 0) return undefined;
    const i = (this.head + this.len - 1) & this.mask;
    const x = this.buf[i];
    this.buf[i] = undefined;
    this.len--;
    return x;
  }

  get(i: number): T | undefined {
    return i >= 0 && i < this.len ? this.buf[(this.head + i) & this.mask] : undefined;
  }

  set(i: number, x: T): void {
    if (i < 0 || i >= this.len) throw new RangeError("index out of range");
    this.buf[(this.head + i) & this.mask] = x;
  }

  /** Elements in [start, end). */
  slice(start: number, end: number): T[] {
    const out: T[] = [];
    for (let i = Math.max(0, start); i < Math.min(end, this.len); i++) out.push(this.buf[(this.head + i) & this.mask] as T);
    return out;
  }

  toArray(): T[] {
    return this.slice(0, this.len);
  }

  *[Symbol.iterator](): Iterator<T> {
    for (let i = 0; i < this.len; i++) yield this.buf[(this.head + i) & this.mask] as T;
  }
}
