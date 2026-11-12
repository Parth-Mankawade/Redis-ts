import { RedisError, WRONGTYPE } from "./errors";
import { globToRegExp } from "./glob";
import { Deque } from "./deque";

export type Value = { type: "string"; value: string } | { type: "list"; value: Deque<string> };
export type Entry = Value & { expiresAt: number | null };

/**
 * In-memory keyspace.
 *
 * Expiry works the same way as in Redis:
 *  - Lazy: every read checks the deadline and deletes the key if it has passed.
 *  - Active: a periodic sweep samples keys that have a TTL and deletes the
 *    expired ones, repeating while more than 25% of a sample was expired, so
 *    memory is reclaimed even for keys that are never read again.
 */
export class Store {
  private data = new Map<string, Entry>();
  private volatile = new Set<string>(); // keys that have a TTL
  private cursor: Iterator<string> = this.volatile.values();
  private timer: NodeJS.Timeout;

  constructor(sweepIntervalMs = 100) {
    this.timer = setInterval(() => this.activeExpire(), sweepIntervalMs);
    this.timer.unref();
  }

  stop(): void {
    clearInterval(this.timer);
  }

  get(key: string): Entry | undefined {
    const e = this.data.get(key);
    if (e && e.expiresAt !== null && e.expiresAt <= Date.now()) {
      this.delete(key);
      return undefined;
    }
    return e;
  }

  has(key: string): boolean {
    return this.get(key) !== undefined;
  }

  getString(key: string): string | null {
    const e = this.get(key);
    if (!e) return null;
    if (e.type !== "string") throw new RedisError(WRONGTYPE);
    return e.value;
  }

  /** Returns the list stored at key; creates an empty one when `create` is set. */
  getList(key: string, create = false): Deque<string> | null {
    const e = this.get(key);
    if (!e) {
      if (!create) return null;
      const list = new Deque<string>();
      this.data.set(key, { type: "list", value: list, expiresAt: null });
      return list;
    }
    if (e.type !== "list") throw new RedisError(WRONGTYPE);
    return e.value;
  }

  set(key: string, entry: Entry): void {
    this.data.set(key, entry);
    if (entry.expiresAt !== null) this.volatile.add(key);
    else this.volatile.delete(key);
  }

  setString(key: string, value: string, expiresAt: number | null = null): void {
    this.set(key, { type: "string", value, expiresAt });
  }

  setExpiry(key: string, expiresAt: number | null): boolean {
    const e = this.get(key);
    if (!e) return false;
    e.expiresAt = expiresAt;
    if (expiresAt !== null) this.volatile.add(key);
    else this.volatile.delete(key);
    return true;
  }

  delete(key: string): boolean {
    this.volatile.delete(key);
    return this.data.delete(key);
  }

  /** Deletes the key if it holds an empty list (Redis never keeps empty lists). */
  dropIfEmpty(key: string): void {
    const e = this.data.get(key);
    if (e && e.type === "list" && e.value.length === 0) this.delete(key);
  }

  keys(pattern = "*"): string[] {
    const re = pattern === "*" ? null : globToRegExp(pattern);
    const out: string[] = [];
    for (const k of [...this.data.keys()]) if (this.get(k) && (!re || re.test(k))) out.push(k);
    return out;
  }

  size(): number {
    return this.keys().length;
  }

  expiresCount(): number {
    return this.volatile.size;
  }

  flush(): void {
    this.data.clear();
    this.volatile.clear();
    this.cursor = this.volatile.values();
  }

  /** Live (non-expired) entries, used for RDB snapshots. */
  *entries(): IterableIterator<[string, Entry]> {
    for (const k of [...this.data.keys()]) {
      const e = this.get(k);
      if (e) yield [k, e];
    }
  }

  private activeExpire(): void {
    const SAMPLE = 20;
    const deadline = Date.now() + 25; // never spend more than ~25ms per tick
    while (Date.now() < deadline && this.volatile.size > 0) {
      let sampled = 0;
      let expired = 0;
      const now = Date.now();
      while (sampled < SAMPLE) {
        let next = this.cursor.next();
        if (next.done) {
          this.cursor = this.volatile.values();
          next = this.cursor.next();
          if (next.done) break;
        }
        sampled++;
        const e = this.data.get(next.value);
        if (!e || (e.expiresAt !== null && e.expiresAt <= now)) {
          this.delete(next.value);
          expired++;
        }
      }
      if (sampled === 0 || expired / sampled <= 0.25) break;
    }
  }
}
