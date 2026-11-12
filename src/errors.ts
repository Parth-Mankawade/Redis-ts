/** An error that is sent back to the client as a RESP error reply. */
export class RedisError extends Error {}

export const WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
export const NOT_INTEGER = "ERR value is not an integer or out of range";
export const SYNTAX = "ERR syntax error";
