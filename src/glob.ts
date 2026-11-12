/** Converts a Redis glob pattern (*, ?, [abc], [^a], [a-z], \x) to a RegExp. */
export function globToRegExp(pattern: string): RegExp {
  let re = "^";
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i];
    if (ch === "*") re += ".*";
    else if (ch === "?") re += ".";
    else if (ch === "\\" && i + 1 < pattern.length) re += escape(pattern[++i]);
    else if (ch === "[") {
      const close = pattern.indexOf("]", i + 1);
      if (close === -1) {
        re += "\\[";
        continue;
      }
      let cls = pattern.slice(i + 1, close);
      if (cls.startsWith("^")) cls = "^" + cls.slice(1).replace(/[\]\\]/g, "\\$&");
      else cls = cls.replace(/[\]\\^]/g, "\\$&");
      re += `[${cls}]`;
      i = close;
    } else re += escape(ch);
  }
  return new RegExp(re + "$", "s");
}

function escape(ch: string): string {
  return ch.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
