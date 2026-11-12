export interface Config {
  port: number;
  dir: string;
  dbfilename: string;
  /** Snapshot to disk every N seconds if there were writes (0 = disabled). */
  save: number;
  replicaof: { host: string; port: number } | null;
}

export const defaults: Config = {
  port: 6379,
  dir: process.cwd(),
  dbfilename: "dump.rdb",
  save: 60,
  replicaof: null,
};

const USAGE = `Usage: redis-ts [--port N] [--dir PATH] [--dbfilename FILE] [--save SECONDS] [--replicaof "HOST PORT"]`;

export function parseArgs(argv: string[]): Config {
  const cfg: Config = { ...defaults };
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    const val = argv[i + 1];
    if (flag === "--help" || flag === "-h") {
      console.log(USAGE);
      process.exit(0);
    }
    if (val === undefined) throw new Error(`missing value for ${flag}\n${USAGE}`);
    switch (flag) {
      case "--port":
        cfg.port = Number(val);
        break;
      case "--dir":
        cfg.dir = val;
        break;
      case "--dbfilename":
        cfg.dbfilename = val;
        break;
      case "--save":
        cfg.save = Number(val);
        break;
      case "--replicaof": {
        const [host, port] = val.trim().split(/\s+/);
        cfg.replicaof = { host, port: Number(port) };
        break;
      }
      default:
        throw new Error(`unknown option ${flag}\n${USAGE}`);
    }
    i++;
  }
  if (!Number.isInteger(cfg.port) || cfg.port < 0 || cfg.port > 65535) throw new Error("invalid --port");
  if (cfg.replicaof && !Number.isInteger(cfg.replicaof.port)) throw new Error("invalid --replicaof");
  return cfg;
}
