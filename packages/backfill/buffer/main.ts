import { toBytes } from "@atcute/cbor";
import {
  decodeChunk,
  FirehoseSubscription,
  type FirehoseSubscriptionOptions,
} from "@futur/bsky-indexer";
import { execSync } from "node:child_process";
import console from "node:console";
import { createWriteStream, type WriteStream } from "node:fs";
import { existsSync } from "node:fs";
import process from "node:process";

declare global {
  namespace NodeJS {
    interface ProcessEnv {
      BUFFER_REPO_PROVIDER: string;
    }
  }
}

for (const envVar of ["BUFFER_REPO_PROVIDER"]) {
  if (!process.env[envVar]) throw new Error(`Missing env var ${envVar}`);
}

let initialCursor: number | undefined = 0;
if (process.argv.join(" ").includes("--cursor")) {
  const cursor = process.argv[process.argv.indexOf("--cursor") + 1];
  initialCursor = cursor === "latest" ? undefined : parseInt(cursor);
} else if (existsSync("relay-buffer.jsonl")) {
  try {
    const cursor = execSync("tail -n 1 relay-buffer.jsonl | jq -r '.seq'")
      .toString()
      .trim();
    initialCursor = parseInt(cursor);
  } catch (e) {
    console.error("Failed to read cursor from relay-buffer.jsonl", e);
  }
}

class ToBufferSubscription extends FirehoseSubscription {
  private stream: WriteStream;

  constructor(
    opts: Omit<FirehoseSubscriptionOptions, "dbOptions">,
    filename: string,
  ) {
    super(
      {
        ...opts,
        minWorkers: 1,
        maxWorkers: 2,
        dbOptions: { url: "" },
        idResolverOptions: {},
        statsFrequencyMs: 0,
      },
      new URL("./dummyWorker.ts", import.meta.url),
    );

    // todo: remove once @futur/bsky-indexer 0.1.7 is published
    if (opts.cursor !== undefined) this.cursor = `${opts.cursor}`;

    this.stream = createWriteStream(filename, { flags: "a" });
    process.on("exit", () => this.stream.close());
  }

  override onMessage = async ({ data }: { data: ArrayBuffer }) => {
    this.stream.write(
      lexToJson(decodeChunk(new Uint8Array(data)) ?? {}) + "\n",
    );
  };
}

function main() {
  const filename = "relay-buffer.jsonl";

  const sub = new ToBufferSubscription(
    {
      service: process.env.BUFFER_REPO_PROVIDER,
      ...(initialCursor !== undefined ? { cursor: initialCursor } : {}),
      onError: (err) =>
        console.error(...(err.cause ? [err.message, err.cause] : [err])),
    },
    filename,
  );

  return sub.start();
}

function lexToJson(record: Record<string, unknown>) {
  return JSON.stringify(record, (_, value) => {
    try {
      if (value instanceof Uint8Array) {
        return { $bytes: toBytes(value).$bytes };
      }
      // CIDs are already encoded as strings in event data; records are already valid JSON
      return value;
    } catch (e) {
      console.error("Error encoding value", value, e);
      return value;
    }
  });
}

void main();
