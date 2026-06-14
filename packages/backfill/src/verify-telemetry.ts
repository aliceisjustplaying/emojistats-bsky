import type { ClickHouseClient } from '@clickhouse/client';

export interface VerifyProgress {
  phase: string;
  reposTotal: number;
  reposChecked: number;
  exact: number;
  loose: number;
  mismatches: number;
  looseEmitted: number;
  sampleChecked: number;
  sampleFailures: number;
  done: boolean;
  error?: string;
}

/** 'YYYY-MM-DD HH:MM:SS' UTC, the JSONEachRow-friendly DateTime form. */
function chDateTime(ms: number): string {
  return new Date(ms).toISOString().slice(0, 19).replace('T', ' ');
}

export class VerifyTelemetry {
  #lastInsertMs = 0;
  #lastProgress: VerifyProgress | undefined;
  readonly #ch: ClickHouseClient;
  readonly #runId: string;
  readonly #shard: string;
  readonly #ledgerPath: string;

  constructor(
    ch: ClickHouseClient,
    runId: string,
    shard: string,
    ledgerPath: string,
  ) {
    this.#ch = ch;
    this.#runId = runId;
    this.#shard = shard;
    this.#ledgerPath = ledgerPath;
  }

  async ensureTable(): Promise<void> {
    await this.#ch.command({
      query: `
        CREATE TABLE IF NOT EXISTS backfill_verify_progress (
          ts              DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
          run_id          LowCardinality(String),
          shard           LowCardinality(String),
          ledger_path     String CODEC(ZSTD(1)),
          phase           LowCardinality(String),
          repos_total     UInt64,
          repos_checked   UInt64,
          exact           UInt64,
          loose           UInt64,
          mismatches      UInt64,
          loose_emitted   UInt64,
          sample_checked  UInt64,
          sample_failures UInt64,
          done            UInt8,
          error           String CODEC(ZSTD(3))
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (run_id, shard, ts)
        TTL ts + INTERVAL 6 MONTH DELETE
      `,
    });
  }

  async record(progress: VerifyProgress, force = false): Promise<void> {
    this.#lastProgress = progress;
    const now = Date.now();
    if (!force && now - this.#lastInsertMs < 5_000) return;
    this.#lastInsertMs = now;
    await this.#ch.insert({
      table: 'backfill_verify_progress',
      values: [
        {
          ts: chDateTime(now),
          run_id: this.#runId,
          shard: this.#shard,
          ledger_path: this.#ledgerPath,
          phase: progress.phase,
          repos_total: progress.reposTotal,
          repos_checked: progress.reposChecked,
          exact: progress.exact,
          loose: progress.loose,
          mismatches: progress.mismatches,
          loose_emitted: progress.looseEmitted,
          sample_checked: progress.sampleChecked,
          sample_failures: progress.sampleFailures,
          done: progress.done ? 1 : 0,
          error: progress.error ?? '',
        },
      ],
      format: 'JSONEachRow',
    });
  }

  async finish(progress: VerifyProgress): Promise<void> {
    await this.record({ ...progress, done: true }, true);
  }

  async fail(err: unknown): Promise<void> {
    await this.record(
      {
        ...(this.#lastProgress ?? {
          phase: 'failed',
          reposTotal: 0,
          reposChecked: 0,
          exact: 0,
          loose: 0,
          mismatches: 0,
          looseEmitted: 0,
          sampleChecked: 0,
          sampleFailures: 0,
          done: false,
        }),
        phase: 'failed',
        done: true,
        error: err instanceof Error ? err.message : String(err),
      },
      true,
    );
  }
}
