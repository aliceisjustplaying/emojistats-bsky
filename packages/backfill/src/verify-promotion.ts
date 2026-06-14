import type Database from 'better-sqlite3';

export type VerificationPromotionResult = 'exact' | 'loose';
export const VERIFY_STAGED_LOADED_TABLE = 'verify_staged_loaded';

export interface LoadedPromotionStage {
  stagedLoadedTable: string;
}

export function promotionResultListSql(includeLoose: boolean): string {
  return includeLoose ? "('exact', 'loose')" : "('exact')";
}

function sqliteIdentifier(name: string): string {
  if (!/^[a-z][a-z0-9_]*$/.test(name)) {
    throw new Error(`Unsafe sqlite identifier: ${name}`);
  }
  return name;
}

export function createLoadedPromotionStage(
  db: Database.Database,
): LoadedPromotionStage {
  const table = sqliteIdentifier(VERIFY_STAGED_LOADED_TABLE);
  db.exec(`
    DROP TABLE IF EXISTS ${table};
    CREATE TEMP TABLE ${table} (
      did TEXT PRIMARY KEY
    ) WITHOUT ROWID
  `);
  return { stagedLoadedTable: table };
}

export function stageLoadedReposForPromotion(
  db: Database.Database,
  stage: LoadedPromotionStage,
  dids: readonly string[],
): void {
  const table = sqliteIdentifier(stage.stagedLoadedTable);
  const insertStagedLoaded = db.prepare(
    `INSERT INTO ${table} (did) VALUES (?)`,
  );
  const flushStagedLoaded = db.transaction(
    (stagedLoadedDids: readonly string[]) => {
      for (const did of stagedLoadedDids) insertStagedLoaded.run(did);
    },
  );
  flushStagedLoaded(dids);
}

export function promoteLoadedReposByDid(
  db: Database.Database,
  dids: readonly string[],
  stage: LoadedPromotionStage,
): number {
  const stagedLoadedTable = sqliteIdentifier(stage.stagedLoadedTable);
  db.exec(`
    DROP TABLE IF EXISTS verify_promotable;
    CREATE TEMP TABLE verify_promotable (
      did TEXT PRIMARY KEY
    ) WITHOUT ROWID
  `);
  const insertPromotable = db.prepare(
    'INSERT INTO verify_promotable (did) VALUES (?)',
  );
  const stagePromotable = db.transaction(
    (promotableDids: readonly string[]) => {
      for (const did of promotableDids) insertPromotable.run(did);
    },
  );
  stagePromotable(dids);
  return db
    .prepare(
      `UPDATE repos SET status = 'verified'
       WHERE status = 'loaded'
         AND did IN (SELECT did FROM ${stagedLoadedTable})
         AND did IN (SELECT did FROM verify_promotable)`,
    )
    .run().changes;
}
