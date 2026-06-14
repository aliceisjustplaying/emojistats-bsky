import type Database from 'better-sqlite3';

export type VerificationPromotionResult = 'exact' | 'loose';

export function promotionResultListSql(includeLoose: boolean): string {
  return includeLoose ? "('exact', 'loose')" : "('exact')";
}

export function promoteLoadedReposByDid(
  db: Database.Database,
  dids: readonly string[],
): number {
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
         AND did IN (SELECT did FROM verify_staged_loaded)
         AND did IN (SELECT did FROM verify_promotable)`,
    )
    .run().changes;
}
