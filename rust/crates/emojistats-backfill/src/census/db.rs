use std::{path::Path, time::Duration};

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::{
    census::{
        plc::pds_host_from_endpoint,
        types::{
            HostCandidate, HostCensusStatus, HostCheckResult, PagePersistSummary, PlcExportLine,
            system_time_millis,
        },
    },
    ledger::SqliteLedger,
};

const PLC_META_CURSOR: &str = "plc_cursor";

pub(super) fn open_census_connection(path: &Path) -> anyhow::Result<Connection> {
    drop(SqliteLedger::open(path)?);
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(30))?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(connection)
}

pub(super) fn create_census_schema(connection: &Connection) -> anyhow::Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS plc_meta (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS plc_identities (
            did TEXT PRIMARY KEY NOT NULL,
            pds_host TEXT,
            endpoint TEXT,
            seq INTEGER NOT NULL CHECK (seq >= 0),
            nullified INTEGER NOT NULL CHECK (nullified IN (0, 1))
        );
        CREATE INDEX IF NOT EXISTS idx_plc_identities_host
            ON plc_identities (pds_host, nullified);
        CREATE TABLE IF NOT EXISTS pds_census (
            host TEXT PRIMARY KEY NOT NULL,
            endpoint TEXT,
            status TEXT NOT NULL CHECK (status IN ('admitted', 'quarantined')),
            error TEXT,
            checked_at_ms INTEGER NOT NULL,
            repo_count INTEGER
        );
        ",
    )?;
    Ok(())
}

pub(super) fn load_cursor(connection: &Connection) -> anyhow::Result<u64> {
    let cursor = connection
        .query_row(
            "SELECT value FROM plc_meta WHERE key = ?1",
            params![PLC_META_CURSOR],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map_or(Ok(0_u64), |value| value.parse::<u64>())?;
    Ok(cursor)
}

pub(super) fn set_cursor(connection: &Connection, cursor: u64) -> anyhow::Result<()> {
    connection.execute(
        "
        INSERT INTO plc_meta (key, value) VALUES (?1, ?2)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        ",
        params![PLC_META_CURSOR, cursor.to_string()],
    )?;
    Ok(())
}

pub(super) fn persist_plc_page(
    connection: &mut Connection,
    previous_cursor: u64,
    page: &[PlcExportLine],
    update_cursor: bool,
) -> anyhow::Result<PagePersistSummary> {
    let transaction = connection.transaction()?;
    let mut summary = PagePersistSummary {
        first_seq: previous_cursor,
        cursor: previous_cursor,
        ..PagePersistSummary::default()
    };
    for line in page {
        let seq = line
            .seq
            .ok_or_else(|| anyhow::anyhow!("PLC export line missing seq for {}", line.did))?;
        if summary.ops == 0 {
            summary.first_seq = seq;
        }
        summary.cursor = seq;
        summary.ops = summary
            .ops
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PLC page op count overflow"))?;
        if line.nullified == Some(true) || line.operation.kind == "plc_tombstone" {
            upsert_plc_identity(&transaction, &line.did, None, None, seq, true)?;
            summary.tombstoned = summary
                .tombstoned
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PLC page tombstone count overflow"))?;
            continue;
        }
        let Some(endpoint) = endpoint_from_operation(&line.operation) else {
            summary.skipped = summary
                .skipped
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PLC page skipped count overflow"))?;
            continue;
        };
        let Some(host) = pds_host_from_endpoint(&endpoint) else {
            summary.skipped = summary
                .skipped
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PLC page skipped count overflow"))?;
            continue;
        };
        upsert_plc_identity(
            &transaction,
            &line.did,
            Some(&host),
            Some(&endpoint),
            seq,
            false,
        )?;
        summary.upserted = summary
            .upserted
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PLC page upsert count overflow"))?;
    }
    if update_cursor {
        transaction.execute(
            "
            INSERT INTO plc_meta (key, value) VALUES (?1, ?2)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ",
            params![PLC_META_CURSOR, summary.cursor.to_string()],
        )?;
    }
    transaction.commit()?;
    Ok(summary)
}

fn upsert_plc_identity(
    transaction: &Transaction<'_>,
    did: &str,
    host: Option<&str>,
    endpoint: Option<&str>,
    seq: u64,
    nullified: bool,
) -> anyhow::Result<()> {
    let seq_i64 = i64::try_from(seq)?;
    transaction.execute(
        "
        INSERT INTO plc_identities (did, pds_host, endpoint, seq, nullified)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(did) DO UPDATE SET
            pds_host = excluded.pds_host,
            endpoint = excluded.endpoint,
            seq = excluded.seq,
            nullified = excluded.nullified
        WHERE excluded.seq >= plc_identities.seq
        ",
        params![did, host, endpoint, seq_i64, i64::from(nullified)],
    )?;
    Ok(())
}

fn endpoint_from_operation(operation: &crate::census::types::PlcOperation) -> Option<String> {
    if operation.kind == "create" {
        return operation.service.clone();
    }
    if operation.kind == "plc_operation" {
        return operation
            .services
            .as_ref()
            .and_then(|services| services.get("atproto_pds"))
            .and_then(|service| service.endpoint.clone());
    }
    None
}

pub(super) fn load_host_candidates(
    connection: &Connection,
    max_hosts: Option<u64>,
) -> anyhow::Result<Vec<HostCandidate>> {
    let limit = max_hosts
        .map(i64::try_from)
        .transpose()?
        .unwrap_or(i64::MAX);
    let mut statement = connection.prepare(
        "
        SELECT pds_host, MIN(endpoint)
        FROM plc_identities
        WHERE nullified = 0 AND pds_host IS NOT NULL
        GROUP BY pds_host
        ORDER BY pds_host
        LIMIT ?1
        ",
    )?;
    let rows = statement.query_map(params![limit], |row| {
        Ok(HostCandidate {
            host: row.get(0)?,
            endpoint: row.get(1)?,
        })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

pub(super) fn persist_host_checks(
    connection: &Connection,
    checks: &[HostCheckResult],
) -> anyhow::Result<()> {
    let checked_at_ms = system_time_millis(std::time::SystemTime::now())?;
    let mut statement = connection.prepare(
        "
        INSERT INTO pds_census (host, endpoint, status, error, checked_at_ms, repo_count)
        VALUES (?1, ?2, ?3, ?4, ?5, NULL)
        ON CONFLICT(host) DO UPDATE SET
            endpoint = excluded.endpoint,
            status = excluded.status,
            error = excluded.error,
            checked_at_ms = excluded.checked_at_ms,
            repo_count = excluded.repo_count
        ",
    )?;
    for check in checks {
        statement.execute(params![
            check.host.as_str(),
            check.endpoint.as_deref(),
            check.status.as_str(),
            check.error.as_deref(),
            checked_at_ms,
        ])?;
    }
    Ok(())
}

pub(super) fn persist_disabled_host_overrides(
    connection: &Connection,
    checks: &[HostCheckResult],
) -> anyhow::Result<()> {
    let mut statement = connection.prepare(
        "
        INSERT INTO host_overrides (
            host,
            disabled,
            concurrency_cap,
            min_interval_ms,
            revive_after_ms,
            force_mode,
            force_mode_revive_after_ms,
            never_diff
        ) VALUES (?1, 1, NULL, NULL, NULL, NULL, NULL, 1)
        ON CONFLICT(host) DO UPDATE SET
            disabled = 1,
            revive_after_ms = NULL,
            never_diff = 1
        ",
    )?;
    for check in checks
        .iter()
        .filter(|check| check.status == HostCensusStatus::Quarantined)
    {
        statement.execute(params![check.host.as_str()])?;
    }
    Ok(())
}
