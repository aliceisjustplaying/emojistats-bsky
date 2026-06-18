use emojistats_backfill::clickhouse::ClickHouseInsertPayload;

pub(super) fn increment(value: &mut u64, context: &str) -> anyhow::Result<()> {
    *value = value
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("{context} overflow"))?;
    Ok(())
}

pub(super) fn add_count(value: &mut u64, addend: u64, context: &str) -> anyhow::Result<()> {
    *value = value
        .checked_add(addend)
        .ok_or_else(|| anyhow::anyhow!("{context} overflow"))?;
    Ok(())
}

pub(super) fn count_len(value: usize, context: &str) -> anyhow::Result<u64> {
    u64::try_from(value).map_err(|_error| anyhow::anyhow!("{context} overflow"))
}

pub(super) fn payload_row_count(payloads: &[ClickHouseInsertPayload]) -> anyhow::Result<u64> {
    payloads.iter().try_fold(0_u64, |total, payload| {
        let rows = count_len(payload.row_count, "payload row count")?;
        total
            .checked_add(rows)
            .ok_or_else(|| anyhow::anyhow!("payload row total overflow"))
    })
}
