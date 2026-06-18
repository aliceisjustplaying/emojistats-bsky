use emojistats_backfill::clickhouse::{
    ClickHouseClientConfig, aggregate_rebuild_sql, aggregate_rebuild_statements,
};

pub(super) async fn run_clickhouse_rebuild_aggregates_command(
    clickhouse_url: &str,
    clickhouse_database: &str,
    clickhouse_user: &str,
    clickhouse_password: &str,
    dry_run: bool,
) -> anyhow::Result<()> {
    if dry_run {
        println!("{}", aggregate_rebuild_sql(clickhouse_database)?);
        return Ok(());
    }

    let config = ClickHouseClientConfig::new(
        clickhouse_url,
        clickhouse_database,
        clickhouse_user,
        clickhouse_password,
        "emojistats-backfill-aggregate-rebuild",
    )?;
    let client = config.http_client()?;
    let statements = aggregate_rebuild_statements(clickhouse_database)?;
    let receipts = config.execute_sql_statements(&client, &statements).await?;
    println!(
        "rebuilt ClickHouse aggregates with {} statement(s)",
        receipts.len()
    );
    Ok(())
}
