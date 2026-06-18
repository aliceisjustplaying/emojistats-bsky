#[tokio::main]
async fn main() -> anyhow::Result<()> {
    emojistats_backfill::app::run_cli().await
}
