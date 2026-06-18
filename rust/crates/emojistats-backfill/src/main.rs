#[path = "app.rs"]
mod app;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    app::run_cli().await
}
