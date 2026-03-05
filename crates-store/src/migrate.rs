pub async fn run_migrations(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    // Path is relative to this crate directory at compile time.
    sqlx::migrate!("../migrations").run(pool).await?;
    Ok(())
}
