use serde_json::json;
use tokio::task::JoinSet;
use tokio_postgres::{Client, NoTls};
use tracing::info;

use super::config;
use crate::recreate_database;

#[ontol_macros::test(tokio::test)]
async fn pg_experiment() -> anyhow::Result<()> {
    let dbname = "test_pg_experiment";
    recreate_database(dbname, &config("postgres")).await?;

    let (client, connection) = config(dbname).connect(NoTls).await?;
    let mut join_set = JoinSet::<()>::new();
    join_set.spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .query(
            "CREATE TABLE docs (
                data jsonb NOT NULL
            )",
            &[],
        )
        .await?;

    client
        .query("CREATE UNIQUE INDEX docs_key ON docs((data->>'id'))", &[])
        .await?;

    info!("first doc");
    insert_doc(json!({ "id": "a" }), &client).await?;
    info!("second doc");
    insert_doc(json!({ "id": "b" }), &client).await?;

    info!("created table");

    Ok(())
}

async fn insert_doc(doc: serde_json::Value, client: &Client) -> anyhow::Result<()> {
    client
        .query(
            "INSERT INTO docs (data) VALUES ($1)",
            &[&tokio_postgres::types::Json(doc)],
        )
        .await?;
    Ok(())
}
