use anyhow::anyhow;
use domain_engine_core::{
    data_store::{DataStoreAPI, Request, Response},
    system::ArcSystemApi,
    DomainError, DomainResult,
};
use sql::EscapeIdentifier;
use tokio_postgres::NoTls;

pub mod migrate;
mod sql;

pub use deadpool_postgres;
pub use tokio_postgres;
use tracing::{debug, error};

pub struct PostgresDataStore {
    pub pool: deadpool_postgres::Pool,
    pub system: ArcSystemApi,
}

#[async_trait::async_trait]
impl DataStoreAPI for PostgresDataStore {
    async fn execute(
        &self,
        _request: Request,
        _session: domain_engine_core::Session,
    ) -> DomainResult<Response> {
        Err(DomainError::DataStore(anyhow!(
            "not implemented for Postgres"
        )))
    }
}

pub async fn recreate_database(
    db_name: &str,
    master_config: &tokio_postgres::Config,
) -> anyhow::Result<()> {
    debug!(
        "recreate database `{db_name}` (will hang if there are open connections to this database)"
    );

    let (client, connection) = master_config.connect(NoTls).await.unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    let join_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    client
        .query(
            &format!("DROP DATABASE IF EXISTS {}", EscapeIdentifier(db_name)),
            &[],
        )
        .await?;
    client
        .query(
            &format!("CREATE DATABASE {}", EscapeIdentifier(db_name)),
            &[],
        )
        .await?;

    drop(client);
    join_handle.await.unwrap();

    Ok(())
}
