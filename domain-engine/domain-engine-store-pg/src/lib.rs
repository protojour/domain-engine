#![forbid(unsafe_code)]

use std::sync::Arc;

use domain_engine_core::{
    data_store::DataStoreAPI,
    system::ArcSystemApi,
    transact::{ReqMessage, RespMessage},
    DomainError, DomainResult, Session,
};
use futures_util::{stream::BoxStream, StreamExt};
use sql::EscapeIdent;
use tokio_postgres::NoTls;

pub mod migrate;
mod sql;

pub use deadpool_postgres;
pub use tokio_postgres;
use tracing::{error, info};

pub struct PostgresDataStore {
    pub pool: deadpool_postgres::Pool,
    pub system: ArcSystemApi,
}

pub struct PostgresHandle {
    pub(crate) _database: Arc<PostgresDataStore>,
}

impl From<PostgresDataStore> for PostgresHandle {
    fn from(value: PostgresDataStore) -> Self {
        PostgresHandle {
            _database: Arc::new(value),
        }
    }
}

#[async_trait::async_trait]
impl DataStoreAPI for PostgresHandle {
    async fn transact(
        &self,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        _session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        Ok(async_stream::stream! {
            for await message in messages {
                match message? {
                    ReqMessage::Query(..) => {
                        yield Err(DomainError::data_store("Query not implemented for Postgres"))?;
                    }
                    ReqMessage::Insert(..) => {
                        yield Err(DomainError::data_store("Insert not implemented for Postgres"))?;
                    }
                    ReqMessage::Update(..) => {
                        yield Err(DomainError::data_store("Update not implemented for Postgres"))?;
                    }
                    ReqMessage::Upsert(..) => {
                        yield Err(DomainError::data_store("Upsert not implemented for Postgres"))?;
                    }
                    ReqMessage::Delete(..) => {
                        yield Err(DomainError::data_store("Delete not implemented for Postgres"))?;
                    }
                    ReqMessage::Argument(..) => {}
                }
            }
        }
        .boxed())
    }
}

pub async fn recreate_database(
    db_name: &str,
    master_config: &tokio_postgres::Config,
) -> anyhow::Result<()> {
    info!(
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
            &format!("DROP DATABASE IF EXISTS {}", EscapeIdent(db_name)),
            &[],
        )
        .await?;
    client
        .query(&format!("CREATE DATABASE {}", EscapeIdent(db_name)), &[])
        .await?;

    drop(client);
    join_handle.await.unwrap();

    Ok(())
}
