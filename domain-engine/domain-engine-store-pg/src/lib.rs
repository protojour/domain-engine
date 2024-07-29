#![forbid(unsafe_code)]

use anyhow::anyhow;
use domain_engine_core::{
    data_store::{BatchWriteRequest, DataStoreAPI, Request, Response},
    system::ArcSystemApi,
    transaction::{ReqMessage, RespMessage},
    DomainError, DomainResult, Session,
};
use futures_util::stream::BoxStream;
use sql::EscapeIdent;
use tokio_postgres::NoTls;

pub mod migrate;
mod sql;

pub use deadpool_postgres;
pub use tokio_postgres;
use tracing::{debug, error, info};

pub struct PostgresDataStore {
    pub pool: deadpool_postgres::Pool,
    pub system: ArcSystemApi,
}

#[async_trait::async_trait]
impl DataStoreAPI for PostgresDataStore {
    async fn execute(
        &self,
        request: Request,
        _session: domain_engine_core::Session,
    ) -> DomainResult<Response> {
        match request {
            Request::Query(_) => Err(DomainError::DataStore(anyhow!(
                "Query not implemented for Postgres"
            ))),
            Request::BatchWrite(write_requests) => {
                for write_request in write_requests {
                    match write_request {
                        BatchWriteRequest::Insert(values, _) => {
                            for value in values {
                                debug!("insert {value:?}");
                            }
                        }
                        BatchWriteRequest::Update(values, _) => {
                            for value in values {
                                debug!("update {value:?}");
                            }
                        }
                        BatchWriteRequest::Upsert(values, _) => {
                            for value in values {
                                debug!("upsert {value:?}");
                            }
                        }
                        BatchWriteRequest::Delete(values, _) => {
                            for value in values {
                                debug!("delete {value:?}");
                            }
                        }
                    }
                }

                Err(DomainError::DataStore(anyhow!(
                    "BatchWrite not implemented for Postgres"
                )))
            }
        }
    }

    async fn transact<'a>(
        &'a self,
        _messages: BoxStream<'a, DomainResult<ReqMessage>>,
        _session: Session,
    ) -> DomainResult<BoxStream<'_, DomainResult<RespMessage>>> {
        todo!()
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
