#![forbid(unsafe_code)]

use std::{collections::BTreeSet, sync::Arc};

use domain_engine_core::{
    data_store::DataStoreAPI,
    system::ArcSystemApi,
    transact::{ReqMessage, RespMessage, TransactionMode},
    DomainResult, Session,
};
use futures_util::stream::BoxStream;
use ontol_runtime::{ontology::Ontology, PackageId};
use tokio_postgres::NoTls;

pub use pg_model::PgModel;

mod migrate;
mod pg_error;
mod pg_model;
mod sql;
mod sql_record;
mod sql_value;
mod transact;

pub use deadpool_postgres;
pub use tokio_postgres;
use tracing::{error, info};

pub type PgResult<T> = Result<T, tokio_postgres::Error>;

pub struct PostgresDataStore {
    #[allow(unused)]
    pg_model: PgModel,
    ontology: Arc<Ontology>,
    pool: deadpool_postgres::Pool,
    #[allow(unused)]
    system: ArcSystemApi,
}

impl PostgresDataStore {
    pub fn new(
        pg_model: PgModel,
        ontology: Arc<Ontology>,
        pool: deadpool_postgres::Pool,
        system: ArcSystemApi,
    ) -> Self {
        Self {
            pg_model,
            ontology,
            pool,
            system,
        }
    }
}

pub struct PostgresHandle {
    pub(crate) store: Arc<PostgresDataStore>,
}

impl From<PostgresDataStore> for PostgresHandle {
    fn from(value: PostgresDataStore) -> Self {
        PostgresHandle {
            store: Arc::new(value),
        }
    }
}

pub async fn connect_and_migrate(
    persistent_domains: &BTreeSet<PackageId>,
    ontology: &Ontology,
    pg_config: &tokio_postgres::Config,
) -> anyhow::Result<PgModel> {
    let db_name = pg_config.get_dbname().unwrap();
    let (mut client, connection) = pg_config.connect(NoTls).await.unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    let join_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let pg_model = migrate::migrate(persistent_domains, ontology, db_name, &mut client).await?;
    drop(client);

    join_handle.await.unwrap();

    Ok(pg_model)
}

#[async_trait::async_trait]
impl DataStoreAPI for PostgresHandle {
    async fn transact(
        &self,
        mode: TransactionMode,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        _session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        transact::transact(self.store.clone(), mode, messages).await
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
            &format!("DROP DATABASE IF EXISTS {}", sql::Ident(db_name)),
            &[],
        )
        .await?;
    client
        .query(&format!("CREATE DATABASE {}", sql::Ident(db_name)), &[])
        .await?;

    drop(client);
    join_handle.await.unwrap();

    Ok(())
}

#[derive(Default)]
struct IgnoreRows;

impl<T> Extend<T> for IgnoreRows {
    fn extend<I: IntoIterator<Item = T>>(&mut self, _iter: I) {}
}

#[derive(Default)]
struct CountRows(usize);

impl<T> Extend<T> for CountRows {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let count = iter.into_iter().count();
        self.0 += count;
    }
}
