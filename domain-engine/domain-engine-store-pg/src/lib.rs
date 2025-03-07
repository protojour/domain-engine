#![forbid(unsafe_code)]

use std::{collections::BTreeSet, sync::Arc};

pub use pg_model::PgModel;

use compaction::{CompactionCtx, compaction_task};
use domain_engine_core::{
    DomainResult, Session,
    data_store::DataStoreAPI,
    system::ArcSystemApi,
    transact::{ReqMessage, RespMessage, TransactionMode},
};
use futures_util::stream::BoxStream;
use migrate::txn_wrapper::TxnWrapper;
use ontol_runtime::{
    DomainIndex, PropId,
    ontology::{Ontology, aspects::DefsAspect},
};
use pg_model::RegVersion;
use tokio_postgres::NoTls;

mod address;
mod compaction;
mod migrate;
mod pg_error;
mod pg_model;
mod sql;
mod sql_array;
mod sql_iter;
mod sql_record;
mod sql_value;
mod statement;
mod transact;

pub use deadpool_postgres;
pub use tokio_postgres;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, error, info, info_span};

pub type PgResult<T> = Result<T, tokio_postgres::Error>;

#[derive(Clone)]
pub struct PostgresConnection {
    pool: deadpool_postgres::Pool,
    reg_version: RegVersion,
}

pub struct PostgresDataStore {
    pg_model: PgModel,
    ontology: Arc<Ontology>,
    pool: deadpool_postgres::Pool,
    system: ArcSystemApi,
    datastore_mutated: tokio::sync::watch::Sender<()>,
    compaction_ctx: CompactionCtx,
}

impl PostgresDataStore {
    pub fn new(
        pg_model: PgModel,
        ontology: Arc<Ontology>,
        connection: PostgresConnection,
        system: ArcSystemApi,
        datastore_mutated: tokio::sync::watch::Sender<()>,
    ) -> Self {
        Self {
            pg_model,
            ontology,
            pool: connection.pool,
            system,
            datastore_mutated,
            compaction_ctx: CompactionCtx::default(),
        }
    }

    /// Set the number of individual / incremental changes to a CRDT document
    /// can be saved before compaction is triggered
    pub fn with_crdt_compaction_threshold(mut self, threshold: u32) -> Self {
        self.compaction_ctx.crdt_compaction_threshold = threshold;
        self
    }
}

pub struct PostgresHandle {
    pub(crate) store: Arc<PostgresDataStore>,
    _bg_tasks: Vec<AbortOnDropHandle<()>>,
}

impl From<PostgresDataStore> for PostgresHandle {
    fn from(store: PostgresDataStore) -> Self {
        let store = Arc::new(store);

        let bg_tasks = vec![AbortOnDropHandle::new(tokio::spawn(
            compaction_task(store.clone()).instrument(info_span!("compaction")),
        ))];

        PostgresHandle {
            store,
            _bg_tasks: bg_tasks,
        }
    }
}

/// Connect to Postgres and migrate ontology-independent core structures
pub async fn connect(
    pool: deadpool_postgres::Pool,
    db_name: &str,
) -> anyhow::Result<PostgresConnection> {
    let mut txn_wrapper = TxnWrapper {
        conn: pool.get().await?,
    };

    let reg_version = migrate::migrate_registry(&mut txn_wrapper, db_name).await?;

    Ok(PostgresConnection { pool, reg_version })
}

pub async fn migrate_ontology(
    persistent_domains: &BTreeSet<DomainIndex>,
    ontology_defs: &DefsAspect,
    connection: PostgresConnection,
) -> anyhow::Result<PgModel> {
    let conn = connection.pool.get().await?;

    let pg_model = migrate::migrate_ontology(
        persistent_domains,
        ontology_defs,
        connection.reg_version,
        conn,
    )
    .await?;

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

    fn stable_property_index(&self, prop_id: PropId) -> Option<u32> {
        self.store.pg_model.stable_property_index(prop_id)
    }
}

pub async fn recreate_database(
    db_name: &str,
    master_config: &tokio_postgres::Config,
) -> anyhow::Result<()> {
    info!(
        "recreate database `{db_name}` (will hang if there are open connections to this database)"
    );

    let (client, connection) = master_config.connect(NoTls).await?;

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
    join_handle.await?;

    Ok(())
}

#[derive(Default)]
#[expect(unused)]
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
