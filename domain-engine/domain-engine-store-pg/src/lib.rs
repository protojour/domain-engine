#![forbid(unsafe_code)]

use std::{collections::BTreeSet, sync::Arc};

use anyhow::anyhow;
use compaction::{compaction_task, CompactionCtx};
use domain_engine_core::{
    data_store::DataStoreAPI,
    system::ArcSystemApi,
    transact::{ReqMessage, RespMessage, TransactionMode},
    DomainResult, Session,
};
use futures_util::stream::BoxStream;
use ontol_runtime::{
    ontology::{aspects::DefsAspect, Ontology},
    DomainIndex, PropId,
};
use tokio_postgres::NoTls;

pub use pg_model::PgModel;

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
use tracing::{error, info, info_span, Instrument};

pub type PgResult<T> = Result<T, tokio_postgres::Error>;

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
        pool: deadpool_postgres::Pool,
        system: ArcSystemApi,
        datastore_mutated: tokio::sync::watch::Sender<()>,
    ) -> Self {
        Self {
            pg_model,
            ontology,
            pool,
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

pub async fn connect_and_migrate(
    persistent_domains: &BTreeSet<DomainIndex>,
    ontology_defs: &DefsAspect,
    pg_config: &tokio_postgres::Config,
) -> anyhow::Result<PgModel> {
    let db_name = pg_config
        .get_dbname()
        .ok_or_else(|| anyhow!("missing db name"))?;
    let (mut client, connection) = pg_config.connect(NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    let join_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let pg_model =
        migrate::migrate(persistent_domains, ontology_defs, db_name, &mut client).await?;
    drop(client);

    join_handle.await?;

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
