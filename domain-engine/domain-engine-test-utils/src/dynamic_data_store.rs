use std::{collections::BTreeSet, sync::Arc};

use domain_engine_core::{
    DomainResult,
    data_store::{DataStoreAPI, DataStoreConnection, DataStoreConnectionSync, DataStoreParams},
    domain_error::DomainErrorKind,
};
use domain_engine_store_inmemory::InMemoryConnection;
use domain_engine_tantivy::{TantivyConfig, TantivyIndexSource, make_tantivy_layer};
use ontol_runtime::{DomainIndex, ontology::aspects::OntologyAspects};
use pg::PgTestDatastoreConnection;

#[derive(Clone)]
pub struct DynamicDataStoreClient {
    impl_name: String,
    tantivy_index: bool,
    crdt_compaction_threshold: Option<u32>,
}

impl DynamicDataStoreClient {
    /// Create a data store builder using the provided name of an implementation
    pub fn new(impl_name: impl Into<String>) -> Self {
        Self {
            impl_name: impl_name.into(),
            tantivy_index: false,
            crdt_compaction_threshold: None,
        }
    }

    pub fn tantivy_index(mut self) -> Self {
        self.tantivy_index = true;
        self
    }

    pub fn crdt_compaction_threshold(mut self, threshold: u32) -> Self {
        self.crdt_compaction_threshold = Some(threshold);
        self
    }

    /// Connect to a data store implementation
    pub async fn connect(self) -> DomainResult<DynamicDataStoreConnection> {
        let backend = match self.impl_name.as_str() {
            "inmemory" => DynamicDataStoreConnectionBackend::InMemory,
            "pg" => DynamicDataStoreConnectionBackend::Pg(pg::create_pg_connection().await?),
            other => return Err(DomainErrorKind::UnknownDataStore(other.to_string()).into_error()),
        };

        Ok(DynamicDataStoreConnection {
            builder: self,
            backend,
        })
    }

    fn add_layers(
        &self,
        inner: Arc<dyn DataStoreAPI + Send + Sync>,
        params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
        if self.tantivy_index {
            make_tantivy_layer(TantivyConfig {
                data_store_params: params,
                datastore: inner.clone(),
                indexing_datastore: inner.clone(),
                index_source: TantivyIndexSource::InMemory,
                vertex_index_queue_size: 64,
                index_writer_mem_budget: 50_000_000,
                cancel: Default::default(),
            })
        } else {
            Ok(inner)
        }
    }
}

#[derive(Clone)]
pub struct DynamicDataStoreConnection {
    builder: DynamicDataStoreClient,
    backend: DynamicDataStoreConnectionBackend,
}

#[derive(Clone)]
enum DynamicDataStoreConnectionBackend {
    InMemory,
    #[cfg(feature = "store-pg")]
    Pg(PgTestDatastoreConnection),
}

#[async_trait::async_trait]
impl DataStoreConnection for DynamicDataStoreConnection {
    async fn migrate(
        &self,
        persisted: &BTreeSet<DomainIndex>,
        mut params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
        // test stripped down ontology
        params.ontology = Arc::new(
            params
                .ontology
                .aspect_subset(OntologyAspects::DEFS | OntologyAspects::SERDE),
        );

        let api = match &self.backend {
            DynamicDataStoreConnectionBackend::InMemory => {
                InMemoryConnection
                    .migrate(persisted, params.clone())
                    .await?
            }
            DynamicDataStoreConnectionBackend::Pg(connection) => {
                connection.migrate(persisted, params.clone()).await?
            }
        };

        self.builder.add_layers(api, params)
    }
}

impl DataStoreConnectionSync for DynamicDataStoreClient {
    fn migrate_sync(
        &self,
        persisted: &BTreeSet<DomainIndex>,
        params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
        let api = match self.impl_name.as_str() {
            "inmemory" => InMemoryConnection.migrate_sync(persisted, params.clone()),
            other => panic!("cannot synchronously create `{other}` factory"),
        }?;

        self.add_layers(api, params)
    }
}

#[cfg(feature = "store-pg")]
mod pg {
    use std::collections::BTreeSet;
    use std::env;
    use std::sync::{Arc, OnceLock};

    use domain_engine_core::data_store::{DataStoreAPI, DataStoreParams};
    use domain_engine_core::domain_error::DomainErrorContext;
    use domain_engine_core::transact::{ReqMessage, RespMessage, TransactionMode};
    use domain_engine_core::{DomainError, DomainResult, Session};
    use domain_engine_store_pg::{
        PostgresConnection, PostgresDataStore, deadpool_postgres, migrate_ontology, tokio_postgres,
    };
    use domain_engine_store_pg::{PostgresHandle, recreate_database};
    use futures_util::stream::BoxStream;
    use ontol_runtime::PropId;
    use tokio::sync::{OwnedSemaphorePermit, Semaphore};

    /// Make sure not to overwhelm the test postgres instance.
    /// This number can be tweaked, it's about finding a sweet spot.
    /// A small number of concurrent tests should be fine,
    /// since the intention is for each test to run in a separate database (this is best effort though).
    const MAX_CONCURRENT_PG_TESTS: usize = 6;

    /// this global semaphore limits the number of PgTestDatastores that can exist at the same time.
    static PG_TEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

    #[derive(Clone)]
    pub struct PgTestDatastoreConnection {
        connection: PostgresConnection,
        #[expect(unused)]
        permit: Arc<OwnedSemaphorePermit>,
    }

    struct PgTestDatastore {
        handle: PostgresHandle,
    }

    #[async_trait::async_trait]
    impl domain_engine_core::data_store::DataStoreConnection for PgTestDatastoreConnection {
        async fn migrate(
            &self,
            persisted: &BTreeSet<ontol_runtime::DomainIndex>,
            params: DataStoreParams,
        ) -> DomainResult<Arc<dyn domain_engine_core::data_store::DataStoreAPI + Send + Sync>>
        {
            let pg_model = migrate_ontology(
                persisted,
                params.ontology.as_ref().as_ref(),
                self.connection.clone(),
            )
            .await
            .map_err(|err| DomainError::data_store(format!("{err:?}")))?;

            let data_store = PostgresDataStore::new(
                pg_model,
                params.ontology,
                self.connection.clone(),
                params.system,
                params.datastore_mutated,
            );

            Ok(Arc::new(PgTestDatastore {
                handle: data_store.into(),
            }))
        }
    }

    #[async_trait::async_trait]
    impl DataStoreAPI for PgTestDatastore {
        async fn transact(
            &self,
            mode: TransactionMode,
            messages: BoxStream<'static, Result<ReqMessage, DomainError>>,
            session: Session,
        ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
            self.handle.transact(mode, messages, session).await
        }

        fn stable_property_index(&self, prop_id: PropId) -> Option<u32> {
            self.handle.stable_property_index(prop_id)
        }
    }

    pub async fn create_pg_connection() -> DomainResult<PgTestDatastoreConnection> {
        let semaphore = PG_TEST_SEMAPHORE
            .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_PG_TESTS)))
            .clone();
        // The test will wait here until the semaphore is ready to hand out a permit
        let permit = semaphore
            .acquire_owned()
            .await
            .map_err(|_| DomainError::data_store("could not acquire semaphore permit"))?;

        let test_name = format!("testdb_{}", super::detect_test_name("::ds_pg"));

        {
            let master_config = test_pg_config("domainengine");
            recreate_database(&test_name, &master_config)
                .await
                .map_err(|err| DomainError::data_store(format!("{err}")))
                .with_context(|| "recreate database")?;
        }

        let test_config = test_pg_config(&test_name);
        let db_name = test_config.get_dbname().unwrap().to_string();

        let deadpool_manager = deadpool_postgres::Manager::from_config(
            test_config,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );

        let pool = deadpool_postgres::Pool::builder(deadpool_manager)
            .max_size(4)
            .build()
            .map_err(|err| DomainError::data_store(format!("deadpool: {err}")))?;

        let connection = domain_engine_store_pg::connect(pool, &db_name)
            .await
            .map_err(|err| DomainError::data_store(format!("{err:?}")))?;

        Ok(PgTestDatastoreConnection {
            connection,
            permit: Arc::new(permit),
        })
    }

    fn test_pg_config(dbname: &str) -> tokio_postgres::Config {
        let mut config = tokio_postgres::Config::default();
        let host = match env::var("DOMAIN_ENGINE_TEST_PG_HOST") {
            Ok(host) => host,
            Err(_) => "localhost".to_string(),
        };

        config
            .host(host)
            .port(5432)
            .dbname(dbname)
            .user("domainengine")
            .password("postgres");
        config
    }
}

#[allow(unused)]
fn detect_test_name(suffix: &str) -> String {
    let thread_name = std::thread::current().name().unwrap().to_string();
    let thread_name_no_suffix = thread_name.strip_suffix(suffix).unwrap();
    let test_name = thread_name_no_suffix.split("::").last().unwrap();

    test_name.to_string()
}
