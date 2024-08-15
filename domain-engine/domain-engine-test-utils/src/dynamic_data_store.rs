use std::{collections::BTreeSet, sync::Arc};

use domain_engine_core::{
    data_store::{DataStoreAPI, DataStoreFactory, DataStoreFactorySync},
    domain_error::DomainErrorKind,
    system::ArcSystemApi,
    DomainResult, Session,
};
use domain_engine_store_inmemory::InMemoryDataStoreFactory;
use ontol_runtime::{
    ontology::{config::DataStoreConfig, Ontology},
    PackageId,
};
use tracing::error;

pub struct DynamicDataStoreFactory {
    name: String,
    recreate_db: bool,
}

impl DynamicDataStoreFactory {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            recreate_db: true,
        }
    }

    pub fn reuse_db(mut self) -> Self {
        self.recreate_db = false;
        self
    }
}

#[async_trait::async_trait]
impl DataStoreFactory for DynamicDataStoreFactory {
    async fn new_api(
        &self,
        package_ids: &BTreeSet<PackageId>,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> DomainResult<Box<dyn DataStoreAPI + Send + Sync>> {
        let result = match self.name.as_str() {
            "arango" => {
                arango::ArangoTestDatastoreFactory {
                    recreate_db: self.recreate_db,
                }
                .new_api(package_ids, config, session, ontology, system)
                .await
            }
            "inmemory" => {
                InMemoryDataStoreFactory
                    .new_api(package_ids, config, session, ontology, system)
                    .await
            }
            "pg" => {
                pg::PgTestDatastoreFactory {
                    recreate_db: self.recreate_db,
                }
                .new_api(package_ids, config, session, ontology, system)
                .await
            }
            other => Err(DomainErrorKind::UnknownDataStore(other.to_string()).into_error()),
        };

        result.map_err(|err| {
            error!("datastore not created: {err:?}");
            err
        })
    }
}

impl DataStoreFactorySync for DynamicDataStoreFactory {
    fn new_api_sync(
        &self,
        package_ids: &BTreeSet<PackageId>,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> DomainResult<Box<dyn DataStoreAPI + Send + Sync>> {
        match self.name.as_str() {
            "inmemory" => InMemoryDataStoreFactory.new_api_sync(
                package_ids,
                config,
                session,
                ontology,
                system,
            ),
            other => panic!("cannot synchronously create `{other}` factory"),
        }
    }
}

mod arango {
    use std::{collections::BTreeSet, env};

    use domain_engine_core::{DomainResult, Session};
    use domain_engine_store_arango::ArangoDatabaseHandle;
    use ontol_runtime::ontology::Ontology;

    #[derive(Default)]
    pub struct ArangoTestDatastoreFactory {
        pub recreate_db: bool,
    }

    #[async_trait::async_trait]
    impl domain_engine_core::data_store::DataStoreFactory for ArangoTestDatastoreFactory {
        async fn new_api(
            &self,
            package_ids: &BTreeSet<ontol_runtime::PackageId>,
            _config: ontol_runtime::ontology::config::DataStoreConfig,
            _session: Session,
            ontology: std::sync::Arc<Ontology>,
            system: domain_engine_core::system::ArcSystemApi,
        ) -> DomainResult<Box<dyn domain_engine_core::data_store::DataStoreAPI + Send + Sync>>
        {
            let host = match env::var("DOMAIN_ENGINE_TEST_ARANGO_HOST") {
                Ok(host) => host,
                Err(_) => "localhost".to_string(),
            };
            let client = domain_engine_store_arango::ArangoClient::new(
                &format!("http://{host}:8529"),
                reqwest_middleware::ClientWithMiddleware::new(reqwest::Client::new(), vec![]),
            );
            let test_name = super::detect_test_name("::ds_arango");
            let mut db_name = &test_name[..];
            if db_name.len() >= 64 {
                db_name = &db_name[0..63];
            }
            if self.recreate_db {
                let _ = client.drop_database(db_name).await;
            }
            let mut db = client.db(db_name, ontology, system);
            db.init(package_ids, true).await.unwrap();
            Ok(Box::new(ArangoDatabaseHandle::from(db)))
        }
    }
}

mod pg {
    use std::collections::BTreeSet;
    use std::env;
    use std::sync::{Arc, OnceLock};

    use domain_engine_core::data_store::DataStoreAPI;
    use domain_engine_core::domain_error::DomainErrorContext;
    use domain_engine_core::transact::{ReqMessage, RespMessage, TransactionMode};
    use domain_engine_core::{DomainError, DomainResult, Session};
    use domain_engine_store_pg::{connect_and_migrate, recreate_database, PostgresHandle};
    use domain_engine_store_pg::{deadpool_postgres, tokio_postgres, PostgresDataStore};
    use futures_util::stream::BoxStream;
    use ontol_runtime::ontology::Ontology;
    use tokio::sync::{OwnedSemaphorePermit, Semaphore};

    /// Make sure not to overwhelm the test postgres instance.
    /// This number can be tweaked, it's about finding a sweet spot.
    /// A small number of concurrent tests should be fine,
    /// since the intention is for each test to run in a separate database (this is best effort though).
    const MAX_CONCURRENT_PG_TESTS: usize = 6;

    /// this global semaphore limits the number of PgTestDatastores that can exist at the same time.
    static PG_TEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

    #[derive(Default)]
    pub struct PgTestDatastoreFactory {
        pub recreate_db: bool,
    }

    struct PgTestDatastore {
        handle: PostgresHandle,
        /// As long as this datastore still lives, it will hold this permit
        #[allow(unused)]
        permit: OwnedSemaphorePermit,
    }

    #[async_trait::async_trait]
    impl domain_engine_core::data_store::DataStoreFactory for PgTestDatastoreFactory {
        async fn new_api(
            &self,
            package_ids: &BTreeSet<ontol_runtime::PackageId>,
            config: ontol_runtime::ontology::config::DataStoreConfig,
            session: Session,
            ontology: std::sync::Arc<Ontology>,
            system: domain_engine_core::system::ArcSystemApi,
        ) -> DomainResult<Box<dyn domain_engine_core::data_store::DataStoreAPI + Send + Sync>>
        {
            test_pg_api(
                package_ids,
                config,
                session,
                ontology,
                system,
                self.recreate_db,
            )
            .await
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
    }

    async fn test_pg_api(
        package_ids: &BTreeSet<ontol_runtime::PackageId>,
        _config: ontol_runtime::ontology::config::DataStoreConfig,
        _session: Session,
        ontology: std::sync::Arc<Ontology>,
        system: domain_engine_core::system::ArcSystemApi,
        recreate_db: bool,
    ) -> DomainResult<Box<dyn domain_engine_core::data_store::DataStoreAPI + Send + Sync>> {
        let semaphore = PG_TEST_SEMAPHORE
            .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_PG_TESTS)))
            .clone();
        // The test will wait here until the semaphore is ready to hand out a permit
        let permit = semaphore
            .acquire_owned()
            .await
            .map_err(|_| DomainError::data_store("could not acquire semaphore permit"))?;

        let test_name = format!("testdb_{}", super::detect_test_name("::ds_pg"));

        if recreate_db {
            let master_config = test_pg_config("postgres");
            recreate_database(&test_name, &master_config)
                .await
                .map_err(|err| DomainError::data_store(format!("{err}")))
                .with_context(|| "recreate database")?;
        }

        let test_config = test_pg_config(&test_name);

        let pg_model = connect_and_migrate(package_ids, ontology.as_ref(), &test_config)
            .await
            .map_err(DomainError::data_store_from_anyhow)
            .with_context(|| "connect and migrate")?;

        let deadpool_manager = deadpool_postgres::Manager::from_config(
            test_config,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );

        Ok(Box::new(PgTestDatastore {
            handle: PostgresDataStore::new(
                pg_model,
                ontology,
                deadpool_postgres::Pool::builder(deadpool_manager)
                    .max_size(1)
                    .build()
                    .map_err(|err| DomainError::data_store(format!("deadpool: {err}")))?,
                system,
            )
            .into(),
            permit,
        }))
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
            .user("postgres")
            .password("postgres");
        config
    }
}

fn detect_test_name(suffix: &str) -> String {
    let thread_name = std::thread::current().name().unwrap().to_string();
    let thread_name_no_suffix = thread_name.strip_suffix(suffix).unwrap();
    let test_name = thread_name_no_suffix.split("::").last().unwrap();

    test_name.to_string()
}
