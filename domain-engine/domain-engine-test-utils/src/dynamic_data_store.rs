use std::sync::Arc;

use domain_engine_core::{
    data_store::{DataStoreAPI, DataStoreFactory, DataStoreFactorySync},
    system::ArcSystemApi,
    Session,
};
use domain_engine_store_inmemory::InMemoryDataStoreFactory;
use ontol_runtime::{
    ontology::{config::DataStoreConfig, Ontology},
    PackageId,
};

pub struct DynamicDataStoreFactory {
    name: String,
}

impl DynamicDataStoreFactory {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait::async_trait]
impl DataStoreFactory for DynamicDataStoreFactory {
    async fn new_api(
        &self,
        package_id: PackageId,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        match self.name.as_str() {
            "arango" => {
                arango::ArangoTestDatastoreFactory
                    .new_api(package_id, config, session, ontology, system)
                    .await
            }
            "inmemory" => {
                InMemoryDataStoreFactory
                    .new_api(package_id, config, session, ontology, system)
                    .await
            }
            "pg" => {
                pg::PgTestDatastoreFactory
                    .new_api(package_id, config, session, ontology, system)
                    .await
            }
            _ => panic!(),
        }
    }
}

impl DataStoreFactorySync for DynamicDataStoreFactory {
    fn new_api_sync(
        &self,
        package_id: PackageId,
        config: DataStoreConfig,
        session: Session,
        ontology: Arc<Ontology>,
        system: ArcSystemApi,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        match self.name.as_str() {
            "inmemory" => {
                InMemoryDataStoreFactory.new_api_sync(package_id, config, session, ontology, system)
            }
            other => panic!("cannot synchronously create `{other}` factory"),
        }
    }
}

mod arango {
    use domain_engine_core::Session;
    use ontol_runtime::ontology::Ontology;

    #[derive(Default)]
    pub struct ArangoTestDatastoreFactory;

    #[async_trait::async_trait]
    impl domain_engine_core::data_store::DataStoreFactory for ArangoTestDatastoreFactory {
        async fn new_api(
            &self,
            package_id: ontol_runtime::PackageId,
            _config: ontol_runtime::ontology::config::DataStoreConfig,
            _session: Session,
            ontology: std::sync::Arc<Ontology>,
            system: domain_engine_core::system::ArcSystemApi,
        ) -> anyhow::Result<Box<dyn domain_engine_core::data_store::DataStoreAPI + Send + Sync>>
        {
            let client = domain_engine_store_arango::ArangoClient::new(
                "http://localhost:8529",
                reqwest_middleware::ClientWithMiddleware::new(reqwest::Client::new(), vec![]),
            );
            let test_name = super::detect_test_name("::ds_arango");
            let mut db_name = &test_name[..];
            if db_name.len() >= 64 {
                db_name = &db_name[0..63];
            }
            let _ = client.drop_database(db_name).await;
            let mut db = client.db(db_name, ontology, system);
            db.init(package_id, true).await.unwrap();
            Ok(Box::new(db))
        }
    }
}

mod pg {
    use domain_engine_core::Session;
    use domain_engine_store_pg::migrate::connect_and_migrate;
    use domain_engine_store_pg::recreate_database;
    use domain_engine_store_pg::{deadpool_postgres, tokio_postgres, PostgresDataStore};
    use ontol_runtime::ontology::Ontology;

    #[derive(Default)]
    pub struct PgTestDatastoreFactory;

    #[async_trait::async_trait]
    impl domain_engine_core::data_store::DataStoreFactory for PgTestDatastoreFactory {
        async fn new_api(
            &self,
            package_id: ontol_runtime::PackageId,
            config: ontol_runtime::ontology::config::DataStoreConfig,
            session: Session,
            ontology: std::sync::Arc<Ontology>,
            system: domain_engine_core::system::ArcSystemApi,
        ) -> anyhow::Result<Box<dyn domain_engine_core::data_store::DataStoreAPI + Send + Sync>>
        {
            test_pg_api(package_id, config, session, ontology, system).await
        }
    }

    async fn test_pg_api(
        package_id: ontol_runtime::PackageId,
        _config: ontol_runtime::ontology::config::DataStoreConfig,
        _session: Session,
        ontology: std::sync::Arc<Ontology>,
        system: domain_engine_core::system::ArcSystemApi,
    ) -> anyhow::Result<Box<dyn domain_engine_core::data_store::DataStoreAPI + Send + Sync>> {
        let test_name = format!("testdb_{}", super::detect_test_name("::ds_pg"));

        {
            let master_config = test_pg_config("postgres");
            recreate_database(&test_name, &master_config).await?;
        }

        let test_config = test_pg_config(&test_name);

        connect_and_migrate(&[package_id], ontology.as_ref(), &test_config).await?;

        let deadpool_manager = deadpool_postgres::Manager::from_config(
            test_config,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );

        Ok(Box::new(PostgresDataStore {
            pool: deadpool_postgres::Pool::builder(deadpool_manager)
                .max_size(1)
                .build()?,
            system,
        }))
    }

    fn test_pg_config(dbname: &str) -> tokio_postgres::Config {
        let mut config = tokio_postgres::Config::default();
        config
            .host("localhost")
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
