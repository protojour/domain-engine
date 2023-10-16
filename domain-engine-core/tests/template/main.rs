use domain_engine_core::data_store::{DataStoreFactory, DefaultDataStoreFactory};

include!(concat!(env!("OUT_DIR"), "/data_store_tests.rs"));

struct TestDataStoreFactory;

impl DataStoreFactory for TestDataStoreFactory {
    fn new_api(
        config: &DataStoreConfig,
        ontology: &ontol_runtime::ontology::Ontology,
        package_id: ontol_runtime::PackageId,
    ) -> Box<dyn domain_engine_core::data_store::DataStoreAPI + Send + Sync> {
        DefaultDataStoreFactory::new_api(config, ontology, package_id)
    }
}
