mod data_store_tests {
    include!(concat!(env!("OUT_DIR"), "/data_store_tests.rs"));
}

type TestDataStoreFactory = domain_engine_core::data_store::DefaultDataStoreFactory;
