#[derive(Default, Debug)]
pub struct PackageConfig {
    pub data_store: Option<DataStoreConfig>,
}

#[derive(Clone, Debug)]
pub enum DataStoreConfig {
    InMemory,
}
