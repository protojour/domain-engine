use ::serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct PackageConfig {
    pub data_store: Option<DataStoreConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataStoreConfig {
    InMemory,
}
