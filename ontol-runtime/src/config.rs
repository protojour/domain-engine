use ::serde::{Deserialize, Serialize};
use smartstring::alias::String;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct PackageConfig {
    pub data_store: Option<DataStoreConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataStoreConfig {
    Default,
    ByName(String),
}
