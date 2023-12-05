use ::serde::{Deserialize, Serialize};
use smartstring::alias::String;

use crate::{ontology::Ontology, PackageId};

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct PackageConfig {
    pub data_store: Option<DataStoreConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataStoreConfig {
    Default,
    ByName(String),
}

pub fn data_store_backed_domains(
    ontology: &Ontology,
) -> impl Iterator<Item = (PackageId, &DataStoreConfig)> {
    ontology.domains().filter_map(|(package_id, _domain)| {
        ontology
            .get_package_config(*package_id)
            .and_then(|config| config.data_store.as_ref())
            .map(|data_store_config| (*package_id, data_store_config))
    })
}
