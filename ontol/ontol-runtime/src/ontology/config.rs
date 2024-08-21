use std::collections::{BTreeMap, BTreeSet};

use ::serde::{Deserialize, Serialize};

use crate::{ontology::Ontology, PackageId};

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct PackageConfig {
    pub data_store: Option<DataStoreConfig>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub enum DataStoreConfig {
    Default,
    ByName(String),
}

pub fn data_store_backed_domains(
    ontology: &Ontology,
) -> impl Iterator<Item = (DataStoreConfig, BTreeSet<PackageId>)> {
    let mut configs: BTreeMap<DataStoreConfig, BTreeSet<PackageId>> = Default::default();

    for (package_id, _) in ontology.domains() {
        let Some(config) = ontology.get_package_config(package_id) else {
            continue;
        };
        let Some(ds_config) = config.data_store.clone() else {
            continue;
        };

        configs.entry(ds_config).or_default().insert(package_id);
    }

    configs.into_iter()
}
