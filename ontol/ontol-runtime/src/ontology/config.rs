use std::collections::{BTreeMap, BTreeSet};

use ::serde::{Deserialize, Serialize};

use crate::{ontology::Ontology, DomainIndex};

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct DomainConfig {
    pub data_store: Option<DataStoreConfig>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub enum DataStoreConfig {
    Default,
    ByName(String),
}

pub fn persisted_domains(
    ontology: &Ontology,
) -> impl Iterator<Item = (DataStoreConfig, BTreeSet<DomainIndex>)> {
    let mut configs: BTreeMap<DataStoreConfig, BTreeSet<DomainIndex>> = Default::default();

    for (index, _) in ontology.domains() {
        let Some(config) = ontology.get_domain_config(index) else {
            continue;
        };
        let Some(ds_config) = config.data_store.clone() else {
            continue;
        };

        configs.entry(ds_config).or_default().insert(index);
    }

    configs.into_iter()
}
