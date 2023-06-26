use ontol_runtime::env::Domain;

use crate::data_source::DataSourceAPI;

pub struct InMemory {}

impl InMemory {
    pub fn from_domain(_domain: &Domain) -> Self {
        Self {}
    }
}

impl DataSourceAPI for InMemory {}
