use std::sync::Arc;

use ontol_runtime::{
    config::DataStoreConfig,
    ontology::Ontology,
    query::{EntityQuery, Query},
    value::{Attribute, Value},
    PackageId,
};
use tracing::debug;

use crate::{
    data_store::DataStore, domain_error::DomainResult, in_memory_store::api::InMemoryDb,
    query_data_flow::translate_entity_query, resolve_path::ResolverGraph, Config, DomainError,
};

pub struct DomainEngine {
    ontology: Arc<Ontology>,
    config: Arc<Config>,
    resolver_graph: ResolverGraph,

    #[allow(unused)]
    data_store: Option<DataStore>,
}

impl DomainEngine {
    pub fn builder(ontology: Arc<Ontology>) -> Builder {
        Builder {
            ontology,
            config: Config::default(),
            data_store: None,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    fn get_data_store(&self) -> DomainResult<&DataStore> {
        self.data_store.as_ref().ok_or(DomainError::NoDataStore)
    }

    pub async fn query_entities(&self, mut query: EntityQuery) -> DomainResult<Vec<Attribute>> {
        let data_store = self.get_data_store()?;
        let ontology = self.ontology();

        let (mut cur_def_id, resolve_path) = self
            .resolver_graph
            .probe_path_for_entity_query(ontology, &query, data_store)
            .ok_or(DomainError::NoResolvePathToDataStore)?;

        let original_def_id = cur_def_id;

        // Transform query
        for next_def_id in resolve_path.iter() {
            translate_entity_query(&mut query, cur_def_id.into(), next_def_id.into(), ontology);
            cur_def_id = next_def_id;
        }

        debug!(
            "Resolve path: {resolve_path:?} to: {:?}",
            ontology.get_type_info(cur_def_id)
        );

        let mut edges = data_store.api().query(self, query).await?;

        if resolve_path.is_empty() {
            return Ok(edges);
        }

        // Transform result
        for next_def_id in resolve_path.reverse(original_def_id) {
            let procedure = ontology
                .get_mapper_proc(cur_def_id.into(), (next_def_id).into())
                .expect("No mapping procedure for query output");

            for attr in edges.iter_mut() {
                attr.value = ontology.new_vm().eval(procedure, [attr.value.take()]);
            }

            cur_def_id = next_def_id;
        }

        assert_eq!(cur_def_id, original_def_id);

        Ok(edges)
    }

    pub async fn store_new_entity(&self, entity: Value, query: Query) -> DomainResult<Value> {
        // TODO: Domain translation by finding optimal mapping path
        self.get_data_store()?
            .api()
            .store_new_entity(self, entity, query)
            .await
    }
}

pub struct Builder {
    ontology: Arc<Ontology>,
    config: Config,
    data_store: Option<DataStore>,
}

impl Builder {
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn data_store(mut self, data_store: DataStore) -> Self {
        self.data_store = Some(data_store);
        self
    }

    pub fn mock_data_store(mut self, package_id: PackageId, setup: impl unimock::Clause) -> Self {
        self.data_store = Some(DataStore::new(
            package_id,
            Box::new(unimock::Unimock::new(setup)),
        ));
        self
    }

    pub fn build(self) -> DomainEngine {
        let data_store = self.data_store.or_else(|| {
            let mut data_store: Option<DataStore> = None;

            for (package_id, _) in self.ontology.domains() {
                if let Some(config) = self.ontology.get_package_config(*package_id) {
                    if let Some(DataStoreConfig::InMemory) = config.data_store {
                        data_store = Some(DataStore::new(
                            *package_id,
                            Box::new(InMemoryDb::new(&self.ontology, *package_id)),
                        ));
                    }
                }
            }

            data_store
        });

        let resolver_graph = ResolverGraph::new(&self.ontology);

        DomainEngine {
            ontology: self.ontology,
            config: Arc::new(self.config),
            resolver_graph,
            data_store,
        }
    }
}
