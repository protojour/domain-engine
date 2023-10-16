use std::sync::Arc;

use ontol_runtime::{
    interface::serde::processor::ProcessorMode,
    ontology::Ontology,
    select::{EntitySelect, Select},
    value::{Attribute, Value},
    vm::{proc::Yield, VmState},
    MapKey, PackageId,
};
use tracing::debug;

use crate::{
    data_store::{DataStore, DataStoreFactory},
    domain_error::DomainResult,
    resolve_path::ResolverGraph,
    select_data_flow::translate_entity_select,
    system::{SystemAPI, TestSystem},
    value_generator::Generator,
    Config, DomainError,
};

pub struct DomainEngine {
    ontology: Arc<Ontology>,
    config: Arc<Config>,
    resolver_graph: ResolverGraph,
    data_store: Option<DataStore>,
    system: Box<dyn SystemAPI + Send + Sync>,
}

impl DomainEngine {
    pub fn builder(ontology: Arc<Ontology>) -> Builder {
        Builder {
            ontology,
            config: Config::default(),
            data_store: None,
            system: None,
        }
    }

    pub fn test_builder(ontology: Arc<Ontology>) -> Builder {
        Self::builder(ontology).system(Box::new(TestSystem))
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    pub fn ontology_owned(&self) -> Arc<Ontology> {
        self.ontology.clone()
    }

    pub fn system(&self) -> &dyn SystemAPI {
        self.system.as_ref()
    }

    fn get_data_store(&self) -> DomainResult<&DataStore> {
        self.data_store.as_ref().ok_or(DomainError::NoDataStore)
    }

    pub async fn call_map(&self, key: [MapKey; 2], input: Attribute) -> DomainResult<Attribute> {
        let proc = self
            .ontology
            .get_mapper_proc(key)
            .ok_or(DomainError::MappingProcedureNotFound)?;
        let mut vm = self.ontology.new_vm(proc);

        let input_value = input.value;

        loop {
            match vm.run([input_value]) {
                VmState::Complete(value) => return Ok(value.into()),
                VmState::Yielded(Yield::Match(condition)) => todo!("match {condition:?}"),
            }
        }
    }

    pub async fn query_entities(&self, mut select: EntitySelect) -> DomainResult<Vec<Attribute>> {
        let data_store = self.get_data_store()?;
        let ontology = self.ontology();

        let (mut cur_def_id, resolve_path) = self
            .resolver_graph
            .probe_path_for_entity_select(ontology, &select, data_store)
            .ok_or(DomainError::NoResolvePathToDataStore)?;

        let original_def_id = cur_def_id;

        // Transform select
        for next_def_id in resolve_path.iter() {
            translate_entity_select(&mut select, cur_def_id.into(), next_def_id.into(), ontology);
            cur_def_id = next_def_id;
        }

        debug!(
            "Resolve path: {resolve_path:?} to: {:?}",
            ontology.get_type_info(cur_def_id)
        );

        let mut edges = data_store.api().query(self, select).await?;

        if resolve_path.is_empty() {
            return Ok(edges);
        }

        // Transform result
        for next_def_id in resolve_path.reverse(original_def_id) {
            let procedure = ontology
                .get_mapper_proc([cur_def_id.into(), (next_def_id).into()])
                .expect("No mapping procedure for query output");

            for attr in edges.iter_mut() {
                let mut vm = ontology.new_vm(procedure);
                let param = attr.value.take();

                attr.value = loop {
                    match vm.run([param]) {
                        VmState::Complete(value) => {
                            break value;
                        }
                        VmState::Yielded(_yield) => {
                            todo!()
                        }
                    }
                };
            }

            cur_def_id = next_def_id;
        }

        assert_eq!(cur_def_id, original_def_id);

        Ok(edges)
    }

    pub async fn store_new_entity(&self, mut entity: Value, select: Select) -> DomainResult<Value> {
        // TODO: Domain translation by finding optimal mapping path

        Generator::new(self, ProcessorMode::Create).generate_values(&mut entity);

        self.get_data_store()?
            .api()
            .store_new_entity(self, entity, select)
            .await
    }
}

pub struct Builder {
    ontology: Arc<Ontology>,
    config: Config,
    data_store: Option<DataStore>,
    system: Option<Box<dyn SystemAPI + Send + Sync>>,
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

    pub fn system(mut self, system: Box<dyn SystemAPI + Send + Sync>) -> Self {
        self.system = Some(system);
        self
    }

    pub fn mock_data_store(mut self, package_id: PackageId, setup: impl unimock::Clause) -> Self {
        self.data_store = Some(DataStore::new(
            package_id,
            Box::new(unimock::Unimock::new(setup)),
        ));
        self
    }

    pub fn build<F: DataStoreFactory>(self) -> DomainEngine {
        let data_store = self.data_store.or_else(|| {
            let mut data_store: Option<DataStore> = None;

            for (package_id, _) in self.ontology.domains() {
                if let Some(config) = self.ontology.get_package_config(*package_id) {
                    if let Some(data_store_config) = &config.data_store {
                        let api = F::new_api(data_store_config, &self.ontology, *package_id);
                        data_store = Some(DataStore::new(*package_id, api));
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
            system: self.system.expect("No system API provided!"),
        }
    }
}
