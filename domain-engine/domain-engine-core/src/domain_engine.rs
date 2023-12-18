use std::sync::Arc;

use anyhow::{anyhow, Context};
use ontol_runtime::{
    config::data_store_backed_domains,
    interface::serde::processor::ProcessorMode,
    ontology::{Ontology, ValueCardinality},
    resolve_path::{ProbeOptions, ResolvePath, ResolverGraph},
    select::{EntitySelect, Select, StructOrUnionSelect},
    sequence::Sequence,
    value::{Data, Value},
    var::Var,
    vm::{proc::Yield, VmState},
    DefId, MapKey, PackageId,
};
use tracing::debug;

use crate::{
    data_store::{
        self, BatchWriteRequest, BatchWriteResponse, DataStore, DataStoreFactory,
        DataStoreFactorySync,
    },
    domain_error::DomainResult,
    match_utils::find_entity_id_in_condition_for_var,
    select_data_flow::{translate_entity_select, translate_select},
    system::{SystemAPI, TestSystem},
    value_generator::Generator,
    Config, DomainError, FindEntitySelect,
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
        Self::builder(ontology).system(Box::<TestSystem>::default())
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

    pub fn get_data_store(&self) -> DomainResult<&DataStore> {
        self.data_store.as_ref().ok_or(DomainError::NoDataStore)
    }

    pub async fn exec_map(
        &self,
        key: [MapKey; 2],
        mut input: Value,
        selects: &mut (dyn FindEntitySelect + Send),
    ) -> DomainResult<Value> {
        let proc = self
            .ontology
            .get_mapper_proc(key)
            .ok_or(DomainError::MappingProcedureNotFound)?;
        let mut vm = self.ontology.new_vm(proc);

        loop {
            match vm.run([input])? {
                VmState::Complete(value) => return Ok(value),
                VmState::Yielded(Yield::Match(match_var, value_cardinality, condition)) => {
                    let mut entity_select = selects.find_select(match_var, &condition);

                    // Merge the condition into the select
                    assert!(entity_select.condition.clauses.is_empty());
                    entity_select.condition = condition;

                    input = self
                        .exec_map_query(match_var, value_cardinality, entity_select)
                        .await?;
                }
            }
        }
    }

    pub async fn query_entities(&self, mut select: EntitySelect) -> DomainResult<Sequence> {
        let data_store = self.get_data_store()?;
        let ontology = self.ontology();

        let resolve_path = self
            .resolver_graph
            .probe_path_for_entity_select(ontology, &select, data_store.package_id())
            .ok_or(DomainError::NoResolvePathToDataStore)?;

        // Transform select
        for (from, to) in resolve_path.iter() {
            translate_entity_select(&mut select, from.into(), to.into(), ontology);
        }

        let data_store::Response::Query(mut edge_seq) = data_store
            .api()
            .execute(data_store::Request::Query(select), self)
            .await?
        else {
            return Err(DomainError::DataStore(anyhow!(
                "data store returned invalid response"
            )));
        };

        if resolve_path.is_empty() {
            return Ok(edge_seq);
        }

        // Transform result
        for (from, to) in resolve_path.reverse() {
            let procedure = ontology
                .get_mapper_proc([from.into(), to.into()])
                .expect("No mapping procedure for query output");

            for attr in edge_seq.attrs.iter_mut() {
                let mut vm = ontology.new_vm(procedure);
                let param = attr.value.take();

                attr.value = match vm.run([param])? {
                    VmState::Complete(value) => value,
                    VmState::Yielded(_yield) => return Err(DomainError::ImpureMapping),
                };
            }
        }

        Ok(edge_seq)
    }

    pub async fn execute_writes(
        &self,
        mut requests: Vec<data_store::BatchWriteRequest>,
    ) -> DomainResult<Vec<data_store::BatchWriteResponse>> {
        // TODO: Domain translation by finding optimal mapping path

        let data_store = self.get_data_store()?;
        let ontology = self.ontology();

        // resolve paths for post-data-store mapping
        let mut ordered_resolve_paths: Vec<Option<ResolvePath>> =
            Vec::with_capacity(requests.len());

        for request in &mut requests {
            match request {
                BatchWriteRequest::Insert(mut_values, select) => {
                    let resolve_path = self
                        .resolver_graph
                        .probe_path_for_select(ontology, select, data_store.package_id())
                        .ok_or(DomainError::NoResolvePathToDataStore)?;

                    for (from, to) in resolve_path.iter() {
                        translate_select(select, from.into(), to.into(), ontology);
                    }

                    for (from, to) in resolve_path.iter() {
                        let procedure = ontology
                            .get_mapper_proc([from.into(), to.into()])
                            .expect("No mapping procedure for query output");

                        for value in mut_values.iter_mut() {
                            let mut vm = ontology.new_vm(procedure);
                            let param = value.take();

                            *value = match vm.run([param])? {
                                VmState::Complete(value) => value,
                                VmState::Yielded(_yield) => {
                                    return Err(DomainError::ImpureMapping);
                                }
                            };
                        }
                    }

                    for mut_value in mut_values.iter_mut() {
                        Generator::new(self, ProcessorMode::Create).generate_values(mut_value);
                    }
                    ordered_resolve_paths.push(Some(resolve_path));
                }
                BatchWriteRequest::Update(mut_values, _) => {
                    // TODO: domain translate
                    for mut_value in mut_values {
                        Generator::new(self, ProcessorMode::Update).generate_values(mut_value);
                    }
                    ordered_resolve_paths.push(None);
                }
                BatchWriteRequest::Delete(_ids, def_id) => {
                    if def_id.package_id() != data_store.package_id() {
                        // TODO: Do ids need to be translated?
                        let path = self
                            .resolver_graph
                            .probe_path(
                                &self.ontology,
                                *def_id,
                                data_store.package_id(),
                                ProbeOptions {
                                    must_be_entity: true,
                                    inverted: true,
                                    filter_lossiness: None,
                                },
                            )
                            .ok_or(DomainError::NoResolvePathToDataStore)?;
                        *def_id = path.iter().last().unwrap().1;
                    }
                    ordered_resolve_paths.push(None);
                }
            }
        }

        let data_store::Response::BatchWrite(mut responses) = data_store
            .api()
            .execute(data_store::Request::BatchWrite(requests), self)
            .await?
        else {
            return Err(DomainError::DataStore(anyhow!(
                "data store returned invalid response"
            )));
        };

        for (response, resolve_path) in responses.iter_mut().zip(ordered_resolve_paths) {
            match response {
                BatchWriteResponse::Inserted(mut_values)
                | BatchWriteResponse::Updated(mut_values) => {
                    if let Some(resolve_path) = resolve_path {
                        for (from, to) in resolve_path.reverse() {
                            let procedure = ontology
                                .get_mapper_proc([from.into(), to.into()])
                                .expect("No mapping procedure for query output");

                            for value in mut_values.iter_mut() {
                                let mut vm = ontology.new_vm(procedure);
                                let param = value.take();

                                *value = match vm.run([param])? {
                                    VmState::Complete(value) => value,
                                    VmState::Yielded(_yield) => {
                                        return Err(DomainError::ImpureMapping);
                                    }
                                };
                            }
                        }
                    }
                }
                BatchWriteResponse::Deleted(..) => {}
            }
        }

        Ok(responses)
    }

    /// Shorthand for storing one entity
    pub async fn store_new_entity(&self, entity: Value, select: Select) -> DomainResult<Value> {
        let write_responses = self
            .execute_writes(vec![BatchWriteRequest::Insert(vec![entity], select)])
            .await?;

        let BatchWriteResponse::Inserted(entities) = write_responses
            .into_iter()
            .next()
            .ok_or_else(|| DomainError::DataStore(anyhow!("Nothing got inserted")))?
        else {
            return Err(DomainError::DataStore(anyhow!(
                "Expected inserted entities"
            )));
        };

        entities
            .into_iter()
            .next()
            .ok_or_else(|| DomainError::DataStore(anyhow!("No entity inserted")))
    }

    async fn exec_map_query(
        &self,
        match_var: Var,
        value_cardinality: ValueCardinality,
        mut entity_select: EntitySelect,
    ) -> DomainResult<ontol_runtime::value::Value> {
        let data_store = self.get_data_store()?;

        debug!("match condition:\n{:#?}", entity_select.condition);

        match &entity_select.source {
            StructOrUnionSelect::Struct(struct_select) => {
                let inner_entity_def_id =
                    find_entity_id_in_condition_for_var(&entity_select.condition, match_var)
                        .expect("Root entity DefId not found in condition clauses");

                // TODO: The probe algorithm here needs to work differently.
                // The map statement (currently) knows about the data store type
                // because the conditions must be expressed in its domain.
                // So we don't need to search for a target type, only a valid resolve
                // path _between_ the two known types
                let resolve_path = self
                    .resolver_graph
                    .probe_path(
                        &self.ontology,
                        struct_select.def_id,
                        data_store.package_id(),
                        ProbeOptions {
                            must_be_entity: true,
                            inverted: true,
                            filter_lossiness: None,
                        },
                    )
                    .ok_or(DomainError::NoResolvePathToDataStore)?;

                if let Some((_from, to)) = resolve_path.iter().last() {
                    assert_eq!(to, inner_entity_def_id);
                }

                // Transform select
                for (from, to) in resolve_path.iter() {
                    translate_entity_select(
                        &mut entity_select,
                        from.into(),
                        to.into(),
                        &self.ontology,
                    );
                }
            }
            _ => todo!("Basically apply the same operation as above, but refactor"),
        }

        let data_store::Response::Query(edge_seq) = data_store
            .api()
            .execute(data_store::Request::Query(entity_select), self)
            .await?
        else {
            return Err(DomainError::DataStore(anyhow!(
                "data store returned invalid response"
            )));
        };

        match value_cardinality {
            ValueCardinality::One => match edge_seq.attrs.into_iter().next() {
                Some(attribute) => Ok(attribute.value),
                None => Ok(Value::unit()),
            },
            ValueCardinality::Many => Ok(Value {
                data: Data::Sequence(edge_seq),
                type_def_id: DefId::unit(),
            }),
        }
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

    pub async fn build<F: DataStoreFactory>(self, factory: F) -> DomainResult<DomainEngine> {
        let data_store = match self.data_store {
            Some(data_store) => Some(data_store),
            None => {
                let mut data_store: Option<DataStore> = None;

                for (package_id, config) in data_store_backed_domains(&self.ontology) {
                    let api = factory
                        .new_api(config, &self.ontology, package_id)
                        .await
                        .with_context(|| format!("Failed to initialize data store {config:?}"))
                        .map_err(DomainError::DataStore)?;
                    data_store = Some(DataStore::new(package_id, api));
                }

                data_store
            }
        };

        let resolver_graph = ResolverGraph::from_ontology(&self.ontology);

        Ok(DomainEngine {
            ontology: self.ontology,
            config: Arc::new(self.config),
            resolver_graph,
            data_store,
            system: self.system.expect("No system API provided!"),
        })
    }

    pub fn build_sync<F: DataStoreFactorySync>(self, factory: F) -> DomainResult<DomainEngine> {
        let data_store = match self.data_store {
            Some(data_store) => Some(data_store),
            None => {
                let mut data_store: Option<DataStore> = None;

                for (package_id, config) in data_store_backed_domains(&self.ontology) {
                    let api = factory
                        .new_api_sync(config, &self.ontology, package_id)
                        .with_context(|| format!("Failed to initialize data store {config:?}"))
                        .map_err(DomainError::DataStore)?;
                    data_store = Some(DataStore::new(package_id, api));
                }

                data_store
            }
        };

        let resolver_graph = ResolverGraph::from_ontology(&self.ontology);

        Ok(DomainEngine {
            ontology: self.ontology,
            config: Arc::new(self.config),
            resolver_graph,
            data_store,
            system: self.system.expect("No system API provided!"),
        })
    }
}
