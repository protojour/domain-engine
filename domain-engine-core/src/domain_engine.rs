use std::sync::Arc;

use ontol_runtime::{
    interface::serde::processor::ProcessorMode,
    ontology::{Ontology, PropertyCardinality, ValueCardinality},
    select::{EntitySelect, Select, StructOrUnionSelect},
    sequence::Sequence,
    value::{Attribute, Data, Value},
    var::Var,
    vm::{proc::Yield, VmState},
    DefId, MapKey, PackageId,
};
use tracing::debug;

use crate::{
    data_store::{DataStore, DataStoreFactory},
    domain_error::DomainResult,
    match_utils::find_entity_id_in_condition_for_var,
    resolve_path::{ProbeOptions, ResolverGraph},
    select_data_flow::translate_entity_select,
    system::{SystemAPI, TestSystem},
    value_generator::Generator,
    Config, DomainError, EntityQuery, FindEntityQuery,
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

    pub async fn exec_map(
        &self,
        key: [MapKey; 2],
        input: Attribute,
        queries: &mut (dyn FindEntityQuery + Send),
    ) -> DomainResult<Value> {
        let proc = self
            .ontology
            .get_mapper_proc(key)
            .ok_or(DomainError::MappingProcedureNotFound)?;
        let mut vm = self.ontology.new_vm(proc);

        let mut input_value = input.value;

        loop {
            match vm.run([input_value]) {
                VmState::Complete(value) => return Ok(value),
                VmState::Yielded(Yield::Match(match_var, condition)) => {
                    let mut entity_query = queries.find_query(match_var, &condition);

                    // Merge the condition into the select
                    assert!(entity_query.select.condition.clauses.is_empty());
                    entity_query.select.condition = condition;

                    input_value = self.exec_map_query(match_var, entity_query).await?;
                }
            }
        }
    }

    pub async fn query_entities(&self, mut select: EntitySelect) -> DomainResult<Sequence> {
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

        let mut edge_seq = data_store.api().query(select, self).await?;

        if resolve_path.is_empty() {
            return Ok(edge_seq);
        }

        // Transform result
        for next_def_id in resolve_path.reverse(original_def_id) {
            let procedure = ontology
                .get_mapper_proc([cur_def_id.into(), (next_def_id).into()])
                .expect("No mapping procedure for query output");

            for attr in edge_seq.attrs.iter_mut() {
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

        Ok(edge_seq)
    }

    pub async fn store_new_entity(&self, mut entity: Value, select: Select) -> DomainResult<Value> {
        // TODO: Domain translation by finding optimal mapping path

        Generator::new(self, ProcessorMode::Create).generate_values(&mut entity);

        self.get_data_store()?
            .api()
            .store_new_entity(entity, select, self)
            .await
    }

    async fn exec_map_query(
        &self,
        match_var: Var,
        mut entity_query: EntityQuery,
    ) -> DomainResult<ontol_runtime::value::Value> {
        let data_store = self.get_data_store()?;

        debug!("match condition:\n{:#?}", entity_query.select.condition);

        match &entity_query.select.source {
            StructOrUnionSelect::Struct(struct_select) => {
                let inner_entity_def_id =
                    find_entity_id_in_condition_for_var(&entity_query.select.condition, match_var)
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
                        },
                    )
                    .ok_or(DomainError::NoResolvePathToDataStore)?;

                if let Some(dest) = resolve_path.iter().last() {
                    assert_eq!(dest, inner_entity_def_id);
                }

                let mut cur_def_id = struct_select.def_id;

                // Transform select
                for next_def_id in resolve_path.iter() {
                    translate_entity_select(
                        &mut entity_query.select,
                        cur_def_id.into(),
                        next_def_id.into(),
                        &self.ontology,
                    );
                    cur_def_id = next_def_id;
                }
            }
            _ => todo!("Basically apply the same operation as above, but refactor"),
        }

        let edge_seq = data_store.api().query(entity_query.select, self).await?;

        debug!("cardinality: {:?}", entity_query.cardinality);

        match entity_query.cardinality.1 {
            ValueCardinality::One => match (
                edge_seq.attrs.into_iter().next(),
                entity_query.cardinality.0,
            ) {
                (Some(attribute), _) => Ok(attribute.value),
                (None, PropertyCardinality::Optional) => Ok(Value::unit()),
                (None, PropertyCardinality::Mandatory) => Err(DomainError::EntityNotFound),
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

    pub async fn build<F: DataStoreFactory>(self) -> DomainEngine {
        let data_store = match self.data_store {
            Some(data_store) => Some(data_store),
            None => {
                let mut data_store: Option<DataStore> = None;

                for (package_id, _) in self.ontology.domains() {
                    if let Some(config) = self.ontology.get_package_config(*package_id) {
                        if let Some(data_store_config) = &config.data_store {
                            let api =
                                F::new_api(data_store_config, &self.ontology, *package_id).await;
                            data_store = Some(DataStore::new(*package_id, api));
                        }
                    }
                }

                data_store
            }
        };

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
