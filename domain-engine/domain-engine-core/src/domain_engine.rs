use std::sync::Arc;

use anyhow::{anyhow, Context};
use ontol_runtime::{
    interface::serde::processor::ProcessorMode,
    ontology::{config::data_store_backed_domains, map::Extern, Ontology},
    property::ValueCardinality,
    query::select::{EntitySelect, Select, StructOrUnionSelect},
    resolve_path::{ProbeDirection, ProbeFilter, ProbeOptions, ResolvePath, ResolverGraph},
    sequence::Sequence,
    value::Value,
    vm::{ontol_vm::OntolVm, proc::Yield, VmState},
    DefId, MapKey, PackageId,
};
use serde::de::DeserializeSeed;
use tracing::{debug, error, trace};

use crate::{
    data_store::{
        self, BatchWriteRequest, BatchWriteResponse, DataStore, DataStoreFactory,
        DataStoreFactorySync,
    },
    domain_error::DomainResult,
    select_data_flow::{translate_entity_select, translate_select},
    system::{ArcSystemApi, SystemAPI},
    update::sanitize_update,
    DomainError, FindEntitySelect, MaybeSelect, Session,
};

pub struct DomainEngine {
    ontology: Arc<Ontology>,
    resolver_graph: ResolverGraph,
    data_store: Option<DataStore>,
    system: ArcSystemApi,
}

impl DomainEngine {
    pub fn builder(ontology: Arc<Ontology>) -> Builder {
        Builder {
            ontology,
            data_store: None,
            system: None,
        }
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
        key: MapKey,
        input: Value,
        selects: &mut (dyn FindEntitySelect + Send),
        session: Session,
    ) -> DomainResult<Value> {
        let proc = self
            .ontology
            .get_mapper_proc(&key)
            .ok_or(DomainError::MappingProcedureNotFound)?;
        let mut vm = self.ontology.new_vm(proc);

        self.run_vm_to_completion(&mut vm, input, &mut Some(selects), session)
            .await
    }

    pub async fn query_entities(
        &self,
        mut select: EntitySelect,
        session: Session,
    ) -> DomainResult<Sequence> {
        let data_store = self.get_data_store()?;
        let ontology = self.ontology();

        let resolve_path = self
            .resolver_graph
            .probe_path_for_entity_select(ontology, &select, data_store.package_id())
            .ok_or(DomainError::NoResolvePathToDataStore)?;

        // Transform select
        for map_key in resolve_path.iter() {
            translate_entity_select(&mut select, &map_key, ontology);
        }

        let data_store::Response::Query(mut edge_seq) = data_store
            .api()
            .execute(data_store::Request::Query(select), session.clone())
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
        for map_key in resolve_path.reverse() {
            let procedure = ontology
                .get_mapper_proc(&map_key)
                .expect("No mapping procedure for query output");

            for attr in edge_seq.attrs.iter_mut() {
                let mut vm = ontology.new_vm(procedure);

                attr.val = self
                    .run_vm_to_completion(&mut vm, attr.val.take(), &mut None, session.clone())
                    .await?;
            }
        }

        Ok(edge_seq)
    }

    pub async fn execute_writes(
        &self,
        mut requests: Vec<data_store::BatchWriteRequest>,
        session: Session,
    ) -> DomainResult<Vec<data_store::BatchWriteResponse>> {
        let data_store = self.get_data_store()?;
        let ontology = self.ontology();

        // resolve paths for post-data-store mapping
        let mut ordered_resolve_paths: Vec<Option<ResolvePath>> =
            Vec::with_capacity(requests.len());

        for request in &mut requests {
            match request {
                BatchWriteRequest::Insert(mut_values, select) => {
                    let input_path = self
                        .resolver_graph
                        .probe_path_for_select(
                            ontology,
                            select,
                            data_store.package_id(),
                            ProbeDirection::ByInput,
                            ProbeFilter::Complete,
                        )
                        .ok_or(DomainError::NoResolvePathToDataStore)?;

                    for map_key in input_path.iter() {
                        let procedure = ontology
                            .get_mapper_proc(&map_key)
                            .expect("No mapping procedure for query output");

                        for value in mut_values.iter_mut() {
                            let mut vm = ontology.new_vm(procedure);

                            *value = self
                                .run_vm_to_completion(
                                    &mut vm,
                                    value.take(),
                                    &mut None,
                                    session.clone(),
                                )
                                .await?;
                        }
                    }

                    let select_path = self
                        .resolver_graph
                        .probe_path_for_select(
                            ontology,
                            select,
                            data_store.package_id(),
                            ProbeDirection::ByOutput,
                            ProbeFilter::Complete,
                        )
                        .ok_or(DomainError::NoResolvePathToDataStore)?;

                    for map_key in select_path.iter() {
                        translate_select(select, &map_key, ontology);
                    }

                    ordered_resolve_paths.push(Some(select_path));
                }
                BatchWriteRequest::Update(mut_values, select) => {
                    let input_path = self
                        .resolver_graph
                        .probe_path_for_select(
                            ontology,
                            select,
                            data_store.package_id(),
                            ProbeDirection::ByInput,
                            ProbeFilter::Pure,
                        )
                        .ok_or(DomainError::NoResolvePathToDataStore)?;

                    debug!("UPDATE input resolve path: {input_path:?}");

                    for map_key in input_path.iter() {
                        let procedure = ontology
                            .get_mapper_proc(&map_key)
                            .expect("No mapping procedure for query output");

                        for value in mut_values.iter_mut() {
                            let mut vm = ontology.new_vm(procedure);

                            *value = self
                                .run_vm_to_completion(
                                    &mut vm,
                                    value.take(),
                                    &mut None,
                                    session.clone(),
                                )
                                .await?;
                        }
                    }

                    for value in mut_values.iter_mut() {
                        sanitize_update(value);
                    }

                    let select_path = self
                        .resolver_graph
                        .probe_path_for_select(
                            ontology,
                            select,
                            data_store.package_id(),
                            ProbeDirection::ByOutput,
                            ProbeFilter::Complete,
                        )
                        .ok_or(DomainError::NoResolvePathToDataStore)?;

                    for map_key in select_path.iter() {
                        translate_select(select, &map_key, ontology);
                    }

                    ordered_resolve_paths.push(Some(select_path));
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
                                    direction: ProbeDirection::ByOutput,
                                    filter: ProbeFilter::Complete,
                                },
                            )
                            .ok_or(DomainError::NoResolvePathToDataStore)?;
                        *def_id = path.iter().last().unwrap().input.def_id;
                    }
                    ordered_resolve_paths.push(None);
                }
            }
        }

        let data_store::Response::BatchWrite(mut responses) = data_store
            .api()
            .execute(data_store::Request::BatchWrite(requests), session.clone())
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
                        for map_key in resolve_path.iter().rev() {
                            trace!("post-mutation translation {map_key:?}");

                            let procedure = ontology
                                .get_mapper_proc(&map_key)
                                .expect("No mapping procedure for query output");

                            for value in mut_values.iter_mut() {
                                let mut vm = ontology.new_vm(procedure);

                                *value = self
                                    .run_vm_to_completion(
                                        &mut vm,
                                        value.take(),
                                        &mut None,
                                        session.clone(),
                                    )
                                    .await?;
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
    pub async fn store_new_entity(
        &self,
        entity: Value,
        select: Select,
        session: Session,
    ) -> DomainResult<Value> {
        let write_responses = self
            .execute_writes(
                vec![BatchWriteRequest::Insert(vec![entity], select)],
                session,
            )
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

    async fn run_vm_to_completion(
        &self,
        vm: &mut OntolVm<'_>,
        mut param: Value,
        selects: &mut Option<&mut (dyn FindEntitySelect + Send)>,
        session: Session,
    ) -> DomainResult<Value> {
        loop {
            match vm.run([param])? {
                VmState::Complete(value) => return Ok(value),
                VmState::Yield(vm_yield) => {
                    param = self.exec_yield(vm_yield, selects, session.clone()).await?;
                }
            }
        }
    }

    async fn exec_yield(
        &self,
        vm_yield: Yield,
        selects: &mut Option<&mut (dyn FindEntitySelect + Send)>,
        session: Session,
    ) -> DomainResult<Value> {
        match vm_yield {
            Yield::Match(match_var, value_cardinality, filter) => {
                if let Some(selects) = selects {
                    match selects.find_select(match_var, filter.condition()) {
                        MaybeSelect::Select(mut entity_select) => {
                            // Merge the filter into the select
                            assert!(entity_select.filter.condition().expansions().is_empty());
                            entity_select.filter = filter;

                            self.exec_map_query(value_cardinality, entity_select, session.clone())
                                .await
                        }
                        MaybeSelect::Skip(def_id) => {
                            debug!("skipping selection");
                            match value_cardinality {
                                ValueCardinality::Unit => Ok(Value::unit()),
                                ValueCardinality::OrderedSet | ValueCardinality::List => {
                                    Ok(Value::Sequence(Sequence::default(), def_id))
                                }
                            }
                        }
                    }
                } else {
                    Err(DomainError::ImpureMapping)
                }
            }
            Yield::CallExtern(extern_def_id, input, output_def_id) => {
                let ontology = &self.ontology;
                let input_operator_addr = ontology
                    .get_type_info(input.type_def_id())
                    .operator_addr
                    .ok_or(DomainError::SerializationFailed)?;
                let output_operator_addr = ontology
                    .get_type_info(output_def_id)
                    .operator_addr
                    .ok_or_else(|| {
                        error!("No deserialization operator");
                        DomainError::DeserializationFailed
                    })?;

                match self
                    .ontology
                    .get_extern(extern_def_id)
                    .ok_or(DomainError::MappingProcedureNotFound)?
                {
                    Extern::HttpJson { url } => {
                        let url = &self.ontology[*url];
                        let mut input_json: Vec<u8> = vec![];
                        self.ontology
                            .new_serde_processor(input_operator_addr, ProcessorMode::Read)
                            .serialize_value(
                                &input,
                                None,
                                &mut serde_json::Serializer::new(&mut input_json),
                            )
                            .map_err(|_| DomainError::SerializationFailed)?;

                        let output_json = self
                            .system
                            .call_http_json_hook(url, session, input_json)
                            .await?;

                        debug!("output json: `{output_json:?}`");

                        let output_attr = self
                            .ontology
                            .new_serde_processor(output_operator_addr, ProcessorMode::Read)
                            .deserialize(&mut serde_json::Deserializer::from_slice(&output_json))
                            .map_err(|error| {
                                debug!("hook deserialization error: {error:?}");
                                DomainError::DeserializationFailed
                            })?;

                        Ok(output_attr.val)
                    }
                }
            }
        }
    }

    async fn exec_map_query(
        &self,
        value_cardinality: ValueCardinality,
        mut entity_select: EntitySelect,
        session: Session,
    ) -> DomainResult<ontol_runtime::value::Value> {
        let data_store = self.get_data_store()?;

        debug!("match filter:\n{:#?}", entity_select.filter);

        match &entity_select.source {
            StructOrUnionSelect::Struct(struct_select) => {
                let inner_entity_def_id = entity_select
                    .filter
                    .condition()
                    .root_def_id()
                    .expect("Root entity DefId not found in condition clauses");

                debug!("exec_map_query: inner entity def id: {inner_entity_def_id:?}. struct_select.def_id: {:?}", struct_select.def_id);

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
                            direction: ProbeDirection::ByOutput,
                            filter: ProbeFilter::Complete,
                        },
                    )
                    .ok_or(DomainError::NoResolvePathToDataStore)?;

                if let Some(map_key) = resolve_path.iter().last() {
                    assert_eq!(map_key.input.def_id, inner_entity_def_id);
                }

                // Transform select
                for map_key in resolve_path.iter() {
                    translate_entity_select(&mut entity_select, &map_key, &self.ontology);
                }
            }
            _ => todo!("Basically apply the same operation as above, but refactor"),
        }

        let data_store::Response::Query(edge_seq) = data_store
            .api()
            .execute(data_store::Request::Query(entity_select), session)
            .await?
        else {
            return Err(DomainError::DataStore(anyhow!(
                "data store returned invalid response"
            )));
        };

        match value_cardinality {
            ValueCardinality::Unit => match edge_seq.attrs.into_iter().next() {
                Some(attribute) => Ok(attribute.val),
                None => Ok(Value::unit()),
            },
            ValueCardinality::OrderedSet | ValueCardinality::List => {
                Ok(Value::Sequence(edge_seq, DefId::unit()))
            }
        }
    }
}

pub struct Builder {
    ontology: Arc<Ontology>,
    data_store: Option<DataStore>,
    system: Option<ArcSystemApi>,
}

impl Builder {
    pub fn data_store(mut self, data_store: DataStore) -> Self {
        self.data_store = Some(data_store);
        self
    }

    pub fn system(mut self, system: Box<dyn SystemAPI + Send + Sync>) -> Self {
        self.system = Some(system.into());
        self
    }

    pub fn mock_data_store(mut self, package_id: PackageId, setup: impl unimock::Clause) -> Self {
        self.data_store = Some(DataStore::new(
            package_id,
            Box::new(unimock::Unimock::new(setup)),
        ));
        self
    }

    pub async fn build<F: DataStoreFactory>(
        self,
        factory: F,
        session: Session,
    ) -> DomainResult<DomainEngine> {
        let system = self.system.expect("No system API provided!");

        let data_store = match self.data_store {
            Some(data_store) => Some(data_store),
            None => {
                let mut data_store: Option<DataStore> = None;

                for (package_id, config) in data_store_backed_domains(&self.ontology) {
                    let api = factory
                        .new_api(
                            package_id,
                            config.clone(),
                            session.clone(),
                            self.ontology.clone(),
                            system.clone(),
                        )
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
            resolver_graph,
            data_store,
            system,
        })
    }

    pub fn build_sync<F: DataStoreFactorySync>(
        self,
        factory: F,
        session: Session,
    ) -> DomainResult<DomainEngine> {
        let system = self.system.expect("No system API provided!");

        let data_store = match self.data_store {
            Some(data_store) => Some(data_store),
            None => {
                let mut data_store: Option<DataStore> = None;

                for (package_id, config) in data_store_backed_domains(&self.ontology) {
                    let api = factory
                        .new_api_sync(
                            package_id,
                            config.clone(),
                            session.clone(),
                            self.ontology.clone(),
                            system.clone(),
                        )
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
            resolver_graph,
            data_store,
            system,
        })
    }
}
