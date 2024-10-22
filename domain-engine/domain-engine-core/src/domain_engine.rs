use std::sync::Arc;

use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use ontol_runtime::{
    attr::AttrRef,
    interface::serde::processor::ProcessorMode,
    ontology::{config::persisted_domains, domain::DataRelationshipKind, map::Extern, Ontology},
    property::ValueCardinality,
    query::{
        condition::Condition,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    resolve_path::{ProbeDirection, ProbeFilter, ProbeOptions, ResolverGraph},
    sequence::Sequence,
    value::{Value, ValueTag},
    vm::{ontol_vm::OntolVm, proc::Yield, VmState},
    MapKey,
};
use serde::de::DeserializeSeed;
use tracing::{debug, error};

use crate::{
    data_store::{DataStore, DataStoreFactory, DataStoreFactorySync, DataStoreParams},
    domain_error::{DomainErrorContext, DomainErrorKind, DomainResult},
    select_data_flow::translate_entity_select,
    system::{ArcSystemApi, SystemAPI},
    transact::{AccumulateSequences, ReqMessage, RespMessage, TransactionMode, UpMap},
    DomainError, FindEntitySelect, SelectMode, Session,
};

pub struct DomainEngine {
    ontology: Arc<Ontology>,
    pub(crate) resolver_graph: ResolverGraph,
    data_store: Option<DataStore>,
    system: ArcSystemApi,
    datastore_mutated_signal: tokio::sync::watch::Receiver<()>,
    index_mutated_signal: tokio::sync::watch::Receiver<()>,
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

    pub fn datastore_mutated_signal(&self) -> &tokio::sync::watch::Receiver<()> {
        &self.datastore_mutated_signal
    }

    pub fn index_mutated_signal(&self) -> tokio::sync::watch::Receiver<()> {
        self.index_mutated_signal.clone()
    }

    pub fn get_data_store(&self) -> DomainResult<&DataStore> {
        self.data_store
            .as_ref()
            .ok_or(DomainErrorKind::NoDataStore.into())
    }

    pub async fn transact(
        self: &Arc<Self>,
        mode: TransactionMode,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        let (upmaps_tx, upmaps_rx) = tokio::sync::mpsc::channel::<UpMap>(1);

        let messages = self
            .clone()
            .map_req_messages(messages, upmaps_tx, session.clone());

        let data_store = self.get_data_store()?;
        let responses = data_store
            .api()
            .transact(mode, messages, session.clone())
            .await?;

        Ok(self
            .clone()
            .map_responses(responses, upmaps_rx, session.clone()))
    }

    pub async fn exec_map(
        self: &Arc<Self>,
        key: MapKey,
        input: Value,
        selects: &mut (dyn FindEntitySelect + Send),
        session: Session,
    ) -> DomainResult<Value> {
        let proc = self
            .ontology
            .get_mapper_proc(&key)
            .ok_or(DomainErrorKind::MappingProcedureNotFound.into_error())?;
        let mut vm = self.ontology.new_vm(proc);

        self.run_vm_to_completion(&mut vm, input, &mut Some(selects), &session)
            .await
    }

    pub(crate) async fn run_vm_to_completion(
        self: &Arc<Self>,
        vm: &mut OntolVm<'_>,
        mut param: Value,
        selects: &mut Option<&mut (dyn FindEntitySelect + Send)>,
        session: &Session,
    ) -> DomainResult<Value> {
        loop {
            match vm
                .run([param])
                .map_err(|err| DomainErrorKind::OntolVm(err).into_error())?
            {
                VmState::Complete(value) => return Ok(value),
                VmState::Yield(vm_yield) => {
                    param = self.exec_yield(vm_yield, selects, session).await?;
                }
            }
        }
    }

    async fn exec_yield(
        self: &Arc<Self>,
        vm_yield: Yield,
        selects: &mut Option<&mut (dyn FindEntitySelect + Send)>,
        session: &Session,
    ) -> DomainResult<Value> {
        match vm_yield {
            Yield::Match(match_var, value_cardinality, filter) => {
                if let Some(selects) = selects {
                    match selects.find_select(match_var, filter.condition()) {
                        SelectMode::Dynamic(mut entity_select) => {
                            // Merge the filter into the select
                            assert!(entity_select.filter.condition().expansions().is_empty());
                            entity_select.filter = filter;

                            self.exec_map_query(value_cardinality, entity_select, session.clone())
                                .await
                        }
                        SelectMode::Static(def_id) => {
                            let def = self.ontology.def(def_id);
                            if def.entity().is_some() {
                                let mut struct_select = StructSelect {
                                    def_id,
                                    properties: Default::default(),
                                };

                                for (prop_id, rel_info) in &def.data_relationships {
                                    match &rel_info.kind {
                                        DataRelationshipKind::Id
                                        | DataRelationshipKind::Tree(_) => {
                                            struct_select.properties.insert(*prop_id, Select::Unit);
                                        }
                                        DataRelationshipKind::Edge(_) => {}
                                    }
                                }

                                let entity_select = EntitySelect {
                                    source: StructOrUnionSelect::Struct(struct_select),
                                    filter,
                                    after_cursor: None,
                                    limit: Some(self.system().default_query_limit()),
                                    include_total_len: false,
                                };

                                self.exec_map_query(
                                    value_cardinality,
                                    entity_select,
                                    session.clone(),
                                )
                                .await
                            } else {
                                match value_cardinality {
                                    ValueCardinality::Unit => Ok(Value::unit()),
                                    ValueCardinality::IndexSet | ValueCardinality::List => {
                                        Ok(Value::Sequence(Sequence::default(), def_id.into()))
                                    }
                                }
                            }
                        }
                    }
                } else {
                    Err(DomainErrorKind::ImpureMapping.into_error())
                }
            }
            Yield::CallExtern(extern_def_id, input, output_def_id) => {
                let ontology = &self.ontology;
                let input_operator_addr = ontology
                    .def(input.type_def_id())
                    .operator_addr
                    .ok_or(DomainErrorKind::SerializationFailed.into_error())?;
                let output_operator_addr =
                    ontology.def(output_def_id).operator_addr.ok_or_else(|| {
                        error!("No deserialization operator");
                        DomainErrorKind::DeserializationFailed.into_error()
                    })?;

                match self
                    .ontology
                    .get_extern(extern_def_id)
                    .ok_or(DomainErrorKind::MappingProcedureNotFound.into_error())?
                {
                    Extern::HttpJson { url } => {
                        let url = &self.ontology[*url];
                        let mut input_json: Vec<u8> = vec![];
                        self.ontology
                            .new_serde_processor(input_operator_addr, ProcessorMode::Read)
                            .serialize_attr(
                                AttrRef::Unit(&input),
                                &mut serde_json::Serializer::new(&mut input_json),
                            )
                            .map_err(|_| DomainErrorKind::SerializationFailed.into_error())?;

                        let output_json = self
                            .system
                            .call_http_json_hook(url, session.clone(), input_json)
                            .await?;

                        let output_attr = self
                            .ontology
                            .new_serde_processor(output_operator_addr, ProcessorMode::Read)
                            .deserialize(&mut serde_json::Deserializer::from_slice(&output_json))
                            .map_err(|error| {
                                debug!("hook deserialization error: {error:?}");
                                DomainErrorKind::DeserializationFailed.into_error()
                            })?;

                        Ok(output_attr
                            .into_unit()
                            .expect("multi-valued ONTOL attribute"))
                    }
                }
            }
        }
    }

    async fn exec_map_query(
        self: &Arc<Self>,
        value_cardinality: ValueCardinality,
        mut entity_select: EntitySelect,
        session: Session,
    ) -> DomainResult<ontol_runtime::value::Value> {
        debug!("match filter:\n{:#?}", entity_select.filter);

        let mut static_conditions: Vec<Condition> = vec![];

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
                let up_path = self
                    .resolver_graph
                    .probe_path(
                        self.ontology.as_ref().as_ref(),
                        struct_select.def_id,
                        ProbeOptions {
                            must_be_entity: true,
                            direction: ProbeDirection::Up,
                            filter: ProbeFilter::Complete,
                        },
                    )
                    .ok_or(DomainErrorKind::NoResolvePathToDataStore.into_error())?;

                if let Some(map_key) = up_path.iter().last() {
                    assert_eq!(map_key.input.def_id, inner_entity_def_id);
                }

                // Transform select
                for map_key in up_path.iter() {
                    if !static_conditions.is_empty() {
                        todo!("translate static conditions");
                    }

                    if let Some(static_condition) = self.ontology.get_static_condition(&map_key) {
                        static_conditions.push(static_condition.clone());
                    }

                    translate_entity_select(&mut entity_select, &map_key, &self.ontology);
                }
            }
            _ => todo!("Basically apply the same operation as above, but refactor"),
        }

        for static_condition in static_conditions {
            debug!(
                "merge {static_condition} INTO: {}",
                entity_select.filter.condition()
            );

            entity_select.filter.condition_mut().merge(static_condition);
        }

        let sequences: Vec<_> = self
            .clone()
            .transact(
                TransactionMode::ReadOnly,
                futures_util::stream::iter([Ok(ReqMessage::Query(0, entity_select.clone()))])
                    .boxed(),
                session.clone(),
            )
            .await?
            .accumulate_sequences()
            .try_collect()
            .await?;

        let Some(edge_seq) = sequences.into_iter().next() else {
            return Err(DomainError::data_store("nothing returned from data store"));
        };

        match value_cardinality {
            ValueCardinality::Unit => Ok(edge_seq
                .into_elements()
                .into_iter()
                .next()
                .unwrap_or(Value::unit())),
            ValueCardinality::IndexSet | ValueCardinality::List => {
                Ok(Value::Sequence(edge_seq, ValueTag::unit()))
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

    pub async fn build<F: DataStoreFactory>(
        self,
        factory: F,
        session: Session,
    ) -> DomainResult<DomainEngine> {
        let system = self.system.expect("No system API provided!");

        let (datastore_mutated_tx, datastore_mutated_rx) = tokio::sync::watch::channel(());
        let (index_mutated_tx, index_mutated_rx) = tokio::sync::watch::channel(());

        let data_store = match self.data_store {
            Some(data_store) => Some(data_store),
            None => {
                let mut data_store: Option<DataStore> = None;

                for (config, persisted_set) in persisted_domains(&self.ontology) {
                    let api = factory
                        .new_api(
                            &persisted_set,
                            DataStoreParams {
                                config: config.clone(),
                                session: session.clone(),
                                ontology: self.ontology.clone(),
                                system: system.clone(),
                                datastore_mutated: datastore_mutated_tx.clone(),
                                index_mutated: index_mutated_tx.clone(),
                            },
                        )
                        .await
                        .with_context(|| format!("Failed to initialize data store {config:?}"))?;
                    data_store = Some(DataStore::new(persisted_set, api));
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
            datastore_mutated_signal: datastore_mutated_rx,
            index_mutated_signal: index_mutated_rx,
        })
    }

    pub fn build_sync<F: DataStoreFactorySync>(
        self,
        factory: F,
        session: Session,
    ) -> DomainResult<DomainEngine> {
        let system = self.system.expect("No system API provided!");

        let (datastore_mutated_tx, datastore_mutated_rx) = tokio::sync::watch::channel(());
        let (index_mutated_tx, index_mutated_rx) = tokio::sync::watch::channel(());

        let data_store = match self.data_store {
            Some(data_store) => Some(data_store),
            None => {
                let mut data_store: Option<DataStore> = None;

                for (config, persisted_set) in persisted_domains(&self.ontology) {
                    let api = factory
                        .new_api_sync(
                            &persisted_set,
                            DataStoreParams {
                                config: config.clone(),
                                session: session.clone(),
                                ontology: self.ontology.clone(),
                                system: system.clone(),
                                datastore_mutated: datastore_mutated_tx.clone(),
                                index_mutated: index_mutated_tx.clone(),
                            },
                        )
                        .with_context(|| format!("Failed to initialize data store {config:?}"))?;
                    data_store = Some(DataStore::new(persisted_set, api));
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
            datastore_mutated_signal: datastore_mutated_rx,
            index_mutated_signal: index_mutated_rx,
        })
    }
}
