use std::{collections::BTreeSet, sync::Arc};

use futures_util::{stream::BoxStream, Stream, StreamExt};
use ontol_runtime::{
    query::{
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    resolve_path::{ProbeDirection, ProbeFilter, ProbeOptions, ResolvePath},
    sequence::{Sequence, SubSequence},
    value::Value,
    DefId,
};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    domain_error::DomainErrorKind,
    select_data_flow::{translate_entity_select, translate_select},
    system::SystemAPI,
    update::sanitize_update,
    DomainEngine, DomainError, DomainResult, Session, VertexAddr,
};

/// The kind of data store transaction
#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum TransactionMode {
    /// Different queries might see different data states (caused by concurrent mutations).
    ReadOnly,
    /// Different queries will see the same data state.
    ReadOnlyAtomic,
    /// Like ReadOnly, but supports writing to data store.
    ReadWrite,
    /// Like ReadOnlyAtomic, but supports writing.
    ReadWriteAtomic,
}

impl TransactionMode {
    pub fn is_atomic(&self) -> bool {
        matches!(self, Self::ReadOnlyAtomic | Self::ReadWriteAtomic)
    }

    pub fn is_write(&self) -> bool {
        matches!(self, Self::ReadWrite | Self::ReadWriteAtomic)
    }
}

/// Operation sequence number (within one transaction)
pub type OpSequence = u32;

#[derive(Serialize, Deserialize, Debug)]
pub enum ReqMessage {
    /// Query for output elements
    /// Queries do not accept any arguments.
    Query(OpSequence, EntitySelect),
    /// Insert the following arguments
    Insert(OpSequence, Select),
    /// Update the following arguments
    Update(OpSequence, Select),
    /// Upsert the following arguments
    Upsert(OpSequence, Select),
    /// Delete the following arguments
    Delete(OpSequence, DefId),
    /// Argument to the previous mutation message
    Argument(Value),
}

#[derive(Serialize, Deserialize)]
pub enum RespMessage {
    /// Marks the start of a new output sequence.
    /// The subsequent Element messages are the elements of that sequence.
    SequenceStart(OpSequence),
    Element(Value, DataOperation),
    SequenceEnd(OpSequence, Option<Box<SubSequence>>),
    /// Write transaction complete and committed
    WriteComplete(Box<WriteStats>),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum DataOperation {
    Queried,
    Inserted,
    Updated,
    Deleted,
}

#[derive(Serialize, Deserialize)]
pub struct WriteStats {
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    mutated: BTreeSet<DefId>,
    deleted: Vec<VertexAddr>,
}

impl WriteStats {
    pub fn builder(mode: TransactionMode, system: &dyn SystemAPI) -> WriteStatsBuilder {
        WriteStatsBuilder {
            mode,
            start: if mode.is_write() {
                system.current_time()
            } else {
                Default::default()
            },
            mutated: Default::default(),
            deleted: Default::default(),
        }
    }

    pub fn start(&self) -> chrono::DateTime<chrono::Utc> {
        self.start
    }

    pub fn end(&self) -> chrono::DateTime<chrono::Utc> {
        self.end
    }

    pub fn mutated(&self) -> impl Iterator<Item = DefId> + '_ {
        self.mutated.iter().copied()
    }

    pub fn deleted(&self) -> impl Iterator<Item = VertexAddr> + '_ {
        self.deleted.iter().cloned()
    }

    pub fn take_deleted(&mut self) -> impl Iterator<Item = VertexAddr> + '_ {
        std::mem::take(&mut self.deleted).into_iter()
    }
}

pub struct WriteStatsBuilder {
    mode: TransactionMode,
    start: chrono::DateTime<chrono::Utc>,
    deleted: Vec<VertexAddr>,
    mutated: BTreeSet<DefId>,
}

impl WriteStatsBuilder {
    pub fn mark_mutated(&mut self, def_id: DefId) {
        self.mutated.insert(def_id);
    }

    pub fn mark_deleted(&mut self, address: VertexAddr) {
        self.deleted.push(address);
    }

    pub fn finish(self, system: &dyn SystemAPI) -> Option<WriteStats> {
        if self.mode.is_write() {
            Some(WriteStats {
                start: self.start,
                end: system.current_time(),
                mutated: self.mutated,
                deleted: self.deleted,
            })
        } else {
            None
        }
    }
}

pub trait AccumulateSequences<'a> {
    fn accumulate_sequences(self) -> impl Stream<Item = DomainResult<Sequence<Value>>> + 'a;
}

impl<'a> AccumulateSequences<'a> for BoxStream<'a, DomainResult<RespMessage>> {
    fn accumulate_sequences(self) -> impl Stream<Item = DomainResult<Sequence<Value>>> + 'a {
        async_stream::try_stream! {
            let mut current: Option<Sequence<Value>> = None;

            for await resp_message in self {
                match resp_message? {
                    RespMessage::SequenceStart(_) => {
                        current = Some(Sequence::default());
                    }
                    RespMessage::Element(value, _reason) => {
                        if let Some(current) = &mut current {
                            current.push(value);
                        }
                    }
                    RespMessage::SequenceEnd(_, sub_seq) => {
                        if let Some(mut current) = current.take() {
                            if let Some(sub_seq) = sub_seq {
                                current = current.with_sub(*sub_seq);
                            }

                            yield current;
                        }
                    }
                    RespMessage::WriteComplete(_) => {}
                }
            }

            if let Some(current) = current {
                yield current;
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct UpMap(OpSequence, ResolvePath);

struct DownMap(ResolvePath);

impl DomainEngine {
    pub(crate) fn map_req_messages(
        self: Arc<Self>,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        upmaps_tx: tokio::sync::mpsc::Sender<UpMap>,
        session: Session,
    ) -> BoxStream<'static, DomainResult<ReqMessage>> {
        async_stream::stream! {
            let mut cur_downmap = None;
            let mut updating = false;

            for await result in messages {
                let req_msg = result?;
                yield self.process_req_message(req_msg, &mut cur_downmap, &upmaps_tx, &mut updating, &session).await;
            }
        }.boxed()
    }

    pub(crate) fn map_responses(
        self: Arc<Self>,
        responses: BoxStream<'static, DomainResult<RespMessage>>,
        mut upmaps_rx: tokio::sync::mpsc::Receiver<UpMap>,
        session: Session,
    ) -> BoxStream<'static, DomainResult<RespMessage>> {
        let mut cur_upmap = UpMap(0, ResolvePath::default());

        async_stream::try_stream! {
            for await resp_msg in responses {
                match resp_msg? {
                    RespMessage::SequenceStart(op_seq) => {
                        self.recv_upmap(op_seq, &mut upmaps_rx, &mut cur_upmap).await?;
                        yield RespMessage::SequenceStart(op_seq);
                    }
                    RespMessage::Element(value, reason) => {
                        yield RespMessage::Element(
                            self.upmap_value(value, &session, &cur_upmap).await.unwrap(),
                            reason
                        );
                    }
                    RespMessage::SequenceEnd(op_seq, sub_sequence) => {
                        yield RespMessage::SequenceEnd(op_seq, sub_sequence);
                    }
                    RespMessage::WriteComplete(stats) => {
                        yield RespMessage::WriteComplete(stats);
                    }
                };
            }
        }
        .boxed()
    }

    async fn process_req_message(
        self: &Arc<Self>,
        req_msg: ReqMessage,
        cur_downmap: &mut Option<DownMap>,
        upmaps_tx: &tokio::sync::mpsc::Sender<UpMap>,
        updating: &mut bool,
        session: &Session,
    ) -> DomainResult<ReqMessage> {
        match req_msg {
            ReqMessage::Query(op_seq, mut select) => {
                *updating = false;
                let resolve_path = self
                    .resolver_graph
                    .probe_path_for_entity_select(self.ontology(), &select)
                    .ok_or(DomainErrorKind::NoResolvePathToDataStore.into_error())?;

                for map_key in resolve_path.iter() {
                    translate_entity_select(&mut select, &map_key, self.ontology());
                }

                self.datastore_transform_entity_select(&mut select)?;

                send_upmap(op_seq, resolve_path, upmaps_tx).await?;
                *cur_downmap = None;

                Ok(ReqMessage::Query(op_seq, select))
            }
            ReqMessage::Insert(op_seq, mut select) => {
                *updating = false;
                self.setup_duplex_map(
                    op_seq,
                    &mut select,
                    ProbeFilter::Complete,
                    cur_downmap,
                    upmaps_tx,
                )
                .await?;
                Ok(ReqMessage::Insert(op_seq, select))
            }
            ReqMessage::Update(op_seq, mut select) => {
                *updating = true;
                self.setup_duplex_map(
                    op_seq,
                    &mut select,
                    ProbeFilter::Pure,
                    cur_downmap,
                    upmaps_tx,
                )
                .await?;
                Ok(ReqMessage::Update(op_seq, select))
            }
            ReqMessage::Upsert(op_seq, mut select) => {
                *updating = false;
                self.setup_duplex_map(
                    op_seq,
                    &mut select,
                    ProbeFilter::Complete,
                    cur_downmap,
                    upmaps_tx,
                )
                .await?;
                Ok(ReqMessage::Upsert(op_seq, select))
            }
            ReqMessage::Delete(op_seq, mut def_id) => {
                *updating = false;
                self.setup_delete(op_seq, &mut def_id, upmaps_tx).await?;
                *cur_downmap = None;
                Ok(ReqMessage::Delete(op_seq, def_id))
            }
            ReqMessage::Argument(value) => {
                let mut value = self.downmap_value(value, session, cur_downmap).await?;

                if *updating {
                    sanitize_update(&mut value);
                }

                Ok(ReqMessage::Argument(value))
            }
        }
    }

    async fn setup_duplex_map(
        &self,
        op_seq: OpSequence,
        select: &mut Select,
        down_filter: ProbeFilter,
        cur_downmap: &mut Option<DownMap>,
        upmaps_tx: &tokio::sync::mpsc::Sender<UpMap>,
    ) -> DomainResult<()> {
        let ontology = self.ontology();
        let down_path = self
            .resolver_graph
            .probe_path_for_select(ontology, select, ProbeDirection::Down, down_filter)
            .ok_or(DomainErrorKind::NoResolvePathToDataStore.into_error())?;

        let up_path = self
            .resolver_graph
            .probe_path_for_select(ontology, select, ProbeDirection::Up, ProbeFilter::Complete)
            .ok_or(DomainErrorKind::NoResolvePathToDataStore.into_error())?;

        for map_key in up_path.iter() {
            translate_select(select, &map_key, ontology);
        }

        self.datastore_transform_select(select)?;

        *cur_downmap = Some(DownMap(down_path));
        send_upmap(op_seq, up_path, upmaps_tx).await?;

        Ok(())
    }

    async fn setup_delete(
        &self,
        op_seq: OpSequence,
        def_id: &mut DefId,
        upmaps_tx: &tokio::sync::mpsc::Sender<UpMap>,
    ) -> DomainResult<()> {
        if !self
            .get_data_store()?
            .persisted()
            .contains(&def_id.domain_index())
        {
            // TODO: Do ids need to be translated?
            let up_path = self
                .resolver_graph
                .probe_path(
                    self.ontology(),
                    *def_id,
                    ProbeOptions {
                        must_be_entity: true,
                        direction: ProbeDirection::Up,
                        filter: ProbeFilter::Complete,
                    },
                )
                .ok_or(DomainErrorKind::NoResolvePathToDataStore.into_error())?;

            *def_id = up_path.iter().last().unwrap().input.def_id;
        }

        send_upmap(op_seq, ResolvePath::default(), upmaps_tx).await?;

        Ok(())
    }

    async fn downmap_value(
        self: &Arc<Self>,
        mut value: Value,
        session: &Session,
        cur_downmap: &Option<DownMap>,
    ) -> DomainResult<Value> {
        let Some(DownMap(resolve_path)) = cur_downmap.as_ref() else {
            return Ok(value);
        };

        if resolve_path.is_empty() {
            return Ok(value);
        }

        trace!("downmapping {resolve_path:?}");

        for map_key in resolve_path.iter() {
            let procedure = self
                .ontology()
                .get_mapper_proc(&map_key)
                .expect("No downmap procedure for query input");

            let mut vm = self.ontology().new_vm(procedure);

            value = self
                .run_vm_to_completion(&mut vm, value.take(), &mut None, session)
                .await?;
        }

        Ok(value)
    }

    async fn recv_upmap(
        &self,
        op_seq: OpSequence,
        upmaps_rx: &mut tokio::sync::mpsc::Receiver<UpMap>,
        cur_upmap: &mut UpMap,
    ) -> DomainResult<()> {
        match upmaps_rx.recv().await {
            Some(upmap) => {
                if upmap.0 != op_seq {
                    return Err(DomainError::data_store("OpSequence out of sync"));
                }

                *cur_upmap = upmap;

                Ok(())
            }
            None => Err(DomainError::data_store("upmap value not availabe")),
        }
    }

    async fn upmap_value(
        self: &Arc<Self>,
        mut value: Value,
        session: &Session,
        current_upmap: &UpMap,
    ) -> DomainResult<Value> {
        if current_upmap.1.is_empty() {
            return Ok(value);
        }

        for map_key in current_upmap.1.iter().rev() {
            let procedure = self
                .ontology()
                .get_mapper_proc(&map_key)
                .expect("No upmap procedure for query output");

            let mut vm = self.ontology().new_vm(procedure);

            value = self
                .run_vm_to_completion(&mut vm, value.take(), &mut None, session)
                .await?;
        }

        Ok(value)
    }

    fn datastore_transform_select(&self, select: &mut Select) -> DomainResult<()> {
        match select {
            Select::Unit => {}
            Select::EntityId => {}
            Select::VertexAddress => {}
            Select::Struct(struct_select) => {
                self.datastore_transform_struct_select(struct_select)?;
            }
            Select::StructUnion(_, struct_selects) => {
                for struct_select in struct_selects {
                    self.datastore_transform_struct_select(struct_select)?;
                }
            }
            Select::Entity(entity_select) => {
                self.datastore_transform_entity_select(entity_select)?;
            }
        }

        Ok(())
    }

    fn datastore_transform_entity_select(
        &self,
        entity_select: &mut EntitySelect,
    ) -> DomainResult<()> {
        match &mut entity_select.source {
            StructOrUnionSelect::Struct(struct_select) => {
                self.datastore_transform_filter(&mut entity_select.filter, struct_select.def_id)?;
                self.datastore_transform_struct_select(struct_select)?;
            }
            StructOrUnionSelect::Union(_, struct_selects) => {
                entity_select.filter.set_vertex_order(vec![]);

                for struct_select in struct_selects {
                    self.datastore_transform_struct_select(struct_select)?;
                }
            }
        }

        Ok(())
    }

    fn datastore_transform_struct_select(
        &self,
        struct_select: &mut StructSelect,
    ) -> DomainResult<()> {
        for select in struct_select.properties.values_mut() {
            self.datastore_transform_select(select)?;
        }

        Ok(())
    }

    pub(super) fn datastore_transform_filter(
        &self,
        filter: &mut Filter,
        def_id: DefId,
    ) -> DomainResult<()> {
        let symbolic_order = filter.symbolic_order().unwrap();

        let info = self.ontology().extended_entity_info(def_id).unwrap();
        let order_vec = symbolic_order
            .iter()
            .map(|sym| info.order_table.get(&sym.type_def_id()).unwrap().clone())
            .collect::<Vec<_>>();

        filter.set_vertex_order(order_vec);
        Ok(())
    }
}

async fn send_upmap(
    op_seq: OpSequence,
    resolve_path: ResolvePath,
    upmaps_tx: &tokio::sync::mpsc::Sender<UpMap>,
) -> DomainResult<()> {
    upmaps_tx
        .send(UpMap(op_seq, resolve_path))
        .await
        .map_err(|_| DomainError::data_store("upmap not sendable"))?;
    Ok(())
}
