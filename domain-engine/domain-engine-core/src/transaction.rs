use anyhow::anyhow;
use futures_util::{stream::BoxStream, Stream, StreamExt};
use ontol_runtime::{
    query::select::{EntitySelect, Select},
    resolve_path::{ProbeDirection, ProbeFilter, ProbeOptions, ResolvePath},
    sequence::{Sequence, SubSequence},
    value::Value,
    DefId,
};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    select_data_flow::{translate_entity_select, translate_select},
    DomainEngine, DomainError, DomainResult, Session,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
pub struct OpSequence(pub u32);

#[derive(Serialize, Deserialize, Debug)]
pub enum ReqMessage {
    Query(OpSequence, EntitySelect),
    Insert(OpSequence, Select),
    Update(OpSequence, Select),
    Upsert(OpSequence, Select),
    Delete(OpSequence, DefId),
    NextValue(Value),
}

#[derive(Serialize, Deserialize)]
pub enum RespMessage {
    SequenceStart(OpSequence, Option<Box<SubSequence>>),
    NextValue(Value, ValueReason),
}

#[derive(Serialize, Deserialize)]
pub enum ValueReason {
    Queried,
    Inserted,
    Updated,
    Deleted,
}

#[derive(Debug)]
struct UpMap(OpSequence, ResolvePath);

struct DownMap(ResolvePath);

impl DomainEngine {
    pub async fn transact<'a>(
        &'a self,
        messages: BoxStream<'a, ReqMessage>,
        session: Session,
    ) -> DomainResult<BoxStream<'_, DomainResult<RespMessage>>> {
        let data_store = self.get_data_store()?;
        let (upmaps_tx, upmaps_rx) = tokio::sync::mpsc::channel::<UpMap>(1);

        let messages = self.map_req_messages(messages, upmaps_tx, session.clone());

        let responses = data_store.api().transact(messages, session.clone()).await?;

        Ok(self.map_responses(responses, upmaps_rx, session.clone()))
    }

    fn map_req_messages<'a>(
        &'a self,
        messages: BoxStream<'a, ReqMessage>,
        upmaps_tx: tokio::sync::mpsc::Sender<UpMap>,
        session: Session,
    ) -> BoxStream<'a, DomainResult<ReqMessage>> {
        async_stream::stream! {
            let mut cur_downmap = None;

            for await req_msg in messages {
                yield self.process_req_message(req_msg, &mut cur_downmap, &upmaps_tx, &session).await;
            }
        }.boxed()
    }

    fn map_responses<'a>(
        &'a self,
        responses: BoxStream<'a, DomainResult<RespMessage>>,
        mut upmaps_rx: tokio::sync::mpsc::Receiver<UpMap>,
        session: Session,
    ) -> BoxStream<'a, DomainResult<RespMessage>> {
        let mut cur_upmap = UpMap(OpSequence(0), ResolvePath::default());

        async_stream::try_stream! {
            for await resp_msg in responses {
                match resp_msg? {
                    RespMessage::SequenceStart(op_seq, sub_seq) => {
                        self.recv_upmap(op_seq, &mut upmaps_rx, &mut cur_upmap).await?;
                        yield RespMessage::SequenceStart(op_seq, sub_seq);
                    }
                    RespMessage::NextValue(value, reason) => {
                        yield RespMessage::NextValue(
                            self.upmap_value(value, &session, &cur_upmap).await.unwrap(),
                            reason
                        );
                    }
                };
            }
        }
        .boxed()
    }

    async fn process_req_message(
        &self,
        req_msg: ReqMessage,
        cur_downmap: &mut Option<DownMap>,
        upmaps_tx: &tokio::sync::mpsc::Sender<UpMap>,
        session: &Session,
    ) -> DomainResult<ReqMessage> {
        match req_msg {
            ReqMessage::Query(op_seq, mut select) => {
                let resolve_path = self
                    .resolver_graph
                    .probe_path_for_entity_select(self.ontology(), &select)
                    .ok_or(DomainError::NoResolvePathToDataStore)?;

                for map_key in resolve_path.iter() {
                    translate_entity_select(&mut select, &map_key, self.ontology());
                }

                send_upmap(op_seq, resolve_path, upmaps_tx).await?;
                *cur_downmap = None;

                Ok(ReqMessage::Query(op_seq, select))
            }
            ReqMessage::Insert(op_seq, mut select) => {
                self.setup_duplex(
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
                self.setup_duplex(
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
                self.setup_duplex(
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
                self.setup_delete(op_seq, &mut def_id, upmaps_tx).await?;
                *cur_downmap = None;
                Ok(ReqMessage::Delete(op_seq, def_id))
            }
            ReqMessage::NextValue(value) => Ok(ReqMessage::NextValue(
                self.downmap_value(value, session, &cur_downmap).await?,
            )),
        }
    }

    async fn setup_duplex(
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
            .ok_or(DomainError::NoResolvePathToDataStore)?;

        let up_path = self
            .resolver_graph
            .probe_path_for_select(ontology, select, ProbeDirection::Up, ProbeFilter::Complete)
            .ok_or(DomainError::NoResolvePathToDataStore)?;

        for map_key in up_path.iter() {
            translate_select(select, &map_key, ontology);
        }

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
            .package_ids()
            .contains(&def_id.package_id())
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
                .ok_or(DomainError::NoResolvePathToDataStore)?;

            *def_id = up_path.iter().last().unwrap().input.def_id;
        }

        send_upmap(op_seq, ResolvePath::default(), upmaps_tx).await?;

        Ok(())
    }

    async fn downmap_value(
        &self,
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
                    return Err(DomainError::DataStore(anyhow!("OpSequence out of sync")));
                }

                *cur_upmap = upmap;

                Ok(())
            }
            None => Err(DomainError::DataStore(anyhow!("upmap value not availabe"))),
        }
    }

    async fn upmap_value(
        &self,
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
}

async fn send_upmap(
    op_seq: OpSequence,
    resolve_path: ResolvePath,
    upmaps_tx: &tokio::sync::mpsc::Sender<UpMap>,
) -> DomainResult<()> {
    upmaps_tx
        .send(UpMap(op_seq, resolve_path))
        .await
        .map_err(|_| DomainError::DataStore(anyhow!("upmap not sendable")))?;
    Ok(())
}

pub fn collect_sequences<'a>(
    input: BoxStream<'a, DomainResult<RespMessage>>,
) -> impl Stream<Item = DomainResult<Sequence<Value>>> + 'a {
    async_stream::try_stream! {
        let mut current: Option<Sequence<Value>> = None;

        for await resp_message in input {
            match resp_message? {
                RespMessage::SequenceStart(_, sub_seq) => {
                    if let Some(current) = current {
                        yield current;
                    }

                    let mut next = Sequence::default();
                    if let Some(sub_seq) = sub_seq {
                        next = next.with_sub(*sub_seq);
                    }

                    current = Some(next);
                }
                RespMessage::NextValue(value, _reason) => {
                    if let Some(current) = &mut current {
                        current.push(value);
                    }
                }
            }
        }

        if let Some(current) = current {
            yield current;
        }
    }
}
