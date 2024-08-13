use std::sync::Arc;

use domain_engine_core::{
    object_generator::ObjectGenerator,
    system::SystemAPI,
    transact::{DataOperation, OpSequence, ReqMessage, RespMessage},
    DomainResult,
};
use futures_util::{stream::BoxStream, StreamExt};
use insert::InsertMode;
use ontol_runtime::{
    interface::serde::processor::ProcessorMode, ontology::Ontology, query::select::Select, DefId,
};
use query::QueryFrame;
use tokio_postgres::IsolationLevel;
use tracing::{debug, trace};

use crate::{ds_err, pg_model::PgDef, PgModel, PostgresDataStore};

mod data;
mod delete;
mod insert;
mod query;
mod edge_query;
mod struct_analyzer;
mod edge_patch;
mod fields;
mod update;

struct TransactCtx<'a> {
    pg_model: &'a PgModel,
    ontology: &'a Ontology,
    system: &'a (dyn SystemAPI + Send + Sync),
    connection_state: ConnectionState<'a>,
}

impl<'a> TransactCtx<'a> {
    pub fn client(&self) -> &tokio_postgres::Client {
        match &self.connection_state {
            ConnectionState::Transaction(txn) => txn.client(),
        }
    }

    /// Look up ontology def and PG data for the given def_id
    pub fn lookup_def(&self, def_id: DefId) -> DomainResult<PgDef<'a>> {
        let def = self.ontology.def(def_id);
        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.package_id(), def_id)?;

        Ok(PgDef { def, pg })
    }
}

enum State {
    Insert(OpSequence, Select),
    Update(OpSequence, Select),
    Upsert(OpSequence, Select),
    Delete(OpSequence, DefId),
}

/// TODO: support out-of-transaction statements by expanding ReqMessage
/// with an explicit Start/End Transaction
/// 
/// Since a transaction needs a &mut Client,
/// there seems to be no other way to dynamically support
/// this than re-acquire the connection every time the state needs to be changed.
enum ConnectionState<'a> {
    Transaction(deadpool_postgres::Transaction<'a>),
}

fn write_state(
    op_seq: OpSequence,
    new_state: State,
    state: &mut Option<State>,
) -> Vec<RespMessage> {
    let mut messages = vec![];

    match state {
        Some(
            State::Insert(prev_op, _)
            | State::Update(prev_op, _)
            | State::Upsert(prev_op, _)
            | State::Delete(prev_op, _),
        ) => {
            messages.push(RespMessage::SequenceEnd(*prev_op, None));
        }
        None => {}
    }

    *state = Some(new_state);
    messages.push(RespMessage::SequenceStart(op_seq));
    messages
}

pub async fn transact(
    store: Arc<PostgresDataStore>,
    messages: BoxStream<'static, DomainResult<ReqMessage>>,
) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
    let mut pg_client = store
        .pool
        .get()
        .await
        .map_err(|_| ds_err("could not acquire database connection"))?;

    Ok(async_stream::try_stream! {
        let ctx = TransactCtx {
            pg_model: &store.pg_model,
            ontology: &store.ontology,
            system: store.system.as_ref(),
            connection_state: ConnectionState::Transaction(
                pg_client
                    .build_transaction()
                    .deferrable(false)
                    .isolation_level(IsolationLevel::ReadCommitted)
                    .start()
                    .await
                    .map_err(|err| {
                        debug!("transaction not initiated: {err:?}");
                        ds_err("could not initiate transaction")
                    })?
            )
        };

        let mut state: Option<State> = None;

        for await message in messages {
            match message? {
                ReqMessage::Query(op_seq, entity_select) => {
                    state = None;
                    let stream = ctx.query_vertex(&entity_select).await?;

                    yield RespMessage::SequenceStart(op_seq);

                    for await result in stream {
                        match result? {
                            QueryFrame::Row(row) => {
                                yield RespMessage::Element(row.value, DataOperation::Queried);
                            }
                            QueryFrame::Footer(sub_sequence) => {
                                yield RespMessage::SequenceEnd(op_seq, sub_sequence);
                            }
                        }
                    }
                }
                ReqMessage::Insert(op_seq, select) => {
                    for msg in write_state(op_seq, State::Insert(op_seq, select), &mut state) {
                        yield msg;
                    }
                }
                ReqMessage::Update(op_seq, select) => {
                    for msg in write_state(op_seq, State::Update(op_seq, select), &mut state) {
                        yield msg;
                    }
                }
                ReqMessage::Upsert(op_seq, select) => {
                    for msg in write_state(op_seq, State::Upsert(op_seq, select), &mut state) {
                        yield msg;
                    }
                }
                ReqMessage::Delete(op_seq, def_id) => {
                    for msg in write_state(op_seq, State::Delete(op_seq, def_id), &mut state) {
                        yield msg;
                    }
                }
                ReqMessage::Argument(mut value) => {
                    match state.as_ref() {
                        Some(State::Insert(_, select)) => {
                            ObjectGenerator::new(ProcessorMode::Create, ctx.ontology, ctx.system)
                                .generate_objects(&mut value);

                            let row = ctx.insert_vertex(value.into(), InsertMode::Insert, select).await?;
                            yield RespMessage::Element(row.value, row.op);
                        }
                        Some(State::Update(_, select)) => {
                            ObjectGenerator::new(ProcessorMode::Update, ctx.ontology, ctx.system)
                                .generate_objects(&mut value);

                            let value = ctx.update_vertex_with_select(value.into(), select).await?;
                            yield RespMessage::Element(value, DataOperation::Updated);
                        }
                        Some(State::Upsert(_, select)) => {
                            ObjectGenerator::new(ProcessorMode::Create, ctx.ontology, ctx.system)
                                .generate_objects(&mut value);

                            let row = ctx.insert_vertex(value.into(), InsertMode::Upsert, select).await?;
                            yield RespMessage::Element(row.value, row.op);
                        }
                        Some(State::Delete(_, def_id)) => {
                            let deleted = ctx.delete_vertex(*def_id, value).await?;

                            yield RespMessage::Element(ctx.ontology.bool_value(deleted), DataOperation::Deleted);
                        }
                        None => {
                            Err(ds_err("invalid transaction state"))?
                        }
                    }
                }
            }
        }

        match ctx.connection_state {
            ConnectionState::Transaction(txn) => {
                txn.commit().await.map_err(|err| {
                    debug!("transaction not committed: {err:?}");
                    ds_err("transaction could not be commmitted")
                })?;
        
                trace!("COMMIT OK");
            }
        }

    }
    .boxed())
}
