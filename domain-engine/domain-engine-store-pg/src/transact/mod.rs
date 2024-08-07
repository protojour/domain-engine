use std::sync::Arc;

use domain_engine_core::{
    object_generator::ObjectGenerator,
    system::SystemAPI,
    transact::{DataOperation, OpSequence, ReqMessage, RespMessage},
    DomainResult,
};
use futures_util::{stream::BoxStream, StreamExt};
use mutate::InsertMode;
use ontol_runtime::{
    interface::serde::processor::ProcessorMode, ontology::Ontology, query::select::Select, DefId,
};
use query::QueryFrame;
use tokio_postgres::IsolationLevel;
use tracing::debug;

use crate::{ds_err, PgModel, PostgresDataStore};

mod data;
mod mutate;
mod query;
mod struct_analyzer;
mod struct_select;

struct TransactCtx<'a> {
    pg_model: &'a PgModel,
    ontology: &'a Ontology,
    system: &'a (dyn SystemAPI + Send + Sync),
    txn: deadpool_postgres::Transaction<'a>,
}

enum State {
    Insert(OpSequence, Select),
    Update(OpSequence, Select),
    Upsert(OpSequence, Select),
    Delete(OpSequence, DefId),
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
            txn: pg_client
                .build_transaction()
                .deferrable(false)
                .isolation_level(IsolationLevel::ReadCommitted)
                .start()
                .await
                .map_err(|err| {
                    debug!("transaction not initiated: {err:?}");
                    ds_err("could not initiate transaction")
                })?
        };

        let mut state: Option<State> = None;

        for await message in messages {
            match message? {
                ReqMessage::Query(op_seq, entity_select) => {
                    state = None;
                    let stream = ctx.query(entity_select).await?;

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
                        Some(State::Update(_, _select)) => {
                            ObjectGenerator::new(ProcessorMode::Update, ctx.ontology, ctx.system)
                                .generate_objects(&mut value);

                            Err(ds_err("Update not implemented for Postgres"))?;
                        }
                        Some(State::Upsert(_, select)) => {
                            ObjectGenerator::new(ProcessorMode::Create, ctx.ontology, ctx.system)
                                .generate_objects(&mut value);

                            let row = ctx.insert_vertex(value.into(), InsertMode::Upsert, select).await?;
                            yield RespMessage::Element(row.value, row.op);
                        }
                        Some(State::Delete(_, _def_id)) => {
                            Err(ds_err("Delete not implemented for Postgres"))?;
                        }
                        None => {
                            Err(ds_err("invalid transaction state"))?
                        }
                    }
                }
            }
        }

        ctx.txn.commit().await.map_err(|err| {
            debug!("transaction not committed: {err:?}");
            ds_err("transaction could not be commmitted")
        })?;
    }
    .boxed())
}
