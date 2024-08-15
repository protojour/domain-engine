use std::sync::Arc;

use domain_engine_core::{
    object_generator::ObjectGenerator,
    system::SystemAPI,
    transact::{DataOperation, OpSequence, ReqMessage, RespMessage, TransactionMode},
    DomainError, DomainResult,
};
use futures_util::{stream::BoxStream, StreamExt};
use ontol_runtime::{
    interface::serde::processor::ProcessorMode, ontology::Ontology, query::select::Select, DefId,
};
use query::QueryFrame;
use tokio_postgres::IsolationLevel;
use tracing::trace;

use crate::{
    pg_error::{PgError, PgModelError},
    pg_model::PgDef,
    PgModel, PostgresDataStore,
};

mod data;
mod delete;
mod edge_patch;
mod edge_query;
mod fields;
mod insert;
mod query;
mod update;

#[derive(Clone, Copy, Debug)]
pub enum MutationMode {
    Create(InsertMode),
    Update,
    UpdateEdgeCardinal,
}

impl MutationMode {
    pub const fn insert() -> Self {
        Self::Create(InsertMode::Insert)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum InsertMode {
    Insert,
    Upsert,
}

struct TransactCtx<'a> {
    pg_model: &'a PgModel,
    ontology: &'a Ontology,
    system: &'a (dyn SystemAPI + Send + Sync),
    connection_state: ConnectionState<'a>,
}

impl<'a> TransactCtx<'a> {
    pub fn client(&self) -> &tokio_postgres::Client {
        match &self.connection_state {
            ConnectionState::NonAtomic(conn) => conn,
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

enum ConnectionState<'a> {
    NonAtomic(deadpool::managed::Object<deadpool_postgres::Manager>),
    Transaction(deadpool_postgres::Transaction<'a>),
}

pub async fn transact(
    store: Arc<PostgresDataStore>,
    mode: TransactionMode,
    messages: BoxStream<'static, DomainResult<ReqMessage>>,
) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
    let mut connection = store
        .pool
        .get()
        .await
        .map_err(|e| PgError::DbConnectionAcquire(e.into()))?;

    Ok(async_stream::try_stream! {
        let connection_state = match mode {
            TransactionMode::ReadOnly | TransactionMode::ReadWrite => {
                ConnectionState::NonAtomic(connection)
            }
            TransactionMode::ReadOnlyAtomic | TransactionMode::ReadWriteAtomic => {
                ConnectionState::Transaction(
                    connection
                        .build_transaction()
                        .deferrable(false)
                        .isolation_level(IsolationLevel::ReadCommitted)
                        .read_only(matches!(mode, TransactionMode::ReadOnly | TransactionMode::ReadOnlyAtomic))
                        .start()
                        .await
                        .map_err(PgError::BeginTransaction)?,
                )
            }
        };

        let ctx = TransactCtx {
            pg_model: &store.pg_model,
            ontology: &store.ontology,
            system: store.system.as_ref(),
            connection_state
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
                            Result::<(), DomainError>::Err(PgModelError::InvalidTransactionState.into())?;
                        }
                    }
                }
            }
        }

        if let ConnectionState::Transaction(txn) = ctx.connection_state {
            txn.commit().await.map_err(PgError::CommitTransaction)?;
            trace!("COMMIT OK");
        }
    }
    .boxed())
}
