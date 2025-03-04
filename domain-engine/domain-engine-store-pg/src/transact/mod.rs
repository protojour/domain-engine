use std::sync::Arc;

use domain_engine_core::{
    DomainError, DomainResult,
    make_interfacable::MakeInterfacable,
    make_storable::MakeStorable,
    system::SystemAPI,
    transact::{DataOperation, OpSequence, ReqMessage, RespMessage, TransactionMode},
};
use futures_util::{StreamExt, stream::BoxStream};
use mut_ctx::PgMutCtx;
use ontol_runtime::{
    DefId,
    interface::serde::processor::ProcessorMode,
    ontology::aspects::{DefsAspect, SerdeAspect},
    query::select::Select,
    value::Value,
};
use query::QueryFrame;
use tokio_postgres::IsolationLevel;
use tracing::trace;

use crate::{
    PgModel, PostgresDataStore,
    compaction::CompactionCtx,
    pg_error::{PgError, PgModelError},
    pg_model::PgDef,
};

mod condition;
mod crdt;
mod data;
mod delete;
mod edge_patch;
mod edge_query;
mod fields;
mod insert;
mod mut_ctx;
mod order;
mod query;
mod query_select;
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

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum InsertMode {
    /// Insert - no conflict tolerated
    Insert,
    /// Upsert - update on matching primary key
    Upsert,
}

pub struct TransactCtx<'a> {
    txn_mode: TransactionMode,
    pg_model: &'a PgModel,
    ontology_defs: &'a DefsAspect,
    ontology_serde: &'a SerdeAspect,
    system: &'a (dyn SystemAPI + Send + Sync),
    pub connection_state: ConnectionState<'a>,
    compaction_ctx: &'a CompactionCtx,
}

impl AsRef<DefsAspect> for TransactCtx<'_> {
    fn as_ref(&self) -> &DefsAspect {
        self.ontology_defs
    }
}

impl AsRef<SerdeAspect> for TransactCtx<'_> {
    fn as_ref(&self) -> &SerdeAspect {
        self.ontology_serde
    }
}

impl<'a> TransactCtx<'a> {
    pub fn new(
        txn_mode: TransactionMode,
        connection_state: ConnectionState<'a>,
        store: &'a PostgresDataStore,
    ) -> Self {
        Self {
            txn_mode,
            pg_model: &store.pg_model,
            ontology_defs: store.ontology.as_ref().as_ref(),
            ontology_serde: store.ontology.as_ref().as_ref(),
            system: store.system.as_ref(),
            connection_state,
            compaction_ctx: &store.compaction_ctx,
        }
    }

    pub fn client(&self) -> &tokio_postgres::Client {
        match &self.connection_state {
            ConnectionState::NonAtomic(conn) => conn,
            ConnectionState::Transaction(txn) => txn.client(),
        }
    }

    /// Look up ontology def and PG data for the given def_id
    pub fn lookup_def(&self, def_id: DefId) -> DomainResult<PgDef<'a>> {
        let def = self.ontology_defs.def(def_id);
        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.domain_index(), def_id)?;

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

pub enum ConnectionState<'a> {
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
        let ctx = TransactCtx::new(mode, connection_state, &store);
        let mut mut_ctx = PgMutCtx::new(mode, store.system.as_ref());

        let mut state: Option<State> = None;

        for await message in messages {
            match message? {
                ReqMessage::Query(op_seq, entity_select) => {
                    mut_ctx.cache.clear_select_dependent();
                    state = None;
                    let stream = ctx.query_vertex(&entity_select, &mut mut_ctx).await?;

                    yield RespMessage::SequenceStart(op_seq);

                    for await result in stream {
                        match result? {
                            QueryFrame::Row(mut row) => {
                                MakeInterfacable::new(&ctx).make_interfacable(&mut row.value)?;
                                yield RespMessage::Element(row.value, DataOperation::Queried);
                            }
                            QueryFrame::Footer(sub_sequence) => {
                                yield RespMessage::SequenceEnd(op_seq, sub_sequence);
                            }
                        }
                    }
                }
                ReqMessage::Insert(op_seq, select) => {
                    mut_ctx.cache.clear_select_dependent();
                    for msg in write_state(op_seq, State::Insert(op_seq, select), &mut state) {
                        yield msg;
                    }
                }
                ReqMessage::Update(op_seq, select) => {
                    mut_ctx.cache.clear_select_dependent();
                    for msg in write_state(op_seq, State::Update(op_seq, select), &mut state) {
                        yield msg;
                    }
                }
                ReqMessage::Upsert(op_seq, select) => {
                    mut_ctx.cache.clear_select_dependent();
                    for msg in write_state(op_seq, State::Upsert(op_seq, select), &mut state) {
                        yield msg;
                    }
                }
                ReqMessage::Delete(op_seq, def_id) => {
                    mut_ctx.cache.clear_select_dependent();
                    for msg in write_state(op_seq, State::Delete(op_seq, def_id), &mut state) {
                        yield msg;
                    }
                }
                ReqMessage::Argument(mut value) => {
                    match state.as_ref() {
                        Some(State::Insert(_, select)) => {
                            MakeStorable::without_timestamps(ProcessorMode::Create, &ctx, ctx.system)
                                .make_storable(&mut value)?;

                            let timestamp = ctx.system.current_time();
                            let row = ctx.insert_vertex(value.into(), InsertMode::Insert, select, timestamp, &mut mut_ctx).await?;
                            yield RespMessage::Element(row.value, row.op);
                        }
                        Some(State::Update(_, select)) => {
                            MakeStorable::without_timestamps(ProcessorMode::Update, &ctx, ctx.system)
                                .make_storable(&mut value)?;

                            let timestamp = ctx.system.current_time();
                            let value = ctx.update_vertex_with_select(value.into(), select, timestamp, &mut mut_ctx).await?;
                            yield RespMessage::Element(value, DataOperation::Updated);
                        }
                        Some(State::Upsert(_, select)) => {
                            MakeStorable::without_timestamps(ProcessorMode::Create, &ctx, ctx.system)
                                .make_storable(&mut value)?;

                            let timestamp = ctx.system.current_time();
                            let row = ctx.insert_vertex(value.into(), InsertMode::Upsert, select, timestamp, &mut mut_ctx).await?;
                            yield RespMessage::Element(row.value, row.op);
                        }
                        Some(State::Delete(_, def_id)) => {
                            let deleted = ctx.delete_vertex(*def_id, value, &mut mut_ctx).await?;

                            yield RespMessage::Element(Value::boolean(deleted), DataOperation::Deleted);
                        }
                        None => {
                            Result::<(), DomainError>::Err(PgModelError::InvalidTransactionState.into())?;
                        }
                    }
                }
                ReqMessage::CrdtGet(_def_id, vertex_addr, prop_id) => {
                    let octets = ctx.crdt_get_by_vertex_addr(vertex_addr, prop_id).await?;

                    yield RespMessage::SequenceStart(0);
                    if let Some(octets) = octets {
                        yield RespMessage::Element(Value::octet_sequence(octets), DataOperation::Queried);
                    }
                    yield RespMessage::SequenceEnd(0, None);
                },
                ReqMessage::CrdtSaveIncremental(_def_id, vertex_addr, prop_id, payload) => {
                    ctx.save_crdt_incremental(vertex_addr, prop_id,  payload, &mut mut_ctx).await?;
                    yield RespMessage::SequenceStart(0);
                    yield RespMessage::SequenceEnd(0, None);
                },
            }
        }

        ctx.check_unresolved_foreign_keys(&mut_ctx)?;

        if let ConnectionState::Transaction(txn) = ctx.connection_state {
            txn.commit().await.map_err(PgError::CommitTransaction)?;
            trace!("COMMIT OK");
        }

        if let Some(write_stats) = mut_ctx.write_stats.finish(ctx.system) {
            yield RespMessage::WriteComplete(Box::new(write_stats));
            let _ = store.datastore_mutated.send(());
        }
    }
    .boxed())
}
