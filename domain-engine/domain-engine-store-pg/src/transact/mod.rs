use std::sync::Arc;

use domain_engine_core::{
    transact::{ReqMessage, RespMessage},
    DomainError, DomainResult,
};
use futures_util::{stream::BoxStream, StreamExt};
use mutate::InsertMode;
use ontol_runtime::{query::select::Select, DefId};
use tracing::debug;

use crate::{PgModel, PostgresDataStore};

mod mutate;

enum State {
    Insert(Select),
    Update(Select),
    Upsert(Select),
    Delete(DefId),
}

struct TransactCtx<'m, 't> {
    pg_model: &'m PgModel,
    txn: deadpool_postgres::Transaction<'t>,
}

pub async fn transact(
    store: Arc<PostgresDataStore>,
    messages: BoxStream<'static, DomainResult<ReqMessage>>,
) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
    let mut pg_client = store
        .pool
        .get()
        .await
        .map_err(|_| DomainError::data_store("could not acquire database connection"))?;

    Ok(async_stream::try_stream! {
        let txn = pg_client
            .build_transaction()
            .deferrable(false)
            // TODO: Could we get a hint on what kind of transaction is needed?
            // The interface levels of the system could know this.
            .isolation_level(tokio_postgres::IsolationLevel::Serializable)
            .start()
            .await
            .map_err(|err| {
                debug!("transaction not initiated: {err:?}");
                DomainError::data_store("could not initiate transaction")
            })?;

        let ctx = TransactCtx {
            pg_model: &store.pg_model,
            txn
        };

        if false {
            // FIXME: remove (needed for type inference)
            yield RespMessage::SequenceStart(0, None);
        }

        let mut state: Option<State> = None;

        for await message in messages {
            match message? {
                ReqMessage::Query(..) => {
                    Err(DomainError::data_store("Query not implemented for Postgres"))?;
                }
                ReqMessage::Insert(op_seq, select) => {
                    state = Some(State::Insert(select));
                    yield RespMessage::SequenceStart(op_seq, None);
                }
                ReqMessage::Update(op_seq, select) => {
                    state = Some(State::Update(select));
                    yield RespMessage::SequenceStart(op_seq, None);
                }
                ReqMessage::Upsert(op_seq, select) => {
                    state = Some(State::Upsert(select));
                    yield RespMessage::SequenceStart(op_seq, None);
                }
                ReqMessage::Delete(op_seq, def_id) => {
                    state = Some(State::Delete(def_id));
                    yield RespMessage::SequenceStart(op_seq, None);
                }
                ReqMessage::Argument(value) => {
                    match state.as_ref() {
                        Some(State::Insert(select)) => {
                            let (value, op) = ctx.insert(value.into(), InsertMode::Insert, select).await?;
                            yield RespMessage::Element(value, op);
                        }
                        Some(State::Update(_select)) => {
                            Err(DomainError::data_store("Update not implemented for Postgres"))?;
                        }
                        Some(State::Upsert(select)) => {
                            let (value, op) = ctx.insert(value.into(), InsertMode::Upsert, select).await?;
                            yield RespMessage::Element(value, op);
                        }
                        Some(State::Delete(_def_id)) => {
                            Err(DomainError::data_store("Delete not implemented for Postgres"))?;
                        }
                        None => {
                            Err(DomainError::data_store("invalid transaction state"))?
                        }
                    }
                }
            }
        }

        ctx.txn.commit().await.map_err(|err| {
            debug!("transaction not committed: {err:?}");
            DomainError::data_store("transaction could not be commmitted")
        })?;
    }
    .boxed())
}
