use std::sync::Arc;

use domain_engine_core::{
    transact::{AccumulateSequences, ReqMessage, RespMessage, TransactionMode},
    DomainEngine, DomainResult, Session,
};
use futures_util::{StreamExt, TryStreamExt};
use ontol_runtime::{
    query::select::{EntitySelect, Select},
    sequence::Sequence,
    value::Value,
};

pub async fn insert_entity_select_entityid(
    engine: &Arc<DomainEngine>,
    entity: Value,
) -> DomainResult<Value> {
    let response_messages: Vec<_> = engine
        .transact(
            TransactionMode::ReadWriteAtomic,
            futures_util::stream::iter(vec![
                Ok(ReqMessage::Insert(0, Select::EntityId)),
                Ok(ReqMessage::Argument(entity)),
            ])
            .boxed(),
            Session::default(),
        )
        .await?
        .try_collect()
        .await?;

    let mut iter = response_messages.into_iter();
    let _sequence_start = iter.next().unwrap();
    let RespMessage::Element(value, _) = iter.next().unwrap() else {
        panic!()
    };

    Ok(value)
}

pub async fn query_entities(
    engine: &Arc<DomainEngine>,
    select: EntitySelect,
) -> DomainResult<Sequence<Value>> {
    let responses: Vec<_> = engine
        .transact(
            TransactionMode::ReadOnly,
            futures_util::stream::iter(vec![Ok(ReqMessage::Query(0, select))]).boxed(),
            Session::default(),
        )
        .await?
        .accumulate_sequences()
        .try_collect()
        .await?;

    Ok(responses.into_iter().next().unwrap())
}
