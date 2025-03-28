use std::{collections::BTreeSet, sync::Arc};

use domain_engine_core::{
    DomainResult, Session,
    data_store::{DataStoreAPI, DataStoreConnectionSync, DataStoreParams},
    transact::{DataOperation, ReqMessage, RespMessage, TransactionMode},
};
use futures_util::{StreamExt, stream::BoxStream};
use ontol_runtime::value::Value;
use unimock::{MockFn, Unimock, matching};

/// API that helps to linearize transaction values for use with the mock object
#[unimock::unimock(api = LinearTransactMock)]
#[async_trait::async_trait]
pub trait LinearTransact {
    async fn transact(
        &self,
        mode: TransactionMode,
        messages: Vec<DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<Vec<DomainResult<RespMessage>>>;
}

pub fn mock_data_store_query_entities_empty() -> impl unimock::Clause {
    LinearTransactMock::transact
        .next_call(matching!(
            TransactionMode::ReadOnly,
            [Ok(ReqMessage::Query(0, _))],
            _session
        ))
        .returns(Ok(vec![
            Ok(RespMessage::SequenceStart(0)),
            Ok(RespMessage::SequenceEnd(0, None)),
        ]))
}

pub fn respond_inserted(
    values: impl IntoIterator<Item = Value>,
) -> DomainResult<Vec<DomainResult<RespMessage>>> {
    let mut messages = vec![Ok(RespMessage::SequenceStart(0))];
    for value in values {
        messages.push(Ok(RespMessage::Element(value, DataOperation::Inserted)));
    }
    messages.push(Ok(RespMessage::SequenceEnd(0, None)));

    Ok(messages)
}

pub fn respond_queried(
    values: impl IntoIterator<Item = Value>,
) -> DomainResult<Vec<DomainResult<RespMessage>>> {
    let mut messages = vec![Ok(RespMessage::SequenceStart(0))];
    for value in values {
        messages.push(Ok(RespMessage::Element(value, DataOperation::Queried)));
    }
    messages.push(Ok(RespMessage::SequenceEnd(0, None)));

    Ok(messages)
}

pub struct LinearDataStoreAdapter(unimock::Unimock);

impl LinearDataStoreAdapter {
    pub fn new(unimock: unimock::Unimock) -> Self {
        Self(unimock)
    }
}

#[async_trait::async_trait]
impl DataStoreAPI for LinearDataStoreAdapter {
    async fn transact(
        &self,
        mode: TransactionMode,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        let messages: Vec<_> = messages.collect().await;
        let responses =
            <Unimock as LinearTransact>::transact(&self.0, mode, messages, session).await?;

        Ok(futures_util::stream::iter(responses).boxed())
    }
}

impl DataStoreConnectionSync for LinearDataStoreAdapter {
    fn migrate_sync(
        &self,
        _persisted: &BTreeSet<ontol_runtime::DomainIndex>,
        _params: DataStoreParams,
    ) -> DomainResult<Arc<dyn DataStoreAPI + Send + Sync>> {
        Ok(Arc::new(LinearDataStoreAdapter(self.0.clone())))
    }
}
