use domain_engine_core::{
    data_store::{DataStoreAPI, Request, Response},
    transact::{ReqMessage, RespMessage, ValueReason},
    DomainResult, Session,
};
use futures_util::{stream::BoxStream, StreamExt};
use ontol_runtime::value::Value;
use unimock::{matching, MockFn, Unimock};

/// API that helps to linearize transaction values for use with the mock object
#[unimock::unimock(api = LinearTransactMock)]
#[async_trait::async_trait]
pub trait LinearTransact {
    async fn transact<'a>(
        &self,
        messages: Vec<DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<Vec<DomainResult<RespMessage>>>;
}

pub fn mock_data_store_query_entities_empty() -> impl unimock::Clause {
    LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Query(0, _))], _session))
        .returns(Ok(vec![Ok(RespMessage::SequenceStart(0, None))]))
}

pub fn respond_inserted(
    values: impl IntoIterator<Item = Value>,
) -> DomainResult<Vec<DomainResult<RespMessage>>> {
    let mut messages = vec![Ok(RespMessage::SequenceStart(0, None))];
    for value in values {
        messages.push(Ok(RespMessage::Element(value, ValueReason::Inserted)));
    }

    Ok(messages)
}

pub fn respond_queried(
    values: impl IntoIterator<Item = Value>,
) -> DomainResult<Vec<DomainResult<RespMessage>>> {
    let mut messages = vec![Ok(RespMessage::SequenceStart(0, None))];
    for value in values {
        messages.push(Ok(RespMessage::Element(value, ValueReason::Queried)));
    }

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
    async fn execute(
        &self,
        _request: Request,
        _session: domain_engine_core::Session,
    ) -> DomainResult<Response> {
        unimplemented!()
    }

    async fn transact<'a>(
        &'a self,
        messages: BoxStream<'a, DomainResult<ReqMessage>>,
        session: Session,
    ) -> DomainResult<BoxStream<'_, DomainResult<RespMessage>>> {
        let messages: Vec<_> = messages.collect().await;
        let responses = <Unimock as LinearTransact>::transact(&self.0, messages, session).await?;

        Ok(futures_util::stream::iter(responses).boxed())
    }
}
