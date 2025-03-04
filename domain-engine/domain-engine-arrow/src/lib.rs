use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use batch_builder::RecordBatchBuilder;
use domain_engine_core::{
    DomainEngine, DomainResult, Session,
    transact::{ReqMessage, TransactionMode},
};
use futures_util::{StreamExt, stream::BoxStream};
use ontol_runtime::{PropId, query::select::EntitySelect};
use schema::FieldType;
use serde::{Deserialize, Serialize};

pub mod arrow_http;
pub mod schema;

mod batch_builder;

/// API for executing queries translated to the Apache Arrow format.
pub trait ArrowTransactAPI {
    fn arrow_transact(
        &self,
        req: ArrowReqMessage,
        config: ArrowConfig,
        session: Session,
    ) -> BoxStream<'static, DomainResult<ArrowRespMessage>>;
}

pub struct ArrowConfig {
    pub batch_size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ArrowReqMessage {
    Query(ArrowQuery),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ArrowQuery {
    pub entity_select: EntitySelect,
    pub column_selection: Vec<(PropId, FieldType)>,
    pub projection: Option<Vec<usize>>,
    pub schema: SchemaRef,
}

pub enum ArrowRespMessage {
    RecordBatch(RecordBatch),
}

impl ArrowTransactAPI for Arc<DomainEngine> {
    fn arrow_transact(
        &self,
        req: ArrowReqMessage,
        config: ArrowConfig,
        session: Session,
    ) -> BoxStream<'static, DomainResult<ArrowRespMessage>> {
        match req {
            ArrowReqMessage::Query(query) => {
                let domain_engine = self.clone();
                let ontology = domain_engine.ontology_owned();

                let msg_stream =
                    futures_util::stream::iter([Ok(ReqMessage::Query(0, query.entity_select))])
                        .boxed();
                let mut batch_builder = RecordBatchBuilder::new(
                    query.column_selection,
                    query.schema,
                    ontology,
                    config.batch_size,
                );

                async_stream::try_stream! {
                    let datastore_stream = domain_engine.transact(
                        TransactionMode::ReadOnly,
                        msg_stream,
                        session,
                    )
                    .await?;

                    for await result in datastore_stream {
                        if let Some(batch) = batch_builder.handle_msg(result?) {
                            yield ArrowRespMessage::RecordBatch(batch);
                        }
                    }

                    if let Some(batch) = batch_builder.yield_batch() {
                        yield ArrowRespMessage::RecordBatch(batch);
                    }
                }
                .boxed()
            }
        }
    }
}
