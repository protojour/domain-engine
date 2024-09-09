#![forbid(unsafe_code)]

use std::{convert::Infallible, sync::Arc};

use axum::{
    body::Body,
    extract::{FromRequest, FromRequestParts},
    http::StatusCode,
    response::IntoResponse,
    routing::{MethodFilter, MethodRouter},
    Extension,
};
use axum_extra::extract::JsonLines;
use bytes::Bytes;
use content_type::JsonContentType;
use domain_engine_core::{
    domain_error::DomainErrorKind,
    transact::{ReqMessage, TransactionMode},
    DomainEngine, DomainError, DomainResult, Session,
};
use futures_util::{stream::StreamExt, TryStreamExt};
use http_error::domain_error_to_response;
use ontol_runtime::{
    attr::Attr,
    interface::{
        serde::{
            operator::SerdeOperatorAddr,
            processor::{ProcessorMode, SerdeProcessor},
        },
        DomainInterface,
    },
    query::select::Select,
    value::Value,
    DomainIndex,
};
use serde::{de::DeserializeSeed, Deserializer};
use tracing::{debug, error};

mod content_type;
mod http_error;

#[derive(Clone)]
struct Endpoint {
    engine: Arc<DomainEngine>,
    operator_addr: SerdeOperatorAddr,
}

pub fn create_httpjson_router<State, Auth>(
    engine: Arc<DomainEngine>,
    domain_index: DomainIndex,
) -> Option<axum::Router<State>>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Send + Into<Session> + 'static,
{
    let httpjson = engine
        .ontology()
        .domain_interfaces(domain_index)
        .iter()
        .filter_map(|interface| match interface {
            DomainInterface::HttpJson(httpjson) => Some(httpjson),
            _ => None,
        })
        .next()?;

    let mut domain_router: axum::Router<State> = axum::Router::new();

    for resource in &httpjson.resources {
        let mut method_router: MethodRouter<State, Infallible> = MethodRouter::default();

        if resource.put.is_some() {
            method_router = method_router.on(MethodFilter::PUT, put_resource::<State, Auth>);
        }

        let route_name = format!("/{}", &engine.ontology()[resource.name]);
        debug!("add route `{route_name}`");

        domain_router = domain_router.route(
            &route_name,
            method_router.layer(Extension(Endpoint {
                engine: engine.clone(),
                operator_addr: resource.operator_addr,
            })),
        );
    }

    Some(domain_router)
}

async fn put_resource<State, Auth>(
    Extension(endpoint): Extension<Endpoint>,
    auth: Auth,
    req: axum::http::Request<Body>,
) -> axum::response::Response
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Into<Session> + 'static,
{
    let session = auth.into();

    let json_content_type = match JsonContentType::parse(&req) {
        Ok(ct) => ct,
        Err(err) => return err.into_response(),
    };

    let transaction_msg_stream = match json_content_type {
        JsonContentType::Json => {
            let bytes = match Bytes::from_request(req, &()).await {
                Ok(bytes) => bytes,
                Err(err) => return err.into_response(),
            };

            let ontol_value = match deserialize_ontol_value(
                &endpoint
                    .engine
                    .ontology()
                    .new_serde_processor(endpoint.operator_addr, ProcessorMode::Update),
                &mut serde_json::Deserializer::from_slice(&bytes),
            ) {
                Ok(value) => value,
                Err(error) => return domain_error_to_response(error),
            };

            futures_util::stream::iter([
                Ok(ReqMessage::Upsert(0, Select::EntityId)),
                Ok(ReqMessage::Argument(ontol_value)),
            ])
            .boxed()
        }
        JsonContentType::JsonLines => {
            let lines = match JsonLines::<serde_json::Value>::from_request(req, &()).await {
                Ok(lines) => lines,
                Err(err) => return err.into_response(),
            };
            let endpoint = endpoint.clone();

            async_stream::try_stream! {
                yield ReqMessage::Upsert(0, Select::EntityId);

                for await json_result in lines {
                    let json = json_result.map_err(|e| DomainErrorKind::BadInputFormat(format!("{e}")).into_error())?;
                    let ontol_value = deserialize_ontol_value(&endpoint
                        .engine
                        .ontology()
                        .new_serde_processor(endpoint.operator_addr, ProcessorMode::Update), json)?;

                    yield ReqMessage::Argument(ontol_value);
                }
            }
            .boxed()
        }
    };

    let result = endpoint
        .engine
        .transact(
            TransactionMode::ReadWriteAtomic,
            transaction_msg_stream,
            session.clone(),
        )
        .await;

    let stream = match result {
        Ok(stream) => stream,
        Err(error) => return domain_error_to_response(error),
    };

    let collected: DomainResult<Vec<_>> = stream.try_collect().await;
    match collected {
        Ok(_) => StatusCode::OK.into_response(),
        Err(error) => domain_error_to_response(error),
    }
}

fn deserialize_ontol_value<'d>(
    processor: &SerdeProcessor,
    deserializer: impl Deserializer<'d, Error = serde_json::Error>,
) -> Result<Value, DomainError> {
    let ontol_attr = match processor.deserialize(deserializer) {
        Ok(value) => value,
        Err(error) => {
            return Err(match error.classify() {
                serde_json::error::Category::Data => {
                    DomainErrorKind::BadInputData(format!("{error}")).into_error()
                }
                _ => DomainErrorKind::BadInputFormat(format!("{error}")).into_error(),
            })
        }
    };
    let ontol_value = match ontol_attr {
        Attr::Unit(value) => value,
        _ => {
            error!("deserialized attribute was not a unit");
            return Err(DomainErrorKind::DeserializationFailed.into_error());
        }
    };

    Ok(ontol_value)
}
