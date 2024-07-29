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
use domain_engine_core::{transact::ReqMessage, DomainEngine, DomainResult, Session};
use futures_util::{stream::StreamExt, TryStreamExt};
use http_error::{domain_error_to_response, json_error, ErrorJson};
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
    PackageId,
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
    package_id: PackageId,
) -> Option<axum::Router<State>>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Send + Into<Session> + 'static,
{
    let httpjson = engine
        .ontology()
        .domain_interfaces(package_id)
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

    let serde_processor = endpoint
        .engine
        .ontology()
        .new_serde_processor(endpoint.operator_addr, ProcessorMode::Update);

    let mut messages = vec![ReqMessage::Upsert(0, Select::EntityId)];

    match json_content_type {
        JsonContentType::Json => {
            let bytes = match Bytes::from_request(req, &()).await {
                Ok(bytes) => bytes,
                Err(err) => return err.into_response(),
            };

            let ontol_value = match deserialize_ontol_value(
                &serde_processor,
                &mut serde_json::Deserializer::from_slice(&bytes),
            ) {
                Ok(value) => value,
                Err(response) => return response,
            };

            messages.push(ReqMessage::Argument(ontol_value));
        }
        JsonContentType::JsonLines => {
            // TODO: transaction streaming
            let mut lines = match JsonLines::<serde_json::Value>::from_request(req, &()).await {
                Ok(lines) => lines,
                Err(err) => return err.into_response(),
            };

            while let Some(result) = read_next_json_line(&mut lines).await {
                let json_value = match result {
                    Ok(value) => value,
                    Err(err) => {
                        return (StatusCode::BAD_REQUEST, axum::Json(err)).into_response();
                    }
                };
                let ontol_value = match deserialize_ontol_value(&serde_processor, json_value) {
                    Ok(value) => value,
                    Err(response) => return response,
                };
                messages.push(ReqMessage::Argument(ontol_value));
            }
        }
    };

    let result = endpoint
        .engine
        .transact(
            futures_util::stream::iter(messages).boxed(),
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

async fn read_next_json_line(
    payload: &mut JsonLines<serde_json::Value>,
) -> Option<Result<serde_json::Value, ErrorJson>> {
    payload.next().await.map(|result| {
        result.map_err(|err| ErrorJson {
            message: format!("Failed to read JSON: {err}"),
        })
    })
}

fn deserialize_ontol_value<'d>(
    processor: &SerdeProcessor,
    deserializer: impl Deserializer<'d, Error = serde_json::Error>,
) -> Result<Value, axum::response::Response> {
    let ontol_attr = match processor.deserialize(deserializer) {
        Ok(value) => value,
        Err(error) => {
            return Err(match error.classify() {
                serde_json::error::Category::Data => (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    json_error(format!("{error}")),
                )
                    .into_response(),
                _ => (StatusCode::BAD_REQUEST, json_error(format!("{error}"))).into_response(),
            })
        }
    };
    let ontol_value = match ontol_attr {
        Attr::Unit(value) => value,
        _ => {
            error!("deserialized attribute was not a unit");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                json_error("server error".to_string()),
            )
                .into_response());
        }
    };

    Ok(ontol_value)
}
