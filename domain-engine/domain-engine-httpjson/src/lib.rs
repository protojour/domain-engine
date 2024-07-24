use std::{convert::Infallible, sync::Arc};

use axum::{
    body::Body,
    extract::{FromRequest, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{MethodFilter, MethodRouter},
};
use axum_extra::extract::JsonLines;
use bytes::Bytes;
use content_type::JsonContentType;
use domain_engine_core::{data_store::BatchWriteRequest, DomainEngine, Session};
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
use serde::Serialize;
use serde::{de::DeserializeSeed, Deserializer};
use tokio_stream::StreamExt;
use tracing::error;

mod content_type;

pub type SessionFromRequest = fn(&http::Request<Body>) -> Result<Session, (StatusCode, String)>;

#[derive(Serialize)]
pub struct ErrorJson {
    message: String,
}

#[derive(Clone)]
struct Endpoint {
    domain_engine: Arc<DomainEngine>,
    operator_addr: SerdeOperatorAddr,
    session_from_request: SessionFromRequest,
}

pub fn create_httpjson_router(
    engine: Arc<DomainEngine>,
    package_id: PackageId,
    session_from_request: SessionFromRequest,
) -> Option<axum::Router> {
    let httpjson = engine
        .ontology()
        .domain_interfaces(package_id)
        .iter()
        .filter_map(|interface| match interface {
            DomainInterface::HttpJson(httpjson) => Some(httpjson),
            _ => None,
        })
        .next()?;

    let mut domain_router: axum::Router = axum::Router::new();

    for resource in &httpjson.resources {
        let mut method_router: MethodRouter<Endpoint, Infallible> = MethodRouter::default();

        if resource.put.is_some() {
            method_router = method_router.on(MethodFilter::PUT, put_resource);
        }

        let method_router: MethodRouter<(), Infallible> = method_router.with_state(Endpoint {
            domain_engine: engine.clone(),
            operator_addr: resource.operator_addr,
            session_from_request,
        });

        let route_name = format!("/{}", &engine.ontology()[resource.name]);
        domain_router = domain_router.route(&route_name, method_router);
    }

    Some(domain_router)
}

async fn put_resource(
    State(endpoint): State<Endpoint>,
    req: axum::http::Request<Body>,
) -> axum::response::Response {
    let session = match (endpoint.session_from_request)(&req) {
        Ok(session) => session,
        Err((status, msg)) => return (status, json_error(msg)).into_response(),
    };

    let json_content_type = match JsonContentType::parse(&req) {
        Ok(ct) => ct,
        Err(err) => return err.into_response(),
    };

    let serde_processor = endpoint
        .domain_engine
        .ontology()
        .new_serde_processor(endpoint.operator_addr, ProcessorMode::Update);

    let mut values = vec![];

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

            values.push(ontol_value);
        }
        JsonContentType::JsonLines => {
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
                values.push(ontol_value);
            }
        }
    };

    let result = endpoint
        .domain_engine
        .execute_writes(
            vec![BatchWriteRequest::Upsert(values, Select::EntityId)],
            session.clone(),
        )
        .await;

    if let Err(error) = result {
        return (StatusCode::BAD_REQUEST, json_error(format!("{error}"))).into_response();
    }

    StatusCode::OK.into_response()
}

fn json_error(message: String) -> axum::Json<ErrorJson> {
    axum::Json(ErrorJson { message })
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
