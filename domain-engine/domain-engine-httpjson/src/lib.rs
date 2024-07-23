use std::{convert::Infallible, sync::Arc};

use axum::{
    extract::{FromRequest, State},
    http::{header::CONTENT_TYPE, StatusCode},
    response::IntoResponse,
    routing::{MethodFilter, MethodRouter},
};
use axum_extra::extract::JsonLines;
use domain_engine_core::{DomainEngine, Session};
use ontol_runtime::{
    attr::Attr,
    interface::{
        serde::{operator::SerdeOperatorAddr, processor::ProcessorMode},
        DomainInterface,
    },
    query::select::Select,
    PackageId,
};
use serde::de::DeserializeSeed;
use serde::Serialize;
use tokio_stream::StreamExt;

const JSON: &[u8] = b"application/json";
const JSONLINES: &[u8] = b"application/jsonlines";

#[derive(Serialize)]
pub struct ErrorJson {
    message: String,
}

#[derive(Clone)]
struct Endpoint {
    domain_engine: Arc<DomainEngine>,
    operator_addr: SerdeOperatorAddr,
}

pub fn create_httpjson_router(
    engine: Arc<DomainEngine>,
    package_id: PackageId,
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
        });

        let route_name = format!("/{}", &engine.ontology()[resource.name]);
        domain_router = domain_router.route(&route_name, method_router);
    }

    Some(domain_router)
}

async fn put_resource(
    State(endpoint): State<Endpoint>,
    request: axum::http::Request<axum::body::Body>,
) -> axum::response::Response {
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .map(|value| value.as_bytes());

    let serde_processor = endpoint
        .domain_engine
        .ontology()
        // FIXME: Need a new mode for put/"upsert" semantics?
        .new_serde_processor(endpoint.operator_addr, ProcessorMode::Create);

    match content_type {
        Some(JSON) | None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Some(JSONLINES) => {
            let Ok(mut lines): Result<JsonLines<serde_json::Value>, _> =
                FromRequest::from_request(request, &()).await
            else {
                return StatusCode::BAD_REQUEST.into_response();
            };

            while let Some(result) = read_next_json_line(&mut lines).await {
                let json_value = match result {
                    Ok(value) => value,
                    Err(err) => {
                        return (StatusCode::BAD_REQUEST, axum::Json(err)).into_response();
                    }
                };
                let ontol_attr = match serde_processor.deserialize(json_value) {
                    Ok(value) => value,
                    Err(error) => {
                        return (StatusCode::BAD_REQUEST, json_error(format!("{error}")))
                            .into_response()
                    }
                };
                let ontol_value = match ontol_attr {
                    Attr::Unit(value) => value,
                    _ => {
                        return (
                            StatusCode::BAD_REQUEST,
                            json_error("bad format".to_string()),
                        )
                            .into_response();
                    }
                };

                let result = endpoint
                    .domain_engine
                    .store_new_entity(ontol_value, Select::Leaf, Session::default())
                    .await;

                if let Err(error) = result {
                    return (StatusCode::BAD_REQUEST, json_error(format!("{error}")))
                        .into_response();
                }
            }

            StatusCode::OK.into_response()
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
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
