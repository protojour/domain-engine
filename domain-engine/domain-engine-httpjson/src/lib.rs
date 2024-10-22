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
use bytes::{BufMut, Bytes, BytesMut};
use content_type::JsonContentType;
use domain_engine_core::{
    domain_error::DomainErrorKind,
    domain_select::domain_select_no_edges,
    transact::{AccumulateSequences, ReqMessage, TransactionMode},
    DomainEngine, DomainError, DomainResult, Session,
};
use futures_util::{stream::StreamExt, TryStreamExt};
use http::{header, HeaderValue};
use http_error::{json_error, HttpJsonError};
use ontol_runtime::{
    attr::{Attr, AttrRef},
    interface::{
        serde::{
            operator::SerdeOperatorAddr,
            processor::{ProcessorMode, SerdeProcessor},
        },
        DomainInterface,
    },
    query::{
        condition::{Clause, CondTerm, SetOperator},
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect},
    },
    sequence::Sequence,
    value::Value,
    DefId, DomainIndex, PropId,
};
use serde::{
    de::{value::StringDeserializer, DeserializeSeed},
    Deserializer,
};
use tracing::{debug, error};

pub mod http_error;

mod content_type;

#[derive(Clone)]
struct UnkeyedEndpoint {
    engine: Arc<DomainEngine>,
    operator_addr: SerdeOperatorAddr,
}

#[derive(Clone)]
struct KeyedEndpoint {
    engine: Arc<DomainEngine>,
    resource_def_id: DefId,
    key_operator_addr: SerdeOperatorAddr,
    key_prop_id: PropId,
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
    let ontology = engine.ontology();

    for resource in &httpjson.resources {
        let mut method_router: MethodRouter<State, Infallible> = MethodRouter::default();

        if resource.put.is_some() {
            method_router =
                method_router.on(MethodFilter::PUT, put_resource_unkeyed::<State, Auth>);
        }

        let route_name = format!("/{resource}", resource = &ontology[resource.name]);
        debug!("add route `{route_name}`");

        domain_router = domain_router.route(
            &route_name,
            method_router.layer(Extension(UnkeyedEndpoint {
                engine: engine.clone(),
                operator_addr: resource.operator_addr,
            })),
        );

        for keyed in &resource.keyed {
            let mut method_router: MethodRouter<State, Infallible> = MethodRouter::default();

            if keyed.get.is_some() {
                method_router =
                    method_router.on(MethodFilter::GET, get_resource_keyed::<State, Auth>);
            }

            let route_name = format!(
                "/{resource}/{key}/:{key}",
                resource = &ontology[resource.name],
                key = &ontology[keyed.key_name]
            );

            debug!("add route `{route_name}`");

            domain_router = domain_router.route(
                &route_name,
                method_router.layer(Extension(KeyedEndpoint {
                    engine: engine.clone(),
                    resource_def_id: resource.def_id,
                    key_operator_addr: keyed.key_operator_addr,
                    key_prop_id: keyed.key_prop_id,
                    operator_addr: resource.operator_addr,
                })),
            );
        }
    }

    Some(domain_router)
}

async fn put_resource_unkeyed<State, Auth>(
    Extension(endpoint): Extension<UnkeyedEndpoint>,
    auth: Auth,
    req: axum::http::Request<Body>,
) -> Result<axum::response::Response, HttpJsonError>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Into<Session> + 'static,
{
    let session = auth.into();

    let json_content_type = match JsonContentType::parse(&req) {
        Ok(ct) => ct,
        Err(err) => return Ok(err.into_response()),
    };

    let transaction_msg_stream = match json_content_type {
        JsonContentType::Json => {
            let bytes = match Bytes::from_request(req, &()).await {
                Ok(bytes) => bytes,
                Err(err) => return Ok(err.into_response()),
            };

            let ontol_value = deserialize_ontol_value(
                &endpoint
                    .engine
                    .ontology()
                    .new_serde_processor(endpoint.operator_addr, ProcessorMode::Update),
                &mut serde_json::Deserializer::from_slice(&bytes),
            )?;

            futures_util::stream::iter([
                Ok(ReqMessage::Upsert(0, Select::EntityId)),
                Ok(ReqMessage::Argument(ontol_value)),
            ])
            .boxed()
        }
        JsonContentType::JsonLines => {
            let lines = match JsonLines::<serde_json::Value>::from_request(req, &()).await {
                Ok(lines) => lines,
                Err(err) => return Ok(err.into_response()),
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

    let stream = endpoint
        .engine
        .transact(
            TransactionMode::ReadWriteAtomic,
            transaction_msg_stream,
            session.clone(),
        )
        .await?;

    let collected: DomainResult<Vec<_>> = stream.try_collect().await;
    let _ = collected?;

    Ok(StatusCode::OK.into_response())
}

async fn get_resource_keyed<State, Auth>(
    Extension(endpoint): Extension<KeyedEndpoint>,
    auth: Auth,
    key: axum::extract::Path<String>,
) -> Result<axum::response::Response, HttpJsonError>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Into<Session> + 'static,
{
    let session = auth.into();
    let ontology = endpoint.engine.ontology();
    let key = match ontology
        .new_serde_processor(endpoint.key_operator_addr, ProcessorMode::Read)
        .deserialize(StringDeserializer::<serde_json::Error>::new(key.0))
    {
        Ok(Attr::Unit(key)) => key,
        Ok(_) => {
            error!("key must be a unit attr");
            return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
        Err(err) => {
            return Ok((
                StatusCode::BAD_REQUEST,
                json_error(format!("invalid path parameter: {err}")),
            )
                .into_response());
        }
    };

    let entity_select = {
        let struct_select =
            match domain_select_no_edges(endpoint.resource_def_id, ontology.as_ref()) {
                Select::Struct(struct_select) => struct_select,
                _ => {
                    error!("must be struct select");
                    return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
                }
            };
        let filter = {
            let mut filter = Filter::default_for_domain();
            let condition = filter.condition_mut();
            let cond_var = condition.mk_cond_var();
            let set_var = condition.mk_cond_var();
            condition.add_clause(
                cond_var,
                Clause::MatchProp(endpoint.key_prop_id, SetOperator::ElementIn, set_var),
            );
            condition.add_clause(
                set_var,
                Clause::Member(CondTerm::Wildcard, CondTerm::Value(key)),
            );
            filter
        };

        EntitySelect {
            source: StructOrUnionSelect::Struct(struct_select),
            filter,
            limit: Some(1),
            after_cursor: None,
            include_total_len: false,
        }
    };

    let transaction_msg_stream =
        futures_util::stream::iter([Ok(ReqMessage::Query(0, entity_select))]).boxed();

    let collected: Vec<Sequence<Value>> = endpoint
        .engine
        .transact(
            TransactionMode::ReadOnly,
            transaction_msg_stream,
            session.clone(),
        )
        .await?
        .accumulate_sequences()
        .try_collect()
        .await?;

    let Some(first_sequence) = collected.into_iter().next() else {
        return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
    };
    let Some(value) = first_sequence.into_elements().into_iter().next() else {
        return Err(DomainErrorKind::EntityNotFound.into_error().into());
    };

    let mut json_bytes = BytesMut::with_capacity(128).writer();

    ontology
        .new_serde_processor(endpoint.operator_addr, ProcessorMode::Read)
        .serialize_attr(
            AttrRef::Unit(&value),
            &mut serde_json::Serializer::new(&mut json_bytes),
        )
        .map_err(|err| DomainErrorKind::Interface(format!("{err}")).into_error())?;

    Ok((
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static(mime::APPLICATION_JSON.as_ref()),
        )],
        json_bytes.into_inner().freeze(),
    )
        .into_response())
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
