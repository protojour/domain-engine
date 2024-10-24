#![forbid(unsafe_code)]

use std::{convert::Infallible, sync::Arc};

use axum::{
    body::Body,
    extract::{FromRequest, FromRequestParts, OriginalUri},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, MethodFilter, MethodRouter},
    Extension,
};
use axum_extra::extract::JsonLines;
use bytes::{BufMut, Bytes, BytesMut};
use content_type::JsonContentType;
use crdt::{
    broker::{load_broker, BrokerManagerHandle},
    doc_repository::DocRepository,
    sync_session::SyncSession,
    ActorExt, DocAddr,
};
use domain_engine_core::{
    domain_error::DomainErrorKind,
    domain_select::domain_select_no_edges,
    transact::{AccumulateSequences, ReqMessage, TransactionMode},
    CrdtActor, DomainEngine, DomainError, Session,
};
use futures_util::{stream::StreamExt, TryStreamExt};
use http::{header, HeaderValue, Uri};
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
    ontology::ontol::TextConstant,
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
    Deserialize, Deserializer,
};
use tracing::{debug, error};
use uuid::Uuid;

pub mod crdt;
pub mod http_error;

mod content_type;

#[derive(Default)]
pub struct DomainRouterBuilder {
    broker_manager: BrokerManagerHandle,
}

#[derive(Clone)]
struct UnkeyedEndpoint {
    engine: Arc<DomainEngine>,
    operator_addr: SerdeOperatorAddr,
    keyed_name: Option<TextConstant>,
}

#[derive(Clone)]
struct KeyedEndpoint {
    engine: Arc<DomainEngine>,
    resource_def_id: DefId,
    key_operator_addr: SerdeOperatorAddr,
    key_prop_id: PropId,
    operator_addr: SerdeOperatorAddr,
}

#[derive(Clone)]
struct CrdtBrokerEndpoint {
    doc_repository: DocRepository,
    broker_manager: BrokerManagerHandle,
    resource_def_id: DefId,
    key_operator_addr: SerdeOperatorAddr,
    crdt_prop_id: PropId,
}

impl DomainRouterBuilder {
    pub fn create_httpjson_router<State, Auth>(
        &self,
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

            if resource.post.is_some() {
                method_router =
                    method_router.on(MethodFilter::POST, post_resource_unkeyed::<State, Auth>);
            }

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
                    keyed_name: resource.keyed.iter().map(|keyed| keyed.key_name).next(),
                })),
            );

            for keyed in &resource.keyed {
                let resource_name = &ontology[resource.name];
                let key_name = &ontology[keyed.key_name];

                let mut method_router: MethodRouter<State, Infallible> = MethodRouter::default();

                if keyed.get.is_some() {
                    method_router =
                        method_router.on(MethodFilter::GET, get_resource_keyed::<State, Auth>);
                }

                let route_name = format!("/{resource_name}/{key_name}/:{key_name}");

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

                for (crdt_prop_id, prop_name) in &keyed.crdts {
                    let prop_name = &ontology[*prop_name];

                    let sync_route_name =
                        format!("/{resource_name}/{key_name}/:{key_name}/{prop_name}");
                    debug!("add route `{sync_route_name}`");
                    domain_router = domain_router.route(
                        &sync_route_name,
                        get(get_crdt_broker_ws::<State, Auth>).layer(Extension(
                            CrdtBrokerEndpoint {
                                doc_repository: DocRepository::from(engine.clone()),
                                broker_manager: self.broker_manager.clone(),
                                resource_def_id: resource.def_id,
                                key_operator_addr: keyed.key_operator_addr,
                                crdt_prop_id: *crdt_prop_id,
                            },
                        )),
                    );

                    let actor_route_name =
                        format!("/{resource_name}/{key_name}/:{key_name}/{prop_name}/actor");
                    debug!("add route `{actor_route_name}`");
                    domain_router = domain_router.route(
                        &actor_route_name,
                        post(post_crdt_actor::<State, Auth>).layer(Extension(CrdtBrokerEndpoint {
                            doc_repository: DocRepository::from(engine.clone()),
                            broker_manager: self.broker_manager.clone(),
                            resource_def_id: resource.def_id,
                            key_operator_addr: keyed.key_operator_addr,
                            crdt_prop_id: *crdt_prop_id,
                        })),
                    );
                }
            }
        }

        Some(domain_router)
    }
}

///
/// POST /resource
///
async fn post_resource_unkeyed<State, Auth>(
    endpoint: Extension<UnkeyedEndpoint>,
    auth: Auth,
    OriginalUri(original_uri): OriginalUri,
    req: axum::http::Request<Body>,
) -> Result<axum::response::Response, HttpJsonError>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Into<Session> + 'static,
{
    let json_content_type = match JsonContentType::parse(&req) {
        Ok(ct) => ct,
        Err(err) => return Ok(err.into_response()),
    };
    let ontology = endpoint.engine.ontology_owned();
    let keyed_name = endpoint.keyed_name;

    let sequence = create::create_common(
        ProcessorMode::Create,
        json_content_type,
        endpoint,
        auth,
        req,
    )
    .await?;

    match json_content_type {
        JsonContentType::Json => {
            // one element, set Location header
            let Some(value) = sequence.into_elements().into_iter().next() else {
                error!("expected one element");
                return Ok(StatusCode::CREATED.into_response());
            };

            let Some(keyed_name) = keyed_name else {
                return Ok(StatusCode::CREATED.into_response());
            };

            let location_uri = {
                let mut parts = original_uri.into_parts();
                let path = parts.path_and_query.as_ref().unwrap().path();

                let location_path = format!(
                    "{path}/{key_name}/{key}",
                    key_name = &ontology[keyed_name],
                    key = ontol_runtime::format_utils::format_value(&value, ontology.as_ref())
                );

                parts.path_and_query = Some(location_path.parse().unwrap());
                Uri::from_parts(parts).unwrap()
            };

            Ok((
                [(
                    header::LOCATION,
                    HeaderValue::from_str(&location_uri.to_string()).unwrap(),
                )],
                StatusCode::CREATED,
            )
                .into_response())
        }
        JsonContentType::JsonLines => Ok(StatusCode::OK.into_response()),
    }
}

///
/// PUT /resource
///
async fn put_resource_unkeyed<State, Auth>(
    endpoint: Extension<UnkeyedEndpoint>,
    auth: Auth,
    req: axum::http::Request<Body>,
) -> Result<axum::response::Response, HttpJsonError>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Into<Session> + 'static,
{
    let json_content_type = match JsonContentType::parse(&req) {
        Ok(ct) => ct,
        Err(err) => return Ok(err.into_response()),
    };
    create::create_common(
        ProcessorMode::Update,
        json_content_type,
        endpoint,
        auth,
        req,
    )
    .await?;

    Ok(StatusCode::OK.into_response())
}

///
/// GET /resource/{key_name}/:{key}
///
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
    let key = match key::deserialize_key(key.0, endpoint.key_operator_addr, ontology) {
        Ok(key) => key,
        Err(response) => return Ok(response),
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

#[derive(Deserialize)]
struct CrdtBrokerParams {
    actor: String,
}

///
/// GET /resource/{key_name}/:{key}/{crdt_prop}
///
/// This is the WebSocket sync for the CRDT.
///
async fn get_crdt_broker_ws<State, Auth>(
    Extension(endpoint): Extension<CrdtBrokerEndpoint>,
    auth: Auth,
    key: axum::extract::Path<String>,
    params: axum::extract::Query<CrdtBrokerParams>,
    ws_upgrade: axum::extract::WebSocketUpgrade,
) -> Result<axum::response::Response, HttpJsonError>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Into<Session> + 'static,
{
    let session = auth.into();
    let ontology = endpoint.doc_repository.domain_engine().ontology();
    let key = match key::deserialize_key(key.0, endpoint.key_operator_addr, ontology) {
        Ok(key) => key,
        Err(response) => return Ok(response),
    };
    let crdt_actor = CrdtActor::deserialize_from_hex(&params.actor).ok_or_else(|| {
        DomainErrorKind::BadInputFormat(
            "bad actor, allocate a new actor by calling the /actor endpoint".to_string(),
        )
        .into_error()
    })?;

    // verify crdt actor
    endpoint
        .doc_repository
        .domain_engine()
        .system()
        .verify_session_user_id(&crdt_actor.user_id, session.clone())?;

    let actor_id: automerge::ActorId = crdt_actor.clone().into();

    // find the vertex that owns the CRDT
    let vertex_addr = endpoint
        .doc_repository
        .fetch_vertex_addr(endpoint.resource_def_id, key, session.clone())
        .await?
        .ok_or_else(|| DomainErrorKind::EntityNotFound.into_error())?;

    let doc_addr = DocAddr(vertex_addr, endpoint.crdt_prop_id);

    // load or create a broker for that document
    let broker_handle = match load_broker(
        doc_addr.clone(),
        actor_id.clone(),
        endpoint.broker_manager.clone(),
        endpoint.doc_repository.clone(),
        session.clone(),
    )
    .await?
    {
        Some(broker) => broker,
        None => return Err(DomainErrorKind::EntityNotFound.into_error().into()),
    };

    Ok(ws_upgrade.on_upgrade(|socket| async move {
        let session = SyncSession {
            actor: actor_id,
            doc_addr,
            socket,
            broker_handle,
            doc_repository: endpoint.doc_repository,
            session,
        };
        let _ = session.run().await;
    }))
}

///
/// POST /resource/{key_name}/:{key}/{crdt_prop}/actor
///
/// Allocates an actor ID for use with CRDT
///
async fn post_crdt_actor<State, Auth>(
    Extension(endpoint): Extension<CrdtBrokerEndpoint>,
    auth: Auth,
) -> Result<axum::Json<String>, HttpJsonError>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Into<Session> + 'static,
{
    let session: Session = auth.into();
    let user_id = endpoint
        .doc_repository
        .domain_engine()
        .system()
        .get_user_id(session)?;

    let actor = CrdtActor {
        actor_id: Uuid::new_v4(),
        user_id,
    };

    Ok(axum::Json(actor.serialize_to_hex()))
}

mod create {
    use super::*;

    pub async fn create_common<State, Auth>(
        mode: ProcessorMode,
        json_content_type: JsonContentType,
        Extension(endpoint): Extension<UnkeyedEndpoint>,
        auth: Auth,
        req: axum::http::Request<Body>,
    ) -> Result<Sequence<Value>, HttpJsonError>
    where
        State: Send + Sync + Clone + 'static,
        Auth: FromRequestParts<State> + Into<Session> + 'static,
    {
        let session = auth.into();
        let select = Select::EntityId;
        let req_message = match mode {
            ProcessorMode::Create => ReqMessage::Insert(0, select),
            ProcessorMode::Update => ReqMessage::Upsert(0, select),
            _ => return Err(HttpJsonError::status(StatusCode::INTERNAL_SERVER_ERROR)),
        };

        let transaction_msg_stream = match json_content_type {
            JsonContentType::Json => {
                let bytes = Bytes::from_request(req, &())
                    .await
                    .map_err(|err| HttpJsonError::Response(err.into_response()))?;

                let ontol_value = deserialize_ontol_value(
                    &endpoint
                        .engine
                        .ontology()
                        .new_serde_processor(endpoint.operator_addr, mode),
                    &mut serde_json::Deserializer::from_slice(&bytes),
                )?;

                futures_util::stream::iter([Ok(req_message), Ok(ReqMessage::Argument(ontol_value))])
                    .boxed()
            }
            JsonContentType::JsonLines => {
                let lines = JsonLines::<serde_json::Value>::from_request(req, &())
                    .await
                    .map_err(|err| HttpJsonError::Response(err.into_response()))?;
                let endpoint = endpoint.clone();

                async_stream::try_stream! {
                    yield req_message;

                    for await json_result in lines {
                        let json = json_result.map_err(|e| DomainErrorKind::BadInputFormat(format!("{e}")).into_error())?;
                        let ontol_value = deserialize_ontol_value(&endpoint
                            .engine
                            .ontology()
                            .new_serde_processor(endpoint.operator_addr, mode), json)?;

                        yield ReqMessage::Argument(ontol_value);
                    }
                }
                .boxed()
            }
        };

        let collected: Vec<Sequence<Value>> = endpoint
            .engine
            .transact(
                TransactionMode::ReadWriteAtomic,
                transaction_msg_stream,
                session.clone(),
            )
            .await?
            .accumulate_sequences()
            .try_collect()
            .await?;

        let sequence = collected
            .into_iter()
            .next()
            .expect("There should be one sequence");

        Ok(sequence)
    }
}

mod key {
    use ontol_runtime::ontology::Ontology;

    use super::*;

    pub fn deserialize_key(
        input: String,
        key_operator_addr: SerdeOperatorAddr,
        ontology: &Ontology,
    ) -> Result<Value, axum::response::Response> {
        match ontology
            .new_serde_processor(key_operator_addr, ProcessorMode::Read)
            .deserialize(StringDeserializer::<serde_json::Error>::new(input))
        {
            Ok(Attr::Unit(key)) => Ok(key),
            Ok(_) => {
                error!("key must be a unit attr");
                Err(StatusCode::INTERNAL_SERVER_ERROR.into_response())
            }
            Err(err) => Err((
                StatusCode::BAD_REQUEST,
                json_error(format!("invalid path parameter: {err}")),
            )
                .into_response()),
        }
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
