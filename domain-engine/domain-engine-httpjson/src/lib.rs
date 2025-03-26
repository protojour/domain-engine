#![forbid(unsafe_code)]

use std::{convert::Infallible, sync::Arc};

use axum::{
    Extension,
    body::Body,
    extract::{FromRequest, FromRequestParts, OriginalUri},
    handler::Handler,
    http::StatusCode,
    response::IntoResponse,
    routing::{MethodFilter, MethodRouter, get, post},
};
use axum_extra::extract::JsonLines;
use bytes::{BufMut, Bytes, BytesMut};
use content_type::JsonContentType;
use crdt::{
    ActorExt, DocAddr,
    broker::{BrokerManagerHandle, load_broker},
    doc_repository::DocRepository,
    sync_session::SyncSession,
};
use domain_engine_core::{
    CrdtActor, DomainEngine, DomainError, FindEntitySelect, SelectMode, Session,
    domain_error::DomainErrorKind,
    domain_select::domain_select_no_edges,
    transact::{AccumulateSequences, ReqMessage, TransactionMode},
};
use futures_util::{TryStreamExt, stream::StreamExt};
use http::{HeaderValue, Uri, header};
use http_error::{HttpJsonError, json_error};
use ontol_runtime::{
    DefId, DomainIndex, MapKey, PropId,
    attr::{Attr, AttrMatrix, AttrRef},
    interface::{
        DomainInterface,
        http_json::{HttpDefResource, HttpMapGetResource, HttpResource},
        serde::{
            operator::SerdeOperatorAddr,
            processor::{ProcessorMode, SerdeProcessor},
        },
    },
    ontology::ontol::TextConstant,
    query::{
        condition::{Clause, CondTerm, SetOperator},
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect},
    },
    sequence::Sequence,
    value::Value,
};
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, value::StringDeserializer},
};
use tracing::{Instrument, debug, error, info, info_span};
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
struct UnkeyedMapGetEndpoint {
    engine: Arc<DomainEngine>,
    output_operator_addr: SerdeOperatorAddr,
    map_key: MapKey,
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

        for resource in &httpjson.resources {
            match resource {
                HttpResource::Def(resource) => {
                    domain_router =
                        self.add_def_endpoint::<State, Auth>(resource, domain_router, &engine);
                }
                HttpResource::MapGet(resource) => {
                    let route_name =
                        format!("/{resource}", resource = &engine.ontology()[resource.name]);

                    let method_router = self.add_mapped_get_method_route::<State, Auth>(
                        resource,
                        MethodRouter::default(),
                        &engine,
                    );

                    debug!("add route `{route_name}`");
                    domain_router = domain_router.route(&route_name, method_router);
                }
            }
        }

        Some(domain_router)
    }

    fn add_def_endpoint<State, Auth>(
        &self,
        resource: &HttpDefResource,
        mut domain_router: axum::Router<State>,
        engine: &Arc<DomainEngine>,
    ) -> axum::Router<State>
    where
        State: Send + Sync + Clone + 'static,
        Auth: FromRequestParts<State> + Send + Into<Session> + 'static,
    {
        let ontology = engine.ontology();

        let mut method_router: MethodRouter<State, Infallible> = MethodRouter::default();

        if let Some(map_get_resource) = &resource.get {
            method_router = self.add_mapped_get_method_route::<State, Auth>(
                map_get_resource,
                method_router,
                engine,
            );
        }

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

            let route_name = format!("/{resource_name}/{key_name}/{{{key_name}}}");

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
                    format!("/{resource_name}/{key_name}/{{{key_name}}}/{prop_name}");
                debug!("add route `{sync_route_name}`");
                domain_router = domain_router.route(
                    &sync_route_name,
                    get(get_crdt_broker_ws::<State, Auth>).layer(Extension(CrdtBrokerEndpoint {
                        doc_repository: DocRepository::from(engine.clone()),
                        broker_manager: self.broker_manager.clone(),
                        resource_def_id: resource.def_id,
                        key_operator_addr: keyed.key_operator_addr,
                        crdt_prop_id: *crdt_prop_id,
                    })),
                );

                let actor_route_name =
                    format!("/{resource_name}/{key_name}/{{{key_name}}}/{prop_name}/actor");
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

        domain_router
    }

    fn add_mapped_get_method_route<State, Auth>(
        &self,
        resource: &HttpMapGetResource,
        method_router: MethodRouter<State, Infallible>,
        engine: &Arc<DomainEngine>,
    ) -> MethodRouter<State, Infallible>
    where
        State: Send + Sync + Clone + 'static,
        Auth: FromRequestParts<State> + Send + Into<Session> + 'static,
    {
        method_router.on(
            MethodFilter::GET,
            get_resource_mapped::<State, Auth>.layer(Extension(UnkeyedMapGetEndpoint {
                engine: engine.clone(),
                output_operator_addr: resource.output_operator_addr,
                map_key: resource.map_key,
            })),
        )
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

///
/// GET /resource
///
async fn get_resource_mapped<State, Auth>(
    Extension(endpoint): Extension<UnkeyedMapGetEndpoint>,
    auth: Auth,
) -> Result<axum::response::Response, HttpJsonError>
where
    State: Send + Sync + Clone + 'static,
    Auth: FromRequestParts<State> + Into<Session> + 'static,
{
    let engine = endpoint.engine;
    let ontology = engine.ontology();

    let session: Session = auth.into();
    let struct_select =
        match domain_select_no_edges(endpoint.map_key.output.def_id, ontology.as_ref()) {
            Select::Struct(struct_select) => struct_select,
            _ => {
                error!("must be struct select");
                return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
            }
        };
    let entity_select = EntitySelect {
        source: StructOrUnionSelect::Struct(struct_select),
        filter: Filter::default_for_domain(),
        limit: Some(engine.system().default_query_limit()),
        after_cursor: None,
        include_total_len: false,
    };

    struct SelectProvider(Option<EntitySelect>);

    impl FindEntitySelect for SelectProvider {
        fn find_select(
            &mut self,
            _match_var: ontol_runtime::var::Var,
            _condition: &ontol_runtime::query::condition::Condition,
        ) -> domain_engine_core::SelectMode {
            self.0.take().map(SelectMode::Dynamic).unwrap()
        }
    }

    let mut select_provider = SelectProvider(Some(entity_select));

    let output = engine
        .exec_map(
            endpoint.map_key,
            Value::unit(),
            &mut select_provider,
            session,
        )
        .await?;

    let mut json_bytes = BytesMut::with_capacity(128).writer();

    let output_attr = match output {
        Value::Sequence(sequence, _) => {
            let matrix = AttrMatrix {
                columns: [sequence].into_iter().collect(),
            };
            Attr::Matrix(matrix)
        }
        other => Attr::Unit(other),
    };

    ontology
        .new_serde_processor(endpoint.output_operator_addr, ProcessorMode::Read)
        .serialize_attr(
            output_attr.as_ref(),
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
    let domain_engine = endpoint.doc_repository.domain_engine();
    let ontology = domain_engine.ontology();
    let resource_def_id = endpoint.resource_def_id;

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
    if crdt_actor.user_id != domain_engine.system().get_user_id(session.clone())? {
        info!("CrdtActor user_id does not match the session user id");
        return Err(DomainErrorKind::Unauthorized.into_error().into());
    }

    let actor_id: automerge::ActorId = crdt_actor.clone().into();

    // find the vertex that owns the CRDT
    let vertex_addr = endpoint
        .doc_repository
        .fetch_vertex_addr(endpoint.resource_def_id, key, session.clone())
        .instrument(info_span!("sync", %actor_id))
        .await?
        .ok_or_else(|| DomainErrorKind::EntityNotFound.into_error())?;

    let doc_addr = DocAddr(vertex_addr, endpoint.crdt_prop_id);

    // load or create a broker for that document
    let broker_handle = match load_broker(
        endpoint.resource_def_id,
        doc_addr.clone(),
        actor_id.clone(),
        endpoint.broker_manager.clone(),
        endpoint.doc_repository.clone(),
        session.clone(),
    )
    .instrument(info_span!("sync", %doc_addr, %actor_id))
    .await?
    {
        Some(broker) => broker,
        None => return Err(DomainErrorKind::EntityNotFound.into_error().into()),
    };

    Ok(ws_upgrade.on_upgrade(move |socket| async move {
        let session = SyncSession {
            resource_def_id,
            actor: actor_id.clone(),
            doc_addr: doc_addr.clone(),
            socket,
            broker_handle,
            doc_repository: endpoint.doc_repository,
            session,
        };
        let _ = session
            .run()
            .instrument(info_span!("sync", %doc_addr, %actor_id))
            .await;
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
            });
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
