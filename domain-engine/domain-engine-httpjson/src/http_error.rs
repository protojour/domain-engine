use axum::{body::Body, response::IntoResponse};
use domain_engine_core::{domain_error::DomainErrorKind, DomainError};
use http::StatusCode;
use serde::Serialize;
use tracing::info;

#[derive(Serialize)]
pub struct ErrorJson {
    pub message: String,
}

pub fn json_error(message: impl Into<String>) -> axum::Json<ErrorJson> {
    axum::Json(ErrorJson {
        message: message.into(),
    })
}

pub fn domain_error_to_response(error: DomainError) -> http::Response<Body> {
    info!("{error:?}");

    match error.kind() {
        DomainErrorKind::Unauthenticated => {
            (StatusCode::UNAUTHORIZED, json_error("not authenticated")).into_response()
        }
        DomainErrorKind::Unauthorized => {
            (StatusCode::FORBIDDEN, json_error("operation not permitted")).into_response()
        }
        DomainErrorKind::AuthProvision(_) => (
            StatusCode::FORBIDDEN,
            json_error("auth provisioning problem"),
        )
            .into_response(),
        DomainErrorKind::MappingProcedureNotFound => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("mapping procedure not found"),
        )
            .into_response(),
        DomainErrorKind::NoDataStore => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("no data store found"),
        )
            .into_response(),
        DomainErrorKind::UnknownDataStore(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("unknown data store"),
        )
            .into_response(),
        DomainErrorKind::NoResolvePathToDataStore => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("no resolve path to data store"),
        )
            .into_response(),
        DomainErrorKind::EntityNotFound => {
            (StatusCode::NOT_FOUND, json_error("entity not found")).into_response()
        }
        DomainErrorKind::NotAnEntity(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("not an entity"),
        )
            .into_response(),
        DomainErrorKind::EntityMustBeStruct => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("entity must be struct"),
        )
            .into_response(),
        DomainErrorKind::EntityAlreadyExists => {
            (StatusCode::CONFLICT, json_error("entity already exists")).into_response()
        }
        DomainErrorKind::InherentIdNotFound => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("inherent id not found"),
        )
            .into_response(),
        DomainErrorKind::InvalidEntityDefId => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("invalid entity definition"),
        )
            .into_response(),
        DomainErrorKind::TypeCannotBeUsedForIdGeneration => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("type cannot be used for id generation"),
        )
            .into_response(),
        DomainErrorKind::BadInputFormat(err) => (
            StatusCode::BAD_REQUEST,
            json_error(format!("bad input format: {err:?}")),
        )
            .into_response(),
        DomainErrorKind::BadInputData(err) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error(format!("bad input: {err:?}")),
        )
            .into_response(),
        DomainErrorKind::UnresolvedForeignKey(key) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error(format!("unresolved foreign key: {key}")),
        )
            .into_response(),
        DomainErrorKind::NotImplemented => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("not implemented"),
        )
            .into_response(),
        DomainErrorKind::ImpureMapping => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("impure mapping"),
        )
            .into_response(),
        DomainErrorKind::DataStore(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error(format!("data store: {err:?}")),
        )
            .into_response(),
        DomainErrorKind::DataStoreBadRequest(err) => {
            (StatusCode::BAD_REQUEST, json_error(format!("{err:?}"))).into_response()
        }
        DomainErrorKind::OntolVm(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, json_error("ONTOL error")).into_response()
        }
        DomainErrorKind::SerializationFailed => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("serialization failed"),
        )
            .into_response(),
        DomainErrorKind::DeserializationFailed => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("deserialization failed"),
        )
            .into_response(),
        DomainErrorKind::Protocol(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("server protocol error"),
        )
            .into_response(),
    }
}
