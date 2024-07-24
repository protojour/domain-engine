use axum::{body::Body, response::IntoResponse};
use domain_engine_core::DomainError;
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

    match error {
        DomainError::Unauthorized => {
            (StatusCode::FORBIDDEN, json_error("operation not permitted")).into_response()
        }
        DomainError::MappingProcedureNotFound => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("mapping procedure not found"),
        )
            .into_response(),
        DomainError::NoDataStore => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("no data store found"),
        )
            .into_response(),
        DomainError::NoResolvePathToDataStore => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("no resolve path to data store"),
        )
            .into_response(),
        DomainError::EntityNotFound => {
            (StatusCode::NOT_FOUND, json_error("entity not found")).into_response()
        }
        DomainError::NotAnEntity(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("not an entity"),
        )
            .into_response(),
        DomainError::EntityMustBeStruct => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("entity must be struct"),
        )
            .into_response(),
        DomainError::EntityAlreadyExists => {
            (StatusCode::CONFLICT, json_error("entity already exists")).into_response()
        }
        DomainError::InherentIdNotFound => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("inherent id not found"),
        )
            .into_response(),
        DomainError::InvalidEntityDefId => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("invalid entity definition"),
        )
            .into_response(),
        DomainError::TypeCannotBeUsedForIdGeneration => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("type cannot be used for id generation"),
        )
            .into_response(),
        DomainError::BadInput(err) => (
            StatusCode::BAD_REQUEST,
            json_error(format!("bad input: {err:?}")),
        )
            .into_response(),
        DomainError::UnresolvedForeignKey(key) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error(format!("unresolved foreign key: {key}")),
        )
            .into_response(),
        DomainError::NotImplemented => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error("not implemented"),
        )
            .into_response(),
        DomainError::ImpureMapping => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("impure mapping"),
        )
            .into_response(),
        DomainError::DataStore(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json_error(format!("data store: {err:?}")),
        )
            .into_response(),
        DomainError::DataStoreBadRequest(err) => {
            (StatusCode::BAD_REQUEST, json_error(format!("{err:?}"))).into_response()
        }
        DomainError::OntolVm(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, json_error("ONTOL error")).into_response()
        }
        DomainError::SerializationFailed => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("serialization failed"),
        )
            .into_response(),
        DomainError::DeserializationFailed => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json_error("deserialization failed"),
        )
            .into_response(),
    }
}
