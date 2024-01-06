use ontol_runtime::{vm::VmError, DefId};
use smartstring::alias::String;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("mapping procedure not found")]
    MappingProcedureNotFound,
    #[error("no data store")]
    NoDataStore,
    #[error("no resolve path to data store")]
    NoResolvePathToDataStore,
    #[error("entity not found")]
    EntityNotFound,
    #[error("not an entity")]
    NotAnEntity(DefId),
    #[error("entity must be a struct")]
    EntityMustBeStruct,
    #[error("entity already exists")]
    EntityAlreadyExists,
    #[error("inherent ID not found in structure")]
    InherentIdNotFound,
    #[error("BUG: Invalid entity DefId")]
    InvalidEntityDefId,
    #[error("type cannot be used for id generation")]
    TypeCannotBeUsedForIdGeneration,
    #[error("bad input: {0}")]
    BadInput(anyhow::Error),
    #[error("unresolved foreign key: {0}")]
    UnresolvedForeignKey(String),
    #[error("not implemented")]
    NotImplemented,
    #[error("impure mapping where a pure mapping was expected")]
    ImpureMapping,
    #[error("datastore error: {0}")]
    DataStore(anyhow::Error),
    #[error("datastore bad request: {0}")]
    DataStoreBadRequest(anyhow::Error),
    #[error("ontol data error: {0}")]
    OntolVm(#[from] VmError),
}

pub type DomainResult<T> = Result<T, DomainError>;
