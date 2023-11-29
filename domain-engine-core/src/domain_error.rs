use ontol_runtime::{vm::VmError, DefId};
use smartstring::alias::String;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("Mapping procedure not found")]
    MappingProcedureNotFound,
    #[error("No data store")]
    NoDataStore,
    #[error("No resolve path to data store")]
    NoResolvePathToDataStore,
    #[error("Entity not found")]
    EntityNotFound,
    #[error("Not an entity")]
    NotAnEntity(DefId),
    #[error("Entity must be a struct")]
    EntityMustBeStruct,
    #[error("Inherent ID not found in structure")]
    InherentIdNotFound,
    #[error("BUG: Invalid entity DefId")]
    InvalidEntityDefId,
    #[error("Type cannot be used for id generation")]
    TypeCannotBeUsedForIdGeneration,
    #[error("Bad input: {0}")]
    BadInput(anyhow::Error),
    #[error("Unresolved foreign key: {0}")]
    UnresolvedForeignKey(String),
    #[error("Not implemented")]
    NotImplemented,
    #[error("Datastore error: {0}")]
    DataStore(anyhow::Error),
    #[error("Ontol data error")]
    OntolVm(#[from] VmError),
}

pub type DomainResult<T> = Result<T, DomainError>;
