use ontol_runtime::DefId;
use smartstring::alias::String;
use thiserror::Error;

#[derive(Error, Clone, Debug)]
pub enum DomainError {
    #[error("No data store")]
    NoDataStore,
    #[error("No resolve path to data store")]
    NoResolvePathToDataStore,
    #[error("Not an entity")]
    NotAnEntity(DefId),
    #[error("Entity must be a struct")]
    EntityMustBeStruct,
    #[error("Id not found in structure")]
    IdNotFound,
    #[error("BUG: Invalid entity DefId")]
    InvalidEntityDefId,
    #[error("Type cannot be used for id generation")]
    TypeCannotBeUsedForIdGeneration,
    #[error("Unresolved foreign key: {0}")]
    UnresolvedForeignKey(String),
    #[error("Not implemented")]
    NotImplemented,
}

pub type DomainResult<T> = Result<T, DomainError>;
