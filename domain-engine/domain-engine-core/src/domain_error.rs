use std::fmt::Display;

use ontol_runtime::{vm::VmError, DefId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DomainError {
    kind: DomainErrorKind,
    stack: Vec<String>,
}

impl DomainError {
    pub fn data_store(str: impl Into<String>) -> Self {
        DomainErrorKind::DataStore(str.into()).into_error()
    }

    pub fn data_store_from_anyhow(error: anyhow::Error) -> Self {
        let mut stack = vec![];

        for error in error.chain() {
            stack.push(format!("{error}"));
        }

        let msg = if let Some(root_cause) = stack.pop() {
            root_cause
        } else {
            "chainless error".to_string()
        };

        Self {
            kind: DomainErrorKind::DataStore(msg),
            stack,
        }
    }

    pub fn data_store_bad_request(str: impl Into<String>) -> Self {
        DomainErrorKind::DataStoreBadRequest(str.into()).into_error()
    }

    pub fn protocol(str: impl Into<String>) -> Self {
        DomainErrorKind::Protocol(str.into()).into_error()
    }

    pub fn kind(&self) -> &DomainErrorKind {
        &self.kind
    }
}

impl Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;

        if !self.stack.is_empty() {
            writeln!(f)?;
            writeln!(f, "caused by:")?;

            for (idx, cause) in self.stack.iter().rev().enumerate() {
                writeln!(f, "    {idx}: {cause}")?;
            }
        }

        Ok(())
    }
}

impl std::error::Error for DomainError {}

pub trait DomainErrorContext {
    fn with_context<S: Into<String>>(self, context: impl FnOnce() -> S) -> Self;
}

impl<T> DomainErrorContext for DomainResult<T> {
    fn with_context<S: Into<String>>(self, context: impl FnOnce() -> S) -> Self {
        self.map_err(move |mut domain_error| {
            domain_error.stack.push(context().into());
            domain_error
        })
    }
}

impl From<DomainErrorKind> for DomainError {
    fn from(value: DomainErrorKind) -> Self {
        DomainError {
            kind: value,
            stack: vec![],
        }
    }
}

#[derive(displaydoc::Display, Debug, Serialize, Deserialize)]
pub enum DomainErrorKind {
    /// not authenticated
    Unauthenticated,
    /// unauthorized
    Unauthorized,
    /// auth provisioning problem: {0}
    AuthProvision(String),
    /// mapping procedure not found
    MappingProcedureNotFound,
    /// no data store
    NoDataStore,
    /// unknown data store: {0}
    UnknownDataStore(String),
    /// no resolve path to data store
    NoResolvePathToDataStore,
    /// entity not found
    EntityNotFound,
    /// edge not found
    EdgeNotFound,
    /// not an entity
    NotAnEntity(DefId),
    /// entity must be a struct
    EntityMustBeStruct,
    /// entity already exists
    EntityAlreadyExists,
    /// inherent ID not found in structure
    InherentIdNotFound,
    /// BUG: Invalid entity DefId
    InvalidEntityDefId,
    /// type cannot be used for id generation
    TypeCannotBeUsedForIdGeneration,
    /// bad input format: {0}
    BadInputFormat(String),
    /// bad input data: {0}
    BadInputData(String),
    /// unresolved foreign key: {0}
    UnresolvedForeignKey(String),
    /// not implemented
    NotImplemented,
    /// impure mapping where a pure mapping was expected
    ImpureMapping,
    /// datastore error: {0}
    DataStore(String),
    /// datastore bad request: {0}
    DataStoreBadRequest(String),
    /// ontol data error: {0}
    OntolVm(VmError),
    /// serialization failed
    SerializationFailed,
    /// deserialization failed
    DeserializationFailed,
    /// protocol error: {0}
    Protocol(String),
}

impl DomainErrorKind {
    pub fn into_error(self) -> DomainError {
        DomainError {
            kind: self,
            stack: vec![],
        }
    }
}

pub type DomainResult<T> = Result<T, DomainError>;
