use std::sync::Arc;

use domain_engine_core::{domain_error::DomainErrorKind, DomainError};
use ontol_runtime::{tuple::CardinalIdx, DefId, EdgeId, PackageId, PropId};
use tracing::{error, info, warn};

use crate::pg_model::PgRegKey;

pub fn ds_err(s: impl Into<String>) -> DomainError {
    DomainError::data_store(s)
}

pub fn ds_bad_req(s: impl Into<String>) -> DomainError {
    DomainError::data_store_bad_request(s)
}

pub fn map_row_error(pg_err: tokio_postgres::Error) -> DomainError {
    if let Some(db_error) = pg_err.as_db_error() {
        if db_error
            .message()
            .starts_with("duplicate key value violates unique constraint")
        {
            DomainErrorKind::EntityAlreadyExists.into_error()
        } else {
            info!("row fetch error: {db_error:?}");
            ds_err("could not fetch row")
        }
    } else {
        error!("row fetch error: {pg_err:?}");
        ds_err("could not fetch row")
    }
}

/// Errors resulting from postgres interaction in some way
#[derive(displaydoc::Display, Debug)]
pub enum PgError {
    /// database connection problem
    DbConnectionAcquire(anyhow::Error),
    /// transaction problem
    BeginTransaction(tokio_postgres::Error),
    /// transaction problem
    CommitTransaction(tokio_postgres::Error),
    /// foreign key lookup
    ForeignKeyLookup(tokio_postgres::Error),
    /// invalid dynamic data type
    InvalidDynamicDataType(PgRegKey),
    /// select
    SelectQuery(tokio_postgres::Error),
    /// select data
    SelectRow(tokio_postgres::Error),
    /// insert
    InsertQuery(tokio_postgres::Error),
    /// insert edge fetch
    InsertEdgeFetch(tokio_postgres::Error),
    /// insertion problem
    InsertRowStreamNotClosed(tokio_postgres::Error),
    /// nothing inserted
    NothingInserted,
    /// insertion problem
    InsertIncorrectAffectCount,
    /// insertion problem
    InsertNoRowsAffected,
    /// update
    UpdateQuery(tokio_postgres::Error),
    /// update key-fetch
    UpdateQueryKeyFetch(tokio_postgres::Error),
    /// update data
    UpdateRow(tokio_postgres::Error),
    /// nothing updated
    NothingUpdated,
    /// edge insertion
    EdgeInsertion(tokio_postgres::Error),
    /// edge update
    EdgeUpdate(tokio_postgres::Error),
    /// edge deletion
    EdgeDeletion(tokio_postgres::Error),
    /// invalid query frame
    InvalidQueryFrame,
    /// invalid type
    InvalidType(&'static str),
    /// invalid type
    ExpectedType(&'static str),
    /// field missing in datastore
    MissingField(PropId),
    /// union field
    UnionField,
    /// edge parameters missing
    EdgeParametersMissing,
    /// statement preparation
    PrepareStatement(Arc<String>, tokio_postgres::Error),
}

impl From<PgError> for DomainError {
    fn from(value: PgError) -> Self {
        warn!("pg error: {value:?}");
        DomainErrorKind::DataStore(format!("{value}")).into_error()
    }
}

/// errors resulting from not being able to make sense of input
#[derive(displaydoc::Display, Debug)]
pub enum PgInputError {
    /// id must be a scalar
    IdMustBeScalar,
    /// vertex must be a struct
    VertexMustBeStruct,
    /// invalid data relationship
    DataRelationshipNotFound(PropId),
    /// missing value without generator
    MissingValueWithoutGenerator,
    /// compound foreign key
    CompoundForeignKey,
    /// not an entity
    NotAnEntity,
    /// union variant not found
    UnionVariantNotFound,
    /// matrix in matrix
    MatrixInMatrix,
    /// union top-level query not supported
    UnionTopLevelQuery,
    /// bad cursor
    BadCursor(anyhow::Error),
    /// non-unit sub-value
    MultivaluedSubValue(PropId),
}

impl From<PgInputError> for DomainError {
    fn from(value: PgInputError) -> Self {
        info!("pg input error: {value:?}");
        DomainErrorKind::DataStoreBadRequest(format!("{value}")).into_error()
    }
}

/// public facing postgres errors about model invariant violations
#[derive(displaydoc::Display, Debug)]
pub enum PgModelError {
    /// domain not found for {0:?}
    DomainNotFound(PackageId),
    /// not found in registry: {0}
    NotFoundInRegistry(PgRegKey),
    /// collection not found for {0:?} => {1:?}
    CollectionNotFound(PackageId, DefId),
    /// edge not found for {0:?}
    EdgeNotFound(EdgeId),
    /// field not found for {1} in {0}
    PropertyNotFound(Box<str>, PropId),
    /// edge cardinal {0} not found
    EdgeCardinalNotFound(CardinalIdx),
    /// unhandled repr
    UnhandledRepr,
    /// duplicate edge cardinal
    DuplicateEdgeCardinal(usize),
    /// non-existent field
    NonExistentField(PropId),
    /// invalid transaction state
    InvalidTransactionState,
    /// invalid unique cardinal
    InvalidUniqueCardinal,
    /// data type not supported
    DataTypeNotSupported(&'static str),
}

impl From<PgModelError> for DomainError {
    fn from(value: PgModelError) -> Self {
        panic!("pg model error: {value:?}");
        DomainErrorKind::DataStore(format!("{value}")).into_error()
    }
}

/// public facing postgres errors about model invariant violations
#[derive(displaydoc::Display, Debug)]
pub enum PgMigrationError {
    /// cardinal change detected
    CardinalChangeDetected,
    /// change index type not implemented
    ChangeIndexTypeNotImplemented,
    /// ambiguous edge cardinal
    AmbiguousEdgeCardinal,
    /// union target
    UnionTarget(PropId),
    /// ambiguous ID
    AmbiguousId(DefId),
    /// incompatible property `{1}`
    IncompatibleProperty(PropId, Box<str>),
}

impl From<PgMigrationError> for DomainError {
    fn from(value: PgMigrationError) -> Self {
        error!("pg model error: {value:?}");
        DomainErrorKind::DataStore(format!("{value}")).into_error()
    }
}

impl std::error::Error for PgMigrationError {}
