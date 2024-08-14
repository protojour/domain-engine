use domain_engine_core::{domain_error::DomainErrorKind, DomainError};
use ontol_runtime::{tuple::CardinalIdx, DefId, EdgeId, PackageId, RelId};
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
    MissingField(RelId),
    /// union field
    UnionField,
    /// edge parameters missing
    EdgeParametersMissing,
}

impl From<PgError> for DomainError {
    fn from(value: PgError) -> Self {
        warn!("pg data error: {value:?}");
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
    DataRelationshipNotFound(RelId),
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
}

impl From<PgInputError> for DomainError {
    fn from(value: PgInputError) -> Self {
        info!("pg input error: {value:?}");
        DomainErrorKind::DataStoreBadRequest(format!("{value}")).into_error()
    }
}

/// public facing postgres errors
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
    FieldNotFound(Box<str>, RelId),
    /// edge cardinal {0} not found
    EdgeCardinalNotFound(CardinalIdx),
    /// unhandled repr
    UnhandledRepr,
    /// duplicate edge cardinal
    DuplicateEdgeCardinal(usize),
    /// non-existent field
    NonExistentField(RelId),
    /// invalid transaction state
    InvalidTransactionState,
}

impl From<PgModelError> for DomainError {
    fn from(value: PgModelError) -> Self {
        error!("pg model error: {value:?}");
        DomainErrorKind::DataStore(format!("{value}")).into_error()
    }
}
