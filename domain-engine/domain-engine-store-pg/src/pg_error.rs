use arcstr::ArcStr;
use domain_engine_core::{domain_error::DomainErrorKind, DomainError};
use ontol_runtime::{tuple::CardinalIdx, DefId, DomainIndex, PropId};
use tracing::{error, info, warn};

use crate::pg_model::{EdgeId, PgDomainTableType, PgRegKey};

pub fn ds_err(s: impl Into<String>) -> DomainError {
    DomainError::data_store(s)
}

pub fn ds_bad_req(s: impl Into<String>) -> DomainError {
    DomainError::data_store_bad_request(s)
}

pub fn map_row_error(
    pg_err: tokio_postgres::Error,
    domain_table_type: PgDomainTableType,
) -> DomainErrorKind {
    if let Some(db_error) = pg_err.as_db_error() {
        if db_error
            .message()
            .starts_with("duplicate key value violates unique constraint")
        {
            match domain_table_type {
                PgDomainTableType::Vertex => DomainErrorKind::EntityAlreadyExists,
                PgDomainTableType::Edge => DomainErrorKind::EdgeAlreadyExists,
            }
        } else {
            info!("row fetch DB error: {db_error:?}");
            DomainErrorKind::DataStore("could not fetch row".to_string())
        }
    } else {
        error!("row fetch PG error: {pg_err:?}");
        DomainErrorKind::DataStore("could not fetch row".to_string())
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
    /// tentative foreign key
    TentativeForeignKey(tokio_postgres::Error),
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
    PrepareStatement(ArcStr, tokio_postgres::Error),
    /// condition error: {0}
    Condition(&'static str),
    /// order error: {0}
    Order(&'static str),
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
    DomainNotFound(DomainIndex),
    /// not found in registry: {0}
    NotFoundInRegistry(PgRegKey),
    /// collection not found for {0:?} => {1:?}
    CollectionNotFound(DomainIndex, DefId),
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
    /// no address in cardinal
    NoAddressInCardinal,
}

impl From<PgModelError> for DomainError {
    fn from(value: PgModelError) -> Self {
        error!("pg model error: {value:?}");
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
