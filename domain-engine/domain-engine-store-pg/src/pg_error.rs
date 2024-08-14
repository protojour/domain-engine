use domain_engine_core::{domain_error::DomainErrorKind, DomainError};
use ontol_runtime::{tuple::CardinalIdx, DefId, EdgeId, PackageId, RelId};
use tracing::{error, info, warn};

use crate::pg_model::PgRegKey;

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
}

impl From<PgModelError> for DomainError {
    fn from(value: PgModelError) -> Self {
        error!("pg model error: {value:?}");
        DomainErrorKind::DataStore(format!("{value}")).into_error()
    }
}

#[derive(displaydoc::Display, Debug)]
pub enum PgDataError {
    /// foreign key lookup
    ForeignKeyLookup(tokio_postgres::Error),
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
}

impl From<PgDataError> for DomainError {
    fn from(value: PgDataError) -> Self {
        warn!("pg data error: {value:?}");
        DomainErrorKind::DataStore(format!("{value}")).into_error()
    }
}
