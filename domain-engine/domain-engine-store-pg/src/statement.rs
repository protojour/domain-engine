use std::{fmt::Display, ops::Deref};

use domain_engine_core::DomainResult;

use crate::pg_error::PgError;

pub struct PreparedStatement {
    statement: tokio_postgres::Statement,
    src: String,
}

impl Deref for PreparedStatement {
    type Target = tokio_postgres::Statement;

    fn deref(&self) -> &Self::Target {
        &self.statement
    }
}

impl Display for PreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.src)
    }
}

pub trait Prepare {
    async fn prepare(self, client: &tokio_postgres::Client) -> DomainResult<PreparedStatement>;
}

impl Prepare for String {
    async fn prepare(self, client: &tokio_postgres::Client) -> DomainResult<PreparedStatement> {
        let statement = client
            .prepare(&self)
            .await
            .map_err(|e| PgError::PrepareStatement(self.clone(), e))?;

        Ok(PreparedStatement {
            statement,
            src: self,
        })
    }
}
