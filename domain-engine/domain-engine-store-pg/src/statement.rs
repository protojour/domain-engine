use std::{fmt::Display, ops::Deref};

use arcstr::ArcStr;
use domain_engine_core::DomainResult;

use crate::pg_error::PgError;

#[derive(Clone)]
pub struct PreparedStatement {
    prepared: tokio_postgres::Statement,
    src: ArcStr,
}

impl PreparedStatement {
    pub fn src(&self) -> &ArcStr {
        &self.src
    }
}

impl Deref for PreparedStatement {
    type Target = tokio_postgres::Statement;

    fn deref(&self) -> &Self::Target {
        &self.prepared
    }
}

impl Display for PreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.src)
    }
}

pub trait ToArcStr {
    fn to_arcstr(&self) -> ArcStr;
}

impl<T: Display> ToArcStr for T {
    fn to_arcstr(&self) -> ArcStr {
        arcstr::format!("{self}")
    }
}

pub trait Prepare {
    async fn prepare(self, client: &tokio_postgres::Client) -> DomainResult<PreparedStatement>;
}

impl Prepare for ArcStr {
    async fn prepare(self, client: &tokio_postgres::Client) -> DomainResult<PreparedStatement> {
        let statement = client
            .prepare(&self)
            .await
            .map_err(|e| PgError::PrepareStatement(self.clone(), e))?;

        Ok(PreparedStatement {
            prepared: statement,
            src: self,
        })
    }
}
