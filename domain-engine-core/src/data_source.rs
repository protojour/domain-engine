use ontol_runtime::{env::Env, value::Value, DefId};

use crate::DomainError;

#[async_trait::async_trait]
pub trait DataSourceAPI {
    async fn store_entity(&self, env: &Env, entity: Value) -> Result<Value, DomainError>;

    async fn query(&self, env: &Env, def_id: DefId) -> Result<Vec<Value>, DomainError>;
}
