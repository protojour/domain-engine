use domain_engine_core::{transact::DataOperation, DomainError, DomainResult};
use ontol_runtime::{query::select::Select, value::Value};

use crate::pg_model::InDomain;

use super::TransactCtx;

pub enum InsertMode {
    Insert,
    Upsert,
}

impl<'m, 't> TransactCtx<'m, 't> {
    pub async fn insert(
        &self,
        value: InDomain<Value>,
        mode: InsertMode,
        select: &Select,
    ) -> DomainResult<(Value, DataOperation)> {
        let data_table = self
            .pg_model
            .get_datatable(value.pkg_id, value.type_def_id())
            .ok_or_else(|| DomainError::data_store("datatable not found"))?;

        Err(DomainError::data_store(
            "insert not implemented for Postgres",
        ))
    }
}
