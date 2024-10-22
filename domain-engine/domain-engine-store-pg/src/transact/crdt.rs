use domain_engine_core::DomainResult;
use ontol_runtime::crdt::Automerge;
use tracing::debug;

use crate::{pg_error::PgError, sql};

use super::{data::ParentProp, mut_ctx::PgMutCtx, TransactCtx};

impl<'a> TransactCtx<'a> {
    pub async fn insert_initial_automerge_crdt(
        &self,
        parent: ParentProp,
        automerge: Automerge,
        _mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<()> {
        let domain_index = parent.prop_id.0.domain_index();
        let pg_domain = self.pg_model.pg_domain(domain_index)?;

        let parent_prop_key = self
            .pg_model
            .datatable(domain_index, parent.prop_id.0)?
            .abstract_property(&parent.prop_id)?;

        let insert = sql::Insert {
            with: None,
            into: sql::TableName(&pg_domain.schema_name, "crdt"),
            as_: None,
            column_names: vec!["_fprop", "_fkey", "chunk_type", "chunk"],
            values: vec![
                sql::Expr::param(0),
                sql::Expr::param(1),
                sql::Expr::LiteralStr("snapshot"),
                sql::Expr::param(2),
            ],
            on_conflict: None,
            returning: vec![],
        };

        let sql = insert.to_string();

        debug!("{sql}");

        self.client()
            .query(
                &sql,
                &[&parent_prop_key, &parent.key, &automerge.save().as_slice()],
            )
            .await
            .map_err(PgError::InsertQuery)?;

        Ok(())
    }
}
