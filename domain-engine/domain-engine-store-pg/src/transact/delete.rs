use domain_engine_core::{domain_error::DomainErrorKind, DomainResult};
use ontol_runtime::{value::Value, DefId};
use tracing::{debug, warn};

use crate::{
    address::make_vertex_addr,
    pg_error::{PgError, PgInputError},
    pg_model::PgDataKey,
    sql,
    transact::data::Data,
};

use super::{mut_ctx::PgMutCtx, TransactCtx};

impl<'a> TransactCtx<'a> {
    /// TODO: prepare statement(s)?
    pub async fn delete_vertex(
        &self,
        def_id: DefId,
        id: Value,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<bool> {
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(def_id).into_error()
        })?;

        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.domain_index(), def_id)?;
        let pg_id_field = pg.table.column(&entity.id_prop)?;

        let Data::Sql(id_param) = self.data_from_value(id)? else {
            return Err(PgInputError::CompoundForeignKey.into());
        };

        let sql_delete = sql::Delete {
            from: pg.table_name(),
            where_: Some(sql::Expr::eq(
                sql::Expr::path1(pg_id_field.col_name),
                sql::Expr::param(0),
            )),
            returning: vec![sql::Expr::path1("_key")],
        };

        let sql = sql_delete.to_string();
        debug!("{sql}");

        let rows = self
            .client()
            .query(&sql, &[&id_param])
            .await
            .map_err(PgError::EdgeDeletion)?;

        if rows.is_empty() {
            Ok(false)
        } else {
            for row in rows {
                let data_key: PgDataKey = row.get(0);

                let vertex_addr = make_vertex_addr(pg.table.key, data_key);
                mut_ctx.write_stats.mark_deleted(vertex_addr);
            }

            Ok(true)
        }
    }
}
