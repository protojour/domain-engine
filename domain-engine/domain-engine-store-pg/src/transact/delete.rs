use domain_engine_core::{domain_error::DomainErrorKind, DomainResult};
use ontol_runtime::{value::Value, DefId};
use tracing::{debug, warn};

use crate::{ds_bad_req, ds_err, sql, transact::data::Data};

use super::TransactCtx;

impl<'a> TransactCtx<'a> {
    /// TODO: prepare statement(s)?
    pub async fn delete_vertex(&self, def_id: DefId, id: Value) -> DomainResult<bool> {
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(def_id).into_error()
        })?;

        let pg = self.pg_model.pg_domain_table(def_id.package_id(), def_id)?;
        let pg_id_field = pg.datatable.field(&entity.id_relationship_id)?;

        let Data::Sql(id_param) = self.data_from_value(id)? else {
            return Err(ds_bad_req("compound foreign key"));
        };

        let sql_delete = sql::Delete {
            from: pg.table_name(),
            where_: Some(sql::Expr::eq(
                sql::Expr::path1(pg_id_field.col_name.as_ref()),
                sql::Expr::param(0),
            )),
            returning: vec![sql::Expr::path1("_key")],
        };

        let sql = sql_delete.to_string();
        debug!("{sql}");

        let rows = self
            .txn
            .query(&sql, &[&id_param])
            .await
            .map_err(|e| ds_err(format!("unable to delete: {e:?}")))?;

        Ok(!rows.is_empty())
    }
}
