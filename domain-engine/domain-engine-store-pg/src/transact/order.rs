use domain_engine_core::DomainResult;
use ontol_runtime::{
    ontology::domain::{DataRelationshipTarget, Def, EntityOrder, FieldPath},
    DefId,
};
use tracing::{debug, error};

use crate::{
    pg_error::PgError,
    pg_model::{PgDomainTable, PgProperty, PgRepr},
    sql,
};

use super::TransactCtx;

impl<'a> TransactCtx<'a> {
    pub fn select_order(
        &self,
        def_id: DefId,
        entity_order_tuple: &[EntityOrder],
    ) -> DomainResult<sql::OrderBy<'a>> {
        if entity_order_tuple.is_empty() {
            return Ok(Default::default());
        }

        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.domain_index(), def_id)?;

        let mut order_by = sql::OrderBy::default();

        let def = self.ontology.def(def_id);

        for item in entity_order_tuple {
            let direction = item.direction;

            debug!("{direction:?}");

            for field_path in item.tuple.as_ref() {
                if let Some(expr) = self.order_expr_for_field_path(pg, def, field_path)? {
                    order_by.expressions.push(sql::OrderByExpr(expr, direction));
                }
            }
        }

        Ok(order_by)
    }

    fn order_expr_for_field_path(
        &self,
        pg: PgDomainTable<'a>,
        def: &Def,
        path: &FieldPath,
    ) -> DomainResult<Option<sql::Expr<'a>>> {
        if path.0.len() > 1 {
            return Err(PgError::Order("field path too long").into());
        }

        if let Some(prop_id) = (&path.0).into_iter().next() {
            debug!("order by property {prop_id:?}");
            match pg.table.properties.get(&prop_id.tag()) {
                Some(PgProperty::Column(pg_column)) => {
                    return Ok(Some(sql::Expr::path1(pg_column.col_name.as_ref())));
                }
                Some(PgProperty::Abstract(_)) => {
                    error!("order by abstract property {prop_id:?}");
                    return Ok(None);
                }
                None => {
                    let Some(rel_info) = def.data_relationships.get(prop_id) else {
                        return Err(PgError::Order("order by rel_info not in def").into());
                    };

                    let target_def_id = match rel_info.target {
                        DataRelationshipTarget::Unambiguous(def_id) => def_id,
                        DataRelationshipTarget::Union(_) => {
                            return Err(PgError::Order("order by union target").into());
                        }
                    };

                    match PgRepr::classify_property(rel_info, target_def_id, self.ontology) {
                        PgRepr::CreatedAtColumn => {
                            return Ok(Some(sql::Expr::path1("_created")));
                        }
                        PgRepr::UpdatedAtColumn => {
                            return Ok(Some(sql::Expr::path1("_updated")));
                        }
                        PgRepr::Unit
                        | PgRepr::Scalar(..)
                        | PgRepr::Abstract
                        | PgRepr::NotSupported(_) => {
                            error!("order by non-existent property {prop_id:?}");
                            return Ok(None);
                        }
                    }
                }
            }
        }

        Err(PgError::Order("nothing to order by").into())
    }
}
