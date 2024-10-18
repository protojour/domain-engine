use domain_engine_core::DomainResult;
use ontol_runtime::{
    ontology::domain::{DataRelationshipTarget, Def, FieldPath, VertexOrder},
    DefId, OntolDefTag, PropId,
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
        vertex_order_tuple: &[VertexOrder],
    ) -> DomainResult<sql::OrderBy<'a>> {
        if vertex_order_tuple.is_empty() {
            return Ok(Default::default());
        }

        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.domain_index(), def_id)?;

        let mut order_by = sql::OrderBy::default();

        let def = self.ontology_defs.def(def_id);

        for item in vertex_order_tuple {
            let direction = item.direction;

            debug!(?direction, "order item");

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
            debug!(?prop_id, "order by property");
            match pg.table.properties.get(&prop_id.tag()) {
                Some(PgProperty::Column(pg_column)) => {
                    return Ok(Some(sql::Expr::path1(pg_column.col_name.as_ref())));
                }
                Some(PgProperty::Abstract(_)) => {
                    error!(?prop_id, "order by abstract property");
                    return Ok(None);
                }
                None => match self.order_fallback(def, prop_id)? {
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
                        error!(?prop_id, "order by non-existent property");
                        return Ok(None);
                    }
                },
            }
        }

        Err(PgError::Order("nothing to order by").into())
    }

    fn order_fallback(&self, def: &Def, prop_id: &PropId) -> Result<PgRepr, PgError> {
        match def.data_relationships.get(prop_id) {
            Some(rel_info) => {
                let target_def_id = match rel_info.target {
                    DataRelationshipTarget::Unambiguous(def_id) => def_id,
                    DataRelationshipTarget::Union(_) => {
                        return Err(PgError::Order("order by union target"));
                    }
                };

                Ok(PgRepr::classify_property(
                    rel_info,
                    target_def_id,
                    self.ontology_defs,
                ))
            }
            None => {
                if prop_id.0 == OntolDefTag::CreateTime.def_id() {
                    Ok(PgRepr::CreatedAtColumn)
                } else if prop_id.0 == OntolDefTag::UpdateTime.def_id() {
                    Ok(PgRepr::UpdatedAtColumn)
                } else {
                    Err(PgError::Order("order by rel_info not in def"))
                }
            }
        }
    }
}
