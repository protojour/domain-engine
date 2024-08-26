use domain_engine_core::DomainResult;
use itertools::Itertools;
use ontol_runtime::{ontology::domain::FieldPath, value::Value, DefId};
use tracing::{debug, error, warn};

use crate::{
    pg_error::PgError,
    pg_model::{PgDomainTable, PgProperty},
    sql,
};

use super::TransactCtx;

impl<'a> TransactCtx<'a> {
    pub fn select_order(
        &self,
        def_id: DefId,
        order_symbols: &[Value],
    ) -> DomainResult<sql::OrderBy<'a>> {
        if order_symbols.is_empty() {
            return Ok(Default::default());
        }

        debug!("select order for {def_id:?}");

        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.package_id(), def_id)?;

        let Some(info) = self.ontology.extended_entity_info(def_id) else {
            warn!("no extended entity info; cannot order");
            return Ok(Default::default());
        };

        let entity_order_tuple = order_symbols
            .iter()
            .map(|sym| info.order_table.get(&sym.type_def_id()).unwrap())
            .collect_vec();

        let mut order_by = sql::OrderBy::default();

        for item in entity_order_tuple {
            let direction = item.direction;

            debug!("{direction:?}");

            for field_path in item.tuple.as_ref() {
                if let Some(expr) = self.order_expr_for_field_path(pg, field_path)? {
                    order_by.expressions.push(sql::OrderByExpr(expr, direction));
                }
            }
        }

        Ok(order_by)
    }

    fn order_expr_for_field_path(
        &self,
        pg: PgDomainTable<'a>,
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
                    error!("order by non-existent property {prop_id:?}");
                    return Ok(None);
                }
            }
        }

        Err(PgError::Order("nothing to order by").into())
    }
}
