use domain_engine_core::DomainResult;
use fnv::FnvHashMap;
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget, Def},
    RelId,
};

use crate::{
    ds_err,
    pg_model::{PgTable, PgType},
    sql,
    sql_value::{CodecResult, Layout, SqlVal},
};

use super::TransactCtx;

impl<'a> TransactCtx<'a> {
    /// Select the columns in the order of the data relationships in the Def.
    pub fn select_inherent_struct_fields(
        &self,
        def: &Def,
        pg_datatable: &'a PgTable,
        table_alias: Option<sql::Alias>,
        sql_expressions: &mut Vec<sql::Expr<'a>>,
        layout: &mut Vec<Layout>,
    ) -> DomainResult<()> {
        for (rel_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    let data_field = pg_datatable.data_fields.get(&rel_id.tag()).unwrap();

                    let target_det_id = match rel.target {
                        DataRelationshipTarget::Unambiguous(def_id) => def_id,
                        DataRelationshipTarget::Union(_) => {
                            return Err(ds_err("union doesn't work here"));
                        }
                    };

                    sql_expressions.push(match table_alias {
                        Some(alias) => sql::Expr::path2(alias, data_field.col_name.as_ref()),
                        None => sql::Expr::path1(data_field.col_name.as_ref()),
                    });
                    layout.push(
                        PgType::from_def_id(target_det_id, self.ontology)?
                            .map(Layout::Scalar)
                            .unwrap_or(Layout::Ignore),
                    );
                }
                DataRelationshipKind::Edge(_) => {}
            }
        }

        Ok(())
    }

    /// Read the columns in the order of the data relationships in the Def.
    pub fn read_inherent_struct_fields<'b>(
        &self,
        def: &Def,
        row: &mut impl Iterator<Item = CodecResult<SqlVal<'b>>>,
        attrs: &mut FnvHashMap<RelId, Attr>,
    ) -> DomainResult<()> {
        for (rel_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    let sql_val = SqlVal::next_column(row)?;

                    if let Some(sql_val) = sql_val.null_filter() {
                        match rel.target {
                            DataRelationshipTarget::Unambiguous(def_id) => {
                                attrs.insert(
                                    *rel_id,
                                    Attr::Unit(self.deserialize_sql(def_id, sql_val)?),
                                );
                            }
                            DataRelationshipTarget::Union(_) => {}
                        }
                    }
                }
                DataRelationshipKind::Edge(_) => {}
            }
        }

        Ok(())
    }
}
