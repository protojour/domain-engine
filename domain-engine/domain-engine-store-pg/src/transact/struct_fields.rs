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
    sql_record::SqlRecordIterator,
    sql_value::Layout,
};

use super::TransactCtx;

impl<'a> TransactCtx<'a> {
    /// Select the columns in the order of the data relationships in the Def.
    pub fn select_inherent_struct_fields(
        &self,
        def: &Def,
        pg_datatable: &'a PgTable,
        output: &mut Vec<sql::Expr<'a>>,
        table_alias: Option<sql::Alias>,
    ) -> DomainResult<()> {
        for (rel_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    let data_field = pg_datatable.data_fields.get(&rel_id.tag()).unwrap();

                    if let Some(table_alias) = table_alias {
                        output.push(sql::Expr::path2(table_alias, data_field.col_name.as_ref()));
                    } else {
                        output.push(sql::Expr::path1(data_field.col_name.as_ref()));
                    }
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
        record_iter: &mut impl SqlRecordIterator<'b>,
        attrs: &mut FnvHashMap<RelId, Attr>,
    ) -> DomainResult<()> {
        for (rel_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    // TODO: Might be able to cache this,
                    // but is it worth it?
                    let target_def_id = match rel.target {
                        DataRelationshipTarget::Unambiguous(def_id) => def_id,
                        DataRelationshipTarget::Union(_) => {
                            return Err(ds_err("union doesn't work here"));
                        }
                    };

                    let sql_val = record_iter.next_field(
                        &PgType::from_def_id(target_def_id, self.ontology)?
                            .map(Layout::Scalar)
                            .unwrap_or(Layout::Ignore),
                    )?;

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
