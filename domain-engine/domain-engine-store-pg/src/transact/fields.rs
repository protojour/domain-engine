use domain_engine_core::DomainResult;
use fnv::FnvHashMap;
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipInfo, DataRelationshipKind, Def},
    value::Value,
    PropId,
};
use tracing::warn;

use crate::{
    pg_error::PgModelError,
    pg_model::{PgDomainTable, PgRepr, PgTable},
    sql,
    sql_record::SqlRecordIterator,
    sql_value::Layout,
};

use super::TransactCtx;

#[derive(Default)]
pub struct SelectStats {
    pub edge_count: usize,
}

impl<'a> TransactCtx<'a> {
    pub fn initial_standard_data_fields(&self, pg: PgDomainTable<'a>) -> [sql::Expr<'a>; 2] {
        [
            // Always present: the def key of the vertex.
            // This is known ahead of time.
            // It will be used later to parse unions.
            sql::Expr::LiteralInt(pg.table.key),
            // Always present: the data key of the vertex
            sql::Expr::path1("_key"),
        ]
    }

    /// Select the columns in the order of the data relationships in the Def.
    pub fn select_inherent_struct_fields(
        &self,
        def: &Def,
        pg_datatable: &'a PgTable,
        output: &mut Vec<sql::Expr<'a>>,
        table_alias: Option<sql::Alias>,
    ) -> DomainResult<SelectStats> {
        let mut stats = SelectStats { edge_count: 0 };

        for (prop_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    if let Some(pg_column) = pg_datatable.find_column(prop_id) {
                        if let Some(table_alias) = table_alias {
                            output.push(sql::Expr::path2(table_alias, pg_column.col_name.as_ref()));
                        } else {
                            output.push(sql::Expr::path1(pg_column.col_name.as_ref()));
                        }
                    }
                }
                DataRelationshipKind::Edge(_) => {
                    stats.edge_count += 1;
                }
            }
        }

        Ok(stats)
    }

    /// Read the columns in the order of the data relationships in the Def.
    pub fn read_inherent_struct_fields<'b>(
        &self,
        def: &Def,
        record_iter: &mut impl SqlRecordIterator<'b>,
        attrs: &mut FnvHashMap<PropId, Attr>,
    ) -> DomainResult<()> {
        for (prop_id, rel_info) in &def.data_relationships {
            match &rel_info.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    if let Some(value) = self.read_field(rel_info, record_iter)? {
                        attrs.insert(*prop_id, Attr::Unit(value));
                    }
                }
                DataRelationshipKind::Edge(_) => {}
            }
        }

        Ok(())
    }

    pub fn read_field<'b>(
        &self,
        rel_info: &DataRelationshipInfo,
        record_iter: &mut impl SqlRecordIterator<'b>,
    ) -> DomainResult<Option<Value>> {
        let target_def_id = rel_info.target.def_id();

        // TODO: Might be able to cache this in some way,
        // e.g. a Vec<PgType> for the property columns in each PgTable.
        // But is the increased memory usage worth it?
        let pg_repr = PgRepr::classify(target_def_id, self.ontology);

        match pg_repr {
            PgRepr::Column(pg_type) => {
                let sql_val = record_iter.next_field(&Layout::Scalar(pg_type))?;

                Ok(if let Some(sql_val) = sql_val.null_filter() {
                    Some(self.deserialize_sql(target_def_id, sql_val)?)
                } else {
                    None
                })
            }
            PgRepr::Unit => match self.ontology.try_produce_constant(target_def_id) {
                Some(value) => Ok(Some(value)),
                None => {
                    warn!("can't produce a representation of constant unit {target_def_id:?}");
                    Ok(None)
                }
            },
            PgRepr::Abstract => Ok(None),
            PgRepr::NotSupported(msg) => Err(PgModelError::DataTypeNotSupported(msg).into()),
        }
    }
}
