use domain_engine_core::DomainResult;
use fnv::FnvHashMap;
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipInfo, DataRelationshipTarget, Def},
    value::Value,
    DefId, OntolDefTag, PropId,
};
use tracing::warn;

use crate::{
    pg_error::PgModelError,
    pg_model::{PgDataKey, PgDomainTable, PgRepr, PgTable, PgType},
    sql,
    sql_record::SqlRecordIterator,
    sql_value::{Layout, PgTimestamp, SqlOutput, SqlScalar},
};

use super::{query_select::VertexSelect, TransactCtx};

pub enum AbstractKind<'a> {
    VertexUnion(&'a [DefId]),
    Scalar {
        pg_type: PgType,
        ontol_def_tag: OntolDefTag,
        target_def_id: DefId,
    },
}

pub struct StandardAttrs {
    pub data_key: PgDataKey,
    pub created_at: PgTimestamp,
    pub updated_at: PgTimestamp,
}

impl<'a> TransactCtx<'a> {
    pub fn initial_standard_data_fields(&self, pg: PgDomainTable<'a>) -> [sql::Expr<'a>; 4] {
        [
            // Always present: the def key of the vertex.
            // This is known ahead of time.
            // It will be used later to parse unions.
            sql::Expr::LiteralInt(pg.table.key),
            // Always present: the data key of the vertex
            sql::Expr::path1("_key"),
            sql::Expr::path1("_created"),
            sql::Expr::path1("_updated"),
        ]
    }

    /// Select the columns in the order of the data relationships in the Def.
    pub fn select_inherent_vertex_fields(
        &self,
        pg_datatable: &'a PgTable,
        vertex_select: &VertexSelect,
        output: &mut Vec<sql::Expr<'a>>,
        table_alias: Option<sql::Alias>,
    ) {
        for prop_id in &vertex_select.inherent_set {
            if let Some(pg_column) = pg_datatable.find_column(prop_id) {
                if pg_column.standard {
                    continue;
                }

                if let Some(table_alias) = table_alias {
                    output.push(sql::Expr::path2(table_alias, pg_column.col_name));
                } else {
                    output.push(sql::Expr::path1(pg_column.col_name));
                }
            }
        }
    }

    /// Read the columns in the order of the VertexSelect.
    pub fn read_inherent_vertex_fields<'b>(
        &self,
        record_iter: &mut impl SqlRecordIterator<'b>,
        def: &Def,
        vertex_select: &VertexSelect,
        attrs: &mut FnvHashMap<PropId, Attr>,
        standard_fields: Option<&StandardAttrs>,
    ) -> DomainResult<()> {
        for prop_id in &vertex_select.inherent_set {
            if let Some(rel_info) = def.data_relationships.get(prop_id) {
                if let Some(value) = self.read_field(rel_info, record_iter, standard_fields)? {
                    attrs.insert(*prop_id, Attr::Unit(value));
                }
            } else if prop_id.0 == OntolDefTag::CreateTime.def_id() {
                let standard_fields = standard_fields.unwrap();
                attrs.insert(
                    *prop_id,
                    Attr::Unit(Value::ChronoDateTime(
                        standard_fields.created_at,
                        OntolDefTag::DateTime.def_id().into(),
                    )),
                );
            } else if prop_id.0 == OntolDefTag::UpdateTime.def_id() {
                let standard_fields = standard_fields.unwrap();
                attrs.insert(
                    *prop_id,
                    Attr::Unit(Value::ChronoDateTime(
                        standard_fields.updated_at,
                        OntolDefTag::DateTime.def_id().into(),
                    )),
                );
            }
        }

        Ok(())
    }

    pub fn read_field<'b>(
        &self,
        rel_info: &DataRelationshipInfo,
        record_iter: &mut impl SqlRecordIterator<'b>,
        standard_fields: Option<&StandardAttrs>,
    ) -> DomainResult<Option<Value>> {
        let target_def_id = rel_info.target.def_id();

        // TODO: Might be able to cache this in some way,
        // e.g. a Vec<PgType> for the property columns in each PgTable.
        // But is the increased memory usage worth it?
        let pg_repr = PgRepr::classify_property(rel_info, target_def_id, self.ontology);

        match pg_repr {
            PgRepr::Scalar(pg_type, _) => {
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
            PgRepr::CreatedAtColumn => {
                let Some(standard_fields) = standard_fields else {
                    return Ok(None);
                };
                Ok(Some(self.deserialize_sql(
                    target_def_id,
                    SqlOutput::Scalar(SqlScalar::Timestamp(standard_fields.created_at)),
                )?))
            }
            PgRepr::UpdatedAtColumn => {
                let Some(standard_fields) = standard_fields else {
                    return Ok(None);
                };
                Ok(Some(self.deserialize_sql(
                    target_def_id,
                    SqlOutput::Scalar(SqlScalar::Timestamp(standard_fields.updated_at)),
                )?))
            }
            PgRepr::Abstract => Ok(None),
            PgRepr::NotSupported(msg) => Err(PgModelError::DataTypeNotSupported(msg).into()),
        }
    }

    pub fn abstract_kind(&self, target: &'a DataRelationshipTarget) -> AbstractKind<'a> {
        match target {
            DataRelationshipTarget::Unambiguous(target_def_id) => {
                let target_def = self.ontology.def(*target_def_id);
                if let Some(PgRepr::Scalar(pg_type, ontol_def_tag)) =
                    PgRepr::classify_opt_def_repr(target_def.repr(), self.ontology)
                {
                    AbstractKind::Scalar {
                        pg_type,
                        ontol_def_tag,
                        target_def_id: *target_def_id,
                    }
                } else {
                    AbstractKind::VertexUnion(std::slice::from_ref(target_def_id))
                }
            }
            DataRelationshipTarget::Union(union_def_id) => {
                AbstractKind::VertexUnion(self.ontology.union_variants(*union_def_id))
            }
        }
    }
}
