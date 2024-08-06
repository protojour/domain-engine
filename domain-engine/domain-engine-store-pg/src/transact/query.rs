use domain_engine_core::{transact::DataOperation, DomainError, DomainResult};
use fnv::FnvHashMap;
use futures_util::Stream;
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget, Def},
    query::select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    value::Value,
    DefId, RelId,
};
use pin_utils::pin_mut;
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::{
    pg_model::{PgDataTable, PgDomain, PgType},
    sql::{self, IndexIdent},
    sql_value::{domain_codec_error, CodecResult, Layout, RowDecodeIterator, SqlVal},
};

use super::{data::RowValue, TransactCtx};

impl<'a> TransactCtx<'a> {
    pub async fn query(
        &self,
        entity_select: EntitySelect,
    ) -> DomainResult<impl Stream<Item = DomainResult<RowValue>> + '_> {
        debug!("query {entity_select:?}");

        let struct_select = match entity_select.source {
            StructOrUnionSelect::Struct(struct_select) => struct_select,
            StructOrUnionSelect::Union(..) => {
                return Err(DomainError::data_store_bad_request(
                    "union top-level query not supported (yet)",
                ))
            }
        };

        let def_id = struct_select.def_id;

        let def = self.ontology.def(def_id);
        let pg_domain = self.pg_model.pg_domain(def_id.package_id())?;
        let pg_datatable = self.pg_model.datatable(def_id.package_id(), def_id)?;

        let mut col_ident = IndexIdent(0);

        let mut row_layout: Vec<Layout> = vec![];

        let sql_select = sql::Select {
            expressions: self.sql_select_expressions(
                def_id,
                &struct_select,
                pg_domain,
                pg_datatable,
                &mut col_ident,
                &mut row_layout,
            )?,
            from: vec![sql::TableName(&pg_domain.schema_name, &pg_datatable.table_name).into()],
            where_: None,
            limit: Some(entity_select.limit),
        };

        let sql = sql_select.to_string();
        debug!("{sql}");
        debug!("{row_layout:?}");

        let row_stream = self
            .txn
            .query_raw(&sql, slice_iter(&[]))
            .await
            .map_err(|err| DomainError::data_store(format!("{err}")))?;

        Ok(async_stream::try_stream! {
            pin_mut!(row_stream);

            for await row_result in row_stream {
                let row = row_result.map_err(|_| DomainError::data_store("unable to fetch row"))?;
                let row_value = self.read_row_value(RowDecodeIterator::new(&row, &row_layout), def, &struct_select)?;
                yield row_value;
            }
        })
    }

    fn sql_select_expressions(
        &self,
        def_id: DefId,
        struct_select: &StructSelect,
        pg_domain: &'a PgDomain,
        pg_datatable: &'a PgDataTable,
        col_ident: &mut IndexIdent,
        layout: &mut Vec<Layout>,
    ) -> DomainResult<Vec<sql::Expr<'_>>> {
        let def = self.ontology.def(def_id);

        let mut sql_expressions = vec![sql::Expr::Column("_key")];
        layout.push(Layout::Scalar(PgType::BigInt));

        // select data properties
        for (rel_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    let data_field = pg_datatable.data_fields.get(&rel_id.tag()).unwrap();

                    sql_expressions.push(sql::Expr::AsIndex(
                        Box::new(sql::Expr::Column(&data_field.col_name)),
                        col_ident.incr(),
                    ));

                    let target_det_id = match rel.target {
                        DataRelationshipTarget::Unambiguous(def_id) => def_id,
                        DataRelationshipTarget::Union(_) => {
                            return Err(DomainError::data_store("union doesn't work here"));
                        }
                    };

                    layout.push(
                        PgType::from_def_id(target_det_id, self.ontology)?
                            .map(Layout::Scalar)
                            .unwrap_or(Layout::Ignore),
                    );
                }
                DataRelationshipKind::Edge(_) => {}
            }
        }

        // select edges
        for (rel_id, select) in &struct_select.properties {
            let Some(rel_info) = def.data_relationships.get(rel_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(proj) = &rel_info.kind else {
                continue;
            };

            let edge = self.ontology.find_edge(proj.id).unwrap();

            let pg_edge = pg_domain
                .edges
                .get(&proj.id.1)
                .ok_or_else(|| DomainError::data_store("edge not found"))?;
            let _pg_subj = pg_edge.cardinal(proj.subject)?;
            let _pg_obj = pg_edge.cardinal(proj.object)?;

            let obj_cardinal = &edge.cardinals[proj.object.0 as usize];
            let mut sub_layout: Vec<Layout> = vec![];

            let expressions = if obj_cardinal.target.len() == 1 {
                let target_def_id = obj_cardinal.target.iter().next().unwrap();
                let target_def = self.ontology.def(*target_def_id);

                match select {
                    Select::Leaf => {
                        let Some(target_entity) = target_def.entity() else {
                            return Err(DomainError::data_store_bad_request(
                                "cannot select id from non-entity",
                            ));
                        };

                        let pg_datatable = self
                            .pg_model
                            .datatable(target_def_id.package_id(), def_id)?;
                        let pg_datafield = pg_datatable.field(&target_entity.id_relationship_id)?;

                        sub_layout.push(Layout::Scalar(pg_datafield.pg_type));
                        vec![sql::Expr::Column(&pg_datafield.col_name)]
                    }
                    Select::Struct(struct_select) => {
                        let def_id = struct_select.def_id;
                        let pg_domain = self.pg_model.pg_domain(def_id.package_id())?;
                        let pg_datatable = self.pg_model.datatable(def_id.package_id(), def_id)?;

                        self.sql_select_expressions(
                            def_id,
                            struct_select,
                            pg_domain,
                            pg_datatable,
                            col_ident,
                            &mut sub_layout,
                        )?
                    }
                    _ => todo!(),
                }
            } else {
                panic!("union edge");
            };

            let sql_subselect = sql::Select {
                expressions: vec![sql::Expr::Row(expressions)],
                from: vec![sql::TableName(&pg_domain.schema_name, &pg_edge.table_name).into()],
                where_: None,
                limit: None,
            };

            sql_expressions.push(sql::Expr::AsIndex(
                Box::new(sql::Expr::Array(Box::new(sql::Expr::Select(Box::new(
                    sql_subselect,
                ))))),
                col_ident.incr(),
            ));

            layout.push(Layout::Array(Box::new(Layout::Record(sub_layout))));
        }

        Ok(sql_expressions)
    }

    fn read_row_value<'b>(
        &self,
        mut row: impl Iterator<Item = CodecResult<SqlVal<'b>>>,
        def: &Def,
        struct_select: &StructSelect,
    ) -> DomainResult<RowValue> {
        let mut attrs: FnvHashMap<RelId, Attr> =
            FnvHashMap::with_capacity_and_hasher(def.data_relationships.len(), Default::default());

        let key = next_column(&mut row)?.into_i64()?;

        // retrieve data properties
        for (rel_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    let sql_val = next_column(&mut row)?;

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

        // retrieve edges
        for rel_id in struct_select.properties.keys() {
            let Some(rel) = def.data_relationships.get(rel_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(_proj) = &rel.kind else {
                continue;
            };

            let array = next_column(&mut row)?.into_array()?;
            for element in array.elements() {
                let record = element.map_err(domain_codec_error)?.into_record()?;
                let field = record.fields().next().unwrap().unwrap();
                todo!("field: {field:?}");
            }
        }

        Ok(RowValue {
            value: Value::Struct(Box::new(attrs), def.id.into()),
            key,
            op: DataOperation::Inserted,
        })
    }
}

fn next_column<'b>(
    iter: &mut impl Iterator<Item = CodecResult<SqlVal<'b>>>,
) -> DomainResult<SqlVal<'b>> {
    match iter.next() {
        Some(result) => result.map_err(domain_codec_error),
        None => Err(DomainError::data_store("too few columns")),
    }
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a dyn ToSql> + 'a {
    s.iter().map(|s| *s as _)
}
