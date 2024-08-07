use domain_engine_core::{transact::DataOperation, DomainResult};
use fnv::FnvHashMap;
use futures_util::Stream;
use ontol_runtime::{
    attr::{Attr, AttrMatrix},
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget, Def},
    property::ValueCardinality,
    query::select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    sequence::SubSequence,
    value::Value,
    DefId, RelId,
};
use pin_utils::pin_mut;
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::{
    ds_bad_req, ds_err,
    pg_model::{PgDataTable, PgDomainTable, PgEdgeCardinal, PgType},
    sql::{self, Alias},
    sql_value::{domain_codec_error, CodecResult, Layout, RowDecodeIterator, SqlVal},
};

use super::{data::RowValue, TransactCtx};

pub enum QueryFrame {
    Row(RowValue),
    Footer(Option<Box<SubSequence>>),
}

#[derive(Default)]
struct QueryBuildCtx {
    alias: Alias,
}

impl<'a> TransactCtx<'a> {
    pub async fn query(
        &self,
        entity_select: EntitySelect,
    ) -> DomainResult<impl Stream<Item = DomainResult<QueryFrame>> + '_> {
        debug!("query {entity_select:?}");

        let include_total_len = entity_select.include_total_len;
        let limit = entity_select.limit;

        let struct_select = match entity_select.source {
            StructOrUnionSelect::Struct(struct_select) => struct_select,
            StructOrUnionSelect::Union(..) => {
                return Err(ds_bad_req("union top-level query not supported (yet)"))
            }
        };

        let def_id = struct_select.def_id;

        let def = self.ontology.def(def_id);
        let pg = self.pg_model.pg_domain_table(def_id.package_id(), def_id)?;

        let mut row_layout: Vec<Layout> = vec![];
        let mut ctx = QueryBuildCtx::default();
        let root_alias = ctx.alias;

        let mut sql_select = sql::Select {
            expressions: vec![],
            from: vec![pg.table_name().as_(root_alias)],
            where_: None,
            limit: Some(limit + 1),
        };

        if include_total_len {
            sql_select.expressions.push(sql::Expr::CountStarOver);
            row_layout.push(Layout::Scalar(PgType::BigInt));
        }

        sql_select.expressions.extend(self.sql_select_expressions(
            def_id,
            &struct_select,
            pg,
            root_alias,
            &mut row_layout,
            &mut ctx,
        )?);

        let sql = sql_select.to_string();
        debug!("{sql}");

        let row_stream = self
            .txn
            .query_raw(&sql, slice_iter(&[]))
            .await
            .map_err(|err| ds_err(format!("{err}")))?;

        Ok(async_stream::try_stream! {
            pin_mut!(row_stream);

            let mut total_len: Option<usize> = None;
            let mut observed_count = 0;

            for await row_result in row_stream {
                let row = row_result.map_err(|_| ds_err("unable to fetch row"))?;
                observed_count += 1;
                let mut row_iter = RowDecodeIterator::new(&row, &row_layout);

                if include_total_len {
                    // must read this for every row if include_total_len was true
                    total_len = Some(row_iter.next().unwrap().map_err(domain_codec_error)?.into_i64()? as usize);
                }

                let row_value = self.read_struct_row_value(row_iter, def, &struct_select)?;

                if observed_count <= limit {
                    yield QueryFrame::Row(row_value);
                } else {
                    break;
                }
            }

            yield QueryFrame::Footer(
                Some(Box::new(SubSequence {
                    end_cursor: Some(Box::new([])),
                    // The actual SQL limit is input limit + 1
                    has_next: observed_count > limit,
                    total_len
                }))
            );
        })
    }

    fn sql_select_expressions(
        &self,
        def_id: DefId,
        struct_select: &StructSelect,
        pg: PgDomainTable<'a>,
        cur_alias: sql::Alias,
        layout: &mut Vec<Layout>,
        ctx: &mut QueryBuildCtx,
    ) -> DomainResult<Vec<sql::Expr<'_>>> {
        let def = self.ontology.def(def_id);

        let mut sql_expressions = vec![
            // Always present: the def key of the vertex.
            // This is known ahead of time.
            // It will be used later to parse unions.
            sql::Expr::LiteralInt(pg.datatable.key),
            // Always present: the data key of the vertex
            sql::Expr::path2(cur_alias, "_key"),
        ];
        layout.extend([
            Layout::Scalar(PgType::Integer),
            Layout::Scalar(PgType::BigInt),
        ]);

        // select data properties
        self.select_inherent_struct_fields(
            def,
            pg.datatable,
            Some(cur_alias),
            &mut sql_expressions,
            layout,
        )?;

        // select edges
        for (rel_id, select) in &struct_select.properties {
            let Some(rel_info) = def.data_relationships.get(rel_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(proj) = &rel_info.kind else {
                continue;
            };

            let edge = self.ontology.find_edge(proj.id).unwrap();

            let pg_edge = pg
                .domain
                .edges
                .get(&proj.id.1)
                .ok_or_else(|| ds_err("edge not found"))?;
            let pg_edge_subj = pg_edge.cardinal(proj.subject)?;
            let pg_edge_obj = pg_edge.cardinal(proj.object)?;

            let obj_cardinal = &edge.cardinals[proj.object.0 as usize];
            let mut sub_layout: Vec<Layout> = vec![];
            let edge_alias = ctx.alias.incr();
            let sub_alias = ctx.alias.incr();

            if obj_cardinal.target.len() == 1 {
                let target_def_id = *obj_cardinal.target.iter().next().unwrap();
                let target_def = self.ontology.def(target_def_id);

                let pg_sub = self
                    .pg_model
                    .pg_domain_table(target_def_id.package_id(), target_def_id)?;

                let expressions = match select {
                    Select::Leaf => {
                        let Some(target_entity) = target_def.entity() else {
                            return Err(ds_bad_req("cannot select id from non-entity"));
                        };

                        let pg_id = pg_sub.datatable.field(&target_entity.id_relationship_id)?;

                        sub_layout.push(Layout::Scalar(pg_id.pg_type));
                        vec![sql::Expr::path2(sub_alias, pg_id.col_name.as_ref())]
                    }
                    Select::Struct(struct_select) => {
                        let def_id = struct_select.def_id;

                        self.sql_select_expressions(
                            def_id,
                            struct_select,
                            self.pg_model
                                .pg_domain_table(target_def_id.package_id(), target_def_id)?,
                            sub_alias,
                            &mut sub_layout,
                            ctx,
                        )?
                    }
                    _ => todo!(),
                };

                let mut sql_subselect = sql::Select {
                    expressions: vec![sql::Expr::Row(expressions)],
                    from: vec![sql::Join {
                        first: pg_sub.table_name().as_(sub_alias),
                        second: sql::TableName(&pg.domain.schema_name, &pg_edge.table_name)
                            .as_(edge_alias),
                        on: edge_join_condition(
                            edge_alias,
                            pg_edge_obj,
                            sub_alias,
                            pg_sub.datatable,
                        ),
                    }
                    .into()],
                    where_: Some(edge_join_condition(
                        edge_alias,
                        pg_edge_subj,
                        cur_alias,
                        pg.datatable,
                    )),
                    limit: None,
                };

                match rel_info.cardinality.1 {
                    ValueCardinality::Unit => {
                        sql_subselect.limit = Some(1);
                        sql_expressions.push(sql_subselect.into());
                        layout.push(Layout::Record(sub_layout));
                    }
                    ValueCardinality::IndexSet | ValueCardinality::List => {
                        sql_expressions.push(sql::Expr::array(sql_subselect.into()));
                        layout.push(Layout::Array(Box::new(Layout::Record(sub_layout))));
                    }
                }
            } else {
                panic!("union edge");
            };
        }

        Ok(sql_expressions)
    }

    fn read_struct_row_value<'b>(
        &self,
        mut row: impl Iterator<Item = CodecResult<SqlVal<'b>>>,
        def: &Def,
        struct_select: &StructSelect,
    ) -> DomainResult<RowValue> {
        let mut attrs: FnvHashMap<RelId, Attr> =
            FnvHashMap::with_capacity_and_hasher(def.data_relationships.len(), Default::default());

        let _def_key = SqlVal::next_column(&mut row)?.into_i32()?;
        let data_key = SqlVal::next_column(&mut row)?.into_i64()?;

        // retrieve data properties
        self.read_inherent_struct_fields(def, &mut row, &mut attrs)?;

        // retrieve edges
        for (rel_id, select) in &struct_select.properties {
            let Some(rel_info) = def.data_relationships.get(rel_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(_proj) = &rel_info.kind else {
                continue;
            };

            let target_def_id = match rel_info.target {
                DataRelationshipTarget::Unambiguous(def_id) => def_id,
                DataRelationshipTarget::Union(_) => todo!(),
            };

            debug!("target def id: {target_def_id:?}");

            let sql_val = SqlVal::next_column(&mut row)?;

            match rel_info.cardinality.1 {
                ValueCardinality::Unit => {
                    if let Some(sql_val) = sql_val.null_filter() {
                        let value = self.read_record(sql_val, target_def_id, select)?;
                        attrs.insert(*rel_id, Attr::Unit(value));
                    }
                }
                ValueCardinality::IndexSet | ValueCardinality::List => {
                    let mut matrix = AttrMatrix::default();
                    matrix.columns.push(Default::default());

                    for result in sql_val.into_array()?.elements() {
                        let sql_val = result.map_err(domain_codec_error)?;
                        let value = self.read_record(sql_val, target_def_id, select)?;
                        matrix.columns[0].push(value);
                    }
                    attrs.insert(*rel_id, Attr::Matrix(matrix));
                }
            }
        }

        Ok(RowValue {
            value: Value::Struct(Box::new(attrs), def.id.into()),
            data_key,
            op: DataOperation::Inserted,
        })
    }

    fn read_record(
        &self,
        sql_val: SqlVal,
        target_def_id: DefId,
        select: &Select,
    ) -> DomainResult<Value> {
        let sql_record = sql_val.into_record()?;
        match select {
            Select::Leaf => {
                let sql_field = sql_record
                    .fields()
                    .next()
                    .ok_or_else(|| ds_err("no fields"))?
                    .unwrap();
                let entity = self.ontology.def(target_def_id).entity().unwrap();

                let value = self.deserialize_sql(entity.id_value_def_id, sql_field)?;
                Ok(value)
            }
            Select::Struct(struct_select) => {
                let def = self.ontology.def(target_def_id);
                let row_value =
                    self.read_struct_row_value(sql_record.fields(), def, struct_select)?;

                Ok(row_value.value)
            }
            _ => todo!("unhandled select"),
        }
    }
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a dyn ToSql> + 'a {
    s.iter().map(|s| *s as _)
}

fn edge_join_condition<'d>(
    edge_alias: Alias,
    cardinal: &'d PgEdgeCardinal,
    data_alias: Alias,
    data: &'d PgDataTable,
) -> sql::Expr<'d> {
    sql::Expr::And(vec![
        sql::Expr::eq(
            sql::Expr::path2(edge_alias, cardinal.def_col_name.as_ref()),
            sql::Expr::LiteralInt(data.key),
        ),
        sql::Expr::eq(
            sql::Expr::path2(edge_alias, cardinal.key_col_name.as_ref()),
            sql::Expr::path2(data_alias, "_key"),
        ),
    ])
}
