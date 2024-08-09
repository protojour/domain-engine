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
use serde::{Deserialize, Serialize};
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::{
    ds_bad_req, ds_err,
    pg_model::{PgDomainTable, PgEdgeCardinal, PgEdgeCardinalKind, PgTable, PgType},
    sql::{self, Alias},
    sql_value::{domain_codec_error, CodecResult, Layout, RowDecodeIterator, SqlVal},
};

use super::{data::RowValue, TransactCtx};

pub enum QueryFrame {
    Row(RowValue),
    Footer(Option<Box<SubSequence>>),
}

#[derive(Default)]
struct QueryBuildCtx<'d> {
    alias: Alias,
    with_def_aliases: FnvHashMap<DefId, Alias>,
    with_queries: Vec<sql::WithQuery<'d>>,
}

#[derive(Serialize, Deserialize, Debug)]
enum Cursor {
    /// The only pagination supported for now, is offset-pagination.
    /// Future improvements include value pagination (WHERE (set of values) > (cursor values))
    /// for a specific ordering.
    Offset(usize),
}

impl<'a> TransactCtx<'a> {
    pub async fn query(
        &self,
        entity_select: EntitySelect,
    ) -> DomainResult<impl Stream<Item = DomainResult<QueryFrame>> + '_> {
        debug!("query {entity_select:?}");

        let include_total_len = entity_select.include_total_len;
        let limit = entity_select.limit;
        let after_cursor: Option<Cursor> = match entity_select.after_cursor {
            Some(bytes) => {
                Some(bincode::deserialize(&bytes).map_err(|_| ds_bad_req("bad cursor"))?)
            }
            None => None,
        };

        debug!("after cursor: {after_cursor:?}");

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

        let sql_select = {
            let mut expressions = vec![];

            if include_total_len {
                expressions.push(sql::Expr::CountStarOver);
                row_layout.push(Layout::Scalar(PgType::BigInt));
            }

            let (from, _alias, tail_expressions) =
                self.sql_select_expressions(def_id, &struct_select, pg, &mut row_layout, &mut ctx)?;
            expressions.extend(tail_expressions);

            sql::Select {
                with: if !ctx.with_queries.is_empty() {
                    Some(std::mem::take(&mut ctx.with_queries).into())
                } else {
                    None
                },
                expressions,
                from: vec![from],
                where_: None,
                limit: Some(limit + 1),
                offset: match &after_cursor {
                    Some(Cursor::Offset(offset)) => Some(*offset),
                    _ => None,
                },
            }
        };

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
            let mut observed_values = 0;
            let mut observed_rows = 0;

            for await row_result in row_stream {
                let row = row_result.map_err(|_| ds_err("unable to fetch row"))?;
                let mut row_iter = RowDecodeIterator::new(&row, &row_layout);

                if include_total_len {
                    // must read this for every row if include_total_len was true
                    total_len = Some(row_iter.next().unwrap().map_err(domain_codec_error)?.into_i64()? as usize);
                }

                if observed_rows < limit {
                    let row_value = self.read_struct_row_value(row_iter, def, &struct_select)?;
                    yield QueryFrame::Row(row_value);

                    observed_values += 1;
                    observed_rows += 1;
                } else {
                    observed_rows += 1;
                    break;
                }
            }

            let end_cursor = if observed_values > 0 {
                let original_offset = match &after_cursor {
                    Some(Cursor::Offset(offset)) => *offset,
                    None => 0,
                };
                Some(bincode::serialize(&Cursor::Offset(original_offset + observed_values)).unwrap().into_boxed_slice())
            } else {
                None
            };

            yield QueryFrame::Footer(
                Some(Box::new(SubSequence {
                    end_cursor,
                    // The actual SQL limit is (input limit) + 1
                    has_next: observed_rows > observed_values,
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
        layout: &mut Vec<Layout>,
        ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<(sql::FromItem<'_>, sql::Alias, Vec<sql::Expr<'_>>)> {
        let def = self.ontology.def(def_id);

        // select data properties
        let data_alias = self.select_datatable_fields_as_alias(def, pg, layout, ctx)?;
        let mut sql_expressions = vec![sql::Expr::path2(data_alias, sql::PathSegment::Asterisk)];

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
            let pg_edge_subj = pg_edge.edge_cardinal(proj.subject)?;
            let pg_edge_obj = pg_edge.edge_cardinal(proj.object)?;

            let obj_cardinal = &edge.cardinals[proj.object.0 as usize];
            let mut sub_layout: Vec<Layout> = vec![];
            let edge_alias = ctx.alias.incr();

            if obj_cardinal.target.len() != 1 {
                panic!("union edge");
            }
            let target_def_id = *obj_cardinal.target.iter().next().unwrap();
            let target_def = self.ontology.def(target_def_id);

            let pg_sub = self
                .pg_model
                .pg_domain_table(target_def_id.package_id(), target_def_id)?;

            let (from, sub_alias, expressions) = match select {
                Select::Leaf => {
                    let Some(target_entity) = target_def.entity() else {
                        return Err(ds_bad_req("cannot select id from non-entity"));
                    };

                    let pg_id = pg_sub.table.field(&target_entity.id_relationship_id)?;
                    let leaf_alias = ctx.alias.incr();

                    sub_layout.push(Layout::Scalar(pg_id.pg_type));
                    (
                        pg_sub.table_name().as_(leaf_alias),
                        leaf_alias,
                        vec![sql::Expr::path2(leaf_alias, pg_id.col_name.as_ref())],
                    )
                }
                Select::Struct(struct_select) => self.sql_select_expressions(
                    struct_select.def_id,
                    struct_select,
                    self.pg_model
                        .pg_domain_table(target_def_id.package_id(), target_def_id)?,
                    &mut sub_layout,
                    ctx,
                )?,
                Select::Entity(entity_select) => match &entity_select.source {
                    StructOrUnionSelect::Struct(struct_select) => self.sql_select_expressions(
                        struct_select.def_id,
                        struct_select,
                        self.pg_model
                            .pg_domain_table(target_def_id.package_id(), target_def_id)?,
                        &mut sub_layout,
                        ctx,
                    )?,
                    StructOrUnionSelect::Union(_union_def_id, _variant_selects) => {
                        todo!("union subselect")
                    }
                },
                Select::EntityId => return Err(ds_err("select entity id")),
                Select::StructUnion(_, _) => todo!("struct-union"),
            };

            let mut sql_subselect = sql::Select {
                expressions: vec![sql::Expr::Row(expressions)],
                from: vec![sql::Join {
                    first: from,
                    second: sql::TableName(&pg.domain.schema_name, &pg_edge.table_name)
                        .as_(edge_alias),
                    on: edge_join_condition(edge_alias, pg_edge_obj, sub_alias, pg_sub.table),
                }
                .into()],
                where_: Some(edge_join_condition(
                    edge_alias,
                    pg_edge_subj,
                    data_alias,
                    pg.table,
                )),
                ..Default::default()
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
        }

        Ok((
            sql::FromItem::Alias(data_alias),
            data_alias,
            sql_expressions,
        ))
    }

    fn select_datatable_fields_as_alias(
        &self,
        def: &Def,
        pg: PgDomainTable<'a>,
        layout: &mut Vec<Layout>,
        ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<Alias> {
        layout.extend([
            Layout::Scalar(PgType::Integer),
            Layout::Scalar(PgType::BigInt),
        ]);

        if let Some(with_alias) = ctx.with_def_aliases.get(&def.id) {
            // still need to fetch layout
            self.select_inherent_struct_fields(def, pg.table, None, layout)?;
            Ok(*with_alias)
        } else {
            let with_alias = ctx.alias.incr();

            let mut expressions = vec![
                // Always present: the def key of the vertex.
                // This is known ahead of time.
                // It will be used later to parse unions.
                sql::Expr::LiteralInt(pg.table.key),
                // Always present: the data key of the vertex
                sql::Expr::path1("_key"),
            ];
            self.select_inherent_struct_fields(def, pg.table, Some(&mut expressions), layout)?;

            ctx.with_queries.push(sql::WithQuery {
                name: sql::Name::Alias(with_alias),
                column_names: vec![],
                stmt: sql::Stmt::Select(sql::Select {
                    expressions,
                    from: vec![pg.table_name().into()],
                    ..Default::default()
                }),
            });

            ctx.with_def_aliases.insert(def.id, with_alias);

            Ok(with_alias)
        }
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

        let value = Value::Struct(Box::new(attrs), def.id.into());

        // debug!("queried value: {value:#?}");

        Ok(RowValue {
            value,
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

                if let Some(entity) = self.ontology.def(target_def_id).entity() {
                    self.deserialize_sql(entity.id_value_def_id, sql_field)
                } else {
                    self.deserialize_sql(target_def_id, sql_field)
                }
            }
            Select::Struct(struct_select) => Ok(self
                .read_struct_row_value(
                    sql_record.fields(),
                    self.ontology.def(target_def_id),
                    struct_select,
                )?
                .value),
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => Ok(self
                    .read_struct_row_value(
                        sql_record.fields(),
                        self.ontology.def(target_def_id),
                        struct_select,
                    )?
                    .value),
                StructOrUnionSelect::Union(..) => todo!("unhandled union"),
            },
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
    data: &'d PgTable,
) -> sql::Expr<'d> {
    match &cardinal.kind {
        PgEdgeCardinalKind::Dynamic {
            def_col_name,
            key_col_name,
        } => sql::Expr::And(vec![
            sql::Expr::eq(
                sql::Expr::path2(edge_alias, def_col_name.as_ref()),
                sql::Expr::LiteralInt(data.key),
            ),
            sql::Expr::eq(
                sql::Expr::path2(edge_alias, key_col_name.as_ref()),
                sql::Expr::path2(data_alias, "_key"),
            ),
        ]),
        PgEdgeCardinalKind::Unique { key_col_name, .. } => sql::Expr::eq(
            sql::Expr::path2(edge_alias, key_col_name.as_ref()),
            sql::Expr::path2(data_alias, "_key"),
        ),
        PgEdgeCardinalKind::Parameters(_params_def_id) => unreachable!(),
    }
}
