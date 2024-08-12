use domain_engine_core::{transact::DataOperation, DomainResult};
use fnv::FnvHashMap;
use futures_util::Stream;
use ontol_runtime::{
    attr::{Attr, AttrMatrix},
    ontology::domain::{DataRelationshipKind, Def},
    property::ValueCardinality,
    query::select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    sequence::SubSequence,
    tuple::CardinalIdx,
    value::Value,
    DefId, RelId,
};
use pin_utils::pin_mut;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::{
    ds_bad_req, ds_err,
    pg_model::{PgDef, PgDomainTable, PgType},
    sql,
    sql_record::{SqlColumnStream, SqlRecord, SqlRecordIterator},
    sql_value::{Layout, SqlVal},
};

use super::{
    data::RowValue,
    query_edge::{
        CardinalSelect, EdgeUnionSelectBuilder, EdgeUnionVariantSelectBuilder, PgEdgeProjection,
    },
    TransactCtx,
};

pub enum QueryFrame {
    Row(RowValue),
    Footer(Option<Box<SubSequence>>),
}

#[derive(Default)]
pub(super) struct QueryBuildCtx<'d> {
    pub alias: sql::Alias,
    pub with_def_aliases: FnvHashMap<DefId, sql::Alias>,
    pub with_queries: Vec<sql::WithQuery<'d>>,
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
        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.package_id(), def_id)?;

        let mut row_layout: Vec<Layout> = vec![];
        let mut ctx = QueryBuildCtx::default();

        let sql_select = {
            let mut expressions = sql::Expressions {
                items: vec![],
                multiline: true,
            };

            if include_total_len {
                expressions.items.push(sql::Expr::CountStarOver);
                row_layout.push(Layout::Scalar(PgType::BigInt));
            }

            let (from, _alias, tail_expressions) =
                self.sql_select_expressions(def_id, &struct_select, pg, &mut ctx)?;
            expressions.items.extend(tail_expressions);

            sql::Select {
                with: if !ctx.with_queries.is_empty() {
                    Some(std::mem::take(&mut ctx.with_queries).into())
                } else {
                    None
                },
                expressions,
                from: vec![from],
                where_: None,
                limit: sql::Limit {
                    limit: Some(limit + 1),
                    offset: match &after_cursor {
                        Some(Cursor::Offset(offset)) => Some(*offset),
                        _ => None,
                    },
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
                let mut row_iter = SqlColumnStream::new(&row);

                if include_total_len {
                    // must read this for every row if include_total_len was true
                    total_len = Some(
                        row_iter
                            .next_field(&Layout::Scalar(PgType::BigInt))?.into_i64()? as usize
                    );
                }

                if observed_rows < limit {
                    let row_value = self.read_struct_row_value(row_iter, def, &struct_select.properties)?;
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

    pub(super) fn sql_select_expressions(
        &self,
        def_id: DefId,
        struct_select: &StructSelect,
        pg: PgDomainTable<'a>,
        ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<(sql::FromItem<'a>, sql::Alias, Vec<sql::Expr<'a>>)> {
        let def = self.ontology.def(def_id);

        // select data properties
        let data_alias = self.select_inherent_fields_as_alias(def, pg, ctx)?;
        let mut sql_expressions = vec![sql::Expr::path2(data_alias, sql::PathSegment::Asterisk)];

        // select edges
        for (rel_id, select) in &struct_select.properties {
            let Some(rel_info) = def.data_relationships.get(rel_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(proj) = &rel_info.kind else {
                continue;
            };

            let edge_alias = ctx.alias.incr();
            let edge_info = self.ontology.find_edge(proj.id).unwrap();
            let pg_edge = self.pg_model.pg_domain_edgetable(&proj.id)?;
            let pg_subj_cardinal = pg_edge.table.edge_cardinal(proj.subject)?;

            let pg_proj = PgEdgeProjection {
                id: proj.id,
                subject_index: proj.subject,
                object_index: proj.object,
                pg_subj_data: pg,
                pg_subj_cardinal,
                subj_alias: data_alias,
                edge_info,
                pg_edge,
                edge_alias,
            };
            let cardinal_select = CardinalSelect::from_select(select);

            let mut union_builder = EdgeUnionSelectBuilder::default();

            self.sql_select_edge_cardinals(
                CardinalIdx(0),
                &pg_proj,
                cardinal_select,
                EdgeUnionVariantSelectBuilder::default(),
                &mut union_builder,
                ctx,
            )?;

            let mut union_iter = union_builder.union_exprs.into_iter();

            if let Some(mut union_expr) = union_iter.next() {
                for next in union_iter {
                    union_expr = sql::Expr::Union(Box::new(sql::Union {
                        first: union_expr,
                        all: true,
                        second: next,
                    }));
                }

                debug!("union expr: {union_expr}");

                match rel_info.cardinality.1 {
                    ValueCardinality::Unit => {
                        sql_expressions.push(sql::Expr::paren(sql::Expr::Limit(
                            Box::new(union_expr),
                            sql::Limit {
                                limit: Some(1),
                                offset: None,
                            },
                        )));
                    }
                    ValueCardinality::IndexSet | ValueCardinality::List => {
                        sql_expressions.push(sql::Expr::array(union_expr));
                    }
                }
            }
        }

        Ok((
            sql::FromItem::Alias(data_alias),
            data_alias,
            sql_expressions,
        ))
    }

    fn select_inherent_fields_as_alias(
        &self,
        def: &Def,
        pg: PgDomainTable<'a>,
        ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<sql::Alias> {
        if let Some(with_alias) = ctx.with_def_aliases.get(&def.id) {
            Ok(*with_alias)
        } else {
            let with_alias = ctx.alias.incr();

            let mut expressions = sql::Expressions {
                items: vec![
                    // Always present: the def key of the vertex.
                    // This is known ahead of time.
                    // It will be used later to parse unions.
                    sql::Expr::LiteralInt(pg.table.key),
                    // Always present: the data key of the vertex
                    sql::Expr::path1("_key"),
                ],
                multiline: false,
            };

            self.select_inherent_struct_fields(def, pg.table, Some(&mut expressions.items))?;

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
        mut iterator: impl SqlRecordIterator<'b>,
        def: &Def,
        select_properties: &FnvHashMap<RelId, Select>,
    ) -> DomainResult<RowValue> {
        let mut attrs: FnvHashMap<RelId, Attr> =
            FnvHashMap::with_capacity_and_hasher(def.data_relationships.len(), Default::default());

        let _def_key = iterator
            .next_field(&Layout::Scalar(PgType::Integer))?
            .into_i32()?;
        let data_key = iterator
            .next_field(&Layout::Scalar(PgType::BigInt))?
            .into_i64()?;

        // retrieve data properties
        self.read_inherent_struct_fields(def, &mut iterator, &mut attrs)?;

        // retrieve edges
        for (rel_id, select) in select_properties {
            let Some(rel_info) = def.data_relationships.get(rel_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(_proj) = &rel_info.kind else {
                continue;
            };

            match rel_info.cardinality.1 {
                ValueCardinality::Unit => {
                    let sql_edge_tuple = iterator.next_field(&Layout::Record)?.into_record()?;

                    let sql_field = sql_edge_tuple.fields().next_field(&Layout::Record)?;
                    let value = self.read_dyn_record(sql_field, select)?;
                    attrs.insert(*rel_id, Attr::Unit(value));
                }
                ValueCardinality::IndexSet | ValueCardinality::List => {
                    let sql_array = iterator.next_field(&Layout::Array)?.into_array()?;

                    let mut matrix = AttrMatrix::default();
                    matrix.columns.push(Default::default());

                    for result in sql_array.elements(&Layout::Record) {
                        let sql_edge_tuple = result?.into_record()?;
                        let sql_field = sql_edge_tuple.fields().next_field(&Layout::Record)?;
                        let value = self.read_dyn_record(sql_field, select)?;
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

    fn read_dyn_record(&self, sql_val: SqlVal, select: &Select) -> DomainResult<Value> {
        let sql_dyn_record = sql_val.into_record()?;
        let def_key = sql_dyn_record.def_key()?;
        let (_pkg_id, def_id) = self.pg_model.datatable_key_by_def_key(def_key)?;
        let pg_def = self.lookup_def(def_id)?;

        match select {
            Select::Leaf => {
                let Some(entity) = pg_def.def.entity() else {
                    return Err(ds_err("not an entity"));
                };

                let pg_id = pg_def.pg.table.field(&entity.id_relationship_id)?;
                let mut fields = sql_dyn_record.fields();
                fields.next_field(&Layout::Scalar(PgType::Integer))?;
                let sql_field = fields.next_field(&Layout::Scalar(pg_id.pg_type))?;

                self.deserialize_sql(entity.id_value_def_id, sql_field)
            }
            Select::Struct(struct_select) => {
                self.read_dyn_struct(pg_def, sql_dyn_record, &struct_select.properties)
            }
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => {
                    self.read_dyn_struct(pg_def, sql_dyn_record, &struct_select.properties)
                }
                StructOrUnionSelect::Union(_, variants) => {
                    let actual_select = variants
                        .iter()
                        .find(|sel| sel.def_id == def_id)
                        .ok_or_else(|| ds_err("actual value not found in select union"))?;

                    self.read_dyn_struct(pg_def, sql_dyn_record, &actual_select.properties)
                }
            },
            Select::StructUnion(_, variants) => {
                let actual_select = variants
                    .iter()
                    .find(|sel| sel.def_id == def_id)
                    .ok_or_else(|| ds_err("actual value not found in select union"))?;

                self.read_dyn_struct(pg_def, sql_dyn_record, &actual_select.properties)
            }
            _ => todo!("unhandled select"),
        }
    }

    fn read_dyn_struct(
        &self,
        pg_def: PgDef,
        sql_dyn_record: SqlRecord,
        select_properties: &FnvHashMap<RelId, Select>,
    ) -> DomainResult<Value> {
        let row_value =
            self.read_struct_row_value(sql_dyn_record.fields(), pg_def.def, select_properties)?;

        Ok(row_value.value)
    }
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a dyn ToSql> + 'a {
    s.iter().map(|s| *s as _)
}
