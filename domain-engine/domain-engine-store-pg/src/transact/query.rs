use std::{borrow::Cow, collections::BTreeSet};

use domain_engine_core::{
    domain_error::DomainErrorKind, filter::walker::ConditionWalker, transact::DataOperation,
    DomainError, DomainResult,
};
use fnv::FnvHashMap;
use futures_util::Stream;
use ontol_runtime::{
    attr::{Attr, AttrMatrix},
    ontology::domain::{DataRelationshipKind, Def},
    property::{PropertyCardinality, ValueCardinality},
    query::{
        filter::Filter,
        select::{EntitySelect, StructOrUnionSelect},
    },
    sequence::SubSequence,
    tuple::EndoTuple,
    value::Value,
    DefId, DomainIndex, PropId,
};
use pin_utils::pin_mut;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::{debug, trace};

use crate::{
    address::make_ontol_vertex_address,
    pg_error::{PgError, PgInputError, PgModelError},
    pg_model::{
        EdgeId, PgDataKey, PgDomainTable, PgEdgeCardinalKind, PgRegKey, PgTable, PgTableKey, PgType,
    },
    sql::{self, FromItem, WhereExt},
    sql_record::{SqlColumnStream, SqlRecord, SqlRecordIterator},
    sql_value::{Layout, SqlScalar},
    transact::query_select::QuerySelectRef,
};

use super::{
    data::RowValue,
    edge_query::{EdgeUnionSelectBuilder, EdgeUnionVariantSelectBuilder, PgEdgeProjection},
    fields::{AbstractKind, StandardAttrs},
    mut_ctx::PgMutCtx,
    query_select::{CardinalSelect, QuerySelect, VertexSelect},
    TransactCtx,
};

pub enum QueryFrame {
    Row(RowValue),
    Footer(Option<Box<SubSequence>>),
}

#[derive(Default)]
pub(super) struct QueryBuildCtx<'a> {
    pub alias: sql::Alias,
    pub with_def_aliases: FnvHashMap<DefAliasKey<'static>, sql::Alias>,
    pub with_queries: Vec<sql::WithQuery<'a>>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefAliasKey<'a> {
    pub def_id: DefId,
    pub inherent_set: Cow<'a, BTreeSet<PropId>>,
}

impl<'a> QueryBuildCtx<'a> {
    pub fn with(&mut self) -> Option<sql::With<'a>> {
        if !self.with_queries.is_empty() {
            Some(std::mem::take(&mut self.with_queries).into())
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Cursor {
    /// The only pagination supported for now, is offset-pagination.
    /// Future improvements include value pagination (WHERE (set of values) > (cursor values))
    /// for a specific ordering.
    Offset(usize),
}

pub enum IncludeJoinedAttrs {
    Yes,
    No,
}

pub struct Query {
    pub include_total_len: bool,
    pub limit: Option<usize>,
    pub after_cursor: Option<Cursor>,
    pub native_id_condition: Option<PgDataKey>,
}

impl<'a> TransactCtx<'a> {
    pub async fn query_vertex<'s>(
        &'s self,
        entity_select: &'s EntitySelect,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<impl Stream<Item = DomainResult<QueryFrame>> + '_> {
        let struct_select = match &entity_select.source {
            StructOrUnionSelect::Struct(struct_select) => struct_select,
            StructOrUnionSelect::Union(..) => return Err(PgInputError::UnionTopLevelQuery.into()),
        };
        let after_cursor: Option<Cursor> = match &entity_select.after_cursor {
            Some(bytes) => {
                Some(bincode::deserialize(bytes).map_err(|e| PgInputError::BadCursor(e.into()))?)
            }
            None => None,
        };

        let def_id = struct_select.def_id;
        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.domain_index(), def_id)?;
        let def = self.ontology.def(def_id);

        let vertex_select =
            self.analyze_vertex_select_properties(def, pg.table, &struct_select.properties)?;

        self.query(
            struct_select.def_id,
            Query {
                include_total_len: entity_select.include_total_len,
                limit: entity_select.limit,
                after_cursor,
                native_id_condition: None,
            },
            Some(&entity_select.filter),
            QuerySelect::Vertex(vertex_select),
            mut_ctx,
        )
        .await
    }

    pub async fn query<'s>(
        &'s self,
        def_id: DefId,
        q: Query,
        filter: Option<&Filter>,
        query_select: QuerySelect,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<impl Stream<Item = DomainResult<QueryFrame>> + 's> {
        debug!("query {def_id:?} after cursor: {:?}", q.after_cursor);

        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.domain_index(), def_id)?;

        let mut row_layout: Vec<Layout> = vec![];
        let mut query_ctx = QueryBuildCtx::default();
        let mut sql_params: Vec<SqlScalar> = vec![];

        let sql_select = {
            let mut expressions = sql::Expressions {
                items: vec![],
                multiline: true,
            };

            if q.include_total_len {
                expressions.items.push(sql::Expr::CountStarOver);
                row_layout.push(Layout::Scalar(PgType::BigInt));
            }

            let (from, alias, tail_expressions) = {
                match query_select.as_ref() {
                    QuerySelectRef::VertexUnion(vertex_selects) => {
                        // no disambiguation should be needed here, since at root level
                        let vertex_select = vertex_selects.iter().next().unwrap();
                        self.sql_select_vertex_expressions_with_alias(
                            def_id,
                            vertex_select,
                            pg,
                            &mut query_ctx,
                            mut_ctx,
                        )?
                    }
                    QuerySelectRef::VertexAddress => (
                        pg.table_name().into(),
                        sql::Alias(0),
                        vec![sql::Expr::path1("_key")],
                    ),
                    QuerySelectRef::EntityId => {
                        let mut fields: Vec<_> = self.initial_standard_data_fields(pg).into();
                        let def = self.ontology.def(def_id);
                        let Some(entity) = def.entity() else {
                            return Err(DomainErrorKind::NotAnEntity(def_id).into_error());
                        };

                        fields.push(sql::Expr::path1(pg.table.column(&entity.id_prop)?.col_name));

                        (pg.table_name().into(), sql::Alias(0), fields)
                    }
                    QuerySelectRef::Unit => {
                        let fields: Vec<_> = self.initial_standard_data_fields(pg).into();
                        (pg.table_name().into(), sql::Alias(0), fields)
                    }
                }
            };

            expressions.items.extend(tail_expressions);

            let mut sql_select = sql::Select {
                expressions,
                from: vec![from],
                limit: sql::Limit {
                    limit: q.limit.map(|limit| limit + 1),
                    offset: match &q.after_cursor {
                        Some(Cursor::Offset(offset)) => Some(*offset),
                        _ => None,
                    },
                },
                ..Default::default()
            };

            if let Some(native_id_condition) = q.native_id_condition {
                sql_select.where_and(sql::Expr::eq(sql::Expr::path1("_key"), sql::Expr::param(0)));
                sql_params.push(SqlScalar::I64(native_id_condition));
            }

            if let Some(filter) = filter {
                debug!("condition {}", filter.condition());
                if let Some(condition) = self.sql_where_condition(
                    def_id,
                    alias,
                    ConditionWalker::new(filter.condition()),
                    &mut sql_params,
                    &mut query_ctx,
                )? {
                    sql_select.where_and(condition);
                }

                let vertex_order_tuple = filter
                    .vertex_order()
                    .ok_or_else(|| DomainError::data_store("vertex order undefined"))?;
                sql_select.order_by = self.select_order(def_id, vertex_order_tuple)?;
            }

            sql_select.with = query_ctx.with();
            sql_select
        };

        let sql = sql_select.to_string();
        debug!("{sql}");
        trace!("{sql_params:?}");

        let row_stream = self
            .client()
            .query_raw(&sql, &sql_params)
            .await
            .map_err(PgError::SelectQuery)?;

        Ok(async_stream::try_stream! {
            pin_mut!(row_stream);

            let mut total_len: Option<usize> = None;
            let mut observed_values = 0;
            let mut observed_rows = 0;

            for await row_result in row_stream {
                let row = row_result.map_err(PgError::SelectRow)?;
                let mut row_iter = SqlColumnStream::new(&row);

                if q.include_total_len {
                    // must read this for every row if include_total_len was true
                    total_len = Some(
                        row_iter
                            .next_field(&Layout::Scalar(PgType::BigInt))?.into_i64()? as usize
                    );
                }

                if q.limit.map(|limit| observed_rows < limit).unwrap_or(false) {
                    let row_value = self.read_vertex_row_value(row_iter, query_select.as_ref(), IncludeJoinedAttrs::Yes, DataOperation::Queried)?;
                    yield QueryFrame::Row(row_value);

                    observed_values += 1;
                    observed_rows += 1;
                } else {
                    observed_rows += 1;
                    break;
                }
            }

            let end_cursor = if observed_values > 0 {
                let original_offset = match &q.after_cursor {
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
                    has_next: if q.limit.is_some() {
                        observed_rows > observed_values
                    } else {
                        false
                    },
                    total_len
                }))
            );
        })
    }

    pub(super) fn sql_select_vertex_expressions_with_alias(
        &self,
        def_id: DefId,
        vertex_select: &VertexSelect,
        pg: PgDomainTable<'a>,
        query_ctx: &mut QueryBuildCtx<'a>,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<(sql::FromItem<'a>, sql::Alias, Vec<sql::Expr<'a>>)> {
        let def = self.ontology.def(def_id);

        // select data properties
        let mut sql_expressions = vec![];
        let data_alias =
            self.select_inherent_fields_as_alias((def, pg), vertex_select, query_ctx)?;
        sql_expressions.push(sql::Expr::path2(data_alias, sql::PathSegment::Asterisk));

        self.sql_select_abstract_properties(
            (def, pg),
            data_alias,
            vertex_select,
            query_ctx,
            &mut sql_expressions,
        )?;

        self.sql_select_edge_properties(
            (def, pg),
            data_alias,
            vertex_select,
            query_ctx,
            &mut sql_expressions,
            mut_ctx,
        )?;

        Ok((
            sql::FromItem::Alias(data_alias),
            data_alias,
            sql_expressions,
        ))
    }

    fn sql_select_abstract_properties(
        &self,
        (def, pg): (&Def, PgDomainTable<'a>),
        parent_alias: sql::Alias,
        // currently abstract properties are selected unconditionally
        vertex_select: &VertexSelect,
        query_ctx: &mut QueryBuildCtx<'a>,
        output: &mut Vec<sql::Expr<'a>>,
    ) -> DomainResult<()> {
        // TODO: The abstract props can be cached in PgTable
        for (prop_id, sub_select) in &vertex_select.abstract_set {
            let Some(rel_info) = def.data_relationships.get(prop_id) else {
                continue;
            };

            let Some(prop_key) = pg.table.find_abstract_property(prop_id) else {
                continue;
            };

            match self.abstract_kind(&rel_info.target) {
                AbstractKind::VertexUnion(def_ids) => {
                    let mut union_operands: Vec<sql::Expr> = vec![];

                    for sub_def_id in def_ids.iter().copied() {
                        let sub_def = self.ontology.def(sub_def_id);
                        let sub_pg = self
                            .pg_model
                            .pg_domain_datatable(sub_def_id.domain_index(), sub_def_id)?;
                        let sub_alias = query_ctx.alias.incr();

                        let mut record_items: Vec<_> = vec![];

                        match sub_select.as_ref() {
                            QuerySelectRef::Unit => {
                                record_items.extend(self.initial_standard_data_fields(sub_pg));
                            }
                            QuerySelectRef::VertexUnion(vertex_selects) => {
                                let Some(vertex_select) =
                                    vertex_selects.iter().find(|sel| sel.def_id == sub_def_id)
                                else {
                                    continue;
                                };

                                record_items.extend(self.initial_standard_data_fields(sub_pg));

                                self.select_inherent_vertex_fields(
                                    sub_pg.table,
                                    vertex_select,
                                    &mut record_items,
                                    Some(sub_alias),
                                );

                                self.sql_select_abstract_properties(
                                    (sub_def, sub_pg),
                                    sub_alias,
                                    vertex_select,
                                    query_ctx,
                                    &mut record_items,
                                )?;
                            }
                            QuerySelectRef::VertexAddress => todo!(),
                            QuerySelectRef::EntityId => todo!(),
                        };

                        union_operands.push(
                            sql::Select {
                                with: None,
                                expressions: sql::Expressions {
                                    items: vec![sql::Expr::Row(record_items)],
                                    multiline: false,
                                },
                                from: vec![FromItem::TableNameAs(
                                    sub_pg.table_name(),
                                    sub_alias.into(),
                                )],
                                where_: Some(sql::Expr::eq(
                                    sql::Expr::Tuple(vec![
                                        sql::Expr::path2(sub_alias, "_fprop"),
                                        sql::Expr::path2(sub_alias, "_fkey"),
                                    ]),
                                    sql::Expr::Tuple(vec![
                                        sql::Expr::LiteralInt(prop_key),
                                        sql::Expr::path2(parent_alias, "_key"),
                                    ]),
                                )),
                                ..Default::default()
                            }
                            .into(),
                        );
                    }

                    let mut union_iter = union_operands.into_iter();
                    if let Some(mut union_expr) = union_iter.next() {
                        for next in union_iter {
                            union_expr = sql::Expr::Union(Box::new(sql::Union {
                                first: union_expr,
                                all: true,
                                second: next,
                            }));
                        }

                        output.push(match rel_info.cardinality.1 {
                            ValueCardinality::Unit => sql::Expr::paren(sql::Expr::Limit(
                                Box::new(union_expr),
                                sql::Limit {
                                    limit: Some(1),
                                    offset: None,
                                },
                            )),
                            ValueCardinality::IndexSet | ValueCardinality::List => {
                                sql::Expr::array(union_expr)
                            }
                        });
                    }
                }
                AbstractKind::Scalar { ontol_def_tag, .. } => {
                    let domain_index = prop_id.0.domain_index();
                    let sub_def_id = ontol_def_tag.def_id();

                    let sub_pg = self
                        .pg_model
                        .pg_domain_datatable(domain_index, sub_def_id)?;

                    // abstract scalar should always be an array
                    output.push(sql::Expr::array(sql::Select {
                        with: None,
                        expressions: sql::Expressions {
                            items: vec![sql::Expr::path1("value")],
                            multiline: false,
                        },
                        from: vec![sub_pg.table_name().into()],
                        where_: Some(sql::Expr::eq(
                            sql::Expr::Tuple(vec![
                                sql::Expr::path1("_fprop"),
                                sql::Expr::path1("_fkey"),
                            ]),
                            sql::Expr::Tuple(vec![
                                sql::Expr::LiteralInt(prop_key),
                                sql::Expr::path2(parent_alias, "_key"),
                            ]),
                        )),
                        ..Default::default()
                    }));
                }
            }
        }

        Ok(())
    }

    pub fn sql_select_edge_properties(
        &self,
        (def, pg): (&Def, PgDomainTable<'a>),
        data_alias: sql::Alias,
        vertex_select: &VertexSelect,
        query_ctx: &mut QueryBuildCtx<'a>,
        output: &mut Vec<sql::Expr<'a>>,
        mut_ctx: &mut PgMutCtx,
    ) -> DomainResult<()> {
        for (prop_id, cardinal_selects) in &vertex_select.edge_set {
            let Some(rel_info) = def.data_relationships.get(prop_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(proj) = &rel_info.kind else {
                continue;
            };

            let edge_alias = query_ctx.alias.incr();
            let edge_info = self.ontology.find_edge(proj.edge_id).unwrap();
            let pg_edge = self.pg_model.pg_domain_edgetable(&EdgeId(proj.edge_id))?;
            let pg_subj_cardinal = pg_edge.table.edge_cardinal(proj.subject)?;

            let pg_proj = PgEdgeProjection {
                id: EdgeId(proj.edge_id),
                pg_subj_data: pg,
                pg_subj_cardinal,
                subj_alias: data_alias,
                edge_info,
                pg_edge,
                edge_alias,
            };

            let mut union_builder = EdgeUnionSelectBuilder::default();

            self.sql_select_edge_cardinals(
                &pg_proj,
                (cardinal_selects, 0),
                EdgeUnionVariantSelectBuilder::default(),
                &mut union_builder,
                query_ctx,
                mut_ctx,
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

                trace!("union expr: {union_expr}");

                match rel_info.cardinality.1 {
                    ValueCardinality::Unit => {
                        output.push(sql::Expr::paren(sql::Expr::Limit(
                            Box::new(union_expr),
                            sql::Limit {
                                limit: Some(1),
                                offset: None,
                            },
                        )));
                    }
                    ValueCardinality::IndexSet | ValueCardinality::List => {
                        output.push(sql::Expr::array(union_expr));
                    }
                }
            }
        }

        Ok(())
    }

    fn select_inherent_fields_as_alias(
        &self,
        (def, pg): (&Def, PgDomainTable<'a>),
        vertex_select: &VertexSelect,
        query_ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<sql::Alias> {
        if let Some(with_alias) = query_ctx.with_def_aliases.get(&DefAliasKey {
            def_id: def.id,
            inherent_set: Cow::Borrowed(&vertex_select.inherent_set),
        }) {
            Ok(*with_alias)
        } else {
            let with_alias = query_ctx.alias.incr();

            let mut expressions = sql::Expressions {
                items: self.initial_standard_data_fields(pg).into(),
                multiline: false,
            };

            self.select_inherent_vertex_fields(
                pg.table,
                vertex_select,
                &mut expressions.items,
                None,
            );

            query_ctx.with_queries.push(sql::WithQuery {
                name: with_alias.into(),
                column_names: vec![],
                stmt: sql::Stmt::Select(sql::Select {
                    expressions,
                    from: vec![pg.table_name().into()],
                    ..Default::default()
                }),
            });

            query_ctx.with_def_aliases.insert(
                DefAliasKey {
                    def_id: def.id,
                    inherent_set: Cow::Owned(vertex_select.inherent_set.clone()),
                },
                with_alias,
            );

            Ok(with_alias)
        }
    }

    pub fn read_vertex_row_value<'b>(
        &self,
        mut iterator: impl SqlRecordIterator<'b>,
        query_select: QuerySelectRef,
        include_joined_attrs: IncludeJoinedAttrs,
        op: DataOperation,
    ) -> DomainResult<RowValue> {
        let def_key = iterator
            .next_field(&Layout::Scalar(PgType::Integer))?
            .into_i32()?;

        let data_key = iterator
            .next_field(&Layout::Scalar(PgType::BigInt))?
            .into_i64()?;
        let created_at = iterator
            .next_field(&Layout::Scalar(PgType::TimestampTz))?
            .into_timestamp()?;
        let updated_at = iterator
            .next_field(&Layout::Scalar(PgType::TimestampTz))?
            .into_timestamp()?;

        let standard_attrs = StandardAttrs {
            data_key,
            created_at,
            updated_at,
        };

        match query_select {
            QuerySelectRef::Unit => Ok(RowValue {
                value: Value::unit(),
                def_key,
                data_key,
                updated_at,
                op,
            }),
            QuerySelectRef::VertexAddress => todo!("vertex address"),
            QuerySelectRef::VertexUnion(vertex_selects) => {
                let Some(PgTableKey::Data {
                    domain_index,
                    def_id,
                }) = self.pg_model.reg_key_to_table_key.get(&def_key)
                else {
                    return Err(PgError::InvalidDynamicDataType(def_key).into());
                };
                match vertex_selects.iter().find(|vs| vs.def_id == *def_id) {
                    Some(vertex_select) => self.read_row_value_with_vertex_select(
                        iterator,
                        (def_key, *domain_index, *def_id),
                        standard_attrs,
                        vertex_select,
                        include_joined_attrs,
                        op,
                    ),
                    None => Ok(RowValue {
                        value: Value::Void(DefId::unit().into()),
                        def_key,
                        data_key,
                        updated_at,
                        op,
                    }),
                }
            }
            QuerySelectRef::EntityId => {
                let Some(PgTableKey::Data { def_id, .. }) =
                    self.pg_model.reg_key_to_table_key.get(&def_key)
                else {
                    return Err(PgError::InvalidDynamicDataType(def_key).into());
                };
                let def = self.ontology.def(*def_id);
                let Some(entity) = def.entity() else {
                    return Err(DomainErrorKind::NotAnEntity(*def_id).into_error());
                };

                let field_value = self
                    .read_field(
                        def.data_relationships
                            .get(&entity.id_prop)
                            .ok_or(PgModelError::NonExistentField(entity.id_prop))?,
                        &mut iterator,
                        Some(&standard_attrs),
                    )?
                    .ok_or(PgError::MissingField(entity.id_prop))?;

                Ok(RowValue {
                    value: field_value,
                    def_key,
                    data_key,
                    updated_at,
                    op,
                })
            }
        }
    }

    pub fn read_row_value_with_vertex_select<'b>(
        &self,
        mut iterator: impl SqlRecordIterator<'b>,
        (def_key, domain_index, def_id): (PgRegKey, DomainIndex, DefId),
        standard_attrs: StandardAttrs,
        vertex_select: &VertexSelect,
        include_joined_attrs: IncludeJoinedAttrs,
        op: DataOperation,
    ) -> DomainResult<RowValue> {
        let def = self.ontology.def(def_id);

        let mut attrs: FnvHashMap<PropId, Attr> =
            FnvHashMap::with_capacity_and_hasher(def.data_relationships.len(), Default::default());

        attrs.insert(
            PropId::data_store_address(),
            Attr::Unit(make_ontol_vertex_address(def_key, standard_attrs.data_key)),
        );

        // retrieve data properties
        self.read_inherent_vertex_fields(
            &mut iterator,
            def,
            vertex_select,
            &mut attrs,
            Some(&standard_attrs),
        )?;

        if matches!(&include_joined_attrs, IncludeJoinedAttrs::Yes) {
            // retrieve abstract properties
            let pg = self.pg_model.pg_domain_datatable(domain_index, def_id)?;

            for (prop_id, abstract_select) in &vertex_select.abstract_set {
                if pg.table.find_abstract_property(prop_id).is_none() {
                    continue;
                }

                let Some(rel_info) = def.data_relationships.get(prop_id) else {
                    continue;
                };

                match (
                    self.abstract_kind(&rel_info.target),
                    &rel_info.cardinality.1,
                ) {
                    (AbstractKind::VertexUnion(_), ValueCardinality::Unit) => {
                        if let Some(sql_val) = iterator.next_field(&Layout::Record)?.null_filter() {
                            let sql_record = sql_val.into_record()?;
                            let row_value = self.read_vertex_row_value(
                                sql_record.fields(),
                                abstract_select.as_ref(),
                                IncludeJoinedAttrs::Yes,
                                DataOperation::Queried,
                            )?;

                            debug!("abstract value: {:?}", row_value.value);

                            attrs.insert(*prop_id, Attr::Unit(row_value.value));
                        }
                    }
                    (
                        AbstractKind::VertexUnion(_),
                        ValueCardinality::IndexSet | ValueCardinality::List,
                    ) => {
                        let sql_array = iterator.next_field(&Layout::Array)?.into_array()?;
                        let mut matrix = AttrMatrix::default();
                        matrix.columns.push(Default::default());

                        for result in sql_array.elements(&Layout::Record) {
                            let sql_record = result?.into_record()?;

                            let row_value = self.read_vertex_row_value(
                                sql_record.fields(),
                                abstract_select.as_ref(),
                                IncludeJoinedAttrs::Yes,
                                op,
                            )?;

                            matrix.columns[0].push(row_value.value);
                        }

                        if matches!(rel_info.cardinality.0, PropertyCardinality::Mandatory)
                            || !matrix.columns[0].elements().is_empty()
                        {
                            attrs.insert(*prop_id, Attr::Matrix(matrix));
                        }
                    }
                    (
                        AbstractKind::Scalar {
                            pg_type,
                            target_def_id,
                            ..
                        },
                        _,
                    ) => {
                        let sql_array = iterator.next_field(&Layout::Array)?.into_array()?;

                        let mut matrix = AttrMatrix::default();
                        matrix.columns.push(Default::default());

                        for result in sql_array.elements(&Layout::Scalar(pg_type)) {
                            let sql_val = result?;

                            let value = self.deserialize_sql(target_def_id, sql_val)?;

                            matrix.columns[0].push(value);
                        }

                        if matches!(rel_info.cardinality.0, PropertyCardinality::Mandatory)
                            || !matrix.columns[0].elements().is_empty()
                        {
                            attrs.insert(*prop_id, Attr::Matrix(matrix));
                        }
                    }
                }
            }

            self.read_edge_attributes(&mut iterator, def, vertex_select, &mut attrs)?;
        }

        let value = Value::Struct(Box::new(attrs), def.id.into());

        Ok(RowValue {
            value,
            def_key,
            data_key: standard_attrs.data_key,
            updated_at: standard_attrs.updated_at,
            op,
        })
    }

    pub fn read_edge_attributes<'b>(
        &self,
        record_iter: &mut impl SqlRecordIterator<'b>,
        def: &Def,
        vertex_select: &VertexSelect,
        attrs: &mut FnvHashMap<PropId, Attr>,
    ) -> DomainResult<()> {
        for (prop_id, cardinal_selects) in &vertex_select.edge_set {
            let Some(rel_info) = def.data_relationships.get(prop_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(proj) = &rel_info.kind else {
                continue;
            };

            let pg_edge = self.pg_model.edgetable(&EdgeId(proj.edge_id))?;

            match rel_info.cardinality.1 {
                ValueCardinality::Unit => {
                    if let Some(sql_edge_tuple) =
                        record_iter.next_field(&Layout::Record)?.null_filter()
                    {
                        let sql_edge_tuple = sql_edge_tuple.into_record()?;
                        let attr = self.read_edge_tuple_as_attr(
                            sql_edge_tuple,
                            cardinal_selects,
                            pg_edge,
                        )?;
                        attrs.insert(*prop_id, attr);
                    }
                }
                ValueCardinality::IndexSet | ValueCardinality::List => {
                    let sql_array = record_iter.next_field(&Layout::Array)?.into_array()?;

                    let mut matrix = AttrMatrix::default();
                    matrix.columns.push(Default::default());

                    for result in sql_array.elements(&Layout::Record) {
                        let sql_edge_tuple = result?.into_record()?;

                        match self.read_edge_tuple_as_attr(
                            sql_edge_tuple,
                            cardinal_selects,
                            pg_edge,
                        )? {
                            Attr::Unit(value) => {
                                matrix.columns[0].push(value);
                            }
                            Attr::Tuple(tuple) => {
                                let mut tuple_iter = tuple.elements.into_iter();
                                matrix.columns[0].push(tuple_iter.next().unwrap());

                                let mut column_index: usize = 1;

                                for value in tuple_iter {
                                    if matrix.columns.len() <= column_index {
                                        matrix.columns.push(Default::default());
                                    }

                                    matrix.columns[column_index].push(value);
                                    column_index += 1;
                                }
                            }
                            Attr::Matrix(_) => return Err(PgInputError::MatrixInMatrix.into()),
                        }
                    }

                    attrs.insert(*prop_id, Attr::Matrix(matrix));
                }
            }
        }

        Ok(())
    }

    fn read_edge_tuple_as_attr(
        &self,
        sql_edge_tuple: SqlRecord,
        cardinal_selects: &[CardinalSelect],
        pg_edge: &PgTable,
    ) -> DomainResult<Attr> {
        let mut tuple_fields = sql_edge_tuple.fields();
        let len = cardinal_selects.len();
        let mut tup_elements: SmallVec<Value, 1> = Default::default();

        debug!("read edge tuple {cardinal_selects:#?}");

        for cardinal_select in cardinal_selects {
            let sql_record = tuple_fields.next_field(&Layout::Record)?.into_record()?;
            let Some(pg_cardinal) = pg_edge.edge_cardinals.get(&cardinal_select.cardinal_idx)
            else {
                continue;
            };

            let value = match &pg_cardinal.kind {
                PgEdgeCardinalKind::Parameters(def_id) => {
                    let def = self.ontology.def(*def_id);
                    let mut attrs: FnvHashMap<PropId, Attr> = FnvHashMap::with_capacity_and_hasher(
                        def.data_relationships.len(),
                        Default::default(),
                    );

                    // retrieve data properties
                    if let QuerySelect::Vertex(vertex_select) = &cardinal_select.select {
                        self.read_inherent_vertex_fields(
                            &mut sql_record.fields(),
                            def,
                            vertex_select,
                            &mut attrs,
                            None,
                        )?;
                    }

                    Value::Struct(Box::new(attrs), (*def_id).into())
                }
                _ => self.read_edge_cardinal(sql_record, cardinal_select.select.as_ref())?,
            };

            if len == 1 {
                return Ok(Attr::Unit(value));
            } else {
                tup_elements.push(value);
            }
        }

        Ok(Attr::Tuple(Box::new(EndoTuple {
            elements: tup_elements,
        })))
    }

    fn read_edge_cardinal(
        &self,
        sql_record: SqlRecord,
        select: QuerySelectRef,
    ) -> DomainResult<Value> {
        let def_key = sql_record.def_key()?;
        let (_domain_index, def_id) = self.pg_model.datatable_key_by_def_key(def_key)?;
        let pg_def = self.lookup_def(def_id)?;

        match select {
            QuerySelectRef::Unit | QuerySelectRef::EntityId => {
                let Some(entity) = pg_def.def.entity() else {
                    return Err(PgInputError::NotAnEntity.into());
                };

                let pg_id = pg_def.pg.table.column(&entity.id_prop)?;
                let mut fields = sql_record.fields();
                fields.next_field(&Layout::Scalar(PgType::Integer))?;
                let sql_field = fields.next_field(&Layout::Scalar(pg_id.pg_type))?;

                self.deserialize_sql(entity.id_value_def_id, sql_field)
            }
            QuerySelectRef::VertexAddress => {
                let mut fields = sql_record.fields();

                let _ = fields.next_field(&Layout::Scalar(PgType::Integer))?;
                let data_key = fields
                    .next_field(&Layout::Scalar(PgType::BigInt))?
                    .into_i64()?;

                Ok(make_ontol_vertex_address(def_key, data_key))
            }
            QuerySelectRef::VertexUnion(variants) => {
                let vertex_select = variants
                    .iter()
                    .find(|sel| sel.def_id == def_id)
                    .ok_or(PgInputError::UnionVariantNotFound)?;

                let row_value = self.read_vertex_row_value(
                    sql_record.fields(),
                    QuerySelectRef::VertexUnion(std::slice::from_ref(vertex_select)),
                    IncludeJoinedAttrs::Yes,
                    DataOperation::Queried,
                )?;

                Ok(row_value.value)
            }
        }
    }
}
