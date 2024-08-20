use domain_engine_core::{transact::DataOperation, DomainResult};
use fnv::FnvHashMap;
use futures_util::Stream;
use ontol_runtime::{
    attr::{Attr, AttrMatrix},
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget, Def},
    property::ValueCardinality,
    query::select::{EntitySelect, Select, StructOrUnionSelect},
    sequence::SubSequence,
    tuple::{CardinalIdx, EndoTuple},
    value::Value,
    DefId, PropId,
};
use pin_utils::pin_mut;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use tracing::{debug, trace};

use crate::{
    pg_error::{PgError, PgInputError, PgModelError},
    pg_model::{PgDataKey, PgDomainTable, PgEdgeCardinalKind, PgTable, PgTableKey, PgType},
    sql::{self, FromItem},
    sql_record::{SqlColumnStream, SqlRecord, SqlRecordIterator},
    sql_value::{Layout, SqlVal},
};

use super::{
    data::RowValue,
    edge_query::{
        CardinalSelect, EdgeUnionSelectBuilder, EdgeUnionVariantSelectBuilder, PgEdgeProjection,
    },
    TransactCtx,
};

pub enum QueryFrame {
    Row(RowValue),
    Footer(Option<Box<SubSequence>>),
}

#[derive(Default)]
pub(super) struct QueryBuildCtx<'a> {
    pub alias: sql::Alias,
    pub with_def_aliases: FnvHashMap<DefId, sql::Alias>,
    pub with_queries: Vec<sql::WithQuery<'a>>,
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

#[derive(Clone, Copy)]
pub enum QuerySelect<'a> {
    Unit,
    Struct(&'a FnvHashMap<PropId, Select>),
    Field(PropId),
}

pub enum IncludeJoinedAttrs {
    Yes,
    No,
}

impl<'a> TransactCtx<'a> {
    pub async fn query_vertex<'s>(
        &'s self,
        entity_select: &'s EntitySelect,
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

        self.query(
            struct_select.def_id,
            entity_select.include_total_len,
            entity_select.limit,
            after_cursor,
            None,
            Some(QuerySelect::Struct(&struct_select.properties)),
        )
        .await
    }

    pub async fn query<'s>(
        &'s self,
        def_id: DefId,
        include_total_len: bool,
        limit: usize,
        after_cursor: Option<Cursor>,
        native_id_condition: Option<PgDataKey>,
        query_select: Option<QuerySelect<'s>>,
    ) -> DomainResult<impl Stream<Item = DomainResult<QueryFrame>> + 's> {
        debug!("after cursor: {after_cursor:?}");

        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.package_id(), def_id)?;

        let mut row_layout: Vec<Layout> = vec![];
        let mut ctx = QueryBuildCtx::default();

        let mut sql_select = {
            let mut expressions = sql::Expressions {
                items: vec![],
                multiline: true,
            };

            if include_total_len {
                expressions.items.push(sql::Expr::CountStarOver);
                row_layout.push(Layout::Scalar(PgType::BigInt));
            }

            let (from, _alias, tail_expressions) = match query_select {
                Some(QuerySelect::Struct(properties)) => {
                    self.sql_select_vertex_expressions_with_alias(def_id, properties, pg, &mut ctx)?
                }
                Some(QuerySelect::Field(prop_id)) => {
                    let mut fields: Vec<_> = self.initial_standard_data_fields(pg).into();
                    fields.push(sql::Expr::path1(
                        pg.table.column(&prop_id)?.col_name.as_ref(),
                    ));

                    (pg.table_name().into(), sql::Alias(0), fields)
                }
                Some(QuerySelect::Unit) | None => {
                    let fields: Vec<_> = self.initial_standard_data_fields(pg).into();
                    (pg.table_name().into(), sql::Alias(0), fields)
                }
            };
            expressions.items.extend(tail_expressions);

            sql::Select {
                with: ctx.with(),
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

        let mut select_params: Vec<SqlVal> = vec![];

        if let Some(native_id_condition) = native_id_condition {
            sql_select.where_ = Some(sql::Expr::eq(sql::Expr::path1("_key"), sql::Expr::param(0)));
            select_params.push(SqlVal::I64(native_id_condition));
        }

        let sql = sql_select.to_string();
        debug!("{sql}");
        trace!("{select_params:?}");

        let row_stream = self
            .client()
            .query_raw(&sql, &select_params)
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

                if include_total_len {
                    // must read this for every row if include_total_len was true
                    total_len = Some(
                        row_iter
                            .next_field(&Layout::Scalar(PgType::BigInt))?.into_i64()? as usize
                    );
                }

                if observed_rows < limit {
                    let row_value = self.read_row_value_as_vertex(row_iter, query_select, IncludeJoinedAttrs::Yes, DataOperation::Queried)?;
                    trace!("query returned row value {:?}", row_value.value);
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

    pub(super) fn sql_select_vertex_expressions_with_alias(
        &self,
        def_id: DefId,
        properties: &FnvHashMap<PropId, Select>,
        pg: PgDomainTable<'a>,
        ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<(sql::FromItem<'a>, sql::Alias, Vec<sql::Expr<'a>>)> {
        let def = self.ontology.def(def_id);

        // select data properties
        let mut sql_expressions = vec![];
        let data_alias = self.select_inherent_fields_as_alias(def, pg, ctx)?;
        sql_expressions.push(sql::Expr::path2(data_alias, sql::PathSegment::Asterisk));

        self.sql_select_abstract_properties(
            def,
            data_alias,
            properties,
            pg,
            ctx,
            &mut sql_expressions,
        )?;

        self.sql_select_edge_properties(
            def,
            data_alias,
            properties,
            pg,
            ctx,
            &mut sql_expressions,
        )?;

        Ok((
            sql::FromItem::Alias(data_alias),
            data_alias,
            sql_expressions,
        ))
    }

    fn sql_select_abstract_properties(
        &self,
        def: &Def,
        parent_alias: sql::Alias,
        // currently abstract properties are selected unconditionally
        _properties: &FnvHashMap<PropId, Select>,
        pg: PgDomainTable<'a>,
        ctx: &mut QueryBuildCtx<'a>,
        output: &mut Vec<sql::Expr<'a>>,
    ) -> DomainResult<()> {
        // TODO: The abstract props can be cached in PgTable
        for (prop_id, rel) in &def.data_relationships {
            match &rel.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    let Some(prop_key) = pg.table.find_abstract_property(prop_id) else {
                        continue;
                    };

                    let sub_def_ids = match &rel.target {
                        DataRelationshipTarget::Unambiguous(def_id) => std::slice::from_ref(def_id),
                        DataRelationshipTarget::Union(union_def_id) => {
                            self.ontology.union_variants(*union_def_id)
                        }
                    };

                    let mut union_operands: Vec<sql::Expr> = vec![];

                    for sub_def_id in sub_def_ids.iter().copied() {
                        let sub_def = self.ontology.def(sub_def_id);
                        let sub_pg = self
                            .pg_model
                            .pg_domain_datatable(sub_def_id.package_id(), sub_def_id)?;
                        let sub_alias = ctx.alias.incr();

                        let mut record_items: Vec<_> =
                            self.initial_standard_data_fields(sub_pg).into();

                        self.select_inherent_struct_fields(
                            sub_def,
                            sub_pg.table,
                            &mut record_items,
                            Some(sub_alias),
                        )?;

                        self.sql_select_abstract_properties(
                            sub_def,
                            sub_alias,
                            &Default::default(),
                            sub_pg,
                            ctx,
                            &mut record_items,
                        )?;

                        union_operands.push(
                            sql::Select {
                                with: None,
                                expressions: sql::Expressions {
                                    items: vec![sql::Expr::Row(record_items)],
                                    multiline: false,
                                },
                                from: vec![FromItem::TableNameAs(
                                    sub_pg.table_name(),
                                    sql::Name::Alias(sub_alias),
                                )],
                                where_: Some(sql::Expr::And(vec![
                                    sql::Expr::eq(
                                        sql::Expr::path2(sub_alias, "_fprop"),
                                        sql::Expr::LiteralInt(prop_key),
                                    ),
                                    sql::Expr::eq(
                                        sql::Expr::path2(sub_alias, "_fkey"),
                                        sql::Expr::path2(parent_alias, "_key"),
                                    ),
                                ])),
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

                        output.push(sql::Expr::paren(sql::Expr::Limit(
                            Box::new(union_expr),
                            sql::Limit {
                                // TODO: Only limit if it's not "matrix"?
                                limit: Some(1),
                                offset: None,
                            },
                        )));
                    }
                }
                DataRelationshipKind::Edge(_) => {}
            }
        }

        Ok(())
    }

    pub fn sql_select_edge_properties(
        &self,
        def: &Def,
        data_alias: sql::Alias,
        properties: &FnvHashMap<PropId, Select>,
        pg: PgDomainTable<'a>,
        ctx: &mut QueryBuildCtx<'a>,
        output: &mut Vec<sql::Expr<'a>>,
    ) -> DomainResult<()> {
        for (prop_id, select) in properties {
            let Some(rel_info) = def.data_relationships.get(prop_id) else {
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
        def: &Def,
        pg: PgDomainTable<'a>,
        ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<sql::Alias> {
        if let Some(with_alias) = ctx.with_def_aliases.get(&def.id) {
            Ok(*with_alias)
        } else {
            let with_alias = ctx.alias.incr();

            let mut expressions = sql::Expressions {
                items: self.initial_standard_data_fields(pg).into(),
                multiline: false,
            };

            self.select_inherent_struct_fields(def, pg.table, &mut expressions.items, None)?;

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

    pub fn read_row_value_as_vertex<'b>(
        &self,
        mut iterator: impl SqlRecordIterator<'b>,
        query_select: Option<QuerySelect>,
        include_joined_attrs: IncludeJoinedAttrs,
        op: DataOperation,
    ) -> DomainResult<RowValue> {
        let def_key = iterator
            .next_field(&Layout::Scalar(PgType::Integer))?
            .into_i32()?;

        let Some(PgTableKey::Data { pkg_id, def_id }) =
            self.pg_model.reg_key_to_table_key.get(&def_key)
        else {
            return Err(PgError::InvalidDynamicDataType(def_key).into());
        };
        let def = self.ontology.def(*def_id);

        let data_key = iterator
            .next_field(&Layout::Scalar(PgType::BigInt))?
            .into_i64()?;

        match query_select {
            Some(QuerySelect::Unit) => Ok(RowValue {
                value: Value::unit(),
                def_key,
                data_key,
                op,
            }),
            Some(QuerySelect::Struct(properties)) => {
                let mut attrs: FnvHashMap<PropId, Attr> = FnvHashMap::with_capacity_and_hasher(
                    def.data_relationships.len(),
                    Default::default(),
                );

                // retrieve data properties
                self.read_inherent_struct_fields(def, &mut iterator, &mut attrs)?;

                if matches!(&include_joined_attrs, IncludeJoinedAttrs::Yes) {
                    // retrieve abstract properties
                    let pg = self.pg_model.pg_domain_datatable(*pkg_id, *def_id)?;

                    for (prop_id, rel) in &def.data_relationships {
                        match &rel.kind {
                            DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                                if pg.table.find_abstract_property(prop_id).is_none() {
                                    continue;
                                }

                                if let Some(sql_val) =
                                    iterator.next_field(&Layout::Record)?.null_filter()
                                {
                                    let sql_record = sql_val.into_record()?;
                                    let row_value = self.read_row_value_as_vertex(
                                        sql_record.fields(),
                                        Some(QuerySelect::Struct(&Default::default())),
                                        IncludeJoinedAttrs::Yes,
                                        DataOperation::Queried,
                                    )?;

                                    attrs.insert(*prop_id, Attr::Unit(row_value.value));
                                }
                            }
                            DataRelationshipKind::Edge(_) => {}
                        }
                    }

                    self.read_edge_attributes(&mut iterator, def, properties, &mut attrs)?;
                }

                let value = Value::Struct(Box::new(attrs), def.id.into());

                Ok(RowValue {
                    value,
                    def_key,
                    data_key,
                    op,
                })
            }
            Some(QuerySelect::Field(prop_id)) => {
                let field_value = self
                    .read_field(
                        def.data_relationships
                            .get(&prop_id)
                            .ok_or(PgModelError::NonExistentField(prop_id))?,
                        &mut iterator,
                    )?
                    .ok_or(PgError::MissingField(prop_id))?;

                Ok(RowValue {
                    value: field_value,
                    def_key,
                    data_key,
                    op,
                })
            }
            None => Ok(RowValue {
                value: Value::Void(DefId::unit().into()),
                def_key,
                data_key,
                op,
            }),
        }
    }

    pub fn read_edge_attributes<'b>(
        &self,
        record_iter: &mut impl SqlRecordIterator<'b>,
        def: &Def,
        properties: &FnvHashMap<PropId, Select>,
        attrs: &mut FnvHashMap<PropId, Attr>,
    ) -> DomainResult<()> {
        for (prop_id, select) in properties {
            let Some(rel_info) = def.data_relationships.get(prop_id) else {
                continue;
            };
            let DataRelationshipKind::Edge(proj) = &rel_info.kind else {
                continue;
            };

            let pg_edge = self.pg_model.edgetable(&proj.id)?;

            match rel_info.cardinality.1 {
                ValueCardinality::Unit => {
                    if let Some(sql_edge_tuple) =
                        record_iter.next_field(&Layout::Record)?.null_filter()
                    {
                        let sql_edge_tuple = sql_edge_tuple.into_record()?;
                        let attr = self.read_edge_tuple_as_attr(sql_edge_tuple, select, pg_edge)?;
                        attrs.insert(*prop_id, attr);
                    }
                }
                ValueCardinality::IndexSet | ValueCardinality::List => {
                    let sql_array = record_iter.next_field(&Layout::Array)?.into_array()?;

                    let mut matrix = AttrMatrix::default();
                    matrix.columns.push(Default::default());

                    for result in sql_array.elements(&Layout::Record) {
                        let sql_edge_tuple = result?.into_record()?;

                        match self.read_edge_tuple_as_attr(sql_edge_tuple, select, pg_edge)? {
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
        select: &Select,
        pg_edge: &PgTable,
    ) -> DomainResult<Attr> {
        let mut fields = sql_edge_tuple.fields();

        let sql_field = fields.next_field(&Layout::Record)?;
        let value = self.read_record(sql_field, select)?;

        if sql_edge_tuple.field_count() > 1 {
            // handle parameters
            let sql_field = fields.next_field(&Layout::Record)?;

            for cardinal in pg_edge.edge_cardinals.values() {
                if let PgEdgeCardinalKind::Parameters(def_id) = &cardinal.kind {
                    let def = self.ontology.def(*def_id);
                    let mut attrs: FnvHashMap<PropId, Attr> = FnvHashMap::with_capacity_and_hasher(
                        def.data_relationships.len(),
                        Default::default(),
                    );

                    let struct_record = sql_field.into_record()?;

                    // retrieve data properties
                    self.read_inherent_struct_fields(def, &mut struct_record.fields(), &mut attrs)?;

                    return Ok(Attr::Tuple(Box::new(EndoTuple {
                        elements: smallvec![
                            value,
                            Value::Struct(Box::new(attrs), (*def_id).into())
                        ],
                    })));
                }
            }

            Err(PgError::EdgeParametersMissing.into())
        } else {
            Ok(Attr::Unit(value))
        }
    }

    fn read_record(&self, sql_val: SqlVal, select: &Select) -> DomainResult<Value> {
        let sql_record = sql_val.into_record()?;
        let def_key = sql_record.def_key()?;
        let (_pkg_id, def_id) = self.pg_model.datatable_key_by_def_key(def_key)?;
        let pg_def = self.lookup_def(def_id)?;

        match select {
            Select::Leaf => {
                let Some(entity) = pg_def.def.entity() else {
                    return Err(PgInputError::NotAnEntity.into());
                };

                let pg_id = pg_def.pg.table.column(&entity.id_prop)?;
                let mut fields = sql_record.fields();
                fields.next_field(&Layout::Scalar(PgType::Integer))?;
                let sql_field = fields.next_field(&Layout::Scalar(pg_id.pg_type))?;

                self.deserialize_sql(entity.id_value_def_id, sql_field)
            }
            Select::Struct(struct_select) => {
                self.read_record_as_struct(sql_record, &struct_select.properties)
            }
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => {
                    self.read_record_as_struct(sql_record, &struct_select.properties)
                }
                StructOrUnionSelect::Union(_, variants) => {
                    let actual_select = variants
                        .iter()
                        .find(|sel| sel.def_id == def_id)
                        .ok_or(PgInputError::UnionVariantNotFound)?;

                    self.read_record_as_struct(sql_record, &actual_select.properties)
                }
            },
            Select::StructUnion(_, variants) => {
                let actual_select = variants
                    .iter()
                    .find(|sel| sel.def_id == def_id)
                    .ok_or(PgInputError::UnionVariantNotFound)?;

                self.read_record_as_struct(sql_record, &actual_select.properties)
            }
            _ => todo!("unhandled select"),
        }
    }

    fn read_record_as_struct(
        &self,
        sql_record: SqlRecord,
        select_properties: &FnvHashMap<PropId, Select>,
    ) -> DomainResult<Value> {
        let row_value = self.read_row_value_as_vertex(
            sql_record.fields(),
            Some(QuerySelect::Struct(select_properties)),
            IncludeJoinedAttrs::Yes,
            DataOperation::Queried,
        )?;

        Ok(row_value.value)
    }
}
