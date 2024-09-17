use domain_engine_core::DomainResult;
use ontol_runtime::ontology::domain::EdgeInfo;
use tracing::trace;

use crate::{
    pg_error::{PgInputError, PgModelError},
    pg_model::{EdgeId, PgDomainTable, PgEdgeCardinal, PgEdgeCardinalKind, PgTable},
    sql,
    transact::query_select::{QuerySelect, QuerySelectRef},
};

use super::{cache::PgCache, query::QueryBuildCtx, query_select::CardinalSelect, TransactCtx};

pub struct PgEdgeProjection<'a> {
    pub id: EdgeId,

    pub pg_subj_data: PgDomainTable<'a>,
    pub pg_subj_cardinal: &'a PgEdgeCardinal,
    pub subj_alias: sql::Alias,

    pub edge_info: &'a EdgeInfo,
    pub pg_edge: PgDomainTable<'a>,
    pub edge_alias: sql::Alias,
}

#[derive(Default)]
pub struct EdgeUnionSelectBuilder<'a> {
    pub union_exprs: Vec<sql::Expr<'a>>,
}

#[derive(Default)]
pub struct EdgeUnionVariantSelectBuilder<'a> {
    cardinals: Vec<EdgeUnionCardinalVariantSelect<'a>>,
}

impl<'a> EdgeUnionVariantSelectBuilder<'a> {
    fn append(&self, cardinal_variant: EdgeUnionCardinalVariantSelect<'a>) -> Self {
        let mut cardinals = self.cardinals.clone();
        cardinals.push(cardinal_variant);
        Self { cardinals }
    }
}

#[derive(Clone)]
pub enum EdgeUnionCardinalVariantSelect<'a> {
    Vertex {
        expr: sql::Expr<'a>,
        from: sql::FromItem<'a>,
        join_condition: sql::Expr<'a>,
        where_condition: Option<sql::Expr<'a>>,
    },
    VertexAddress {
        expr: sql::Expr<'a>,
        where_condition: Option<sql::Expr<'a>>,
    },
    Parameters {
        expr: sql::Expr<'a>,
    },
}

impl<'a> TransactCtx<'a> {
    /// produces a cartesian product-UNION ALL using recursion
    pub(super) fn sql_select_edge_cardinals<'b>(
        &self,
        pg_proj: &PgEdgeProjection<'a>,
        cardinal_selects: &[CardinalSelect],
        index_level: usize,
        variant_builder: EdgeUnionVariantSelectBuilder<'a>,
        union_builder: &mut EdgeUnionSelectBuilder<'a>,
        query_ctx: &mut QueryBuildCtx<'a>,
        cache: &mut PgCache,
    ) -> DomainResult<()> {
        let Some(cardinal_select) = cardinal_selects.get(index_level) else {
            self.finalize(pg_proj, variant_builder, union_builder);
            return Ok(());
        };
        let cardinal_idx = cardinal_select.cardinal_idx;

        let Some(pg_cardinal) = pg_proj
            .pg_edge
            .table
            .edge_cardinals
            .get(&cardinal_select.cardinal_idx)
        else {
            panic!()
        };

        trace!(
            "select edge {:?} cardinal {cardinal_idx} {cardinal_select:?}",
            pg_proj.id
        );
        /*
        trace!(
            "pg edge cardinals: {:?}",
            pg_proj.pg_edge.table.edge_cardinals
        );
        */

        match &pg_cardinal.kind {
            PgEdgeCardinalKind::Dynamic { .. } | PgEdgeCardinalKind::PinnedDef { .. } => {
                match cardinal_select.select.as_ref() {
                    QuerySelectRef::VertexAddress => {
                        let edge_path = sql::Path::from_iter([pg_proj.edge_alias.into()]);

                        let expr = match &pg_cardinal.kind {
                            PgEdgeCardinalKind::Dynamic {
                                def_col_name,
                                key_col_name,
                            } => sql::Expr::Row(vec![
                                edge_path.join(def_col_name.as_ref()).into(),
                                edge_path.join(key_col_name.as_ref()).into(),
                            ]),
                            PgEdgeCardinalKind::PinnedDef {
                                pinned_def_id,
                                key_col_name,
                            } => {
                                let pinned_table = self
                                    .pg_model
                                    .datatable(pinned_def_id.domain_index(), *pinned_def_id)?;

                                sql::Expr::Row(vec![
                                    sql::Expr::LiteralInt(pinned_table.key),
                                    edge_path.join(key_col_name.as_ref()).into(),
                                ])
                            }
                            PgEdgeCardinalKind::Parameters(_) => {
                                return Err(PgModelError::NoAddressInCardinal.into());
                            }
                        };

                        self.sql_select_edge_cardinals(
                            pg_proj,
                            cardinal_selects,
                            index_level + 1,
                            variant_builder.append(EdgeUnionCardinalVariantSelect::VertexAddress {
                                expr,
                                where_condition: Some(sql::Expr::arc(edge_join_condition(
                                    sql::Path::from_iter([pg_proj.edge_alias.into()]),
                                    pg_proj.pg_subj_cardinal,
                                    pg_proj.pg_subj_data.table,
                                    sql::Expr::path2(pg_proj.subj_alias, "_key"),
                                ))),
                            }),
                            union_builder,
                            query_ctx,
                            cache,
                        )?;
                    }
                    QuerySelectRef::Unit | QuerySelectRef::EntityId => {
                        let edge_cardinal = pg_proj
                            .edge_info
                            .cardinals
                            .get(cardinal_idx.0 as usize)
                            .unwrap();

                        for target_def_id in edge_cardinal.target.iter() {
                            let pg_def = self.lookup_def(*target_def_id)?;

                            let Some(target_entity) = pg_def.def.entity() else {
                                return Err(PgInputError::NotAnEntity.into());
                            };

                            let pg_id = pg_def.pg.table.column(&target_entity.id_prop)?;
                            let leaf_alias = query_ctx.alias.incr();

                            self.sql_select_edge_cardinals(
                                pg_proj,
                                cardinal_selects,
                                index_level + 1,
                                variant_builder.append(EdgeUnionCardinalVariantSelect::Vertex {
                                    expr: sql::Expr::Row(vec![
                                        sql::Expr::LiteralInt(pg_def.pg.table.key),
                                        sql::Expr::path2(leaf_alias, pg_id.col_name.as_ref()),
                                    ]),
                                    from: pg_def.pg.table_name().as_(leaf_alias),
                                    join_condition: edge_join_condition(
                                        sql::Path::from_iter([pg_proj.edge_alias.into()]),
                                        pg_cardinal,
                                        pg_def.pg.table,
                                        sql::Expr::path2(leaf_alias, "_key"),
                                    ),
                                    where_condition: Some(sql::Expr::arc(edge_join_condition(
                                        sql::Path::from_iter([pg_proj.edge_alias.into()]),
                                        pg_proj.pg_subj_cardinal,
                                        pg_proj.pg_subj_data.table,
                                        sql::Expr::path2(pg_proj.subj_alias, "_key"),
                                    ))),
                                }),
                                union_builder,
                                query_ctx,
                                cache,
                            )?;
                        }
                    }
                    QuerySelectRef::VertexUnion(union) => {
                        for vertex_select in union {
                            let target_def_id = vertex_select.def_id;
                            let pg_def = self.lookup_def(vertex_select.def_id)?;

                            let (from, vertex_alias, expressions) = self
                                .sql_select_vertex_expressions_with_alias(
                                    target_def_id,
                                    &vertex_select,
                                    pg_def.pg,
                                    query_ctx,
                                    cache,
                                )?;
                            self.sql_select_edge_cardinals(
                                pg_proj,
                                cardinal_selects,
                                index_level + 1,
                                variant_builder.append(EdgeUnionCardinalVariantSelect::Vertex {
                                    expr: sql::Expr::arc(sql::Expr::Row(expressions)),
                                    from,
                                    join_condition: edge_join_condition(
                                        sql::Path::from_iter([pg_proj.edge_alias.into()]),
                                        pg_cardinal,
                                        pg_def.pg.table,
                                        sql::Expr::path2(vertex_alias, "_key"),
                                    ),
                                    where_condition: Some(sql::Expr::arc(edge_join_condition(
                                        sql::Path::from_iter([pg_proj.edge_alias.into()]),
                                        pg_proj.pg_subj_cardinal,
                                        pg_proj.pg_subj_data.table,
                                        sql::Expr::path2(pg_proj.subj_alias, "_key"),
                                    ))),
                                }),
                                union_builder,
                                query_ctx,
                                cache,
                            )?;
                        }
                    }
                }
            }
            PgEdgeCardinalKind::Parameters(_) => {
                let mut sql_expressions = vec![];
                if let QuerySelect::Vertex(vertex_select) = &cardinal_select.select {
                    self.select_inherent_vertex_fields(
                        pg_proj.pg_edge.table,
                        vertex_select,
                        &mut sql_expressions,
                        Some(pg_proj.edge_alias),
                    );
                }

                return self.sql_select_edge_cardinals(
                    pg_proj,
                    cardinal_selects,
                    index_level + 1,
                    variant_builder.append(EdgeUnionCardinalVariantSelect::Parameters {
                        expr: sql::Expr::arc(sql::Expr::Row(sql_expressions)),
                    }),
                    union_builder,
                    query_ctx,
                    cache,
                );
            }
        }

        Ok(())
    }

    fn finalize(
        &self,
        pg_proj: &PgEdgeProjection<'a>,
        variant_builder: EdgeUnionVariantSelectBuilder<'a>,
        union_builder: &mut EdgeUnionSelectBuilder<'a>,
    ) {
        let mut row_tuple = Vec::with_capacity(variant_builder.cardinals.len());
        let mut final_from: sql::FromItem = sql::TableName(
            &pg_proj.pg_edge.domain.schema_name,
            &pg_proj.pg_edge.table.table_name,
        )
        .as_(pg_proj.edge_alias);
        let mut where_conjunctions: Vec<sql::Expr> = vec![];

        for cardinal in variant_builder.cardinals {
            match cardinal {
                EdgeUnionCardinalVariantSelect::Vertex {
                    expr,
                    from,
                    join_condition,
                    where_condition,
                } => {
                    row_tuple.push(expr);
                    final_from = sql::FromItem::Join(Box::new(sql::Join {
                        first: final_from,
                        second: from,
                        on: join_condition,
                    }));
                    where_conjunctions.extend(where_condition);
                }
                EdgeUnionCardinalVariantSelect::VertexAddress {
                    expr,
                    where_condition,
                } => {
                    row_tuple.push(expr);
                    where_conjunctions.extend(where_condition);
                }
                EdgeUnionCardinalVariantSelect::Parameters { expr } => {
                    row_tuple.push(expr);
                }
            }
        }

        union_builder.union_exprs.push(
            sql::Select {
                with: None,
                expressions: sql::Expressions {
                    items: vec![sql::Expr::Row(row_tuple)],
                    multiline: false,
                },
                from: vec![final_from],
                where_: if where_conjunctions.is_empty() {
                    None
                } else {
                    Some(sql::Expr::And(where_conjunctions))
                },
                ..Default::default()
            }
            .into(),
        );
    }
}

pub fn edge_join_condition<'a>(
    edge_path: sql::Path<'a>,
    pg_cardinal: &'a PgEdgeCardinal,
    data: &'a PgTable,
    data_key_expr: sql::Expr<'a>,
) -> sql::Expr<'a> {
    match &pg_cardinal.kind {
        PgEdgeCardinalKind::Dynamic {
            def_col_name,
            key_col_name,
        } => sql::Expr::eq(
            sql::Expr::Tuple(vec![
                edge_path.join(def_col_name.as_ref()).into(),
                edge_path.join(key_col_name.as_ref()).into(),
            ]),
            sql::Expr::Tuple(vec![sql::Expr::LiteralInt(data.key), data_key_expr]),
        ),
        PgEdgeCardinalKind::PinnedDef { key_col_name, .. } => {
            sql::Expr::eq(edge_path.join(key_col_name.as_ref()), data_key_expr)
        }
        PgEdgeCardinalKind::Parameters(_params_def_id) => unreachable!(),
    }
}
