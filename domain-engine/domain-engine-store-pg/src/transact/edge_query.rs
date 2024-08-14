use domain_engine_core::DomainResult;
use ontol_runtime::{
    ontology::domain::EdgeInfo,
    query::select::{Select, StructOrUnionSelect, StructSelect},
    tuple::CardinalIdx,
    EdgeId,
};
use tracing::debug;

use crate::{
    pg_error::{ds_err, PgInputError},
    pg_model::{PgDomainTable, PgEdgeCardinal, PgEdgeCardinalKind, PgTable},
    sql::{self, Path},
};

use super::{query::QueryBuildCtx, TransactCtx};

#[derive(Clone, Copy, Debug)]
pub enum CardinalSelect<'a> {
    Leaf,
    StructUnion(&'a [StructSelect]),
}

impl<'a> CardinalSelect<'a> {
    pub fn from_select(select: &'a Select) -> Self {
        match select {
            Select::EntityId => Self::Leaf,
            Select::Leaf => Self::Leaf,
            Select::Struct(s) => Self::StructUnion(std::slice::from_ref(s)),
            Select::StructUnion(_, u) => Self::StructUnion(u),
            Select::Entity(e) => match &e.source {
                StructOrUnionSelect::Struct(s) => Self::StructUnion(std::slice::from_ref(s)),
                StructOrUnionSelect::Union(_, u) => Self::StructUnion(u),
            },
        }
    }
}

pub struct PgEdgeProjection<'a> {
    pub id: EdgeId,
    pub subject_index: CardinalIdx,
    pub object_index: CardinalIdx,

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
    #[allow(unused)]
    Parameters { expr: sql::Expr<'a> },
}

impl<'a> TransactCtx<'a> {
    /// produces a cartesian product-UNION ALL using recursion
    pub(super) fn sql_select_edge_cardinals(
        &self,
        cardinal_idx: CardinalIdx,
        pg_proj: &PgEdgeProjection<'a>,
        select: CardinalSelect,
        variant_builder: EdgeUnionVariantSelectBuilder<'a>,
        union_builder: &mut EdgeUnionSelectBuilder<'a>,
        ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<()> {
        let Some(pg_cardinal) = pg_proj.pg_edge.table.edge_cardinals.get(&cardinal_idx) else {
            self.finalize(pg_proj, variant_builder, union_builder);
            return Ok(());
        };

        debug!(
            "select edge {:?} cardinal {cardinal_idx} {select:?}",
            pg_proj.id
        );
        /*
        debug!(
            "pg edge cardinals: {:?}",
            pg_proj.pg_edge.table.edge_cardinals
        );
        */

        match &pg_cardinal.kind {
            PgEdgeCardinalKind::Dynamic { .. } | PgEdgeCardinalKind::Unique { .. } => {
                // potentially skip cardinal
                // FIXME: select more than one non-parameter cardinal, but CardinalSelect model has to improve
                if cardinal_idx == pg_proj.subject_index || cardinal_idx != pg_proj.object_index {
                    return self.sql_select_edge_cardinals(
                        next_cardinal_idx(cardinal_idx),
                        pg_proj,
                        select,
                        variant_builder,
                        union_builder,
                        ctx,
                    );
                }

                match select {
                    CardinalSelect::Leaf => {
                        let edge_cardinal = pg_proj
                            .edge_info
                            .cardinals
                            .get(cardinal_idx.0 as usize)
                            .unwrap();

                        if edge_cardinal.target.len() != 1 {
                            todo!("union leaf");
                        };

                        let target_def_id = *edge_cardinal.target.iter().next().unwrap();
                        let pg_def = self.lookup_def(target_def_id)?;

                        let Some(target_entity) = pg_def.def.entity() else {
                            return Err(PgInputError::NotAnEntity.into());
                        };

                        let pg_id = pg_def.pg.table.field(&target_entity.id_relationship_id)?;
                        let leaf_alias = ctx.alias.incr();

                        self.sql_select_edge_cardinals(
                            next_cardinal_idx(cardinal_idx),
                            pg_proj,
                            select,
                            variant_builder.append(EdgeUnionCardinalVariantSelect::Vertex {
                                expr: sql::Expr::Row(vec![
                                    sql::Expr::LiteralInt(pg_def.pg.table.key),
                                    sql::Expr::path2(leaf_alias, pg_id.col_name.as_ref()),
                                ]),
                                // expr: sql::Expr::path2(leaf_alias, pg_id.col_name.as_ref()),
                                from: pg_def.pg.table_name().as_(leaf_alias),
                                join_condition: edge_join_condition(
                                    Path::from_iter([pg_proj.edge_alias.into()]),
                                    pg_cardinal,
                                    pg_def.pg.table,
                                    sql::Expr::path2(leaf_alias, "_key"),
                                ),
                                where_condition: Some(sql::Expr::arc(edge_join_condition(
                                    Path::from_iter([pg_proj.edge_alias.into()]),
                                    pg_proj.pg_subj_cardinal,
                                    pg_proj.pg_subj_data.table,
                                    sql::Expr::path2(pg_proj.subj_alias, "_key"),
                                ))),
                            }),
                            union_builder,
                            ctx,
                        )?;
                    }
                    CardinalSelect::StructUnion(union) => {
                        let cardinal_idx = pg_proj.object_index;

                        for struct_select in union {
                            let target_def_id = struct_select.def_id;
                            let pg_def = self.lookup_def(struct_select.def_id)?;

                            let (from, vertex_alias, expressions) = self
                                .sql_select_vertex_expressions(
                                    target_def_id,
                                    &struct_select.properties,
                                    pg_def.pg,
                                    ctx,
                                )?;
                            self.sql_select_edge_cardinals(
                                next_cardinal_idx(cardinal_idx),
                                pg_proj,
                                select,
                                variant_builder.append(EdgeUnionCardinalVariantSelect::Vertex {
                                    expr: sql::Expr::arc(sql::Expr::Row(expressions)),
                                    from,
                                    join_condition: edge_join_condition(
                                        Path::from_iter([pg_proj.edge_alias.into()]),
                                        pg_cardinal,
                                        pg_def.pg.table,
                                        sql::Expr::path2(vertex_alias, "_key"),
                                    ),
                                    where_condition: Some(sql::Expr::arc(edge_join_condition(
                                        Path::from_iter([pg_proj.edge_alias.into()]),
                                        pg_proj.pg_subj_cardinal,
                                        pg_proj.pg_subj_data.table,
                                        sql::Expr::path2(pg_proj.subj_alias, "_key"),
                                    ))),
                                }),
                                union_builder,
                                ctx,
                            )?;
                        }
                    }
                }
            }
            PgEdgeCardinalKind::Parameters(_) => {
                let edge_cardinal = pg_proj
                    .edge_info
                    .cardinals
                    .get(cardinal_idx.0 as usize)
                    .ok_or_else(|| ds_err("no edge cardinal for parameters"))?;

                let params_def_id = *edge_cardinal
                    .target
                    .iter()
                    .next()
                    .ok_or_else(|| ds_err("no target for edge cardinal"))?;
                let params_def = self.ontology.def(params_def_id);

                let mut sql_expressions = vec![];
                self.select_inherent_struct_fields(
                    params_def,
                    pg_proj.pg_edge.table,
                    &mut sql_expressions,
                    Some(pg_proj.edge_alias),
                )?;

                return self.sql_select_edge_cardinals(
                    next_cardinal_idx(cardinal_idx),
                    pg_proj,
                    select,
                    variant_builder.append(EdgeUnionCardinalVariantSelect::Parameters {
                        expr: sql::Expr::arc(sql::Expr::Row(sql_expressions)),
                    }),
                    union_builder,
                    ctx,
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
    cardinal: &'a PgEdgeCardinal,
    data: &'a PgTable,
    data_key_expr: sql::Expr<'a>,
) -> sql::Expr<'a> {
    match &cardinal.kind {
        PgEdgeCardinalKind::Dynamic {
            def_col_name,
            key_col_name,
        } => sql::Expr::And(vec![
            sql::Expr::eq(
                edge_path.join(def_col_name.as_ref()),
                sql::Expr::LiteralInt(data.key),
            ),
            sql::Expr::eq(edge_path.join(key_col_name.as_ref()), data_key_expr),
        ]),
        PgEdgeCardinalKind::Unique { key_col_name, .. } => {
            sql::Expr::eq(edge_path.join(key_col_name.as_ref()), data_key_expr)
        }
        PgEdgeCardinalKind::Parameters(_params_def_id) => unreachable!(),
    }
}

fn next_cardinal_idx(idx: CardinalIdx) -> CardinalIdx {
    CardinalIdx(idx.0 + 1)
}
