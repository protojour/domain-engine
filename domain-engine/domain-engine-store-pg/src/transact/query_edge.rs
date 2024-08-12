use std::collections::BTreeMap;

use domain_engine_core::DomainResult;
use ontol_runtime::{
    ontology::domain::EdgeInfo,
    query::select::{Select, StructOrUnionSelect, StructSelect},
    tuple::CardinalIdx,
    EdgeId,
};
use tracing::debug;

use crate::{
    ds_bad_req,
    pg_model::{PgDomainTable, PgEdgeCardinal, PgEdgeCardinalKind, PgTable},
    sql,
    sql_value::Layout,
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
pub struct EdgeUnionSelectBuilder<'d> {
    pub edge_layout: BTreeMap<CardinalIdx, Layout>,
    pub union_exprs: Vec<sql::Expr<'d>>,
}

impl<'d> EdgeUnionSelectBuilder<'d> {
    pub fn set_dyn_record_layout(&mut self, cardinal_idx: CardinalIdx) {
        self.edge_layout
            .entry(cardinal_idx)
            .or_insert_with(|| Layout::Record);
    }
}

#[derive(Default)]
pub struct EdgeUnionVariantSelectBuilder<'d> {
    cardinals: Vec<EdgeUnionCardinalVariantSelect<'d>>,
}

impl<'d> EdgeUnionVariantSelectBuilder<'d> {
    fn append(&self, cardinal_variant: EdgeUnionCardinalVariantSelect<'d>) -> Self {
        let mut cardinals = self.cardinals.clone();
        cardinals.push(cardinal_variant);
        Self { cardinals }
    }
}

#[derive(Clone)]
pub enum EdgeUnionCardinalVariantSelect<'d> {
    Vertex {
        expr: sql::Expr<'d>,
        from: sql::FromItem<'d>,
        join_condition: sql::Expr<'d>,
        where_condition: Option<sql::Expr<'d>>,
    },
    #[allow(unused)]
    Parameters { expr: sql::Expr<'d> },
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

                union_builder.set_dyn_record_layout(cardinal_idx);

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
                            return Err(ds_bad_req("cannot select id from non-entity"));
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
                                    pg_proj.edge_alias,
                                    pg_cardinal,
                                    leaf_alias,
                                    pg_def.pg.table,
                                ),
                                where_condition: Some(sql::Expr::arc(edge_join_condition(
                                    pg_proj.edge_alias,
                                    pg_proj.pg_subj_cardinal,
                                    pg_proj.subj_alias,
                                    pg_proj.pg_subj_data.table,
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

                            let (from, vertex_alias, expressions) = self.sql_select_expressions(
                                target_def_id,
                                struct_select,
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
                                        pg_proj.edge_alias,
                                        pg_cardinal,
                                        vertex_alias,
                                        pg_def.pg.table,
                                    ),
                                    where_condition: Some(sql::Expr::arc(edge_join_condition(
                                        pg_proj.edge_alias,
                                        pg_proj.pg_subj_cardinal,
                                        pg_proj.subj_alias,
                                        pg_proj.pg_subj_data.table,
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
                union_builder.set_dyn_record_layout(cardinal_idx);

                todo!("parameters");
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
                EdgeUnionCardinalVariantSelect::Parameters { .. } => {}
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

fn edge_join_condition<'d>(
    edge_alias: sql::Alias,
    cardinal: &'d PgEdgeCardinal,
    data_alias: sql::Alias,
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

fn next_cardinal_idx(idx: CardinalIdx) -> CardinalIdx {
    CardinalIdx(idx.0 + 1)
}
