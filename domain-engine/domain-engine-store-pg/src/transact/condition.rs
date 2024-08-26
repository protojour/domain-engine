use domain_engine_core::{filter::walker::ConditionWalker, DomainResult};
use ontol_runtime::{
    ontology::domain::{DataRelationshipInfo, DataRelationshipKind, Def, EdgeCardinalProjection},
    query::condition::{Clause, CondTerm, SetOperator},
    tuple::CardinalIdx,
    var::Var,
    DefId, PropId,
};
use tracing::{debug, error};

use crate::{
    pg_error::PgError,
    pg_model::{PgColumn, PgProperty, PgRegKey},
    sql::{self, WhereExt},
    sql_value::SqlVal,
    transact::{data::Data, edge_query::edge_join_condition},
};

use super::{fields::AbstractKind, query::QueryBuildCtx, TransactCtx};

struct ConditionCtx<'a, 's> {
    root_def_id: DefId,
    root_alias: sql::Alias,
    sql_params: &'s mut Vec<SqlVal<'a>>,
    query_ctx: &'s mut QueryBuildCtx<'a>,
    output_clauses: Vec<sql::Expr<'a>>,
}

#[derive(Clone, Default)]
struct PathBuilder {
    items: Vec<CondPathItem>,
}

impl PathBuilder {
    fn add(&self, item: CondPathItem) -> Self {
        let mut cloned = self.clone();
        cloned.items.push(item);
        cloned
    }

    fn add_cardinal_idx(&self, idx: CardinalIdx) -> Self {
        let mut items = self.items.clone();
        let CondPathItem::EdgeJoin { proj, .. } = items.pop().unwrap();
        items.push(CondPathItem::EdgeJoin { proj, idx });
        Self { items }
    }
}

#[derive(Clone, Debug)]
enum CondPathItem {
    EdgeJoin {
        proj: EdgeCardinalProjection,
        idx: CardinalIdx,
    },
}

impl<'a> TransactCtx<'a> {
    pub fn sql_where_condition(
        &self,
        def_id: DefId,
        root_alias: sql::Alias,
        condition_walker: ConditionWalker,
        sql_params: &mut Vec<SqlVal<'a>>,
        query_ctx: &mut QueryBuildCtx<'a>,
    ) -> DomainResult<Option<sql::Expr>> {
        let mut ctx = ConditionCtx {
            root_def_id: def_id,
            root_alias,
            sql_params,
            query_ctx,
            output_clauses: vec![],
        };

        let path_builder = PathBuilder::default();
        self.traverse_conditions(
            Var(0),
            std::slice::from_ref(&def_id),
            condition_walker,
            path_builder,
            &mut ctx,
        )?;

        let output_clauses = ctx.output_clauses;

        if output_clauses.is_empty() {
            Ok(None)
        } else {
            Ok(Some(sql::Expr::And(output_clauses)))
        }
    }

    fn traverse_conditions(
        &self,
        cond_var: Var,
        def_id_set: &[DefId],
        walker: ConditionWalker,
        path_builder: PathBuilder,
        ctx: &mut ConditionCtx<'a, '_>,
    ) -> DomainResult<()> {
        let def_id = if def_id_set.len() != 1 {
            let mut found_def_id: Option<DefId> = None;

            for clause in walker.clauses(cond_var) {
                if let Clause::IsDef(def_id) = clause {
                    match found_def_id {
                        None => {
                            found_def_id = Some(*def_id);
                        }
                        Some(_) => {
                            return Err(PgError::Condition(
                                "condition ambiguous with regard to def",
                            )
                            .into());
                        }
                    }
                }
            }

            found_def_id.ok_or_else(|| PgError::Condition("ambiguous def"))?
        } else {
            def_id_set.iter().next().copied().unwrap()
        };

        let def = self.ontology.def(def_id);

        debug!("traverse condition {cond_var}");

        for clause in walker.clauses(cond_var) {
            match clause {
                Clause::Root => {}
                Clause::IsDef(pred_def_id) => {
                    if def_id != *pred_def_id {
                        error!("def_id mismatch: {def_id:?} was not {pred_def_id:?}");
                        return Err(PgError::Condition("DefId mismatch").into());
                    }
                }
                Clause::MatchProp(prop_id, set_operator, prop_var) => {
                    self.match_prop(
                        def_id,
                        def,
                        (*prop_id, *set_operator, *prop_var),
                        walker,
                        path_builder.clone(),
                        ctx,
                    )?;
                }
                Clause::Member(_rel, _val) => {
                    return Err(PgError::Condition("weird member term").into())
                }
            }
        }

        Ok(())
    }

    fn match_prop(
        &self,
        def_id: DefId,
        def: &'a Def,
        (prop_id, set_operator, var): (PropId, SetOperator, Var),
        walker: ConditionWalker,
        path_builder: PathBuilder,
        ctx: &mut ConditionCtx<'a, '_>,
    ) -> DomainResult<()> {
        let Some(rel_info) = def.data_relationships.get(&prop_id) else {
            return Ok(());
        };

        let pg = self
            .pg_model
            .pg_domain_datatable(def_id.package_id(), def_id)?;

        match (rel_info.kind, pg.table.properties.get(&prop_id.1)) {
            (
                DataRelationshipKind::Id | DataRelationshipKind::Tree,
                Some(PgProperty::Column(pg_column)),
            ) => {
                debug!("compare column: {:?} in {def_id:?}", path_builder.items);

                if path_builder.items.is_empty() {
                    if let Some(leaf_condition) = self.column_condition(
                        var,
                        ctx.root_alias,
                        pg_column,
                        set_operator,
                        walker,
                        ctx,
                    )? {
                        ctx.output_clauses.push(leaf_condition);
                    }
                } else {
                    let mut path_iter = path_builder.items.iter().rev();

                    let leaf_alias = ctx.query_ctx.alias.incr();
                    let leaf_condition = self.column_condition(
                        var,
                        leaf_alias,
                        pg_column,
                        set_operator,
                        walker,
                        ctx,
                    )?;

                    let mut from_item =
                        sql::FromItem::TableNameAs(pg.table_name(), leaf_alias.into());
                    let mut outer_proj: Option<EdgeCardinalProjection> = None;

                    let mut alias = leaf_alias;

                    while let Some(path) = path_iter.next() {
                        match path {
                            CondPathItem::EdgeJoin { proj, idx } => {
                                let edge_alias = ctx.query_ctx.alias.incr();
                                let pg_edge = self.pg_model.pg_domain_edgetable(&proj.id)?;
                                let pg_cardinal = pg_edge.table.edge_cardinal(*idx)?;
                                from_item = sql::Join {
                                    first: from_item,
                                    second: sql::FromItem::TableNameAs(
                                        pg_edge.table_name(),
                                        edge_alias.into(),
                                    ),
                                    on: edge_join_condition(
                                        sql::Path::from_iter([edge_alias.into()]),
                                        pg_cardinal,
                                        pg.table,
                                        sql::Expr::path2(alias, "_key"),
                                    ),
                                }
                                .into();
                                alias = edge_alias;
                                outer_proj = Some(*proj);
                            }
                        }
                    }

                    let mut sql_select = sql::Select {
                        with: None,
                        expressions: Default::default(),
                        from: vec![from_item],
                        where_: None,
                        limit: sql::Limit::default(),
                    };

                    if let Some(outer_proj) = outer_proj {
                        let pg_edge = self.pg_model.pg_domain_edgetable(&outer_proj.id)?;
                        let pg_cardinal = pg_edge.table.edge_cardinal(outer_proj.subject)?;
                        let pg_outer = self
                            .pg_model
                            .pg_domain_datatable(ctx.root_def_id.package_id(), ctx.root_def_id)?;

                        sql_select.where_and(edge_join_condition(
                            sql::Path::from_iter([alias.into()]),
                            pg_cardinal,
                            pg_outer.table,
                            sql::Expr::path2(ctx.root_alias, "_key"),
                        ));
                    }

                    if let Some(leaf_condition) = leaf_condition {
                        sql_select.where_and(leaf_condition);
                    }

                    ctx.output_clauses
                        .push(sql::Expr::Exists(Box::new(sql_select.into())));
                }
            }
            (
                DataRelationshipKind::Id | DataRelationshipKind::Tree,
                Some(PgProperty::Abstract(_reg_key)),
            ) => {
                let prop_key = pg.table.abstract_property(&prop_id)?;

                if path_builder.items.is_empty() {
                    match set_operator {
                        SetOperator::ElementIn => {
                            let mut or_exprs: Vec<sql::Expr> = vec![];

                            for clause in walker.clauses(var) {
                                if let Clause::Member(_, CondTerm::Variable(mem_var)) = clause {
                                    if let Some(leaf_condition) = self.abstract_leaf_condition(
                                        *mem_var,
                                        prop_key,
                                        ctx.root_alias,
                                        rel_info,
                                        set_operator,
                                        walker,
                                        ctx,
                                    )? {
                                        or_exprs.push(leaf_condition);
                                    }
                                }
                            }

                            if !or_exprs.is_empty() {
                                ctx.output_clauses.push(sql::Expr::Or(or_exprs));
                            }
                        }
                        _ => {
                            return Err(PgError::Condition(
                                "unhandled operator for abstract property",
                            )
                            .into());
                        }
                    }
                } else {
                    return Err(PgError::Condition("abstract property under edge").into());
                }
            }
            (DataRelationshipKind::Edge(proj), None) => {
                let path_builder = path_builder.add(CondPathItem::EdgeJoin {
                    proj,
                    idx: CardinalIdx(0),
                });
                let edge = self.ontology.find_edge(proj.id).unwrap();

                match set_operator {
                    SetOperator::ElementIn => {
                        for clause in walker.clauses(var) {
                            let Clause::Member(rel, val) = clause else {
                                continue;
                            };

                            let params_cardinal = CardinalIdx(edge.cardinals.len() as u8);

                            match rel {
                                CondTerm::Wildcard => {}
                                CondTerm::Variable(var) => {
                                    let target_cardinal = edge.cardinals.last().unwrap();
                                    self.traverse_conditions(
                                        *var,
                                        target_cardinal.target.as_slice(),
                                        walker,
                                        path_builder.add_cardinal_idx(params_cardinal),
                                        ctx,
                                    )?;
                                }
                                CondTerm::Value(_val) => {
                                    let _target_cardinal = edge.cardinals.last().unwrap();
                                    let _builder = path_builder.add_cardinal_idx(params_cardinal);
                                    return Err(PgError::Condition("rel value term").into());
                                }
                            }

                            match val {
                                CondTerm::Wildcard => {}
                                CondTerm::Variable(var) => {
                                    let target_cardinal =
                                        edge.cardinals.get(proj.object.0 as usize).unwrap();

                                    self.traverse_conditions(
                                        *var,
                                        target_cardinal.target.as_slice(),
                                        walker,
                                        path_builder.add_cardinal_idx(proj.object),
                                        ctx,
                                    )?;
                                }
                                CondTerm::Value(_val) => {
                                    let _builder = path_builder.add_cardinal_idx(proj.object);
                                    return Err(PgError::Condition("value term").into());
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(
                            PgError::Condition("unhandled set operator in edge matcher").into()
                        )
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn column_condition(
        &self,
        cond_var: Var,
        leaf_alias: sql::Alias,
        pg_column: &'a PgColumn,
        set_operator: SetOperator,
        walker: ConditionWalker,
        ctx: &mut ConditionCtx<'a, '_>,
    ) -> DomainResult<Option<sql::Expr<'a>>> {
        match set_operator {
            SetOperator::ElementIn => {
                let mut or_exprs: Vec<sql::Expr> = vec![];

                for clause in walker.clauses(cond_var) {
                    match clause {
                        Clause::Member(_, CondTerm::Value(value)) => {
                            let param_idx = ctx.sql_params.len();
                            or_exprs.push(sql::Expr::eq(
                                sql::Expr::path2(leaf_alias, pg_column.col_name.as_ref()),
                                sql::Expr::param(param_idx),
                            ));
                            let Data::Sql(sql_val) = self.data_from_value(value.clone())? else {
                                return Err(PgError::Condition("compound condition value").into());
                            };
                            ctx.sql_params.push(sql_val);
                        }
                        _ => {
                            return Err(PgError::Condition(
                                "leaf condition expected a member clause",
                            )
                            .into())
                        }
                    }
                }

                if !or_exprs.is_empty() {
                    Ok(Some(sql::Expr::Or(or_exprs)))
                } else {
                    Ok(None)
                }
            }
            _ => Err(PgError::Condition("unhandled set operator").into()),
        }
    }

    fn abstract_leaf_condition(
        &self,
        cond_var: Var,
        prop_key: PgRegKey,
        leaf_alias: sql::Alias,
        rel_info: &DataRelationshipInfo,
        _set_operator: SetOperator,
        walker: ConditionWalker,
        ctx: &mut ConditionCtx<'a, '_>,
    ) -> DomainResult<Option<sql::Expr<'a>>> {
        match self.abstract_kind(&rel_info.target) {
            AbstractKind::VertexUnion(_def_ids) => {
                let mut exprs: Vec<sql::Expr> = vec![];

                for clause in walker.clauses(cond_var) {
                    match clause {
                        Clause::Root => {}
                        Clause::IsDef(def_id) => {
                            let pg = self
                                .pg_model
                                .pg_domain_datatable(def_id.package_id(), *def_id)?;
                            if !pg.table.has_fkey {
                                return Err(PgError::Condition("abstract column not usable").into());
                            }

                            let alias = ctx.query_ctx.alias.incr();

                            let sql_select = sql::Select {
                                with: None,
                                expressions: Default::default(),
                                from: vec![pg.table_name().as_(alias)],
                                where_: Some(sql::Expr::eq(
                                    sql::Expr::Tuple(vec![
                                        sql::Expr::path2(alias, "_fprop"),
                                        sql::Expr::path2(alias, "_fkey"),
                                    ]),
                                    sql::Expr::Tuple(vec![
                                        sql::Expr::LiteralInt(prop_key),
                                        sql::Expr::path2(leaf_alias, "_key"),
                                    ]),
                                )),
                                limit: Default::default(),
                            };

                            exprs.push(sql::Expr::Exists(Box::new(sql_select.into())));
                        }
                        Clause::MatchProp(_, _, _) => {
                            todo!("match-prop!");
                        }
                        Clause::Member(_, _) => {
                            todo!("member!");
                        }
                    }
                }

                Ok(if exprs.is_empty() {
                    None
                } else {
                    Some(sql::Expr::And(exprs))
                })
            }
            AbstractKind::Scalar { .. } => {
                Err(PgError::Condition("compare scalar set-to-set").into())
            }
        }
    }
}
