use domain_engine_core::{
    DomainError, DomainResult,
    filter::walker::{ConditionWalker, MemberPredicate, Members},
};
use ontol_runtime::{
    DefId,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget},
    query::{
        condition::{Clause, CondTerm, SetOperator},
        filter::Filter as OntolFilter,
        order::Direction as OntolDirection,
        select::{Select, StructSelect},
    },
    value::{Value, ValueFormatRaw},
    var::Var,
};
use tracing::{debug, error};

use super::aql::*;

#[derive(Clone, Debug)]
struct FilterQuery {
    def_id: DefId,
    comp: Option<Comparison>,
    prop: String,
    values: Vec<String>,
}

impl FilterQuery {
    fn from_context(def_id: DefId) -> Self {
        Self {
            def_id,
            prop: "".to_string(),
            comp: None,
            values: vec![],
        }
    }
}

impl MetaQuery<'_> {
    /// Add an ONTOL Filter to MetaQuery
    pub fn add_filter(&mut self, def_id: DefId, filter: &OntolFilter) -> DomainResult<()> {
        debug!("add filter {filter:#?}");

        let mut filter_query = FilterQuery::from_context(def_id);
        let walker = ConditionWalker::new(filter.condition());

        self.eval_cond_var(&mut filter_query, Var(0), walker, false)?;

        let vertex_order_tuple = filter
            .vertex_order()
            .ok_or_else(|| DomainError::data_store("no vertex order found"))?;
        if vertex_order_tuple.is_empty() {
            return Ok(());
        }

        let def = self.ontology.def(def_id);
        let mut sorts = vec![];

        for order in vertex_order_tuple {
            for field_path in order.tuple.iter() {
                let mut path = vec![self.var.to_string()];

                for prop_id in field_path.0.iter() {
                    if let Some(rel_info) = def.data_relationships.get(prop_id) {
                        let prop_name = self.ontology[rel_info.name].to_string();
                        path.push(prop_name);
                    }
                }

                sorts.push(SortExpression {
                    var: path.join("."),
                    direction: match filter.direction().chain(order.direction) {
                        OntolDirection::Ascending => SortDirection::Asc,
                        OntolDirection::Descending => SortDirection::Desc,
                    },
                });
            }
        }

        if !sorts.is_empty() {
            self.ops.push(Operation::Sort(Sort { sorts }));
        }

        Ok(())
    }

    /// Evaluate condition, adding Filters to MetaQuery
    fn eval_cond_var(
        &mut self,
        filter: &mut FilterQuery,
        cond_var: Var,
        walker: ConditionWalker,
        join: bool,
    ) -> DomainResult<()> {
        for clause in walker.clauses(cond_var) {
            match clause {
                Clause::IsDef(var_def_id) => {
                    if *var_def_id == filter.def_id {
                        continue;
                    }

                    let collection = self
                        .database
                        .collections
                        .get(&filter.def_id)
                        .expect("collection should exist");

                    self.ops.push(Operation::Filter(Filter {
                        var: Expr::Complex(format!(
                            "IS_SAME_COLLECTION({}, {})",
                            collection, self.var
                        )),
                        ..Default::default()
                    }));
                }
                Clause::MatchProp(prop_id, set_op, set_var) => {
                    let def = self.ontology.def(filter.def_id);
                    let rel_info = def.data_relationships.get(prop_id).unwrap_or_else(|| {
                        error!("data relationships: {:?}", def.data_relationships.keys());

                        //
                        panic!("prop id {prop_id:?} should belong to type")
                    });

                    if let DataRelationshipKind::Edge(_projection) = rel_info.kind {
                        filter.def_id = match rel_info.target {
                            DataRelationshipTarget::Unambiguous(def_id) => def_id,
                            DataRelationshipTarget::Union(union_def_id) => union_def_id,
                        };
                    };

                    let id_prop = def.entity().unwrap().id_prop;
                    if id_prop == *prop_id {
                        filter.prop = "_key".to_string();
                    } else {
                        filter.prop = self.ontology[rel_info.name].to_string();
                    }

                    self.eval_match_prop(filter, *set_op, *set_var, walker)?;

                    if join {
                        return Ok(());
                    }

                    let val = match filter.values.len() {
                        1 => Some(filter.values.first().unwrap().clone()),
                        _ => Some(format!("[{}]", filter.values.join(", "))),
                    };

                    // debug!("{filter:#?}");

                    match rel_info.kind {
                        DataRelationshipKind::Id | DataRelationshipKind::Tree(_) => {
                            self.ops.push(Operation::Filter(Filter {
                                var: Expr::complex(format!("{}.{}", self.var, filter.prop)),
                                comp: filter.comp.clone(),
                                val,
                            }));
                        }
                        DataRelationshipKind::Edge(_projection) => {
                            let var_name =
                                format!("{}_{}", self.var.raw_str(), &self.ontology[rel_info.name]);

                            if !self.ops.iter().any(|op| {
                                if let Operation::Let(Let { var, .. }) = op {
                                    var.raw_str() == var_name
                                } else {
                                    false
                                }
                            }) {
                                // add relation query if not included in Select
                                self.query_relation(
                                    &Select::Struct(StructSelect {
                                        def_id: filter.def_id,
                                        properties: Default::default(),
                                    }),
                                    rel_info,
                                    &def.id,
                                )?;
                            }

                            self.ops.push(Operation::Filter(Filter {
                                var: Expr::complex(format!("LENGTH({})", var_name)),
                                ..Default::default()
                            }));

                            if filter.values.is_empty() {
                                return Ok(());
                            }

                            for op in self.ops.iter_mut() {
                                if let Operation::Let(Let { var, query }) = op {
                                    if var.raw_str() != var_name {
                                        continue;
                                    }
                                    if let Some(sub_ops) = query.operations.as_mut() {
                                        sub_ops.push(Operation::Filter(Filter {
                                            var: Expr::complex(format!(
                                                "{}.{}",
                                                query.returns.var, filter.prop
                                            )),
                                            comp: filter.comp.clone(),
                                            val: val.clone(),
                                        }));
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn eval_match_prop(
        &mut self,
        filter: &mut FilterQuery,
        set_op: SetOperator,
        set_var: Var,
        walker: ConditionWalker,
    ) -> DomainResult<()> {
        let members = match walker.set_members(set_var) {
            Members::Empty => {
                return Err(DomainError::data_store("not implemented"));
            }
            Members::Join(var) => {
                debug!("members join {var:?}, not implemented");
                return Err(DomainError::data_store("not implemented"));
            }
            Members::Members(members) => members,
        };

        match set_op {
            SetOperator::ElementIn => {
                filter.comp = match members.size() {
                    1 => Some(Comparison::Eq),
                    _ => Some(Comparison::In),
                };
            }
            SetOperator::SubsetOf => {
                return Err(DomainError::data_store("not implemented"));
            }
            SetOperator::SupersetOf => {
                return Err(DomainError::data_store("not implemented"));
            }
            SetOperator::SetIntersects => {
                return Err(DomainError::data_store("not implemented"));
            }
            SetOperator::SetEquals => {
                filter.comp = Some(Comparison::Eq);
            }
        }

        for member_pred in members.iter() {
            match member_pred {
                MemberPredicate::Element(rel_term, val_term) => {
                    self.eval_term(filter, rel_term, walker)?;
                    self.eval_term(filter, val_term, walker)?;
                }
                MemberPredicate::Predicate => {}
            }
        }

        Ok(())
    }

    fn eval_term(
        &mut self,
        filter: &mut FilterQuery,
        term: &CondTerm,
        walker: ConditionWalker,
    ) -> DomainResult<()> {
        match term {
            CondTerm::Wildcard => {}
            CondTerm::Variable(cond_var) => {
                self.eval_cond_var(filter, *cond_var, walker, true)?;
            }
            CondTerm::Value(value) => {
                let val = match value {
                    // quoted
                    Value::Text(_, _)
                    | Value::OctetSequence(_, _)
                    | Value::ChronoDateTime(_, _)
                    | Value::Struct(_, _) => format!(
                        r#""{}""#,
                        ValueFormatRaw::new(value, value.type_def_id(), self.ontology.as_ref())
                    ),
                    // unquoted
                    _ => ValueFormatRaw::new(value, value.type_def_id(), self.ontology.as_ref())
                        .to_string(),
                };

                filter.values.push(val);
            }
        }
        Ok(())
    }
}
