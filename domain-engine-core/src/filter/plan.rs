use bit_set::BitSet;
use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    condition::{Clause, CondTerm, Condition, UniVar},
    ontology::{DataRelationshipKind, Ontology, ValueCardinality},
    value::PropertyId,
    DefId,
};
use ordered_float::NotNan;

use crate::filter::disjoint_clause_sets::disjoint_clause_sets;

use super::condition_utils::{get_clause_vars, TermVars};

/// A concrete "execution plan" for filtering
#[derive(Debug, PartialEq, Eq)]
pub enum Plan {
    /// A root plan that filters specific entities
    EntitiesOf(DefId, Vec<Plan>),
    /// A root plan that is the output edge of a Join (many input edges).
    /// The UniVar specifies the id of the join.
    /// The semantics is that the match must be for the _same entity_, but from different input paths.
    JoinRoot(UniVar, Vec<Plan>),
    /// An scalar attribute that matches the given plans
    Attr(PropertyId, Vec<Plan>),
    /// A scalar multi-attribute where every attribute matches the given plans
    AllAttrs(PropertyId, Vec<Plan>),
    /// A graph edge attribute which matches the given attribute plan
    Edge(PropertyId, EdgeAttr<Vec<Plan>>),
    /// A multi-graph edge attribute which matches the given attribute plan
    AllEdges(PropertyId, EdgeAttr<Vec<Plan>>),
    /// Expression must match the given scalar
    Eq(Scalar),
    /// Expression must be an element in the given set of scalars
    In(Vec<Scalar>),
    /// A Join for which the same entity must match in several branches.
    Join(UniVar),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Scalar {
    Text(Box<str>),
    I64(i64),
    F64(NotNan<f64>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct EdgeAttr<T> {
    pub rel: T,
    pub val: T,
}

struct PlanBuilder<'on> {
    ontology: &'on Ontology,
    term_var_counts: FnvHashMap<UniVar, usize>,
    current_join_roots: FnvHashMap<UniVar, DefId>,
}

pub fn compute_plans(condition: &Condition, ontology: &Ontology) -> Vec<Plan> {
    let clauses = &condition.clauses;
    let mut output = vec![];

    let mut plan_builder = PlanBuilder {
        ontology,
        term_var_counts: Default::default(),
        current_join_roots: Default::default(),
    };

    for clause in &condition.clauses {
        let mut term_vars = TermVars(BitSet::new());
        get_clause_vars(clause, &mut term_vars);
        for var in &term_vars.0 {
            let uni_var: UniVar = var.into();
            *plan_builder.term_var_counts.entry(uni_var).or_default() += 1;
        }
    }

    for group in disjoint_clause_sets(clauses) {
        let clauses = group.into_iter().map(|index| &clauses[index]).collect_vec();
        if let Some(plan) = compute_plan(&clauses, &mut plan_builder) {
            output.push(plan);
        }

        while !plan_builder.current_join_roots.is_empty() {
            for (var, target) in std::mem::take(&mut plan_builder.current_join_roots) {
                let join_plans = sub_plans(
                    Origin {
                        def_id: target,
                        binder: var,
                    },
                    &clauses,
                    &mut plan_builder,
                );

                output.push(Plan::JoinRoot(var, join_plans));
            }
        }
    }

    output
}

fn compute_plan(clauses: &[&Clause], builder: &mut PlanBuilder) -> Option<Plan> {
    let root_var = clauses.iter().find_map(|clause| match clause {
        Clause::Root(var) => Some(*var),
        _ => None,
    })?;
    let entity_def_id = clauses.iter().find_map(|clause| match clause {
        Clause::IsEntity(CondTerm::Var(var), def_id) if *var == root_var => Some(*def_id),
        _ => None,
    })?;

    let sub_plans = sub_plans(
        Origin {
            def_id: entity_def_id,
            binder: root_var,
        },
        clauses,
        builder,
    );

    Some(Plan::EntitiesOf(entity_def_id, sub_plans))
}

struct Origin {
    def_id: DefId,
    binder: UniVar,
}

fn sub_plans(origin: Origin, clauses: &[&Clause], builder: &mut PlanBuilder) -> Vec<Plan> {
    let mut plans: Vec<Plan> = vec![];

    let type_info = builder.ontology.get_type_info(origin.def_id);

    for clause in clauses {
        match clause {
            Clause::Root(_) => {}
            Clause::IsEntity(_, _) => {}
            Clause::Attr(var, property_id, (rel, val)) => {
                if *var != origin.binder {
                    continue;
                }

                let Some(data_relationship) = type_info.data_relationships.get(property_id) else {
                    continue;
                };

                let val_plans = term_plans(val, data_relationship.target, clauses, builder);

                match data_relationship.kind {
                    DataRelationshipKind::Tree => match data_relationship.cardinality.1 {
                        ValueCardinality::One => {
                            plans.push(Plan::Attr(*property_id, val_plans));
                        }
                        ValueCardinality::Many => {
                            plans.push(Plan::AllAttrs(*property_id, val_plans));
                        }
                    },
                    DataRelationshipKind::EntityGraph { rel_params } => {
                        let attribute = EdgeAttr {
                            rel: rel_params
                                .as_ref()
                                .map(|rel_params| term_plans(rel, *rel_params, clauses, builder))
                                .unwrap_or_else(|| vec![]),
                            val: val_plans,
                        };

                        match data_relationship.cardinality.1 {
                            ValueCardinality::One => {
                                plans.push(Plan::Edge(*property_id, attribute));
                            }
                            ValueCardinality::Many => {
                                plans.push(Plan::AllEdges(*property_id, attribute));
                            }
                        }
                    }
                }
            }
            Clause::Eq(var, term) => {
                if *var != origin.binder {
                    continue;
                }

                match term {
                    CondTerm::Wildcard => {}
                    CondTerm::Var(_) => todo!(),
                    CondTerm::Text(scalar) => {
                        plans.push(Plan::Eq(Scalar::Text(scalar.clone())));
                    }
                    CondTerm::F64(scalar) => {
                        plans.push(Plan::Eq(Scalar::F64((*scalar).try_into().unwrap())))
                    }
                    CondTerm::I64(scalar) => {
                        plans.push(Plan::Eq(Scalar::I64(*scalar)));
                    }
                }
            }
            Clause::Or(_) => todo!(),
        }
    }

    plans
}

fn term_plans(
    term: &CondTerm,
    target: DefId,
    clauses: &[&Clause],
    builder: &mut PlanBuilder,
) -> Vec<Plan> {
    let mut plans = vec![];
    match term {
        CondTerm::Wildcard => {}
        CondTerm::Var(var) => {
            if builder
                .term_var_counts
                .get(var)
                .cloned()
                .unwrap_or_default()
                > 1
            {
                plans.push(Plan::Join(*var));
                builder.current_join_roots.insert(*var, target);
            } else {
                plans.extend(sub_plans(
                    Origin {
                        def_id: target,
                        binder: *var,
                    },
                    clauses,
                    builder,
                ))
            }
        }
        CondTerm::Text(scalar) => {
            plans.push(Plan::Eq(Scalar::Text(scalar.clone())));
        }
        CondTerm::F64(scalar) => plans.push(Plan::Eq(Scalar::F64((*scalar).try_into().unwrap()))),
        CondTerm::I64(scalar) => {
            plans.push(Plan::Eq(Scalar::I64(*scalar)));
        }
    }
    plans
}

#[cfg(test)]
mod tests {
    use ontol_test_utils::{expect_eq, TestCompile};

    use super::*;

    fn wild() -> CondTerm {
        CondTerm::Wildcard
    }

    fn text(text: &str) -> Scalar {
        Scalar::Text(text.into())
    }

    #[test]
    fn basic_tree() {
        "
        pub def foo {
            rel .id: { rel .is: text }
            rel .'a': text
        }
        pub def bar {
            rel .id: { rel .is: text }
            rel .'b': text
            rel [.] 'foos'::'bars' [foo]
        }
        "
        .compile_then(|test| {
            let [foo, bar] = test.bind(["foo", "bar"]);
            let a = foo.find_property("a").unwrap();
            let bars = foo.find_property("bars").unwrap();
            let b = bar.find_property("b").unwrap();

            let plan = compute_plans(
                &Condition {
                    clauses: vec![
                        Clause::Root(UniVar(0)),
                        Clause::IsEntity(CondTerm::Var(UniVar(0)), foo.def_id()),
                        Clause::Attr(UniVar(0), a, (CondTerm::Wildcard, CondTerm::Var(UniVar(1)))),
                        Clause::Eq(UniVar(1), CondTerm::Text("match1".into())),
                        Clause::Attr(
                            UniVar(0),
                            bars,
                            (CondTerm::Wildcard, CondTerm::Var(UniVar(2))),
                        ),
                        Clause::Attr(UniVar(2), b, (wild(), CondTerm::Text("match2".into()))),
                    ],
                },
                &test.ontology,
            );

            expect_eq!(
                actual = plan,
                expected = vec![Plan::EntitiesOf(
                    foo.def_id(),
                    vec![
                        Plan::Attr(a, vec![Plan::Eq(Scalar::Text("match1".into()))]),
                        Plan::AllEdges(
                            bars,
                            EdgeAttr {
                                rel: vec![],
                                val: vec![Plan::Attr(
                                    b,
                                    vec![Plan::Eq(Scalar::Text("match2".into()))]
                                )],
                            }
                        )
                    ]
                )]
            );
        });
    }

    #[test]
    fn basic_merge() {
        "
        pub def foo {
            rel .id: { rel .is: text }
            rel .'a': text
        }
        pub def bar {
            rel .id: { rel .is: text }
            rel .'b': text
        }
        rel [foo] 'bars_a'::'foos' [bar]
        rel [foo] 'bars_b': [bar]
        "
        .compile_then(|test| {
            let [foo, bar] = test.bind(["foo", "bar"]);
            let a = foo.find_property("a").unwrap();
            let b = bar.find_property("b").unwrap();
            let bars_a = foo.find_property("bars_a").unwrap();
            let bars_b = foo.find_property("bars_b").unwrap();
            let foos = bar.find_property("foos").unwrap();

            let plan = compute_plans(
                &Condition {
                    clauses: vec![
                        Clause::Root(UniVar(0)),
                        Clause::IsEntity(CondTerm::Var(UniVar(0)), foo.def_id()),
                        Clause::Attr(UniVar(0), bars_a, (wild(), CondTerm::Var(UniVar(1)))),
                        Clause::Attr(UniVar(0), bars_b, (wild(), CondTerm::Var(UniVar(2)))),
                        Clause::Attr(UniVar(1), b, (wild(), CondTerm::Text("match1".into()))),
                        Clause::Attr(UniVar(2), b, (wild(), CondTerm::Text("match2".into()))),
                        Clause::Attr(UniVar(1), foos, (wild(), CondTerm::Var(UniVar(3)))),
                        Clause::Attr(UniVar(2), foos, (wild(), CondTerm::Var(UniVar(3)))),
                        Clause::Attr(UniVar(3), a, (wild(), CondTerm::Text("match3".into()))),
                    ],
                },
                &test.ontology,
            );

            expect_eq!(
                actual = plan,
                expected = vec![
                    Plan::EntitiesOf(
                        foo.def_id(),
                        vec![
                            Plan::AllEdges(
                                bars_a,
                                EdgeAttr {
                                    rel: vec![],
                                    val: vec![
                                        Plan::Attr(b, vec![Plan::Eq(text("match1"))]),
                                        Plan::AllEdges(
                                            foos,
                                            EdgeAttr {
                                                rel: vec![],
                                                val: vec![Plan::Join(UniVar(3))]
                                            }
                                        )
                                    ],
                                }
                            ),
                            Plan::AllEdges(
                                bars_b,
                                EdgeAttr {
                                    rel: vec![],
                                    val: vec![
                                        Plan::Attr(b, vec![Plan::Eq(text("match2"))]),
                                        Plan::AllEdges(
                                            foos,
                                            EdgeAttr {
                                                rel: vec![],
                                                val: vec![Plan::Join(UniVar(3))]
                                            }
                                        )
                                    ],
                                }
                            ),
                        ]
                    ),
                    Plan::JoinRoot(
                        UniVar(3),
                        vec![Plan::Attr(a, vec![Plan::Eq(text("match3"))])]
                    )
                ]
            );
        });
    }
}
