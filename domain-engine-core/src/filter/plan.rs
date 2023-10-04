use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    condition::{Clause, CondTerm, Condition},
    ontology::{DataRelationshipKind, Ontology, ValueCardinality},
    value::{Data, PropertyId, Value},
    var::{Var, VarSet},
    DefId,
};
use ordered_float::NotNan;

use crate::filter::disjoint_clause_sets::disjoint_clause_sets;

use super::condition_utils::{get_clause_vars, TermVars};

/// A concrete "execution plan" for filtering
#[derive(Debug, PartialEq, Eq)]
#[allow(unused)]
pub enum Plan {
    /// A root plan that filters specific entities
    EntitiesOf(DefId, Vec<Plan>),
    /// A root plan that is the output edge of a Join (many input edges).
    /// The UniVar specifies the id of the join.
    /// The semantics is that the match must be for the _same entity_, but from different input paths.
    JoinRoot(Var, Vec<Plan>),
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
    Join(Var),
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
    term_var_counts: FnvHashMap<Var, usize>,
    current_join_roots: FnvHashMap<Var, DefId>,
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
        let mut term_vars = TermVars(VarSet::default());
        get_clause_vars(clause, &mut term_vars);
        for var in &term_vars.0 {
            *plan_builder.term_var_counts.entry(var).or_default() += 1;
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
    binder: Var,
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
                                .unwrap_or_else(Vec::new),
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
                    CondTerm::Value(value) => {
                        plans.push(Plan::Eq(value_to_scalar(value)));
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
        CondTerm::Value(value) => {
            plans.push(Plan::Eq(value_to_scalar(value)));
        }
    }
    plans
}

fn value_to_scalar(value: &Value) -> Scalar {
    match &value.data {
        Data::Text(text) => Scalar::Text(text.as_str().into()),
        Data::I64(int) => Scalar::I64(*int),
        Data::F64(float) => Scalar::F64((*float).try_into().expect("NaN")),
        _ => panic!(),
    }
}

#[cfg(test)]
mod tests {
    use ontol_test_utils::{expect_eq, TestCompile};

    use super::*;

    fn var(str: &str) -> Var {
        format!("${str}").parse().unwrap()
    }

    fn wild() -> CondTerm {
        CondTerm::Wildcard
    }

    fn text(text: &str) -> Scalar {
        Scalar::Text(text.into())
    }

    impl From<Scalar> for Value {
        fn from(scalar: Scalar) -> Self {
            match scalar {
                Scalar::Text(text) => Value::new(Data::Text(text.into()), DefId::unit()),
                Scalar::I64(int) => Value::new(Data::I64(int), DefId::unit()),
                Scalar::F64(float) => Value::new(Data::F64(*float), DefId::unit()),
            }
        }
    }

    impl Scalar {
        fn to_term(&self) -> CondTerm {
            CondTerm::Value(self.clone().into())
        }
    }

    #[test]
    fn basic_tree() {
        "
        pub def foo {
            rel .id: { rel .is: text }
            rel .'x': text
        }
        pub def bar {
            rel .id: { rel .is: text }
            rel .'y': text
            rel [.] 'foos'::'bars' [foo]
        }
        "
        .compile_then(|test| {
            let [foo, bar] = test.bind(["foo", "bar"]);
            let x = foo.find_property("x").unwrap();
            let bars = foo.find_property("bars").unwrap();
            let y = bar.find_property("y").unwrap();

            use CondTerm::*;

            let plan = compute_plans(
                &Condition {
                    clauses: vec![
                        Clause::Root(var("a")),
                        Clause::IsEntity(Var(var("a")), foo.def_id()),
                        Clause::Attr(var("a"), x, (Wildcard, Var(var("b")))),
                        Clause::Eq(var("b"), text("match1").to_term()),
                        Clause::Attr(var("a"), bars, (CondTerm::Wildcard, Var(var("c")))),
                        Clause::Attr(var("c"), y, (wild(), text("match2").to_term())),
                    ],
                },
                &test.ontology,
            );

            expect_eq!(
                actual = plan,
                expected = vec![Plan::EntitiesOf(
                    foo.def_id(),
                    vec![
                        Plan::Attr(x, vec![Plan::Eq(Scalar::Text("match1".into()))]),
                        Plan::AllEdges(
                            bars,
                            EdgeAttr {
                                rel: vec![],
                                val: vec![Plan::Attr(
                                    y,
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
            rel .'x': text
        }
        pub def bar {
            rel .id: { rel .is: text }
            rel .'y': text
        }
        rel [foo] 'bars_x'::'foos' [bar]
        rel [foo] 'bars_y': [bar]
        "
        .compile_then(|test| {
            let [foo, bar] = test.bind(["foo", "bar"]);
            let x = foo.find_property("x").unwrap();
            let y = bar.find_property("y").unwrap();
            let bars_x = foo.find_property("bars_x").unwrap();
            let bars_y = foo.find_property("bars_y").unwrap();
            let foos = bar.find_property("foos").unwrap();

            use CondTerm::*;

            let plan = compute_plans(
                &Condition {
                    clauses: vec![
                        Clause::Root(var("a")),
                        Clause::IsEntity(Var(var("a")), foo.def_id()),
                        Clause::Attr(var("a"), bars_x, (wild(), Var(var("b")))),
                        Clause::Attr(var("a"), bars_y, (wild(), Var(var("c")))),
                        Clause::Attr(var("b"), y, (wild(), text("match1").to_term())),
                        Clause::Attr(var("c"), y, (wild(), text("match2").to_term())),
                        Clause::Attr(var("b"), foos, (wild(), Var(var("d")))),
                        Clause::Attr(var("c"), foos, (wild(), Var(var("d")))),
                        Clause::Attr(var("d"), x, (wild(), text("match3").to_term())),
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
                                bars_x,
                                EdgeAttr {
                                    rel: vec![],
                                    val: vec![
                                        Plan::Attr(y, vec![Plan::Eq(text("match1"))]),
                                        Plan::AllEdges(
                                            foos,
                                            EdgeAttr {
                                                rel: vec![],
                                                val: vec![Plan::Join(var("d"))]
                                            }
                                        )
                                    ],
                                }
                            ),
                            Plan::AllEdges(
                                bars_y,
                                EdgeAttr {
                                    rel: vec![],
                                    val: vec![
                                        Plan::Attr(y, vec![Plan::Eq(text("match2"))]),
                                        Plan::AllEdges(
                                            foos,
                                            EdgeAttr {
                                                rel: vec![],
                                                val: vec![Plan::Join(var("d"))]
                                            }
                                        )
                                    ],
                                }
                            ),
                        ]
                    ),
                    Plan::JoinRoot(
                        var("d"),
                        vec![Plan::Attr(x, vec![Plan::Eq(text("match3"))])]
                    )
                ]
            );
        });
    }
}
