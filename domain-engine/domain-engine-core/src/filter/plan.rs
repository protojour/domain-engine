use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    condition::{Clause, CondTerm, Condition, SetOperator},
    ontology::{DataRelationshipKind, DataRelationshipTarget, Ontology, ValueCardinality},
    value::{Attribute, PropertyId, Value},
    var::{Var, VarSet},
    DefId,
};
use ordered_float::NotNan;
use thin_vec::{thin_vec, ThinVec};

use crate::filter::disjoint_clause_sets::disjoint_clause_sets;

use super::condition_utils::{get_clause_vars, TermVars};

/// A concrete "execution plan" for filtering
#[derive(PartialEq, Eq, Debug)]
#[allow(unused)]
pub enum PlanEntry {
    /// A root plan that filters specific entities
    EntitiesOf(DefId, Vec<PlanEntry>),
    /// A root plan that is the output edge of a Join (many input edges).
    /// The Var specifies the id of the join.
    /// The semantics is that the match must be for the _same entity_, but from different input paths.
    JoinRoot(Var, Vec<PlanEntry>),
    /// A scalar attribute that matches the given plans
    Attr(PropertyId, Vec<PlanEntry>),
    /// A scalar multi-attribute where every attribute matches the given plans
    AllAttrs(PropertyId, Vec<PlanEntry>),
    /// A graph edge attribute which matches the given attribute plan
    Edge(PropertyId, Attribute<Vec<PlanEntry>>),
    /// A multi-graph edge attribute which matches the given attribute plan
    AllEdges(PropertyId, Attribute<Vec<PlanEntry>>),
    PropMatch(PropertyId, SetOperator, Set<PlanEntry>),
    EdgeMatch(PropertyId, SetOperator, Set<Attribute<PlanEntry>>),
    /// Expression must match the given scalar
    Eq(Scalar),
    /// Expression must be an element in the given set of scalars
    In(Vec<Scalar>),
    /// A Join for which the same entity must match in several branches.
    Join(Var),
}

#[derive(PartialEq, Eq, Debug)]
pub enum Set<T> {
    Set(ThinVec<T>),
    Range {
        lower: Option<Box<T>>,
        upper: Option<Box<T>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scalar {
    Text(Box<str>),
    I64(i64),
    F64(NotNan<f64>),
}

#[derive(Debug)]
pub enum PlanError {
    InvalidScalar,
}

type PlanResult<T> = Result<T, PlanError>;

struct PlanBuilder<'on> {
    ontology: &'on Ontology,
    term_var_counts: FnvHashMap<Var, usize>,
    current_join_roots: FnvHashMap<Var, DefId>,
}

pub fn compute_filter_plan(
    condition: &Condition,
    ontology: &Ontology,
) -> PlanResult<Vec<PlanEntry>> {
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
        if let Some(result) = compute_plan(&clauses, &mut plan_builder) {
            output.push(result?);
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
                )?;

                output.push(PlanEntry::JoinRoot(var, join_plans));
            }
        }
    }

    Ok(output)
}

fn compute_plan(
    clauses: &[&Clause<Var, CondTerm>],
    builder: &mut PlanBuilder,
) -> Option<PlanResult<PlanEntry>> {
    let root_var = clauses.iter().find_map(|clause| match clause {
        Clause::Root(var) => Some(*var),
        _ => None,
    })?;
    let entity_def_id = clauses.iter().find_map(|clause| match clause {
        Clause::IsEntity(CondTerm::Var(var), def_id) if *var == root_var => Some(*def_id),
        _ => None,
    })?;

    match sub_plans(
        Origin {
            def_id: entity_def_id,
            binder: root_var,
        },
        clauses,
        builder,
    ) {
        Ok(sub_plans) => Some(Ok(PlanEntry::EntitiesOf(entity_def_id, sub_plans))),
        Err(error) => Some(Err(error)),
    }
}

struct Origin {
    def_id: DefId,
    binder: Var,
}

fn sub_plans(
    origin: Origin,
    clauses: &[&Clause<Var, CondTerm>],
    builder: &mut PlanBuilder,
) -> PlanResult<Vec<PlanEntry>> {
    let mut plans: Vec<PlanEntry> = vec![];

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

                let val_plans = match data_relationship.target {
                    DataRelationshipTarget::Unambiguous(target_def_id) => {
                        term_plans(val, target_def_id, clauses, builder)?
                    }
                    DataRelationshipTarget::Union { .. } => {
                        todo!()
                    }
                };

                let cardinality = data_relationship.cardinality_by_role(property_id.role);

                match data_relationship.kind {
                    DataRelationshipKind::Tree => match cardinality.1 {
                        ValueCardinality::One => {
                            plans.push(PlanEntry::Attr(*property_id, val_plans));
                        }
                        ValueCardinality::Many => {
                            plans.push(PlanEntry::AllAttrs(*property_id, val_plans));
                        }
                    },
                    DataRelationshipKind::EntityGraph { rel_params } => {
                        let attribute = Attribute {
                            rel: rel_params
                                .as_ref()
                                .map(|rel_params| term_plans(rel, *rel_params, clauses, builder))
                                .transpose()?
                                .unwrap_or_default(),
                            val: val_plans,
                        };

                        match cardinality.1 {
                            ValueCardinality::One => {
                                plans.push(PlanEntry::Edge(*property_id, attribute));
                            }
                            ValueCardinality::Many => {
                                plans.push(PlanEntry::AllEdges(*property_id, attribute));
                            }
                        }
                    }
                }
            }
            Clause::MatchProp(struct_var, prop_id, set_operator, set_var) => {
                if *struct_var != origin.binder {
                    continue;
                }

                let Some(data_relationship) = type_info.data_relationships.get(prop_id) else {
                    continue;
                };

                let val_target = match data_relationship.target {
                    DataRelationshipTarget::Unambiguous(target_def_id) => target_def_id,
                    DataRelationshipTarget::Union { .. } => {
                        todo!()
                    }
                };

                match data_relationship.kind {
                    DataRelationshipKind::Tree => {
                        let set_plans = prop_set_plans(*set_var, val_target, clauses, builder)?;
                        plans.push(PlanEntry::PropMatch(*prop_id, *set_operator, set_plans));
                    }
                    DataRelationshipKind::EntityGraph { rel_params } => {
                        let set_plans =
                            attr_set_plans(*set_var, (rel_params, val_target), clauses, builder)?;
                        plans.push(PlanEntry::EdgeMatch(*prop_id, *set_operator, set_plans));
                    }
                }
            }
            Clause::Element(..) => {}
            Clause::Eq(var, term) => {
                if *var != origin.binder {
                    continue;
                }

                match term {
                    CondTerm::Wildcard => {}
                    CondTerm::Var(_) => todo!(),
                    CondTerm::Value(value) => {
                        plans.push(PlanEntry::Eq(value_to_scalar(value)?));
                    }
                }
            }
            Clause::Or(_) => todo!(),
        }
    }

    Ok(plans)
}

fn prop_set_plans(
    set_var: Var,
    target: DefId,
    mut clauses: &[&Clause<Var, CondTerm>],
    builder: &mut PlanBuilder,
) -> PlanResult<Set<PlanEntry>> {
    let mut plans = thin_vec![];

    for clause in clauses {
        if let Clause::Element(var, (_rel, val)) = clause {
            if *var == set_var {
                let entry = term_plans(val, target, clauses, builder)?;

                plans.extend(entry);
            }
        }
    }

    Ok(Set::Set(plans))
}

fn attr_set_plans(
    _set_var: Var,
    target: (Option<DefId>, DefId),
    _clauses: &[&Clause<Var, CondTerm>],
    _builder: &mut PlanBuilder,
) -> PlanResult<Set<Attribute<PlanEntry>>> {
    todo!()
}

fn term_plans(
    term: &CondTerm,
    target: DefId,
    clauses: &[&Clause<Var, CondTerm>],
    builder: &mut PlanBuilder,
) -> PlanResult<Vec<PlanEntry>> {
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
                plans.push(PlanEntry::Join(*var));
                builder.current_join_roots.insert(*var, target);
            } else {
                plans.extend(sub_plans(
                    Origin {
                        def_id: target,
                        binder: *var,
                    },
                    clauses,
                    builder,
                )?)
            }
        }
        CondTerm::Value(value) => {
            plans.push(PlanEntry::Eq(value_to_scalar(value)?));
        }
    }
    Ok(plans)
}

fn value_to_scalar(value: &Value) -> PlanResult<Scalar> {
    match value {
        Value::Text(text, _) => Ok(Scalar::Text(text.as_str().into())),
        Value::I64(int, _) => Ok(Scalar::I64(*int)),
        Value::F64(float, _) => Ok(Scalar::F64(
            (*float).try_into().map_err(|_| PlanError::InvalidScalar)?,
        )),
        _ => Err(PlanError::InvalidScalar),
    }
}

#[cfg(test)]
mod tests {
    use ontol_test_utils::{expect_eq, TestCompile};
    use test_log::test;

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
                Scalar::Text(text) => Value::Text(text.into(), DefId::unit()),
                Scalar::I64(int) => Value::I64(int, DefId::unit()),
                Scalar::F64(float) => Value::F64(*float, DefId::unit()),
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
        def foo(
            rel .id: (rel .is: text)
            rel .'x': text
        )
        def bar(
            rel .id: (rel .is: text)
            rel .'y': text
            rel {.} 'foos'::'bars' {foo}
        )
        "
        .compile_then(|test| {
            let [foo, bar] = test.bind(["foo", "bar"]);
            let [x, bars, y] = test.prop_ids([(&foo, "x"), (&foo, "bars"), (&bar, "y")]);

            use CondTerm::*;

            let plan = compute_filter_plan(
                &Condition::from(thin_vec![
                    Clause::Root(var("a")),
                    Clause::IsEntity(Var(var("a")), foo.def_id()),
                    Clause::Attr(var("a"), x, (Wildcard, Var(var("b")))),
                    Clause::Eq(var("b"), text("match1").to_term()),
                    Clause::Attr(var("a"), bars, (CondTerm::Wildcard, Var(var("c")))),
                    Clause::Attr(var("c"), y, (wild(), text("match2").to_term())),
                ]),
                &test.ontology,
            )
            .unwrap();

            expect_eq!(
                actual = plan,
                expected = vec![PlanEntry::EntitiesOf(
                    foo.def_id(),
                    vec![
                        PlanEntry::Attr(x, vec![PlanEntry::Eq(Scalar::Text("match1".into()))]),
                        PlanEntry::AllEdges(
                            bars,
                            Attribute {
                                rel: vec![],
                                val: vec![PlanEntry::Attr(
                                    y,
                                    vec![PlanEntry::Eq(Scalar::Text("match2".into()))]
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
        def foo(
            rel .id: (rel .is: text)
            rel .'x': text
        )
        def bar(
            rel .id: (rel .is: text)
            rel .'y': text
        )
        rel {foo} 'bars_x'::'foos' {bar}
        rel {foo} 'bars_y': {bar}
        "
        .compile_then(|test| {
            let [foo, bar] = test.bind(["foo", "bar"]);
            let [x, y, bars_x, bars_y, foos] = test.prop_ids([
                (&foo, "x"),
                (&bar, "y"),
                (&foo, "bars_x"),
                (&foo, "bars_y"),
                (&bar, "foos"),
            ]);

            use CondTerm::*;

            let plan = compute_filter_plan(
                &Condition::from(thin_vec![
                    Clause::Root(var("a")),
                    Clause::IsEntity(Var(var("a")), foo.def_id()),
                    Clause::Attr(var("a"), bars_x, (wild(), Var(var("b")))),
                    Clause::Attr(var("a"), bars_y, (wild(), Var(var("c")))),
                    Clause::Attr(var("b"), y, (wild(), text("match1").to_term())),
                    Clause::Attr(var("c"), y, (wild(), text("match2").to_term())),
                    Clause::Attr(var("b"), foos, (wild(), Var(var("d")))),
                    Clause::Attr(var("c"), foos, (wild(), Var(var("d")))),
                    Clause::Attr(var("d"), x, (wild(), text("match3").to_term())),
                ]),
                &test.ontology,
            )
            .unwrap();

            expect_eq!(
                actual = plan,
                expected = vec![
                    PlanEntry::EntitiesOf(
                        foo.def_id(),
                        vec![
                            PlanEntry::AllEdges(
                                bars_x,
                                Attribute {
                                    rel: vec![],
                                    val: vec![
                                        PlanEntry::Attr(y, vec![PlanEntry::Eq(text("match1"))]),
                                        PlanEntry::AllEdges(
                                            foos,
                                            Attribute {
                                                rel: vec![],
                                                val: vec![PlanEntry::Join(var("d"))]
                                            }
                                        )
                                    ],
                                }
                            ),
                            PlanEntry::AllEdges(
                                bars_y,
                                Attribute {
                                    rel: vec![],
                                    val: vec![
                                        PlanEntry::Attr(y, vec![PlanEntry::Eq(text("match2"))]),
                                        PlanEntry::AllEdges(
                                            foos,
                                            Attribute {
                                                rel: vec![],
                                                val: vec![PlanEntry::Join(var("d"))]
                                            }
                                        )
                                    ],
                                }
                            ),
                        ]
                    ),
                    PlanEntry::JoinRoot(
                        var("d"),
                        vec![PlanEntry::Attr(x, vec![PlanEntry::Eq(text("match3"))])]
                    )
                ]
            );
        });
    }

    #[test]
    fn element_in() {
        "
        def foo(
            rel .id: (rel .is: text)
            rel .'bar': bar
        )
        def bar(
            rel .'val': text
        )
        "
        .compile_then(|test| {
            let [foo, bar] = test.bind(["foo", "bar"]);
            let [bar, val] = test.prop_ids([(&foo, "bar"), (&bar, "val")]);

            use ontol_runtime::condition::SetOperator::*;
            use CondTerm::*;

            let plan = compute_filter_plan(
                &Condition::from(thin_vec![
                    Clause::Root(var("a")),
                    Clause::IsEntity(Var(var("a")), foo.def_id()),
                    Clause::MatchProp(var("a"), bar, ElementIn, var("b")),
                    Clause::Element(var("b"), (wild(), Var(var("c")))),
                    Clause::Attr(var("c"), val, (wild(), text("text1").to_term())),
                    Clause::Element(var("b"), (wild(), Var(var("d")))),
                    Clause::Attr(var("d"), val, (wild(), text("text2").to_term())),
                ]),
                &test.ontology,
            )
            .unwrap();

            expect_eq!(
                actual = plan,
                expected = vec![PlanEntry::EntitiesOf(foo.def_id(), vec![]),]
            );
        });
    }
}
