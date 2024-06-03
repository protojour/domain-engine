use anyhow::anyhow;
use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::domain::DataRelationshipKind,
    query::condition::{Clause, CondTerm, Condition, SetOperator},
    value::{Attribute, Value},
    var::Var,
    DefId, RelationshipId,
};

use domain_engine_core::{
    filter::walker::{ConditionWalker, Members},
    // filter::plan::{PlanEntry, Scalar},
    DomainError,
    DomainResult,
};
use tracing::{error, warn};

use crate::core::DbContext;

use super::core::{DynamicKey, EntityKey, InMemoryStore};

#[derive(Clone, Copy, Debug)]
pub(super) enum FilterVal<'d> {
    Struct {
        type_def_id: DefId,
        dynamic_key: Option<&'d DynamicKey>,
        prop_tree: &'d FnvHashMap<RelationshipId, Attribute>,
    },
    Sequence(&'d [Attribute]),
    Scalar(&'d Value),
}

impl<'d> FilterVal<'d> {
    fn from_entity(
        entity_key: &'d EntityKey,
        prop_tree: &'d FnvHashMap<RelationshipId, Attribute>,
    ) -> Self {
        Self::Struct {
            type_def_id: entity_key.type_def_id,
            dynamic_key: Some(&entity_key.dynamic_key),
            prop_tree,
        }
    }

    fn from_value(value: &'d Value) -> Self {
        match value {
            Value::Struct(map, type_def_id) => Self::Struct {
                type_def_id: *type_def_id,
                dynamic_key: None,
                prop_tree: map,
            },
            Value::Sequence(seq, _) => Self::Sequence(seq.attrs()),
            _ => Self::Scalar(value),
        }
    }
}

enum Proof {
    Proven,
    Maybe,
}

impl Proof {
    fn merge(&mut self, other: Proof) {
        match (&self, other) {
            (Self::Proven, Self::Proven) => {
                *self = Self::Proven;
            }
            _ => {
                *self = Self::Maybe;
            }
        }
    }
}

enum ProofError {
    Disproven,
    Domain(DomainError),
}

// #[derive(Default)]
// struct JoinTable {
//     current_subplan: usize,
//     joins: FnvHashMap<Var, JoinTableEntry>,
// }
//
// #[derive(Default)]
// struct JoinTableEntry {
//     in_subplans: BitSet,
//     keys: HashMap<EntityKey, usize>,
// }

impl InMemoryStore {
    pub fn eval_condition(
        &self,
        val: FilterVal,
        condition: &Condition,
        ctx: &DbContext,
    ) -> DomainResult<bool> {
        let walker = ConditionWalker::new(condition);

        match self.eval_cond_var(val, Var(0), walker, ctx) {
            Ok(_) => Ok(true),
            Err(ProofError::Disproven) => Ok(false),
            Err(ProofError::Domain(err)) => Err(err),
        }
    }

    fn eval_cond_var(
        &self,
        val: FilterVal,
        cond_var: Var,
        walker: ConditionWalker,
        ctx: &DbContext,
    ) -> Result<Proof, ProofError> {
        let mut proof = Proof::Proven;

        for clause in walker.clauses(cond_var) {
            match (clause, val) {
                (Clause::Root, _) => {}
                (Clause::IsEntity(expected_def_id), FilterVal::Struct { type_def_id, .. }) => {
                    if type_def_id != *expected_def_id {
                        return Err(ProofError::Disproven);
                    }
                }
                (
                    Clause::MatchProp(prop_id, set_op, set_var),
                    FilterVal::Struct {
                        type_def_id,
                        prop_tree,
                        dynamic_key,
                    },
                ) => {
                    let type_info = ctx.ontology.get_type_info(type_def_id);

                    let Some(data_relationship) = type_info.data_relationships.get(prop_id) else {
                        return Err(ProofError::Disproven);
                    };

                    match &data_relationship.kind {
                        DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                            let attr = prop_tree.get(prop_id).ok_or(ProofError::Disproven)?;
                            proof.merge(self.eval_match_prop(
                                (
                                    FilterVal::from_value(&attr.rel),
                                    FilterVal::from_value(&attr.val),
                                ),
                                *set_op,
                                *set_var,
                                walker,
                                ctx,
                            )?);
                        }
                        DataRelationshipKind::Edge(projection) => {
                            let Some(key) = dynamic_key else {
                                return Err(ProofError::Domain(DomainError::DataStore(anyhow!(
                                    "cannot filter edge without dynamic key"
                                ))));
                            };

                            let edge_collection = self
                                .edge_collections
                                .get(&projection.id)
                                .ok_or(ProofError::Disproven)?;

                            let (target_key, rel_params) = match projection.proj() {
                                (0, 1) => edge_collection.edges.iter().find_map(|edge| {
                                    if edge.from.dynamic_key == *key {
                                        Some((&edge.to, &edge.params))
                                    } else {
                                        None
                                    }
                                }),
                                (1, 0) => edge_collection.edges.iter().find_map(|edge| {
                                    if edge.to.dynamic_key == *key {
                                        Some((&edge.from, &edge.params))
                                    } else {
                                        None
                                    }
                                }),
                                _ => None,
                            }
                            .ok_or(ProofError::Disproven)?;

                            let entity = self
                                .look_up_entity(target_key.type_def_id, &target_key.dynamic_key)
                                .map(|prop_tree| FilterVal::from_entity(target_key, prop_tree))
                                .ok_or(ProofError::Disproven)?;

                            proof.merge(self.eval_match_prop(
                                (FilterVal::from_value(rel_params), entity),
                                *set_op,
                                *set_var,
                                walker,
                                ctx,
                            )?);
                        }
                    }
                }
                (clause, filter_val) => {
                    error!("unhandled combination: {clause:?} on {filter_val:?}");

                    return Err(ProofError::Domain(DomainError::DataStore(anyhow!(
                        "unexpected clause"
                    ))));
                }
            }
        }

        Ok(proof)
    }

    fn eval_match_prop(
        &self,
        (rel, val): (FilterVal, FilterVal),
        set_op: SetOperator,
        set_var: Var,
        walker: ConditionWalker,
        ctx: &DbContext,
    ) -> Result<Proof, ProofError> {
        let members = match walker.set_members(set_var) {
            Members::Empty => {
                return Err(ProofError::Disproven);
            }
            Members::Join(_) => {
                warn!("Handle join");
                return Err(ProofError::Disproven);
            }
            Members::Members(members) => members,
        };

        // The set operators:
        // the left operand is (rel, val)
        // the right operand is the members (rel_term, val_term)
        match set_op {
            SetOperator::ElementIn => {
                for (rel_term, val_term) in members.iter() {
                    let r = prove(self.eval_filter_term(rel, rel_term, walker, ctx))?;
                    let v = prove(self.eval_filter_term(val, val_term, walker, ctx))?;

                    if r && v {
                        return Ok(Proof::Proven);
                    }
                }

                Err(ProofError::Disproven)
            }
            SetOperator::SubsetOf => {
                let attrs = get_seq(val)?;

                for attr in attrs {
                    let mut found = false;

                    for (rel_term, val_term) in members.iter() {
                        let r = prove(self.eval_term(&attr.rel, rel_term, walker, ctx))?;
                        let v = prove(self.eval_term(&attr.val, val_term, walker, ctx))?;

                        if r && v {
                            found = true;
                            break;
                        }
                    }

                    if !found {
                        return Err(ProofError::Disproven);
                    }
                }

                Ok(Proof::Proven)
            }
            SetOperator::SupersetOf => {
                let attrs = get_seq(val)?;

                for (rel_term, val_term) in members.iter() {
                    let mut found = false;

                    for attr in attrs {
                        let r = prove(self.eval_term(&attr.rel, rel_term, walker, ctx))?;
                        let v = prove(self.eval_term(&attr.val, val_term, walker, ctx))?;

                        if r && v {
                            found = true;
                            break;
                        }
                    }

                    if !found {
                        return Err(ProofError::Disproven);
                    }
                }

                Ok(Proof::Proven)
            }
            SetOperator::SetIntersects => {
                let attrs = get_seq(val)?;

                for (rel_term, val_term) in members.iter() {
                    for attr in attrs {
                        let r = prove(self.eval_term(&attr.rel, rel_term, walker, ctx))?;
                        let v = prove(self.eval_term(&attr.val, val_term, walker, ctx))?;

                        if r && v {
                            return Ok(Proof::Proven);
                        }
                    }
                }

                Err(ProofError::Disproven)
            }
            SetOperator::SetEquals => {
                let attrs = get_seq(val)?;

                if attrs.len() != members.size() {
                    return Err(ProofError::Disproven);
                }

                let mut matches: FnvHashMap<usize, usize> = Default::default();
                for (index, (rel_term, val_term)) in members.iter().enumerate() {
                    for attr in attrs {
                        let r = prove(self.eval_term(&attr.rel, rel_term, walker, ctx))?;
                        let v = prove(self.eval_term(&attr.val, val_term, walker, ctx))?;

                        if r && v {
                            *matches.entry(index).or_default() += 1;
                        }
                    }
                }

                if matches.values().all(|count| *count == 1) {
                    Ok(Proof::Proven)
                } else {
                    Err(ProofError::Disproven)
                }
            }
        }
    }

    fn eval_term(
        &self,
        value: &Value,
        term: &CondTerm,
        walker: ConditionWalker,
        ctx: &DbContext,
    ) -> Result<Proof, ProofError> {
        match term {
            CondTerm::Wildcard => Ok(Proof::Proven),
            CondTerm::Variable(cond_var) => {
                self.eval_cond_var(FilterVal::from_value(value), *cond_var, walker, ctx)
            }
            CondTerm::Value(term_val) => {
                if value == term_val {
                    Ok(Proof::Proven)
                } else {
                    Err(ProofError::Disproven)
                }
            }
        }
    }

    fn eval_filter_term(
        &self,
        filter_val: FilterVal,
        term: &CondTerm,
        walker: ConditionWalker,
        ctx: &DbContext,
    ) -> Result<Proof, ProofError> {
        match term {
            CondTerm::Wildcard => Ok(Proof::Proven),
            CondTerm::Variable(cond_var) => self.eval_cond_var(filter_val, *cond_var, walker, ctx),
            CondTerm::Value(term_val) => match filter_val {
                FilterVal::Scalar(scalar) => {
                    if scalar == term_val {
                        Ok(Proof::Proven)
                    } else {
                        Err(ProofError::Disproven)
                    }
                }
                FilterVal::Struct { prop_tree, .. } => {
                    if let Value::Struct(term_props, _) = term_val {
                        if term_props.as_ref() == prop_tree {
                            Ok(Proof::Proven)
                        } else {
                            Err(ProofError::Disproven)
                        }
                    } else {
                        Err(ProofError::Disproven)
                    }
                }
                _ => Err(ProofError::Disproven),
            },
        }
    }
}

fn get_seq(filter_val: FilterVal) -> Result<&[Attribute], ProofError> {
    match filter_val {
        FilterVal::Sequence(attributes) => Ok(attributes),
        _ => Err(ProofError::Disproven),
    }
}

fn prove(result: Result<Proof, ProofError>) -> Result<bool, ProofError> {
    match result {
        Ok(Proof::Proven) => Ok(true),
        Ok(Proof::Maybe) => Ok(false),
        Err(ProofError::Disproven) => Ok(false),
        Err(other) => Err(other),
    }
}
