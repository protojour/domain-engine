use fnv::FnvHashMap;
use ontol_runtime::{
    attr::{Attr, AttrMatrix},
    ontology::domain::{DataRelationshipKind, EdgeCardinalProjection},
    query::condition::{Clause, CondTerm, Condition, SetOperator},
    value::Value,
    var::Var,
    DefId, RelId,
};

use domain_engine_core::{
    filter::walker::{ConditionWalker, Members},
    DomainError, DomainResult,
};
use tracing::{error, warn};

use crate::core::{DbContext, EdgeVectorData, HyperEdgeTable};

use super::core::{DynamicKey, InMemoryStore, VertexKey};

#[derive(Clone, Copy, Debug)]
pub(super) enum FilterVal<'d> {
    Struct {
        type_def_id: DefId,
        dynamic_key: Option<&'d DynamicKey>,
        prop_tree: &'d FnvHashMap<RelId, Attr>,
    },
    #[allow(unused)]
    Sequence(&'d [Value]),
    Scalar(&'d Value),
}

pub(super) enum FilterAttr<'d> {
    Tuple(FilterVal<'d>, FilterVal<'d>),
    Matrix(&'d AttrMatrix),
    Scalar(FilterVal<'d>),
}

impl<'d> FilterVal<'d> {
    fn from_entity(entity_key: &'d VertexKey, prop_tree: &'d FnvHashMap<RelId, Attr>) -> Self {
        Self::Struct {
            type_def_id: entity_key.type_def_id,
            dynamic_key: Some(&entity_key.dynamic_key),
            prop_tree,
        }
    }

    fn from_value(value: &'d Value) -> Self {
        match value {
            Value::Struct(map, tag) => Self::Struct {
                type_def_id: (*tag).into(),
                dynamic_key: None,
                prop_tree: map,
            },
            Value::Sequence(seq, _) => Self::Sequence(seq.elements()),
            _ => Self::Scalar(value),
        }
    }
}

impl<'d> FilterAttr<'d> {
    fn from_attr(attr: &'d Attr) -> Self {
        match attr {
            Attr::Unit(u) => Self::Scalar(FilterVal::from_value(u)),
            Attr::Tuple(tup) => {
                if tup.elements.len() == 2 {
                    Self::Tuple(
                        FilterVal::from_value(&tup.elements[1]),
                        FilterVal::from_value(&tup.elements[0]),
                    )
                } else {
                    static UNIT: Value = Value::unit();
                    Self::Tuple(
                        FilterVal::from_value(&UNIT),
                        FilterVal::from_value(&tup.elements[0]),
                    )
                }
            }
            Attr::Matrix(mat) => Self::Matrix(mat),
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
                    let def = ctx.ontology.def(type_def_id);

                    let Some(data_relationship) = def.data_relationships.get(prop_id) else {
                        return Err(ProofError::Disproven);
                    };

                    match &data_relationship.kind {
                        DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                            let attr = prop_tree.get(prop_id).ok_or(ProofError::Disproven)?;
                            proof.merge(self.eval_match_attr(
                                FilterAttr::from_attr(attr),
                                *set_op,
                                *set_var,
                                walker,
                                ctx,
                            )?);
                        }
                        DataRelationshipKind::Edge(projection) => {
                            let Some(key) = dynamic_key else {
                                return Err(ProofError::Domain(DomainError::data_store(
                                    "cannot filter edge without dynamic key",
                                )));
                            };

                            fn edge_lookup<'e>(
                                edge_store: &'e HyperEdgeTable,
                                projection: &EdgeCardinalProjection,
                                key: &DynamicKey,
                            ) -> Option<(&'e VertexKey, &'e Value)> {
                                let EdgeVectorData::Keys(source_keys) =
                                    &edge_store.columns[projection.subject.0 as usize].data
                                else {
                                    return None;
                                };

                                let idx = source_keys
                                    .iter()
                                    .position(|elem| &elem.dynamic_key == key)?;

                                let EdgeVectorData::Keys(target_keys) =
                                    &edge_store.columns[projection.object.0 as usize].data
                                else {
                                    return None;
                                };

                                let target_key = &target_keys[idx];

                                static UNIT_VALUE: Value = Value::unit();

                                let target_rel = edge_store
                                    .columns
                                    .iter()
                                    .find_map(|column| match &column.data {
                                        EdgeVectorData::Values(values) => Some(values),
                                        EdgeVectorData::Keys(_) => None,
                                    })
                                    .map(|values| &values[idx])
                                    .unwrap_or(&UNIT_VALUE);

                                Some((target_key, target_rel))
                            }

                            let (target_key, rel_params) = edge_lookup(
                                self.edges
                                    .get(&projection.id)
                                    .ok_or(ProofError::Disproven)?,
                                projection,
                                key,
                            )
                            .ok_or(ProofError::Disproven)?;

                            let entity = self
                                .look_up_vertex(target_key.type_def_id, &target_key.dynamic_key)
                                .map(|prop_tree| FilterVal::from_entity(target_key, prop_tree))
                                .ok_or(ProofError::Disproven)?;

                            proof.merge(self.eval_match_attr(
                                FilterAttr::Tuple(FilterVal::from_value(rel_params), entity),
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

                    return Err(ProofError::Domain(DomainError::data_store(
                        "unexpected clause",
                    )));
                }
            }
        }

        Ok(proof)
    }

    fn eval_match_attr(
        &self,
        attr: FilterAttr,
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
        match (attr, set_op) {
            (FilterAttr::Scalar(val), SetOperator::ElementIn) => {
                for (_rel_term, val_term) in members.iter() {
                    // let r = prove(self.eval_filter_term(rel, rel_term, walker, ctx))?;
                    let v = prove(self.eval_filter_term(val, val_term, walker, ctx))?;

                    if v {
                        return Ok(Proof::Proven);
                    }
                }

                Err(ProofError::Disproven)
            }
            (FilterAttr::Tuple(rel, val), SetOperator::ElementIn) => {
                for (rel_term, val_term) in members.iter() {
                    let r = prove(self.eval_filter_term(rel, rel_term, walker, ctx))?;
                    let v = prove(self.eval_filter_term(val, val_term, walker, ctx))?;

                    if r && v {
                        return Ok(Proof::Proven);
                    }
                }

                Err(ProofError::Disproven)
            }
            (FilterAttr::Matrix(mat), SetOperator::SubsetOf) => {
                let mut tup = Default::default();
                let mut rows = mat.rows();
                while rows.iter_next(&mut tup) {
                    let mut found = false;

                    for (rel_term, val_term) in members.iter() {
                        let (r, v) = self.prove_tuple(
                            tup.elements.as_slice(),
                            rel_term,
                            val_term,
                            walker,
                            ctx,
                        )?;

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
            (FilterAttr::Matrix(mat), SetOperator::SupersetOf) => {
                let mut tup = Default::default();

                for (rel_term, val_term) in members.iter() {
                    let mut found = false;

                    let mut rows = mat.rows();
                    while rows.iter_next(&mut tup) {
                        let (r, v) = self.prove_tuple(
                            tup.elements.as_slice(),
                            rel_term,
                            val_term,
                            walker,
                            ctx,
                        )?;

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
            (FilterAttr::Matrix(mat), SetOperator::SetIntersects) => {
                let mut tup = Default::default();

                for (rel_term, val_term) in members.iter() {
                    let mut rows = mat.rows();
                    while rows.iter_next(&mut tup) {
                        let (r, v) = self.prove_tuple(
                            tup.elements.as_slice(),
                            rel_term,
                            val_term,
                            walker,
                            ctx,
                        )?;

                        if r && v {
                            return Ok(Proof::Proven);
                        }
                    }
                }

                Err(ProofError::Disproven)
            }
            (FilterAttr::Matrix(mat), SetOperator::SetEquals) => {
                if mat.row_count() != members.size() {
                    return Err(ProofError::Disproven);
                }

                let mut matches: FnvHashMap<usize, usize> = Default::default();
                let mut tup = Default::default();

                for (index, (rel_term, val_term)) in members.iter().enumerate() {
                    let mut rows = mat.rows();
                    while rows.iter_next(&mut tup) {
                        let (r, v) = self.prove_tuple(
                            tup.elements.as_slice(),
                            rel_term,
                            val_term,
                            walker,
                            ctx,
                        )?;

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
            _ => Err(ProofError::Disproven),
        }
    }

    fn prove_tuple(
        &self,
        tup: &[&Value],
        rel_term: &CondTerm,
        val_term: &CondTerm,
        walker: ConditionWalker,
        ctx: &DbContext,
    ) -> Result<(bool, bool), ProofError> {
        if tup.len() == 1 {
            let v = prove(self.eval_term(tup[0], val_term, walker, ctx))?;
            Ok((true, v))
        } else {
            let r = prove(self.eval_term(tup[0], rel_term, walker, ctx))?;
            let v = prove(self.eval_term(tup[1], val_term, walker, ctx))?;

            Ok((r, v))
        }
    }

    /*
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
    */

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

// fn get_seq(filter_val: FilterVal) -> Result<&[Attr], ProofError> {
//     match filter_val {
//         FilterVal::Sequence(attributes) => Ok(attributes),
//         _ => Err(ProofError::Disproven),
//     }
// }

fn prove(result: Result<Proof, ProofError>) -> Result<bool, ProofError> {
    match result {
        Ok(Proof::Proven) => Ok(true),
        Ok(Proof::Maybe) => Ok(false),
        Err(ProofError::Disproven) => Ok(false),
        Err(other) => Err(other),
    }
}
