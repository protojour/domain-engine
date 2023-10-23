use std::collections::{BTreeMap, HashMap};

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_runtime::{
    value::{Attribute, Data, PropertyId, Value},
    var::Var,
    DefId, Role,
};

use crate::filter::plan::{PlanEntry, Scalar};

use super::in_memory_core::{DynamicKey, EntityKey, InMemoryStore};

pub(super) enum FilterVal<'d> {
    Struct {
        type_def_id: DefId,
        dynamic_key: Option<&'d DynamicKey>,
        prop_tree: &'d BTreeMap<PropertyId, Attribute>,
    },
    Sequence(&'d [Attribute]),
    Scalar(&'d Value),
}

impl<'d> FilterVal<'d> {
    fn from_entity(
        entity_key: &'d EntityKey,
        prop_tree: &'d BTreeMap<PropertyId, Attribute>,
    ) -> Self {
        Self::Struct {
            type_def_id: entity_key.type_def_id,
            dynamic_key: Some(&entity_key.dynamic_key),
            prop_tree,
        }
    }

    fn from_value(value: &'d Value) -> Self {
        match &value.data {
            Data::Struct(map) => Self::Struct {
                type_def_id: value.type_def_id,
                dynamic_key: None,
                prop_tree: map,
            },
            Data::Sequence(seq) => Self::Sequence(seq),
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

struct Disproven;

#[derive(Default)]
struct JoinTable {
    current_subplan: usize,
    joins: FnvHashMap<Var, JoinTableEntry>,
}

#[derive(Default)]
struct JoinTableEntry {
    in_subplans: BitSet,
    keys: HashMap<EntityKey, usize>,
}

impl InMemoryStore {
    pub fn eval_filter_plan(&self, val: &FilterVal, plan: &[PlanEntry]) -> bool {
        if plan.is_empty() {
            return true;
        }

        let mut join_table = JoinTable::default();

        for (plan_idx, plan_entry) in plan.iter().enumerate() {
            join_table.current_subplan = plan_idx;

            if self
                .eval_filter_plan_entry(val, plan_entry, &mut join_table)
                .is_err()
            {
                return false;
            }
        }

        join_table.joins.is_empty()
    }

    fn eval_filter_plan_entry(
        &self,
        val: &FilterVal,
        entry: &PlanEntry,
        join_table: &mut JoinTable,
    ) -> Result<Proof, Disproven> {
        match (entry, val) {
            (PlanEntry::EntitiesOf(def_id, entries), FilterVal::Struct { type_def_id, .. }) => {
                if def_id != type_def_id {
                    return Err(Disproven);
                }

                if entries.is_empty() {
                    return Ok(Proof::Proven);
                }

                for entry in entries {
                    self.eval_filter_plan_entry(val, entry, join_table)?;
                }

                Ok(Proof::Proven)
            }
            (PlanEntry::JoinRoot(var, sub_entries), _) => {
                let join_table_entry = join_table.joins.remove(var).ok_or(Disproven)?;

                let observed_count = join_table_entry.in_subplans.iter().count();

                let mut proof = Proof::Proven;

                for (typed_key, count) in join_table_entry.keys {
                    if count < observed_count {
                        return Err(Disproven);
                    }

                    let filter_val = self
                        .look_up_entity(typed_key.type_def_id, &typed_key.dynamic_key)
                        .map(|prop_tree| FilterVal::from_entity(&typed_key, prop_tree))
                        .ok_or(Disproven)?;

                    for entry in sub_entries {
                        proof.merge(self.eval_filter_plan_entry(&filter_val, entry, join_table)?);
                    }
                }

                Ok(proof)
            }
            (PlanEntry::Attr(prop_id, entries), FilterVal::Struct { prop_tree: map, .. }) => {
                let attr = map.get(prop_id).ok_or(Disproven)?;
                self.eval_attr_entries(attr, entries, join_table)
            }
            (PlanEntry::AllAttrs(prop_id, entries), FilterVal::Struct { prop_tree: map, .. }) => {
                let attr = map.get(prop_id).ok_or(Disproven)?;
                let Data::Sequence(seq) = &attr.value.data else {
                    return Err(Disproven);
                };

                let mut proof = Proof::Proven;

                for attr in seq {
                    proof.merge(self.eval_attr_entries(attr, entries, join_table)?);
                }

                Ok(proof)
            }
            (
                PlanEntry::Edge(prop_id, edge_attr),
                FilterVal::Struct {
                    dynamic_key: Some(key),
                    ..
                },
            ) => {
                let edge_collection = self
                    .edge_collections
                    .get(&prop_id.relationship_id)
                    .ok_or(Disproven)?;

                let (target_key, rel_params) = match prop_id.role {
                    Role::Subject => edge_collection.edges.iter().find_map(|edge| {
                        if edge.from.dynamic_key == **key {
                            Some((&edge.to, FilterVal::from_value(&edge.params)))
                        } else {
                            None
                        }
                    }),
                    Role::Object => edge_collection.edges.iter().find_map(|edge| {
                        if edge.to.dynamic_key == **key {
                            Some((&edge.from, FilterVal::from_value(&edge.params)))
                        } else {
                            None
                        }
                    }),
                }
                .ok_or(Disproven)?;

                let entity = self
                    .look_up_entity(target_key.type_def_id, &target_key.dynamic_key)
                    .map(|prop_tree| FilterVal::from_entity(target_key, prop_tree))
                    .ok_or(Disproven)?;

                let mut proof = Proof::Proven;

                for rel_entry in &edge_attr.rel {
                    proof.merge(self.eval_filter_plan_entry(&rel_params, rel_entry, join_table)?);
                }

                for val_entry in &edge_attr.val {
                    proof.merge(self.eval_filter_plan_entry(&entity, val_entry, join_table)?);
                }

                Ok(proof)
            }
            (PlanEntry::AllEdges(..), _) => todo!(),
            (PlanEntry::Eq(pred_scalar), FilterVal::Scalar(val_scalar)) => {
                match (&val_scalar.data, pred_scalar) {
                    (Data::Text(data), Scalar::Text(pred)) => {
                        if data.as_str() == pred.as_ref() {
                            Ok(Proof::Proven)
                        } else {
                            Err(Disproven)
                        }
                    }
                    _ => Err(Disproven),
                }
            }
            (PlanEntry::In(..), _) => todo!(),
            (
                PlanEntry::Join(var),
                FilterVal::Struct {
                    type_def_id,
                    dynamic_key: Some(dynamic_key),
                    ..
                },
            ) => {
                // Register key in join table. Each key must be observed the same number of times
                // from any branch leading into the same join var.

                let join = join_table.joins.entry(*var).or_default();
                join.in_subplans.insert(join_table.current_subplan);

                let observed_count = join
                    .keys
                    .entry(EntityKey {
                        type_def_id: *type_def_id,
                        dynamic_key: (*dynamic_key).clone(),
                    })
                    .or_default();
                (*observed_count) += 1;

                Ok(Proof::Maybe)
            }
            _ => Err(Disproven),
        }
    }

    fn eval_attr_entries(
        &self,
        attr: &Attribute,
        entries: &[PlanEntry],
        join_table: &mut JoinTable,
    ) -> Result<Proof, Disproven> {
        if entries.is_empty() {
            return Ok(Proof::Proven);
        }

        let mut proof = Proof::Proven;

        for entry in entries {
            proof.merge(self.eval_filter_plan_entry(
                &FilterVal::from_value(&attr.value),
                entry,
                join_table,
            )?);
        }

        Ok(proof)
    }
}
