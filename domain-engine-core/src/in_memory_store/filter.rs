#![allow(unused)]

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_runtime::{
    condition::{Clause, CondTerm, Condition, UniVar},
    DefId,
};

use crate::filter::walk_plan::compute_walk_plan;

use super::store::{DynamicKey, InMemoryStore};

pub fn filter(condition: &Condition, store: &InMemoryStore) -> Vec<(DefId, DynamicKey)> {
    compute_walk_plan(condition);
    vec![]
}
