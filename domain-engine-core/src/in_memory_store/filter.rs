#![allow(unused)]

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_runtime::{
    condition::{Clause, CondTerm, Condition},
    ontology::Ontology,
    DefId,
};

use crate::filter::plan::compute_plans;

use super::store::{DynamicKey, InMemoryStore};

pub fn filter(
    condition: &Condition,
    store: &InMemoryStore,
    ontology: &Ontology,
) -> Vec<(DefId, DynamicKey)> {
    compute_plans(condition, ontology);
    vec![]
}
