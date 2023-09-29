use itertools::Itertools;
use ontol_runtime::condition::Condition;

use crate::filter::disjoint_clause_sets::disjoint_clause_sets;

pub enum WalkPlan {}

pub fn compute_walk_plan(condition: &Condition) -> Vec<WalkPlan> {
    let clauses = &condition.clauses;

    for group in disjoint_clause_sets(clauses) {
        let clauses = group.into_iter().map(|index| &clauses[index]).collect_vec();
    }

    vec![]
}
