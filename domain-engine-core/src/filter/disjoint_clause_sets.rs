use bit_set::BitSet;
use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::condition::{Clause, UniVar};

use super::condition_utils::get_clause_vars;

/// Finds the disjoint clauses.
/// Disjoint clauses are groups with no variable "leakage" to other groups.
/// The return value is grouped indexes into the passed clause slice.
pub fn disjoint_clause_sets(clauses: &[Clause]) -> Vec<Vec<usize>> {
    let mut forest = ClauseForest::default();

    for (index, clause) in clauses.iter().enumerate() {
        let mut var_set = BitSet::new();
        get_clause_vars(clause, &mut var_set);
        forest.union_clause(index, var_set);
    }

    compute_disjoint_sets(&forest)
}

fn compute_disjoint_sets(forest: &ClauseForest) -> Vec<Vec<usize>> {
    let mut by_root: FnvHashMap<Option<UniVar>, Vec<usize>> = Default::default();

    for (index, local_root) in forest.clause_roots.iter().enumerate() {
        let root = local_root.map(|local_root| forest.find_global_root(local_root));
        by_root.entry(root).or_default().push(index);
    }

    let mut by_root = by_root.into_values().collect_vec();
    by_root.sort();
    by_root
}

#[derive(Default)]
struct ClauseForest {
    /// Union-Find for finding disjoint sets of UniVars.
    /// Maps a variable to its root.
    /// The root of a variable is the lowest variable of the variables it's unioned together with.
    forest: FnvHashMap<UniVar, Option<UniVar>>,

    /// Map from a clause index to its local variable root
    clause_roots: Vec<Option<UniVar>>,
}

impl ClauseForest {
    fn union_clause(&mut self, index: usize, var_set: BitSet) {
        let local_root: Option<UniVar> = var_set.iter().min().map(|var| var.into());

        if let Some(local_root) = &local_root {
            for var in &var_set {
                self.union_with_root(var.into(), *local_root);
            }
        }

        let required_len = usize::max(self.clause_roots.len(), index + 1);
        self.clause_roots.resize_with(required_len, || None);
        self.clause_roots[index] = local_root;
    }

    fn union_with_root(&mut self, mut var: UniVar, union_root: UniVar) {
        loop {
            let root_entry = self.forest.entry(var).or_default();
            match root_entry {
                Some(cur_root) => {
                    if *cur_root <= union_root {
                        return;
                    } else {
                        var = *cur_root;
                        *cur_root = union_root;
                    }
                }
                None => {
                    *root_entry = Some(union_root);
                    return;
                }
            };
        }
    }

    fn find_global_root(&self, mut var: UniVar) -> UniVar {
        loop {
            match self.forest.get(&var) {
                Some(Some(root)) => {
                    if root == &var {
                        return var;
                    }
                    var = *root;
                }
                _ => {
                    return var;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bit_set::BitSet;
    use ontol_runtime::{
        condition::{Clause, CondTerm, UniVar},
        value::PropertyId,
        DefId, RelationshipId, Role,
    };

    use crate::filter::disjoint_clause_sets::{compute_disjoint_sets, disjoint_clause_sets};

    use super::ClauseForest;

    const PROP: PropertyId = PropertyId {
        role: Role::Subject,
        relationship_id: RelationshipId(DefId::unit()),
    };

    fn var(value: usize) -> UniVar {
        value.into()
    }

    #[test]
    fn forest_join() {
        let mut forest = ClauseForest::default();
        forest.union_clause(0, BitSet::from_iter([2, 0, 1]));
        forest.union_clause(1, BitSet::from_iter([3, 1, 2]));
        assert_eq!(compute_disjoint_sets(&forest), vec![vec![0, 1]]);
    }

    #[test]
    fn disjoint_clause_set() {
        let disjoint_sets = disjoint_clause_sets(&[
            Clause::Prop(var(6), PROP, (CondTerm::Var(var(7)), CondTerm::Var(var(8)))),
            Clause::Prop(var(3), PROP, (CondTerm::Var(var(4)), CondTerm::Var(var(5)))),
            Clause::Prop(var(1), PROP, (CondTerm::Var(var(2)), CondTerm::Var(var(3)))),
            Clause::IsEntity(CondTerm::Var(var(7)), DefId::unit()),
        ]);
        assert_eq!(disjoint_sets, vec![vec![0, 3], vec![1, 2]]);
    }
}
