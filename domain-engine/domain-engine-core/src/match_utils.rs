use ontol_runtime::{
    condition::{Clause, CondTerm, Condition},
    var::Var,
    DefId,
};
use tracing::debug;

pub fn find_entity_id_in_condition_for_var(condition: &Condition, var: Var) -> Option<DefId> {
    debug!("find root {var} in condition {condition:#?}");

    condition.clauses.iter().find_map(|clause| {
        if let Clause::IsEntity(CondTerm::Var(entity_var), def_id) = clause {
            if *entity_var == var {
                Some(*def_id)
            } else {
                None
            }
        } else {
            None
        }
    })
}
