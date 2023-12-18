use ontol_runtime::{
    condition::{Clause, CondTerm, Condition},
    var::Var,
    DefId,
};

pub fn find_entity_id_in_condition_for_var(
    condition: &Condition<CondTerm>,
    var: Var,
) -> Option<DefId> {
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
