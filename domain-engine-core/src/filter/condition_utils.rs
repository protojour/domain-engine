use bit_set::BitSet;
use ontol_runtime::condition::{Clause, CondTerm};

pub fn get_clause_vars(clause: &Clause, output: &mut BitSet) {
    match clause {
        Clause::IsEntity(term, _def_id) => {
            get_term_vars(term, output);
        }
        Clause::Prop(var, _, (rel, val)) => {
            output.insert((*var).into());
            get_term_vars(rel, output);
            get_term_vars(val, output);
        }
        Clause::ElementIn(var, term) => {
            output.insert((*var).into());
            get_term_vars(term, output);
        }
        Clause::Or(_clauses) => {
            todo!()
        }
    }
}

pub fn get_term_vars(term: &CondTerm, output: &mut BitSet) {
    if let CondTerm::Var(var) = term {
        output.insert((*var).into());
    }
}
