use bit_set::BitSet;
use ontol_runtime::condition::{Clause, CondTerm, UniVar};

pub trait Collector {
    fn collect_input_var(&mut self, var: UniVar);
    fn collect_term_var(&mut self, var: UniVar);
}

pub struct AllVars(pub BitSet);

impl Collector for AllVars {
    fn collect_input_var(&mut self, var: UniVar) {
        self.0.insert(var.into());
    }

    fn collect_term_var(&mut self, var: UniVar) {
        self.0.insert(var.into());
    }
}

pub struct TermVars(pub BitSet);

impl Collector for TermVars {
    fn collect_input_var(&mut self, _var: UniVar) {}

    fn collect_term_var(&mut self, var: UniVar) {
        self.0.insert(var.into());
    }
}

pub fn get_clause_vars(clause: &Clause, collector: &mut impl Collector) {
    match clause {
        Clause::Root(var) => {
            collector.collect_input_var(*var);
        }
        Clause::IsEntity(term, _def_id) => {
            get_term_vars(term, collector);
        }
        Clause::Attr(var, _, (rel, val)) => {
            collector.collect_input_var(*var);
            get_term_vars(rel, collector);
            get_term_vars(val, collector);
        }
        Clause::Eq(var, term) => {
            collector.collect_input_var(*var);
            get_term_vars(term, collector);
        }
        Clause::Or(_clauses) => {
            todo!()
        }
    }
}

pub fn get_term_vars(term: &CondTerm, collector: &mut impl Collector) {
    if let CondTerm::Var(var) = term {
        collector.collect_term_var(*var);
    }
}
