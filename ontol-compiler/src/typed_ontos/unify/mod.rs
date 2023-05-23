use bit_set::BitSet;
use ontos::{visitor::OntosVisitor, Variable};

use super::lang::TypedOntos;

pub mod unifier;

mod tagged_node;
mod unification_tree;
mod var_path;

#[derive(Debug)]
pub enum UnifierError {
    NonUniqueVariableDatapoints(BitSet),
    NoInputBinder,
}

struct VariableTracker {
    largest: Variable,
}

impl<'a> OntosVisitor<'a, TypedOntos> for VariableTracker {
    fn visit_variable(&mut self, variable: &mut Variable) {
        self.observe(*variable);
    }

    fn visit_binder(&mut self, variable: &mut Variable) {
        self.observe(*variable);
    }
}

impl Default for VariableTracker {
    fn default() -> Self {
        Self {
            largest: Variable(0),
        }
    }
}

impl VariableTracker {
    fn observe(&mut self, var: Variable) {
        if var.0 > self.largest.0 {
            self.largest.0 = var.0;
        }
    }

    fn next_variable(&self) -> Variable {
        let idx = self.largest.0 + 1;
        Variable(idx)
    }
}
