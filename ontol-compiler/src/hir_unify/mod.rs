use std::fmt::Debug;

use bit_set::BitSet;
use ontol_hir::{visitor::HirVisitor, Variable};

use crate::typed_hir::TypedHir;

pub mod unifier;
pub mod unifier2;

mod tagged_node;
mod unification_tree;
mod unification_tree2;
mod unified_target_node;
mod var_path;

#[derive(Debug)]
pub enum UnifierError {
    NonUniqueVariableDatapoints(BitSet),
    NoInputBinder,
    SequenceInputNotSupported,
}

struct VariableTracker {
    largest: Variable,
}

impl<'a> HirVisitor<'a, TypedHir> for VariableTracker {
    fn visit_variable(&mut self, variable: &Variable) {
        self.observe(*variable);
    }

    fn visit_binder(&mut self, variable: &Variable) {
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

pub struct DebugVariables<'a>(pub &'a BitSet);

impl<'a> Debug for DebugVariables<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut iterator = self.0.iter().peekable();
        while let Some(var) = iterator.next() {
            write!(f, "{}", Variable(var as u32))?;
            if iterator.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}
