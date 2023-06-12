use std::fmt::Debug;

use bit_set::BitSet;
use ontol_hir::{visitor::HirVisitor, Label, Var};

use crate::{typed_hir::TypedHir, SourceSpan};

pub mod tree;

#[derive(Debug)]
pub enum UnifierError {
    NonUniqueVariableDatapoints(VarSet),
    NoInputBinder,
    SequenceInputNotSupported,
    MultipleVariablesInExpression(SourceSpan),
}

pub type UnifierResult<T> = Result<T, UnifierError>;

struct VariableTracker {
    largest: Var,
}

impl<'s, 'm: 's> HirVisitor<'s, 'm, TypedHir> for VariableTracker {
    fn visit_var(&mut self, var: &Var) {
        self.observe(*var);
    }

    fn visit_binder(&mut self, var: &Var) {
        self.observe(*var);
    }

    fn visit_label(&mut self, label: &Label) {
        self.observe(Var(label.0))
    }
}

impl Default for VariableTracker {
    fn default() -> Self {
        Self { largest: Var(0) }
    }
}

impl VariableTracker {
    fn observe(&mut self, var: Var) {
        if var.0 > self.largest.0 {
            self.largest.0 = var.0;
        }
    }

    fn next_variable(&self) -> Var {
        let idx = self.largest.0 + 1;
        Var(idx)
    }
}

#[derive(Clone, Default)]
pub struct VarSet(pub BitSet);

impl VarSet {
    pub fn iter(&self) -> VarSetIter {
        VarSetIter(self.0.iter())
    }
}

impl Debug for VarSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut set = f.debug_set();
        for bit in &self.0 {
            set.entry(&ontol_hir::Var(bit as u32));
        }

        set.finish()
    }
}

impl<I> From<I> for VarSet
where
    I: IntoIterator<Item = Var>,
{
    fn from(value: I) -> Self {
        Self(value.into_iter().map(|var| var.0 as usize).collect())
    }
}

impl<'a> IntoIterator for &'a VarSet {
    type Item = Var;
    type IntoIter = VarSetIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        VarSetIter(self.0.iter())
    }
}

pub struct VarSetIter<'b>(bit_set::Iter<'b, u32>);

impl<'b> Iterator for VarSetIter<'b> {
    type Item = ontol_hir::Var;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;
        Some(ontol_hir::Var(next.try_into().unwrap()))
    }
}

pub struct DebugVariables<'a>(pub &'a BitSet);

impl<'a> Debug for DebugVariables<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut iterator = self.0.iter().peekable();
        while let Some(var) = iterator.next() {
            write!(f, "{}", Var(var as u32))?;
            if iterator.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}
