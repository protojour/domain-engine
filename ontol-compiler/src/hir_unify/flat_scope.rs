use std::fmt::{Debug, Display};

use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc, DefId};

use crate::typed_hir::{self, TypedHirKind, TypedLabel};

use super::VarSet;

pub struct FlatScope<'m> {
    pub scope_nodes: Vec<ScopeNode<'m>>,
}

impl<'m> Display for FlatScope<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for node in &self.scope_nodes {
            writeln!(f, "{node}")?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct ScopeNode<'m>(pub Kind<'m>, pub Meta<'m>);

impl<'m> ScopeNode<'m> {
    #[inline]
    pub fn kind(&self) -> &Kind<'m> {
        &self.0
    }

    #[inline]
    pub fn meta(&self) -> &Meta<'m> {
        &self.1
    }
}

impl<'m> Display for ScopeNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, meta) = (self.kind(), self.meta());
        write!(
            f,
            "{}: {:?} - {:?} {:?}",
            meta.scope_var.0, meta.deps, kind, meta.defs,
        )
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct ScopeVar(pub ontol_hir::Var);

#[derive(Clone)]
pub struct Meta<'m> {
    /// The specific variable introduced by the node
    pub scope_var: ScopeVar,
    /// Variables that are dependencies of this node
    pub deps: VarSet,
    /// All free vars under this node
    pub free_vars: VarSet,
    /// Vars defined atomically by this node
    pub defs: VarSet,

    pub hir_meta: typed_hir::Meta<'m>,
}

#[derive(Clone, Copy)]
pub struct PropDepth(pub u32);

impl PropDepth {
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Debug for PropDepth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "d={}", self.0)
    }
}

#[derive(Clone, Debug)]
pub enum Kind<'m> {
    Var,
    Const(Const<'m>),
    Struct,
    PropVariant(PropDepth, ontol_hir::Optional, ontol_hir::Var, PropertyId),
    PropRelParam,
    PropValue,
    SeqPropVariant(
        TypedLabel<'m>,
        OutputVar,
        ontol_hir::Optional,
        ontol_hir::HasDefault,
        ontol_hir::Var,
        PropertyId,
    ),
    IterElement(ontol_hir::Label, OutputVar),
    Call(BuiltinProc),
    Regex(DefId),
    RegexAlternation,
    RegexCapture(u32),
}

#[derive(Clone)]
pub struct Const<'m>(pub TypedHirKind<'m>);

#[derive(Clone, Copy, Debug)]
pub struct OutputVar(pub ontol_hir::Var);

impl<'m> Debug for Const<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
