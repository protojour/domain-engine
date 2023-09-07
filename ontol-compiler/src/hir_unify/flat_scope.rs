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
        write!(
            f,
            "{}: {:?} - {:?} {:?}",
            self.1.var, self.1.deps, self.0, self.1.pub_vars,
        )
    }
}

#[derive(Clone)]
pub struct Meta<'m> {
    /// The specific variable introduced by the node
    pub var: ontol_hir::Var,
    /// All _visible_ variables dominated by this node
    pub pub_vars: VarSet,
    /// Variables that are dependencies of this node
    pub deps: VarSet,

    pub hir_meta: typed_hir::Meta<'m>,
}

#[derive(Clone, Debug)]
pub enum Kind<'m> {
    Var,
    Const(Const<'m>),
    Struct,
    PropVariant(ontol_hir::Optional, ontol_hir::Var, PropertyId),
    PropRelParam,
    PropValue,
    SeqPropVariant(
        TypedLabel<'m>,
        OutputVar,
        ontol_hir::Optional,
        ontol_hir::Var,
        PropertyId,
    ),
    IterElement(OutputVar),
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
