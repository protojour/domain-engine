use ontol_hir::kind::Optional;
use ontol_runtime::value::PropertyId;

use crate::{
    hir_unify::VarSet,
    typed_hir::{Meta, TypedBinder, TypedHirNode},
};

#[derive(Clone, Debug)]
pub struct Scope<'m> {
    pub kind: Kind<'m>,
    pub vars: VarSet,
    pub meta: Meta<'m>,
}

#[derive(Clone, Debug)]
pub enum Kind<'m> {
    Unit,
    Var(ontol_hir::Var),
    Struct(Struct<'m>),
    Let(Let<'m>),
    // Prop(Prop<'m>),
}

#[derive(Clone, Debug)]
pub struct Struct<'m>(pub ontol_hir::Binder, pub Vec<Prop<'m>>);

#[derive(Clone, Debug)]
pub struct Let<'m> {
    pub outer_binder: Option<TypedBinder<'m>>,
    pub inner_binder: ontol_hir::Binder,
    pub def: TypedHirNode<'m>,
    pub sub_scope: Box<Scope<'m>>,
}

#[derive(Clone, Debug)]
pub struct Prop<'m> {
    pub struct_var: ontol_hir::Var,
    pub optional: Optional,
    pub prop_id: PropertyId,
    pub disjoint_group: usize,
    pub kind: PropKind<'m>,
    pub vars: VarSet,
}

#[derive(Clone, Debug)]
pub enum PropKind<'m> {
    Attr(PatternBinding<'m>, PatternBinding<'m>),
    Seq(PatternBinding<'m>),
}

#[derive(Clone, Debug)]
pub enum PatternBinding<'m> {
    Wildcard,
    Scope(ontol_hir::Var, Scope<'m>),
}

impl<'m> PatternBinding<'m> {
    pub fn hir_pattern_binding(&self) -> ontol_hir::kind::PatternBinding {
        match &self {
            Self::Wildcard => ontol_hir::kind::PatternBinding::Wildcard,
            Self::Scope(binder, _) => ontol_hir::kind::PatternBinding::Binder(*binder),
        }
    }
}
