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

impl<'m> Scope<'m> {
    pub fn union_var(mut self, var: ontol_hir::Var) -> Self {
        self.vars.0.insert(var.0 as usize);
        self
    }
}

impl<'m> Scope<'m> {
    #[allow(unused)]
    pub fn debug_short(&self) -> String {
        match &self.kind {
            Kind::Const => format!("Const"),
            Kind::Var(var) => format!("Var({var})"),
            Kind::Struct(_) => format!("Struct"),
            Kind::Let(let_) => format!("Let({})", let_.inner_binder.0),
            Kind::Gen(gen) => format!("Gen({})", gen.input_seq),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Kind<'m> {
    Const,
    Var(ontol_hir::Var),
    Struct(Struct<'m>),
    Let(Let<'m>),
    Gen(Gen<'m>),
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
pub struct Gen<'m> {
    pub input_seq: ontol_hir::Var,
    pub output_seq: ontol_hir::Var,
    pub bindings: Box<(PatternBinding<'m>, PatternBinding<'m>)>,
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
    Seq(ontol_hir::Label, PatternBinding<'m>, PatternBinding<'m>),
}

#[derive(Clone, Debug)]
pub enum PatternBinding<'m> {
    Wildcard(Meta<'m>),
    Scope(ontol_hir::Var, Scope<'m>),
}

impl<'m> PatternBinding<'m> {
    pub fn hir_pattern_binding(&self) -> ontol_hir::kind::PatternBinding {
        match &self {
            Self::Wildcard(_) => ontol_hir::kind::PatternBinding::Wildcard,
            Self::Scope(binder, _) => ontol_hir::kind::PatternBinding::Binder(*binder),
        }
    }
}

impl<'m> super::hierarchy::Scope for Scope<'m> {
    fn vars(&self) -> &VarSet {
        &self.vars
    }
}

impl<'m> super::hierarchy::Scope for Prop<'m> {
    fn vars(&self) -> &VarSet {
        &self.vars
    }
}
