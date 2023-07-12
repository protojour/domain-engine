use ontol_runtime::value::PropertyId;

use crate::{
    hir_unify::VarSet,
    typed_hir::{self, TypedBinder, TypedHir, TypedHirNode, TypedLabel},
};

#[derive(Clone, Debug)]
pub struct Scope<'m>(pub Kind<'m>, pub Meta<'m>);

impl<'m> Scope<'m> {
    pub fn kind(&self) -> &Kind<'m> {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct Meta<'m> {
    pub vars: VarSet,
    pub dependencies: VarSet,
    pub hir_meta: typed_hir::Meta<'m>,
}

impl<'m> From<typed_hir::Meta<'m>> for Meta<'m> {
    fn from(value: typed_hir::Meta<'m>) -> Self {
        Self {
            vars: VarSet::default(),
            dependencies: VarSet::default(),
            hir_meta: value,
        }
    }
}

/// The kind of scoping
#[derive(Clone, Debug)]
pub enum Kind<'m> {
    /// Constant scope - this node puts no variables into scope.
    Const,
    /// Puts one variable into scope
    Var(ontol_hir::Var),
    /// Puts a set of properties into scope - typically a struct or merged structs
    PropSet(PropSet<'m>),
    /// Puts a function of another (in scope) variable into scope, binding its result to a new binder
    Let(Let<'m>),
    /// Puts a sequence generator into scope
    Gen(Gen<'m>),
}

impl<'m> Kind<'m> {
    #[allow(unused)]
    pub fn debug_short(&self) -> String {
        match self {
            Self::Const => "Const".to_string(),
            Self::Var(var) => format!("Var({var})"),
            Self::PropSet(_) => "PropSet".to_string(),
            Self::Let(let_) => format!("Let({})", let_.inner_binder.var),
            Self::Gen(gen) => format!("Gen({})", gen.input_seq),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PropSet<'m>(pub Option<TypedBinder<'m>>, pub Vec<Prop<'m>>);

#[derive(Clone, Debug)]
pub struct Let<'m> {
    pub outer_binder: Option<TypedBinder<'m>>,
    pub inner_binder: TypedBinder<'m>,
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
    pub optional: ontol_hir::Optional,
    pub prop_id: PropertyId,
    pub disjoint_group: usize,
    pub dependencies: VarSet,
    pub kind: PropKind<'m>,
    pub vars: VarSet,
}

#[derive(Clone, Debug)]
pub enum PropKind<'m> {
    Attr(PatternBinding<'m>, PatternBinding<'m>),
    Seq(
        TypedLabel<'m>,
        ontol_hir::HasDefault,
        PatternBinding<'m>,
        PatternBinding<'m>,
    ),
}

#[derive(Clone, Debug)]
pub enum PatternBinding<'m> {
    Wildcard(typed_hir::Meta<'m>),
    Scope(TypedBinder<'m>, Scope<'m>),
}

impl<'m> PatternBinding<'m> {
    pub fn hir_binding(&self) -> ontol_hir::Binding<'m, TypedHir> {
        match &self {
            Self::Wildcard(_) => ontol_hir::Binding::Wildcard,
            Self::Scope(binder, _) => ontol_hir::Binding::Binder(*binder),
        }
    }
}

impl<'m> super::dep_tree::Scope for Scope<'m> {
    fn vars(&self) -> &VarSet {
        &self.1.vars
    }

    fn dependencies(&self) -> &VarSet {
        &self.1.dependencies
    }
}

impl<'m> super::dep_tree::Scope for Prop<'m> {
    fn vars(&self) -> &VarSet {
        &self.vars
    }

    fn dependencies(&self) -> &VarSet {
        &self.dependencies
    }
}
