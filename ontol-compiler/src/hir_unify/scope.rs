use ontol_runtime::{
    value::PropertyId,
    var::{Var, VarSet},
    DefId,
};

use crate::typed_hir::{self, TypedHir, TypedHirData, UNIT_META};

#[derive(Clone, Debug)]
pub struct Scope<'m>(pub Kind<'m>, pub Meta<'m>);

impl<'m> Scope<'m> {
    pub fn kind(&self) -> &Kind<'m> {
        &self.0
    }
}

pub fn constant<'m>() -> Scope<'m> {
    Scope(
        Kind::Const,
        Meta {
            hir_meta: UNIT_META,
            vars: VarSet::default(),
            dependencies: VarSet::default(),
        },
    )
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
    Var(Var),
    /// Puts a set of properties into scope - typically a struct or merged structs
    PropSet(PropSet<'m>),
    /// Puts a function of another (in scope) variable into scope, binding its result to a new binder
    Let(Let<'m>),
    /// Puts a sequence generator into scope
    Gen(Gen<'m>),
    /// Puts the result of a regex match into scope
    Regex(Var, DefId, Box<[ScopeCaptureGroup<'m>]>),
    /// Escape one level of scope
    Escape(Box<Self>),
}

impl<'m> Kind<'m> {
    #[allow(unused)]
    pub fn debug_short(&self) -> String {
        match self {
            Self::Const => "Const".to_string(),
            Self::Var(var) => format!("Var({var})"),
            Self::PropSet(prop_set) => format!(
                "PropSet({:?})",
                prop_set
                    .1
                    .iter()
                    .map(|prop| prop.prop_id)
                    .collect::<Vec<_>>()
            ),
            Self::Let(let_) => format!("Let({})", let_.inner_binder.hir().var),
            Self::Gen(gen) => format!("Gen({})", gen.input_seq),
            Self::Regex(_, _, captures) => format!("Regex({:?})", captures),
            Self::Escape(inner) => format!("Esc({})", inner.debug_short()),
        }
    }

    fn collect_seq_labels(&self, output: &mut VarSet) {
        match self {
            Self::Const => {}
            Self::Var(_) => {}
            Self::PropSet(prop_set) => {
                for prop in &prop_set.1 {
                    prop.collect_seq_labels(output);
                }
            }
            Self::Let(let_scope) => {
                let_scope.sub_scope.0.collect_seq_labels(output);
            }
            Self::Gen(gen) => {
                output.insert(gen.input_seq);
                gen.bindings.0.collect_seq_labels(output);
                gen.bindings.1.collect_seq_labels(output);
            }
            Self::Regex(..) => {}
            Self::Escape(inner) => {
                inner.collect_seq_labels(output);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct PropSet<'m>(
    pub Option<TypedHirData<'m, ontol_hir::Binder>>,
    pub Vec<Prop<'m>>,
);

#[derive(Clone, Debug)]
pub struct Let<'m> {
    pub outer_binder: Option<TypedHirData<'m, ontol_hir::Binder>>,
    pub inner_binder: TypedHirData<'m, ontol_hir::Binder>,
    pub def: ontol_hir::RootNode<'m, TypedHir>,
    pub sub_scope: Box<Scope<'m>>,
}

#[derive(Clone, Debug)]
pub struct Gen<'m> {
    pub input_seq: Var,
    pub output_seq: Var,
    pub bindings: Box<(PatternBinding<'m>, PatternBinding<'m>)>,
}

#[derive(Clone, Debug)]
pub struct Prop<'m> {
    pub struct_var: Var,
    pub flags: ontol_hir::PropFlags,
    pub prop_id: PropertyId,
    pub disjoint_group: usize,
    pub dependencies: VarSet,
    pub kind: PropKind<'m>,
    pub vars: VarSet,
}

impl<'m> Prop<'m> {
    fn collect_seq_labels(&self, output: &mut VarSet) {
        match &self.kind {
            PropKind::Seq(label, _, rel, val) => {
                output.insert(Var(label.hir().0));
                rel.collect_seq_labels(output);
                val.collect_seq_labels(output);
            }
            PropKind::Attr(rel, val) => {
                rel.collect_seq_labels(output);
                val.collect_seq_labels(output);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum PropKind<'m> {
    Attr(PatternBinding<'m>, PatternBinding<'m>),
    Seq(
        TypedHirData<'m, ontol_hir::Label>,
        ontol_hir::HasDefault,
        PatternBinding<'m>,
        PatternBinding<'m>,
    ),
}

#[derive(Clone, Debug)]
pub enum PatternBinding<'m> {
    Wildcard(typed_hir::Meta<'m>),
    Scope(TypedHirData<'m, ontol_hir::Binder>, Scope<'m>),
}

impl<'m> PatternBinding<'m> {
    pub fn hir_binding(&self) -> ontol_hir::Binding<'m, TypedHir> {
        match &self {
            Self::Wildcard(_) => ontol_hir::Binding::Wildcard,
            Self::Scope(binder, _) => ontol_hir::Binding::Binder(*binder),
        }
    }

    fn collect_seq_labels(&self, output: &mut VarSet) {
        if let Self::Scope(_, scope) = self {
            scope.0.collect_seq_labels(output);
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScopeCaptureGroup<'m> {
    pub index: u32,
    pub binder: TypedHirData<'m, ontol_hir::Binder>,
}

impl<'m> super::dep_tree::Scope for Scope<'m> {
    fn all_vars(&self) -> &VarSet {
        &self.1.vars
    }

    fn dependencies(&self) -> &VarSet {
        &self.1.dependencies
    }

    fn scan_seq_labels(&self, output: &mut VarSet) {
        self.0.collect_seq_labels(output);
    }
}

impl<'m> super::dep_tree::Scope for Prop<'m> {
    fn all_vars(&self) -> &VarSet {
        &self.vars
    }

    fn dependencies(&self) -> &VarSet {
        &self.dependencies
    }

    fn scan_seq_labels(&self, output: &mut VarSet) {
        self.collect_seq_labels(output);
    }
}
