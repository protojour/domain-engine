use ontol_hir::{
    visitor::HirVisitor, Attribute, Binder, Binding, CaptureGroup, Node, PropFlags, PropVariant,
    VarAllocator,
};
use ontol_runtime::{
    value::PropertyId,
    var::{Var, VarSet},
    DefId, MapFlags,
};
use thin_vec::ThinVec;

use crate::{
    typed_hir::{Meta, TypedHir, TypedHirData},
    types::TypeRef,
    SourceSpan,
};

#[derive(Clone, Default)]
pub struct ScopeTracker<'m> {
    pub in_scope: VarSet,
    pub potential_lets: Vec<SpannedLet<'m>>,
}

#[derive(Clone, Copy)]
pub enum ExprMode {
    Expr(MapFlags),
    Match { cond_var: Var, struct_level: usize },
}

impl ExprMode {
    pub fn any_struct(self) -> Self {
        match self {
            Self::Expr(flags) => Self::Expr(flags),
            Self::Match {
                cond_var,
                struct_level,
            } => Self::Match {
                cond_var,
                struct_level: struct_level + 1,
            },
        }
    }

    pub fn match_struct(self, cond_var: Var) -> Self {
        match self {
            Self::Expr(flags) if flags.contains(MapFlags::PURE_PARTIAL) => Self::Expr(flags),
            Self::Expr(_) => Self::Match {
                cond_var,
                struct_level: 0,
            },
            Self::Match {
                cond_var,
                struct_level,
            } => Self::Match {
                cond_var,
                struct_level: struct_level + 1,
            },
        }
    }
}

#[derive(Clone, Copy)]
pub enum Scoped {
    Yes,
    MaybeVoid,
}

impl Scoped {
    pub fn prop(self, flags: MapFlags) -> Self {
        if flags.contains(MapFlags::PURE_PARTIAL) {
            Self::MaybeVoid
        } else {
            self
        }
    }
}

#[derive(Clone)]
pub enum Let<'m> {
    Prop(Attribute<Binding<'m, TypedHir>>, (Var, PropertyId)),
    PropDefault(
        Attribute<Binding<'m, TypedHir>>,
        (Var, PropertyId),
        Attribute<Node>,
    ),
    Regex(ThinVec<ThinVec<CaptureGroup<'m, TypedHir>>>, DefId, Var),
    RegexIter(
        TypedHirData<'m, Binder>,
        ThinVec<ThinVec<CaptureGroup<'m, TypedHir>>>,
        DefId,
        Var,
    ),
}

impl<'m> Let<'m> {
    pub fn defines(&self) -> VarSet {
        match self {
            Self::Prop(attr, _) | Self::PropDefault(attr, ..) => {
                Self::binding_defines(&attr.rel).union(&Self::binding_defines(&attr.val))
            }
            Self::Regex(groups_list, ..) => {
                let mut var_set = VarSet::default();
                for groups in groups_list {
                    for group in groups {
                        var_set.insert(group.binder.hir().var);
                    }
                }
                var_set
            }
            Self::RegexIter(binder, ..) => VarSet::from_iter([binder.hir().var]),
        }
    }

    pub fn dependency(&self) -> Var {
        match self {
            Self::Prop(_, (var, _)) | Self::PropDefault(_, (var, _), _) => *var,
            Self::Regex(.., var) => *var,
            Self::RegexIter(.., var) => *var,
        }
    }

    fn binding_defines(binding: &Binding<'m, TypedHir>) -> VarSet {
        match binding {
            Binding::Binder(binder) => VarSet::from_iter([binder.hir().var]),
            Binding::Wildcard => VarSet::default(),
        }
    }
}

pub type SpannedLet<'m> = (Let<'m>, SourceSpan);

#[derive(Default)]
pub struct Catcher {
    catch_label: Option<ontol_hir::Label>,
}

impl Catcher {
    pub fn make_catch_label(&mut self, var_allocator: &mut VarAllocator) -> ontol_hir::Label {
        match &self.catch_label {
            Some(label) => *label,
            None => {
                let label = ontol_hir::Label(var_allocator.alloc().0);
                self.catch_label = Some(label);
                label
            }
        }
    }

    pub fn finish(self) -> Option<ontol_hir::Label> {
        self.catch_label
    }
}

#[derive(Clone)]
pub enum ExtendedScope<'m> {
    Wildcard,
    Node(ontol_hir::Node),
    SeqUnpack(ThinVec<ontol_hir::Binding<'m, TypedHir>>, Meta<'m>),
}

pub trait NodesExt {
    fn push_node(&mut self, node: ontol_hir::Node) -> ontol_hir::Node;
}

impl NodesExt for ontol_hir::Nodes {
    fn push_node(&mut self, node: ontol_hir::Node) -> ontol_hir::Node {
        self.push(node);
        node
    }
}

#[derive(Clone, Copy)]
pub struct TypeMapping<'m> {
    pub from: TypeRef<'m>,
    pub to: TypeRef<'m>,
}

pub fn scan_immediate_free_vars<const N: usize>(
    arena: &ontol_hir::arena::Arena<TypedHir>,
    nodes: [ontol_hir::Node; N],
) -> VarSet {
    #[derive(Default)]
    struct FreeVarsAnalyzer {
        free_vars: VarSet,
        binders: VarSet,
    }

    impl<'h, 'm: 'h> ontol_hir::visitor::HirVisitor<'h, 'm, TypedHir> for FreeVarsAnalyzer {
        fn visit_prop(
            &mut self,
            flags: PropFlags,
            struct_var: Var,
            prop_id: PropertyId,
            variants: &[PropVariant<'m, TypedHir>],
            arena: &'h ontol_hir::arena::Arena<'m, TypedHir>,
        ) {
            // If the property is optional, don't consider its inner variables as required.
            // This should result in catch blocks being generated at the correct levels.
            if flags.pat_optional() {
                return;
            }

            self.traverse_prop(struct_var, prop_id, variants, arena);
        }

        fn visit_var(&mut self, var: Var) {
            if !self.binders.contains(var) {
                self.free_vars.insert(var);
            }
        }

        fn visit_label(&mut self, label: ontol_hir::Label) {
            let var: Var = label.into();
            if !self.binders.contains(var) {
                self.free_vars.insert(var);
            }
        }

        fn visit_binder(&mut self, var: Var) {
            self.binders.insert(var);
        }

        fn visit_prop_variant(
            &mut self,
            _: usize,
            variant: &ontol_hir::PropVariant<'m, TypedHir>,
            arena: &'h ontol_hir::arena::Arena<'m, TypedHir>,
        ) {
            match variant {
                ontol_hir::PropVariant::Set(variant) => {
                    self.visit_label(*variant.label.hir());
                }
                _ => {
                    self.traverse_prop_variant(variant, arena);
                }
            }
        }

        fn visit_set_entry(
            &mut self,
            index: usize,
            entry: &ontol_hir::SetEntry<'m, TypedHir>,
            arena: &'h ontol_hir::arena::Arena<'m, TypedHir>,
        ) {
            if let Some(label) = entry.0 {
                self.visit_label(*label.hir());
            } else {
                self.traverse_set_entry(index, entry, arena);
            }
        }
    }

    let mut analyzer = FreeVarsAnalyzer::default();
    for (index, node) in nodes.into_iter().enumerate() {
        analyzer.visit_node(index, arena.node_ref(node));
    }
    analyzer.free_vars
}

pub fn scan_all_vars_and_labels<const N: usize>(
    arena: &ontol_hir::arena::Arena<TypedHir>,
    nodes: [ontol_hir::Node; N],
) -> VarSet {
    #[derive(Default)]
    struct FreeVarsAnalyzer {
        free_vars: VarSet,
    }

    impl<'h, 'm: 'h> ontol_hir::visitor::HirVisitor<'h, 'm, TypedHir> for FreeVarsAnalyzer {
        fn visit_var(&mut self, var: Var) {
            self.free_vars.insert(var);
        }

        fn visit_label(&mut self, label: ontol_hir::Label) {
            let var: Var = label.into();
            self.free_vars.insert(var);
        }

        fn visit_binder(&mut self, var: Var) {
            self.free_vars.insert(var);
        }
    }

    let mut analyzer = FreeVarsAnalyzer::default();
    for (index, node) in nodes.into_iter().enumerate() {
        analyzer.visit_node(index, arena.node_ref(node));
    }
    analyzer.free_vars
}
