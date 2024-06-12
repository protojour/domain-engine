use fnv::FnvHashMap;
use ontol_hir::{visitor::HirVisitor, Node, PropFlags, PropVariant};
use ontol_runtime::{
    var::{Var, VarAllocator, VarSet},
    MapFlags, RelationshipId,
};
use thin_vec::ThinVec;

use crate::{
    typed_hir::{Meta, TypedHir},
    types::TypeRef,
};

use super::{ssa_scope_graph::SpannedLet, ssa_unifier::SsaUnifier};

#[derive(Clone, Default)]
pub struct ScopeTracker<'m> {
    pub in_scope: VarSet,
    pub potential_lets: Vec<SpannedLet<'m>>,
}

#[derive(Clone, Copy)]
pub enum ExprMode {
    Expr {
        flags: MapFlags,
        struct_level: Option<u16>,
    },
    MatchStruct {
        match_var: Var,
        struct_level: u16,
        match_level: u16,
    },
    MatchSet {
        match_var: Var,
        set_cond_var: Var,
        match_level: u16,
        struct_level: Option<u16>,
    },
}

impl ExprMode {
    pub fn any_struct(self) -> Self {
        match self {
            Self::Expr {
                flags,
                struct_level,
            } => Self::Expr {
                flags,
                struct_level: Some(inc_opt_struct_level(struct_level)),
            },
            Self::MatchStruct {
                match_var,
                struct_level,
                match_level,
            } => Self::MatchStruct {
                match_var,
                struct_level: struct_level + 1,
                match_level: match_level + 1,
            },
            Self::MatchSet {
                struct_level,
                match_var,
                match_level,
                ..
            } => Self::MatchStruct {
                match_var,
                struct_level: inc_opt_struct_level(struct_level),
                match_level: match_level + 1,
            },
        }
    }

    pub fn match_struct(self, match_var: Var) -> Self {
        match self {
            Self::Expr {
                flags,
                struct_level,
            } if flags.contains(MapFlags::PURE_PARTIAL) => Self::Expr {
                flags,
                struct_level: Some(inc_opt_struct_level(struct_level)),
            },
            Self::Expr { struct_level, .. } => Self::MatchStruct {
                match_var,
                struct_level: inc_opt_struct_level(struct_level),
                match_level: 0,
            },
            Self::MatchStruct {
                match_var,
                struct_level,
                match_level,
                ..
            } => Self::MatchStruct {
                match_var,
                struct_level: struct_level + 1,
                match_level: match_level + 1,
            },
            Self::MatchSet {
                match_var,
                struct_level,
                match_level,
                ..
            } => Self::MatchStruct {
                match_var,
                struct_level: inc_opt_struct_level(struct_level),
                match_level: match_level + 1,
            },
        }
    }

    pub fn match_set(self, set_cond_var: Var) -> Self {
        match self {
            Self::Expr { struct_level, .. } => Self::MatchSet {
                struct_level,
                match_level: 0,
                match_var: set_cond_var,
                set_cond_var,
            },
            Self::MatchStruct {
                match_var,
                struct_level,
                match_level,
                ..
            } => Self::MatchSet {
                match_var,
                struct_level: Some(struct_level),
                match_level: match_level + 1,
                set_cond_var,
            },
            Self::MatchSet {
                struct_level,
                match_level,
                match_var,
                ..
            } => Self::MatchSet {
                struct_level,
                match_level: match_level + 1,
                match_var,
                set_cond_var,
            },
        }
    }
}

fn inc_opt_struct_level(level: Option<u16>) -> u16 {
    level.map(|level| level + 1).unwrap_or(0)
}

#[derive(Clone, Copy, Debug)]
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

impl<'c, 'm> SsaUnifier<'c, 'm> {
    pub fn scan_immediate_free_vars(
        &self,
        arena: &ontol_hir::arena::Arena<TypedHir>,
        nodes: &[ontol_hir::Node],
    ) -> VarSet {
        struct FreeVarsAnalyzer<'s> {
            free_vars: VarSet,
            binders: VarSet,
            iter_tuple_vars: &'s FnvHashMap<Var, FnvHashMap<u8, Var>>,
        }

        impl<'h, 'm: 'h, 's> ontol_hir::visitor::HirVisitor<'h, 'm, TypedHir> for FreeVarsAnalyzer<'s> {
            fn visit_prop(
                &mut self,
                flags: PropFlags,
                struct_var: Var,
                rel_id: RelationshipId,
                variant: &PropVariant,
                arena: &'h ontol_hir::arena::Arena<'m, TypedHir>,
            ) {
                // If the property is optional, don't consider its inner variables as required.
                // This should result in catch blocks being generated at the correct levels.
                if flags.pat_optional() {
                    return;
                }

                self.traverse_prop(struct_var, rel_id, variant, arena);
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

                if let Some(expand_tuple) = self.iter_tuple_vars.get(&var) {
                    for var in expand_tuple.values() {
                        self.visit_var(*var);
                    }
                }
            }

            fn visit_binder(&mut self, var: Var) {
                self.binders.insert(var);
            }

            fn visit_set_entry(
                &mut self,
                index: usize,
                entry: &ontol_hir::MatrixRow<'m, TypedHir>,
                arena: &'h ontol_hir::arena::Arena<'m, TypedHir>,
            ) {
                if let Some(label) = entry.0 {
                    self.visit_label(*label.hir());
                } else {
                    self.traverse_set_entry(index, entry, arena);
                }
            }
        }

        let mut analyzer = FreeVarsAnalyzer {
            free_vars: Default::default(),
            binders: Default::default(),
            iter_tuple_vars: &self.iter_tuple_vars,
        };
        for (index, node) in nodes.iter().enumerate() {
            analyzer.visit_node(index, arena.node_ref(*node));
        }
        analyzer.free_vars
    }
}

pub fn scan_all_vars_and_labels(
    arena: &ontol_hir::arena::Arena<TypedHir>,
    nodes: impl IntoIterator<Item = Node>,
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
