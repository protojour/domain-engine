use std::fmt::Debug;

use bit_set::BitSet;
use ontol_hir::visitor::HirVisitor;
use ontol_runtime::format_utils::DebugViaDisplay;
use smartstring::alias::String;
use tracing::{info, warn};

use crate::{
    hir_unify::{expr_builder::ExprBuilder, scope_builder::ScopeBuilder, unifier::Unifier},
    typed_hir::{HirFunc, IntoTypedHirValue, Meta, TypedHir},
    types::Type,
    Compiler, SourceSpan,
};

use self::{
    flat_scope_builder::FlatScopeBuilder, flat_unifier::FlatUnifier, unifier::UnifiedRootNode,
};

mod dep_tree;
mod dependent_scope_analyzer;
mod expr;
mod expr_builder;
mod flat_level_builder;
mod flat_scope;
mod flat_scope_builder;
mod flat_unifier;
mod flat_unifier_table;
mod regroup_match_prop;
mod scope;
mod scope_builder;
mod unifier;
mod unify_props;

#[derive(Debug)]
pub enum UnifierError {
    NonUniqueVariableDatapoints(VarSet),
    NoInputBinder,
    SequenceInputNotSupported,
    MultipleVariablesInExpression(SourceSpan),
    TODO(String),
}

pub type UnifierResult<T> = Result<T, UnifierError>;

const USE_FLAT_UNIFIER: bool = true;
const CLASSIC_UNIFIER_FALLBACK: bool = true;

pub fn unify_to_function<'m>(
    scope: &ontol_hir::RootNode<'m, TypedHir>,
    expr: &ontol_hir::RootNode<'m, TypedHir>,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<HirFunc<'m>> {
    let mut var_tracker = VariableTracker::default();
    var_tracker.track_largest(scope.as_ref());
    var_tracker.track_largest(expr.as_ref());

    let (unified, mut var_allocator) = if !USE_FLAT_UNIFIER {
        unify_classic(scope, expr, var_tracker.var_allocator(), compiler)?
    } else {
        match unify_flat(scope, expr, var_tracker.var_allocator(), compiler) {
            Err(err) => {
                if !CLASSIC_UNIFIER_FALLBACK {
                    return Err(err);
                }

                info!("Using classic unifier output because {err:?}");
                unify_classic(scope, expr, var_tracker.var_allocator(), compiler)?
            }
            Ok(value) => {
                warn!("Using output from flat unifier, which is experimental");
                value
            }
        }
    };

    let scope_ty = scope.as_ref().ty();
    let expr_ty = expr.as_ref().ty();

    match unified.typed_binder {
        Some(arg) => {
            // NB: Error is used in unification tests
            if !matches!(scope_ty, Type::Error) {
                assert_eq!(arg.ty(), scope_ty);
            }
            if !matches!(expr_ty, Type::Error) {
                assert_eq!(unified.node.data().ty(), expr_ty);
            }

            Ok(HirFunc {
                arg,
                body: unified.node,
            })
        }
        None => Ok(HirFunc {
            arg: ontol_hir::Binder {
                var: var_allocator.alloc(),
            }
            .with_meta(Meta {
                ty: scope_ty,
                span: scope.data().span(),
            }),
            body: unified.node,
        }),
    }
}

fn unify_classic<'m>(
    scope: &ontol_hir::RootNode<'m, TypedHir>,
    expr: &ontol_hir::RootNode<'m, TypedHir>,
    var_allocator: ontol_hir::VarAllocator,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<(UnifiedRootNode<'m>, ontol_hir::VarAllocator)> {
    let (scope_binder, var_allocator) = {
        let mut scope_builder = ScopeBuilder::new(var_allocator, scope.arena());
        let scope_binder = scope_builder.build_scope_binder(scope.node())?;
        (scope_binder, scope_builder.var_allocator())
    };

    let (expr, var_allocator) = {
        let mut expr_builder = ExprBuilder::new(var_allocator, &compiler.defs, expr.arena());
        let expr = expr_builder.hir_to_expr(expr.node());
        (expr, expr_builder.var_allocator())
    };

    let mut unifier = Unifier::new(&mut compiler.types, var_allocator);
    let unified = unifier.unify(scope_binder.scope, expr)?;

    Ok((
        UnifiedRootNode {
            typed_binder: unified.typed_binder,
            node: ontol_hir::RootNode::new(unified.node, unifier.hir_arena),
        },
        unifier.var_allocator,
    ))
}

fn unify_flat<'m>(
    scope: &ontol_hir::RootNode<'m, TypedHir>,
    expr: &ontol_hir::RootNode<'m, TypedHir>,
    var_allocator: ontol_hir::VarAllocator,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<(UnifiedRootNode<'m>, ontol_hir::VarAllocator)> {
    let (flat_scope, var_allocator) = {
        let mut scope_builder = FlatScopeBuilder::new(var_allocator, scope.arena());
        let flat_scope = scope_builder.build_flat_scope(scope.node())?;
        (flat_scope, scope_builder.var_allocator())
    };

    let (expr, var_allocator) = {
        let mut expr_builder = ExprBuilder::new(var_allocator, &compiler.defs, expr.arena());
        let expr = expr_builder.hir_to_expr(expr.node());
        (expr, expr_builder.var_allocator())
    };

    let mut unifier = FlatUnifier::new(&mut compiler.types, var_allocator);
    let unified = unifier.unify(flat_scope, expr)?;

    Ok((
        UnifiedRootNode {
            typed_binder: unified.typed_binder,
            node: ontol_hir::RootNode::new(unified.node, unifier.hir_arena),
        },
        unifier.var_allocator,
    ))
}

struct VariableTracker {
    largest: ontol_hir::Var,
}

impl VariableTracker {
    fn track_largest<L: ontol_hir::Lang>(
        &mut self,
        node_ref: ontol_hir::arena::NodeRef<'_, '_, L>,
    ) {
        struct Visitor<'h, 'm, L: ontol_hir::Lang> {
            tracker: &'h mut VariableTracker,
            arena: &'h ontol_hir::arena::Arena<'m, L>,
        }
        impl<'h, 'm: 'h, L: ontol_hir::Lang> ontol_hir::visitor::HirVisitor<'h, 'm, L>
            for Visitor<'h, 'm, L>
        {
            fn arena(&self) -> &'h ontol_hir::arena::Arena<'m, L> {
                self.arena
            }

            fn visit_var(&mut self, var: ontol_hir::Var) {
                self.tracker.observe(var);
            }
            fn visit_binder(&mut self, var: ontol_hir::Var) {
                self.tracker.observe(var);
            }
            fn visit_label(&mut self, label: ontol_hir::Label) {
                self.tracker.observe(ontol_hir::Var(label.0));
            }
        }

        Visitor {
            tracker: self,
            arena: node_ref.arena(),
        }
        .visit_node(0, node_ref.node());
    }
}

impl Default for VariableTracker {
    fn default() -> Self {
        Self {
            largest: ontol_hir::Var(0),
        }
    }
}

impl VariableTracker {
    fn observe(&mut self, var: ontol_hir::Var) {
        if var.0 > self.largest.0 {
            self.largest.0 = var.0;
        }
    }

    fn var_allocator(&self) -> ontol_hir::VarAllocator {
        let idx = self.largest.0 + 1;
        ontol_hir::Var(idx).into()
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct VarSet(pub BitSet);

impl VarSet {
    pub fn iter(&self) -> VarSetIter {
        VarSetIter(self.0.iter())
    }

    #[inline]
    pub fn contains(&self, var: ontol_hir::Var) -> bool {
        self.0.contains(var.0 as usize)
    }

    #[inline]
    pub fn insert(&mut self, var: ontol_hir::Var) -> bool {
        self.0.insert(var.0 as usize)
    }

    #[inline]
    pub fn remove(&mut self, var: ontol_hir::Var) -> bool {
        self.0.remove(var.0 as usize)
    }

    #[inline]
    pub fn union_with(&mut self, other: &Self) {
        self.0.union_with(&other.0);
    }

    #[inline]
    pub fn union(&self, other: &Self) -> Self {
        Self(self.0.union(&other.0).collect())
    }

    #[inline]
    pub fn union_one(&self, var: ontol_hir::Var) -> Self {
        let mut clone = self.clone();
        clone.insert(var);
        clone
    }
}

impl FromIterator<ontol_hir::Var> for VarSet {
    fn from_iter<T: IntoIterator<Item = ontol_hir::Var>>(iter: T) -> Self {
        Self(iter.into_iter().map(|var| var.0 as usize).collect())
    }
}

impl Debug for VarSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut set = f.debug_set();
        for bit in &self.0 {
            set.entry(&DebugViaDisplay(&ontol_hir::Var(bit as u32)));
        }

        set.finish()
    }
}

impl<I> From<I> for VarSet
where
    I: IntoIterator<Item = ontol_hir::Var>,
{
    fn from(value: I) -> Self {
        Self(value.into_iter().map(|var| var.0 as usize).collect())
    }
}

impl<'a> IntoIterator for &'a VarSet {
    type Item = ontol_hir::Var;
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

pub mod test_api {
    use std::fmt::Write;

    use crate::{hir_unify::unify_to_function, mem::Mem, typed_hir::TypedHir, Compiler};

    use super::{flat_scope_builder::FlatScopeBuilder, VariableTracker};

    fn parse_typed<'m>(src: &str) -> ontol_hir::RootNode<'m, TypedHir> {
        ontol_hir::parse::Parser::new(TypedHir)
            .parse_root(src)
            .unwrap()
            .0
    }

    pub fn test_unify(scope: &str, expr: &str) -> String {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, Default::default());
        let func =
            unify_to_function(&parse_typed(scope), &parse_typed(expr), &mut compiler).unwrap();
        let mut output = String::new();
        write!(&mut output, "{func}").unwrap();
        output
    }

    pub fn mk_flat_scope(hir: &str) -> std::string::String {
        let hir_node = parse_typed(hir);

        let mut var_tracker = VariableTracker::default();
        var_tracker.track_largest(hir_node.as_ref());

        let mut builder = FlatScopeBuilder::new(var_tracker.var_allocator(), hir_node.arena());
        let flat_scope = builder.build_flat_scope(hir_node.node()).unwrap();
        let mut output = String::new();
        write!(&mut output, "{flat_scope}").unwrap();

        drop(builder);

        output
    }
}
