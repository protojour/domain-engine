use std::fmt::Debug;

use ontol_hir::visitor::HirVisitor;
use ontol_runtime::var::{Var, VarSet};
use smartstring::alias::String;
use tracing::{info, warn};

use crate::{
    hir_unify::{expr_builder::ExprBuilder, scope_builder::ScopeBuilder, unifier::Unifier},
    typed_hir::{HirFunc, IntoTypedHirData, TypedHir},
    Compiler, SourceSpan, CLASSIC_UNIFIER_FALLBACK, USE_FLAT_UNIFIER,
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
mod flat_unifier_expr_to_hir;
mod flat_unifier_impl;
mod flat_unifier_regex;
mod flat_unifier_scope_assign;
mod flat_unifier_table;
mod regroup_match_prop;
mod scope;
mod scope_builder;
mod seq_type_infer;
mod unifier;
mod unify_props;

#[derive(Debug)]
pub enum UnifierError {
    NonUniqueVariableDatapoints(VarSet),
    NoInputBinder,
    SequenceInputNotSupported,
    MultipleVariablesInExpression(SourceSpan),
    NonEntityQuery,
    TODO(String),
}

pub type UnifierResult<T> = Result<T, UnifierError>;

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
                if !CLASSIC_UNIFIER_FALLBACK || !matches!(&err, UnifierError::TODO(_)) {
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

    Ok(HirFunc {
        arg: unified.typed_binder.unwrap_or_else(|| {
            ontol_hir::Binder {
                var: var_allocator.alloc(),
            }
            .with_meta(*scope.data().meta())
        }),
        body: unified.node,
    })
}

fn unify_classic<'m>(
    scope: &ontol_hir::RootNode<'m, TypedHir>,
    expr: &ontol_hir::RootNode<'m, TypedHir>,
    var_allocator: ontol_hir::VarAllocator,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<(UnifiedRootNode<'m>, ontol_hir::VarAllocator)> {
    let (scope_binder, var_allocator) = {
        let mut scope_builder = ScopeBuilder::new(var_allocator, scope.arena());
        let scope_binder = scope_builder.build_scope_binder(scope.node(), None)?;
        (scope_binder, scope_builder.var_allocator())
    };

    let (expr, var_allocator) = {
        let mut expr_builder = ExprBuilder::new(var_allocator, &compiler.defs);
        let expr = expr_builder.hir_to_expr(expr.as_ref());
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
        let mut expr_builder = ExprBuilder::new(var_allocator, &compiler.defs);
        let expr = expr_builder.hir_to_expr(expr.as_ref());
        (expr, expr_builder.var_allocator())
    };

    let mut unifier = FlatUnifier::new(&mut compiler.types, &compiler.relations, var_allocator);
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
    largest: Var,
}

impl VariableTracker {
    fn track_largest<L: ontol_hir::Lang>(
        &mut self,
        node_ref: ontol_hir::arena::NodeRef<'_, '_, L>,
    ) {
        struct Visitor<'h> {
            tracker: &'h mut VariableTracker,
        }
        impl<'h, 'm: 'h, L: ontol_hir::Lang + 'h> ontol_hir::visitor::HirVisitor<'h, 'm, L>
            for Visitor<'h>
        {
            fn visit_var(&mut self, var: Var) {
                self.tracker.observe(var);
            }
            fn visit_binder(&mut self, var: Var) {
                self.tracker.observe(var);
            }
            fn visit_label(&mut self, label: ontol_hir::Label) {
                self.tracker.observe(Var(label.0));
            }
        }

        Visitor { tracker: self }.visit_node(0, node_ref);
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

    fn var_allocator(&self) -> ontol_hir::VarAllocator {
        let idx = self.largest.0 + 1;
        Var(idx).into()
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
