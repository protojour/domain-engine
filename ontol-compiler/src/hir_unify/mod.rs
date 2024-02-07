use std::fmt::Debug;

use ontol_hir::visitor::HirVisitor;
use ontol_runtime::{
    var::{Var, VarSet},
    MapFlags,
};
use smartstring::alias::String;

use crate::{
    typed_hir::{HirFunc, IntoTypedHirData, TypedHir, TypedHirData},
    Compiler, SourceSpan,
};

use self::ssa_unifier::SsaUnifier;

mod regex_interpolation;
mod ssa_scope_graph;
mod ssa_unifier;
mod ssa_unifier_scope;
mod ssa_util;

#[derive(Debug)]
pub enum UnifierError {
    NonUniqueVariableDatapoints(VarSet),
    NoInputBinder,
    SequenceInputNotSupported,
    MultipleVariablesInExpression(SourceSpan),
    NonEntityQuery,
    ImpossibleMapping,
    NoIterationSource,
    Unimplemented(String),
    TODO(String),
}

struct UnifiedNode<'m> {
    pub typed_binder: Option<TypedHirData<'m, ontol_hir::Binder>>,
    pub node: ontol_hir::Node,
}

struct UnifiedRootNode<'m> {
    pub typed_binder: Option<TypedHirData<'m, ontol_hir::Binder>>,
    pub node: ontol_hir::RootNode<'m, TypedHir>,
}

pub type UnifierResult<T> = Result<T, UnifierError>;

pub fn unify_to_function<'m>(
    scope: &ontol_hir::RootNode<'m, TypedHir>,
    expr: &ontol_hir::RootNode<'m, TypedHir>,
    map_flags: MapFlags,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<HirFunc<'m>> {
    let mut var_tracker = VariableTracker::default();
    var_tracker.track_largest(scope.as_ref());
    var_tracker.track_largest(expr.as_ref());

    let (unified, mut var_allocator) = unify_ssa(
        scope,
        expr,
        map_flags,
        var_tracker.var_allocator(),
        compiler,
    )?;

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

fn unify_ssa<'m>(
    scope: &ontol_hir::RootNode<'m, TypedHir>,
    expr: &ontol_hir::RootNode<'m, TypedHir>,
    map_flags: MapFlags,
    var_allocator: ontol_hir::VarAllocator,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<(UnifiedRootNode<'m>, ontol_hir::VarAllocator)> {
    let mut unifier = SsaUnifier::new(
        scope.arena(),
        expr.arena(),
        var_allocator,
        map_flags,
        compiler,
    );

    let unified = unifier.unify(scope.node(), expr.node())?;

    Ok((
        UnifiedRootNode {
            typed_binder: unified.typed_binder,
            node: ontol_hir::RootNode::new(unified.node, unifier.out_arena),
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
            fn visit_set_entry(
                &mut self,
                index: usize,
                entry: &ontol_hir::SetEntry<'m, L>,
                arena: &'h ontol_hir::arena::Arena<'m, L>,
            ) {
                if let Some(label) = &entry.0 {
                    self.tracker.observe(Var(L::as_hir(label).0));
                }
                self.traverse_set_entry(index, entry, arena);
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

    use ontol_runtime::MapFlags;

    use crate::{hir_unify::unify_to_function, mem::Mem, typed_hir::TypedHir, Compiler};

    fn parse_typed<'m>(src: &str) -> ontol_hir::RootNode<'m, TypedHir> {
        ontol_hir::parse::Parser::new(TypedHir)
            .parse_root(src)
            .unwrap()
            .0
    }

    pub fn test_unify(scope: &str, expr: &str) -> String {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, Default::default());
        let func = unify_to_function(
            &parse_typed(scope),
            &parse_typed(expr),
            MapFlags::empty(),
            &mut compiler,
        )
        .unwrap();
        let mut output = String::new();
        write!(&mut output, "{func}").unwrap();
        output
    }
}
