//!
//! struct {
//!     'foo': struct {
//!         'bar': $a
//!     }
//!     'baz': $a + $b
//! }
//!

use std::collections::HashMap;

use ontol_hir::{visitor::HirVisitor, Node};
use ontol_runtime::vm::proc::BuiltinProc;
use smallvec::SmallVec;

use crate::{
    hir_unify::{UnifierError, UnifierResult, VarSet},
    typed_hir::{self, Meta, TypedBinder, TypedHirNode},
    types::TypeRef,
    SourceSpan,
};

use super::{
    dep_tree::Scope,
    dependent_scope_analyzer::{DepScopeAnalyzer, Path, PropAnalysis},
    scope,
};

#[derive(Debug)]
pub struct ScopeBinder<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub scope: scope::Scope<'m>,
}

impl<'m> ScopeBinder<'m> {
    fn unbound(scope: scope::Scope<'m>) -> Self {
        Self {
            binder: None,
            scope,
        }
    }

    fn into_scope_pattern_binding(self) -> scope::PatternBinding<'m> {
        match self.scope.kind() {
            scope::Kind::Const => scope::PatternBinding::Wildcard(self.scope.1.hir_meta),
            _ => scope::PatternBinding::Scope(
                match self.binder {
                    Some(binder) => binder.var,
                    None => panic!("missing scope binder: {:?}", self.scope),
                },
                self.scope,
            ),
        }
    }
}

pub struct ScopeBuilder<'m> {
    unit_type: TypeRef<'m>,
    in_scope: VarSet,
    next_var: ontol_hir::Var,
    current_prop_path: SmallVec<[u16; 32]>,
    current_prop_analysis_map: Option<HashMap<Path, PropAnalysis>>,
}

impl<'m> ScopeBuilder<'m> {
    pub fn new(next_var: ontol_hir::Var, unit_type: TypeRef<'m>) -> Self {
        Self {
            unit_type,
            in_scope: VarSet::default(),
            next_var,
            current_prop_path: Default::default(),
            current_prop_analysis_map: None,
        }
    }

    pub fn next_var(self) -> ontol_hir::Var {
        self.next_var
    }

    pub fn build(&mut self, node: &TypedHirNode<'m>) -> UnifierResult<ScopeBinder<'m>> {
        // let prop_variant_deps = {
        //     let mut dep_analyzer = DepAnalyzer::default();
        //     dep_analyzer.traverse_kind(node.kind());
        //     dep_analyzer.prop_variant_deps()
        // };
        //
        // panic!("prop_variant_deps: {prop_variant_deps:?}");

        self.build_scope_binder(node)
    }

    fn build_scope_binder(&mut self, node: &TypedHirNode<'m>) -> UnifierResult<ScopeBinder<'m>> {
        let meta = *node.meta();
        match node.kind() {
            ontol_hir::Kind::Var(var) => Ok(self.mk_var_scope(*var, meta)),
            ontol_hir::Kind::Unit | ontol_hir::Kind::Int(_) => Ok(ScopeBinder::unbound(
                self.mk_scope(scope::Kind::Const, meta),
            )),
            ontol_hir::Kind::Let(..) => todo!(),
            ontol_hir::Kind::Call(proc, params) => {
                let (defined_var, dependencies) = match &self.current_prop_analysis_map {
                    Some(map) => match map.get(&self.current_prop_path) {
                        Some(prop_analysis) => (
                            prop_analysis.defined_var,
                            prop_analysis.dependencies.clone(),
                        ),
                        None => {
                            panic!("Property path not found: {:?}", self.current_prop_path)
                        }
                    },
                    None => (None, VarSet::default()),
                };

                let analysis = analyze_expr(node, defined_var)?;
                match &analysis.kind {
                    ExprAnalysisKind::Const => Ok(ScopeBinder::unbound(
                        self.mk_scope(scope::Kind::Const, meta),
                    )),
                    _ => {
                        let binder_var = self.alloc_var();
                        self.invert_expr(
                            *proc,
                            params,
                            analysis,
                            TypedBinder {
                                var: binder_var,
                                ty: node.ty(),
                            },
                            TypedHirNode(ontol_hir::Kind::Var(binder_var), meta),
                            dependencies,
                        )
                    }
                }
            }
            ontol_hir::Kind::Map(arg) => self.build_scope_binder(arg),
            ontol_hir::Kind::Seq(_label, _attr) => Err(UnifierError::SequenceInputNotSupported),
            ontol_hir::Kind::Struct(binder, nodes) => self.enter_binder(*binder, |zelf| {
                if zelf.current_prop_analysis_map.is_none() {
                    zelf.current_prop_analysis_map = Some({
                        let mut dep_analyzer = DepScopeAnalyzer::default();
                        for (index, node) in nodes.iter().enumerate() {
                            dep_analyzer.visit_node(index, node);
                        }
                        let prop_analysis = dep_analyzer.prop_analysis()?;
                        // debug!("prop analysis: {prop_analysis:#?}");
                        prop_analysis
                    });
                }

                // panic!("prop_analysis: {prop_analysis:#?}");

                let mut props = Vec::with_capacity(nodes.len());
                for (disjoint_group, node) in nodes.iter().enumerate() {
                    zelf.enter_child(disjoint_group, |zelf| -> UnifierResult<()> {
                        props.extend(zelf.build_props(node, disjoint_group)?);
                        Ok(())
                    })?;
                }

                let mut union = UnionBuilder::default();
                union.plus_iter(props.iter().map(|prop| &prop.vars));

                Ok(ScopeBinder {
                    binder: Some(TypedBinder {
                        var: binder.0,
                        ty: meta.ty,
                    }),
                    scope: scope::Meta {
                        vars: union.vars,
                        dependencies: VarSet::default(),
                        hir_meta: meta,
                    }
                    .with_kind(scope::Kind::PropSet(scope::PropSet(Some(*binder), props))),
                })
            }),
            ontol_hir::Kind::Prop(..) => panic!("standalone prop"),
            ontol_hir::Kind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            ontol_hir::Kind::Gen(..) => {
                todo!()
            }
            ontol_hir::Kind::Iter(..) => {
                todo!()
            }
            ontol_hir::Kind::Push(..) => {
                todo!()
            }
        }
    }

    fn build_props(
        &mut self,
        node: &TypedHirNode<'m>,
        disjoint_group: usize,
    ) -> UnifierResult<Vec<scope::Prop<'m>>> {
        if let ontol_hir::Kind::Prop(optional, struct_var, prop_id, variants) = node.kind() {
            let mut props = Vec::with_capacity(variants.len());

            for (variant_idx, variant) in variants.iter().enumerate() {
                self.enter_child(variant_idx, |zelf| -> UnifierResult<()> {
                    let (kind, vars, dependencies) = match variant.dimension {
                        ontol_hir::Dimension::Singular => {
                            let mut var_union = UnionBuilder::default();
                            let mut dep_union = UnionBuilder::default();

                            let rel = dep_union
                                .plus_deps(var_union.plus(zelf.enter_child(0, |zelf| {
                                    zelf.build_scope_binder(&variant.attr.rel)
                                })?))
                                .into_scope_pattern_binding();
                            let val = dep_union
                                .plus_deps(var_union.plus(zelf.enter_child(1, |zelf| {
                                    zelf.build_scope_binder(&variant.attr.val)
                                })?))
                                .into_scope_pattern_binding();

                            (
                                scope::PropKind::Attr(rel, val),
                                var_union.vars,
                                dep_union.vars,
                            )
                        }
                        ontol_hir::Dimension::Seq(label) => {
                            let rel = zelf
                                .enter_child(0, |zelf| zelf.build_scope_binder(&variant.attr.rel))?
                                .into_scope_pattern_binding();
                            let val = zelf
                                .enter_child(1, |zelf| zelf.build_scope_binder(&variant.attr.val))?
                                .into_scope_pattern_binding();

                            // Only the label is "visible" to the outside
                            let mut vars = VarSet::default();
                            vars.0.insert(label.0 as usize);

                            (
                                scope::PropKind::Seq(label, rel, val),
                                vars,
                                VarSet::default(),
                            )
                        }
                    };

                    props.push(scope::Prop {
                        struct_var: *struct_var,
                        optional: *optional,
                        prop_id: *prop_id,
                        disjoint_group,
                        dependencies,
                        kind,
                        vars,
                    });

                    Ok(())
                })?;
            }

            Ok(props)
        } else {
            panic!("not a prop: {node}");
        }
    }

    fn invert_expr(
        &mut self,
        proc: BuiltinProc,
        params: &[TypedHirNode<'m>],
        analysis: ExprAnalysis<'m>,
        outer_binder: TypedBinder<'m>,
        let_def: TypedHirNode<'m>,
        dependencies: VarSet,
    ) -> UnifierResult<ScopeBinder<'m>> {
        let (var_param_index, next_analysis) = match analysis.kind {
            ExprAnalysisKind::Const | ExprAnalysisKind::Var(_) => unreachable!(),
            ExprAnalysisKind::FnCall {
                var_param_index,
                child,
                ..
            } => (var_param_index, child),
        };
        let next_analysis = *next_analysis;

        // FIXME: Properly check this
        let inverted_proc = match proc {
            BuiltinProc::Add => BuiltinProc::Sub,
            BuiltinProc::Sub => BuiltinProc::Add,
            BuiltinProc::Mul => BuiltinProc::Div,
            BuiltinProc::Div => BuiltinProc::Mul,
            _ => panic!("Unsupported procedure; cannot invert {proc:?}"),
        };

        let mut inverted_params = Vec::with_capacity(params.len());

        for (param_index, param) in params.iter().enumerate() {
            if param_index != var_param_index {
                inverted_params.push(param.clone());
            }
        }
        inverted_params.insert(var_param_index, let_def);

        let next_let_def = TypedHirNode(
            ontol_hir::Kind::Call(inverted_proc, inverted_params),
            // Is this correct?
            analysis.meta,
        );

        match (params[var_param_index].kind(), next_analysis) {
            (
                ontol_hir::Kind::Var(_),
                ExprAnalysis {
                    kind: ExprAnalysisKind::Var(scoped_var),
                    ..
                },
            ) => {
                let scope_binder = ScopeBinder {
                    binder: Some(outer_binder),
                    scope: scope::Scope(
                        scope::Kind::Let(scope::Let {
                            outer_binder: Some(outer_binder),
                            inner_binder: ontol_hir::Binder(scoped_var),
                            def: next_let_def,
                            sub_scope: Box::new(
                                self.mk_scope(scope::Kind::Const, self.unit_meta()),
                            ),
                        }),
                        scope::Meta {
                            hir_meta: self.unit_meta(),
                            vars: {
                                let mut var_set = VarSet::default();
                                var_set.0.insert(scoped_var.0 as usize);
                                var_set
                            },
                            dependencies,
                        },
                    ),
                };
                Ok(scope_binder)
            }
            (ontol_hir::Kind::Call(next_proc, next_params), child_analysis) => self.invert_expr(
                *next_proc,
                next_params,
                child_analysis,
                outer_binder,
                next_let_def,
                dependencies,
            ),
            _ => panic!("invalid: {}", &params[var_param_index]),
        }
    }

    fn mk_var_scope(&self, var: ontol_hir::Var, hir_meta: typed_hir::Meta<'m>) -> ScopeBinder<'m> {
        let scope = self.mk_scope(scope::Kind::Var(var), hir_meta);
        let scope = if self.in_scope.0.contains(var.0 as usize) {
            scope
        } else {
            scope.union_var(var)
        };

        ScopeBinder {
            binder: Some(TypedBinder {
                var,
                ty: hir_meta.ty,
            }),
            scope,
        }
    }

    fn mk_scope(&self, kind: scope::Kind<'m>, hir_meta: typed_hir::Meta<'m>) -> scope::Scope<'m> {
        scope::Meta {
            vars: VarSet::default(),
            dependencies: VarSet::default(),
            hir_meta,
        }
        .with_kind(kind)
    }

    fn unit_meta(&self) -> Meta<'m> {
        Meta {
            ty: self.unit_type,
            span: SourceSpan::none(),
        }
    }

    fn alloc_var(&mut self) -> ontol_hir::Var {
        let var = self.next_var;
        self.next_var.0 += 1;
        var
    }

    fn enter_binder<T>(
        &mut self,
        binder: ontol_hir::Binder,
        func: impl FnOnce(&mut Self) -> T,
    ) -> T {
        if !self.in_scope.0.insert(binder.0 .0 as usize) {
            panic!("Malformed HIR: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.0.remove(binder.0 .0 as usize);
        value
    }

    fn enter_child<T>(&mut self, index: usize, func: impl FnOnce(&mut Self) -> T) -> T {
        self.current_prop_path.push(index.try_into().unwrap());
        let output = func(self);
        self.current_prop_path.pop();
        output
    }
}

#[derive(Default)]
pub struct UnionBuilder {
    vars: VarSet,
}

impl UnionBuilder {
    fn plus<'m>(&mut self, binder: ScopeBinder<'m>) -> ScopeBinder<'m> {
        self.vars.0.union_with(&binder.scope.vars().0);
        binder
    }

    fn plus_deps<'m>(&mut self, binder: ScopeBinder<'m>) -> ScopeBinder<'m> {
        self.vars.0.union_with(&binder.scope.dependencies().0);
        binder
    }

    fn plus_iter<'a>(&mut self, iter: impl Iterator<Item = &'a VarSet>) {
        for var_set in iter {
            self.vars.0.union_with(&var_set.0);
        }
    }
}

// (+ 5 (* 2 (+ 1 a)))
// (- 1 (/ (- 5 x) 2))

#[derive(Debug)]
struct ExprAnalysis<'m> {
    kind: ExprAnalysisKind<'m>,
    meta: Meta<'m>,
}

// Only one variable is supported for now
#[derive(Debug)]
enum ExprAnalysisKind<'m> {
    Const,
    Var(ontol_hir::Var),
    FnCall {
        var_param_index: usize,
        defining_var: ontol_hir::Var,
        child: Box<ExprAnalysis<'m>>,
    },
}

fn analyze_expr<'m>(
    node: &TypedHirNode<'m>,
    defining_var_hint: Option<ontol_hir::Var>,
) -> UnifierResult<ExprAnalysis<'m>> {
    match node.kind() {
        ontol_hir::Kind::Call(_, args) => {
            let mut kind = ExprAnalysisKind::Const;
            let mut defining_var = None;
            for (index, param) in args.iter().enumerate() {
                let child_analysis = analyze_expr(param, defining_var_hint)?;
                let (child_var, new_kind) = match &child_analysis.kind {
                    ExprAnalysisKind::Const => (None, ExprAnalysisKind::Const),
                    ExprAnalysisKind::Var(child_var) => (
                        Some(*child_var),
                        ExprAnalysisKind::FnCall {
                            var_param_index: index,
                            defining_var: *child_var,
                            child: Box::new(child_analysis),
                        },
                    ),
                    ExprAnalysisKind::FnCall {
                        defining_var: child_var,
                        ..
                    } => (
                        Some(*child_var),
                        ExprAnalysisKind::FnCall {
                            var_param_index: index,
                            defining_var: *child_var,
                            child: Box::new(child_analysis),
                        },
                    ),
                };

                if matches!(new_kind, ExprAnalysisKind::Const) {
                    // nothing to do
                } else if matches!(kind, ExprAnalysisKind::Const) || child_var == defining_var_hint
                {
                    if let Some(defining_var) = defining_var {
                        if child_var == Some(defining_var) {
                            // Currently does not support using the same variable twice in an expression.
                            return Err(UnifierError::MultipleVariablesInExpression(node.span()));
                        }
                    }

                    kind = new_kind;
                    defining_var = child_var;
                }
            }

            Ok(ExprAnalysis {
                kind,
                meta: *node.meta(),
            })
        }
        ontol_hir::Kind::Var(var) => Ok(ExprAnalysis {
            kind: ExprAnalysisKind::Var(*var),
            meta: *node.meta(),
        }),
        _ => Ok(ExprAnalysis {
            kind: ExprAnalysisKind::Const,
            meta: *node.meta(),
        }),
    }
}
