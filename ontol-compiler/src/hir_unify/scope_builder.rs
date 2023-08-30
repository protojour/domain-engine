use std::collections::HashMap;

use ontol_hir::{visitor::HirVisitor, GetKind, SeqPropertyVariant};
use ontol_runtime::vm::proc::BuiltinProc;
use smallvec::SmallVec;

use crate::{
    hir_unify::{UnifierError, UnifierResult, VarSet},
    typed_hir::{self, TypedBinder, TypedHirNode},
    types::TypeRef,
    NO_SPAN,
};

use super::{
    dep_tree::Scope,
    dependent_scope_analyzer::{DepScopeAnalyzer, Path, PropAnalysis},
    scope::{self, ScopeCaptureGroup},
};

#[derive(Debug)]
pub struct ScopeBinder<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub scope: scope::Scope<'m>,
}

impl<'m> ScopeBinder<'m> {
    fn into_scope_pattern_binding(self) -> scope::PatternBinding<'m> {
        match self.scope.kind() {
            scope::Kind::Const => scope::PatternBinding::Wildcard(self.scope.1.hir_meta),
            _ => scope::PatternBinding::Scope(
                match self.binder {
                    Some(binder) => binder,
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
    var_allocator: ontol_hir::VarAllocator,
    current_prop_path: SmallVec<[u16; 32]>,
    current_prop_analysis_map: Option<HashMap<Path, PropAnalysis>>,
}

impl<'m> ScopeBuilder<'m> {
    pub fn new(var_allocator: ontol_hir::VarAllocator, unit_type: TypeRef<'m>) -> Self {
        Self {
            unit_type,
            in_scope: VarSet::default(),
            var_allocator,
            current_prop_path: Default::default(),
            current_prop_analysis_map: None,
        }
    }

    pub fn var_allocator(self) -> ontol_hir::VarAllocator {
        self.var_allocator
    }

    pub fn build_scope_binder(
        &mut self,
        node: &TypedHirNode<'m>,
    ) -> UnifierResult<ScopeBinder<'m>> {
        let hir_meta = *node.meta();
        match node.kind() {
            ontol_hir::Kind::Var(var) => Ok(ScopeBinder {
                binder: Some(TypedBinder {
                    var: *var,
                    meta: hir_meta,
                }),
                scope: scope::Scope(
                    scope::Kind::Var(*var),
                    scope::Meta {
                        vars: if self.in_scope.0.contains(var.0 as usize) {
                            VarSet::default()
                        } else {
                            VarSet::from([*var])
                        },
                        dependencies: VarSet::default(),
                        hir_meta,
                    },
                ),
            }),
            ontol_hir::Kind::Unit
            | ontol_hir::Kind::I64(_)
            | ontol_hir::Kind::F64(_)
            | ontol_hir::Kind::String(_)
            | ontol_hir::Kind::Const(_) => Ok(ScopeBinder {
                binder: None,
                scope: scope::Scope(
                    scope::Kind::Const,
                    scope::Meta {
                        vars: VarSet::default(),
                        dependencies: VarSet::default(),
                        hir_meta,
                    },
                ),
            }),
            ontol_hir::Kind::Let(..) => todo!(),
            ontol_hir::Kind::Call(proc, params) => {
                let (defined_var, dependencies) = match &self.current_prop_analysis_map {
                    Some(map) => {
                        let Some(prop_analysis) = map.get(&self.current_prop_path) else {
                            panic!("Property path not found: {:?}", self.current_prop_path);
                        };

                        (
                            prop_analysis.defined_var,
                            prop_analysis.dependencies.clone(),
                        )
                    }
                    None => (None, VarSet::default()),
                };

                let analysis = analyze_expr(node, defined_var)?;
                match &analysis.kind {
                    ExprAnalysisKind::Const => Ok(ScopeBinder {
                        binder: None,
                        scope: scope::Scope(scope::Kind::Const, scope::Meta::from(hir_meta)),
                    }),
                    _ => {
                        let binder_var = self.var_allocator.alloc();
                        self.invert_expr(
                            *proc,
                            params,
                            analysis,
                            TypedBinder {
                                var: binder_var,
                                meta: hir_meta,
                            },
                            TypedHirNode(ontol_hir::Kind::Var(binder_var), hir_meta),
                            dependencies,
                        )
                    }
                }
            }
            ontol_hir::Kind::Map(arg) => self.build_scope_binder(arg),
            ontol_hir::Kind::DeclSeq(_label, _attr) => Err(UnifierError::SequenceInputNotSupported),
            ontol_hir::Kind::Struct(binder, _flags, nodes) => self.enter_binder(binder, |zelf| {
                if zelf.current_prop_analysis_map.is_none() {
                    zelf.current_prop_analysis_map = Some({
                        let mut dep_analyzer = DepScopeAnalyzer::default();
                        for (index, node) in nodes.iter().enumerate() {
                            dep_analyzer.visit_node(index, node);
                        }
                        dep_analyzer.prop_analysis()?
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
                        var: binder.var,
                        meta: hir_meta,
                    }),
                    scope: scope::Scope(
                        scope::Kind::PropSet(scope::PropSet(Some(*binder), props)),
                        scope::Meta {
                            vars: union.vars,
                            dependencies: VarSet::default(),
                            hir_meta,
                        },
                    ),
                })
            }),
            ontol_hir::Kind::Regex(regex_def_id, capture_groups) => {
                let mut vars = VarSet::default();
                let scope_capture_groups = capture_groups
                    .iter()
                    .map(|capture_group| {
                        vars.insert(capture_group.binder.var);
                        ScopeCaptureGroup {
                            index: capture_group.index,
                            binder: capture_group.binder,
                        }
                    })
                    .collect();

                let input_string = self.var_allocator.alloc();

                Ok(ScopeBinder {
                    binder: Some(TypedBinder {
                        var: input_string,
                        meta: hir_meta,
                    }),
                    scope: scope::Scope(
                        scope::Kind::Regex(input_string, *regex_def_id, scope_capture_groups),
                        scope::Meta {
                            vars,
                            dependencies: VarSet::default(),
                            hir_meta,
                        },
                    ),
                })
            }
            ontol_hir::Kind::Prop(..) => panic!("standalone prop"),
            ontol_hir::Kind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            ontol_hir::Kind::MatchRegex(..) => {
                unimplemented!("BUG: MatchRegex is an output node")
            }
            ontol_hir::Kind::Sequence(..) => {
                todo!()
            }
            ontol_hir::Kind::ForEach(..) => {
                todo!()
            }
            ontol_hir::Kind::SeqPush(..) => {
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
                    let (kind, vars, dependencies) = match variant {
                        ontol_hir::PropVariant::Singleton(attr) => {
                            let mut var_union = UnionBuilder::default();
                            let mut dep_union = UnionBuilder::default();

                            let rel = dep_union
                                .plus_deps(var_union.plus(
                                    zelf.enter_child(0, |zelf| zelf.build_scope_binder(&attr.rel))?,
                                ))
                                .into_scope_pattern_binding();
                            let val = dep_union
                                .plus_deps(var_union.plus(
                                    zelf.enter_child(1, |zelf| zelf.build_scope_binder(&attr.val))?,
                                ))
                                .into_scope_pattern_binding();

                            (
                                scope::PropKind::Attr(rel, val),
                                var_union.vars,
                                dep_union.vars,
                            )
                        }
                        ontol_hir::PropVariant::Seq(SeqPropertyVariant {
                            label,
                            has_default,
                            elements,
                        }) => {
                            let only_element = if elements.len() == 1 {
                                elements.iter().next().unwrap()
                            } else {
                                todo!("More than one seq element");
                            };

                            let rel = zelf
                                .enter_child(0, |zelf| {
                                    zelf.build_scope_binder(&only_element.attribute.rel)
                                })?
                                .into_scope_pattern_binding();
                            let val = zelf
                                .enter_child(1, |zelf| {
                                    zelf.build_scope_binder(&only_element.attribute.val)
                                })?
                                .into_scope_pattern_binding();

                            (
                                scope::PropKind::Seq(*label, *has_default, rel, val),
                                VarSet::from([label.label.into()]),
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
            analysis.hir_meta,
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
                            inner_binder: TypedBinder {
                                var: scoped_var,
                                meta: *next_let_def.meta(),
                            },
                            def: next_let_def,
                            sub_scope: Box::new(scope::Scope(
                                scope::Kind::Const,
                                scope::Meta::from(self.unit_hir_meta()),
                            )),
                        }),
                        scope::Meta {
                            hir_meta: self.unit_hir_meta(),
                            vars: VarSet::from([scoped_var]),
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

    fn unit_hir_meta(&self) -> typed_hir::Meta<'m> {
        typed_hir::Meta {
            ty: self.unit_type,
            span: NO_SPAN,
        }
    }

    fn enter_binder<T>(&mut self, binder: &TypedBinder, func: impl FnOnce(&mut Self) -> T) -> T {
        if !self.in_scope.insert(binder.var) {
            panic!("Malformed HIR: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.remove(binder.var);
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
        self.vars.union_with(binder.scope.all_vars());
        binder
    }

    fn plus_deps<'m>(&mut self, binder: ScopeBinder<'m>) -> ScopeBinder<'m> {
        self.vars.union_with(binder.scope.dependencies());
        binder
    }

    fn plus_iter<'a>(&mut self, iter: impl Iterator<Item = &'a VarSet>) {
        for var_set in iter {
            self.vars.union_with(var_set);
        }
    }
}

// (+ 5 (* 2 (+ 1 a)))
// (- 1 (/ (- 5 x) 2))

#[derive(Debug)]
struct ExprAnalysis<'m> {
    kind: ExprAnalysisKind<'m>,
    hir_meta: typed_hir::Meta<'m>,
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
                hir_meta: *node.meta(),
            })
        }
        ontol_hir::Kind::Var(var) => Ok(ExprAnalysis {
            kind: ExprAnalysisKind::Var(*var),
            hir_meta: *node.meta(),
        }),
        _ => Ok(ExprAnalysis {
            kind: ExprAnalysisKind::Const,
            hir_meta: *node.meta(),
        }),
    }
}
