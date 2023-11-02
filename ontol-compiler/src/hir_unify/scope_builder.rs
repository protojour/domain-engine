use std::collections::HashMap;

use ontol_hir::{visitor::HirVisitor, SeqPropertyVariant};
use ontol_runtime::{var::Var, vm::proc::BuiltinProc};
use smallvec::SmallVec;

use crate::{
    hir_unify::{UnifierError, UnifierResult, VarSet},
    typed_hir::{self, arena_import, IntoTypedHirData, Meta, TypedHir, TypedHirData, UNIT_META},
    types::TypeRef,
};

use super::{
    dep_tree::Scope,
    dependent_scope_analyzer::{DepScopeAnalyzer, Path, PropAnalysis},
    scope::{self, ScopeCaptureGroup},
};

#[derive(Debug)]
pub struct ScopeBinder<'m> {
    pub binder: Option<TypedHirData<'m, ontol_hir::Binder>>,
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

pub struct ScopeBuilder<'h, 'm> {
    in_scope: VarSet,
    var_allocator: ontol_hir::VarAllocator,
    current_prop_path: SmallVec<[u16; 32]>,
    current_prop_analysis_map: Option<HashMap<Path, PropAnalysis>>,
    hir_arena: &'h ontol_hir::arena::Arena<'m, TypedHir>,
}

impl<'h, 'm> ScopeBuilder<'h, 'm> {
    pub fn new(
        var_allocator: ontol_hir::VarAllocator,
        hir_arena: &'h ontol_hir::arena::Arena<'m, TypedHir>,
    ) -> Self {
        Self {
            in_scope: VarSet::default(),
            var_allocator,
            current_prop_path: Default::default(),
            current_prop_analysis_map: None,
            hir_arena,
        }
    }

    pub fn var_allocator(self) -> ontol_hir::VarAllocator {
        self.var_allocator
    }

    pub fn build_scope_binder(
        &mut self,
        node: ontol_hir::Node,
        mapped_scalar_ty: Option<TypeRef<'m>>,
    ) -> UnifierResult<ScopeBinder<'m>> {
        let hir_meta = *self.hir_arena[node].meta();
        match self.hir_arena.kind_of(node) {
            ontol_hir::Kind::Var(var) => Ok(ScopeBinder {
                binder: Some(ontol_hir::Binder { var: *var }.with_meta(hir_meta)),
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
            | ontol_hir::Kind::Text(_)
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
            ontol_hir::Kind::Call(proc, args) => {
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

                let analysis = analyze_expr(node, defined_var, self.hir_arena)?;
                match &analysis.kind {
                    ExprAnalysisKind::Const => Ok(ScopeBinder {
                        binder: None,
                        scope: scope::Scope(scope::Kind::Const, scope::Meta::from(hir_meta)),
                    }),
                    _ => {
                        let binder_var = self.var_allocator.alloc();
                        let next_def = {
                            let def_arena: ontol_hir::arena::Arena<'m, TypedHir> =
                                Default::default();
                            def_arena
                                .add_root(TypedHirData(ontol_hir::Kind::Var(binder_var), hir_meta))
                        };

                        self.invert_expr(
                            *proc,
                            args,
                            analysis,
                            ontol_hir::Binder { var: binder_var }.with_meta(Meta {
                                ty: mapped_scalar_ty.unwrap_or(hir_meta.ty),
                                span: hir_meta.span,
                            }),
                            next_def,
                            dependencies,
                        )
                    }
                }
            }
            ontol_hir::Kind::Map(arg) => {
                self.build_scope_binder(*arg, mapped_scalar_ty.or(Some(self.hir_arena[node].ty())))
            }
            ontol_hir::Kind::DeclSeq(_label, _attr) => Err(UnifierError::SequenceInputNotSupported),
            ontol_hir::Kind::Struct(binder, _flags, nodes) => self.enter_binder(binder, |zelf| {
                if zelf.current_prop_analysis_map.is_none() {
                    zelf.current_prop_analysis_map = Some({
                        let mut dep_analyzer = DepScopeAnalyzer::default();
                        for (index, node) in nodes.iter().enumerate() {
                            dep_analyzer.visit_node(index, self.hir_arena.node_ref(*node));
                        }
                        dep_analyzer.prop_analysis()?
                    });
                }

                // panic!("prop_analysis: {prop_analysis:#?}");

                let mut props = Vec::with_capacity(nodes.len());
                for (disjoint_group, node) in nodes.iter().enumerate() {
                    zelf.enter_child(disjoint_group, |zelf| -> UnifierResult<()> {
                        props.extend(zelf.build_props(*node, disjoint_group)?);
                        Ok(())
                    })?;
                }

                let mut union = UnionBuilder::default();
                union.plus_iter(props.iter().map(|prop| &prop.vars));

                Ok(ScopeBinder {
                    binder: Some(
                        ontol_hir::Binder {
                            var: binder.hir().var,
                        }
                        .with_meta(hir_meta),
                    ),
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
            ontol_hir::Kind::Regex(seq_label, regex_def_id, capture_group_alternations) => {
                if seq_label.is_some() {
                    panic!("This unifier does not handle looping regex");
                }

                let mut vars = VarSet::default();
                let capture_groups = capture_group_alternations.first().unwrap();
                let scope_capture_groups = capture_groups
                    .iter()
                    .map(|capture_group| {
                        vars.insert(capture_group.binder.hir().var);
                        ScopeCaptureGroup {
                            index: capture_group.index,
                            binder: capture_group.binder,
                        }
                    })
                    .collect();

                let input_string = self.var_allocator.alloc();

                Ok(ScopeBinder {
                    binder: Some(ontol_hir::Binder { var: input_string }.with_meta(hir_meta)),
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
            ontol_hir::Kind::Sequence(..) => {
                todo!()
            }
            ontol_hir::Kind::ForEach(..) => {
                todo!()
            }
            ontol_hir::Kind::SeqPush(..) => {
                todo!()
            }
            ontol_hir::Kind::Begin(_)
            | ontol_hir::Kind::MatchProp(..)
            | ontol_hir::Kind::MatchRegex(..)
            | ontol_hir::Kind::StringPush(..)
            | ontol_hir::Kind::PushCondClause(..)
            | ontol_hir::Kind::MoveRestAttrs(..)
            | ontol_hir::Kind::CopySubSeq(..) => {
                unimplemented!("BUG: {} is an output node", self.hir_arena.node_ref(node))
            }
        }
    }

    fn build_props(
        &mut self,
        node: ontol_hir::Node,
        disjoint_group: usize,
    ) -> UnifierResult<Vec<scope::Prop<'m>>> {
        if let ontol_hir::Kind::Prop(optional, struct_var, prop_id, variants) =
            self.hir_arena.kind_of(node)
        {
            let mut props = Vec::with_capacity(variants.len());

            for (variant_idx, variant) in variants.iter().enumerate() {
                self.enter_child(variant_idx, |zelf| -> UnifierResult<()> {
                    let (kind, vars, dependencies) = match variant {
                        ontol_hir::PropVariant::Singleton(attr) => {
                            let mut var_union = UnionBuilder::default();
                            let mut dep_union = UnionBuilder::default();

                            let rel = dep_union
                                .plus_deps(var_union.plus(zelf.enter_child(0, |zelf| {
                                    zelf.build_scope_binder(attr.rel, None)
                                })?))
                                .into_scope_pattern_binding();
                            let val = dep_union
                                .plus_deps(var_union.plus(zelf.enter_child(1, |zelf| {
                                    zelf.build_scope_binder(attr.val, None)
                                })?))
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
                            let (_, only_element_attr) = if elements.len() == 1 {
                                elements.iter().next().unwrap()
                            } else {
                                todo!("More than one seq element");
                            };

                            let rel = zelf
                                .enter_child(0, |zelf| {
                                    zelf.build_scope_binder(only_element_attr.rel, None)
                                })?
                                .into_scope_pattern_binding();
                            let val = zelf
                                .enter_child(1, |zelf| {
                                    zelf.build_scope_binder(only_element_attr.val, None)
                                })?
                                .into_scope_pattern_binding();

                            (
                                scope::PropKind::Seq(*label, *has_default, rel, val),
                                VarSet::from([label.hir().0.into()]),
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
            panic!("not a prop: {}", self.hir_arena.node_ref(node));
        }
    }

    fn invert_expr(
        &mut self,
        proc: BuiltinProc,
        args: &ontol_hir::Nodes,
        analysis: ExprAnalysis<'m>,
        outer_binder: TypedHirData<'m, ontol_hir::Binder>,
        let_def: ontol_hir::RootNode<'m, TypedHir>,
        dependencies: VarSet,
    ) -> UnifierResult<ScopeBinder<'m>> {
        let (var_arg_index, next_analysis) = match analysis.kind {
            ExprAnalysisKind::Const | ExprAnalysisKind::Var(_) => unreachable!(),
            ExprAnalysisKind::FnCall {
                var_arg_index,
                child,
                ..
            } => (var_arg_index, child),
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

        let mut inverted_args = ontol_hir::Nodes::default();
        let (mut def_arena, let_def) = let_def.split();

        for (arg_index, arg) in args.iter().enumerate() {
            if arg_index != var_arg_index {
                inverted_args.push(arena_import(&mut def_arena, self.hir_arena.node_ref(*arg)));
            }
        }
        inverted_args.insert(var_arg_index, let_def);

        let next_let_def = def_arena.add_root(TypedHirData(
            ontol_hir::Kind::Call(inverted_proc, inverted_args),
            // Is this correct?
            analysis.hir_meta,
        ));

        match (self.hir_arena.kind_of(args[var_arg_index]), next_analysis) {
            (
                ontol_hir::Kind::Var(_),
                ExprAnalysis {
                    kind: ExprAnalysisKind::Var(scoped_var),
                    ..
                },
            ) => {
                // Inject a mapping around the entire inverted function application
                // so that the runtime type of the variable is "coerced" into the expected type.
                let (mut def_arena, let_def) = next_let_def.split();
                let map_node = def_arena.add(TypedHirData(
                    ontol_hir::Kind::Map(let_def),
                    *self.hir_arena[args[var_arg_index]].meta(),
                ));
                let let_def = ontol_hir::RootNode::new(map_node, def_arena);

                Ok(ScopeBinder {
                    binder: Some(outer_binder),
                    scope: scope::Scope(
                        scope::Kind::Let(scope::Let {
                            outer_binder: Some(outer_binder),
                            inner_binder: ontol_hir::Binder { var: scoped_var }
                                .with_meta(*let_def.data().meta()),
                            def: let_def,
                            sub_scope: Box::new(scope::Scope(
                                scope::Kind::Const,
                                scope::Meta::from(UNIT_META),
                            )),
                        }),
                        scope::Meta {
                            hir_meta: UNIT_META,
                            vars: VarSet::from([scoped_var]),
                            dependencies,
                        },
                    ),
                })
            }
            (ontol_hir::Kind::Call(next_proc, next_args), child_analysis) => self.invert_expr(
                *next_proc,
                next_args,
                child_analysis,
                outer_binder,
                next_let_def,
                dependencies,
            ),
            _ => panic!("invalid: {}", self.hir_arena.node_ref(args[var_arg_index])),
        }
    }

    fn enter_binder<T>(
        &mut self,
        binder: &TypedHirData<ontol_hir::Binder>,
        func: impl FnOnce(&mut Self) -> T,
    ) -> T {
        if !self.in_scope.insert(binder.hir().var) {
            panic!("Malformed HIR: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.remove(binder.hir().var);
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
    Var(Var),
    FnCall {
        var_arg_index: usize,
        defining_var: Var,
        child: Box<ExprAnalysis<'m>>,
    },
}

fn analyze_expr<'m>(
    node: ontol_hir::Node,
    defining_var_hint: Option<Var>,
    hir_arena: &ontol_hir::arena::Arena<'m, TypedHir>,
) -> UnifierResult<ExprAnalysis<'m>> {
    match hir_arena.kind_of(node) {
        ontol_hir::Kind::Call(_, args) => {
            let mut kind = ExprAnalysisKind::Const;
            let mut defining_var = None;
            for (index, arg) in args.iter().enumerate() {
                let child_analysis = analyze_expr(*arg, defining_var_hint, hir_arena)?;
                let (child_var, new_kind) = match &child_analysis.kind {
                    ExprAnalysisKind::Const => (None, ExprAnalysisKind::Const),
                    ExprAnalysisKind::Var(child_var) => (
                        Some(*child_var),
                        ExprAnalysisKind::FnCall {
                            var_arg_index: index,
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
                            var_arg_index: index,
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
                            return Err(UnifierError::MultipleVariablesInExpression(
                                hir_arena[node].meta().span,
                            ));
                        }
                    }

                    kind = new_kind;
                    defining_var = child_var;
                }
            }

            Ok(ExprAnalysis {
                kind,
                hir_meta: *hir_arena[node].meta(),
            })
        }
        ontol_hir::Kind::Var(var) => Ok(ExprAnalysis {
            kind: ExprAnalysisKind::Var(*var),
            hir_meta: *hir_arena[node].meta(),
        }),
        ontol_hir::Kind::Map(inner) => analyze_expr(*inner, defining_var_hint, hir_arena),
        _ => Ok(ExprAnalysis {
            kind: ExprAnalysisKind::Const,
            hir_meta: *hir_arena[node].meta(),
        }),
    }
}
