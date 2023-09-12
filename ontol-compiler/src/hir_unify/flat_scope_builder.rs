use std::rc::Rc;

use ontol_hir::{visitor::HirVisitor, GetKind, SeqPropertyVariant};

use crate::{
    typed_hir::{TypedHir, TypedHirNode},
    types::TypeRef,
};

use super::{
    flat_scope::{self, FlatScope, OutputVar, PropDepth, ScopeNode, ScopeVar},
    UnifierError, UnifierResult, VarSet,
};

struct NextNode<'a, 'm> {
    node: &'a TypedHirNode<'m>,
    prop_depth: PropDepth,
    constraints: Rc<VarSet>,
    deps: VarSet,
}

pub struct FlatScopeBuilder<'m> {
    #[allow(unused)]
    unit_type: TypeRef<'m>,
    var_allocator: ontol_hir::VarAllocator,
    scope_nodes: Vec<ScopeNode<'m>>,
}

impl<'m> FlatScopeBuilder<'m> {
    pub fn new(var_allocator: ontol_hir::VarAllocator, unit_type: TypeRef<'m>) -> Self {
        Self {
            unit_type,
            // in_scope: VarSet::default(),
            var_allocator,
            // current_prop_path: Default::default(),
            // current_prop_analysis_map: None,
            scope_nodes: vec![],
        }
    }

    pub fn var_allocator(self) -> ontol_hir::VarAllocator {
        self.var_allocator
    }

    pub fn build_flat_scope(&mut self, node: &TypedHirNode<'m>) -> UnifierResult<FlatScope<'m>> {
        let mut node_set = vec![NextNode {
            node,
            prop_depth: PropDepth(0),
            deps: VarSet::default(),
            constraints: Default::default(),
        }];
        let mut next_node_set = vec![];

        while !node_set.is_empty() {
            next_node_set.clear();
            for next_node in std::mem::take(&mut node_set) {
                self.process(next_node, &mut next_node_set)?;
            }

            std::mem::swap(&mut node_set, &mut next_node_set);
        }

        propagate_vars(&mut self.scope_nodes);

        Ok(FlatScope {
            scope_nodes: std::mem::take(&mut self.scope_nodes),
        })
    }

    fn process<'a>(
        &mut self,
        NextNode {
            node,
            mut deps,
            prop_depth,
            constraints,
        }: NextNode<'a, 'm>,
        next_node_set: &mut Vec<NextNode<'a, 'm>>,
    ) -> Result<(), UnifierError> {
        match node.kind() {
            ontol_hir::Kind::Unit
            | ontol_hir::Kind::I64(_)
            | ontol_hir::Kind::F64(_)
            | ontol_hir::Kind::String(_)
            | ontol_hir::Kind::Const(_) => {
                let const_var = ScopeVar(self.var_allocator.alloc());
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Const(flat_scope::Const(node.kind().clone())),
                    flat_scope::Meta {
                        scope_var: const_var,
                        free_vars: Default::default(),
                        constraints,
                        defs: Default::default(),
                        deps,
                        hir_meta: *node.meta(),
                    },
                ))
            }
            ontol_hir::Kind::Var(var) => {
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Var,
                    flat_scope::Meta {
                        scope_var: ScopeVar(*var),
                        free_vars: [*var].into(),
                        constraints,
                        defs: [*var].into(),
                        deps,
                        hir_meta: *node.meta(),
                    },
                ));
            }
            ontol_hir::Kind::Map(inner) => next_node_set.push(NextNode {
                node: inner,
                deps,
                constraints,
                prop_depth,
            }),
            ontol_hir::Kind::Struct(binder, _flags, nodes) => {
                for node in nodes {
                    next_node_set.push(NextNode {
                        node,
                        prop_depth,
                        constraints: constraints.clone(),
                        deps: [].into(),
                    });
                }
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Struct,
                    flat_scope::Meta {
                        scope_var: ScopeVar(binder.var),
                        free_vars: Default::default(),
                        constraints,
                        defs: [binder.var].into(),
                        deps,
                        hir_meta: *node.meta(),
                    },
                ));
            }
            ontol_hir::Kind::Prop(optional, struct_var, property_id, variants) => {
                deps.insert(*struct_var);

                for variant in variants {
                    match variant {
                        ontol_hir::PropVariant::Singleton(attr) => {
                            let variant_var = self.var_allocator.alloc();

                            let constraints = if optional.0 {
                                Rc::new(constraints.union_one(variant_var))
                            } else {
                                constraints.clone()
                            };

                            self.scope_nodes.push(ScopeNode(
                                flat_scope::Kind::PropVariant(
                                    prop_depth,
                                    *optional,
                                    *struct_var,
                                    *property_id,
                                ),
                                flat_scope::Meta {
                                    scope_var: ScopeVar(variant_var),
                                    free_vars: Default::default(),
                                    constraints: constraints.clone(),
                                    defs: Default::default(),
                                    deps: deps.clone(),
                                    hir_meta: *node.meta(),
                                },
                            ));

                            self.process_attribute(
                                &attr.rel,
                                &attr.val,
                                prop_depth.next(),
                                [variant_var].into(),
                                constraints,
                                next_node_set,
                            );
                        }
                        ontol_hir::PropVariant::Seq(SeqPropertyVariant {
                            label,
                            has_default,
                            elements,
                        }) => {
                            let label_var = ontol_hir::Var(label.label.0);
                            let output_var = OutputVar(self.var_allocator.alloc());

                            self.scope_nodes.push(ScopeNode(
                                flat_scope::Kind::SeqPropVariant(
                                    *label,
                                    output_var,
                                    *optional,
                                    *has_default,
                                    *struct_var,
                                    *property_id,
                                ),
                                flat_scope::Meta {
                                    scope_var: ScopeVar(label_var),
                                    free_vars: Default::default(),
                                    constraints: constraints.clone(),
                                    defs: [label_var].into(),
                                    deps: deps.clone(),
                                    hir_meta: *node.meta(),
                                },
                            ));

                            for (iter, attr) in elements {
                                let attr_deps: VarSet = if iter.0 {
                                    let iter_var = self.var_allocator.alloc();
                                    self.scope_nodes.push(ScopeNode(
                                        flat_scope::Kind::IterElement(label.label, output_var),
                                        flat_scope::Meta {
                                            scope_var: ScopeVar(iter_var),
                                            free_vars: Default::default(),
                                            constraints: constraints.clone(),
                                            defs: Default::default(),
                                            deps: [label_var].into(),
                                            hir_meta: *node.meta(),
                                        },
                                    ));
                                    [iter_var].into()
                                } else {
                                    [label_var].into()
                                };
                                self.process_attribute(
                                    &attr.rel,
                                    &attr.val,
                                    prop_depth.next(),
                                    attr_deps,
                                    constraints.clone(),
                                    next_node_set,
                                );
                            }
                        }
                    }
                }
            }
            ontol_hir::Kind::Call(proc, args) => {
                let call_var = self.var_allocator.alloc();
                if is_const(node) {
                    self.scope_nodes.push(ScopeNode(
                        flat_scope::Kind::Const(flat_scope::Const(node.kind().clone())),
                        flat_scope::Meta {
                            scope_var: ScopeVar(call_var),
                            free_vars: Default::default(),
                            constraints,
                            defs: Default::default(),
                            deps,
                            hir_meta: *node.meta(),
                        },
                    ))
                } else {
                    for arg in args {
                        next_node_set.push(NextNode {
                            node: arg,
                            prop_depth,
                            constraints: constraints.clone(),
                            deps: [call_var].into(),
                        });
                    }
                    self.scope_nodes.push(ScopeNode(
                        flat_scope::Kind::Call(*proc),
                        flat_scope::Meta {
                            scope_var: ScopeVar(call_var),
                            free_vars: Default::default(),
                            constraints,
                            defs: Default::default(),
                            deps,
                            hir_meta: *node.meta(),
                        },
                    ));
                }
            }
            ontol_hir::Kind::Regex(opt_seq_label, regex_def_id, capture_group_alternations) => {
                let (scope_var, opt_label) = match opt_seq_label {
                    Some(seq_label) => (ontol_hir::Var(seq_label.label.0), Some(seq_label.label)),
                    None => (self.var_allocator.alloc(), None),
                };

                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Regex(opt_label, *regex_def_id),
                    flat_scope::Meta {
                        scope_var: ScopeVar(scope_var),
                        free_vars: Default::default(),
                        constraints: constraints.clone(),
                        defs: Default::default(),
                        deps,
                        hir_meta: *node.meta(),
                    },
                ));
                for alternation in capture_group_alternations {
                    let alt_scope_var = self.var_allocator.alloc();
                    self.scope_nodes.push(ScopeNode(
                        flat_scope::Kind::RegexAlternation,
                        flat_scope::Meta {
                            scope_var: ScopeVar(alt_scope_var),
                            free_vars: Default::default(),
                            constraints: constraints.clone(),
                            defs: Default::default(),
                            deps: [scope_var].into(),
                            hir_meta: *node.meta(),
                        },
                    ));

                    for capture_group in alternation {
                        self.scope_nodes.push(ScopeNode(
                            flat_scope::Kind::RegexCapture(capture_group.index),
                            flat_scope::Meta {
                                scope_var: ScopeVar(capture_group.binder.var),
                                free_vars: [capture_group.binder.var].into(),
                                constraints: constraints.clone(),
                                defs: [capture_group.binder.var].into(),
                                deps: [alt_scope_var].into(),
                                hir_meta: *node.meta(),
                            },
                        ));
                    }
                }
            }
            ontol_hir::Kind::DeclSeq(..) => return Err(UnifierError::SequenceInputNotSupported),
            kind => todo!("{kind}"),
        }

        Ok(())
    }

    fn process_attribute<'a>(
        &mut self,
        rel: &'a TypedHirNode<'m>,
        val: &'a TypedHirNode<'m>,
        prop_depth: PropDepth,
        deps: VarSet,
        constraints: Rc<VarSet>,
        next_node_set: &mut Vec<NextNode<'a, 'm>>,
    ) {
        if !is_unit(rel) {
            let rel_var = self.var_allocator.alloc();
            next_node_set.push(NextNode {
                node: rel,
                prop_depth,
                constraints: constraints.clone(),
                deps: [rel_var].into(),
            });
            self.scope_nodes.push(ScopeNode(
                flat_scope::Kind::PropRelParam,
                flat_scope::Meta {
                    scope_var: ScopeVar(rel_var),
                    free_vars: Default::default(),
                    constraints: constraints.clone(),
                    defs: Default::default(),
                    deps: deps.clone(),
                    hir_meta: *rel.meta(),
                },
            ));
        }
        if !is_unit(val) {
            let val_var = self.var_allocator.alloc();
            next_node_set.push(NextNode {
                node: val,
                prop_depth,
                constraints: constraints.clone(),
                deps: [val_var].into(),
            });
            self.scope_nodes.push(ScopeNode(
                flat_scope::Kind::PropValue,
                flat_scope::Meta {
                    scope_var: ScopeVar(val_var),
                    free_vars: Default::default(),
                    constraints: constraints.clone(),
                    defs: Default::default(),
                    deps,
                    hir_meta: *val.meta(),
                },
            ));
        }
    }
}

fn is_unit(node: &TypedHirNode) -> bool {
    matches!(node.kind(), ontol_hir::Kind::Unit)
}

fn is_const(node: &TypedHirNode) -> bool {
    struct ConstChecker {
        has_var: bool,
    }

    impl<'s, 'm: 's> ontol_hir::visitor::HirVisitor<'s, 'm, TypedHir> for ConstChecker {
        fn visit_var(&mut self, _: &ontol_hir::Var) {
            self.has_var = true;
        }
    }
    let mut checker = ConstChecker { has_var: false };
    checker.visit_node(0, node);
    !checker.has_var
}

fn propagate_vars(scope_nodes: &mut Vec<ScopeNode>) {
    struct Propagation {
        free_vars: VarSet,
        defs: VarSet,
        target: VarSet,
    }

    let mut propagations = vec![];
    let mut candidates = Vec::with_capacity(scope_nodes.len());
    let mut target_union = VarSet::default();

    for (index, node) in scope_nodes.iter().enumerate() {
        match node.kind() {
            flat_scope::Kind::Var | flat_scope::Kind::RegexCapture(_) => {
                propagations.push(Propagation {
                    free_vars: node.meta().free_vars.clone(),
                    defs: node.meta().defs.clone(),
                    target: node.meta().deps.clone(),
                });
                target_union.union_with(&node.meta().deps);
            }
            _ => {
                candidates.push(index);
            }
        }
    }

    while !candidates.is_empty() && !propagations.is_empty() {
        // Every iteration should do fewer propagations and have fewer candidates.
        // The candidate list is split into propagaitions to do now, and candidates to process later.
        // This must end up at 0 in the end.
        let mut next_propagations = Vec::with_capacity(propagations.len());
        let mut next_candidates = Vec::with_capacity(candidates.len());
        let mut next_target_union = VarSet::default();

        for index in candidates {
            let scope_node = scope_nodes.get_mut(index).unwrap();

            if target_union.contains(scope_node.meta().scope_var.0) {
                for propagation in &propagations {
                    if propagation.target.contains(scope_node.meta().scope_var.0) {
                        scope_node.1.free_vars.union_with(&propagation.free_vars);
                        if !matches!(scope_node.kind(), flat_scope::Kind::IterElement(..)) {
                            scope_node.1.defs.union_with(&propagation.defs);
                        }
                    }
                }

                let next_defs: VarSet = if matches!(
                    scope_node.kind(),
                    flat_scope::Kind::PropVariant(..)
                        | flat_scope::Kind::SeqPropVariant(..)
                        | flat_scope::Kind::Regex(..)
                        | flat_scope::Kind::RegexAlternation
                ) {
                    Default::default()
                } else {
                    scope_node.meta().defs.clone()
                };

                let meta = scope_node.meta();
                next_propagations.push(Propagation {
                    free_vars: meta.free_vars.clone(),
                    defs: next_defs,
                    target: meta.deps.clone(),
                });
                next_target_union.union_with(&meta.deps);
            } else {
                next_candidates.push(index);
            }
        }

        propagations = next_propagations;
        candidates = next_candidates;
        target_union = next_target_union;
    }
}
