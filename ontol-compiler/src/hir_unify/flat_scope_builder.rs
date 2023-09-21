use std::rc::Rc;

use ontol_hir::{visitor::HirVisitor, SeqPropertyVariant};

use crate::typed_hir::{arena_import_root, TypedArena, TypedHir};

use super::{
    flat_scope::{self, FlatScope, OutputVar, PropDepth, ScopeNode, ScopeVar},
    UnifierError, UnifierResult, VarSet,
};

struct NextNode {
    node: ontol_hir::Node,
    prop_depth: PropDepth,
    constraints: Rc<VarSet>,
    deps: VarSet,
}

pub struct FlatScopeBuilder<'h, 'm> {
    var_allocator: ontol_hir::VarAllocator,
    scope_nodes: Vec<ScopeNode<'m>>,
    hir_arena: &'h TypedArena<'m>,
}

impl<'h, 'm> FlatScopeBuilder<'h, 'm> {
    pub fn new(var_allocator: ontol_hir::VarAllocator, hir_arena: &'h TypedArena<'m>) -> Self {
        Self {
            // in_scope: VarSet::default(),
            var_allocator,
            // current_prop_path: Default::default(),
            // current_prop_analysis_map: None,
            scope_nodes: vec![],
            hir_arena,
        }
    }

    pub fn var_allocator(self) -> ontol_hir::VarAllocator {
        self.var_allocator
    }

    pub fn build_flat_scope(&mut self, node: ontol_hir::Node) -> UnifierResult<FlatScope<'m>> {
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

    fn process(
        &mut self,
        NextNode {
            node,
            mut deps,
            prop_depth,
            constraints,
        }: NextNode,
        next_node_set: &mut Vec<NextNode>,
    ) -> Result<(), UnifierError> {
        match self.hir_arena.kind_of(node) {
            ontol_hir::Kind::Unit
            | ontol_hir::Kind::I64(_)
            | ontol_hir::Kind::F64(_)
            | ontol_hir::Kind::Text(_)
            | ontol_hir::Kind::Const(_) => {
                let const_var = ScopeVar(self.var_allocator.alloc());
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Const(arena_import_root(self.hir_arena.node_ref(node))),
                    flat_scope::Meta {
                        scope_var: const_var,
                        free_vars: Default::default(),
                        constraints,
                        defs: Default::default(),
                        deps,
                        hir_meta: *self.hir_arena[node].meta(),
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
                        hir_meta: *self.hir_arena[node].meta(),
                    },
                ));
            }
            ontol_hir::Kind::Map(inner) => next_node_set.push(NextNode {
                node: *inner,
                deps,
                constraints,
                prop_depth,
            }),
            ontol_hir::Kind::Struct(binder, _flags, nodes) => {
                for node in nodes {
                    next_node_set.push(NextNode {
                        node: *node,
                        prop_depth,
                        constraints: constraints.clone(),
                        deps: [].into(),
                    });
                }
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Struct,
                    flat_scope::Meta {
                        scope_var: ScopeVar(binder.hir().var),
                        free_vars: Default::default(),
                        constraints,
                        defs: [binder.hir().var].into(),
                        deps,
                        hir_meta: *self.hir_arena[node].meta(),
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
                                    hir_meta: *self.hir_arena[node].meta(),
                                },
                            ));

                            self.process_attribute(
                                attr.rel,
                                attr.val,
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
                            let label_var = ontol_hir::Var(label.hir().0);
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
                                    hir_meta: *self.hir_arena[node].meta(),
                                },
                            ));

                            for (iter, attr) in elements {
                                let attr_deps: VarSet = if iter.0 {
                                    let iter_var = self.var_allocator.alloc();
                                    self.scope_nodes.push(ScopeNode(
                                        flat_scope::Kind::IterElement(*label.hir(), output_var),
                                        flat_scope::Meta {
                                            scope_var: ScopeVar(iter_var),
                                            free_vars: Default::default(),
                                            constraints: constraints.clone(),
                                            defs: Default::default(),
                                            deps: [label_var].into(),
                                            hir_meta: *self.hir_arena[node].meta(),
                                        },
                                    ));
                                    [iter_var].into()
                                } else {
                                    [label_var].into()
                                };
                                self.process_attribute(
                                    attr.rel,
                                    attr.val,
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
                if self.is_const(node) {
                    self.scope_nodes.push(ScopeNode(
                        flat_scope::Kind::Const(arena_import_root(self.hir_arena.node_ref(node))),
                        flat_scope::Meta {
                            scope_var: ScopeVar(call_var),
                            free_vars: Default::default(),
                            constraints,
                            defs: Default::default(),
                            deps,
                            hir_meta: *self.hir_arena[node].meta(),
                        },
                    ))
                } else {
                    for arg in args {
                        next_node_set.push(NextNode {
                            node: *arg,
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
                            hir_meta: *self.hir_arena[node].meta(),
                        },
                    ));
                }
            }
            ontol_hir::Kind::Regex(opt_seq_label, regex_def_id, capture_group_alternations) => {
                let (scope_var, opt_label) = match opt_seq_label {
                    Some(seq_label) => (ontol_hir::Var(seq_label.hir().0), Some(seq_label.hir())),
                    None => (self.var_allocator.alloc(), None),
                };

                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Regex(opt_label.cloned(), *regex_def_id),
                    flat_scope::Meta {
                        scope_var: ScopeVar(scope_var),
                        free_vars: Default::default(),
                        constraints: constraints.clone(),
                        defs: Default::default(),
                        deps,
                        hir_meta: *self.hir_arena[node].meta(),
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
                            hir_meta: *self.hir_arena[node].meta(),
                        },
                    ));

                    for capture_group in alternation {
                        let binder_var = capture_group.binder.hir().var;
                        self.scope_nodes.push(ScopeNode(
                            flat_scope::Kind::RegexCapture(capture_group.index),
                            flat_scope::Meta {
                                scope_var: ScopeVar(binder_var),
                                free_vars: [binder_var].into(),
                                constraints: constraints.clone(),
                                defs: [binder_var].into(),
                                deps: [alt_scope_var].into(),
                                hir_meta: *self.hir_arena[node].meta(),
                            },
                        ));
                    }
                }
            }
            ontol_hir::Kind::DeclSeq(..) => return Err(UnifierError::SequenceInputNotSupported),
            _ => todo!("{}", self.hir_arena.node_ref(node)),
        }

        Ok(())
    }

    fn process_attribute(
        &mut self,
        rel: ontol_hir::Node,
        val: ontol_hir::Node,
        prop_depth: PropDepth,
        deps: VarSet,
        constraints: Rc<VarSet>,
        next_node_set: &mut Vec<NextNode>,
    ) {
        if !self.is_unit(rel) {
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
                    hir_meta: *self.hir_arena[rel].meta(),
                },
            ));
        }
        if !self.is_unit(val) {
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
                    hir_meta: *self.hir_arena[val].meta(),
                },
            ));
        }
    }

    fn is_unit(&self, node: ontol_hir::Node) -> bool {
        matches!(self.hir_arena.kind_of(node), ontol_hir::Kind::Unit)
    }

    fn is_const(&self, node: ontol_hir::Node) -> bool {
        struct ConstChecker {
            has_var: bool,
        }

        impl<'h, 'm: 'h> ontol_hir::visitor::HirVisitor<'h, 'm, TypedHir> for ConstChecker {
            fn visit_var(&mut self, _: ontol_hir::Var) {
                self.has_var = true;
            }
        }
        let mut checker = ConstChecker { has_var: false };
        checker.visit_node(0, self.hir_arena.node_ref(node));
        !checker.has_var
    }
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
