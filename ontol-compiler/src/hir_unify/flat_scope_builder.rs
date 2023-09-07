use ontol_hir::{visitor::HirVisitor, GetKind, SeqPropertyVariant};

use crate::{
    typed_hir::{TypedHir, TypedHirNode},
    types::TypeRef,
};

use super::{
    flat_scope::{self, FlatScope, OutputVar, ScopeNode},
    UnifierError, UnifierResult, VarSet,
};

struct NextNode<'a, 'm> {
    node: &'a TypedHirNode<'m>,
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
            deps: Default::default(),
        }];
        let mut next_node_set = vec![];

        while !node_set.is_empty() {
            next_node_set.clear();
            for next_node in std::mem::take(&mut node_set) {
                self.process(next_node, &mut next_node_set)?;
            }

            std::mem::swap(&mut node_set, &mut next_node_set);
        }

        propagate_pub_vars(&mut self.scope_nodes);

        Ok(FlatScope {
            scope_nodes: std::mem::take(&mut self.scope_nodes),
        })
    }

    fn process<'a>(
        &mut self,
        NextNode { node, mut deps }: NextNode<'a, 'm>,
        next_node_set: &mut Vec<NextNode<'a, 'm>>,
    ) -> Result<(), UnifierError> {
        match node.kind() {
            ontol_hir::Kind::Unit
            | ontol_hir::Kind::I64(_)
            | ontol_hir::Kind::F64(_)
            | ontol_hir::Kind::String(_)
            | ontol_hir::Kind::Const(_) => {
                let const_var = self.var_allocator.alloc();
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Const(flat_scope::Const(node.kind().clone())),
                    flat_scope::Meta {
                        var: const_var,
                        pub_vars: Default::default(),
                        deps,
                        hir_meta: *node.meta(),
                    },
                ))
            }
            ontol_hir::Kind::Var(var) => {
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Var,
                    flat_scope::Meta {
                        var: *var,
                        pub_vars: [*var].into(),
                        deps,
                        hir_meta: *node.meta(),
                    },
                ));
            }
            ontol_hir::Kind::Map(inner) => next_node_set.push(NextNode { node: inner, deps }),
            ontol_hir::Kind::Struct(binder, _flags, nodes) => {
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Struct,
                    flat_scope::Meta {
                        var: binder.var,
                        pub_vars: Default::default(),
                        deps,
                        hir_meta: *node.meta(),
                    },
                ));
                for node in nodes {
                    next_node_set.push(NextNode {
                        node,
                        deps: [].into(),
                    });
                }
            }
            ontol_hir::Kind::Prop(optional, struct_var, property_id, variants) => {
                deps.insert(*struct_var);

                for variant in variants {
                    match variant {
                        ontol_hir::PropVariant::Singleton(attr) => {
                            let variant_var = self.var_allocator.alloc();
                            self.scope_nodes.push(ScopeNode(
                                flat_scope::Kind::PropVariant(*optional, *struct_var, *property_id),
                                flat_scope::Meta {
                                    var: variant_var,
                                    pub_vars: Default::default(),
                                    deps: deps.clone(),
                                    hir_meta: *node.meta(),
                                },
                            ));
                            self.process_attribute(
                                &attr.rel,
                                &attr.val,
                                [variant_var].into(),
                                next_node_set,
                            );
                        }
                        ontol_hir::PropVariant::Seq(SeqPropertyVariant {
                            label,
                            has_default: _,
                            elements,
                        }) => {
                            let label_var = ontol_hir::Var(label.label.0);
                            let output_var = OutputVar(self.var_allocator.alloc());
                            self.scope_nodes.push(ScopeNode(
                                flat_scope::Kind::SeqPropVariant(
                                    *label,
                                    output_var,
                                    *optional,
                                    *struct_var,
                                    *property_id,
                                ),
                                flat_scope::Meta {
                                    var: label_var,
                                    pub_vars: [label_var].into(),
                                    deps: deps.clone(),
                                    hir_meta: *node.meta(),
                                },
                            ));
                            for element in elements {
                                let attr_deps: VarSet = if element.iter {
                                    let iter_var = self.var_allocator.alloc();
                                    self.scope_nodes.push(ScopeNode(
                                        flat_scope::Kind::IterElement(output_var),
                                        flat_scope::Meta {
                                            var: iter_var,
                                            pub_vars: Default::default(),
                                            deps: [label_var].into(),
                                            hir_meta: *node.meta(),
                                        },
                                    ));
                                    [iter_var].into()
                                } else {
                                    [label_var].into()
                                };
                                self.process_attribute(
                                    &element.attribute.rel,
                                    &element.attribute.val,
                                    attr_deps,
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
                            var: call_var,
                            pub_vars: Default::default(),
                            deps,
                            hir_meta: *node.meta(),
                        },
                    ))
                } else {
                    self.scope_nodes.push(ScopeNode(
                        flat_scope::Kind::Call(*proc),
                        flat_scope::Meta {
                            var: call_var,
                            pub_vars: Default::default(),
                            deps,
                            hir_meta: *node.meta(),
                        },
                    ));
                    for arg in args {
                        next_node_set.push(NextNode {
                            node: arg,
                            deps: [call_var].into(),
                        });
                    }
                }
            }
            ontol_hir::Kind::Regex(regex_def_id, capture_groups) => {
                let regex_var = self.var_allocator.alloc();
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::Regex(*regex_def_id),
                    flat_scope::Meta {
                        var: regex_var,
                        pub_vars: Default::default(),
                        deps,
                        hir_meta: *node.meta(),
                    },
                ));
                let alternation_var = self.var_allocator.alloc();
                self.scope_nodes.push(ScopeNode(
                    flat_scope::Kind::RegexAlternation,
                    flat_scope::Meta {
                        var: alternation_var,
                        pub_vars: Default::default(),
                        deps: [regex_var].into(),
                        hir_meta: *node.meta(),
                    },
                ));
                for capture_group in capture_groups {
                    self.scope_nodes.push(ScopeNode(
                        flat_scope::Kind::RegexCapture(capture_group.index),
                        flat_scope::Meta {
                            var: capture_group.binder.var,
                            pub_vars: [capture_group.binder.var].into(),
                            deps: [alternation_var].into(),
                            hir_meta: *node.meta(),
                        },
                    ));
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
        deps: VarSet,
        next_node_set: &mut Vec<NextNode<'a, 'm>>,
    ) {
        if !is_unit(rel) {
            let rel_var = self.var_allocator.alloc();
            self.scope_nodes.push(ScopeNode(
                flat_scope::Kind::PropRelParam,
                flat_scope::Meta {
                    var: rel_var,
                    pub_vars: Default::default(),
                    deps: deps.clone(),
                    hir_meta: *rel.meta(),
                },
            ));
            next_node_set.push(NextNode {
                node: rel,
                deps: [rel_var].into(),
            });
        }
        if !is_unit(val) {
            let val_var = self.var_allocator.alloc();
            self.scope_nodes.push(ScopeNode(
                flat_scope::Kind::PropValue,
                flat_scope::Meta {
                    var: val_var,
                    pub_vars: Default::default(),
                    deps,
                    hir_meta: *val.meta(),
                },
            ));
            next_node_set.push(NextNode {
                node: val,
                deps: [val_var].into(),
            });
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

fn propagate_pub_vars(scope_nodes: &mut Vec<ScopeNode>) {
    struct Propagation {
        pub_vars: VarSet,
        target: VarSet,
    }

    let mut propagations = vec![];
    let mut candidates = Vec::with_capacity(scope_nodes.len());
    let mut target_union = VarSet::default();

    for (index, node) in scope_nodes.iter().enumerate() {
        match node.kind() {
            flat_scope::Kind::Var | flat_scope::Kind::RegexCapture(_) => {
                propagations.push(Propagation {
                    pub_vars: node.meta().pub_vars.clone(),
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

            if target_union.contains(scope_node.meta().var) {
                for propagation in &propagations {
                    if propagation.target.contains(scope_node.meta().var)
                        && !matches!(scope_node.kind(), flat_scope::Kind::IterElement(_))
                    {
                        scope_node.1.pub_vars.union_with(&propagation.pub_vars);
                    }
                }

                // Don't propagate variables across data points such as prop variants
                if !matches!(
                    scope_node.kind(),
                    flat_scope::Kind::PropVariant(..) | flat_scope::Kind::SeqPropVariant(..)
                ) {
                    let meta = scope_node.meta();
                    next_propagations.push(Propagation {
                        pub_vars: meta.pub_vars.clone(),
                        target: meta.deps.clone(),
                    });
                    next_target_union.union_with(&meta.deps);
                }
            } else {
                next_candidates.push(index);
            }
        }

        propagations = next_propagations;
        candidates = next_candidates;
        target_union = next_target_union;
    }
}
