use ontol_hir::{PredicateClosure, PropVariant, SetPropertyVariant};
use ontol_runtime::var::VarSet;

use super::{expr, regex_interpolation::RegexStringInterpolator};
use crate::{
    def::Defs,
    typed_hir::{IntoTypedHirData, TypedHirData, TypedNodeRef},
};

pub struct ExprBuilder<'c, 'm> {
    in_scope: VarSet,
    var_allocator: ontol_hir::VarAllocator,
    defs: &'c Defs<'m>,
}

impl<'c, 'm> ExprBuilder<'c, 'm> {
    pub fn new(var_allocator: ontol_hir::VarAllocator, defs: &'c Defs<'m>) -> Self {
        Self {
            in_scope: Default::default(),
            var_allocator,
            defs,
        }
    }

    pub fn var_allocator(self) -> ontol_hir::VarAllocator {
        self.var_allocator
    }

    pub fn hir_to_expr(&mut self, node_ref: TypedNodeRef<'_, 'm>) -> expr::Expr<'m> {
        let (arena, hir_meta) = (node_ref.arena(), *node_ref.meta());
        match node_ref.kind() {
            ontol_hir::Kind::NoOp => panic!("NoOp"),
            ontol_hir::Kind::Var(var) => {
                let mut free_vars = VarSet::default();
                free_vars.insert(*var);

                expr::Expr(
                    expr::Kind::Var(*var),
                    expr::Meta {
                        hir_meta,
                        free_vars,
                    },
                )
            }
            ontol_hir::Kind::Unit => expr::Expr(
                expr::Kind::Unit,
                expr::Meta {
                    hir_meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::I64(int) => expr::Expr(
                expr::Kind::I64(*int),
                expr::Meta {
                    hir_meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::F64(float) => expr::Expr(
                expr::Kind::F64(*float),
                expr::Meta {
                    hir_meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::Text(string) => expr::Expr(
                expr::Kind::String(string.clone()),
                expr::Meta {
                    hir_meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::Const(const_def_id) => expr::Expr(
                expr::Kind::Const(*const_def_id),
                expr::Meta {
                    hir_meta,
                    free_vars: Default::default(),
                },
            ),
            ontol_hir::Kind::With(..) => panic!(),
            ontol_hir::Kind::Call(proc, args) => {
                let mut expr_args = Vec::with_capacity(args.len());
                for arg in args {
                    expr_args.push(self.hir_to_expr(arena.node_ref(*arg)));
                }
                let mut free_vars = VarSet::default();
                for param in &expr_args {
                    free_vars.union_with(&param.1.free_vars);
                }

                expr::Expr(
                    expr::Kind::Call(expr::Call(*proc, expr_args)),
                    expr::Meta {
                        hir_meta,
                        free_vars,
                    },
                )
            }
            ontol_hir::Kind::Map(arg) => {
                let arg = self.hir_to_expr(arena.node_ref(*arg));
                let expr_meta = expr::Meta {
                    free_vars: arg.1.free_vars.clone(),
                    hir_meta,
                };
                expr::Expr(expr::Kind::Map(Box::new(arg)), expr_meta)
            }
            ontol_hir::Kind::Set(entries) => {
                if entries.len() == 1 {
                    let ontol_hir::SetEntry(typed_label, attr) = entries.first().unwrap();

                    if let Some(typed_label) = typed_label {
                        let rel = self.hir_to_expr(arena.node_ref(attr.rel));
                        let val = self.hir_to_expr(arena.node_ref(attr.val));

                        let mut free_vars = VarSet::default();
                        free_vars.union_with(&rel.1.free_vars);
                        free_vars.union_with(&val.1.free_vars);
                        free_vars.insert((*typed_label.hir()).into());

                        expr::Expr(
                            expr::Kind::IterSet(
                                *typed_label.hir(),
                                Box::new(ontol_hir::Attribute { rel, val }),
                            ),
                            expr::Meta {
                                free_vars,
                                hir_meta,
                            },
                        )
                    } else {
                        todo!()
                    }
                } else {
                    todo!()
                }
            }
            ontol_hir::Kind::Struct(binder, flags, nodes) => self.enter_binder(binder, |zelf| {
                let mut props = Vec::with_capacity(nodes.len());
                for node in nodes {
                    props.extend(zelf.node_to_props(arena.node_ref(*node)));
                }

                let mut free_vars = VarSet::default();
                for prop in &props {
                    free_vars.union_with(&prop.free_vars);
                }

                expr::Expr(
                    expr::Kind::Struct {
                        binder: *binder,
                        flags: *flags,
                        props,
                        opt_output_type: None,
                    },
                    expr::Meta {
                        hir_meta,
                        free_vars,
                    },
                )
            }),
            ontol_hir::Kind::Regex(_seq_label, regex_def_id, capture_group_alternation) => {
                let regex_meta = self
                    .defs
                    .literal_regex_meta_table
                    .get(regex_def_id)
                    .unwrap();

                let mut free_vars = VarSet::default();
                for altneration in capture_group_alternation {
                    for capture_group in altneration {
                        free_vars.insert(capture_group.binder.hir().var);
                    }
                }

                let first_alternation = capture_group_alternation.first().unwrap();

                let mut components = vec![];
                let mut interpolator = RegexStringInterpolator {
                    capture_groups: first_alternation,
                    current_constant: "".into(),
                    components: &mut components,
                };
                interpolator.traverse_hir(&regex_meta.hir);
                interpolator.commit_constant();

                let var = self.var_allocator.alloc();
                expr::Expr(
                    expr::Kind::StringInterpolation(
                        ontol_hir::Binder { var }.with_meta(hir_meta),
                        components,
                    ),
                    expr::Meta {
                        hir_meta,
                        free_vars,
                    },
                )
            }
            ontol_hir::Kind::Prop(..) => panic!("standalone prop"),
            ontol_hir::Kind::MakeSeq(..) => {
                todo!()
            }
            ontol_hir::Kind::Block(_)
            | ontol_hir::Kind::Catch(..)
            | ontol_hir::Kind::Try(..)
            | ontol_hir::Kind::Let(..)
            | ontol_hir::Kind::TryLet(..)
            | ontol_hir::Kind::LetProp(..)
            | ontol_hir::Kind::LetPropDefault(..)
            | ontol_hir::Kind::TryLetProp(..)
            | ontol_hir::Kind::TryLetTup(..)
            | ontol_hir::Kind::LetRegex(..)
            | ontol_hir::Kind::LetRegexIter(..)
            | ontol_hir::Kind::MatchProp(..)
            | ontol_hir::Kind::MatchRegex(..)
            | ontol_hir::Kind::ForEach(..)
            | ontol_hir::Kind::StringPush(..)
            | ontol_hir::Kind::Insert(..)
            | ontol_hir::Kind::PushCondClause(..)
            | ontol_hir::Kind::MoveRestAttrs(..)
            | ontol_hir::Kind::CopySubSeq(..) => {
                unimplemented!("BUG: {} is an output node", node_ref)
            }
        }
    }

    fn node_to_props(&mut self, node_ref: TypedNodeRef<'_, 'm>) -> Vec<expr::Prop<'m>> {
        let arena = node_ref.arena();
        match node_ref.kind() {
            ontol_hir::Kind::Prop(flags, struct_var, prop_id, variants) => {
                let mut output = Vec::with_capacity(variants.len());
                for variant in variants {
                    let prop = match variant {
                        PropVariant::Singleton(ontol_hir::Attribute { rel, val }) => {
                            let mut union = UnionBuilder::default();
                            let rel = union.plus(self.hir_to_expr(arena.node_ref(*rel)));
                            let val = union.plus(self.hir_to_expr(arena.node_ref(*val)));

                            expr::Prop {
                                flags: *flags,
                                prop_id: *prop_id,
                                free_vars: union.vars,
                                seq: None,
                                struct_var: *struct_var,
                                variant: expr::PropVariant::Singleton(ontol_hir::Attribute {
                                    rel,
                                    val,
                                }),
                            }
                        }
                        PropVariant::Set(SetPropertyVariant {
                            label, elements, ..
                        }) => {
                            let mut union = UnionBuilder::default();
                            let prop_elements = elements
                                .iter()
                                .map(|(iter, ontol_hir::Attribute { rel, val })| {
                                    let mut rel = self.hir_to_expr(arena.node_ref(*rel));
                                    let mut val = self.hir_to_expr(arena.node_ref(*val));

                                    if !iter.0 {
                                        // non-iter element variables may refer to outer scope
                                        rel = union.plus(rel);
                                        val = union.plus(val);
                                    }

                                    (*iter, ontol_hir::Attribute { rel, val })
                                })
                                .collect();

                            union.vars.insert((*label.hir()).into());

                            expr::Prop {
                                flags: *flags,
                                prop_id: *prop_id,
                                free_vars: union.vars,
                                seq: Some(*label.hir()),
                                struct_var: *struct_var,
                                variant: expr::PropVariant::Set {
                                    label: *label.hir(),
                                    elements: prop_elements,
                                },
                            }
                        }
                        PropVariant::Predicate(closure) => {
                            let mut union = UnionBuilder::default();
                            let expr_closure = match closure {
                                PredicateClosure::ContainsElement(ontol_hir::Attribute {
                                    rel,
                                    val,
                                }) => {
                                    let rel = union.plus(self.hir_to_expr(arena.node_ref(*rel)));
                                    let val = union.plus(self.hir_to_expr(arena.node_ref(*val)));
                                    ontol_hir::PredicateClosure::ContainsElement(
                                        ontol_hir::Attribute { rel, val },
                                    )
                                }
                                PredicateClosure::ElementIn(node) => PredicateClosure::ElementIn(
                                    union.plus(self.hir_to_expr(arena.node_ref(*node))),
                                ),
                                PredicateClosure::AllInSet(node) => PredicateClosure::AllInSet(
                                    union.plus(self.hir_to_expr(arena.node_ref(*node))),
                                ),
                                PredicateClosure::SetContainsAll(node) => {
                                    PredicateClosure::SetContainsAll(
                                        union.plus(self.hir_to_expr(arena.node_ref(*node))),
                                    )
                                }
                                PredicateClosure::SetIntersects(node) => {
                                    PredicateClosure::SetIntersects(
                                        union.plus(self.hir_to_expr(arena.node_ref(*node))),
                                    )
                                }
                                PredicateClosure::SetEquals(node) => PredicateClosure::SetEquals(
                                    union.plus(self.hir_to_expr(arena.node_ref(*node))),
                                ),
                            };

                            expr::Prop {
                                flags: *flags,
                                prop_id: *prop_id,
                                free_vars: union.vars,
                                seq: None,
                                struct_var: *struct_var,
                                variant: expr::PropVariant::Predicate(expr_closure),
                            }
                        }
                    };

                    output.push(prop);
                }

                output
            }
            _ => panic!("Expected property"),
        }
    }

    // do we even need this, expression are so simple
    fn enter_binder<T>(
        &mut self,
        binder: &TypedHirData<'m, ontol_hir::Binder>,
        func: impl FnOnce(&mut Self) -> T,
    ) -> T {
        if !self.in_scope.insert(binder.hir().var) {
            panic!(
                "Malformed HIR: {:?} variable was already in scope",
                binder.hir().var
            );
        }
        let value = func(self);
        self.in_scope.remove(binder.hir().var);
        value
    }
}

#[derive(Default)]
struct UnionBuilder {
    pub vars: VarSet,
}

impl UnionBuilder {
    fn plus<'m>(&mut self, expr: expr::Expr<'m>) -> expr::Expr<'m> {
        self.vars.union_with(&expr.1.free_vars);
        expr
    }
}
