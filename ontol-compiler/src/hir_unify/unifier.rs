use indexmap::IndexMap;
use ontol_hir::{
    kind::{
        Attribute, Dimension, IterBinder, MatchArm, NodeKind, PatternBinding, PropPattern,
        PropVariant,
    },
    visitor::HirVisitor,
    Binder, Variable,
};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc, DefId};
use smallvec::SmallVec;
use tracing::{debug, trace, warn};

use crate::{
    hir_unify::var_path::locate_variables,
    mem::Intern,
    typed_hir::{HirFunc, Meta, TypedBinder, TypedHir, TypedHirKind, TypedHirNode},
    types::{Type, TypeRef, Types},
    Compiler, SourceSpan,
};

use super::{
    tagged_node::{TaggedKind, TaggedNode, Tagger},
    unification_tree::{Scoping, UnificationNode},
    UnifierError, VariableTracker,
};

type UnifiedNodes<'m> = SmallVec<[TypedHirNode<'m>; 1]>;

type UnifierResult<T> = Result<T, UnifierError>;

pub struct Unified<'m> {
    pub binder: Option<TypedBinder<'m>>,
    pub nodes: UnifiedNodes<'m>,
}

pub fn unify_to_function<'m>(
    scope_source: TypedHirNode<'m>,
    target: TypedHirNode<'m>,
    compiler: &mut Compiler<'m>,
) -> UnifierResult<HirFunc<'m>> {
    let mut var_tracker = VariableTracker::default();
    var_tracker.visit_node(0, &scope_source);
    var_tracker.visit_node(0, &target);

    let unit_type = compiler.types.intern(Type::Unit(DefId::unit()));

    let unified = Unifier {
        root_source: &scope_source,
        next_variable: var_tracker.next_variable(),
        types: &mut compiler.types,
    }
    .unify(Tagger::new(unit_type).tag_node(target), &scope_source)?;

    let body = match unified.nodes.len() {
        0 => panic!("No nodes"),
        1 => unified.nodes.into_iter().next().unwrap(),
        _ => panic!("Too many nodes"),
    };

    Ok(HirFunc {
        arg: unified.binder.ok_or(UnifierError::NoInputBinder)?,
        body,
    })
}

struct Unifier<'a, 'm> {
    root_source: &'a TypedHirNode<'m>,
    next_variable: Variable,
    types: &'a mut Types<'m>,
}

struct InvertedCall<'m> {
    let_binder: Variable,
    def: TypedHirNode<'m>,
    body: UnifiedNodes<'m>,
}

struct UnifyPatternBinding<'m> {
    binding: PatternBinding,
    nodes: Option<UnifiedNodes<'m>>,
}

struct TypedMatchArm<'m> {
    arm: MatchArm<'m, TypedHir>,
    ty: TypeRef<'m>,
}

struct LetAttr<'m> {
    rel_binding: PatternBinding,
    val_binding: PatternBinding,
    nodes: Vec<TypedHirNode<'m>>,
    ty: TypeRef<'m>,
}

struct LetAttrPair<'m> {
    rel: UnifiedTypedPatternBinding<'m>,
    val: UnifiedTypedPatternBinding<'m>,
}

struct UnifiedTypedPatternBinding<'m> {
    binding: PatternBinding,
    nodes: Vec<TypedHirNode<'m>>,
    ty: TypeRef<'m>,
}

impl<'a, 'm> Unifier<'a, 'm> {
    fn alloc_var(&mut self) -> Variable {
        let var = self.next_variable;
        self.next_variable.0 += 1;
        var
    }
}

impl<'a, 'm> Unifier<'a, 'm> {
    fn unify(
        &mut self,
        expr: TaggedNode<'m>,
        scope_node: &TypedHirNode<'m>,
    ) -> UnifierResult<Unified<'m>> {
        let u_tree = build_u_tree(expr, scope_node)?;
        self.unify_u_node(None, u_tree, scope_node)
    }

    fn unify_u_node(
        &mut self,
        debug_index: Option<usize>,
        u_node: UnificationNode<'m>,
        scope_source: &TypedHirNode<'m>,
    ) -> UnifierResult<Unified<'m>> {
        match u_node {
            UnificationNode::Scoping(scoping) => {
                self.unify_scoping(debug_index, scoping, scope_source)
            }
            UnificationNode::Struct(struct_node) => {
                let meta = struct_node.meta;

                let Unified { binder, mut nodes } =
                    self.unify_scoping(debug_index, struct_node.sub_scoping, scope_source)?;

                for node in struct_node.nodes {
                    nodes.push(node.into_hir_node());
                }

                let node = TypedHirNode {
                    kind: NodeKind::Struct(struct_node.binder, nodes.to_vec()),
                    meta,
                };

                Ok(Unified {
                    binder,
                    nodes: [node].into(),
                })
            }
        }
    }

    fn unify_scoping(
        &mut self,
        debug_index: Option<usize>,
        mut scoping: Scoping<'m>,
        scope_source: &TypedHirNode<'m>,
    ) -> UnifierResult<Unified<'m>> {
        let TypedHirNode { kind, meta } = scope_source;
        let meta = *meta;
        match kind {
            NodeKind::VariableRef(var) => {
                debug!("unify_scoping({debug_index:?}, VariableRef)");
                let nodes = self.merge_target_nodes(&mut scoping)?;
                Ok(Unified {
                    binder: Some(TypedBinder {
                        variable: *var,
                        ty: meta.ty,
                    }),
                    nodes,
                })
            }
            NodeKind::Unit => {
                debug!("unify_scoping({debug_index:?}, Unit)");
                let nodes = self.merge_target_nodes(&mut scoping)?;
                Ok(Unified {
                    binder: None,
                    nodes,
                })
            }
            NodeKind::Int(_int) => panic!(),
            NodeKind::Call(proc, args) => match scoping.sub_nodes.len() {
                0 => {
                    debug!("unify_scoping({debug_index:?}, Call const)");
                    let args = self.unify_children(scoping, args)?;
                    Ok(Unified {
                        binder: None,
                        nodes: [TypedHirNode {
                            kind: NodeKind::Call(*proc, args),
                            meta,
                        }]
                        .into(),
                    })
                }
                1 => {
                    debug!("unify_scoping({debug_index:?}, Call var)");

                    // Invert call algorithm:
                    // start with a _variable_ representing this expression.
                    // Expand this expression along the way recursing _down_ to
                    // find the actual inner variable.
                    // That inner variable is the _binder_ of the resulting `let` expression.
                    let subst_var = self.alloc_var();
                    let inner_expr = TypedHirNode {
                        kind: TypedHirKind::VariableRef(subst_var),
                        meta,
                    };
                    let inverted_call =
                        self.invert_call_recursive(proc, args, scoping, meta, inner_expr)?;

                    let return_ty = self.last_type(inverted_call.body.iter());
                    Ok(Unified {
                        binder: Some(TypedBinder {
                            variable: subst_var,
                            ty: meta.ty,
                        }),
                        nodes: [TypedHirNode {
                            kind: NodeKind::Let(
                                Binder(inverted_call.let_binder),
                                Box::new(inverted_call.def),
                                inverted_call.body.into_iter().collect(),
                            ),
                            meta: Meta {
                                ty: return_ty,
                                span: meta.span,
                            },
                        }]
                        .into(),
                    })
                }
                _ => {
                    panic!("Multiple variables in function call!");
                }
            },
            NodeKind::Map(arg) => match scoping.sub_nodes.remove(&0) {
                None => {
                    debug!("unify_scoping({debug_index:?}, Map const)");

                    let unified_arg = self.unify_scoping(Some(0), scoping, arg)?;
                    let mut param: TypedHirNode = unified_arg.nodes.into_iter().next().unwrap();
                    let param_ty = param.meta.ty;
                    param.meta.ty = meta.ty;
                    Ok(Unified {
                        binder: unified_arg.binder,
                        nodes: [TypedHirNode {
                            kind: NodeKind::Map(Box::new(param)),
                            meta: Meta {
                                ty: param_ty,
                                span: meta.span,
                            },
                        }]
                        .into(),
                    })
                }
                Some(sub_node) => {
                    debug!("unify_scoping({debug_index:?}, Map var)");

                    self.unify_u_node(Some(0), sub_node, arg)
                }
            },
            NodeKind::Let(..) => {
                unimplemented!("BUG: Let is an output node")
            }
            NodeKind::Seq(_binder, _attr) => Err(UnifierError::SequenceInputNotSupported),
            NodeKind::Struct(binder, nodes) => {
                debug!("unify_scoping({debug_index:?}, Struct({}))", binder.0);
                self.unify_struct_scoping(scoping, *binder, nodes, meta)
            }
            NodeKind::Prop(struct_var, id, variants) => {
                debug!("unify_scoping({debug_index:?}, Prop)");
                let mut match_arms = vec![];
                let mut ty = &Type::Tautology;
                for (index, variant_u_node) in scoping.sub_nodes {
                    debug!("unify_source_arm({index})");

                    match &variants[index] {
                        PropVariant::Present { dimension, attr } => {
                            if let Some(typed_match_arm) = self.unify_prop_variant_to_match_arm(
                                variant_u_node,
                                *dimension,
                                attr,
                            )? {
                                if let Type::Tautology = typed_match_arm.ty {
                                    panic!("found Tautology");
                                }

                                ty = typed_match_arm.ty;
                                match_arms.push(typed_match_arm.arm);
                            }
                        }
                        PropVariant::NotPresent => {
                            todo!()
                        }
                    }
                }

                Ok(Unified {
                    binder: None,
                    nodes: if match_arms.is_empty() {
                        SmallVec::default()
                    } else {
                        if let Type::Tautology = ty {
                            panic!();
                        }

                        [TypedHirNode {
                            kind: TypedHirKind::MatchProp(*struct_var, *id, match_arms),
                            meta: Meta {
                                ty,
                                span: meta.span,
                            },
                        }]
                        .into()
                    },
                })
            }
            NodeKind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            NodeKind::Gen(..) => {
                debug!("unify_scoping({debug_index:?}, Gen)");
                todo!()
            }
            NodeKind::Iter(..) => {
                debug!("unify_scoping({debug_index:?}, Iter)");
                todo!()
            }
            NodeKind::Push(..) => {
                debug!("unify_scoping({debug_index:?}, Push)");
                todo!()
            }
        }
    }

    fn unify_struct_scoping(
        &mut self,
        scoping: Scoping<'m>,
        binder: Binder,
        nodes: &[TypedHirNode<'m>],
        meta: Meta<'m>,
    ) -> UnifierResult<Unified<'m>> {
        let nodes = self.unify_children(scoping, nodes)?;
        Ok(Unified {
            binder: Some(TypedBinder {
                variable: binder.0,
                ty: meta.ty,
            }),
            nodes: nodes.into(),
        })
    }

    fn unify_prop_variant_to_match_arm(
        &mut self,
        u_node: UnificationNode<'m>,
        scope_dimension: Dimension,
        scope_attr: &Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<TypedMatchArm<'m>>> {
        let scoping = u_node.force_into_scoping();

        if scoping.expressions.is_empty() {
            // treat this "transparently"
            Ok(self
                .let_attr(scoping, scope_attr)?
                .map(|let_attr| TypedMatchArm {
                    arm: MatchArm {
                        pattern: PropPattern::Attr(let_attr.rel_binding, let_attr.val_binding),
                        nodes: let_attr.nodes,
                    },
                    ty: let_attr.ty,
                }))
        } else if scoping.expressions.len() == 1 {
            if !matches!(scope_dimension, Dimension::Seq(_)) {
                panic!("BUG: Non-sequence");
            }

            let expr = scoping.expressions.into_iter().next().unwrap();
            let _meta = expr.meta;

            match expr.kind {
                TaggedKind::Seq(_) => {
                    let mut iterator = expr.children.0.into_iter();
                    let rel_binding =
                        self.unify_expr_pattern_binding(iterator.next().unwrap(), &scope_attr.rel)?;
                    let val_binding =
                        self.unify_expr_pattern_binding(iterator.next().unwrap(), &scope_attr.val)?;

                    let input_seq_var = self.alloc_var();
                    let output_seq_var = self.alloc_var();

                    Ok(self
                        .let_attr_pair(rel_binding, val_binding)?
                        .map(|let_attr| {
                            // FIXME: Array/seq types must take two parameters
                            let seq_ty = self.types.intern(Type::Array(let_attr.val.ty));

                            let gen_node = TypedHirNode {
                                kind: NodeKind::Gen(
                                    input_seq_var,
                                    IterBinder {
                                        seq: PatternBinding::Binder(output_seq_var),
                                        rel: let_attr.rel.binding,
                                        val: let_attr.val.binding,
                                    },
                                    vec![TypedHirNode {
                                        kind: NodeKind::Push(
                                            output_seq_var,
                                            Attribute {
                                                rel: Box::new(self.single_node_or_unit(
                                                    let_attr.rel.nodes.into_iter(),
                                                )),
                                                val: Box::new(self.single_node_or_unit(
                                                    let_attr.val.nodes.into_iter(),
                                                )),
                                            },
                                        ),
                                        meta: Meta {
                                            ty: self.unit_type(),
                                            span: SourceSpan::none(),
                                        },
                                    }],
                                ),
                                meta: Meta {
                                    ty: seq_ty,
                                    span: SourceSpan::none(),
                                },
                            };

                            TypedMatchArm {
                                arm: MatchArm {
                                    pattern: PropPattern::Seq(PatternBinding::Binder(
                                        input_seq_var,
                                    )),
                                    nodes: vec![gen_node],
                                },
                                ty: seq_ty,
                            }
                        }))
                }
                TaggedKind::PresentPropVariant(struct_var, prop_id, Dimension::Seq(_)) => {
                    let mut iterator = expr.children.0.into_iter();
                    let rel_binding =
                        self.unify_expr_pattern_binding(iterator.next().unwrap(), &scope_attr.rel)?;
                    let val_binding =
                        self.unify_expr_pattern_binding(iterator.next().unwrap(), &scope_attr.val)?;

                    let input_seq_var = self.alloc_var();
                    let output_seq_var = self.alloc_var();

                    Ok(self
                        .let_attr_pair(rel_binding, val_binding)?
                        .map(|let_attr| {
                            // FIXME: Array/seq types must take two parameters
                            let seq_ty = self.types.intern(Type::Array(let_attr.val.ty));

                            let gen_node = TypedHirNode {
                                kind: NodeKind::Gen(
                                    input_seq_var,
                                    IterBinder {
                                        seq: PatternBinding::Binder(output_seq_var),
                                        rel: let_attr.rel.binding,
                                        val: let_attr.val.binding,
                                    },
                                    vec![TypedHirNode {
                                        kind: NodeKind::Push(
                                            output_seq_var,
                                            Attribute {
                                                rel: Box::new(self.single_node_or_unit(
                                                    let_attr.rel.nodes.into_iter(),
                                                )),
                                                val: Box::new(self.single_node_or_unit(
                                                    let_attr.val.nodes.into_iter(),
                                                )),
                                            },
                                        ),
                                        meta: Meta {
                                            ty: self.unit_type(),
                                            span: SourceSpan::none(),
                                        },
                                    }],
                                ),
                                meta: Meta {
                                    ty: seq_ty,
                                    span: SourceSpan::none(),
                                },
                            };

                            let prop_node = TypedHirNode {
                                // FIXME: It feels wrong to construct the NodeKind::Prop explicitly here.
                                // this node should already be in target_nodes, so what should instead be done
                                // is to put its variables into scope.
                                kind: NodeKind::Prop(
                                    struct_var,
                                    prop_id,
                                    vec![PropVariant::Present {
                                        dimension: Dimension::Singular,
                                        attr: Attribute {
                                            rel: Box::new(self.unit_node()),
                                            val: Box::new(gen_node),
                                        },
                                    }],
                                ),
                                meta: Meta {
                                    ty: self.unit_type(),
                                    span: SourceSpan::none(),
                                },
                            };

                            TypedMatchArm {
                                arm: MatchArm {
                                    pattern: PropPattern::Seq(PatternBinding::Binder(
                                        input_seq_var,
                                    )),
                                    nodes: vec![prop_node],
                                },
                                ty: seq_ty,
                            }
                        }))
                }
                other => panic!("BUG: Unsupported kind: {other:?}"),
            }
        } else {
            panic!("BUG: Many target nodes for prop variant");
        }
    }

    fn let_attr(
        &mut self,
        mut scoping: Scoping<'m>,
        scope_attr: &Attribute<Box<TypedHirNode<'m>>>,
    ) -> UnifierResult<Option<LetAttr<'m>>> {
        let rel_binding =
            self.unify_pattern_binding(Some(0), scoping.sub_nodes.remove(&0), &scope_attr.rel)?;
        let val_binding =
            self.unify_pattern_binding(Some(1), scoping.sub_nodes.remove(&1), &scope_attr.val)?;

        Ok(match (rel_binding.nodes, val_binding.nodes) {
            (None, None) => {
                debug!("No nodes in variant binding");
                None
            }
            (Some(rel), None) => Self::last_type_opt(rel.iter()).map(|ty| LetAttr {
                rel_binding: rel_binding.binding,
                val_binding: PatternBinding::Wildcard,
                nodes: rel.into_iter().collect(),
                ty,
            }),
            (None, Some(val)) => Self::last_type_opt(val.iter()).map(|ty| LetAttr {
                rel_binding: PatternBinding::Wildcard,
                val_binding: val_binding.binding,
                nodes: val.into_iter().collect(),
                ty,
            }),
            (Some(rel), Some(val)) => {
                let mut concatenated: Vec<TypedHirNode> = vec![];
                concatenated.extend(rel);
                concatenated.extend(val);
                Self::last_type_opt(concatenated.iter()).map(|ty| LetAttr {
                    rel_binding: rel_binding.binding,
                    val_binding: val_binding.binding,
                    nodes: concatenated,
                    ty,
                })
            }
        })
    }

    fn let_attr_pair(
        &mut self,
        rel_binding: UnifyPatternBinding<'m>,
        val_binding: UnifyPatternBinding<'m>,
    ) -> UnifierResult<Option<LetAttrPair<'m>>> {
        Ok(match (rel_binding.nodes, val_binding.nodes) {
            (None, None) => {
                debug!("No nodes in variant (pair) binding");
                None
            }
            (Some(rel), None) => Self::last_type_opt(rel.iter()).map(|ty| LetAttrPair {
                rel: UnifiedTypedPatternBinding {
                    binding: rel_binding.binding,
                    nodes: rel.into_iter().collect(),
                    ty,
                },
                val: UnifiedTypedPatternBinding {
                    binding: PatternBinding::Wildcard,
                    nodes: vec![],
                    ty: self.unit_type(),
                },
            }),
            (None, Some(val)) => Self::last_type_opt(val.iter()).map(|ty| LetAttrPair {
                rel: UnifiedTypedPatternBinding {
                    binding: PatternBinding::Wildcard,
                    nodes: vec![],
                    ty: self.unit_type(),
                },
                val: UnifiedTypedPatternBinding {
                    binding: val_binding.binding,
                    nodes: val.into_iter().collect(),
                    ty,
                },
            }),
            (Some(rel), Some(val)) => Self::last_type_opt(val.iter()).map(|val_ty| {
                let rel_ty = Self::last_type_opt(rel.iter()).unwrap_or_else(|| self.unit_type());
                LetAttrPair {
                    rel: UnifiedTypedPatternBinding {
                        binding: rel_binding.binding,
                        nodes: rel.into_iter().collect(),
                        ty: rel_ty,
                    },
                    val: UnifiedTypedPatternBinding {
                        binding: val_binding.binding,
                        nodes: val.into_iter().collect(),
                        ty: val_ty,
                    },
                }
            }),
        })
    }

    fn invert_call_recursive(
        &mut self,
        proc: &BuiltinProc,
        args: &[TypedHirNode<'m>],
        mut scoping: Scoping<'m>,
        meta: Meta<'m>,
        inner_expr: TypedHirNode<'m>,
    ) -> UnifierResult<InvertedCall<'m>> {
        if scoping.sub_nodes.len() > 1 {
            panic!("Too many sub unifications in function call");
        }

        // FIXME: Properly check this
        let inverted_proc = match proc {
            BuiltinProc::Add => BuiltinProc::Sub,
            BuiltinProc::Sub => BuiltinProc::Add,
            BuiltinProc::Mul => BuiltinProc::Div,
            BuiltinProc::Div => BuiltinProc::Mul,
            _ => panic!("Unsupported procedure; cannot invert {proc:?}"),
        };

        let mut new_args: Vec<TypedHirNode<'m>> = vec![];

        let (unification_idx, sub_unification) = scoping.sub_nodes.pop_first().unwrap();
        let mut sub_scoping = sub_unification.force_into_scoping();

        for (index, arg) in args.iter().enumerate() {
            if index != unification_idx {
                new_args.push(arg.clone());
            }
        }

        let pivot_arg = &args[unification_idx];

        match &pivot_arg.kind {
            NodeKind::VariableRef(var) => {
                new_args.insert(unification_idx, inner_expr);
                let body = self.merge_target_nodes(&mut sub_scoping)?;

                Ok(InvertedCall {
                    let_binder: *var,
                    def: TypedHirNode {
                        kind: NodeKind::Call(inverted_proc, new_args),
                        meta,
                    },
                    body,
                })
            }
            NodeKind::Call(child_proc, child_args) => {
                new_args.insert(unification_idx, inner_expr);
                let new_inner_expr = TypedHirNode {
                    kind: NodeKind::Call(inverted_proc, new_args),
                    meta,
                };
                self.invert_call_recursive(
                    child_proc,
                    child_args,
                    sub_scoping,
                    pivot_arg.meta,
                    new_inner_expr,
                )
            }
            _ => unimplemented!(),
        }
    }

    fn merge_target_nodes(&mut self, scoping: &mut Scoping<'m>) -> UnifierResult<UnifiedNodes<'m>> {
        let mut merged: UnifiedNodes<'m> = Default::default();
        let mut property_groups: IndexMap<(Variable, PropertyId), Vec<(Dimension, TaggedNode)>> =
            Default::default();

        for expr in std::mem::take(&mut scoping.expressions) {
            match &expr.kind {
                TaggedKind::PresentPropVariant(variable, property_id, dimension) => {
                    property_groups
                        .entry((*variable, *property_id))
                        .or_default()
                        .push((*dimension, expr));
                }
                _ => {
                    merged.push(expr.into_hir_node());
                }
            }
        }

        for ((variable, property_id), dimension_variants) in property_groups {
            let mut variants = vec![];
            let mut ty = &Type::Tautology;
            for (dimension, variant) in dimension_variants {
                let (rel, val) = variant.children.into_hir_pair();
                ty = variant.meta.ty;
                if let Type::Tautology = ty {
                    warn!(
                        "Prop variant is Tautology (rel, val): ({:?}, {:?}) ({}, {})",
                        rel.meta.ty, val.meta.ty, rel, val
                    );
                }
                variants.push(PropVariant::Present {
                    dimension,
                    attr: Attribute {
                        rel: Box::new(rel),
                        val: Box::new(val),
                    },
                })
            }

            merged.push(TypedHirNode {
                kind: NodeKind::Prop(variable, property_id, variants),
                meta: Meta {
                    ty,
                    span: SourceSpan::none(),
                },
            })
        }

        for root_u_node in std::mem::take(&mut scoping.dependent_scopes) {
            let unified = self.unify_u_node(None, root_u_node, self.root_source)?;
            merged.extend(unified.nodes);
        }

        Ok(merged)
    }

    fn unify_children(
        &mut self,
        scoping: Scoping<'m>,
        children: &[TypedHirNode<'m>],
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let mut output = vec![];
        for (index, sub_node) in scoping.sub_nodes {
            output.extend(
                self.unify_u_node(Some(index), sub_node, &children[index])?
                    .nodes,
            );
        }

        Ok(output)
    }

    fn unify_expr_pattern_binding(
        &mut self,
        expr: TaggedNode<'m>,
        scope_source: &TypedHirNode<'m>,
    ) -> UnifierResult<UnifyPatternBinding<'m>> {
        let u_tree = build_u_tree(expr, scope_source)?;
        self.unify_pattern_binding(None, Some(u_tree), scope_source)
    }

    fn unify_pattern_binding(
        &mut self,
        debug_index: Option<usize>,
        u_node: Option<UnificationNode<'m>>,
        source: &TypedHirNode<'m>,
    ) -> UnifierResult<UnifyPatternBinding<'m>> {
        let u_node = match u_node {
            Some(u_node) => u_node,
            None => {
                return Ok(UnifyPatternBinding {
                    binding: PatternBinding::Wildcard,
                    nodes: None,
                })
            }
        };

        let Unified { nodes, binder } = self.unify_u_node(debug_index, u_node, source)?;
        let binding = match binder {
            Some(TypedBinder { variable, .. }) => PatternBinding::Binder(variable),
            None => PatternBinding::Wildcard,
        };
        Ok(UnifyPatternBinding {
            binding,
            nodes: Some(nodes),
        })
    }

    fn last_type<'n>(
        &mut self,
        mut iterator: impl Iterator<Item = &'n TypedHirNode<'m>>,
    ) -> TypeRef<'m>
    where
        'm: 'n,
    {
        if let Some(next) = iterator.next() {
            let mut ty = next.meta.ty;
            for next in iterator {
                ty = next.meta.ty;
            }
            ty
        } else {
            self.unit_type()
        }
    }

    fn last_type_opt<'n>(
        mut iterator: impl Iterator<Item = &'n TypedHirNode<'m>>,
    ) -> Option<TypeRef<'m>>
    where
        'm: 'n,
    {
        if let Some(next) = iterator.next() {
            let mut ty = next.meta.ty;
            for next in iterator {
                ty = next.meta.ty;
            }
            Some(ty)
        } else {
            None
        }
    }

    fn unit_type(&mut self) -> TypeRef<'m> {
        self.types.intern(Type::Unit(DefId::unit()))
    }

    fn unit_node(&mut self) -> TypedHirNode<'m> {
        TypedHirNode {
            kind: NodeKind::Unit,
            meta: Meta {
                ty: self.unit_type(),
                span: SourceSpan::none(),
            },
        }
    }

    fn single_node_or_unit(
        &mut self,
        mut iterator: impl Iterator<Item = TypedHirNode<'m>>,
    ) -> TypedHirNode<'m> {
        match iterator.next() {
            Some(node) => {
                if iterator.next().is_some() {
                    panic!("Contained more than a single node");
                }
                node
            }
            None => self.unit_node(),
        }
    }
}

fn build_u_tree<'m>(
    expr: TaggedNode<'m>,
    scope_node: &TypedHirNode<'m>,
) -> UnifierResult<UnificationNode<'m>> {
    trace!(
        "free_variables: {:?}",
        super::DebugVariables(&expr.free_variables)
    );

    let variable_paths = locate_variables(scope_node, &expr.free_variables)?;
    trace!("var_paths: {variable_paths:?}");

    let u_tree = UnificationNode::build(expr, &variable_paths);

    trace!("{u_tree:#?}");
    Ok(u_tree)
}
