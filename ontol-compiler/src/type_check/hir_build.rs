use std::str::FromStr;

use ontol_runtime::{smart_format, DefId};
use tracing::debug;

use crate::{
    def::{Def, DefKind},
    error::CompileError,
    mem::Intern,
    pattern::{PatId, Pattern, PatternKind, RegexPattern, RegexPatternCaptureNode},
    primitive::PrimitiveKind,
    type_check::{
        hir_build_ctx::{ExplicitVariableArm, PatternVariable},
        hir_build_struct::StructInfo,
        inference::UnifyValue,
    },
    typed_hir::{Meta, TypedBinder, TypedHir, TypedHirNode, TypedLabel},
    types::{Type, TypeRef},
    SourceSpan, NO_SPAN,
};

use super::{hir_build_ctx::HirBuildCtx, TypeCheck, TypeEquation, TypeError};

pub(super) struct NodeInfo<'m> {
    pub expected_ty: Option<TypeRef<'m>>,
    pub parent_struct_flags: ontol_hir::StructFlags,
}

/// This is the type check of map statements.
/// The types that are used must be checked with `check_def_sealed`.
impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn build_root_pattern(
        &mut self,
        pat_id: PatId,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        let pattern = self.patterns.table.remove(&pat_id).unwrap();

        let node = self.build_node(
            &pattern,
            NodeInfo {
                // Don't pass inference types as the expected type:
                expected_ty: None,
                parent_struct_flags: ontol_hir::StructFlags::empty(),
            },
            ctx,
        );

        // Save typing result for the final type unification:
        match node.ty() {
            Type::Error | Type::Infer(_) => {}
            _ => {
                let type_var = ctx.inference.new_type_variable(pat_id);
                debug!("Check pat(2) root type result: {:?}", node.ty());
                ctx.inference
                    .eq_relations
                    .unify_var_value(type_var, UnifyValue::Known(node.ty()))
                    .unwrap();
            }
        }

        node
    }

    pub(super) fn build_node(
        &mut self,
        pattern: &Pattern,
        node_info: NodeInfo<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        let node = match (&pattern.kind, node_info.expected_ty) {
            (PatternKind::Call(def_id, args), Some(_expected_output)) => {
                match (
                    self.defs.table.get(def_id),
                    self.def_types.table.get(def_id),
                ) {
                    (
                        Some(Def {
                            kind: DefKind::Fn(proc),
                            ..
                        }),
                        Some(Type::Function { params, output }),
                    ) => {
                        if args.len() != params.len() {
                            return self.error_node(
                                CompileError::IncorrectNumberOfArguments {
                                    expected: u8::try_from(params.len()).unwrap(),
                                    actual: u8::try_from(args.len()).unwrap(),
                                },
                                &pattern.span,
                            );
                        }

                        let mut hir_args = vec![];
                        for (arg, param_ty) in args.iter().zip(*params) {
                            let node = self.build_node(
                                arg,
                                NodeInfo {
                                    expected_ty: Some(param_ty),
                                    parent_struct_flags: node_info.parent_struct_flags,
                                },
                                ctx,
                            );
                            hir_args.push(node);
                        }

                        TypedHirNode(
                            ontol_hir::Kind::Call(*proc, hir_args),
                            Meta {
                                ty: output,
                                span: pattern.span,
                            },
                        )
                    }
                    _ => self.error_node(CompileError::NotCallable, &pattern.span),
                }
            }
            (
                PatternKind::Struct {
                    type_path: Some(type_path),
                    modifier,
                    attributes,
                },
                expected_ty,
            ) => {
                let struct_ty = self.check_def_sealed(type_path.def_id);
                match struct_ty {
                    Type::Domain(def_id) => {
                        assert_eq!(*def_id, type_path.def_id);
                    }
                    _ => return self.error_node(CompileError::DomainTypeExpected, &type_path.span),
                };
                let struct_node = self.build_property_matcher(
                    StructInfo {
                        struct_def_id: type_path.def_id,
                        struct_ty,
                        modifier: *modifier,
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    attributes,
                    pattern.span,
                    ctx,
                );

                let meta = *struct_node.meta();
                match expected_ty {
                    Some(Type::Infer(_)) => struct_node,
                    Some(Type::Domain(_)) => struct_node,
                    Some(Type::Option(Type::Domain(_))) => TypedHirNode(
                        struct_node.into_kind(),
                        Meta {
                            ty: self.types.intern(Type::Option(meta.ty)),
                            span: meta.span,
                        },
                    ),
                    Some(expected_ty) => self.type_error_node(
                        TypeError::Mismatch(TypeEquation {
                            actual: struct_node.meta().ty,
                            expected: expected_ty,
                        }),
                        &pattern.span,
                    ),
                    _ => struct_node,
                }
            }
            (
                PatternKind::Struct {
                    type_path: None,
                    modifier,
                    attributes,
                },
                Some(expected_struct_ty @ Type::Anonymous(def_id)),
            ) => {
                let actual_ty = self.check_def_sealed(*def_id);
                if actual_ty != expected_struct_ty {
                    return self.type_error_node(
                        TypeError::Mismatch(TypeEquation {
                            actual: actual_ty,
                            expected: expected_struct_ty,
                        }),
                        &pattern.span,
                    );
                }

                self.build_property_matcher(
                    StructInfo {
                        struct_def_id: *def_id,
                        struct_ty: actual_ty,
                        modifier: *modifier,
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    attributes,
                    pattern.span,
                    ctx,
                )
            }
            (
                PatternKind::Struct {
                    type_path: None, ..
                },
                _,
            ) => self.type_error_node(TypeError::NoRelationParametersExpected, &pattern.span),
            (PatternKind::Seq(aggr_pat_id, pat_elements), expected_ty) => {
                let (rel_ty, val_ty) = match expected_ty {
                    Some(Type::Seq(rel_ty, val_ty)) => (*rel_ty, *val_ty),
                    Some(other_ty) => {
                        // Handle looping regular expressions
                        if let Type::Primitive(PrimitiveKind::String, _) | Type::StringLike(..) =
                            other_ty
                        {
                            if pat_elements.len() == 1 {
                                let pat_element = pat_elements.iter().next().unwrap();
                                if let PatternKind::Regex(regex_pattern) = &pat_element.pattern.kind
                                {
                                    if pat_element.iter {
                                        return self.build_looping_regex_node(regex_pattern, ctx);
                                    } else {
                                        return self.error_node(
                                            CompileError::RequiresSpreading,
                                            &pat_element.pattern.span,
                                        );
                                    }
                                }
                            }
                        }

                        self.type_error(TypeError::MustBeSequence(other_ty), &pattern.span);
                        (self.unit_type(), self.types.intern(Type::Error))
                    }
                    None => {
                        let pat_id = self.patterns.alloc_pat_id();
                        let val_ty = self
                            .types
                            .intern(Type::Infer(ctx.inference.new_type_variable(pat_id)));

                        debug!("Infer seq val type: {val_ty:?}");
                        (self.unit_type(), val_ty)
                    }
                };

                if pat_elements.len() != 1 {
                    return self.error_node(
                        CompileError::TODO(smart_format!("Standalone seq needs one element")),
                        &pattern.span,
                    );
                }

                let inner_node = self.build_node(
                    &pat_elements.iter().next().unwrap().pattern,
                    NodeInfo {
                        expected_ty: Some(val_ty),
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    ctx,
                );
                let label = *ctx.label_map.get(aggr_pat_id).unwrap();
                let seq_ty = self.types.intern(Type::Seq(rel_ty, val_ty));

                TypedHirNode(
                    ontol_hir::Kind::DeclSeq(
                        TypedLabel { label, ty: seq_ty },
                        ontol_hir::Attribute {
                            rel: Box::new(self.unit_node_no_span()),
                            val: Box::new(inner_node),
                        },
                    ),
                    Meta {
                        ty: seq_ty,
                        span: pattern.span,
                    },
                )
            }
            (PatternKind::ConstI64(int), Some(expected_ty)) => match expected_ty {
                Type::Primitive(PrimitiveKind::I64, _) => TypedHirNode(
                    ontol_hir::Kind::I64(*int),
                    Meta {
                        ty: expected_ty,
                        span: pattern.span,
                    },
                ),
                Type::Primitive(PrimitiveKind::F64, _) => {
                    // Didn't find a way to go from i64 to f64 in Rust std..
                    match f64::from_str(&int.to_string()) {
                        Ok(float) => TypedHirNode(
                            ontol_hir::Kind::F64(float),
                            Meta {
                                ty: expected_ty,
                                span: pattern.span,
                            },
                        ),
                        Err(_) => self.error_node(CompileError::IncompatibleLiteral, &pattern.span),
                    }
                }
                _ => self.error_node(CompileError::IncompatibleLiteral, &pattern.span),
            },
            (PatternKind::ConstString(string), Some(expected_ty)) => match expected_ty {
                Type::Primitive(PrimitiveKind::String, _) => TypedHirNode(
                    ontol_hir::Kind::String(string.clone()),
                    Meta {
                        ty: expected_ty,
                        span: pattern.span,
                    },
                ),
                Type::StringConstant(def_id) => match self.defs.def_kind(*def_id) {
                    DefKind::StringLiteral(lit) if string == lit => TypedHirNode(
                        ontol_hir::Kind::String(string.clone()),
                        Meta {
                            ty: expected_ty,
                            span: pattern.span,
                        },
                    ),
                    _ => self.error_node(CompileError::IncompatibleLiteral, &pattern.span),
                },
                _ => self.error_node(CompileError::IncompatibleLiteral, &pattern.span),
            },
            (PatternKind::Variable(var), expected_ty) => {
                let arm = ctx.arm;
                let explicit_variable = ctx
                    .pattern_variables
                    .get_mut(var)
                    .expect("variable not found");

                let arm_pat_id = {
                    let hir_arm = explicit_variable.hir_arms.entry(arm).or_insert_with(|| {
                        let pat_id = self.patterns.alloc_pat_id();

                        ExplicitVariableArm {
                            pat_id,
                            span: pattern.span,
                        }
                    });
                    hir_arm.pat_id
                };

                let type_var = ctx.inference.new_type_variable(arm_pat_id);

                match expected_ty {
                    Some(Type::Seq(_rel_ty, val_ty)) => self.type_error_node(
                        TypeError::VariableMustBeSequenceEnclosed(val_ty),
                        &pattern.span,
                    ),
                    Some(expected_ty) => {
                        let variable_ref = TypedHirNode(
                            ontol_hir::Kind::Var(*var),
                            Meta {
                                ty: expected_ty,
                                span: pattern.span,
                            },
                        );

                        match ctx
                            .inference
                            .eq_relations
                            .unify_var_value(type_var, UnifyValue::Known(expected_ty))
                        {
                            // Variables are the same type, no mapping necessary:
                            Ok(_) => variable_ref,
                            // Need to map:
                            Err(err @ TypeError::Mismatch(type_eq)) => {
                                match (&type_eq.actual, &type_eq.expected) {
                                    (Type::Domain(_), Type::Domain(_)) => {
                                        panic!("Should not happen anymore");
                                    }

                                    _ => self.type_error_node(err, &pattern.span),
                                }
                            }
                            Err(err) => todo!("Report unification error: {err:?}"),
                        }
                    }
                    None => {
                        todo!()
                    }
                }
            }
            (PatternKind::Regex(regex_pattern), Some(expected_ty)) => match expected_ty {
                // TODO: Handle compile-time match of string constant?
                Type::Primitive(PrimitiveKind::String, _) | Type::StringConstant(_) => {
                    let capture_groups_list = self.build_regex_capture_alternations(
                        &regex_pattern.capture_node,
                        &pattern.span,
                        expected_ty,
                        ctx,
                    );

                    TypedHirNode(
                        ontol_hir::Kind::Regex(
                            None,
                            regex_pattern.regex_def_id,
                            capture_groups_list,
                        ),
                        Meta {
                            ty: expected_ty,
                            span: pattern.span,
                        },
                    )
                }
                _ => self.error_node(CompileError::IncompatibleLiteral, &pattern.span),
            },
            (kind, ty) => self.error_node(
                CompileError::TODO(smart_format!(
                    "Not enough type information for {kind:?}, expected_ty = {ty:?}"
                )),
                &pattern.span,
            ),
        };

        debug!("expected/meta: {:?} {:?}", node_info.expected_ty, node.ty());

        match (node_info.expected_ty, node.ty()) {
            (_, Type::Error | Type::Infer(_)) => {}
            (Some(Type::Infer(type_var)), _) => {
                ctx.inference
                    .eq_relations
                    .unify_var_value(*type_var, UnifyValue::Known(node.ty()))
                    .unwrap();
            }
            _ => {}
        }

        node
    }

    pub(super) fn build_implicit_rel_node(
        &mut self,
        ty: TypeRef<'m>,
        object: &Pattern,
        prop_span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        match &object.kind {
            PatternKind::Variable(object_var) => {
                // implicit mapping; for now the object needs to be a variable
                let edge_var = ctx
                    .object_to_edge_var_table
                    .entry(*object_var)
                    .or_insert_with(|| {
                        let edge_var = ctx.var_allocator.alloc();
                        ctx.pattern_variables.insert(
                            edge_var,
                            PatternVariable {
                                ctrl_group: None,
                                hir_arms: Default::default(),
                            },
                        );
                        edge_var
                    });

                let pat_id = self.patterns.alloc_pat_id();

                self.build_node(
                    &Pattern {
                        id: pat_id,
                        kind: PatternKind::Variable(*edge_var),
                        span: prop_span,
                    },
                    NodeInfo {
                        expected_ty: Some(ty),
                        parent_struct_flags: Default::default(),
                    },
                    ctx,
                )
            }
            PatternKind::Seq(_, elements) => {
                // FIXME: Unsure how correct this is:
                for element in elements {
                    let node = self.build_implicit_rel_node(ty, &element.pattern, prop_span, ctx);
                    if !matches!(node.1.ty, Type::Error) {
                        return node;
                    }
                }

                self.error_node(CompileError::TODO(smart_format!("")), &prop_span)
            }
            _ => self.error_node(CompileError::TODO(smart_format!("")), &prop_span),
        }
    }

    pub(super) fn build_looping_regex_node(
        &mut self,
        _regex_pattern: &RegexPattern,
        _ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        todo!("looping regex")
    }

    fn build_regex_capture_alternations(
        &mut self,
        node: &RegexPatternCaptureNode,
        pattern_span: &SourceSpan,
        // BUG: We want type inference for the capture groups,
        // that falls back to string if both sides are unknown
        expected_ty: TypeRef<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Vec<Vec<ontol_hir::CaptureGroup<'m, TypedHir>>> {
        match node {
            RegexPatternCaptureNode::Alternation { variants } => variants
                .iter()
                .map(|variant| {
                    self.build_regex_capture_groups(variant, pattern_span, expected_ty, ctx)
                })
                .collect(),
            node => {
                vec![self.build_regex_capture_groups(node, pattern_span, expected_ty, ctx)]
            }
        }
    }

    fn build_regex_capture_groups(
        &mut self,
        node: &RegexPatternCaptureNode,
        pattern_span: &SourceSpan,
        // BUG: We want type inference for the capture groups,
        // that falls back to string if both sides are unknown
        expected_ty: TypeRef<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> Vec<ontol_hir::CaptureGroup<'m, TypedHir>> {
        match node {
            RegexPatternCaptureNode::Capture {
                var,
                capture_index,
                name_span,
            } => {
                let arm = ctx.arm;
                let explicit_variable = ctx
                    .pattern_variables
                    .get_mut(var)
                    .expect("variable not found");

                let arm_pat_id = {
                    let hir_arm = explicit_variable.hir_arms.entry(arm).or_insert_with(|| {
                        let pat_id = self.patterns.alloc_pat_id();

                        ExplicitVariableArm {
                            pat_id,
                            span: *pattern_span,
                        }
                    });
                    hir_arm.pat_id
                };

                let _type_var = ctx.inference.new_type_variable(arm_pat_id);

                vec![ontol_hir::CaptureGroup::<'m, TypedHir> {
                    index: *capture_index,
                    binder: TypedBinder {
                        var: *var,
                        meta: Meta {
                            ty: expected_ty,
                            span: *name_span,
                        },
                    },
                }]
            }
            RegexPatternCaptureNode::Concat { nodes } => nodes
                .iter()
                .flat_map(|node| {
                    self.build_regex_capture_groups(node, pattern_span, expected_ty, ctx)
                })
                .collect(),
            RegexPatternCaptureNode::Alternation { .. } => {
                todo!()
            }
            RegexPatternCaptureNode::Repetition { .. } => {
                todo!()
            }
        }
    }

    pub(super) fn type_error_node(
        &mut self,
        error: TypeError<'m>,
        span: &SourceSpan,
    ) -> TypedHirNode<'m> {
        self.type_error(error, span);
        self.make_error_node(span)
    }

    pub(super) fn error_node(
        &mut self,
        error: CompileError,
        span: &SourceSpan,
    ) -> TypedHirNode<'m> {
        self.error(error, span);
        self.make_error_node(span)
    }

    pub(super) fn make_error_node(&mut self, span: &SourceSpan) -> TypedHirNode<'m> {
        TypedHirNode(
            ontol_hir::Kind::Unit,
            Meta {
                ty: self.types.intern(Type::Error),
                span: *span,
            },
        )
    }

    pub(super) fn unit_node_no_span(&mut self) -> TypedHirNode<'m> {
        TypedHirNode(
            ontol_hir::Kind::Unit,
            Meta {
                ty: self.unit_type(),
                span: NO_SPAN,
            },
        )
    }

    pub(super) fn unit_type(&mut self) -> TypeRef<'m> {
        self.types
            .intern(Type::Primitive(PrimitiveKind::Unit, DefId::unit()))
    }
}
