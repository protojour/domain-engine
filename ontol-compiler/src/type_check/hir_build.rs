use std::str::FromStr;

use ontol_hir::StructFlags;
use ontol_runtime::smart_format;
use tracing::debug;

use crate::{
    def::{Def, DefKind},
    error::CompileError,
    mem::Intern,
    pattern::{PatId, Pattern, PatternKind, RegexPatternCaptureNode, TypePath},
    primitive::PrimitiveKind,
    type_check::{
        ena_inference::{InferValue, Strength},
        hir_build_ctx::{ExplicitVariableArm, PatternVariable},
        hir_build_props::UnpackerInfo,
    },
    typed_hir::{IntoTypedHirData, Meta, TypedHir, TypedHirData},
    types::{Type, TypeRef, ERROR_TYPE, UNIT_TYPE},
    SourceSpan, NO_SPAN,
};

use super::{
    ena_inference::KnownType, hir_build_ctx::HirBuildCtx, TypeCheck, TypeEquation, TypeError,
};

pub(super) struct NodeInfo<'m> {
    pub expected_ty: Option<KnownType<'m>>,
    pub parent_struct_flags: ontol_hir::StructFlags,
}

/// This is the type check of map statements.
/// The types that are used must be checked with `check_def_sealed`.
impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn build_root_pattern(
        &mut self,
        pat_id: PatId,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::RootNode<'m, TypedHir> {
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

        let node_meta = ctx.hir_arena[node].meta();

        // Save typing result for the final type unification:
        match node_meta.ty {
            Type::Error | Type::Infer(_) => {}
            _ => {
                let type_var = ctx.inference.new_type_variable(pat_id);
                debug!("Check pat(2) root type result: {:?}", node_meta.ty);
                ctx.inference
                    .eq_relations
                    .unify_var_value(
                        type_var,
                        InferValue::Known((node_meta.ty, Strength::Strong)),
                    )
                    .unwrap();
            }
        }

        ontol_hir::RootNode::new(node, std::mem::take(&mut ctx.hir_arena))
    }

    pub(super) fn build_node(
        &mut self,
        pattern: &Pattern,
        node_info: NodeInfo<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
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
                                ctx,
                            );
                        }

                        let mut hir_args = vec![];
                        for (arg, param_ty) in args.iter().zip(*params) {
                            let node = self.build_node(
                                arg,
                                NodeInfo {
                                    // Function arguments have weak type constraints.
                                    expected_ty: Some((param_ty, Strength::Weak)),
                                    parent_struct_flags: node_info.parent_struct_flags,
                                },
                                ctx,
                            );
                            hir_args.push(node);
                        }

                        ctx.mk_node(
                            ontol_hir::Kind::Call(*proc, hir_args.into()),
                            Meta {
                                ty: output,
                                span: pattern.span,
                            },
                        )
                    }
                    _ => self.error_node(CompileError::NotCallable, &pattern.span, ctx),
                }
            }
            (
                PatternKind::Compound {
                    type_path:
                        TypePath::Specified {
                            def_id: path_def_id,
                            span: path_span,
                        },
                    modifier,
                    is_unit_binding,
                    attributes,
                },
                expected_ty,
            ) => {
                let struct_ty = self.check_def_sealed(*path_def_id);
                match struct_ty {
                    Type::Domain(def_id) => {
                        assert_eq!(*def_id, *path_def_id);
                    }
                    _ => return self.error_node(CompileError::DomainTypeExpected, path_span, ctx),
                };
                let node = self.build_unpacker(
                    UnpackerInfo {
                        type_def_id: *path_def_id,
                        ty: struct_ty,
                        modifier: *modifier,
                        is_unit_binding: *is_unit_binding,
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    attributes,
                    pattern.span,
                    ctx,
                );

                let meta = *ctx.hir_arena[node].meta();
                match expected_ty {
                    Some((Type::Infer(_), _)) => node,
                    Some((Type::Domain(_), _)) => node,
                    Some((Type::Option(Type::Domain(_)), _)) => {
                        *ctx.hir_arena[node].meta_mut() = Meta {
                            ty: self.types.intern(Type::Option(meta.ty)),
                            span: meta.span,
                        };
                        node
                    }
                    Some((expected_ty, _)) => self.type_error_node(
                        TypeError::Mismatch(TypeEquation {
                            actual: (meta.ty, Strength::Strong),
                            expected: (expected_ty, Strength::Strong),
                        }),
                        &pattern.span,
                        ctx,
                    ),
                    _ => node,
                }
            }
            (
                PatternKind::Compound {
                    type_path: TypePath::RelContextual,
                    modifier,
                    is_unit_binding,
                    attributes,
                },
                Some((expected_struct_ty @ Type::Anonymous(def_id), _)),
            ) => {
                let actual_ty = self.check_def_sealed(*def_id);
                if actual_ty != expected_struct_ty {
                    return self.type_error_node(
                        TypeError::Mismatch(TypeEquation {
                            actual: (actual_ty, Strength::Strong),
                            expected: (expected_struct_ty, Strength::Strong),
                        }),
                        &pattern.span,
                        ctx,
                    );
                }

                self.build_unpacker(
                    UnpackerInfo {
                        type_def_id: *def_id,
                        ty: actual_ty,
                        modifier: *modifier,
                        is_unit_binding: *is_unit_binding,
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    attributes,
                    pattern.span,
                    ctx,
                )
            }
            (
                PatternKind::Compound {
                    type_path:
                        TypePath::Inferred {
                            def_id: infer_def_id,
                        },
                    attributes,
                    ..
                },
                expected_ty,
            ) => {
                // ONTOL syntax (currently) requires that the explicit struct type be specified
                // when being part of a parent expression (the reason why the expected_ty would be defined)
                if expected_ty.is_some() {
                    return self.type_error_node(
                        TypeError::StructTypeNotInferrable,
                        &pattern.span,
                        ctx,
                    );
                }

                if attributes.len() > 1 {
                    // TODO: Register types for anonymous struct, with type inference
                    return self.type_error_node(
                        TypeError::NoRelationParametersExpected,
                        &pattern.span,
                        ctx,
                    );
                }

                // Anonymous top-level struct defined _within the map statement_.
                // It can be mapped _from_ but never _to_, so a resemblance to `match`.
                // The difference is that all the field types must be directly inferred.
                // let lolge = self.defs.alloc_def_id(ONTOL_PKG);
                let ty = self.check_def_sealed(*infer_def_id);
                let meta = Meta {
                    ty,
                    span: pattern.span,
                };
                let struct_binder: TypedHirData<'_, ontol_hir::Binder> =
                    TypedHirData(ctx.var_allocator.alloc().into(), meta);

                ctx.mk_node(
                    ontol_hir::Kind::Struct(struct_binder, StructFlags::MATCH, Default::default()),
                    meta,
                )
            }
            (PatternKind::Seq(aggr_pat_id, pat_elements), expected_ty) => {
                let (rel_ty, val_ty) = match expected_ty {
                    Some((Type::Seq(rel_ty, val_ty), _)) => (*rel_ty, *val_ty),
                    Some((other_ty, _strength)) => {
                        // Handle looping regular expressions
                        if let Type::Primitive(PrimitiveKind::Text, _) | Type::TextLike(..) =
                            other_ty
                        {
                            if pat_elements.len() == 1 {
                                let pat_element = pat_elements.iter().next().unwrap();
                                if let PatternKind::Regex(regex_pattern) = &pat_element.pattern.kind
                                {
                                    if pat_element.iter {
                                        let label = *ctx.label_map.get(aggr_pat_id).unwrap();

                                        let capture_groups_list = self
                                            .build_regex_capture_alternations(
                                                &regex_pattern.capture_node,
                                                &pattern.span,
                                                other_ty,
                                                ctx,
                                            );

                                        return ctx.mk_node(
                                            ontol_hir::Kind::Regex(
                                                Some(TypedHirData(
                                                    label,
                                                    Meta {
                                                        ty: other_ty,
                                                        span: NO_SPAN,
                                                    },
                                                )),
                                                regex_pattern.regex_def_id,
                                                capture_groups_list,
                                            ),
                                            Meta {
                                                ty: other_ty,
                                                span: pattern.span,
                                            },
                                        );
                                    } else {
                                        return self.error_node(
                                            CompileError::RequiresSpreading,
                                            &pat_element.pattern.span,
                                            ctx,
                                        );
                                    }
                                }
                            }
                        }

                        self.type_error(TypeError::MustBeSequence(other_ty), &pattern.span);
                        (&UNIT_TYPE, &ERROR_TYPE)
                    }
                    None => {
                        let pat_id = self.patterns.alloc_pat_id();
                        let val_ty = self
                            .types
                            .intern(Type::Infer(ctx.inference.new_type_variable(pat_id)));

                        debug!("Infer seq val type: {val_ty:?}");
                        (&UNIT_TYPE, val_ty)
                    }
                };

                if pat_elements.len() != 1 {
                    return self.error_node(
                        CompileError::TODO(smart_format!("Standalone seq needs one element")),
                        &pattern.span,
                        ctx,
                    );
                }

                let rel = ctx.mk_unit_node_no_span();
                let val = self.build_node(
                    &pat_elements.iter().next().unwrap().pattern,
                    NodeInfo {
                        expected_ty: Some((val_ty, Strength::Strong)),
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    ctx,
                );
                let label = *ctx.label_map.get(aggr_pat_id).unwrap();
                let seq_ty = self.types.intern(Type::Seq(rel_ty, val_ty));

                ctx.mk_node(
                    ontol_hir::Kind::DeclSeq(
                        label.with_ty(seq_ty),
                        ontol_hir::Attribute { rel, val },
                    ),
                    Meta {
                        ty: seq_ty,
                        span: pattern.span,
                    },
                )
            }
            (PatternKind::ConstI64(int), Some(expected_ty)) => match expected_ty {
                (Type::Primitive(PrimitiveKind::I64, _), _strengt) => ctx.mk_node(
                    ontol_hir::Kind::I64(*int),
                    Meta {
                        ty: expected_ty.0,
                        span: pattern.span,
                    },
                ),
                (Type::Primitive(PrimitiveKind::F64, _), _strength) => {
                    // Didn't find a way to go from i64 to f64 in Rust std..
                    match f64::from_str(&int.to_string()) {
                        Ok(float) => ctx.mk_node(
                            ontol_hir::Kind::F64(float),
                            Meta {
                                ty: expected_ty.0,
                                span: pattern.span,
                            },
                        ),
                        Err(_) => {
                            self.error_node(CompileError::IncompatibleLiteral, &pattern.span, ctx)
                        }
                    }
                }
                _ => self.error_node(CompileError::IncompatibleLiteral, &pattern.span, ctx),
            },
            (PatternKind::ConstText(literal), Some(expected_ty)) => match expected_ty {
                (Type::Primitive(PrimitiveKind::Text, _), _strength) => ctx.mk_node(
                    ontol_hir::Kind::Text(literal.clone()),
                    Meta {
                        ty: expected_ty.0,
                        span: pattern.span,
                    },
                ),
                (Type::TextConstant(def_id), _strength) => match self.defs.def_kind(*def_id) {
                    DefKind::TextLiteral(lit) if literal == lit => ctx.mk_node(
                        ontol_hir::Kind::Text(literal.clone()),
                        Meta {
                            ty: expected_ty.0,
                            span: pattern.span,
                        },
                    ),
                    _ => self.error_node(CompileError::IncompatibleLiteral, &pattern.span, ctx),
                },
                _ => self.error_node(CompileError::IncompatibleLiteral, &pattern.span, ctx),
            },
            (PatternKind::Variable(var), expected_ty) => {
                let arm = ctx.arm;
                let pattern_variable = ctx
                    .pattern_variables
                    .get_mut(var)
                    .expect("variable not found");

                let arm_pat_id = {
                    let hir_arm = pattern_variable.hir_arms.entry(arm).or_insert_with(|| {
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
                    Some((Type::Seq(_rel_ty, val_ty), _)) => self.type_error_node(
                        TypeError::VariableMustBeSequenceEnclosed(val_ty),
                        &pattern.span,
                        ctx,
                    ),
                    Some(expected_ty) => {
                        let variable_ref = ctx.mk_node(
                            ontol_hir::Kind::Var(*var),
                            Meta {
                                ty: expected_ty.0,
                                span: pattern.span,
                            },
                        );

                        debug!("Unifying type inference for variable {var}: {expected_ty:?}");

                        match ctx
                            .inference
                            .eq_relations
                            .unify_var_value(type_var, InferValue::Known(expected_ty))
                        {
                            // Variables are the same type, no mapping necessary:
                            Ok(_) => variable_ref,
                            // Need to map:
                            Err(err @ TypeError::Mismatch(type_eq)) => {
                                match (&type_eq.actual, &type_eq.expected) {
                                    ((Type::Domain(_), _), (Type::Domain(_), _)) => {
                                        panic!("Should not happen anymore");
                                    }

                                    _ => self.type_error_node(err, &pattern.span, ctx),
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
                (Type::Primitive(PrimitiveKind::Text, _) | Type::TextConstant(_), _strength) => {
                    let capture_groups_list = self.build_regex_capture_alternations(
                        &regex_pattern.capture_node,
                        &pattern.span,
                        expected_ty.0,
                        ctx,
                    );

                    ctx.mk_node(
                        ontol_hir::Kind::Regex(
                            None,
                            regex_pattern.regex_def_id,
                            capture_groups_list,
                        ),
                        Meta {
                            ty: expected_ty.0,
                            span: pattern.span,
                        },
                    )
                }
                _ => self.error_node(CompileError::IncompatibleLiteral, &pattern.span, ctx),
            },
            (kind, ty) => self.error_node(
                CompileError::TODO(smart_format!(
                    "Not enough type information for {kind:?}, expected_ty = {ty:?}"
                )),
                &pattern.span,
                ctx,
            ),
        };

        let hir_data = &ctx.hir_arena[node];

        debug!(
            "expected/meta: {:?} {:?}",
            node_info.expected_ty,
            hir_data.ty()
        );

        match (node_info.expected_ty, hir_data.ty()) {
            (_, Type::Error | Type::Infer(_)) => {}
            (Some((Type::Infer(type_var), strength)), _) => {
                ctx.inference
                    .eq_relations
                    .unify_var_value(*type_var, InferValue::Known((hir_data.ty(), strength)))
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
    ) -> ontol_hir::Node {
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
                        expected_ty: Some((ty, Strength::Strong)),
                        parent_struct_flags: Default::default(),
                    },
                    ctx,
                )
            }
            PatternKind::Seq(_, elements) => {
                // FIXME: Unsure how correct this is:
                for element in elements {
                    let node = self.build_implicit_rel_node(ty, &element.pattern, prop_span, ctx);
                    if !matches!(ctx.hir_arena[node].meta().ty, Type::Error) {
                        return node;
                    }
                }

                self.error_node(CompileError::TODO(smart_format!("")), &prop_span, ctx)
            }
            _ => self.error_node(CompileError::TODO(smart_format!("")), &prop_span, ctx),
        }
    }

    fn build_regex_capture_alternations(
        &mut self,
        node: &RegexPatternCaptureNode,
        pattern_span: &SourceSpan,
        // BUG: We want type inference for the capture groups,
        // that falls back to text if both sides are unknown
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
        // that falls back to text if both sides are unknown
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
                let pattern_variable = ctx
                    .pattern_variables
                    .get_mut(var)
                    .expect("variable not found");

                let arm_pat_id = {
                    let hir_arm = pattern_variable.hir_arms.entry(arm).or_insert_with(|| {
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
                    binder: ontol_hir::Binder { var: *var }.with_meta(Meta {
                        ty: expected_ty,
                        span: *name_span,
                    }),
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
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        self.type_error(error, span);
        self.make_error_node(span, ctx)
    }

    pub(super) fn error_node(
        &mut self,
        error: CompileError,
        span: &SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        self.error(error, span);
        self.make_error_node(span, ctx)
    }

    pub(super) fn make_error_node(
        &mut self,
        span: &SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        ctx.mk_node(
            ontol_hir::Kind::Unit,
            Meta {
                ty: &ERROR_TYPE,
                span: *span,
            },
        )
    }
}
