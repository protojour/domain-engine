use std::str::FromStr;

use ontol_hir::{Label, StructFlags};
use ontol_parser::source::{NO_SPAN, SourceSpan};
use ontol_runtime::DefId;
use smallvec::{SmallVec, smallvec};
use thin_vec::{ThinVec, thin_vec};
use tracing::debug;

use crate::{
    SpannedCompileError,
    def::{Def, DefKind},
    error::CompileError,
    mem::Intern,
    pattern::{
        CompoundPatternModifier, PatId, Pattern, PatternKind, RegexPatternCaptureNode,
        SetPatternElement, TypePath,
    },
    primitive::PrimitiveKind,
    repr::repr_model::ReprKind,
    type_check::{
        ena_inference::{InferValue, Strength},
        hir_build_ctx::{ExplicitVariableArm, PatternVariable},
        hir_build_props::UnpackerInfo,
    },
    typed_hir::{IntoTypedHirData, Meta, TypedHir, TypedHirData, TypedRootNode},
    types::{ERROR_TYPE, Type, TypeRef, UNIT_TYPE},
};

use super::{
    MapArmsKind, TypeCheck, TypeEquation, TypeError, ena_inference::KnownType,
    hir_build_ctx::HirBuildCtx,
};

pub(super) struct NodeInfo<'m> {
    pub expected_ty: Option<KnownType<'m>>,
    pub parent_struct_flags: ontol_hir::StructFlags,
}

/// This is the type check of map statements.
/// The types that are used must be checked with `check_def_sealed`.
impl<'m> TypeCheck<'_, 'm> {
    pub(super) fn build_root_pattern(
        &mut self,
        pat_id: PatId,
        arms_kind: MapArmsKind,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedRootNode<'m> {
        let pattern = match arms_kind {
            MapArmsKind::Concrete => {
                // pattern will not be reused
                self.patterns.table.remove(&pat_id).unwrap()
            }
            MapArmsKind::Template(def_ids) => self.apply_arm_template(
                self.patterns.table.get(&pat_id).unwrap().clone(),
                def_ids[ctx.current_arm.index()],
            ),
            MapArmsKind::Abstract => unreachable!(),
        };

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
                debug!(ty = ?node_meta.ty, "Check pat(2) root type result");
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
            (PatternKind::Call(def_id, params), Some(expected_ty)) => {
                match (
                    self.defs.table.get(def_id),
                    self.def_ty_ctx.def_table.get(def_id),
                ) {
                    (
                        Some(Def {
                            kind: DefKind::Fn(proc),
                            ..
                        }),
                        Some(Type::Function(func_type)),
                    ) => {
                        let mut infer_args = vec![];
                        for _ in 0..params.len() {
                            infer_args.push(None);
                        }

                        let sig = func_type.signature(
                            &infer_args,
                            Some(expected_ty.0),
                            self.def_ty_ctx,
                            self.repr_ctx,
                            self.type_ctx,
                        );

                        debug!(?sig, "call");

                        if params.len() != sig.args.len() {
                            return self.error_node(
                                CompileError::IncorrectNumberOfArguments {
                                    expected: u8::try_from(sig.args.len()).unwrap(),
                                    actual: u8::try_from(params.len()).unwrap(),
                                }
                                .span(pattern.span),
                                ctx,
                            );
                        }

                        let mut hir_args = vec![];
                        for (param, arg_ty) in params.iter().zip(sig.args) {
                            let node = self.build_node(
                                param,
                                NodeInfo {
                                    // Function arguments have weak type constraints.
                                    expected_ty: Some((arg_ty, Strength::Weak)),
                                    parent_struct_flags: node_info.parent_struct_flags,
                                },
                                ctx,
                            );
                            hir_args.push(node);
                        }

                        ctx.mk_node(
                            ontol_hir::Kind::Call(*proc, hir_args.into()),
                            Meta {
                                ty: sig.output,
                                span: pattern.span,
                            },
                        )
                    }
                    _ => self.error_node(CompileError::NotCallable.span(pattern.span), ctx),
                }
            }
            (
                PatternKind::Compound {
                    type_path: TypePath::RelContextual,
                    modifier,
                    is_unit_binding,
                    attributes,
                    spread_label: _,
                },
                Some((expected_struct_ty @ (Type::DomainDef(def_id) | Type::Anonymous(def_id)), _)),
            ) => {
                let actual_ty = self.check_def(*def_id);
                if actual_ty != expected_struct_ty {
                    return self.type_error_node(
                        TypeError::Mismatch(TypeEquation {
                            actual: (actual_ty, Strength::Strong),
                            expected: (expected_struct_ty, Strength::Strong),
                        }),
                        pattern.span,
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
                    type_path,
                    modifier,
                    is_unit_binding,
                    attributes,
                    spread_label: _,
                },
                expected_ty,
            ) => {
                let mut modifier = *modifier;
                let (struct_ty, path_def_id) = match type_path {
                    TypePath::Specified {
                        def_id: path_def_id,
                        span,
                    } => {
                        let struct_ty = self.check_def(*path_def_id);
                        match struct_ty {
                            Type::DomainDef(def_id) => {
                                assert_eq!(*def_id, *path_def_id);
                            }
                            _ => {
                                return self
                                    .error_node(CompileError::DomainTypeExpected.span(*span), ctx);
                            }
                        };
                        (struct_ty, *path_def_id)
                    }
                    TypePath::Inferred { def_id } => {
                        let struct_ty = self.check_def(*def_id);
                        // ONTOL syntax (currently) requires that the explicit struct type be specified
                        // when being part of a parent expression (the reason why the expected_ty would be defined)
                        if expected_ty.is_some() {
                            return self.type_error_node(
                                TypeError::StructTypeNotInferrable,
                                pattern.span,
                                ctx,
                            );
                        }
                        // Forced match
                        modifier = Some(CompoundPatternModifier::Match);
                        (struct_ty, *def_id)
                    }
                    TypePath::RelContextual => unreachable!("rel contextual"),
                };

                let node = self.build_unpacker(
                    UnpackerInfo {
                        type_def_id: path_def_id,
                        ty: struct_ty,
                        modifier,
                        is_unit_binding: *is_unit_binding,
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    attributes,
                    pattern.span,
                    ctx,
                );

                debug!(?expected_ty, "built unpacker node");

                let meta = *ctx.hir_arena[node].meta();
                match expected_ty {
                    Some((Type::Infer(_), _)) => node,
                    Some((
                        expected_ty @ (Type::DomainDef(def_id) | Type::Anonymous(def_id)),
                        _,
                    )) => {
                        match self.repr_ctx.get_repr_kind(def_id) {
                            Some(ReprKind::Union(members, _)) => {
                                let hir_def_id = meta
                                    .ty
                                    .get_single_def_id()
                                    .unwrap_or_else(|| panic!("no def_id for {:?}", meta.ty));

                                if !members
                                    .iter()
                                    .any(|(member_def_id, _)| *member_def_id == hir_def_id)
                                {
                                    self.type_error_node(
                                        TypeError::Mismatch(TypeEquation {
                                            actual: (meta.ty, Strength::Strong),
                                            expected: (expected_ty, Strength::Strong),
                                        }),
                                        pattern.span,
                                        ctx,
                                    )
                                } else if node_info.parent_struct_flags.contains(StructFlags::MATCH)
                                {
                                    // This is one-way, so a variant is OK
                                    node
                                } else {
                                    ctx.partial = true;
                                    ctx.mk_node(
                                        ontol_hir::Kind::Narrow(node),
                                        Meta {
                                            ty: expected_ty,
                                            span: meta.span,
                                        },
                                    )
                                }
                            }
                            _ => node,
                        }
                    }
                    Some((Type::Option(Type::DomainDef(_)), _)) => {
                        *ctx.hir_arena[node].meta_mut() = Meta {
                            ty: self.type_ctx.intern(Type::Option(meta.ty)),
                            span: meta.span,
                        };
                        node
                    }
                    Some((expected_ty, _)) => self.type_error_node(
                        TypeError::Mismatch(TypeEquation {
                            actual: (meta.ty, Strength::Strong),
                            expected: (expected_ty, Strength::Strong),
                        }),
                        pattern.span,
                        ctx,
                    ),
                    _ => node,
                }
            }
            (
                PatternKind::Set {
                    val_type_def,
                    elements,
                },
                expected_ty,
            ) => {
                let val_ty = match expected_ty {
                    Some((Type::Seq(val_ty), _)) => *val_ty,
                    Some((other_ty, _strength)) => {
                        // Handle looping regular expressions
                        if let Type::Primitive(PrimitiveKind::Text, _) | Type::TextLike(..) =
                            other_ty
                        {
                            if elements.len() == 1 {
                                let pat_element = elements.iter().next().unwrap();
                                if let PatternKind::Regex(regex_pattern) = &pat_element.val.kind {
                                    if pat_element.is_iter {
                                        let label = *ctx.label_map.get(&pat_element.id).unwrap();

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
                                                    Label(label.0),
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
                                            CompileError::RequiresSpreading
                                                .span(pat_element.val.span),
                                            ctx,
                                        );
                                    }
                                }
                            }
                        }

                        TypeError::MustBeSequence(other_ty).report(pattern.span, self);
                        &ERROR_TYPE
                    }
                    None => {
                        let val_ty = val_type_def
                            .map(|def_id| self.check_def(def_id))
                            .unwrap_or_else(|| {
                                let val_pat_id = self.patterns.alloc_pat_id();
                                let val_ty = self.type_ctx.intern(Type::Infer(
                                    ctx.inference.new_type_variable(val_pat_id),
                                ));

                                debug!(?val_ty, "infer seq val type");
                                val_ty
                            });

                        val_ty
                    }
                };

                if elements.len() != 1 {
                    return self.error_node(
                        CompileError::TODO("Standalone set needs one element").span(pattern.span),
                        ctx,
                    );
                }

                let first_element = elements.iter().next().unwrap();

                let val = self.build_node(
                    &first_element.val,
                    NodeInfo {
                        expected_ty: Some((val_ty, Strength::Strong)),
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    ctx,
                );
                let Some(label) = ctx.label_map.get(&first_element.id) else {
                    panic!("No label for pattern element");
                };
                let per_column_types = self.type_ctx.intern([val_ty]);
                let seq_ty = self.type_ctx.intern(Type::Matrix(per_column_types));

                ctx.mk_node(
                    ontol_hir::Kind::Matrix(
                        [ontol_hir::MatrixRow(
                            Some(TypedHirData(
                                Label(label.0),
                                Meta::new(seq_ty, pattern.span),
                            )),
                            smallvec![val],
                        )]
                        .into(),
                    ),
                    Meta {
                        ty: seq_ty,
                        span: pattern.span,
                    },
                )
            }
            (PatternKind::ConstInt(int), Some(expected_ty)) => {
                debug!(?expected_ty, "ConstInt");
                match expected_ty {
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
                            Err(_) => self.error_node(
                                CompileError::IncompatibleLiteral.span(pattern.span),
                                ctx,
                            ),
                        }
                    }
                    _ => self.error_node(CompileError::IncompatibleLiteral.span(pattern.span), ctx),
                }
            }
            (PatternKind::ConstBool(bool), Some(expected_ty)) => match expected_ty {
                (Type::Primitive(PrimitiveKind::Boolean, _), _strengt) => ctx.mk_node(
                    ontol_hir::Kind::I64(if *bool { 1 } else { 0 }),
                    Meta {
                        ty: expected_ty.0,
                        span: pattern.span,
                    },
                ),
                _ => self.error_node(CompileError::IncompatibleLiteral.span(pattern.span), ctx),
            },
            (PatternKind::ConstText(literal), Some(expected_ty)) => match expected_ty {
                (Type::Primitive(PrimitiveKind::Text, _), _strength) => ctx.mk_node(
                    ontol_hir::Kind::Text(literal.into()),
                    Meta {
                        ty: expected_ty.0,
                        span: pattern.span,
                    },
                ),
                (Type::TextConstant(def_id), _strength) => match self.defs.def_kind(*def_id) {
                    DefKind::TextLiteral(lit) if literal == lit => ctx.mk_node(
                        ontol_hir::Kind::Text(literal.into()),
                        Meta {
                            ty: expected_ty.0,
                            span: pattern.span,
                        },
                    ),
                    _ => self.error_node(CompileError::IncompatibleLiteral.span(pattern.span), ctx),
                },
                _ => self.error_node(CompileError::IncompatibleLiteral.span(pattern.span), ctx),
            },
            (PatternKind::Variable(var), expected_ty) => {
                let arm = ctx.current_arm;
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
                    Some((Type::Seq(val_ty), _)) => self.type_error_node(
                        TypeError::VariableMustBeSequenceEnclosed(val_ty),
                        pattern.span,
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

                        debug!(
                            %var,
                            ?expected_ty,
                            "Unifying type inference for variable"
                        );

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
                                    ((Type::DomainDef(_), _), (Type::DomainDef(_), _)) => {
                                        panic!("Should not happen anymore");
                                    }

                                    _ => self.type_error_node(err, pattern.span, ctx),
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
                _ => self.error_node(CompileError::IncompatibleLiteral.span(pattern.span), ctx),
            },
            (PatternKind::Error, _) => self.make_error_node(pattern.span, ctx),
            (kind, ty) => self.error_node(
                CompileError::TODO(format!(
                    "Not enough type information for {kind:?}, expected_ty = {ty:?}"
                ))
                .span(pattern.span),
                ctx,
            ),
        };

        let hir_data = &ctx.hir_arena[node];

        debug!(
            expected = ?node_info.expected_ty,
            ty = ?hir_data.ty(),
            "built node"
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
                                set_element_group: None,
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
            PatternKind::Set { elements, .. } => {
                // FIXME: Unsure how correct this is:
                for element in elements.iter() {
                    let node = self.build_implicit_rel_node(ty, &element.val, prop_span, ctx);
                    if !matches!(ctx.hir_arena[node].meta().ty, Type::Error) {
                        return node;
                    }
                }

                self.error_node(CompileError::TODO("Seq problem").span(prop_span), ctx)
            }
            _pat => self.error_node(
                CompileError::TODO("Explicit mapping of relation parameters needed")
                    .span(prop_span),
                ctx,
            ),
        }
    }

    pub(super) fn build_hir_set_of(
        &mut self,
        elements: &[SetPatternElement],
        rel_ty: TypeRef<'m>,
        val_ty: TypeRef<'m>,
        span: SourceSpan,
        parent_struct_flags: StructFlags,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        let mut set_entries: SmallVec<ontol_hir::MatrixRow<'m, TypedHir>, 1> =
            SmallVec::with_capacity(elements.len());

        for element in elements {
            let rel = element.rel.as_ref().map(|pattern| {
                self.build_node(
                    pattern,
                    NodeInfo {
                        expected_ty: Some((rel_ty, Strength::Strong)),
                        parent_struct_flags,
                    },
                    ctx,
                )
            });
            let val = self.build_node(
                &element.val,
                NodeInfo {
                    expected_ty: Some((val_ty, Strength::Strong)),
                    parent_struct_flags,
                },
                ctx,
            );

            let label = if element.is_iter {
                let Some(label) = ctx.label_map.get(&element.id) else {
                    return self.error_node(CompileError::TODO("missing label").span(span), ctx);
                };
                Some(TypedHirData(
                    *label,
                    Meta {
                        ty: &UNIT_TYPE,
                        span,
                    },
                ))
            } else {
                None
            };

            let mut elements = smallvec![val];
            if let Some(rel) = rel {
                elements.push(rel);
            }

            set_entries.push(ontol_hir::MatrixRow(label, elements));
        }

        ctx.mk_node(
            ontol_hir::Kind::Matrix(set_entries),
            Meta {
                ty: &UNIT_TYPE,
                span,
            },
        )
    }

    fn build_regex_capture_alternations(
        &mut self,
        node: &RegexPatternCaptureNode,
        pattern_span: &SourceSpan,
        // BUG: We want type inference for the capture groups,
        // that falls back to text if both sides are unknown
        expected_ty: TypeRef<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ThinVec<ThinVec<ontol_hir::CaptureGroup<'m, TypedHir>>> {
        match node {
            RegexPatternCaptureNode::Alternation { variants } => variants
                .iter()
                .map(|variant| {
                    self.build_regex_capture_groups(variant, pattern_span, expected_ty, ctx)
                })
                .collect(),
            node => {
                thin_vec![self.build_regex_capture_groups(node, pattern_span, expected_ty, ctx)]
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
    ) -> ThinVec<ontol_hir::CaptureGroup<'m, TypedHir>> {
        match node {
            RegexPatternCaptureNode::Capture {
                var,
                capture_index,
                name_span,
            } => {
                let arm = ctx.current_arm;
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

                thin_vec![ontol_hir::CaptureGroup::<'m, TypedHir> {
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

    fn apply_arm_template(&mut self, mut pattern: Pattern, type_def_id: DefId) -> Pattern {
        if let PatternKind::Compound { type_path, .. } = &mut pattern.kind {
            if let TypePath::Specified { def_id, .. } = type_path {
                *def_id = type_def_id;
            } else {
                CompileError::TODO("cannot apply pattern")
                    .span(pattern.span)
                    .report(self.errors);
            }
        }

        pattern
    }

    pub(super) fn type_error_node(
        &mut self,
        error: TypeError<'m>,
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        error.report(span, self);
        self.make_error_node(span, ctx)
    }

    pub(super) fn error_node(
        &mut self,
        error: SpannedCompileError,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        let span = error.span();
        error.report(self);
        self.make_error_node(span, ctx)
    }

    pub(super) fn make_error_node(
        &mut self,
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> ontol_hir::Node {
        ctx.mk_node(
            ontol_hir::Kind::Unit,
            Meta {
                ty: &ERROR_TYPE,
                span,
            },
        )
    }
}
