use std::str::FromStr;

use indexmap::IndexMap;
use ontol_runtime::{
    ontology::{Cardinality, PropertyCardinality, ValueCardinality},
    smart_format,
    value::PropertyId,
    DefId, Role,
};
use tracing::debug;

use crate::{
    def::{Def, DefKind, LookupRelationshipMeta, RelParams},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, ExprStructAttr, ExprStructModifier},
    mem::Intern,
    primitive::PrimitiveKind,
    type_check::{
        hir_build_ctx::{ExplicitVariableArm, ExpressionVariable},
        inference::UnifyValue,
        repr::repr_model::ReprKind,
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

struct StructInfo<'m> {
    struct_def_id: DefId,
    struct_ty: TypeRef<'m>,
    modifier: Option<ExprStructModifier>,
    parent_struct_flags: ontol_hir::StructFlags,
}

/// This is the type check of map statements.
/// The types that are used must be checked with `check_def_sealed`.
impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn build_root_expr(
        &mut self,
        expr_id: ExprId,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        let expr = self.expressions.table.remove(&expr_id).unwrap();

        let node = self.build_node(
            &expr,
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
                let type_var = ctx.inference.new_type_variable(expr_id);
                debug!("Check expr(2) root type result: {:?}", node.ty());
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
        expr: &Expr,
        node_info: NodeInfo<'m>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        let node = match (&expr.kind, node_info.expected_ty) {
            (ExprKind::Call(def_id, args), Some(_expected_output)) => {
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
                                &expr.span,
                            );
                        }

                        let mut parameters = vec![];
                        for (arg, param_ty) in args.iter().zip(*params) {
                            let node = self.build_node(
                                arg,
                                NodeInfo {
                                    expected_ty: Some(param_ty),
                                    parent_struct_flags: node_info.parent_struct_flags,
                                },
                                ctx,
                            );
                            parameters.push(node);
                        }

                        TypedHirNode(
                            ontol_hir::Kind::Call(*proc, parameters),
                            Meta {
                                ty: output,
                                span: expr.span,
                            },
                        )
                    }
                    _ => self.error_node(CompileError::NotCallable, &expr.span),
                }
            }
            (
                ExprKind::Struct {
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
                    expr.span,
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
                        &expr.span,
                    ),
                    _ => struct_node,
                }
            }
            (
                ExprKind::Struct {
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
                        &expr.span,
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
                    expr.span,
                    ctx,
                )
            }
            (
                ExprKind::Struct {
                    type_path: None, ..
                },
                _,
            ) => self.type_error_node(TypeError::NoRelationParametersExpected, &expr.span),
            (ExprKind::Seq(aggr_expr_id, inner), expected_ty) => {
                let (rel_ty, val_ty) = match expected_ty {
                    Some(Type::Seq(rel_ty, val_ty)) => (*rel_ty, *val_ty),
                    Some(other_ty) => {
                        self.type_error(TypeError::MustBeSequence(other_ty), &expr.span);
                        (self.unit_type(), self.types.intern(Type::Error))
                    }
                    None => {
                        let expr_id = self.expressions.alloc_expr_id();
                        let val_ty = self
                            .types
                            .intern(Type::Infer(ctx.inference.new_type_variable(expr_id)));

                        debug!("Infer seq val type: {val_ty:?}");
                        (self.unit_type(), val_ty)
                    }
                };

                if inner.len() != 1 {
                    return self.error_node(
                        CompileError::TODO(smart_format!("Standalone seq needs one element")),
                        &expr.span,
                    );
                }

                let inner_node = self.build_node(
                    &inner.iter().next().unwrap().expr,
                    NodeInfo {
                        expected_ty: Some(val_ty),
                        parent_struct_flags: node_info.parent_struct_flags,
                    },
                    ctx,
                );
                let label = *ctx.label_map.get(aggr_expr_id).unwrap();
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
                        span: expr.span,
                    },
                )
            }
            (ExprKind::ConstI64(int), Some(expected_ty)) => match expected_ty {
                Type::Primitive(PrimitiveKind::I64, _) => TypedHirNode(
                    ontol_hir::Kind::I64(*int),
                    Meta {
                        ty: expected_ty,
                        span: expr.span,
                    },
                ),
                Type::Primitive(PrimitiveKind::F64, _) => {
                    // Didn't find a way to go from i64 to f64 in Rust std..
                    match f64::from_str(&int.to_string()) {
                        Ok(float) => TypedHirNode(
                            ontol_hir::Kind::F64(float),
                            Meta {
                                ty: expected_ty,
                                span: expr.span,
                            },
                        ),
                        Err(_) => self.error_node(CompileError::IncompatibleLiteral, &expr.span),
                    }
                }
                _ => self.error_node(CompileError::IncompatibleLiteral, &expr.span),
            },
            (ExprKind::ConstString(string), Some(expected_ty)) => match expected_ty {
                Type::Primitive(PrimitiveKind::String, _) => TypedHirNode(
                    ontol_hir::Kind::String(string.clone()),
                    Meta {
                        ty: expected_ty,
                        span: expr.span,
                    },
                ),
                Type::StringConstant(def_id) => match self.defs.def_kind(*def_id) {
                    DefKind::StringLiteral(lit) if string == lit => TypedHirNode(
                        ontol_hir::Kind::String(string.clone()),
                        Meta {
                            ty: expected_ty,
                            span: expr.span,
                        },
                    ),
                    _ => self.error_node(CompileError::IncompatibleLiteral, &expr.span),
                },
                _ => self.error_node(CompileError::IncompatibleLiteral, &expr.span),
            },
            (ExprKind::Variable(var), expected_ty) => {
                let arm = ctx.arm;
                let explicit_variable =
                    ctx.expr_variables.get_mut(var).expect("variable not found");

                let arm_expr_id = {
                    let hir_arm = explicit_variable.hir_arms.entry(arm).or_insert_with(|| {
                        let expr_id = self.expressions.alloc_expr_id();

                        ExplicitVariableArm {
                            expr_id,
                            span: expr.span,
                        }
                    });
                    hir_arm.expr_id
                };

                let type_var = ctx.inference.new_type_variable(arm_expr_id);

                match expected_ty {
                    Some(Type::Seq(_rel_ty, val_ty)) => self.type_error_node(
                        TypeError::VariableMustBeSequenceEnclosed(val_ty),
                        &expr.span,
                    ),
                    Some(expected_ty) => {
                        let variable_ref = TypedHirNode(
                            ontol_hir::Kind::Var(*var),
                            Meta {
                                ty: expected_ty,
                                span: expr.span,
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

                                    _ => self.type_error_node(err, &expr.span),
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
            (ExprKind::Regex(expr_regex), Some(expected_ty)) => match expected_ty {
                // TODO: Handle compile-time match of string constant?
                Type::Primitive(PrimitiveKind::String, _) | Type::StringConstant(_) => {
                    let mut capture_groups: Vec<_> = Vec::with_capacity(expr_regex.captures.len());

                    for (var, group_index) in &expr_regex.captures {
                        let arm = ctx.arm;
                        let explicit_variable =
                            ctx.expr_variables.get_mut(var).expect("variable not found");

                        let arm_expr_id = {
                            let hir_arm =
                                explicit_variable.hir_arms.entry(arm).or_insert_with(|| {
                                    let expr_id = self.expressions.alloc_expr_id();

                                    ExplicitVariableArm {
                                        expr_id,
                                        span: expr.span,
                                    }
                                });
                            hir_arm.expr_id
                        };

                        let _type_var = ctx.inference.new_type_variable(arm_expr_id);

                        capture_groups.push(ontol_hir::CaptureGroup::<'m, TypedHir> {
                            index: *group_index,
                            binder: TypedBinder {
                                var: *var,
                                ty: expected_ty,
                            },
                        })
                    }

                    TypedHirNode(
                        ontol_hir::Kind::Regex(expr_regex.regex_def_id, capture_groups),
                        Meta {
                            ty: expected_ty,
                            span: expr.span,
                        },
                    )
                }
                _ => self.error_node(CompileError::IncompatibleLiteral, &expr.span),
            },
            (kind, ty) => self.error_node(
                CompileError::TODO(smart_format!(
                    "Not enough type information for {kind:?}, expected_ty = {ty:?}"
                )),
                &expr.span,
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

    fn build_property_matcher(
        &mut self,
        StructInfo {
            struct_def_id,
            struct_ty,
            modifier,
            parent_struct_flags,
        }: StructInfo<'m>,
        attributes: &[ExprStructAttr],
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        struct MatchProperty {
            property_id: PropertyId,
            cardinality: Cardinality,
            rel_params_def: Option<DefId>,
            value_def: DefId,
            used: bool,
        }

        let properties = self.relations.properties_by_def_id(struct_def_id);

        let actual_struct_flags = match modifier {
            Some(ExprStructModifier::Match) => ontol_hir::StructFlags::MATCH,
            None => ontol_hir::StructFlags::empty(),
        } | parent_struct_flags;

        let node_kind = match self.seal_ctx.get_repr_kind(&struct_def_id).unwrap() {
            ReprKind::Struct | ReprKind::Unit => {
                match properties.and_then(|props| props.table.as_ref()) {
                    Some(property_set) => {
                        let struct_binder = TypedBinder {
                            var: ctx.var_allocator.alloc(),
                            ty: struct_ty,
                        };

                        let mut match_properties = property_set
                            .iter()
                            .filter_map(|(property_id, _cardinality)| {
                                let meta = self.defs.relationship_meta(property_id.relationship_id);
                                let property_name = match property_id.role {
                                    Role::Subject => match meta.relation_def_kind.value {
                                        DefKind::StringLiteral(lit) => Some(*lit),
                                        _ => panic!("BUG: Expected named subject property"),
                                    },
                                    Role::Object => meta.relationship.object_prop,
                                };
                                let (_, owner_cardinality, _) =
                                    meta.relationship.by(property_id.role);
                                let (value_def_id, _, _) =
                                    meta.relationship.by(property_id.role.opposite());

                                property_name.map(|property_name| {
                                    (
                                        property_name,
                                        MatchProperty {
                                            property_id: *property_id,
                                            cardinality: owner_cardinality,
                                            rel_params_def: match &meta.relationship.rel_params {
                                                RelParams::Type(def_id) => Some(*def_id),
                                                _ => None,
                                            },
                                            value_def: value_def_id,
                                            used: false,
                                        },
                                    )
                                })
                            })
                            .collect::<IndexMap<_, _>>();

                        let mut hir_props = vec![];

                        for ExprStructAttr {
                            key: (def_id, prop_span),
                            rel,
                            bind_option,
                            value,
                        } in attributes
                        {
                            let DefKind::StringLiteral(attr_prop) = self.defs.def_kind(*def_id)
                            else {
                                self.error(CompileError::NamedPropertyExpected, prop_span);
                                continue;
                            };
                            let Some(match_property) = match_properties.get_mut(attr_prop) else {
                                self.error(CompileError::UnknownProperty, prop_span);
                                continue;
                            };
                            if match_property.used {
                                self.error(CompileError::DuplicateProperty, prop_span);
                                continue;
                            }
                            match_property.used = true;

                            let rel_params_ty = match match_property.rel_params_def {
                                Some(rel_def_id) => self.check_def_sealed(rel_def_id),
                                None => self.unit_type(),
                            };
                            debug!("rel_params_ty: {rel_params_ty:?}");

                            let rel_node = match (rel_params_ty, rel) {
                                (Type::Primitive(PrimitiveKind::Unit, _), Some(rel)) => self
                                    .error_node(
                                        CompileError::NoRelationParametersExpected,
                                        &rel.span,
                                    ),
                                (ty @ Type::Primitive(PrimitiveKind::Unit, _), None) => {
                                    TypedHirNode(
                                        ontol_hir::Kind::Unit,
                                        Meta {
                                            ty,
                                            span: *prop_span,
                                        },
                                    )
                                }
                                (_, Some(rel)) => self.build_node(
                                    rel,
                                    NodeInfo {
                                        expected_ty: Some(rel_params_ty),
                                        parent_struct_flags: actual_struct_flags,
                                    },
                                    ctx,
                                ),
                                (ty @ Type::Anonymous(def_id), None) => {
                                    match self.relations.properties_by_def_id(*def_id) {
                                        Some(_) => {
                                            self.build_implicit_rel_node(ty, value, *prop_span, ctx)
                                        }
                                        // An anonymous type without properties, i.e. just "meta relationships" about the relationship itself:
                                        None => TypedHirNode(
                                            ontol_hir::Kind::Unit,
                                            Meta {
                                                ty,
                                                span: *prop_span,
                                            },
                                        ),
                                    }
                                }
                                (ty, None) => {
                                    self.build_implicit_rel_node(ty, value, *prop_span, ctx)
                                }
                            };

                            let value_ty = self.check_def_sealed(match_property.value_def);
                            debug!("value_ty: {value_ty:?}");

                            let prop_variant = match match_property.cardinality.1 {
                                ValueCardinality::One => {
                                    let val_node = self.build_node(
                                        value,
                                        NodeInfo {
                                            expected_ty: Some(value_ty),
                                            parent_struct_flags: actual_struct_flags,
                                        },
                                        ctx,
                                    );
                                    ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                        rel: Box::new(rel_node),
                                        val: Box::new(val_node),
                                    })
                                }
                                ValueCardinality::Many => match &value.kind {
                                    ExprKind::Seq(aggr_expr_id, expr_elements) => {
                                        let mut hir_elements =
                                            Vec::with_capacity(expr_elements.len());
                                        for element in expr_elements {
                                            let val_node = self.build_node(
                                                &element.expr,
                                                NodeInfo {
                                                    expected_ty: Some(value_ty),
                                                    parent_struct_flags: actual_struct_flags,
                                                },
                                                ctx,
                                            );
                                            hir_elements.push(ontol_hir::SeqPropertyElement {
                                                iter: element.iter,
                                                attribute: ontol_hir::Attribute {
                                                    rel: rel_node.clone(),
                                                    val: val_node,
                                                },
                                            });
                                        }

                                        let label = *ctx.label_map.get(aggr_expr_id).unwrap();
                                        let seq_ty =
                                            self.types.intern(Type::Seq(rel_params_ty, value_ty));

                                        ontol_hir::PropVariant::Seq(ontol_hir::SeqPropertyVariant {
                                            label: TypedLabel { label, ty: seq_ty },
                                            has_default: ontol_hir::HasDefault(matches!(
                                                match_property.property_id.role,
                                                Role::Object
                                            )),
                                            elements: hir_elements,
                                        })
                                    }
                                    _ => {
                                        self.type_error(
                                            TypeError::VariableMustBeSequenceEnclosed(value_ty),
                                            &value.span,
                                        );
                                        continue;
                                    }
                                },
                            };

                            let optional = ontol_hir::Optional(matches!(
                                match_property.cardinality.0,
                                PropertyCardinality::Optional
                            ));

                            let prop_variants: Vec<ontol_hir::PropVariant<'_, TypedHir>> =
                                match match_property.cardinality.0 {
                                    PropertyCardinality::Mandatory => {
                                        vec![prop_variant]
                                    }
                                    PropertyCardinality::Optional => {
                                        if *bind_option {
                                            vec![prop_variant]
                                        } else {
                                            ctx.partial = true;
                                            self.error(
                                                CompileError::TODO(
                                                    "required to be optional?".into(),
                                                ),
                                                &value.span,
                                            );
                                            vec![]
                                        }
                                    }
                                };

                            hir_props.push(TypedHirNode(
                                ontol_hir::Kind::Prop(
                                    optional,
                                    struct_binder.var,
                                    match_property.property_id,
                                    prop_variants,
                                ),
                                Meta {
                                    ty: self.unit_type(),
                                    span: *prop_span,
                                },
                            ));
                        }

                        if !actual_struct_flags.contains(ontol_hir::StructFlags::MATCH) {
                            for (name, match_property) in match_properties {
                                if match_property.used {
                                    continue;
                                }
                                if matches!(match_property.property_id.role, Role::Object) {
                                    continue;
                                }
                                if matches!(
                                    match_property.cardinality.0,
                                    PropertyCardinality::Optional
                                ) {
                                    continue;
                                }

                                let relationship_id = match_property.property_id.relationship_id;

                                if let Some(const_def_id) = self
                                    .relations
                                    .default_const_objects
                                    .get(&relationship_id)
                                    .cloned()
                                {
                                    // Generate code for default value.
                                    let value_ty = self.check_def_sealed(const_def_id);
                                    hir_props.push(TypedHirNode(
                                        ontol_hir::Kind::Prop(
                                            ontol_hir::Optional(false),
                                            struct_binder.var,
                                            match_property.property_id,
                                            vec![ontol_hir::PropVariant::Singleton(
                                                ontol_hir::Attribute {
                                                    rel: Box::new(self.unit_node_no_span()),
                                                    val: Box::new(TypedHirNode(
                                                        ontol_hir::Kind::Const(const_def_id),
                                                        Meta {
                                                            ty: value_ty,
                                                            span: NO_SPAN,
                                                        },
                                                    )),
                                                },
                                            )],
                                        ),
                                        Meta {
                                            ty: self.unit_type(),
                                            span: NO_SPAN,
                                        },
                                    ));
                                    continue;
                                }

                                if self
                                    .relations
                                    .value_generators
                                    .get(&relationship_id)
                                    .is_some()
                                {
                                    // Value generators should be handled in data storage,
                                    // so leave these fields out when not mentioned.
                                    continue;
                                }

                                ctx.missing_properties
                                    .entry(ctx.arm)
                                    .or_default()
                                    .entry(span)
                                    .or_default()
                                    .push(name.into());
                            }
                        }

                        ontol_hir::Kind::Struct(struct_binder, actual_struct_flags, hir_props)
                    }
                    None => {
                        if !attributes.is_empty() {
                            return self.error_node(CompileError::NoPropertiesExpected, &span);
                        }
                        ontol_hir::Kind::Unit
                    }
                }
            }
            ReprKind::StructIntersection(members) => {
                let mut member_iter = members.iter();
                let single_def_id = match member_iter.next() {
                    Some((def_id, _span)) => {
                        if member_iter.next().is_some() {
                            todo!("More members");
                        }
                        *def_id
                    }
                    None => panic!("No members"),
                };

                let value_object_ty = self.check_def_sealed(single_def_id);
                debug!("value_object_ty: {value_object_ty:?}");

                match value_object_ty {
                    Type::Domain(def_id) => {
                        return self.build_property_matcher(
                            StructInfo {
                                struct_def_id: *def_id,
                                struct_ty: value_object_ty,
                                modifier,
                                parent_struct_flags,
                            },
                            attributes,
                            span,
                            ctx,
                        );
                    }
                    _ => {
                        let mut attributes = attributes.iter();
                        match attributes.next() {
                            Some(ExprStructAttr {
                                key: (def_id, _),
                                rel: _,
                                bind_option: _,
                                value,
                            }) if *def_id == DefId::unit() => {
                                let object_ty = self.check_def_sealed(single_def_id);
                                let inner_node = self.build_node(
                                    value,
                                    NodeInfo {
                                        expected_ty: Some(object_ty),
                                        parent_struct_flags,
                                    },
                                    ctx,
                                );

                                inner_node.into_kind()
                            }
                            _ => {
                                return self
                                    .error_node(CompileError::ExpectedExpressionAttribute, &span);
                            }
                        }
                    }
                }
            }
            ReprKind::Scalar(scalar_def_id, ..) => {
                let scalar_def_id = *scalar_def_id;

                let scalar_object_ty = self.check_def_sealed(scalar_def_id);
                debug!("scalar_object_ty: {struct_def_id:?}: {scalar_object_ty:?}");

                let mut attributes = attributes.iter();
                match attributes.next() {
                    Some(ExprStructAttr {
                        key: (def_id, _),
                        rel: _,
                        bind_option: _,
                        value,
                    }) if *def_id == DefId::unit() => {
                        let object_ty = self.check_def_sealed(scalar_def_id);
                        let inner_node = self.build_node(
                            value,
                            NodeInfo {
                                expected_ty: Some(object_ty),
                                parent_struct_flags,
                            },
                            ctx,
                        );

                        inner_node.into_kind()
                    }
                    _ => {
                        return self.error_node(CompileError::ExpectedExpressionAttribute, &span);
                    }
                }
            }
            ReprKind::Union(_) | ReprKind::StructUnion(_) => {
                return self.error_node(CompileError::CannotMapUnion, &span)
            }
            kind => todo!("{kind:?}"),
        };

        debug!("Struct type: {struct_ty:?}");

        TypedHirNode(
            node_kind,
            Meta {
                ty: struct_ty,
                span,
            },
        )
    }

    fn build_implicit_rel_node(
        &mut self,
        ty: TypeRef<'m>,
        object: &Expr,
        prop_span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        match &object.kind {
            ExprKind::Variable(object_var) => {
                // implicit mapping; for now the object needs to be a variable
                let edge_var = ctx
                    .object_to_edge_var_table
                    .entry(*object_var)
                    .or_insert_with(|| {
                        let edge_var = ctx.var_allocator.alloc();
                        ctx.expr_variables.insert(
                            edge_var,
                            ExpressionVariable {
                                ctrl_group: None,
                                hir_arms: Default::default(),
                            },
                        );
                        edge_var
                    });

                let expr_id = self.expressions.alloc_expr_id();

                self.build_node(
                    &Expr {
                        id: expr_id,
                        kind: ExprKind::Variable(*edge_var),
                        span: prop_span,
                    },
                    NodeInfo {
                        expected_ty: Some(ty),
                        parent_struct_flags: Default::default(),
                    },
                    ctx,
                )
            }
            ExprKind::Seq(_, elements) => {
                // FIXME: Unsure how correct this is:
                for element in elements {
                    let node = self.build_implicit_rel_node(ty, &element.expr, prop_span, ctx);
                    if !matches!(node.1.ty, Type::Error) {
                        return node;
                    }
                }

                self.error_node(CompileError::TODO(smart_format!("")), &prop_span)
            }
            _ => self.error_node(CompileError::TODO(smart_format!("")), &prop_span),
        }
    }

    fn type_error_node(&mut self, error: TypeError<'m>, span: &SourceSpan) -> TypedHirNode<'m> {
        self.type_error(error, span);
        self.make_error_node(span)
    }

    fn error_node(&mut self, error: CompileError, span: &SourceSpan) -> TypedHirNode<'m> {
        self.error(error, span);
        self.make_error_node(span)
    }

    fn make_error_node(&mut self, span: &SourceSpan) -> TypedHirNode<'m> {
        TypedHirNode(
            ontol_hir::Kind::Unit,
            Meta {
                ty: self.types.intern(Type::Error),
                span: *span,
            },
        )
    }

    fn unit_node_no_span(&mut self) -> TypedHirNode<'m> {
        TypedHirNode(
            ontol_hir::Kind::Unit,
            Meta {
                ty: self.unit_type(),
                span: NO_SPAN,
            },
        )
    }

    fn unit_type(&mut self) -> TypeRef<'m> {
        self.types
            .intern(Type::Primitive(PrimitiveKind::Unit, DefId::unit()))
    }
}
