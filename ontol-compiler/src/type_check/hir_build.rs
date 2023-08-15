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
    def::{Def, DefKind, LookupRelationshipMeta, MapDirection, RelParams},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, ExprStructAttr},
    mem::Intern,
    primitive::PrimitiveKind,
    type_check::{
        hir_build_ctx::{Arm, ExplicitVariableArm, ExpressionVariable},
        inference::UnifyValue,
        repr::repr_model::ReprKind,
    },
    typed_hir::{Meta, TypedBinder, TypedHir, TypedHirNode, TypedLabel},
    types::{Type, TypeRef},
    SourceSpan, NO_SPAN,
};

use super::{hir_build_ctx::HirBuildCtx, TypeCheck, TypeEquation, TypeError};

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
            &expr, // Don't pass inference types as the expected type:
            None, ctx,
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
        expected_ty: Option<TypeRef<'m>>,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        let node = match (&expr.kind, expected_ty) {
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
                            let node = self.build_node(arg, Some(param_ty), ctx);
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
                    type_path.def_id,
                    struct_ty,
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

                self.build_property_matcher(*def_id, actual_ty, attributes, expr.span, ctx)
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

                let inner_node = self.build_node(inner, Some(val_ty), ctx);
                let label = *ctx.label_map.get(aggr_expr_id).unwrap();
                let seq_ty = self.types.intern(Type::Seq(rel_ty, val_ty));

                TypedHirNode(
                    ontol_hir::Kind::Seq(
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
            (ExprKind::Variable(expr_id), expected_ty) => {
                let arm = ctx.arm;
                let explicit_variable = ctx
                    .expr_variables
                    .get_mut(expr_id)
                    .expect("variable not found");

                let arm_expr_id = {
                    let hir_arm = explicit_variable.hir_arms.entry(arm).or_insert_with(|| {
                        let expr_id = match arm {
                            Arm::First => *expr_id,
                            Arm::Second => self.expressions.alloc_expr_id(),
                        };

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
                        let variable = explicit_variable.variable;
                        let variable_ref = TypedHirNode(
                            ontol_hir::Kind::Var(variable),
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
            (kind, ty) => self.error_node(
                CompileError::TODO(smart_format!(
                    "Not enough type information for {kind:?}, expected_ty = {ty:?}"
                )),
                &expr.span,
            ),
        };

        debug!("expected/meta: {:?} {:?}", expected_ty, node.ty());

        match (expected_ty, node.ty()) {
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
        struct_def_id: DefId,
        struct_ty: TypeRef<'m>,
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
                                let (_, _, owner_cardinality) =
                                    meta.relationship.left_side(property_id.role);
                                let (value_def_ref, _, _) =
                                    meta.relationship.right_side(property_id.role);

                                property_name.map(|property_name| {
                                    (
                                        property_name,
                                        MatchProperty {
                                            property_id: *property_id,
                                            cardinality: owner_cardinality,
                                            rel_params_def: match &meta.relationship.rel_params {
                                                RelParams::Type(def_reference) => {
                                                    Some(def_reference.def_id)
                                                }
                                                _ => None,
                                            },
                                            value_def: value_def_ref.def_id,
                                            used: false,
                                        },
                                    )
                                })
                            })
                            .collect::<IndexMap<_, _>>();

                        let mut hir_props = vec![];

                        for ExprStructAttr {
                            key: (def, prop_span),
                            rel,
                            bind_option,
                            value,
                        } in attributes
                        {
                            let attr_prop = match self.defs.def_kind(def.def_id) {
                                DefKind::StringLiteral(lit) => lit,
                                _ => {
                                    self.error(CompileError::NamedPropertyExpected, prop_span);
                                    continue;
                                }
                            };
                            let match_property = match match_properties.get_mut(attr_prop) {
                                Some(match_properties) => match_properties,
                                None => {
                                    self.error(CompileError::UnknownProperty, prop_span);
                                    continue;
                                }
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
                                (_, Some(rel)) => self.build_node(rel, Some(rel_params_ty), ctx),
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
                                    let val_node = self.build_node(value, Some(value_ty), ctx);
                                    ontol_hir::PropVariant {
                                        dimension: ontol_hir::AttrDimension::Singular,
                                        attr: ontol_hir::Attribute {
                                            rel: Box::new(rel_node),
                                            val: Box::new(val_node),
                                        },
                                    }
                                }
                                ValueCardinality::Many => match &value.kind {
                                    ExprKind::Seq(aggr_expr_id, value) => {
                                        let val_node = self.build_node(value, Some(value_ty), ctx);
                                        let label = *ctx.label_map.get(aggr_expr_id).unwrap();

                                        let seq_ty =
                                            self.types.intern(Type::Seq(rel_params_ty, value_ty));

                                        ontol_hir::PropVariant {
                                            dimension: ontol_hir::AttrDimension::Seq(
                                                TypedLabel { label, ty: seq_ty },
                                                ontol_hir::HasDefault(matches!(
                                                    match_property.property_id.role,
                                                    Role::Object
                                                )),
                                            ),
                                            attr: ontol_hir::Attribute {
                                                rel: Box::new(rel_node),
                                                val: Box::new(val_node),
                                            },
                                        }
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
                                    ty: self.types.intern(Type::Primitive(
                                        PrimitiveKind::Unit,
                                        DefId::unit(),
                                    )),
                                    span: *prop_span,
                                },
                            ));
                        }

                        match (ctx.arm, ctx.direction) {
                            (Arm::First, MapDirection::Forwards) => {
                                // It's OK to not specify all properties here
                            }
                            _ => {
                                for (name, match_property) in match_properties {
                                    if !match_property.used
                                        && !matches!(match_property.property_id.role, Role::Object)
                                    {
                                        ctx.missing_properties
                                            .entry(ctx.arm)
                                            .or_default()
                                            .entry(span)
                                            .or_default()
                                            .push(name.into());
                                    }
                                }
                            }
                        }

                        ontol_hir::Kind::Struct(struct_binder, hir_props)
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
                            *def_id,
                            value_object_ty,
                            attributes,
                            span,
                            ctx,
                        );
                    }
                    _ => {
                        let mut attributes = attributes.iter();
                        match attributes.next() {
                            Some(ExprStructAttr {
                                key: (def, _),
                                rel: _,
                                bind_option: _,
                                value,
                            }) if def.def_id == DefId::unit() => {
                                let object_ty = self.check_def_sealed(single_def_id);
                                let inner_node = self.build_node(value, Some(object_ty), ctx);

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
                        key: (def, _),
                        rel: _,
                        bind_option: _,
                        value,
                    }) if def.def_id == DefId::unit() => {
                        let object_ty = self.check_def_sealed(scalar_def_id);
                        let inner_node = self.build_node(value, Some(object_ty), ctx);

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
            ExprKind::Variable(object_expr_id) => {
                // implicit mapping; for now the object needs to be a variable
                let edge_expr_id = ctx
                    .object_to_edge_expr_id
                    .entry(*object_expr_id)
                    .or_insert_with(|| {
                        let edge_expr_id = self.expressions.alloc_expr_id();
                        ctx.expr_variables.insert(
                            edge_expr_id,
                            ExpressionVariable {
                                variable: ctx.var_allocator.alloc(),
                                ctrl_group: None,
                                hir_arms: Default::default(),
                            },
                        );
                        edge_expr_id
                    });

                self.build_node(
                    &Expr {
                        id: *edge_expr_id,
                        kind: ExprKind::Variable(*edge_expr_id),
                        span: prop_span,
                    },
                    Some(ty),
                    ctx,
                )
            }
            ExprKind::Seq(_, expr) => self.build_implicit_rel_node(ty, expr, prop_span, ctx),
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
