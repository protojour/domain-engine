use indexmap::IndexMap;
use ontol_hir::{
    kind::{Attribute, Dimension, NodeKind, Optional, PropVariant},
    Binder,
};
use ontol_runtime::{smart_format, value::PropertyId, DefId, RelationId, Role};
use tracing::debug;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, DefKind, PropertyCardinality, ValueCardinality},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, ExprStructAttr, TypePath},
    mem::Intern,
    relation::Constructor,
    type_check::{
        hir_build_ctx::{Arm, ExplicitVariableArm},
        inference::UnifyValue,
    },
    typed_hir::{Meta, TypedHir, TypedHirNode},
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{hir_build_ctx::HirBuildCtx, TypeCheck, TypeEquation, TypeError};

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn build_root_expr(
        &mut self,
        expr_id: ExprId,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        let expr = self.expressions.map.remove(&expr_id).unwrap();

        let node = self.build_node(
            &expr, // Don't pass inference types as the expected type:
            None, ctx,
        );

        // Save typing result for the final type unification:
        match node.meta.ty {
            Type::Error | Type::Infer(_) => {}
            _ => {
                let type_var = ctx.inference.new_type_variable(expr_id);
                debug!("Check expr(2) root type result: {:?}", node.meta.ty);
                ctx.inference
                    .eq_relations
                    .unify_var_value(type_var, UnifyValue::Known(node.meta.ty))
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
                match (self.defs.map.get(def_id), self.def_types.map.get(def_id)) {
                    (
                        Some(Def {
                            kind: DefKind::CoreFn(proc),
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

                        TypedHirNode {
                            kind: NodeKind::Call(*proc, parameters),
                            meta: Meta {
                                ty: output,
                                span: expr.span,
                            },
                        }
                    }
                    _ => self.error_node(CompileError::NotCallable, &expr.span),
                }
            }
            (ExprKind::Struct(type_path, attributes), expected_ty) => {
                let struct_node = self.build_struct(type_path, attributes, expr.span, ctx);
                match expected_ty {
                    Some(Type::Infer(_)) => struct_node,
                    Some(Type::Domain(_)) => struct_node,
                    Some(Type::Option(Type::Domain(_))) => TypedHirNode {
                        kind: struct_node.kind,
                        meta: Meta {
                            ty: self.types.intern(Type::Option(struct_node.meta.ty)),
                            span: struct_node.meta.span,
                        },
                    },
                    Some(expected_ty) => self.type_error_node(
                        TypeError::Mismatch(TypeEquation {
                            actual: struct_node.meta.ty,
                            expected: expected_ty,
                        }),
                        &expr.span,
                    ),
                    _ => struct_node,
                }
            }
            (ExprKind::Seq(aggr_expr_id, inner), expected_ty) => {
                let elem_ty = match expected_ty {
                    Some(Type::Array(elem_ty)) => elem_ty,
                    Some(other_ty) => {
                        self.type_error(TypeError::MustBeSequence(other_ty), &expr.span);
                        self.types.intern(Type::Error)
                    }
                    None => {
                        let expr_id = self.expressions.alloc_expr_id();

                        let ty = self
                            .types
                            .intern(Type::Infer(ctx.inference.new_type_variable(expr_id)));

                        debug!("Infer seq type: {ty:?}");
                        ty
                    }
                };

                let inner_node = self.build_node(inner, Some(elem_ty), ctx);
                let label = *ctx.label_map.get(aggr_expr_id).unwrap();
                let array_ty = self.types.intern(Type::Array(elem_ty));

                TypedHirNode {
                    kind: NodeKind::Seq(
                        label,
                        Attribute {
                            rel: Box::new(self.unit_node_no_span()),
                            val: Box::new(inner_node),
                        },
                    ),
                    meta: Meta {
                        ty: array_ty,
                        span: expr.span,
                    },
                }
            }
            (ExprKind::Constant(k), Some(expected_ty)) => {
                if matches!(expected_ty, Type::Int(_)) {
                    TypedHirNode {
                        kind: NodeKind::Int(*k),
                        meta: Meta {
                            ty: expected_ty,
                            span: expr.span,
                        },
                    }
                } else {
                    self.error_node(
                        CompileError::TODO(smart_format!("Expected integer type")),
                        &expr.span,
                    )
                }
            }
            (ExprKind::Variable(expr_id), expected_ty) => {
                let arm = ctx.arm;
                let explicit_variable = ctx
                    .explicit_variables
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
                    Some(Type::Array(elem_ty)) => self.type_error_node(
                        TypeError::VariableMustBeSequenceEnclosed(elem_ty),
                        &expr.span,
                    ),
                    Some(expected_ty) => {
                        let variable = explicit_variable.variable;
                        let variable_ref = TypedHirNode {
                            kind: NodeKind::Var(variable),
                            meta: Meta {
                                ty: expected_ty,
                                span: expr.span,
                            },
                        };

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

        debug!("expected/meta: {:?} {:?}", expected_ty, node.meta.ty);

        match (expected_ty, node.meta.ty) {
            (_, Type::Error | Type::Infer(_)) => {}
            (Some(Type::Infer(type_var)), _) => {
                ctx.inference
                    .eq_relations
                    .unify_var_value(*type_var, UnifyValue::Known(node.meta.ty))
                    .unwrap();
            }
            _ => {}
        }

        node
    }

    fn build_struct(
        &mut self,
        type_path: &TypePath,
        attributes: &[ExprStructAttr],
        span: SourceSpan,
        ctx: &mut HirBuildCtx<'m>,
    ) -> TypedHirNode<'m> {
        let domain_type = self.check_def(type_path.def_id);
        let subject_id = match domain_type {
            Type::Domain(subject_id) => subject_id,
            _ => return self.error_node(CompileError::DomainTypeExpected, &type_path.span),
        };

        let properties = self.relations.properties_by_type(type_path.def_id);

        let node_kind = match properties.map(|props| &props.constructor) {
            Some(Constructor::Struct) | None => {
                match properties.and_then(|props| props.map.as_ref()) {
                    Some(property_set) => {
                        let struct_binder = Binder(ctx.alloc_variable());

                        struct MatchProperty {
                            relation_id: RelationId,
                            cardinality: Cardinality,
                            object_def: DefId,
                            used: bool,
                        }
                        let mut match_properties = property_set
                            .iter()
                            .filter_map(|(property_id, _cardinality)| match property_id.role {
                                Role::Subject => {
                                    let meta = self
                                        .property_meta_by_subject(
                                            *subject_id,
                                            property_id.relation_id,
                                        )
                                        .expect("BUG: problem getting property meta");
                                    let property_name = meta
                                        .relation
                                        .subject_prop(self.defs)
                                        .expect("BUG: Expected named subject property");

                                    Some((
                                        property_name,
                                        MatchProperty {
                                            relation_id: property_id.relation_id,
                                            cardinality: meta.relationship.subject_cardinality,
                                            object_def: meta.relationship.object.0.def_id,
                                            used: false,
                                        },
                                    ))
                                }
                                Role::Object => None,
                            })
                            .collect::<IndexMap<_, _>>();

                        let mut hir_props = vec![];

                        for ExprStructAttr {
                            key: (def, prop_span),
                            bind_option,
                            expr,
                        } in attributes
                        {
                            let attr_prop = match self.defs.get_def_kind(def.def_id) {
                                Some(DefKind::StringLiteral(lit)) => lit,
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

                            let object_ty = self.check_def(match_property.object_def);
                            debug!("object_ty: {object_ty:?}");

                            let prop_variant = match match_property.cardinality.1 {
                                ValueCardinality::One => {
                                    let node = self.build_node(expr, Some(object_ty), ctx);
                                    PropVariant {
                                        dimension: Dimension::Singular,
                                        attr: Attribute {
                                            rel: Box::new(self.unit_node_no_span()),
                                            val: Box::new(node),
                                        },
                                    }
                                }
                                ValueCardinality::Many => match &expr.kind {
                                    ExprKind::Seq(aggr_expr_id, inner) => {
                                        let node = self.build_node(inner, Some(object_ty), ctx);
                                        let label = *ctx.label_map.get(aggr_expr_id).unwrap();

                                        PropVariant {
                                            dimension: Dimension::Seq(label),
                                            attr: Attribute {
                                                rel: Box::new(self.unit_node_no_span()),
                                                val: Box::new(node),
                                            },
                                        }
                                    }
                                    _ => {
                                        self.type_error(
                                            TypeError::VariableMustBeSequenceEnclosed(object_ty),
                                            &expr.span,
                                        );
                                        continue;
                                    }
                                },
                            };

                            let optional = Optional(matches!(
                                match_property.cardinality.0,
                                PropertyCardinality::Optional
                            ));

                            let prop_variants: Vec<PropVariant<'_, TypedHir>> = match match_property
                                .cardinality
                                .0
                            {
                                PropertyCardinality::Mandatory => {
                                    vec![prop_variant]
                                }
                                PropertyCardinality::Optional => {
                                    if *bind_option {
                                        vec![prop_variant]
                                    } else {
                                        ctx.partial = true;
                                        self.error(
                                            CompileError::TODO("required to be optional?".into()),
                                            &expr.span,
                                        );
                                        vec![]
                                    }
                                }
                            };

                            hir_props.push(TypedHirNode {
                                kind: NodeKind::Prop(
                                    optional,
                                    struct_binder.0,
                                    PropertyId::subject(match_property.relation_id),
                                    prop_variants,
                                ),
                                meta: Meta {
                                    ty: self.types.intern(Type::Unit(DefId::unit())),
                                    span: *prop_span,
                                },
                            });
                        }

                        for (prop_name, match_property) in match_properties.into_iter() {
                            if !match_property.used {
                                self.error(CompileError::MissingProperty(prop_name.into()), &span);
                            }
                        }

                        NodeKind::Struct(struct_binder, hir_props)
                    }
                    None => {
                        if !attributes.is_empty() {
                            return self.error_node(CompileError::NoPropertiesExpected, &span);
                        }
                        NodeKind::Unit
                    }
                }
            }
            Some(Constructor::Value(relationship_id, _, _)) => {
                let mut attributes = attributes.iter();
                match attributes.next() {
                    Some(ExprStructAttr {
                        key: (def, _),
                        bind_option: _,
                        expr: value,
                    }) if def.def_id == DefId::unit() => {
                        let meta = self
                            .get_relationship_meta(*relationship_id)
                            .expect("BUG: problem getting anonymous property meta");

                        let object_ty = self.check_def(meta.relationship.object.0.def_id);
                        let inner_node = self.build_node(value, Some(object_ty), ctx);

                        inner_node.kind
                    }
                    _ => return self.error_node(CompileError::AnonymousPropertyExpected, &span),
                }
            }
            Some(Constructor::Intersection(_)) => {
                todo!()
            }
            Some(Constructor::Union(_property_set)) => {
                return self.error_node(CompileError::CannotMapUnion, &span)
            }
            Some(Constructor::Sequence(_)) => todo!(),
            Some(Constructor::StringFmt(_)) => todo!(),
        };

        debug!("Struct type: {domain_type:?}");

        TypedHirNode {
            kind: node_kind,
            meta: Meta {
                ty: domain_type,
                span,
            },
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
        TypedHirNode {
            kind: NodeKind::Unit,
            meta: Meta {
                ty: self.types.intern(Type::Error),
                span: *span,
            },
        }
    }

    fn unit_node_no_span(&mut self) -> TypedHirNode<'m> {
        TypedHirNode {
            kind: NodeKind::Unit,
            meta: Meta {
                ty: self.types.intern(Type::Unit(DefId::unit())),
                span: SourceSpan::none(),
            },
        }
    }
}
