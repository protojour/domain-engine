use indexmap::IndexMap;
use ontol_runtime::{
    smart_format, value::PropertyId, vm::proc::BuiltinProc, DefId, RelationId, Role,
};
use ontos::{kind::NodeKind, Binder, Variable};
use tracing::{debug, error};

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, DefKind, PropertyCardinality, ValueCardinality},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, ExprStructAttr, TypePath},
    mem::Intern,
    relation::Constructor,
    type_check::inference::UnifyValue,
    typed_ontos::lang::{Meta, OntosNode},
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{unify_ctx::UnifyExprContext, TypeCheck, TypeEquation, TypeError};

const REPORT_ERRORS: bool = false;

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn check_root_expr2(
        &mut self,
        expr_id: ExprId,
        ctx: &mut UnifyExprContext<'m>,
    ) -> OntosNode<'m> {
        // temporarily remove from map
        let expr = self.expressions.map.remove(&expr_id).unwrap();

        let node = self.check_expr2(
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

        self.expressions.map.insert(expr_id, expr);

        node
    }

    fn check_expr2(
        &mut self,
        expr: &Expr,
        expected_ty: Option<TypeRef<'m>>,
        ctx: &mut UnifyExprContext<'m>,
    ) -> OntosNode<'m> {
        match (&expr.kind, expected_ty) {
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
                            let node = self.check_expr2(arg, Some(param_ty), ctx);
                            parameters.push(node);
                        }

                        OntosNode {
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
                let struct_node = self.check_struct2(type_path, attributes, expr.span, ctx);
                match expected_ty {
                    Some(Type::Domain(_)) => struct_node,
                    Some(Type::Option(Type::Domain(_))) => OntosNode {
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
            (ExprKind::Seq(_aggr_expr_id, _inner), _expected_ty) => {
                debug!("TODO: Ontos sequence mapping");
                self.make_error_node(&expr.span)
            }
            (ExprKind::Constant(k), Some(expected_ty)) => {
                if matches!(expected_ty, Type::Int(_)) {
                    OntosNode {
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
                let type_var = ctx.inference.new_type_variable(*expr_id);
                let bound_variable = ctx
                    .bound_variables
                    .get(expr_id)
                    .expect("variable not found");

                match expected_ty {
                    Some(Type::Array(elem_ty)) => OntosNode {
                        kind: NodeKind::Unit,
                        meta: Meta {
                            ty: self.report_type_error(
                                TypeError::VariableMustBeSequenceEnclosed(elem_ty),
                                &expr.span,
                            ),
                            span: expr.span,
                        },
                    },
                    Some(expected_ty) => {
                        let variable_ref = OntosNode {
                            kind: NodeKind::VariableRef(ontos::Variable(bound_variable.node_id.0)),
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
                                    (Type::Domain(_), Type::Domain(_)) => OntosNode {
                                        // FIXME: Properly represent map call
                                        kind: NodeKind::Call(
                                            BuiltinProc::Append,
                                            [variable_ref].into(),
                                        ),
                                        meta: Meta {
                                            ty: expected_ty,
                                            span: expr.span,
                                        },
                                    },

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
        }
    }

    fn check_struct2(
        &mut self,
        type_path: &TypePath,
        attributes: &[ExprStructAttr],
        span: SourceSpan,
        ctx: &mut UnifyExprContext<'m>,
    ) -> OntosNode<'m> {
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

                        let mut typed_properties = IndexMap::new();

                        for ExprStructAttr {
                            key: (def, prop_span),
                            bind_option,
                            expr,
                        } in attributes
                        {
                            let attr_prop = match self.defs.get_def_kind(def.def_id) {
                                Some(DefKind::StringLiteral(lit)) => lit,
                                _ => {
                                    self.report_error(
                                        CompileError::NamedPropertyExpected,
                                        prop_span,
                                    );
                                    continue;
                                }
                            };
                            let match_property = match match_properties.get_mut(attr_prop) {
                                Some(match_properties) => match_properties,
                                None => {
                                    self.report_error(CompileError::UnknownProperty, prop_span);
                                    continue;
                                }
                            };
                            if match_property.used {
                                self.report_error(CompileError::DuplicateProperty, prop_span);
                                continue;
                            }
                            match_property.used = true;

                            let object_ty = self.check_def(match_property.object_def);
                            debug!("object_ty: {object_ty:?}");

                            let object_ty = match match_property.cardinality.1 {
                                ValueCardinality::One => object_ty,
                                ValueCardinality::Many => self.types.intern(Type::Array(object_ty)),
                            };
                            let node = match match_property.cardinality.0 {
                                PropertyCardinality::Mandatory => {
                                    self.check_expr2(expr, Some(object_ty), ctx)
                                }
                                PropertyCardinality::Optional => {
                                    let object_ty = self.types.intern(Type::Option(object_ty));

                                    if *bind_option {
                                    } else {
                                        ctx.partial = true;
                                        panic!("partial unification");
                                    }

                                    self.check_expr2(expr, Some(object_ty), ctx)
                                }
                            };

                            typed_properties
                                .insert(PropertyId::subject(match_property.relation_id), node);
                        }

                        for (prop_name, match_property) in match_properties.into_iter() {
                            if !match_property.used {
                                self.report_error(
                                    CompileError::MissingProperty(prop_name.into()),
                                    &span,
                                );
                            }
                        }

                        NodeKind::Struct(Binder(Variable(0)), [].into())
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
                        let inner_node = self.check_expr2(value, Some(object_ty), ctx);

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

        OntosNode {
            kind: node_kind,
            meta: Meta {
                ty: domain_type,
                span,
            },
        }
    }

    // Note: These should be replaced with versions that report errors to the user

    fn type_error_node(&mut self, error: TypeError<'m>, span: &SourceSpan) -> OntosNode<'m> {
        self.report_type_error(error, span);
        self.make_error_node(span)
    }

    fn error_node(&mut self, error: CompileError, span: &SourceSpan) -> OntosNode<'m> {
        self.report_error(error, span);
        self.make_error_node(span)
    }

    fn report_type_error(&mut self, error: TypeError<'m>, span: &SourceSpan) -> TypeRef<'m> {
        if REPORT_ERRORS {
            self.type_error(error, span);
        } else {
            error!("Type Error (noted): {error:?}");
        }
        self.types.intern(Type::Error)
    }

    fn report_error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        if REPORT_ERRORS {
            self.error(error, span);
        } else {
            error!("expr2 error: {error:?}");
        }
        self.types.intern(Type::Error)
    }

    fn make_error_node(&mut self, span: &SourceSpan) -> OntosNode<'m> {
        OntosNode {
            kind: NodeKind::Unit,
            meta: Meta {
                ty: self.types.intern(Type::Error),
                span: *span,
            },
        }
    }
}
