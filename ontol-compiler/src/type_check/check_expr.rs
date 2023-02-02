use std::{collections::HashMap, ops::Deref};

use ontol_runtime::{DefId, PropertyId};
use tracing::warn;

use crate::{
    codegen::typed_expr::{NodeId, TypedExpr, TypedExprKind, TypedExprTable, ERROR_NODE},
    compiler_queries::GetPropertyMeta,
    def::{Def, DefKind},
    error::CompileError,
    expr::{Expr, ExprId, ExprKind},
    mem::Intern,
    relation::SubjectProperties,
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    inference::{Inference, UnifyValue},
    TypeCheck, TypeError,
};

pub struct CheckExprContext<'m> {
    pub inference: Inference<'m>,
    pub typed_expr_table: TypedExprTable<'m>,
    pub variable_nodes: HashMap<ExprId, NodeId>,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(super) fn check_expr_id(
        &mut self,
        expr_id: ExprId,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, NodeId) {
        match self.expressions.get(&expr_id) {
            Some(expr) => self.check_expr(expr, ctx),
            None => panic!("Expression {expr_id:?} not found"),
        }
    }

    fn check_expr(&mut self, expr: &Expr, ctx: &mut CheckExprContext<'m>) -> (TypeRef<'m>, NodeId) {
        match &expr.kind {
            ExprKind::Call(def_id, args) => {
                match (self.defs.map.get(&def_id), self.def_types.map.get(&def_id)) {
                    (
                        Some(Def {
                            kind: DefKind::CoreFn(proc),
                            ..
                        }),
                        Some(Type::Function { params, output }),
                    ) => {
                        if args.len() != params.len() {
                            return self.expr_error(
                                CompileError::IncorrectNumberOfArguments {
                                    expected: u8::try_from(params.len()).unwrap(),
                                    actual: u8::try_from(args.len()).unwrap(),
                                },
                                &expr.span,
                            );
                        }

                        let mut param_nodes = vec![];
                        for (arg, param_ty) in args.iter().zip(*params) {
                            let (_, node_id) = self.check_expr_expect(arg, param_ty, ctx);
                            param_nodes.push(node_id);
                        }

                        let node_id = ctx.typed_expr_table.add_expr(TypedExpr {
                            ty: *output,
                            kind: TypedExprKind::Call(*proc, param_nodes.into()),
                        });

                        (*output, node_id)
                    }
                    _ => self.expr_error(CompileError::NotCallable, &expr.span),
                }
            }
            ExprKind::Obj(type_path, attributes) => {
                let domain_type = self.check_def(type_path.def_id);
                let Type::Domain(_) = domain_type else {
                    return self.expr_error(CompileError::DomainTypeExpected, &type_path.span);
                };

                let subject_properties = self
                    .relations
                    .properties_by_type(type_path.def_id)
                    .map(|props| &props.subject);

                let node_id = match subject_properties {
                    Some(SubjectProperties::Unit) | None => {
                        if !attributes.is_empty() {
                            return self.expr_error(CompileError::NoPropertiesExpected, &expr.span);
                        }
                        ctx.typed_expr_table.add_expr(TypedExpr {
                            ty: domain_type,
                            kind: TypedExprKind::Unit,
                        })
                    }
                    Some(SubjectProperties::Value(property_id, _)) => match attributes.deref() {
                        [((ast_prop, _), value)] if ast_prop.is_none() => {
                            let (_, relationship, _) = self
                                .get_property_meta(*property_id)
                                .expect("BUG: problem getting anonymous property meta");

                            let object_ty = self.check_def(relationship.object);
                            let node_id = self.check_expr_expect(value, object_ty, ctx).1;

                            ctx.typed_expr_table.add_expr(TypedExpr {
                                ty: domain_type,
                                kind: TypedExprKind::ValueObj(node_id),
                            })
                        }
                        _ => {
                            return self
                                .expr_error(CompileError::AnonymousPropertyExpected, &expr.span)
                        }
                    },
                    Some(SubjectProperties::ValueUnion(property_set)) => {
                        todo!()
                    }
                    Some(SubjectProperties::Map(property_set)) => {
                        struct MatchProperty {
                            property_id: PropertyId,
                            object_def: DefId,
                            used: bool,
                        }
                        let mut match_properties = property_set
                            .iter()
                            .map(|property_id| {
                                let (_, relationship, relation) = self
                                    .get_property_meta(*property_id)
                                    .expect("BUG: problem getting property meta");
                                let property_name = relation
                                    .subject_prop()
                                    .expect("BUG: Expected named subject property");

                                (
                                    property_name.clone(),
                                    MatchProperty {
                                        property_id: *property_id,
                                        object_def: relationship.object,
                                        used: false,
                                    },
                                )
                            })
                            .collect::<HashMap<_, _>>();

                        let mut typed_properties = HashMap::new();

                        for ((attr_prop, prop_span), value) in attributes.iter() {
                            let attr_prop = match attr_prop {
                                Some(attr_prop) => attr_prop,
                                None => {
                                    self.error(CompileError::NamedPropertyExpected, &prop_span);
                                    continue;
                                }
                            };
                            let match_property = match match_properties.get_mut(attr_prop.as_str())
                            {
                                Some(match_properties) => match_properties,
                                None => {
                                    self.error(CompileError::UnknownProperty, &prop_span);
                                    continue;
                                }
                            };
                            if match_property.used {
                                self.error(CompileError::DuplicateProperty, &prop_span);
                                continue;
                            }
                            match_property.used = true;

                            let object_ty = self.check_def(match_property.object_def);
                            let (_, node_id) = self.check_expr_expect(value, object_ty, ctx);

                            typed_properties.insert(match_property.property_id, node_id);
                        }

                        for (prop_name, match_property) in match_properties.into_iter() {
                            if !match_property.used {
                                self.error(
                                    CompileError::MissingProperty(prop_name.into()),
                                    &expr.span,
                                );
                            }
                        }

                        ctx.typed_expr_table.add_expr(TypedExpr {
                            ty: domain_type,
                            kind: TypedExprKind::MapObj(typed_properties),
                        })
                    }
                };

                (domain_type, node_id)
            }
            ExprKind::Constant(k) => {
                let ty = self.def_types.map.get(&self.defs.int()).unwrap();
                let node_id = ctx.typed_expr_table.add_expr(TypedExpr {
                    ty,
                    kind: TypedExprKind::Constant(*k),
                });
                (ty, node_id)
            }
            ExprKind::Variable(expr_id) => {
                let ty = self
                    .types
                    .intern(Type::Infer(ctx.inference.new_type_variable(*expr_id)));
                let node_id = ctx
                    .variable_nodes
                    .get(expr_id)
                    .expect("variable node not found");
                (ty, *node_id)
            }
        }
    }

    fn check_expr_expect(
        &mut self,
        expr: &Expr,
        expected: TypeRef<'m>,
        ctx: &mut CheckExprContext<'m>,
    ) -> (TypeRef<'m>, NodeId) {
        let (ty, node_id) = self.check_expr(expr, ctx);
        match (ty, expected) {
            (Type::Error, _) => (ty, ERROR_NODE),
            (_, Type::Error) => (expected, ERROR_NODE),
            (Type::Infer(..), Type::Infer(..)) => {
                panic!("FIXME: equate variable with variable?");
            }
            (Type::Infer(type_var), expected) => {
                if let Err(error) = ctx
                    .inference
                    .eq_relations
                    .unify_var_value(*type_var, UnifyValue::Known(expected))
                {
                    let err = self.type_error(error, &expr.span);
                    (err, ERROR_NODE)
                } else {
                    warn!("TODO: resolve var?");
                    (ty, node_id)
                }
            }
            (ty, expected) if ty != expected => {
                let err = self.type_error(
                    TypeError::Mismatch {
                        actual: ty,
                        expected,
                    },
                    &expr.span,
                );
                (err, ERROR_NODE)
            }
            _ => {
                // Ok
                (ty, node_id)
            }
        }
    }

    fn expr_error(&mut self, error: CompileError, span: &SourceSpan) -> (TypeRef<'m>, NodeId) {
        self.errors.push(error.spanned(&self.sources, span));
        (self.types.intern(Type::Error), ERROR_NODE)
    }
}
