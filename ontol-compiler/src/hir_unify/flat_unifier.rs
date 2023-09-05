#![allow(clippy::only_used_in_recursion)]

use indexmap::IndexMap;
use ontol_runtime::{smart_format, value::PropertyId, DefId};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    hir_unify::{VarSet, CLASSIC_UNIFIER_FALLBACK},
    mem::Intern,
    primitive::PrimitiveKind,
    typed_hir::{self, TypedBinder, TypedHir, TypedHirNode},
    types::{Type, Types},
    NO_SPAN,
};

use super::{
    expr, flat_scope, flat_unifier_table::Table, unifier::UnifiedNode, UnifierError, UnifierResult,
};

pub struct FlatUnifier<'a, 'm> {
    #[allow(unused)]
    pub(super) types: &'a mut Types<'m>,
    pub(super) var_allocator: ontol_hir::VarAllocator,
}

impl<'a, 'm> FlatUnifier<'a, 'm> {
    pub fn new(types: &'a mut Types<'m>, var_allocator: ontol_hir::VarAllocator) -> Self {
        Self {
            types,
            var_allocator,
        }
    }

    pub(super) fn unify(
        &mut self,
        flat_scope: flat_scope::FlatScope<'m>,
        expr: expr::Expr<'m>,
    ) -> UnifierResult<UnifiedNode<'m>> {
        if true {
            debug!("flat_scope:\n{flat_scope}");
        }

        let mut table = Table::new(flat_scope);

        self.assign_to_scope(expr, &mut table)?;

        for scope_map in table.table_mut() {
            debug!("{}", scope_map.scope);
            if !scope_map.exprs.is_empty() {
                let expr_debug: Vec<_> = scope_map
                    .exprs
                    .iter()
                    .map(|expr| expr.kind().debug_short())
                    .collect();
                debug!("    exprs: {expr_debug:?}");
            }
        }

        let mut in_scope = VarSet::default();

        self.unify_single(None, &mut table, &mut in_scope)
    }

    fn assign_to_scope(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
        table: &mut Table<'m>,
    ) -> UnifierResult<()> {
        match kind {
            expr::Kind::Var(var) => {
                if let Some(index) = table.find_var_index(var) {
                    table
                        .scope_map_mut(index)
                        .exprs
                        .push(expr::Expr(kind, meta));
                } else {
                    table.const_expr = Some(expr::Expr(kind, meta));
                }
            }
            expr::Kind::Struct {
                binder,
                flags,
                props,
            } => {
                // FIXME: Insert according to required scope, instead of scope 0.
                table.scope_map_mut(0).exprs.push(expr::Expr(
                    expr::Kind::Struct {
                        binder,
                        flags,
                        props: vec![],
                    },
                    meta.clone(),
                ));

                for prop in props {
                    let free_vars = prop.free_vars.clone();
                    let unit_ty = self
                        .types
                        .intern(Type::Primitive(PrimitiveKind::Unit, DefId::unit()));
                    self.assign_to_scope(
                        expr::Expr(
                            expr::Kind::Prop(Box::new(prop)),
                            expr::Meta {
                                free_vars,
                                hir_meta: typed_hir::Meta {
                                    ty: unit_ty,
                                    span: NO_SPAN,
                                },
                            },
                        ),
                        table,
                    )?;
                }
            }
            expr::Kind::Prop(prop) => {
                let scope_map = table.assign_free_vars(&prop.free_vars)?;
                let expressions = &mut scope_map.exprs;

                let struct_var = prop.struct_var;
                let mut prop = Some(*prop);

                // Check just adding back to the original struct that owned this property:
                for expression in expressions.iter_mut() {
                    if let expr::Kind::Struct { binder, props, .. } = &mut expression.0 {
                        if binder.var == struct_var {
                            props.push(prop.take().unwrap());
                            break;
                        }
                    }
                }

                if let Some(prop) = prop {
                    expressions.push(expr::Expr(expr::Kind::Prop(Box::new(prop)), meta));
                }
            }
            e => return Err(unifier_todo(smart_format!("expr kind: {e:?}"))),
        }

        Ok(())
    }

    fn unify_single(
        &mut self,
        parent_scope_var: Option<ontol_hir::Var>,
        table: &mut Table<'m>,
        in_scope: &mut VarSet,
    ) -> UnifierResult<UnifiedNode<'m>> {
        if let Some(const_expr) = table.const_expr.take() {
            return Ok(UnifiedNode {
                typed_binder: None,
                node: self.leaf_expr_to_node(const_expr)?,
            });
        }

        if let Some(var) = parent_scope_var {
            if in_scope.contains(var) {
                panic!("Variable is already in scope");
            }
        }

        let indexes = table.dependees(parent_scope_var);
        let Some(index) = indexes.into_iter().next() else {
            panic!("multiple indexes");
        };

        let scope_map = &mut table.scope_map_mut(index);
        let scope_meta = scope_map.scope.meta().clone();

        match (scope_map.take_single_expr(), scope_map.scope.kind()) {
            (Some(expr::Expr(kind, meta)), flat_scope::Kind::Var) => {
                let inner_node = self.leaf_expr_to_node(expr::Expr(kind, meta))?;

                Ok(UnifiedNode {
                    typed_binder: Some(TypedBinder {
                        var: scope_map.scope.meta().var,
                        meta: scope_map.scope.meta().hir_meta,
                    }),
                    node: inner_node,
                })
            }
            (
                Some(expr::Expr(
                    expr::Kind::Struct {
                        binder,
                        flags,
                        props: _,
                    },
                    meta,
                )),
                flat_scope::Kind::Struct,
            ) => {
                in_scope.insert(scope_meta.var);
                let body = self.unify_scope_children(scope_meta.var, table, in_scope)?;
                in_scope.remove(scope_meta.var);

                let node = UnifiedNode {
                    typed_binder: Some(TypedBinder {
                        var: scope_meta.var,
                        meta: scope_meta.hir_meta,
                    }),
                    node: TypedHirNode(ontol_hir::Kind::Struct(binder, flags, body), meta.hir_meta),
                };

                Ok(node)
            }
            other => Err(unifier_todo(smart_format!("Handle pair {other:?}"))),
        }
    }

    fn unify_scope_children(
        &mut self,
        parent_scope_var: ontol_hir::Var,
        table: &mut Table<'m>,
        in_scope: &mut VarSet,
    ) -> UnifierResult<Vec<TypedHirNode<'m>>> {
        let indexes = table.dependees(Some(parent_scope_var));

        let mut output = Vec::with_capacity(indexes.len());

        let mut merged_match_arms_table: IndexMap<
            (ontol_hir::Var, PropertyId),
            MergedMatchArms<'m>,
        > = Default::default();

        for index in indexes {
            let scope_map = &mut table.scope_map_mut(index);
            let scope_var = scope_map.scope.meta().var;

            match scope_map.scope.kind() {
                flat_scope::Kind::PropRelParam
                | flat_scope::Kind::PropValue
                | flat_scope::Kind::Struct
                | flat_scope::Kind::Var => {
                    for expression in std::mem::take(&mut scope_map.exprs) {
                        output.push(self.leaf_expr_to_node(expression)?);
                    }

                    let nodes = self.unify_scope_children(scope_var, table, in_scope)?;
                    output.extend(nodes);
                }
                flat_scope::Kind::PropVariant(optional, struct_var, property_id) => {
                    let optional = *optional;
                    let struct_var = *struct_var;
                    let property_id = *property_id;
                    let exprs = std::mem::take(&mut scope_map.exprs);

                    let var_attribute =
                        self.scope_prop_variant_bindings(scope_map.scope.meta().var, table);

                    fn make_binding<'m>(
                        scope_node: Option<&flat_scope::ScopeNode<'m>>,
                    ) -> ontol_hir::Binding<'m, TypedHir> {
                        match scope_node {
                            Some(scope_node) => ontol_hir::Binding::Binder(TypedBinder {
                                var: scope_node.meta().var,
                                meta: scope_node.meta().hir_meta,
                            }),
                            None => ontol_hir::Binding::Wildcard,
                        }
                    }

                    let rel_binding = make_binding(
                        var_attribute
                            .rel
                            .and_then(|var| table.find_scope_var_child(var)),
                    );
                    let val_binding = make_binding(
                        var_attribute
                            .val
                            .and_then(|var| table.find_scope_var_child(var)),
                    );

                    // TODO: Maybe use loop instead of recursion?
                    let mut body = vec![];

                    for expr in exprs {
                        body.push(self.leaf_expr_to_node(expr)?);
                    }
                    body.extend(self.unify_scope_children(scope_var, table, in_scope)?);

                    let merged_match_arms = merged_match_arms_table
                        .entry((struct_var, property_id))
                        .or_default();

                    if optional.0 {
                        merged_match_arms.optional.0 = true;
                    }

                    merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
                        pattern: ontol_hir::PropPattern::Attr(rel_binding, val_binding),
                        nodes: body,
                    });
                }
                other => return Err(unifier_todo(smart_format!("{other:?}"))),
            }
        }

        for ((struct_var, property_id), mut merged_match_arms) in merged_match_arms_table {
            if merged_match_arms.optional.0 {
                merged_match_arms.match_arms.push(ontol_hir::PropMatchArm {
                    pattern: ontol_hir::PropPattern::Absent,
                    nodes: vec![],
                });
            }

            let meta = self.unit_meta();
            output.push(TypedHirNode(
                ontol_hir::Kind::MatchProp(struct_var, property_id, merged_match_arms.match_arms),
                meta,
            ));
        }

        Ok(output)
    }

    fn scope_prop_variant_bindings(
        &mut self,
        variant_var: ontol_hir::Var,
        table: &mut Table<'m>,
    ) -> ontol_hir::Attribute<Option<ontol_hir::Var>> {
        let mut attribute = ontol_hir::Attribute {
            rel: None,
            val: None,
        };

        for index in table.dependees(Some(variant_var)) {
            let scope_map = &table.scope_map_mut(index);
            match scope_map.scope.kind() {
                flat_scope::Kind::PropRelParam => {
                    attribute.rel = Some(scope_map.scope.meta().var);
                }
                flat_scope::Kind::PropValue => {
                    attribute.val = Some(scope_map.scope.meta().var);
                }
                _ => {}
            }
        }

        attribute
    }

    fn leaf_expr_to_node(
        &mut self,
        expr::Expr(kind, meta): expr::Expr<'m>,
    ) -> UnifierResult<TypedHirNode<'m>> {
        match kind {
            expr::Kind::Unit => Ok(TypedHirNode(ontol_hir::Kind::Unit, meta.hir_meta)),
            expr::Kind::Var(var) => Ok(TypedHirNode(ontol_hir::Kind::Var(var), meta.hir_meta)),
            expr::Kind::Prop(prop) => Ok(TypedHirNode(
                ontol_hir::Kind::Prop(
                    ontol_hir::Optional(false),
                    prop.struct_var,
                    prop.prop_id,
                    match prop.variant {
                        expr::PropVariant::Singleton(attr) => {
                            vec![ontol_hir::PropVariant::Singleton(ontol_hir::Attribute {
                                rel: Box::new(self.leaf_expr_to_node(attr.rel)?),
                                val: Box::new(self.leaf_expr_to_node(attr.val)?),
                            })]
                        }
                        expr::PropVariant::Seq { .. } => todo!("seq"),
                    },
                ),
                meta.hir_meta,
            )),
            expr::Kind::Call(expr::Call(proc, args)) => {
                let mut hir_args = Vec::with_capacity(args.len());
                for arg in args {
                    hir_args.push(self.leaf_expr_to_node(arg)?);
                }
                Ok(TypedHirNode(
                    ontol_hir::Kind::Call(proc, hir_args),
                    meta.hir_meta,
                ))
            }
            expr::Kind::Map(arg) => {
                let hir_arg = self.leaf_expr_to_node(*arg)?;
                Ok(TypedHirNode(
                    ontol_hir::Kind::Map(Box::new(hir_arg)),
                    meta.hir_meta,
                ))
            }
            other => Err(unifier_todo(smart_format!("leaf expr to node: {other:?}"))),
        }
    }

    pub(super) fn unit_meta(&mut self) -> typed_hir::Meta<'m> {
        typed_hir::Meta {
            ty: self.types.unit_type(),
            span: NO_SPAN,
        }
    }
}

#[derive(Default)]
struct MergedMatchArms<'m> {
    optional: ontol_hir::Optional,
    match_arms: Vec<ontol_hir::PropMatchArm<'m, TypedHir>>,
}

pub(super) fn unifier_todo(msg: String) -> UnifierError {
    if !CLASSIC_UNIFIER_FALLBACK {
        todo!("{msg}");
    }
    UnifierError::TODO(msg)
}
