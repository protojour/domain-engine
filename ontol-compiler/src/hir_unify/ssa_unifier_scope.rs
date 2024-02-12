use std::collections::BTreeMap;

use chumsky::chain::Chain;
use ontol_hir::{Binder, Binding, Kind, Label, Node, Nodes, PropVariant, SetEntry};
use ontol_runtime::{
    smart_format,
    value::Attribute,
    var::{Var, VarSet},
};
use smallvec::smallvec;
use thin_vec::thin_vec;
use tracing::trace;

use crate::{
    hir_unify::{ssa_util::scan_all_vars_and_labels, UnifierError},
    mem::Intern,
    primitive::PrimitiveKind,
    typed_hir::{Meta, TypedHir, TypedHirData},
    types::{Type, UNIT_TYPE},
    CompileError, SourceSpan, NO_SPAN,
};

use super::{
    ssa_scope_graph::{ComplexExpr, Let, SpannedLet},
    ssa_unifier::SsaUnifier,
    ssa_util::{Catcher, ExtendedScope, Scoped},
    UnifierResult,
};

impl<'c, 'm> SsaUnifier<'c, 'm> {
    pub(super) fn define_scope_extended(
        &mut self,
        scope_node: ExtendedScope<'m>,
        scoped: Scoped,
        free_vars: &VarSet,
        body: &mut Nodes,
        catcher: &mut Catcher,
    ) -> UnifierResult<Binding<'m, TypedHir>> {
        match scope_node {
            ExtendedScope::Wildcard => Ok(Binding::Wildcard),
            ExtendedScope::Node(node) => self.define_scope(node, scoped, body),
            ExtendedScope::SeqUnpack(bindings, source_meta) => {
                let var = self.var_allocator.alloc();

                let catch_label = catcher.make_catch_label(&mut self.var_allocator);

                let var_node = self.mk_node(Kind::Var(var), source_meta);

                let mut new_bindings = thin_vec![];
                for binding in bindings {
                    match binding {
                        Binding::Wildcard => {
                            new_bindings.push(Binding::Wildcard);
                        }
                        Binding::Binder(binder) => {
                            if free_vars.contains(binder.hir().var) {
                                self.scope_tracker.in_scope.insert(binder.hir().var);
                                new_bindings.push(Binding::Binder(binder))
                            } else {
                                new_bindings.push(Binding::Wildcard);
                            }
                        }
                    }
                }

                body.push(self.mk_node(
                    Kind::TryLetTup(catch_label, new_bindings, var_node),
                    Meta::unit(NO_SPAN),
                ));

                Ok(Binding::Binder(TypedHirData(
                    Binder { var },
                    Meta::unit(NO_SPAN),
                )))
            }
        }
    }

    pub(super) fn define_scope(
        &mut self,
        scope_node: Node,
        scoped: Scoped,
        body: &mut Nodes,
    ) -> UnifierResult<Binding<'m, TypedHir>> {
        let mut scoped_lets = vec![];
        let binding = self.traverse(scope_node, scoped, &mut scoped_lets)?;

        let scoped_lets = self.process_let_graph(scoped_lets)?;

        for (let_node, span) in scoped_lets {
            let kind = match let_node {
                Let::Prop(attr, var_prop) => Kind::LetProp(attr, var_prop),
                Let::PropDefault(attr, var_prop, default) => {
                    Kind::LetPropDefault(attr, var_prop, default)
                }
                Let::Regex(groups_list, def, var) => Kind::LetRegex(groups_list, def, var),
                Let::RegexIter(binder, groups_list, def, var) => {
                    Kind::LetRegexIter(binder, groups_list, def, var)
                }
                Let::Expr(binder, node) => Kind::Let(binder, node),
                Let::Complex(ComplexExpr { .. }) => {
                    continue;
                }
            };
            body.push(self.mk_node(kind, Meta::new(&UNIT_TYPE, span)));
        }

        Ok(binding)
    }

    pub(super) fn new_loop_scope<T>(
        &mut self,
        span: SourceSpan,
        func: impl FnOnce(
            &mut SsaUnifier<'c, 'm>,
            &mut ontol_hir::Nodes,
            &mut Catcher,
        ) -> UnifierResult<(T, ontol_hir::Nodes)>,
    ) -> UnifierResult<(T, ontol_hir::Nodes)> {
        let mut scope_body = smallvec![];
        let mut catch_helper = Catcher::default();

        let scope_backup = self.scope_tracker.clone();

        let (ret, body) = func(self, &mut scope_body, &mut catch_helper)?;

        let body = if let Some(catch_label) = catch_helper.finish() {
            let catch_block = self.prealloc_node();

            scope_body.extend(body);

            smallvec![self.write_node(
                catch_block,
                ontol_hir::Kind::Catch(catch_label, scope_body),
                Meta::new(&UNIT_TYPE, span),
            )]
        } else {
            scope_body.extend(body);
            scope_body
        };

        self.scope_tracker = scope_backup;

        Ok((ret, body))
    }

    pub(super) fn maybe_apply_catch_block(
        &mut self,
        mut free_vars: VarSet,
        span: SourceSpan,
        body_func: &dyn Fn(&mut SsaUnifier<'c, 'm>) -> UnifierResult<ontol_hir::Nodes>,
    ) -> UnifierResult<ontol_hir::Nodes> {
        // disregard variables that are never in scope,
        // to not confuse error reporting
        free_vars
            .0
            .intersect_with(&self.all_scope_vars_and_labels.0);

        trace!("catch block: free vars: {free_vars:?}");
        trace!("catch block: in scope: {:?}", self.scope_tracker.in_scope);
        trace!(
            "catch block: total scope: {:?}",
            self.all_scope_vars_and_labels
        );

        let unscoped = VarSet(
            free_vars
                .0
                .difference(&self.scope_tracker.in_scope.0)
                .collect(),
        );

        if unscoped.0.is_empty() {
            return body_func(self);
        }

        let scope_backup = self.scope_tracker.clone();

        let catch_block = self.prealloc_node();
        let catch_label = ontol_hir::Label(self.var_allocator.alloc().0);
        let mut catch_body = smallvec![];

        trace!("apply catch block: {unscoped:?}");

        for var in unscoped.iter() {
            self.make_try_let(
                var,
                catch_label,
                span,
                &scope_backup.potential_lets,
                &mut catch_body,
            )?;
        }

        catch_body.extend(body_func(self)?);

        self.scope_tracker = scope_backup;

        Ok(smallvec![self.write_node(
            catch_block,
            ontol_hir::Kind::Catch(catch_label, catch_body),
            Meta::new(&UNIT_TYPE, span),
        )])
    }

    fn traverse(
        &mut self,
        scope_node: ontol_hir::Node,
        scoped: Scoped,
        lets: &mut Vec<SpannedLet<'m>>,
    ) -> UnifierResult<ontol_hir::Binding<'m, TypedHir>> {
        let node_ref = self.scope_arena.node_ref(scope_node);

        match node_ref.hir() {
            Kind::NoOp => Ok(Binding::Wildcard),
            Kind::Var(var) => {
                if matches!(scoped, Scoped::Yes) {
                    self.scope_tracker.in_scope.insert(*var);
                }
                Ok(Binding::Binder(TypedHirData(
                    ontol_hir::Binder::from(*var),
                    *node_ref.meta(),
                )))
            }
            Kind::Block(block_body) | Kind::Catch(_, block_body) => {
                if let Some(last) = block_body.last() {
                    self.traverse(*last, scoped, lets)
                } else {
                    Ok(Binding::Wildcard)
                }
            }
            Kind::Unit => Ok(Binding::Wildcard),
            Kind::I64(_) => Ok(Binding::Wildcard),
            Kind::F64(_) => Ok(Binding::Wildcard),
            Kind::Text(_) => Ok(Binding::Wildcard),
            Kind::Const(_) => Ok(Binding::Wildcard),
            Kind::Call(_, nodes) => {
                let var = self.var_allocator.alloc();
                let free_vars = scan_all_vars_and_labels(self.scope_arena, nodes.iter().cloned());
                self.push_let(
                    Let::Complex(ComplexExpr {
                        dependency: TypedHirData(Binder { var }, *node_ref.meta()),
                        produces: free_vars,
                        scope_node,
                    }),
                    node_ref.span(),
                    scoped,
                    lets,
                );
                Ok(Binding::Binder(TypedHirData(
                    Binder { var },
                    *node_ref.meta(),
                )))
            }
            Kind::Map(inner) => match self.traverse(*inner, scoped, lets)? {
                Binding::Wildcard => Ok(Binding::Wildcard),
                Binding::Binder(binder) => Ok(Binding::Binder(TypedHirData(
                    *binder.hir(),
                    // write the outer metadata into the inner binder:
                    *node_ref.meta(),
                ))),
            },
            Kind::Set(entries) => {
                if entries.len() == 1 {
                    let SetEntry(iter, Attribute { rel, val }) = entries.first().unwrap();
                    if let Some(iter_label) = iter {
                        let iter_var: Var = (*iter_label.hir()).into();
                        if self
                            .iter_extended_scope_table
                            .insert(
                                *iter_label.hir(),
                                Attribute {
                                    rel: ExtendedScope::Node(*rel),
                                    val: ExtendedScope::Node(*val),
                                },
                            )
                            .is_some()
                        {
                            return Err(UnifierError::TODO(smart_format!(
                                "fix duplicate iter scope"
                            )));
                        }

                        if matches!(scoped, Scoped::Yes) {
                            self.scope_tracker.in_scope.insert(iter_var);
                        }

                        Ok(Binding::Binder(TypedHirData(
                            Binder { var: iter_var },
                            *iter_label.meta(),
                        )))
                    } else {
                        // TODO
                        // Ok(Binding::Wildcard)
                        self.errors.error(
                            CompileError::PatternRequiresIteratedVariable,
                            &node_ref.span(),
                        );
                        Ok(Binding::Wildcard)
                    }
                } else {
                    // TODO
                    Ok(Binding::Wildcard)
                }
            }
            Kind::Struct(binder, _, struct_body) => {
                for node in struct_body {
                    self.traverse(*node, scoped, lets)?;
                }
                Ok(Binding::Binder(*binder))
            }
            Kind::Prop(flags, var, prop_id, variant) => {
                let mut sub_lets = vec![];

                match variant {
                    PropVariant::Value(Attribute { rel, val }) => {
                        let prop_scoped = scoped.prop(self.map_flags);

                        match (flags.rel_optional(), flags.pat_optional()) {
                            (true, false) => {
                                // This passed type check, so there must be a way to construct a value default
                                let default = Attribute {
                                    rel: self.write_default_node(
                                        *self.scope_arena.node_ref(*rel).meta(),
                                    ),
                                    val: self.write_default_node(
                                        *self.scope_arena.node_ref(*val).meta(),
                                    ),
                                };

                                let rel = self.traverse(*rel, prop_scoped, &mut sub_lets)?;
                                let val = self.traverse(*val, prop_scoped, &mut sub_lets)?;

                                self.push_let(
                                    Let::PropDefault(
                                        Attribute { rel, val },
                                        (*var, *prop_id),
                                        default,
                                    ),
                                    node_ref.span(),
                                    scoped,
                                    lets,
                                );
                            }
                            (rel_optional, _) => {
                                let next_scoped = if rel_optional {
                                    Scoped::MaybeVoid
                                } else {
                                    prop_scoped
                                };

                                let rel = self.traverse(*rel, next_scoped, &mut sub_lets)?;
                                let val = self.traverse(*val, next_scoped, &mut sub_lets)?;

                                self.push_let(
                                    Let::Prop(Attribute { rel, val }, (*var, *prop_id)),
                                    node_ref.span(),
                                    scoped,
                                    lets,
                                );
                            }
                        };
                    }
                    PropVariant::Predicate(..) => {
                        return Err(UnifierError::TODO(smart_format!("predicate prop scope")));
                    }
                }
                lets.extend(sub_lets);
                Ok(Binding::Wildcard)
            }
            Kind::Regex(iter_label, regex_def_id, groups_list) => {
                let haystack_var = self.var_allocator.alloc();
                match iter_label {
                    Some(iter_label) => {
                        let seq_var: Var = (*iter_label.hir()).into();
                        if matches!(scoped, Scoped::Yes) {
                            self.scope_tracker.in_scope.insert(seq_var);
                        }

                        {
                            let mut variables_by_group_index: BTreeMap<u32, VarSet> =
                                Default::default();

                            for groups in groups_list {
                                for group in groups {
                                    variables_by_group_index
                                        .entry(group.index)
                                        .or_default()
                                        .insert(group.binder.hir().var);
                                }
                            }

                            let mut unpack = thin_vec![];

                            for (_, vars) in variables_by_group_index {
                                if vars.len() > 1 {
                                    return Err(UnifierError::Unimplemented(smart_format!(
                                        "Duplicate regex group"
                                    )));
                                }
                                let var = Var(vars.0.iter().next().unwrap() as u32);
                                unpack.push(Binding::Binder(TypedHirData(
                                    Binder { var },
                                    Meta::new(
                                        self.types.intern(Type::Primitive(
                                            PrimitiveKind::Text,
                                            self.primitives.text,
                                        )),
                                        node_ref.meta().span,
                                    ),
                                )));
                            }

                            self.iter_extended_scope_table.insert(
                                *iter_label.hir(),
                                Attribute {
                                    rel: ExtendedScope::Wildcard,
                                    val: ExtendedScope::SeqUnpack(unpack, *node_ref.meta()),
                                },
                            );
                        }

                        self.push_let(
                            Let::RegexIter(
                                TypedHirData(Binder { var: seq_var }, *node_ref.meta()),
                                groups_list.clone(),
                                *regex_def_id,
                                haystack_var,
                            ),
                            node_ref.span(),
                            scoped,
                            lets,
                        );

                        Ok(Binding::Binder(TypedHirData(
                            Binder { var: haystack_var },
                            *node_ref.meta(),
                        )))
                    }
                    None => {
                        self.push_let(
                            Let::Regex(groups_list.clone(), *regex_def_id, haystack_var),
                            node_ref.span(),
                            scoped,
                            lets,
                        );
                        Ok(Binding::Binder(TypedHirData(
                            Binder { var: haystack_var },
                            *node_ref.meta(),
                        )))
                    }
                }
            }
            Kind::With(..)
            | Kind::MoveRestAttrs(_, _)
            | Kind::MakeSeq(_, _)
            | Kind::CopySubSeq(_, _)
            | Kind::ForEach(_, _, _)
            | Kind::Insert(_, _)
            | Kind::StringPush(_, _)
            | Kind::PushCondClause(_, _)
            | Kind::Try(..)
            | Kind::Let(..)
            | Kind::TryLet(..)
            | Kind::LetProp(..)
            | Kind::LetPropDefault(..)
            | Kind::TryLetProp(..)
            | Kind::TryLetTup(..)
            | Kind::LetRegex(..)
            | Kind::LetRegexIter(..)
            | Kind::LetCondVar(..) => Err(UnifierError::TODO(smart_format!(
                "Not a scope node: {node_ref}"
            ))),
        }
    }

    fn push_let(
        &mut self,
        let_node: Let<'m>,
        span: SourceSpan,
        scoped: Scoped,
        scoped_lets: &mut Vec<SpannedLet<'m>>,
    ) {
        match scoped {
            Scoped::Yes => {
                scoped_lets.push((let_node, span));
            }
            Scoped::MaybeVoid => {
                self.scope_tracker.potential_lets.push((let_node, span));
            }
        }
    }

    fn make_try_let(
        &mut self,
        var: Var,
        catch_label: Label,
        default_span: SourceSpan,
        potential_lets: &[SpannedLet<'m>],
        output: &mut Nodes,
    ) -> UnifierResult<()> {
        if self.scope_tracker.in_scope.contains(var) {
            return Ok(());
        }

        for (let_node, span) in potential_lets {
            let defines = let_node.defines();
            if defines.contains(var) {
                self.scope_tracker.in_scope.0.extend(&defines.0);

                for dependency in let_node.dependencies().iter() {
                    assert_ne!(dependency, var);
                    self.make_try_let(dependency, catch_label, *span, potential_lets, output)?;
                }

                let kind = match let_node {
                    Let::Prop(attr, var_prop) => Kind::TryLetProp(catch_label, *attr, *var_prop),
                    _ => {
                        return Err(UnifierError::Unimplemented(smart_format!(
                            "unhandled try let variant"
                        )))
                    }
                };
                output.push(self.mk_node(kind, Meta::new(&UNIT_TYPE, *span)));
                return Ok(());
            }
        }

        self.scope_tracker.in_scope.insert(var);
        output.push(self.mk_node(
            Kind::Try(catch_label, var),
            Meta::new(&UNIT_TYPE, default_span),
        ));

        Ok(())
    }
}
