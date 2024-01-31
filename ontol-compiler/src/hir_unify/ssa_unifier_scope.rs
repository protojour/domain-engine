use std::collections::BTreeMap;

use chumsky::chain::Chain;
use ontol_hir::{Attribute, Nodes};
use ontol_runtime::{
    smart_format,
    var::{Var, VarSet},
};
use smallvec::smallvec;
use thin_vec::thin_vec;
use tracing::debug;

use crate::{
    hir_unify::{ssa_util::NodesExt, UnifierError},
    mem::Intern,
    primitive::PrimitiveKind,
    typed_hir::{Meta, TypedHir, TypedHirData, UNIT_META},
    types::{Type, UNIT_TYPE},
    SourceSpan,
};

use super::{
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
    ) -> UnifierResult<ontol_hir::Binding<'m, TypedHir>> {
        match scope_node {
            ExtendedScope::Wildcard => Ok(ontol_hir::Binding::Wildcard),
            ExtendedScope::Node(node) => self.define_scope(node, scoped, body),
            ExtendedScope::SeqUnpack(bindings, source_meta) => {
                let var = self.var_allocator.alloc();

                let catch_label = catcher.make_catch_label(&mut self.var_allocator);

                let var_node = self.mk_node(ontol_hir::Kind::Var(var), source_meta);

                let mut new_bindings = thin_vec![];
                for binding in bindings {
                    match binding {
                        ontol_hir::Binding::Wildcard => {
                            new_bindings.push(ontol_hir::Binding::Wildcard);
                        }
                        ontol_hir::Binding::Binder(binder) => {
                            if free_vars.contains(binder.hir().var) {
                                self.scope_tracker.in_scope.insert(binder.hir().var);
                                new_bindings.push(ontol_hir::Binding::Binder(binder))
                            } else {
                                new_bindings.push(ontol_hir::Binding::Wildcard);
                            }
                        }
                    }
                }

                body.push(self.mk_node(
                    ontol_hir::Kind::TryLetTup(catch_label, new_bindings, var_node),
                    UNIT_META,
                ));

                Ok(ontol_hir::Binding::Binder(TypedHirData(
                    ontol_hir::Binder { var },
                    UNIT_META,
                )))
            }
        }
    }

    pub(super) fn define_scope(
        &mut self,
        scope_node: ontol_hir::Node,
        scoped: Scoped,
        body: &mut Nodes,
    ) -> UnifierResult<ontol_hir::Binding<'m, TypedHir>> {
        let node_ref = self.scope_arena.node_ref(scope_node);

        use ontol_hir::{Binding, Kind};
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
                body.extend(block_body.iter().cloned());
                Ok(Binding::Wildcard)
            }
            Kind::Unit => Ok(Binding::Wildcard),
            Kind::I64(_) => Ok(Binding::Wildcard),
            Kind::F64(_) => Ok(Binding::Wildcard),
            Kind::Text(_) => Ok(Binding::Wildcard),
            Kind::Const(_) => Ok(Binding::Wildcard),
            Kind::With(_, _, _) => Err(UnifierError::TODO(smart_format!("with scope"))),
            Kind::Call(_, _) => Err(UnifierError::TODO(smart_format!("call scope"))),
            Kind::Map(inner) => match self.define_scope(*inner, scoped, body)? {
                ontol_hir::Binding::Wildcard => Ok(ontol_hir::Binding::Wildcard),
                ontol_hir::Binding::Binder(binder) => Ok(ontol_hir::Binding::Binder(TypedHirData(
                    *binder.hir(),
                    // write the outer metadata into the inner binder:
                    *node_ref.meta(),
                ))),
            },
            Kind::Set(_) => Err(UnifierError::SequenceInputNotSupported),
            Kind::Struct(binder, _, struct_body) => {
                if matches!(scoped, Scoped::Yes) {
                    for node in struct_body {
                        self.define_scope(*node, scoped, body)?;
                    }
                }
                Ok(Binding::Binder(*binder))
            }
            Kind::Prop(flags, var, prop_id, variants) => {
                for variant in variants {
                    match variant {
                        ontol_hir::PropVariant::Singleton(ontol_hir::Attribute { rel, val }) => {
                            let let_prop = body.push_node(self.prealloc_node());
                            let scoped_prop = Scoped::prop(self.map_flags);

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

                                    let rel = self.define_scope(*rel, scoped_prop, body)?;
                                    let val = self.define_scope(*val, scoped_prop, body)?;

                                    self.write_node(
                                        let_prop,
                                        ontol_hir::Kind::LetPropDefault(
                                            Attribute { rel, val },
                                            (*var, *prop_id),
                                            default,
                                        ),
                                        Meta::new(&UNIT_TYPE, node_ref.span()),
                                    );
                                }
                                (rel_optional, _) => {
                                    let next_scoped = if rel_optional {
                                        Scoped::MaybeVoid
                                    } else {
                                        scoped_prop
                                    };

                                    let rel = self.define_scope(*rel, next_scoped, body)?;
                                    let val = self.define_scope(*val, next_scoped, body)?;

                                    self.write_node(
                                        let_prop,
                                        ontol_hir::Kind::LetProp(
                                            Attribute { rel, val },
                                            (*var, *prop_id),
                                        ),
                                        Meta::new(&UNIT_TYPE, node_ref.span()),
                                    );
                                }
                            };
                        }
                        ontol_hir::PropVariant::Set(variant) => {
                            let label = *variant.label.hir();

                            for (iter, attr) in &variant.elements {
                                if iter.0
                                    && self
                                        .iter_extended_scope_table
                                        .insert(
                                            label,
                                            ontol_hir::Attribute {
                                                rel: ExtendedScope::Node(attr.rel),
                                                val: ExtendedScope::Node(attr.val),
                                            },
                                        )
                                        .is_some()
                                {
                                    return Err(UnifierError::TODO(smart_format!(
                                        "fix duplicate iter scope"
                                    )));
                                }
                            }

                            let set_var = Var(label.0);

                            if matches!(scoped, Scoped::Yes) {
                                self.scope_tracker.in_scope.insert(set_var);
                            }

                            let seq_meta = Meta::new(variant.label.ty(), node_ref.span());

                            let binding = Attribute {
                                rel: ontol_hir::Binding::Wildcard,
                                val: ontol_hir::Binding::Binder(TypedHirData(
                                    set_var.into(),
                                    seq_meta,
                                )),
                            };

                            if variant.has_default.0 {
                                let rel = self.mk_node(
                                    ontol_hir::Kind::Unit,
                                    Meta::new(&UNIT_TYPE, node_ref.span()),
                                );
                                let dummy_binder = self.var_allocator.alloc();
                                let val = self.mk_node(
                                    ontol_hir::Kind::MakeSeq(
                                        TypedHirData(dummy_binder.into(), seq_meta),
                                        smallvec![],
                                    ),
                                    seq_meta,
                                );

                                body.push(self.mk_node(
                                    ontol_hir::Kind::LetPropDefault(
                                        binding,
                                        (*var, *prop_id),
                                        ontol_hir::Attribute { rel, val },
                                    ),
                                    Meta::new(&UNIT_TYPE, node_ref.span()),
                                ));
                            } else {
                                body.push(self.mk_node(
                                    ontol_hir::Kind::LetProp(binding, (*var, *prop_id)),
                                    Meta::new(&UNIT_TYPE, node_ref.span()),
                                ));
                            }
                        }
                        ontol_hir::PropVariant::Predicate(_) => {
                            return Err(UnifierError::TODO(smart_format!("predicate prop scope")));
                        }
                    }
                }
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
                                unpack.push(ontol_hir::Binding::Binder(TypedHirData(
                                    ontol_hir::Binder { var },
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
                                ontol_hir::Attribute {
                                    rel: ExtendedScope::Wildcard,
                                    val: ExtendedScope::SeqUnpack(unpack, *node_ref.meta()),
                                },
                            );
                        }

                        body.push(self.mk_node(
                            ontol_hir::Kind::LetRegexIter(
                                TypedHirData(ontol_hir::Binder { var: seq_var }, *node_ref.meta()),
                                groups_list.clone(),
                                *regex_def_id,
                                haystack_var,
                            ),
                            Meta::new(&UNIT_TYPE, node_ref.span()),
                        ));
                        Ok(Binding::Binder(TypedHirData(
                            ontol_hir::Binder { var: haystack_var },
                            *node_ref.meta(),
                        )))
                    }
                    None => {
                        body.push(self.mk_node(
                            ontol_hir::Kind::LetRegex(
                                groups_list.clone(),
                                *regex_def_id,
                                haystack_var,
                            ),
                            Meta::new(&UNIT_TYPE, node_ref.span()),
                        ));
                        Ok(Binding::Binder(TypedHirData(
                            ontol_hir::Binder { var: haystack_var },
                            *node_ref.meta(),
                        )))
                    }
                }
            }
            Kind::MoveRestAttrs(_, _)
            | Kind::MatchProp(_, _, _)
            | Kind::MakeSeq(_, _)
            | Kind::CopySubSeq(_, _)
            | Kind::ForEach(_, _, _)
            | Kind::Insert(_, _)
            | Kind::StringPush(_, _)
            | Kind::MatchRegex(_, _, _, _)
            | Kind::PushCondClause(_, _)
            | Kind::Try(..)
            | Kind::Let(..)
            | Kind::TryLet(..)
            | Kind::LetProp(..)
            | Kind::LetPropDefault(..)
            | Kind::TryLetProp(..)
            | Kind::TryLetTup(..)
            | Kind::LetRegex(..)
            | Kind::LetRegexIter(..) => Err(UnifierError::TODO(smart_format!(
                "Not a scope node: {node_ref}"
            ))),
        }
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

        let unscoped = VarSet(
            free_vars
                .0
                .difference(&self.scope_tracker.in_scope.0)
                .collect(),
        );

        if unscoped.0.is_empty() {
            return body_func(self);
        }

        debug!("catch block: free vars: {free_vars:?}");
        debug!(
            "catch block: total scope: {:?}",
            self.all_scope_vars_and_labels
        );

        let scope_backup = self.scope_tracker.clone();

        let catch_block = self.prealloc_node();
        let catch_label = ontol_hir::Label(self.var_allocator.alloc().0);
        let mut catch_body = smallvec![];

        debug!("apply catch block: {unscoped:?}");

        for var in unscoped.iter() {
            self.scope_tracker.in_scope.insert(var);
            catch_body.push(self.mk_node(
                ontol_hir::Kind::Try(catch_label, var),
                Meta::new(&UNIT_TYPE, span),
            ));
        }

        catch_body.extend(body_func(self)?);

        self.scope_tracker = scope_backup;

        Ok(smallvec![self.write_node(
            catch_block,
            ontol_hir::Kind::Catch(catch_label, catch_body),
            Meta::new(&UNIT_TYPE, span),
        )])
    }
}
