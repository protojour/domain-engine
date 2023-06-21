use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Range,
};

use fnv::FnvHashMap;
use ontol_parser::{ast, Span};
use ontol_runtime::{smart_format, DefId, DefParamId, RelationId, RelationshipId};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    def::{
        Def, DefKind, DefParamBinding, DefReference, FmtFinalState, PropertyCardinality, RelParams,
        Relation, RelationKind, Relationship, TypeDef, TypeDefParam, ValueCardinality, Variables,
    },
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, ExprStructAttr, TypePath},
    namespace::Space,
    package::{PackageReference, CORE_PKG},
    Compiler, Src,
};

pub struct Lowering<'s, 'm> {
    compiler: &'s mut Compiler<'m>,
    src: &'s Src,
    root_defs: Vec<DefId>,
}

type LoweringError = (CompileError, Span);

type Res<T> = Result<T, LoweringError>;
type RootDefs = SmallVec<[DefId; 1]>;

impl<'s, 'm> Lowering<'s, 'm> {
    pub fn new(compiler: &'s mut Compiler<'m>, src: &'s Src) -> Self {
        Self {
            compiler,
            src,
            root_defs: Default::default(),
        }
    }

    pub fn finish(self) -> Vec<DefId> {
        debug!("Lowering finish: {:?}", self.root_defs);
        self.root_defs
    }

    pub fn lower_statement(&mut self, stmt: (ast::Statement, Span)) -> Result<(), ()> {
        match self.stmt_to_def(stmt, BlockContext::NoContext) {
            Ok(root_defs) => {
                self.root_defs.extend(root_defs.into_iter());
                Ok(())
            }
            Err(error) => {
                self.report_error(error);
                Err(())
            }
        }
    }

    fn report_error(&mut self, (error, span): (CompileError, Span)) {
        self.compiler
            .push_error(error.spanned(&self.src.span(&span)));
    }

    fn stmt_to_def(
        &mut self,
        (stmt, span): (ast::Statement, Span),
        block_context: BlockContext,
    ) -> Res<RootDefs> {
        match stmt {
            ast::Statement::Use(use_stmt) => {
                let reference = PackageReference::Named(use_stmt.reference.0);
                let self_package_def_id = self.src.package_id;
                let used_package_def_id = self
                    .compiler
                    .packages
                    .loaded_packages
                    .get(&reference)
                    .ok_or_else(|| (CompileError::PackageNotFound, use_stmt.reference.1))?;

                let type_namespace = self
                    .compiler
                    .namespaces
                    .get_namespace_mut(self_package_def_id, Space::Type);

                type_namespace.insert(use_stmt.as_ident.0, *used_package_def_id);

                Ok(Default::default())
            }
            ast::Statement::Type(type_stmt) => self.define_type(type_stmt, span),
            ast::Statement::With(with_stmt) => {
                let mut root_defs = RootDefs::new();
                let def = self.resolve_type_reference(
                    with_stmt.ty.0,
                    &with_stmt.ty.1,
                    Some(&mut root_defs),
                )?;
                let context_fn = move || def.clone();

                for spanned_stmt in with_stmt.statements.0 {
                    match self.stmt_to_def(spanned_stmt, BlockContext::Context(&context_fn)) {
                        Ok(mut defs) => {
                            root_defs.append(&mut defs);
                        }
                        Err(error) => {
                            self.report_error(error);
                        }
                    }
                }

                Ok(root_defs)
            }
            ast::Statement::Rel(rel_stmt) => {
                self.ast_relationship_to_def(rel_stmt, span, block_context)
            }
            ast::Statement::Fmt(fmt_stmt) => {
                self.fmt_transitions_to_def(fmt_stmt, span, block_context)
            }
            ast::Statement::Map(ast::MapStatement {
                kw: _,
                first,
                second,
            }) => {
                let mut var_table = ExprVarTable::default();
                let first = self.lower_map_arm(first, &mut var_table)?;
                let second = self.lower_map_arm(second, &mut var_table)?;
                let variables = var_table.variables.into_values().collect();

                Ok(
                    [self.define(DefKind::Mapping(Variables(variables), first, second), &span)]
                        .into(),
                )
            }
        }
    }

    fn define_type(&mut self, type_stmt: ast::TypeStatement, span: Span) -> Res<RootDefs> {
        let def_id = match self.named_def_id(Space::Type, &type_stmt.ident.0) {
            Ok(def_id) => def_id,
            Err(_) => return Err((CompileError::DuplicateTypeDefinition, type_stmt.ident.1)),
        };
        let ident = self.compiler.strings.intern(&type_stmt.ident.0);
        debug!("type `{ident}`: got {def_id:?}");
        let params = type_stmt.params.map(|(ast_params, _span)| {
            ast_params
                .into_iter()
                .map(|(param, _span)| {
                    let ident = self.compiler.strings.intern(param.ident.0.as_str());
                    let id = self.compiler.defs.alloc_def_param_id();

                    (ident, TypeDefParam { id })
                })
                .collect()
        });

        self.set_def_kind(
            def_id,
            DefKind::Type(TypeDef {
                public: matches!(type_stmt.visibility.0, ast::Visibility::Public),
                ident: Some(ident),
                params,
                rel_type_for: None,
            }),
            &span,
        );

        let mut root_defs: RootDefs = [def_id].into();

        self.compiler.namespaces.docs.insert(def_id, type_stmt.docs);

        if let Some((ctx_block, _span)) = type_stmt.ctx_block {
            // The inherent relation block on the type uses the just defined
            // type as its context
            let def = DefReference {
                def_id,
                pattern_bindings: Default::default(),
            };
            let context_fn = move || def.clone();

            for spanned_stmt in ctx_block {
                match self.stmt_to_def(spanned_stmt, BlockContext::Context(&context_fn)) {
                    Ok(mut defs) => {
                        root_defs.append(&mut defs);
                    }
                    Err(error) => {
                        self.report_error(error);
                    }
                }
            }
        }

        Ok(root_defs)
    }

    fn ast_relationship_to_def(
        &mut self,
        rel: ast::RelStatement,
        span: Span,
        block_context: BlockContext,
    ) -> Res<RootDefs> {
        let ast::RelStatement {
            docs,
            kw: _,
            subject: (subject, subject_span),
            relations,
            object: (object, object_span),
        } = rel;

        let mut root_defs = RootDefs::default();
        let subject_def = self.resolve_contextual_type_reference(
            subject,
            subject_span.clone(),
            &block_context,
            Some(&mut root_defs),
        )?;
        let object_def = match object {
            Some(ast::TypeOrPattern::Type(ty)) => self.resolve_contextual_type_reference(
                Some(ty),
                object_span.clone(),
                &block_context,
                Some(&mut root_defs),
            )?,
            Some(ast::TypeOrPattern::Pattern(pattern)) => {
                let mut var_table = ExprVarTable::default();
                let expr = self.lower_pattern((pattern, object_span.clone()), &mut var_table)?;
                let expr_id = self.compiler.expressions.alloc_expr_id();
                self.compiler.expressions.map.insert(expr_id, expr);

                DefReference {
                    def_id: self.define(DefKind::Constant(expr_id), &object_span),
                    pattern_bindings: Default::default(),
                }
            }
            None => self.resolve_contextual_type_reference(
                None,
                object_span.clone(),
                &block_context,
                Some(&mut root_defs),
            )?,
        };

        let mut relation_iter = relations.into_iter().peekable();
        let mut docs = Some(docs);

        while let Some(relation) = relation_iter.next() {
            root_defs.extend(self.def_relationship(
                (subject_def.clone(), &subject_span),
                relation,
                (object_def.clone(), &object_span),
                span.clone(),
                if relation_iter.peek().is_some() {
                    docs.clone().unwrap()
                } else {
                    docs.take().unwrap()
                },
            )?);
        }
        Ok(root_defs)
    }

    fn def_relationship(
        &mut self,
        subject: (DefReference, &Span),
        ast_relation: ast::Relation,
        object: (DefReference, &Span),
        span: Span,
        docs: Vec<String>,
    ) -> Res<RootDefs> {
        let mut root_defs = RootDefs::new();
        let ast::Relation {
            ty: relation_ty,
            subject_cardinality,
            object_prop_ident,
            ctx_block,
            object_cardinality,
        } = ast_relation;

        let (key, ident_span, index_range_rel_params): (_, _, Option<Range<Option<u16>>>) =
            match relation_ty {
                ast::RelType::Type((ty, span)) => {
                    let def_ref = self.resolve_type_reference(ty, &span, Some(&mut root_defs))?;

                    match self.compiler.defs.get_def_kind(def_ref.def_id) {
                        Some(DefKind::StringLiteral(_)) => {
                            (RelationKey::Named(def_ref), span.clone(), None)
                        }
                        Some(DefKind::Relation(_relation)) => {
                            (RelationKey::Builtin(def_ref.def_id), span.clone(), None)
                        }
                        _ => return Err((CompileError::InvalidRelationType, span)),
                    }
                }
                ast::RelType::IntRange((range, span)) => (RelationKey::Indexed, span, Some(range)),
            };

        let has_object_prop = object_prop_ident.is_some();

        // This syntax just defines the relation the first time it's used
        let relation_id = match self.define_relation_if_undefined(key) {
            ImplicitRelationId::New(kind, relation_id) => {
                let object_prop =
                    object_prop_ident.map(|ident| self.compiler.strings.intern(&ident.0));

                self.set_def_kind(
                    relation_id.0,
                    DefKind::Relation(Relation {
                        kind,
                        subject_prop: None,
                        object_prop,
                    }),
                    &ident_span,
                );
                relation_id
            }
            ImplicitRelationId::Reused(relation_id) => relation_id,
        };

        let relationship_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        self.compiler.namespaces.docs.insert(relationship_id, docs);

        let rel_params = if let Some(index_range_rel_params) = index_range_rel_params {
            if let Some((_, span)) = ctx_block {
                return Err((
                    CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes,
                    span,
                ));
            }

            RelParams::IndexRange(index_range_rel_params)
        } else if let Some((ctx_block, _)) = ctx_block {
            let rel_def = self.define_anonymous_type(
                TypeDef {
                    public: false,
                    ident: None,
                    params: None,
                    rel_type_for: Some(RelationshipId(relationship_id)),
                },
                &span,
            );
            let context_fn = || rel_def.clone();

            root_defs.push(rel_def.def_id);

            // This type needs to be part of the anonymous part of the namespace
            self.compiler
                .namespaces
                .add_anonymous(self.src.package_id, rel_def.def_id);

            for spanned_stmt in ctx_block {
                match self.stmt_to_def(spanned_stmt, BlockContext::Context(&context_fn)) {
                    Ok(mut defs) => {
                        root_defs.append(&mut defs);
                    }
                    Err(error) => {
                        self.report_error(error);
                    }
                }
            }

            RelParams::Type(rel_def)
        } else {
            RelParams::Unit
        };

        let mut relationship = Relationship {
            relation_id,
            subject: (subject.0, self.src.span(subject.1)),
            subject_cardinality: subject_cardinality
                .map(convert_cardinality)
                .unwrap_or((PropertyCardinality::Mandatory, ValueCardinality::One)),
            object: (object.0, self.src.span(object.1)),
            object_cardinality: object_cardinality
                .map(convert_cardinality)
                .unwrap_or_else(|| {
                    if has_object_prop {
                        // i.e. no syntax sugar: The object prop is explicit,
                        // therefore the object cardinality is explicit.
                        (PropertyCardinality::Mandatory, ValueCardinality::One)
                    } else {
                        // The syntactic sugar case, which is the default behaviour:
                        // Many incoming edges to the same object:
                        (PropertyCardinality::Optional, ValueCardinality::Many)
                    }
                }),
            rel_params,
        };

        // HACK(for now): invert relationship
        if relation_id.0 == self.compiler.primitives.id_relation {
            relationship = Relationship {
                relation_id: RelationId(self.compiler.primitives.identifies_relation),
                subject: relationship.object,
                subject_cardinality: relationship.object_cardinality,
                object: relationship.subject,
                object_cardinality: relationship.subject_cardinality,
                rel_params: relationship.rel_params,
            };
        }

        self.set_def_kind(relationship_id, DefKind::Relationship(relationship), &span);
        root_defs.push(relationship_id);

        Ok(root_defs)
    }

    fn fmt_transitions_to_def(
        &mut self,
        fmt: ast::FmtStatement,
        span: Span,
        block_context: BlockContext,
    ) -> Res<RootDefs> {
        let ast::FmtStatement {
            docs: _,
            kw: _,
            origin: (origin, mut origin_span),
            transitions,
        } = fmt;

        let mut root_defs = SmallVec::new();
        let mut origin_def =
            self.resolve_type_reference(origin, &origin_span, Some(&mut root_defs))?;
        let mut iter = transitions.into_iter().peekable();

        let (mut transition, mut transition_span) = match iter.next() {
            Some((Some(next), span)) => (next, span),
            Some((None, _)) => return Err((CompileError::FmtMisplacedWildcard, span)),
            None => return Err((CompileError::FmtTooFewTransitions, span)),
        };

        let target = loop {
            let (next_transition, span) = match iter.next() {
                Some(item) if iter.peek().is_none() => {
                    // end of iterator, found the final target. Handle this outside the loop:
                    break item;
                }
                Some((Some(item), span)) => (item, span),
                Some((None, _)) => return Err((CompileError::FmtMisplacedWildcard, span)),
                _ => return Err((CompileError::FmtTooFewTransitions, span)),
            };

            let target_def = self.define_anonymous_type(
                TypeDef {
                    public: false,
                    ident: None,
                    params: None,
                    rel_type_for: None,
                },
                &span,
            );

            root_defs.push(self.ast_fmt_transition_to_def(
                (origin_def, &origin_span),
                (transition, span.clone()),
                (target_def.clone(), &span),
                FmtFinalState(false),
                span.clone(),
            )?);

            transition = next_transition;
            transition_span = span.clone();
            origin_def = target_def;
            origin_span = span;
        };

        let end_def = self.resolve_contextual_type_reference(
            target.0,
            target.1.clone(),
            &block_context,
            Some(&mut root_defs),
        )?;

        root_defs.push(self.ast_fmt_transition_to_def(
            (origin_def, &origin_span),
            (transition, transition_span),
            (end_def, &target.1),
            FmtFinalState(true),
            span,
        )?);

        Ok(root_defs)
    }

    fn ast_fmt_transition_to_def(
        &mut self,
        from: (DefReference, &Span),
        transition: (ast::Type, Span),
        to: (DefReference, &Span),
        final_state: FmtFinalState,
        span: Span,
    ) -> Res<DefId> {
        let transition_def = self.resolve_type_reference(transition.0, &transition.1, None)?;
        let relation_key = RelationKey::FmtTransition(transition_def, final_state);

        // This syntax just defines the relation the first time it's used
        let relation_id = match self.define_relation_if_undefined(relation_key) {
            ImplicitRelationId::New(kind, relation_id) => {
                self.set_def_kind(
                    relation_id.0,
                    DefKind::Relation(Relation {
                        kind,
                        subject_prop: None,
                        object_prop: None,
                    }),
                    &transition.1,
                );
                relation_id
            }
            ImplicitRelationId::Reused(relation_id) => relation_id,
        };

        debug!("define transition relation {relation_id:?}");

        Ok(self.define(
            DefKind::Relationship(Relationship {
                relation_id,
                subject: (from.0, self.src.span(from.1)),
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
                object: (to.0, self.src.span(to.1)),
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
                rel_params: RelParams::Unit,
            }),
            &span,
        ))
    }

    fn resolve_contextual_type_reference(
        &mut self,
        ast_ty: Option<ast::Type>,
        span: Range<usize>,
        block_context: &BlockContext,
        root_defs: Option<&mut RootDefs>,
    ) -> Res<DefReference> {
        match (ast_ty, block_context) {
            (Some(ast_ty), _) => self.resolve_type_reference(ast_ty, &span, root_defs),
            (None, BlockContext::Context(func)) => Ok(func()),
            _ => Err((CompileError::WildcardNeedsContextualBlock, span)),
        }
    }

    fn resolve_type_reference(
        &mut self,
        ast_ty: ast::Type,
        span: &Span,
        root_defs: Option<&mut RootDefs>,
    ) -> Res<DefReference> {
        match ast_ty {
            ast::Type::Unit => Ok(DefReference {
                def_id: self.compiler.primitives.unit,
                pattern_bindings: Default::default(),
            }),
            ast::Type::Path(path, param_patterns) => {
                let def_id = self.lookup_path(&path, span)?;
                let args = if let Some((patterns, patterns_span)) = param_patterns {
                    self.resolve_type_pattern_bindings(patterns, patterns_span, def_id)
                } else {
                    Default::default()
                };

                Ok(DefReference {
                    def_id,
                    pattern_bindings: args,
                })
            }
            ast::Type::AnonymousStruct((ctx_block, _block_span)) => {
                if let Some(root_defs) = root_defs {
                    let def = self.define_anonymous_type(
                        TypeDef {
                            public: false,
                            ident: None,
                            params: None,
                            rel_type_for: None,
                        },
                        span,
                    );

                    // This type needs to be part of the anonymous part of the namespace
                    self.compiler
                        .namespaces
                        .add_anonymous(self.src.package_id, def.def_id);
                    root_defs.push(def.def_id);

                    let context_fn = || def.clone();

                    for spanned_stmt in ctx_block {
                        match self.stmt_to_def(spanned_stmt, BlockContext::Context(&context_fn)) {
                            Ok(mut defs) => {
                                root_defs.append(&mut defs);
                            }
                            Err(error) => {
                                self.report_error(error);
                            }
                        }
                    }

                    Ok(DefReference {
                        def_id: def.def_id,
                        pattern_bindings: Default::default(),
                    })
                } else {
                    Err((
                        CompileError::TODO(smart_format!("Anonymous struct not allowed here")),
                        span.clone(),
                    ))
                }
            }
            ast::Type::NumberLiteral(lit) => {
                let lit = self.compiler.strings.intern(&lit);
                let def_id = self.compiler.defs.add_def(
                    DefKind::NumberLiteral(lit),
                    CORE_PKG,
                    self.src.span(span),
                );
                Ok(DefReference {
                    def_id,
                    pattern_bindings: Default::default(),
                })
            }
            ast::Type::StringLiteral(lit) => {
                let def_id = match lit.as_str() {
                    "" => self.compiler.primitives.empty_string,
                    _ => self
                        .compiler
                        .defs
                        .def_string_literal(&lit, &mut self.compiler.strings),
                };
                Ok(DefReference {
                    def_id,
                    pattern_bindings: Default::default(),
                })
            }
            ast::Type::Regex(lit) => {
                let def_id = self
                    .compiler
                    .defs
                    .def_regex(&lit, span, &mut self.compiler.strings)
                    .map_err(|(compile_error, err_span)| {
                        (CompileError::InvalidRegex(compile_error), err_span)
                    })?;
                Ok(DefReference {
                    def_id,
                    pattern_bindings: Default::default(),
                })
            }
        }
    }

    fn resolve_type_pattern_bindings(
        &mut self,
        ast_patterns: Vec<(ast::TypeParamPattern, Range<usize>)>,
        span: Range<usize>,
        def_id: DefId,
    ) -> FnvHashMap<DefParamId, DefParamBinding> {
        match self.compiler.defs.get_def_kind(def_id).unwrap() {
            DefKind::Type(TypeDef {
                params: Some(params),
                ..
            }) => {
                let mut args: FnvHashMap<DefParamId, DefParamBinding> = Default::default();
                for (ast_pattern, _span) in ast_patterns {
                    match params.get(ast_pattern.ident.0.as_str()) {
                        Some(type_def_param) => match ast_pattern.binding {
                            ast::TypeParamPatternBinding::None => {
                                args.insert(type_def_param.id, DefParamBinding::Bound(0));
                            }
                            ast::TypeParamPatternBinding::Equals((ty, ty_span)) => {
                                match self.resolve_type_reference(ty, &ty_span, None) {
                                    Ok(value) => {
                                        args.insert(
                                            type_def_param.id,
                                            DefParamBinding::Provided(
                                                value,
                                                self.src.span(&ty_span),
                                            ),
                                        );
                                    }
                                    Err(error) => {
                                        self.report_error(error);
                                    }
                                }
                            }
                        },
                        None => self.report_error((
                            CompileError::UnknownTypeParameter,
                            ast_pattern.ident.1,
                        )),
                    }
                }

                args
            }
            _ => {
                self.report_error((CompileError::InvalidTypeParameters, span));
                Default::default()
            }
        }
    }

    fn lower_map_arm(
        &mut self,
        ((unit_or_seq, ast), span): ((ast::UnitOrSeq, ast::MapArm), Span),
        var_table: &mut ExprVarTable,
    ) -> Res<ExprId> {
        let unit_expr = match ast {
            ast::MapArm::Struct(ast) => {
                self.lower_struct_pattern((ast, span.clone()), var_table)?
            }
            ast::MapArm::Binding { path, expr } => {
                self.lower_map_binding(path, expr, span.clone(), var_table)?
            }
        };
        let expr = match unit_or_seq {
            ast::UnitOrSeq::Unit => unit_expr,
            ast::UnitOrSeq::Seq => {
                let seq_id = self.compiler.expressions.alloc_expr_id();
                self.expr(ExprKind::Seq(seq_id, Box::new(unit_expr)), &span)
            }
        };

        let expr_id = self.compiler.expressions.alloc_expr_id();
        self.compiler.expressions.map.insert(expr_id, expr);

        Ok(expr_id)
    }

    fn lower_map_binding(
        &mut self,
        path: (ast::Path, Span),
        expr: (ast::ExprPattern, Span),
        span: Span,
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        let type_def_id = self.lookup_path(&path.0, &path.1)?;
        let key = (
            DefReference {
                def_id: DefId::unit(),
                pattern_bindings: Default::default(),
            },
            self.src.span(&span),
        );
        let expr = self.lower_expr_pattern((expr.0, expr.1), var_table)?;

        Ok(self.expr(
            // FIXME: This ExprKind shouldn't really be struct..
            ExprKind::Struct(
                TypePath {
                    def_id: type_def_id,
                    span: self.src.span(&path.1),
                },
                [ExprStructAttr {
                    key,
                    rel: None,
                    bind_option: false,
                    object: expr,
                }]
                .into(),
            ),
            &span,
        ))
    }

    fn lower_struct_pattern(
        &mut self,
        (struct_pat, span): (ast::StructPattern, Span),
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        let type_def_id = self.lookup_path(&struct_pat.path.0, &struct_pat.path.1)?;
        let attributes = self.lower_struct_pattern_attrs(struct_pat.attributes, var_table)?;

        Ok(self.expr(
            ExprKind::Struct(
                TypePath {
                    def_id: type_def_id,
                    span: self.src.span(&struct_pat.path.1),
                },
                attributes,
            ),
            &span,
        ))
    }

    fn lower_struct_pattern_attrs(
        &mut self,
        attributes: Vec<(ast::StructPatternAttr, Range<usize>)>,
        var_table: &mut ExprVarTable,
    ) -> Res<Box<[ExprStructAttr]>> {
        attributes
            .into_iter()
            .map(|(struct_attr, _span)| {
                let ast::StructPatternAttr {
                    relation,
                    relation_attrs,
                    option,
                    object: (object, object_span),
                } = struct_attr;

                let def = self.resolve_type_reference(relation.0, &relation.1, None)?;

                let rel = match relation_attrs {
                    Some((attrs, span)) => {
                        let attributes = self.lower_struct_pattern_attrs(attrs, var_table)?;
                        Some(self.expr(ExprKind::AnonStruct(attributes), &span))
                    }
                    None => None,
                };
                let object_expr = self.lower_pattern((object, object_span), var_table);

                object_expr.map(|object_expr| ExprStructAttr {
                    key: (def, self.src.span(&relation.1)),
                    rel,
                    bind_option: option.is_some(),
                    object: object_expr,
                })
            })
            .collect()
    }

    fn lower_pattern(
        &mut self,
        (pattern, span): (ast::Pattern, Span),
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        match pattern {
            ast::Pattern::Expr(((ast::UnitOrSeq::Unit, expr_pat), _)) => {
                self.lower_expr_pattern((expr_pat, span), var_table)
            }
            ast::Pattern::Expr(((ast::UnitOrSeq::Seq, expr_pat), _)) => {
                let seq_id = self.compiler.expressions.alloc_expr_id();
                let inner = self.lower_expr_pattern((expr_pat, span.clone()), var_table)?;
                Ok(self.expr(ExprKind::Seq(seq_id, Box::new(inner)), &span))
            }
            ast::Pattern::Struct(((ast::UnitOrSeq::Unit, struct_pat), span)) => {
                self.lower_struct_pattern((struct_pat, span), var_table)
            }
            ast::Pattern::Struct(((ast::UnitOrSeq::Seq, struct_pat), span)) => {
                let seq_id = self.compiler.expressions.alloc_expr_id();
                let inner = self.lower_struct_pattern((struct_pat, span.clone()), var_table)?;
                Ok(self.expr(ExprKind::Seq(seq_id, Box::new(inner)), &span))
            }
        }
    }

    fn lower_expr_pattern(
        &mut self,
        (expr_pat, span): (ast::ExprPattern, Span),
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        match expr_pat {
            ast::ExprPattern::NumberLiteral(int) => {
                let int = int
                    .parse()
                    .map_err(|_| (CompileError::InvalidInteger, span.clone()))?;
                Ok(self.expr(ExprKind::Constant(int), &span))
            }
            ast::ExprPattern::Binary(left, op, right) => {
                let fn_ident = match op {
                    ast::BinaryOp::Add => "+",
                    ast::BinaryOp::Sub => "-",
                    ast::BinaryOp::Mul => "*",
                    ast::BinaryOp::Div => "/",
                };

                let def_id = self.lookup_ident(fn_ident, &span)?;

                let left = self.lower_expr_pattern(*left, var_table)?;
                let right = self.lower_expr_pattern(*right, var_table)?;

                Ok(self.expr(ExprKind::Call(def_id, Box::new([left, right])), &span))
            }
            ast::ExprPattern::Variable(var_ident) => {
                self.lower_expr_variable(var_ident, span, var_table)
            }
            _ => Err((CompileError::InvalidExpression, span)),
        }
    }

    fn lower_expr_variable(
        &mut self,
        var_ident: String,
        span: Span,
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        let id = var_table.get_or_create_var_id(var_ident, self.compiler);
        Ok(self.expr(ExprKind::Variable(id), &span))
    }

    fn lookup_ident(&mut self, ident: &str, span: &Span) -> Result<DefId, LoweringError> {
        // A single ident looks in both core and the current package
        match self
            .compiler
            .namespaces
            .lookup(&[self.src.package_id, CORE_PKG], Space::Type, ident)
        {
            Some(def_id) => Ok(def_id),
            None => Err((CompileError::TypeNotFound, span.clone())),
        }
    }

    fn lookup_path(&mut self, path: &ast::Path, span: &Span) -> Result<DefId, LoweringError> {
        match path {
            ast::Path::Ident(ident) => self.lookup_ident(ident, span),
            ast::Path::Path(segments) => {
                // a path is fully qualified
                let mut namespace = self
                    .compiler
                    .namespaces
                    .namespaces
                    .get(&self.src.package_id)
                    .unwrap();

                let mut segment_iter = segments.iter().peekable();
                let mut def_id = None;

                while let Some((segment, segment_span)) = segment_iter.next() {
                    def_id = namespace.space(Space::Type).get(segment);
                    if segment_iter.peek().is_some() {
                        match def_id {
                            Some(def_id) => match self.compiler.defs.get_def_kind(*def_id) {
                                Some(DefKind::Package(package_id)) => {
                                    namespace = self
                                        .compiler
                                        .namespaces
                                        .namespaces
                                        .get(package_id)
                                        .unwrap();
                                }
                                other => {
                                    debug!("namespace not found. def kind was {other:?}");
                                    return Err((
                                        CompileError::NamespaceNotFound,
                                        segment_span.clone(),
                                    ));
                                }
                            },
                            None => {
                                return Err((CompileError::NamespaceNotFound, segment_span.clone()))
                            }
                        }
                    }
                }

                match def_id {
                    Some(def_id) => match self.compiler.defs.get_def_kind(*def_id) {
                        Some(DefKind::Type(TypeDef { public: false, .. })) => {
                            Err((CompileError::PrivateType, span.clone()))
                        }
                        _ => Ok(*def_id),
                    },
                    None => Err((CompileError::TypeNotFound, span.clone())),
                }
            }
        }
    }

    fn define_anonymous_type(&mut self, type_def: TypeDef<'m>, span: &Span) -> DefReference {
        let anonymous_def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        self.set_def_kind(anonymous_def_id, DefKind::Type(type_def), span);
        DefReference {
            def_id: anonymous_def_id,
            pattern_bindings: Default::default(),
        }
    }

    fn define_relation_if_undefined(&mut self, key: RelationKey) -> ImplicitRelationId {
        match key {
            RelationKey::Named(def_ref) => {
                match self.compiler.relations.relations.entry(def_ref.def_id) {
                    Entry::Vacant(vacant) => ImplicitRelationId::New(
                        RelationKind::Named(def_ref),
                        *vacant.insert(RelationId(
                            self.compiler.defs.alloc_def_id(self.src.package_id),
                        )),
                    ),
                    Entry::Occupied(occupied) => ImplicitRelationId::Reused(*occupied.get()),
                }
            }
            RelationKey::FmtTransition(def_ref, final_state) => {
                match self.compiler.relations.relations.entry(def_ref.def_id) {
                    Entry::Vacant(vacant) => ImplicitRelationId::New(
                        RelationKind::FmtTransition(def_ref, final_state),
                        *vacant.insert(RelationId(
                            self.compiler.defs.alloc_def_id(self.src.package_id),
                        )),
                    ),
                    Entry::Occupied(occupied) => ImplicitRelationId::Reused(*occupied.get()),
                }
            }
            RelationKey::Builtin(def_id) => ImplicitRelationId::Reused(RelationId(def_id)),
            RelationKey::Indexed => {
                ImplicitRelationId::Reused(RelationId(self.compiler.primitives.indexed_relation))
            }
        }
    }

    fn named_def_id(&mut self, space: Space, ident: &String) -> Result<DefId, DefId> {
        let def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        match self
            .compiler
            .namespaces
            .get_namespace_mut(self.src.package_id, space)
            .insert(ident.clone(), def_id)
        {
            Some(old_def_id) => Err(old_def_id),
            None => Ok(def_id),
        }
    }

    fn define(&mut self, kind: DefKind<'m>, span: &Span) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        self.set_def_kind(def_id, kind, span);
        def_id
    }

    fn set_def_kind(&mut self, def_id: DefId, kind: DefKind<'m>, span: &Span) {
        self.compiler.defs.map.insert(
            def_id,
            self.compiler.defs.mem.bump.alloc(Def {
                id: def_id,
                package: self.src.package_id,
                kind,
                span: self.src.span(span),
            }),
        );
    }

    fn expr(&mut self, kind: ExprKind, span: &Span) -> Expr {
        Expr {
            id: self.compiler.expressions.alloc_expr_id(),
            kind,
            span: self.src.span(span),
        }
    }
}

enum RelationKey {
    Named(DefReference),
    FmtTransition(DefReference, FmtFinalState),
    Builtin(DefId),
    Indexed,
}

#[derive(Clone)]
enum BlockContext<'a> {
    NoContext,
    Context(&'a dyn Fn() -> DefReference),
}

enum ImplicitRelationId {
    New(RelationKind, RelationId),
    Reused(RelationId),
}

#[derive(Default)]
struct ExprVarTable {
    variables: HashMap<String, ExprId>,
}

impl ExprVarTable {
    fn get_or_create_var_id(&mut self, ident: String, compiler: &mut Compiler) -> ExprId {
        *self
            .variables
            .entry(ident)
            .or_insert_with(|| compiler.expressions.alloc_expr_id())
    }
}

fn convert_cardinality(
    ast_cardinality: ast::Cardinality,
) -> (PropertyCardinality, ValueCardinality) {
    match ast_cardinality {
        ast::Cardinality::Optional => (PropertyCardinality::Optional, ValueCardinality::One),
        ast::Cardinality::Many => (PropertyCardinality::Mandatory, ValueCardinality::Many),
        ast::Cardinality::OptionalMany => (PropertyCardinality::Optional, ValueCardinality::Many),
    }
}
