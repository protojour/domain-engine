use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Range,
};

use fnv::FnvHashMap;
use ontol_parser::{ast, Span, Spanned};
use ontol_runtime::{DefId, DefParamId, RelationId};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    def::{
        Def, DefKind, DefParamBinding, DefReference, PropertyCardinality, RelParams, Relation,
        RelationKind, Relationship, TypeDef, TypeDefParam, ValueCardinality, Variables,
    },
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, TypePath},
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
                let def = self.resolve_type_reference(with_stmt.ty.0, &with_stmt.ty.1)?;
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
                variables: ast_variables,
                first,
                second,
            }) => {
                let mut var_table = ExprVarTable::default();
                let mut variables = vec![];
                for (param, span) in ast_variables.into_iter() {
                    variables.push((
                        var_table.new_var_id(param, self.compiler),
                        self.src.span(&span),
                    ));
                }

                let first = self.lower_struct_pattern_root(first, &mut var_table)?;
                let second = self.lower_struct_pattern_root(second, &mut var_table)?;

                Ok([self.define(
                    DefKind::Mapping(Variables(variables.into()), first, second),
                    &span,
                )]
                .into())
            }
        }
    }

    fn define_type(&mut self, type_stmt: ast::TypeStatement, span: Span) -> Res<RootDefs> {
        let def_id = match self.named_def_id(Space::Type, &type_stmt.ident.0) {
            Ok(def_id) => def_id,
            Err(_) => return Err((CompileError::DuplicateTypeDefinition, type_stmt.ident.1)),
        };
        let ident = self.compiler.strings.intern(&type_stmt.ident.0);
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
            }),
            &span,
        );

        let mut root_defs: RootDefs = [def_id].into();

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
            docs: _,
            kw: _,
            subject: (subject, subject_span),
            relation,
            object: (object, object_span),
            ctx_block,
        } = rel;

        let subject_def =
            self.resolve_contextual_type_reference(subject, subject_span.clone(), &block_context)?;
        let object_def =
            self.resolve_contextual_type_reference(object, object_span.clone(), &block_context)?;

        self.def_relationship(
            (subject_def, &subject_span),
            relation,
            (object_def, &object_span),
            span,
            ctx_block,
        )
    }

    fn def_relationship(
        &mut self,
        subject: (DefReference, &Span),
        ast_relation: ast::Relation,
        object: (DefReference, &Span),
        span: Span,
        ctx_block: Option<Spanned<Vec<Spanned<ast::Statement>>>>,
    ) -> Res<RootDefs> {
        let mut root_defs = RootDefs::new();
        let ast::Relation {
            ty: relation_ty,
            subject_cardinality,
            object_prop_ident,
            object_cardinality,
        } = ast_relation;

        let (kind, ident_span, index_range_rel_params): (_, _, Option<Range<Option<u16>>>) =
            match relation_ty {
                ast::RelType::Type((ty, span)) => {
                    let def = self.resolve_type_reference(ty, &span)?;

                    match self.compiler.defs.get_def_kind(def.def_id) {
                        Some(DefKind::StringLiteral(_)) => {
                            (RelationKind::Named(def), span.clone(), None)
                        }
                        Some(DefKind::Relation(relation)) => {
                            (relation.kind.clone(), span.clone(), None)
                        }
                        _ => return Err((CompileError::InvalidRelationType, span)),
                    }
                }
                ast::RelType::IntRange((range, span)) => (RelationKind::Indexed, span, Some(range)),
            };

        let has_object_prop = object_prop_ident.is_some();

        // This syntax just defines the relation the first time it's used
        let relation_id = match self.define_relation_if_undefined(&kind) {
            ImplicitRelationId::New(relation_id) => {
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

        let rel_params = if let Some(index_range_rel_params) = index_range_rel_params {
            if let Some((_, span)) = ctx_block {
                return Err((
                    CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes,
                    span,
                ));
            }

            RelParams::IndexRange(index_range_rel_params)
        } else if let Some((ctx_block, _)) = ctx_block {
            let rel_def = self.define_anonymous_type(&span);
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

        debug!("define relationship {relation_id:?}");

        let relationship = Relationship {
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

        root_defs.push(self.define(DefKind::Relationship(relationship), &span));

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
        let mut origin_def = self.resolve_type_reference(origin, &origin_span)?;
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

            let target_def = self.define_anonymous_type(&span);

            root_defs.push(self.ast_fmt_transition_to_def(
                (origin_def, &origin_span),
                (transition, span.clone()),
                (target_def.clone(), &span),
                span.clone(),
            )?);

            transition = next_transition;
            transition_span = span.clone();
            origin_def = target_def;
            origin_span = span;
        };

        let end_def =
            self.resolve_contextual_type_reference(target.0, target.1.clone(), &block_context)?;

        root_defs.push(self.ast_fmt_transition_to_def(
            (origin_def, &origin_span),
            (transition, transition_span),
            (end_def, &target.1),
            span,
        )?);

        Ok(root_defs)
    }

    fn ast_fmt_transition_to_def(
        &mut self,
        from: (DefReference, &Span),
        transition: (ast::Type, Span),
        to: (DefReference, &Span),
        span: Span,
    ) -> Res<DefId> {
        let transition_def = self.resolve_type_reference(transition.0, &transition.1)?;
        let kind = RelationKind::FmtTransition(transition_def);

        // This syntax just defines the relation the first time it's used
        let relation_id = match self.define_relation_if_undefined(&kind) {
            ImplicitRelationId::New(relation_id) => {
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
    ) -> Res<DefReference> {
        match (ast_ty, block_context) {
            (Some(ast_ty), _) => self.resolve_type_reference(ast_ty, &span),
            (None, BlockContext::Context(func)) => Ok(func()),
            _ => Err((CompileError::WildcardNeedsContextualBlock, span)),
        }
    }

    fn resolve_type_reference(&mut self, ast_ty: ast::Type, span: &Span) -> Res<DefReference> {
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
            _ => Err((CompileError::InvalidType, span.clone())),
            // ast::Type::EmptySequence => Ok(self.compiler.defs.empty_sequence()),
            // ast::Type::Literal(_) => Err(self.error(CompileError::InvalidType, span)),
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
                                match self.resolve_type_reference(ty, &ty_span) {
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

    fn lower_struct_pattern_root(
        &mut self,
        (unit_or_seq, span): (ast::UnitOrSeq<ast::StructPattern>, Span),
        var_table: &mut ExprVarTable,
    ) -> Res<ExprId> {
        let expr = match unit_or_seq {
            ast::UnitOrSeq::Unit(ast) => self.lower_struct_pattern((ast, span), var_table)?,
            ast::UnitOrSeq::Seq(ast) => {
                let seq_id = var_table.new_aggregation_group();
                let inner = self.lower_struct_pattern((ast, span.clone()), var_table)?;
                self.expr(ExprKind::Seq(seq_id, Box::new(inner)), &span)
            }
        };

        let expr_id = self.compiler.defs.alloc_expr_id();
        self.compiler.expressions.insert(expr_id, expr);

        Ok(expr_id)
    }

    fn lower_struct_pattern(
        &mut self,
        (struct_pat, span): (ast::StructPattern, Span),
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        let type_def_id = self.lookup_path(&struct_pat.path.0, &struct_pat.path.1)?;
        let attributes = struct_pat
            .attributes
            .into_iter()
            .map(|struct_attr| match struct_attr {
                ast::StructPatternAttr::Expr((expr, expr_span)) => {
                    let key = (
                        DefReference {
                            def_id: DefId::unit(),
                            pattern_bindings: Default::default(),
                        },
                        self.src.span(&expr_span),
                    );
                    let expr = self.lower_expr_pattern((expr, expr_span), var_table)?;

                    Ok((key, expr))
                }
                ast::StructPatternAttr::Rel((
                    ast::StructPatternAttrRel {
                        relation,
                        object: (object, object_span),
                        ..
                    },
                    _span,
                )) => {
                    let def = self.resolve_type_reference(relation.0, &relation.1)?;

                    let inner_expr = self.lower_pattern((object, object_span), var_table);

                    inner_expr.map(|expr| ((def, self.src.span(&relation.1)), expr))
                }
            })
            .collect::<Result<_, _>>()?;

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

    fn lower_pattern(
        &mut self,
        (pattern, span): (ast::Pattern, Span),
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        match pattern {
            ast::Pattern::Expr((ast::UnitOrSeq::Unit(expr_pat), _)) => {
                self.lower_expr_pattern((expr_pat, span), var_table)
            }
            ast::Pattern::Expr((ast::UnitOrSeq::Seq(expr_pat), _)) => {
                let seq_id = var_table.new_aggregation_group();
                let inner = self.lower_expr_pattern((expr_pat, span.clone()), var_table)?;
                Ok(self.expr(ExprKind::Seq(seq_id, Box::new(inner)), &span))
            }
            ast::Pattern::Struct((ast::UnitOrSeq::Unit(struct_pat), span)) => {
                self.lower_struct_pattern((struct_pat, span), var_table)
            }
            ast::Pattern::Struct((ast::UnitOrSeq::Seq(struct_pat), span)) => {
                let seq_id = var_table.new_aggregation_group();
                let inner = self.lower_struct_pattern((struct_pat, span.clone()), var_table)?;
                Ok(self.expr(ExprKind::Seq(seq_id, Box::new(inner)), &span))
            }
        }
    }

    fn lower_expr_pattern(
        &mut self,
        (expr_pat, span): (ast::ExprPattern, Span),
        var_table: &ExprVarTable,
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
                let id = var_table
                    .get_var_id(var_ident.as_str())
                    .ok_or_else(|| (CompileError::UndeclaredVariable, span.clone()))?;
                Ok(self.expr(ExprKind::Variable(id), &span))
            }
            _ => Err((CompileError::InvalidExpression, span)),
        }
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

    fn define_anonymous_type(&mut self, span: &Span) -> DefReference {
        let anonymous_def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        self.set_def_kind(
            anonymous_def_id,
            DefKind::Type(TypeDef {
                public: false,
                ident: None,
                params: None,
            }),
            span,
        );
        DefReference {
            def_id: anonymous_def_id,
            pattern_bindings: Default::default(),
        }
    }

    fn define_relation_if_undefined(&mut self, kind: &RelationKind) -> ImplicitRelationId {
        match kind {
            RelationKind::Named(def) | RelationKind::FmtTransition(def) => {
                match self.compiler.relations.relations.entry(def.def_id) {
                    Entry::Vacant(vacant) => ImplicitRelationId::New(*vacant.insert(RelationId(
                        self.compiler.defs.alloc_def_id(self.src.package_id),
                    ))),
                    Entry::Occupied(occupied) => ImplicitRelationId::Reused(*occupied.get()),
                }
            }
            RelationKind::Is => {
                ImplicitRelationId::Reused(RelationId(self.compiler.primitives.is_relation))
            }
            RelationKind::Identifies => {
                ImplicitRelationId::Reused(RelationId(self.compiler.primitives.identifies_relation))
            }
            RelationKind::Indexed => {
                ImplicitRelationId::Reused(RelationId(self.compiler.primitives.indexed_relation))
            }
            RelationKind::Route => {
                ImplicitRelationId::Reused(RelationId(self.compiler.primitives.route_relation))
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

    fn expr(&self, kind: ExprKind, span: &Span) -> Expr {
        Expr {
            id: ExprId(0),
            kind,
            span: self.src.span(span),
        }
    }
}

#[derive(Clone)]
enum BlockContext<'a> {
    NoContext,
    Context(&'a dyn Fn() -> DefReference),
}

enum ImplicitRelationId {
    New(RelationId),
    Reused(RelationId),
}

#[derive(Default)]
struct ExprVarTable {
    variables: HashMap<String, ExprId>,
    aggregation_counter: u32,
}

impl ExprVarTable {
    fn new_var_id(&mut self, ident: String, compiler: &mut Compiler) -> ExprId {
        *self
            .variables
            .entry(ident)
            .or_insert_with(|| compiler.defs.alloc_expr_id())
    }

    fn new_aggregation_group(&mut self) -> ExprId {
        let id = ExprId(self.aggregation_counter);
        self.aggregation_counter += 1;
        id
    }

    fn get_var_id(&self, ident: &str) -> Option<ExprId> {
        self.variables.get(ident).cloned()
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
