use std::{collections::HashMap, ops::Range};

use indexmap::map::Entry;
use ontol_parser::{ast, Span};
use ontol_runtime::{
    ontology::{PropertyCardinality, ValueCardinality},
    smart_format, DefId, RelationshipId,
};
use regex_syntax::hir::{GroupKind, Hir, HirKind};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    def::{Def, DefKind, FmtFinalState, RelParams, Relationship, TypeDef},
    error::CompileError,
    expr::{
        Expr, ExprId, ExprKind, ExprRegex, ExprSeqElement, ExprStructAttr, ExprStructModifier,
        TypePath,
    },
    namespace::Space,
    package::{PackageReference, ONTOL_PKG},
    CompileErrors, Compiler, Src,
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
                self.root_defs.extend(root_defs);
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
            ast::Statement::Def(def_stmt) => {
                let ident_span = def_stmt.ident.1.clone();
                self.named_definition(def_stmt, ident_span)
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
                let var_allocator = var_table.into_allocator();

                Ok([self.define(DefKind::Mapping(var_allocator, first, second), &span)].into())
            }
        }
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
        let subject_def_id = self.resolve_contextual_type_reference(
            subject,
            subject_span.clone(),
            &block_context,
            Some(&mut root_defs),
        )?;
        let object_def_id = match object {
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
                self.compiler.expressions.table.insert(expr_id, expr);

                self.define(DefKind::Constant(expr_id), &object_span)
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
                (subject_def_id, &subject_span),
                relation,
                (object_def_id, &object_span),
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
        subject: (DefId, &Span),
        ast_relation: ast::Relation,
        object: (DefId, &Span),
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
                    let def_id = self.resolve_type_reference(ty, &span, Some(&mut root_defs))?;

                    match self.compiler.defs.def_kind(def_id) {
                        DefKind::StringLiteral(_) => {
                            (RelationKey::Named(def_id), span.clone(), None)
                        }
                        DefKind::BuiltinRelType(..) => {
                            (RelationKey::Builtin(def_id), span.clone(), None)
                        }
                        _ => return Err((CompileError::InvalidRelationType, span)),
                    }
                }
                ast::RelType::IntRange((range, span)) => (RelationKey::Indexed, span, Some(range)),
            };

        let has_object_prop = object_prop_ident.is_some();

        // This syntax just defines the relation the first time it's used
        let relation_def_id = self.define_relation_if_undefined(key, &ident_span);

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
            let rel_def_id = self.define_anonymous(
                TypeDef {
                    public: false,
                    ident: None,
                    rel_type_for: Some(RelationshipId(relationship_id)),
                    concrete: true,
                },
                &span,
            );
            let context_fn = || rel_def_id;

            root_defs.push(rel_def_id);

            // This type needs to be part of the anonymous part of the namespace
            self.compiler
                .namespaces
                .add_anonymous(self.src.package_id, rel_def_id);

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

            RelParams::Type(rel_def_id)
        } else {
            RelParams::Unit
        };

        let object_prop = object_prop_ident.map(|ident| self.compiler.strings.intern(&ident.0));

        let mut relationship = Relationship {
            relation_def_id,
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
            object_prop,
            rel_params,
        };

        // HACK(for now): invert relationship
        if relation_def_id == self.compiler.primitives.relations.id {
            relationship = Relationship {
                relation_def_id: self.compiler.primitives.relations.identifies,
                subject: relationship.object,
                subject_cardinality: relationship.object_cardinality,
                object: relationship.subject,
                object_cardinality: relationship.subject_cardinality,
                object_prop: None,
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
        let mut origin_def_id =
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

            let target_def_id = self.define_anonymous(
                TypeDef {
                    public: false,
                    ident: None,
                    rel_type_for: None,
                    concrete: true,
                },
                &span,
            );

            root_defs.push(self.ast_fmt_transition_to_def(
                (origin_def_id, &origin_span),
                (transition, span.clone()),
                (target_def_id, &span),
                FmtFinalState(false),
                span.clone(),
            )?);

            transition = next_transition;
            transition_span = span.clone();
            origin_def_id = target_def_id;
            origin_span = span;
        };

        let end_def = self.resolve_contextual_type_reference(
            target.0,
            target.1.clone(),
            &block_context,
            Some(&mut root_defs),
        )?;

        root_defs.push(self.ast_fmt_transition_to_def(
            (origin_def_id, &origin_span),
            (transition, transition_span),
            (end_def, &target.1),
            FmtFinalState(true),
            span,
        )?);

        Ok(root_defs)
    }

    fn ast_fmt_transition_to_def(
        &mut self,
        from: (DefId, &Span),
        transition: (ast::Type, Span),
        to: (DefId, &Span),
        final_state: FmtFinalState,
        span: Span,
    ) -> Res<DefId> {
        let transition_def = self.resolve_type_reference(transition.0, &transition.1, None)?;
        let relation_key = RelationKey::FmtTransition(transition_def, final_state);

        // This syntax just defines the relation the first time it's used
        let relation_def_id = self.define_relation_if_undefined(relation_key, &transition.1);

        debug!("{:?}: <transition>", relation_def_id.0);

        Ok(self.define(
            DefKind::Relationship(Relationship {
                relation_def_id,
                subject: (from.0, self.src.span(from.1)),
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
                object: (to.0, self.src.span(to.1)),
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
                object_prop: None,
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
    ) -> Res<DefId> {
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
    ) -> Res<DefId> {
        match ast_ty {
            ast::Type::Unit => Ok(self.compiler.primitives.unit),
            ast::Type::Path(path) => Ok(self.lookup_path(&path, span)?),
            ast::Type::AnonymousStruct((ctx_block, _block_span)) => {
                if let Some(root_defs) = root_defs {
                    let def_id = self.define_anonymous(
                        TypeDef {
                            public: false,
                            ident: None,
                            rel_type_for: None,
                            concrete: true,
                        },
                        span,
                    );

                    // This type needs to be part of the anonymous part of the namespace
                    self.compiler
                        .namespaces
                        .add_anonymous(self.src.package_id, def_id);
                    root_defs.push(def_id);

                    let context_fn = || def_id;

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

                    Ok(def_id)
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
                    ONTOL_PKG,
                    self.src.span(span),
                );
                Ok(def_id)
            }
            ast::Type::StringLiteral(lit) => {
                let def_id = match lit.as_str() {
                    "" => self.compiler.primitives.empty_string,
                    _ => self
                        .compiler
                        .defs
                        .def_string_literal(&lit, &mut self.compiler.strings),
                };
                Ok(def_id)
            }
            ast::Type::Regex(lit) => {
                let def_id = self
                    .compiler
                    .defs
                    .def_regex(&lit, span, &mut self.compiler.strings)
                    .map_err(|(compile_error, err_span)| {
                        (CompileError::InvalidRegex(compile_error), err_span)
                    })?;
                Ok(def_id)
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
                self.expr(
                    ExprKind::Seq(
                        seq_id,
                        vec![ExprSeqElement {
                            iter: true,
                            expr: unit_expr,
                        }],
                    ),
                    &span,
                )
            }
        };

        let expr_id = self.compiler.expressions.alloc_expr_id();
        self.compiler.expressions.table.insert(expr_id, expr);

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
        let key = (DefId::unit(), self.src.span(&span));
        let expr = self.lower_expr_pattern((expr.0, expr.1), var_table)?;

        Ok(self.expr(
            // FIXME: This ExprKind shouldn't really be struct..
            ExprKind::Struct {
                type_path: Some(TypePath {
                    def_id: type_def_id,
                    span: self.src.span(&path.1),
                }),
                modifier: None,
                attributes: [ExprStructAttr {
                    key,
                    rel: None,
                    bind_option: false,
                    value: expr,
                }]
                .into(),
            },
            &span,
        ))
    }

    fn lower_struct_pattern(
        &mut self,
        (struct_pat, span): (ast::StructPattern, Span),
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        let type_def_id = self.lookup_path(&struct_pat.path.0, &struct_pat.path.1)?;
        let attrs = self.lower_struct_pattern_attrs(struct_pat.attributes, var_table)?;

        Ok(self.expr(
            ExprKind::Struct {
                type_path: Some(TypePath {
                    def_id: type_def_id,
                    span: self.src.span(&struct_pat.path.1),
                }),
                modifier: struct_pat.modifier.map(|(modifier, _span)| match modifier {
                    ast::StructPatternModifier::Match => ExprStructModifier::Match,
                }),
                attributes: attrs,
            },
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

                let object_expr = self.lower_pattern((object, object_span), var_table);
                let rel = match relation_attrs {
                    Some((attrs, span)) => {
                        // Inherit modifier from object expr
                        let modifier = match &object_expr {
                            Ok(Expr {
                                kind: ExprKind::Struct { modifier, .. },
                                ..
                            }) => *modifier,
                            _ => None,
                        };

                        let attrs = self.lower_struct_pattern_attrs(attrs, var_table)?;
                        Some(self.expr(
                            ExprKind::Struct {
                                type_path: None,
                                modifier,
                                attributes: attrs,
                            },
                            &span,
                        ))
                    }
                    None => None,
                };

                object_expr.map(|object_expr| ExprStructAttr {
                    key: (def, self.src.span(&relation.1)),
                    rel,
                    bind_option: option.is_some(),
                    value: object_expr,
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
            ast::Pattern::Expr((expr_pat, _)) => {
                self.lower_expr_pattern((expr_pat, span), var_table)
            }
            ast::Pattern::Struct((struct_pat, span)) => {
                self.lower_struct_pattern((struct_pat, span), var_table)
            }
            ast::Pattern::Seq(elements) => {
                if elements.is_empty() {
                    return Err((
                        CompileError::TODO(smart_format!("requires at least one element")),
                        span.clone(),
                    ));
                }

                let mut expr_elements = Vec::with_capacity(elements.len());
                for (element, _element_span) in elements {
                    let expr =
                        self.lower_pattern((element.pattern.0, element.pattern.1), var_table)?;
                    expr_elements.push(ExprSeqElement {
                        iter: element.spread.is_some(),
                        expr,
                    })
                }

                let seq_id = self.compiler.expressions.alloc_expr_id();
                Ok(self.expr(ExprKind::Seq(seq_id, expr_elements), &span))
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
                Ok(self.expr(ExprKind::ConstI64(int), &span))
            }
            ast::ExprPattern::StringLiteral(string) => {
                Ok(self.expr(ExprKind::ConstString(string), &span))
            }
            ast::ExprPattern::RegexLiteral(regex_literal) => {
                let def_id = self
                    .compiler
                    .defs
                    .def_regex(&regex_literal, &span, &mut self.compiler.strings)
                    .map_err(|(compile_error, err_span)| {
                        (CompileError::InvalidRegex(compile_error), err_span)
                    })?;
                let regex_hir = self.compiler.defs.literal_regex_hirs.get(&def_id).unwrap();
                let mut regex_lowering = RegexLowering {
                    output: ExprRegex {
                        regex_def_id: def_id,
                        captures: Default::default(),
                    },
                    regex_span: &span,
                    src: self.src,
                    errors: &mut self.compiler.errors,
                    var_table,
                };
                regex_lowering.analyze_expr_regex(regex_hir)?;
                let expr_regex = regex_lowering.output;

                Ok(self.expr(ExprKind::Regex(expr_regex), &span))
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
        }
    }

    fn lower_expr_variable(
        &mut self,
        var_ident: String,
        span: Span,
        var_table: &mut ExprVarTable,
    ) -> Res<Expr> {
        let var = var_table.get_or_create_var(var_ident);
        Ok(self.expr(ExprKind::Variable(var), &span))
    }

    fn lookup_ident(&mut self, ident: &str, span: &Span) -> Result<DefId, LoweringError> {
        // A single ident looks in both ONTOL_PKG and the current package
        match self
            .compiler
            .namespaces
            .lookup(&[self.src.package_id, ONTOL_PKG], Space::Type, ident)
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
                            Some(def_id) => match self.compiler.defs.def_kind(*def_id) {
                                DefKind::Package(package_id) => {
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
                    Some(def_id) => match self.compiler.defs.def_kind(*def_id) {
                        DefKind::Type(TypeDef { public: false, .. }) => {
                            Err((CompileError::PrivateDefinition, span.clone()))
                        }
                        _ => Ok(*def_id),
                    },
                    None => Err((CompileError::TypeNotFound, span.clone())),
                }
            }
        }
    }

    fn named_definition(&mut self, def_stmt: ast::DefStatement, span: Span) -> Res<RootDefs> {
        let (def_id, coinage) = self.named_def_id(Space::Type, &def_stmt.ident.0, &span)?;
        if matches!(coinage, Coinage::New) {
            let ident = self.compiler.strings.intern(&def_stmt.ident.0);
            debug!("{def_id:?}: `{}`", def_stmt.ident.0);

            self.set_def_kind(
                def_id,
                DefKind::Type(TypeDef {
                    public: matches!(def_stmt.visibility.0, ast::Visibility::Public),
                    ident: Some(ident),
                    rel_type_for: None,
                    concrete: true,
                }),
                &span,
            );
        }

        let mut root_defs: RootDefs = [def_id].into();

        self.compiler
            .namespaces
            .docs
            .entry(def_id)
            .or_default()
            .extend(def_stmt.docs);

        {
            // The inherent relation block on the type uses the just defined
            // type as its context
            let context_fn = move || def_id;

            for spanned_stmt in def_stmt.block.0 {
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

    fn define_anonymous(&mut self, type_def: TypeDef<'m>, span: &Span) -> DefId {
        let anonymous_def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        self.set_def_kind(anonymous_def_id, DefKind::Type(type_def), span);
        debug!("{anonymous_def_id:?}: <anonymous>");
        anonymous_def_id
    }

    fn define_relation_if_undefined(&mut self, key: RelationKey, span: &Range<usize>) -> DefId {
        match key {
            RelationKey::Named(def_id) => def_id,
            RelationKey::FmtTransition(def_ref, final_state) => {
                let relation_def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
                self.set_def_kind(
                    relation_def_id,
                    DefKind::FmtTransition(def_ref, final_state),
                    span,
                );

                relation_def_id
            }
            RelationKey::Builtin(def_id) => def_id,
            RelationKey::Indexed => self.compiler.primitives.relations.indexed,
        }
    }

    fn named_def_id(&mut self, space: Space, ident: &String, span: &Span) -> Res<(DefId, Coinage)> {
        match self
            .compiler
            .namespaces
            .get_namespace_mut(self.src.package_id, space)
            .entry(ident.clone())
        {
            Entry::Occupied(occupied) => {
                if occupied.get().package_id() == self.src.package_id {
                    Ok((*occupied.get(), Coinage::Used))
                } else {
                    Err((
                        CompileError::TODO(smart_format!("definition of external identifier")),
                        span.clone(),
                    ))
                }
            }
            Entry::Vacant(vacant) => {
                let def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
                vacant.insert(def_id);
                Ok((def_id, Coinage::New))
            }
        }
    }

    fn define(&mut self, kind: DefKind<'m>, span: &Span) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        self.set_def_kind(def_id, kind, span);
        def_id
    }

    fn set_def_kind(&mut self, def_id: DefId, kind: DefKind<'m>, span: &Span) {
        self.compiler.defs.table.insert(
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

enum Coinage {
    New,
    Used,
}

enum RelationKey {
    Named(DefId),
    FmtTransition(DefId, FmtFinalState),
    Builtin(DefId),
    Indexed,
}

#[derive(Clone)]
enum BlockContext<'a> {
    NoContext,
    Context(&'a dyn Fn() -> DefId),
}

#[derive(Default)]
struct ExprVarTable {
    variables: HashMap<String, ontol_hir::Var>,
}

impl ExprVarTable {
    fn get_or_create_var(&mut self, ident: String) -> ontol_hir::Var {
        let length = self.variables.len();

        *self
            .variables
            .entry(ident)
            .or_insert_with(|| ontol_hir::Var(length as u32))
    }

    /// Create an allocator for allocating the successive variables
    /// after the explicit ones
    fn into_allocator(self) -> ontol_hir::VarAllocator {
        ontol_hir::VarAllocator::from(ontol_hir::Var(self.variables.len() as u32))
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

pub struct RegexLowering<'s> {
    output: ExprRegex,
    regex_span: &'s Span,
    src: &'s Src,
    errors: &'s mut CompileErrors,
    var_table: &'s mut ExprVarTable,
}

impl<'s> RegexLowering<'s> {
    fn analyze_expr_regex(&mut self, hir: &Hir) -> Res<()> {
        match hir.kind() {
            HirKind::Empty => {}
            HirKind::Literal(_) => {}
            HirKind::Class(_) => {}
            HirKind::Anchor(_) => {}
            HirKind::WordBoundary(_) => {}
            HirKind::Group(group) => match &group.kind {
                GroupKind::CaptureName { name, index: _ } => {
                    let var = self.var_table.get_or_create_var(name.into());
                    self.output.captures.insert(var, name.into());
                }
                GroupKind::CaptureIndex(_) => {
                    // Found no way to extract spans from regex-syntax/Hir, they apparently
                    // disappear in the AST => HIR translation
                    self.errors.push(
                        CompileError::TODO(smart_format!("Capture group must have a name"))
                            .spanned(&self.src.span(self.regex_span)),
                    );
                }
                GroupKind::NonCapturing => {}
            },
            HirKind::Repetition(rep) => {
                self.analyze_expr_regex(&rep.hir)?;
            }
            HirKind::Concat(hirs) => {
                for hir in hirs {
                    self.analyze_expr_regex(hir)?;
                }
            }
            HirKind::Alternation(hirs) => {
                for hir in hirs {
                    self.analyze_expr_regex(hir)?;
                }
            }
        }

        Ok(())
    }
}
