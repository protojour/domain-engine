use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use chumsky::chain::Chain;
use either::Either;
use indexmap::map::Entry;
use ontol_parser::{ast, Span};
use ontol_runtime::{
    ontology::{PropertyCardinality, ValueCardinality},
    smart_format,
    var::{Var, VarAllocator},
    DefId, RelationshipId,
};

use smartstring::alias::String;
use tracing::{debug, debug_span};

use crate::{
    def::{Def, DefKind, DefVisibility, FmtFinalState, RelParams, Relationship, TypeDef},
    error::CompileError,
    namespace::Space,
    package::{PackageReference, ONTOL_PKG},
    pattern::{
        CompoundPatternAttr, CompoundPatternAttrKind, CompoundPatternModifier, PatId, Pattern,
        PatternKind, SetBinaryOperator, SetPatternElement, TypePath,
    },
    regex_util::RegexToPatternLowerer,
    Compiler, Src,
};

pub struct Lowering<'s, 'm> {
    compiler: &'s mut Compiler<'m>,
    src: &'s Src,
    root_defs: Vec<DefId>,
    map_defs: HashSet<DefId>,
}

pub struct Lowered {
    pub root_defs: Vec<DefId>,
    pub map_defs: HashSet<DefId>,
}

type LoweringError = (CompileError, Span);

type Res<T> = Result<T, LoweringError>;
type RootDefs = Vec<DefId>;

struct Open(Option<Span>);
struct Private(Option<Span>);

/// Statement after scanning once
enum PreDefinedStmt {
    Def(PreDefinedDefStmt),
    Rel(ast::RelStatement),
    Fmt(ast::FmtStatement),
    Map(ast::MapStatement),
}

struct PreDefinedDefStmt {
    def_id: DefId,
    docs: Vec<String>,
    block: (Vec<(ast::Statement, Span)>, Span),
}

impl<'s, 'm> Lowering<'s, 'm> {
    pub fn new(compiler: &'s mut Compiler<'m>, src: &'s Src) -> Self {
        Self {
            compiler,
            src,
            root_defs: Default::default(),
            map_defs: Default::default(),
        }
    }

    pub fn finish(self) -> Lowered {
        Lowered {
            root_defs: self.root_defs,
            map_defs: self.map_defs,
        }
    }

    pub fn lower_statements(mut self, statements: Vec<(ast::Statement, Span)>) -> Self {
        let mut pre_defined_statements = vec![];

        // first pass: Register all named definitions into the namespace
        for (stmt, span) in statements {
            match self.pre_define_statement((stmt, span)) {
                Ok(Some(pre_defined)) => {
                    pre_defined_statements.push(pre_defined);
                }
                Ok(None) => {}
                Err(error) => {
                    self.report_error(error);
                }
            }
        }

        // second pass: Handle inner bodies, etc
        for (stmt, span) in pre_defined_statements {
            match self.pre_defined_stmt_to_def((stmt, span), BlockContext::NoContext) {
                Ok(root_defs) => {
                    self.root_defs.extend(root_defs);
                }
                Err(error) => {
                    self.report_error(error);
                }
            }
        }

        self
    }

    fn pre_define_statement(
        &mut self,
        (stmt, span): (ast::Statement, Span),
    ) -> Res<Option<(PreDefinedStmt, Span)>> {
        match stmt {
            ast::Statement::Use(use_stmt) => {
                let reference = PackageReference::Named(use_stmt.reference.0);
                let self_package_def_id = self.src.package_id;
                let used_package_def_id = self
                    .compiler
                    .packages
                    .loaded_packages
                    .get(&reference)
                    .ok_or_else(|| {
                        (
                            CompileError::PackageNotFound(reference),
                            use_stmt.reference.1,
                        )
                    })?;

                let type_namespace = self
                    .compiler
                    .namespaces
                    .get_namespace_mut(self_package_def_id, Space::Type);

                let as_ident = self.compiler.strings.intern(&use_stmt.as_ident.0);
                type_namespace.insert(as_ident, *used_package_def_id);

                Ok(None)
            }
            ast::Statement::Def(def_stmt) => {
                let def_id = self.coin_type_definition(
                    def_stmt.ident.0,
                    &def_stmt.ident.1,
                    Private(def_stmt.private),
                    Open(def_stmt.open),
                )?;

                Ok(Some((
                    PreDefinedStmt::Def(PreDefinedDefStmt {
                        def_id,
                        docs: def_stmt.docs,
                        block: def_stmt.block,
                    }),
                    span,
                )))
            }
            ast::Statement::Rel(rel_stmt) => Ok(Some((PreDefinedStmt::Rel(rel_stmt), span))),
            ast::Statement::Fmt(fmt_stmt) => Ok(Some((PreDefinedStmt::Fmt(fmt_stmt), span))),
            ast::Statement::Map(map_stmt) => Ok(Some((PreDefinedStmt::Map(map_stmt), span))),
        }
    }

    fn report_error(&mut self, (error, span): (CompileError, Span)) {
        self.compiler
            .push_error(error.spanned(&self.src.span(&span)));
    }

    fn pre_defined_stmt_to_def(
        &mut self,
        (stmt, span): (PreDefinedStmt, Span),
        block_context: BlockContext,
    ) -> Res<RootDefs> {
        match stmt {
            PreDefinedStmt::Def(def_stmt) => {
                self.provide_definition(def_stmt.def_id, def_stmt.docs, def_stmt.block)
            }
            PreDefinedStmt::Rel(rel_stmt) => {
                self.ast_relationship_to_def(rel_stmt, span, block_context)
            }
            PreDefinedStmt::Fmt(fmt_stmt) => {
                self.fmt_transitions_to_def(fmt_stmt, span, block_context)
            }
            PreDefinedStmt::Map(map_stmt) => self.map_stmt_to_def((map_stmt, span)),
        }
    }

    fn stmt_to_def(
        &mut self,
        (stmt, span): (ast::Statement, Span),
        block_context: BlockContext,
    ) -> Res<RootDefs> {
        match stmt {
            ast::Statement::Use(_) => Ok(vec![]),
            ast::Statement::Def(def_stmt) => {
                let def_id = self.coin_type_definition(
                    def_stmt.ident.0,
                    &def_stmt.ident.1,
                    Private(def_stmt.private),
                    Open(def_stmt.open),
                )?;
                let mut root_defs: RootDefs = [def_id].into();
                root_defs.extend(self.provide_definition(def_id, def_stmt.docs, def_stmt.block)?);
                Ok(root_defs)
            }
            ast::Statement::Rel(rel_stmt) => {
                self.ast_relationship_to_def(rel_stmt, span, block_context)
            }
            ast::Statement::Fmt(fmt_stmt) => {
                self.fmt_transitions_to_def(fmt_stmt, span, block_context)
            }
            ast::Statement::Map(map_stmt) => self.map_stmt_to_def((map_stmt, span)),
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
            Either::Left(dot) => self.resolve_contextual_type_reference(
                Either::Left(dot),
                object_span.clone(),
                &block_context,
                Some(&mut root_defs),
            )?,
            Either::Right(ast::TypeOrPattern::Type(ty)) => self.resolve_contextual_type_reference(
                Either::Right(ty),
                object_span.clone(),
                &block_context,
                Some(&mut root_defs),
            )?,
            Either::Right(ast::TypeOrPattern::Pattern(ast_pattern)) => {
                let mut var_table = MapVarTable::default();
                let pattern =
                    self.lower_any_pattern((ast_pattern, object_span.clone()), &mut var_table)?;
                let pat_id = self.compiler.patterns.alloc_pat_id();
                self.compiler.patterns.table.insert(pat_id, pattern);

                self.define_anonymous(DefKind::Constant(pat_id), &object_span)
            }
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
                        DefKind::TextLiteral(_) => (RelationKey::Named(def_id), span.clone(), None),
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
            let rel_def_id = self.define_anonymous_type(
                TypeDef {
                    visibility: DefVisibility::Private,
                    open: false,
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

        let mut root_defs = Vec::new();
        let mut origin_def_id =
            self.resolve_type_reference(origin, &origin_span, Some(&mut root_defs))?;
        let mut iter = transitions.into_iter().peekable();

        let (mut transition, mut transition_span) = match iter.next() {
            Some((Either::Left(_), _)) => return Err((CompileError::FmtMisplacedWildcard, span)),
            Some((Either::Right(next), span)) => (next, span),
            None => return Err((CompileError::FmtTooFewTransitions, span)),
        };

        let target = loop {
            let (next_transition, span) = match iter.next() {
                Some(item) if iter.peek().is_none() => {
                    // end of iterator, found the final target. Handle this outside the loop:
                    break item;
                }
                Some((Either::Left(_), _)) => {
                    return Err((CompileError::FmtMisplacedWildcard, span))
                }
                Some((Either::Right(item), span)) => (item, span),
                _ => return Err((CompileError::FmtTooFewTransitions, span)),
            };

            let target_def_id = self.define_anonymous_type(
                TypeDef {
                    visibility: DefVisibility::Private,
                    open: false,
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

        Ok(self.define_anonymous(
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
        ast_ty: Either<ast::Dot, ast::Type>,
        span: Range<usize>,
        block_context: &BlockContext,
        root_defs: Option<&mut RootDefs>,
    ) -> Res<DefId> {
        match (ast_ty, block_context) {
            (Either::Left(_), BlockContext::Context(func)) => Ok(func()),
            (Either::Right(ast_ty), _) => self.resolve_type_reference(ast_ty, &span, root_defs),
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
                    let def_id = self.define_anonymous_type(
                        TypeDef {
                            visibility: DefVisibility::Private,
                            open: false,
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
            ast::Type::TextLiteral(lit) => {
                let def_id = match lit.as_str() {
                    "" => self.compiler.primitives.empty_text,
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

    fn map_stmt_to_def(
        &mut self,
        (
            ast::MapStatement {
                ident,
                first,
                second,
                ..
            },
            span,
        ): (ast::MapStatement, Span),
    ) -> Res<RootDefs> {
        let mut var_table = MapVarTable::default();
        let first = self.lower_map_arm(first, &mut var_table)?;
        let second = self.lower_map_arm(second, &mut var_table)?;
        let var_alloc = var_table.into_allocator();

        let (def_id, ident) = match ident {
            Some((ident, span)) => {
                let (def_id, coinage) = self.named_def_id(Space::Map, &ident, &span)?;
                if matches!(coinage, Coinage::Used) {
                    return Err((CompileError::DuplicateMapIdentifier, span.clone()));
                }
                let ident = self.compiler.strings.intern(&ident);
                (def_id, Some(ident))
            }
            None => (self.compiler.defs.alloc_def_id(self.src.package_id), None),
        };

        self.set_def_kind(
            def_id,
            DefKind::Mapping {
                ident,
                arms: [first, second],
                var_alloc,
            },
            &span,
        );

        self.map_defs.insert(def_id);

        Ok([def_id].into())
    }

    fn lower_map_arm(
        &mut self,
        (ast, span): (ast::MapArm, Span),
        var_table: &mut MapVarTable,
    ) -> Res<PatId> {
        let pattern = match ast {
            ast::MapArm::Struct(ast) => {
                self.lower_struct_pattern((ast, span.clone()), var_table)?
            }
            ast::MapArm::Binding { path, pattern } => match pattern {
                ast::RootBindingPattern::Expr(ast) => {
                    self.lower_map_expr_binding(path, ast, span.clone(), var_table)?
                }
                ast::RootBindingPattern::Set(ast_elements) => {
                    self.lower_set_pattern(Some(path), ast_elements, span, var_table)?
                }
            },
        };

        let pat_id = self.compiler.patterns.alloc_pat_id();
        self.compiler.patterns.table.insert(pat_id, pattern);

        Ok(pat_id)
    }

    fn lower_map_expr_binding(
        &mut self,
        type_path: (ast::Path, Span),
        (ast, expr_span): (ast::ExprPattern, Span),
        span: Span,
        var_table: &mut MapVarTable,
    ) -> Res<Pattern> {
        let type_def_id = self.lookup_path(&type_path.0, &type_path.1)?;
        let key = (DefId::unit(), self.src.span(&span));
        let pattern = self.lower_expr_pattern((ast, expr_span), var_table)?;

        Ok(self.mk_pattern(
            PatternKind::Compound {
                type_path: TypePath::Specified {
                    def_id: type_def_id,
                    span: self.src.span(&type_path.1),
                },
                modifier: None,
                is_unit_binding: true,
                attributes: [CompoundPatternAttr {
                    key,
                    bind_option: false,
                    kind: CompoundPatternAttrKind::Value {
                        rel: None,
                        val: pattern,
                    },
                }]
                .into(),
            },
            &span,
        ))
    }

    fn lower_struct_pattern(
        &mut self,
        (ast, span): (ast::StructPattern, Span),
        var_table: &mut MapVarTable,
    ) -> Res<Pattern> {
        let type_path = match ast.path {
            Some((path, span)) => TypePath::Specified {
                def_id: self.lookup_path(&path, &span)?,
                span: self.src.span(&span),
            },
            None => {
                let def_id = self.define_anonymous_type(
                    TypeDef {
                        visibility: DefVisibility::Public,
                        open: false,
                        ident: None,
                        rel_type_for: None,
                        concrete: true,
                    },
                    &span,
                );
                self.compiler
                    .namespaces
                    .add_anonymous(self.src.package_id, def_id);

                TypePath::Inferred { def_id }
            }
        };
        let attrs = self.lower_struct_pattern_args(ast.args, var_table)?;

        Ok(self.mk_pattern(
            PatternKind::Compound {
                type_path,
                modifier: ast.modifier.map(|(modifier, _span)| match modifier {
                    ast::StructPatternModifier::Match => CompoundPatternModifier::Match,
                }),
                is_unit_binding: false,
                attributes: attrs,
            },
            &span,
        ))
    }

    fn lower_any_pattern(
        &mut self,
        (ast, span): (ast::AnyPattern, Span),
        var_table: &mut MapVarTable,
    ) -> Res<Pattern> {
        match ast {
            ast::AnyPattern::Expr((ast, _)) => self.lower_expr_pattern((ast, span), var_table),
            ast::AnyPattern::Struct((ast, span)) => {
                self.lower_struct_pattern((ast, span), var_table)
            }
            ast::AnyPattern::Set(ast_elements) => {
                self.lower_set_pattern(None, ast_elements, span, var_table)
            }
            ast::AnyPattern::SetAlgebra((_ast, span)) => Err((
                CompileError::TODO(smart_format!("set algebra not supported here")),
                span.clone(),
            )),
        }
    }

    fn lower_struct_pattern_args(
        &mut self,
        attributes: Vec<(ast::StructPatternArgument, Range<usize>)>,
        var_table: &mut MapVarTable,
    ) -> Res<Box<[CompoundPatternAttr]>> {
        attributes
            .into_iter()
            .map(|(struct_arg, span)| match struct_arg {
                ast::StructPatternArgument::Attr(attr) => {
                    self.lower_compound_pattern_attr((attr, span), var_table)
                }
                ast::StructPatternArgument::Spread(_) => todo!(),
            })
            .collect()
    }

    fn lower_struct_pattern_attrs(
        &mut self,
        attributes: Vec<(ast::StructPatternAttr, Range<usize>)>,
        var_table: &mut MapVarTable,
    ) -> Res<Box<[CompoundPatternAttr]>> {
        attributes
            .into_iter()
            .map(|(struct_attr, span)| {
                self.lower_compound_pattern_attr((struct_attr, span), var_table)
            })
            .collect()
    }

    fn lower_compound_pattern_attr(
        &mut self,
        (ast_struct_attr, _span): (ast::StructPatternAttr, Span),
        var_table: &mut MapVarTable,
    ) -> Res<CompoundPatternAttr> {
        let ast::StructPatternAttr {
            relation,
            relation_args: relation_attrs,
            option,
            object: (object, object_span),
        } = ast_struct_attr;

        let def = self.resolve_type_reference(relation.0, &relation.1, None)?;

        match object {
            ast::AnyPattern::SetAlgebra((ast, span)) => {
                if let Some((_, _rel_span)) = relation_attrs {
                    return Err((
                        CompileError::TODO(smart_format!("set algebra not supported here")),
                        span.clone(),
                    ));
                }
                let kind = self.lower_set_algebra_pattern(ast, span, var_table)?;
                Ok(CompoundPatternAttr {
                    key: (def, self.src.span(&relation.1)),
                    bind_option: option.is_some(),
                    kind,
                })
            }
            object => {
                let object_pattern = self.lower_any_pattern((object, object_span), var_table)?;
                let rel = match relation_attrs {
                    Some((attrs, span)) => {
                        // Inherit modifier from object pattern
                        let modifier = match &object_pattern {
                            Pattern {
                                kind: PatternKind::Compound { modifier, .. },
                                ..
                            } => *modifier,
                            _ => None,
                        };

                        let attrs = self.lower_struct_pattern_args(attrs, var_table)?;
                        Some(self.mk_pattern(
                            PatternKind::Compound {
                                type_path: TypePath::RelContextual,
                                modifier,
                                is_unit_binding: false,
                                attributes: attrs,
                            },
                            &span,
                        ))
                    }
                    None => None,
                };

                Ok(CompoundPatternAttr {
                    key: (def, self.src.span(&relation.1)),
                    bind_option: option.is_some(),
                    kind: CompoundPatternAttrKind::Value {
                        rel,
                        val: object_pattern,
                    },
                })
            }
        }
    }

    fn lower_set_pattern(
        &mut self,
        type_path: Option<(ast::Path, Span)>,
        ast_elements: Vec<(ast::SetPatternElement, Span)>,
        span: Span,
        var_table: &mut MapVarTable,
    ) -> Res<Pattern> {
        if ast_elements.is_empty() {
            return Err((
                CompileError::TODO(smart_format!("requires at least one element")),
                span.clone(),
            ));
        }

        let seq_type = type_path
            .map(|(path, span)| self.lookup_path(&path, &span))
            .transpose()?;

        let mut pattern_elements = Vec::with_capacity(ast_elements.len());
        for (ast_element, _element_span) in ast_elements {
            let pattern =
                self.lower_any_pattern((ast_element.pattern.0, ast_element.pattern.1), var_table)?;
            pattern_elements.push(SetPatternElement {
                id: self.compiler.patterns.alloc_pat_id(),
                is_iter: ast_element.spread.is_some(),
                rel: None,
                val: pattern,
            })
        }

        Ok(self.mk_pattern(
            PatternKind::Set {
                val_type_def: seq_type,
                elements: pattern_elements.into_boxed_slice(),
            },
            &span,
        ))
    }

    fn lower_set_algebra_pattern(
        &mut self,
        alg_pattern: ast::SetAlgebraPattern,
        span: Span,
        var_table: &mut MapVarTable,
    ) -> Res<CompoundPatternAttrKind> {
        if alg_pattern.elements.is_empty() {
            return Err((
                CompileError::TODO(smart_format!("inner set must have at least one element")),
                span.clone(),
            ));
        }

        let mut elements = Vec::with_capacity(alg_pattern.len());

        for (ast_element, _span) in alg_pattern.elements {
            elements.push(SetElement {
                iter: ast_element.spread.is_some(),
                rel: match ast_element.relation_attrs {
                    Some((relation_attrs, span)) => {
                        let attrs = self.lower_struct_pattern_attrs(relation_attrs, var_table)?;
                        Some(self.mk_pattern(
                            PatternKind::Compound {
                                type_path: TypePath::RelContextual,
                                modifier: None,
                                is_unit_binding: false,
                                attributes: attrs,
                            },
                            &span,
                        ))
                    }
                    None => None,
                },
                val: self.lower_any_pattern(ast_element.pattern, var_table)?,
            });
        }

        Ok(match alg_pattern.operator.0 {
            ast::SetAlgebraicOperator::In => CompoundPatternAttrKind::SetOperator {
                operator: SetBinaryOperator::ElementIn,
                elements: self.lower_set_pattern_elements(elements),
            },
            ast::SetAlgebraicOperator::AllIn => CompoundPatternAttrKind::SetOperator {
                operator: SetBinaryOperator::AllIn,
                elements: self.lower_set_pattern_elements(elements),
            },
            ast::SetAlgebraicOperator::ContainsAll => CompoundPatternAttrKind::SetOperator {
                operator: SetBinaryOperator::ContainsAll,
                elements: self.lower_set_pattern_elements(elements),
            },
            ast::SetAlgebraicOperator::Intersects => CompoundPatternAttrKind::SetOperator {
                operator: SetBinaryOperator::Intersects,
                elements: self.lower_set_pattern_elements(elements),
            },
            ast::SetAlgebraicOperator::Equals => CompoundPatternAttrKind::SetOperator {
                operator: SetBinaryOperator::SetEquals,
                elements: self.lower_set_pattern_elements(elements),
            },
        })
    }

    fn lower_set_pattern_elements(
        &mut self,
        elements: Vec<SetElement>,
    ) -> Box<[SetPatternElement]> {
        elements
            .into_iter()
            .map(|SetElement { iter, rel, val }| SetPatternElement {
                id: self.compiler.patterns.alloc_pat_id(),
                is_iter: iter,
                rel,
                val,
            })
            .collect()
    }

    fn lower_expr_pattern(
        &mut self,
        (ast, span): (ast::ExprPattern, Span),
        var_table: &mut MapVarTable,
    ) -> Res<Pattern> {
        match ast {
            ast::ExprPattern::NumberLiteral(int) => {
                let int = int
                    .parse()
                    .map_err(|_| (CompileError::InvalidInteger, span.clone()))?;
                Ok(self.mk_pattern(PatternKind::ConstI64(int), &span))
            }
            ast::ExprPattern::TextLiteral(string) => {
                Ok(self.mk_pattern(PatternKind::ConstText(string), &span))
            }
            ast::ExprPattern::RegexLiteral(regex_literal) => {
                let regex_def_id = self
                    .compiler
                    .defs
                    .def_regex(&regex_literal, &span, &mut self.compiler.strings)
                    .map_err(|(compile_error, err_span)| {
                        (CompileError::InvalidRegex(compile_error), err_span)
                    })?;
                let regex_meta = self
                    .compiler
                    .defs
                    .literal_regex_meta_table
                    .get(&regex_def_id)
                    .unwrap();

                let mut regex_lowerer = RegexToPatternLowerer::new(
                    regex_meta.pattern,
                    &span,
                    self.src,
                    var_table,
                    &mut self.compiler.patterns,
                );

                regex_syntax::ast::visit(&regex_meta.ast, regex_lowerer.syntax_visitor()).unwrap();
                regex_syntax::hir::visit(&regex_meta.hir, regex_lowerer.syntax_visitor()).unwrap();

                let expr_regex = regex_lowerer.into_expr(regex_def_id);

                Ok(self.mk_pattern(PatternKind::Regex(expr_regex), &span))
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

                Ok(self.mk_pattern(PatternKind::Call(def_id, Box::new([left, right])), &span))
            }
            ast::ExprPattern::BooleanLiteral(bool) => {
                Ok(self.mk_pattern(PatternKind::ConstBool(bool), &span))
            }
            ast::ExprPattern::Variable(var_ident) => {
                self.lower_map_variable(var_ident, span, var_table)
            }
        }
    }

    fn lower_map_variable(
        &mut self,
        var_ident: String,
        span: Span,
        var_table: &mut MapVarTable,
    ) -> Res<Pattern> {
        let var = var_table.get_or_create_var(var_ident);
        Ok(self.mk_pattern(PatternKind::Variable(var), &span))
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
                    let segment = self.compiler.strings.intern(segment);
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
                        DefKind::Type(TypeDef {
                            visibility: DefVisibility::Private,
                            ..
                        }) => Err((CompileError::PrivateDefinition, span.clone())),
                        _ => Ok(*def_id),
                    },
                    None => Err((CompileError::TypeNotFound, span.clone())),
                }
            }
        }
    }

    fn provide_definition(
        &mut self,
        def_id: DefId,
        docs: Vec<String>,
        def_block: (Vec<(ast::Statement, Span)>, Span),
    ) -> Res<RootDefs> {
        self.compiler
            .namespaces
            .docs
            .entry(def_id)
            .or_default()
            .extend(docs);

        let mut root_defs: RootDefs = [def_id].into();

        let _entered = debug_span!("def", id = ?def_id).entered();

        {
            // The inherent relation block on the type uses the just defined
            // type as its context
            let context_fn = move || def_id;

            for spanned_stmt in def_block.0 {
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

    fn coin_type_definition(
        &mut self,
        ident: String,
        ident_span: &Span,
        private: Private,
        open: Open,
    ) -> Res<DefId> {
        let (def_id, coinage) = self.named_def_id(Space::Type, &ident, ident_span)?;
        if matches!(coinage, Coinage::New) {
            let ident = self.compiler.strings.intern(&ident);
            debug!("{def_id:?}: `{}`", ident);

            self.set_def_kind(
                def_id,
                DefKind::Type(TypeDef {
                    visibility: if private.0.is_some() {
                        DefVisibility::Private
                    } else {
                        DefVisibility::Public
                    },
                    open: open.0.is_some(),
                    ident: Some(ident),
                    rel_type_for: None,
                    concrete: true,
                }),
                ident_span,
            );
        }

        Ok(def_id)
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

    fn named_def_id(&mut self, space: Space, ident: &str, span: &Span) -> Res<(DefId, Coinage)> {
        let ident = self.compiler.strings.intern(ident);
        match self
            .compiler
            .namespaces
            .get_namespace_mut(self.src.package_id, space)
            .entry(ident)
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

    fn define_anonymous_type(&mut self, type_def: TypeDef<'m>, span: &Span) -> DefId {
        let anonymous_def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        self.set_def_kind(anonymous_def_id, DefKind::Type(type_def), span);
        debug!("{anonymous_def_id:?}: <anonymous>");
        anonymous_def_id
    }

    fn define_anonymous(&mut self, kind: DefKind<'m>, span: &Span) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        self.set_def_kind(def_id, kind, span);
        def_id
    }

    fn set_def_kind(&mut self, def_id: DefId, kind: DefKind<'m>, span: &Span) {
        self.compiler.defs.table.insert(
            def_id,
            Def {
                id: def_id,
                package: self.src.package_id,
                kind,
                span: self.src.span(span),
            },
        );
    }

    fn mk_pattern(&mut self, kind: PatternKind, span: &Span) -> Pattern {
        Pattern {
            id: self.compiler.patterns.alloc_pat_id(),
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
pub struct MapVarTable {
    variables: HashMap<String, Var>,
}

impl MapVarTable {
    pub fn get_or_create_var(&mut self, ident: String) -> Var {
        let length = self.variables.len();

        *self
            .variables
            .entry(ident)
            .or_insert_with(|| Var(length as u32))
    }

    /// Create an allocator for allocating the successive variables
    /// after the explicit ones
    fn into_allocator(self) -> VarAllocator {
        VarAllocator::from(Var(self.variables.len() as u32))
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

struct SetElement {
    iter: bool,
    rel: Option<Pattern>,
    val: Pattern,
}
