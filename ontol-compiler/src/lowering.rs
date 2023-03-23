use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Range,
};

use fnv::FnvHashMap;
use ontol_parser::{ast, Span};
use ontol_runtime::{DefId, DefParamId, RelationId};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    def::{
        Def, DefKind, DefParamBinding, DefReference, PropertyCardinality, RelParams, Relation,
        RelationIdent, Relationship, TypeDef, TypeDefParam, ValueCardinality, Variables,
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
        match self.stmt_to_def(stmt) {
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

    fn stmt_to_def(&mut self, (stmt, span): (ast::Statement, Span)) -> Res<RootDefs> {
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
                    .get_mut(self_package_def_id, Space::Type);

                type_namespace.insert(use_stmt.as_ident.0, *used_package_def_id);

                Ok(Default::default())
            }
            ast::Statement::Type(type_stmt) => self.define_type(type_stmt, span),
            ast::Statement::Rel(rel_stmt) => {
                self.ast_relationship_chain_to_def(rel_stmt, span, BlockContext::NoContext)
            }
            ast::Statement::Eq(ast::EqStatement {
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

                let first = self.lower_eq_type_to_obj(first, &mut var_table)?;
                let second = self.lower_eq_type_to_obj(second, &mut var_table)?;

                Ok([self.define(
                    DefKind::Equation(Variables(variables.into()), first, second),
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
                ident: Some(ident),
                params,
            }),
            &span,
        );

        let mut root_defs: RootDefs = [def_id].into();

        if let Some((rel_block, _span)) = type_stmt.rel_block {
            // The inherent relation block on the type uses the just defined
            // type as its context
            let def = DefReference {
                def_id,
                pattern_bindings: Default::default(),
            };
            let block_context = BlockContext::Context((def, span.clone()));

            for (rel_stmt, rel_span) in rel_block {
                match self.ast_relationship_chain_to_def(rel_stmt, rel_span, block_context.clone())
                {
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

    fn ast_relationship_chain_to_def(
        &mut self,
        rel: ast::RelStatement,
        span: Span,
        contextual_def: BlockContext<(DefReference, Span)>,
    ) -> Res<RootDefs> {
        let ast::RelStatement {
            docs: _,
            kw: _,
            subject,
            mut connection,
            chain,
            object,
        } = rel;

        let mut root_defs = SmallVec::new();

        let (mut subject_def, mut subject_span, object_def, object_span) =
            match (subject, object, contextual_def) {
                (Some(subject), Some(object), BlockContext::NoContext) => (
                    self.resolve_type_reference(subject.0, &subject.1)?,
                    subject.1,
                    self.resolve_type_reference(object.0, &object.1)?,
                    object.1,
                ),
                (Some(subject), None, BlockContext::Context(object)) => (
                    self.resolve_type_reference(subject.0, &subject.1)?,
                    subject.1,
                    object.0,
                    object.1,
                ),
                (None, Some(object), BlockContext::Context(subject)) => (
                    subject.0,
                    subject.1,
                    self.resolve_type_reference(object.0, &object.1)?,
                    object.1,
                ),
                _ => return Err((CompileError::TooMuchContextInContextualRel, span)),
            };

        for chain_item in chain {
            let (next_def, next_ty_span) = match chain_item.subject {
                Some((ast_ty, ty_span)) => {
                    (self.resolve_type_reference(ast_ty, &ty_span)?, ty_span)
                }
                None => {
                    // implicit, anonymous type:
                    let anonymous_def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
                    self.set_def_kind(
                        anonymous_def_id,
                        DefKind::Type(TypeDef {
                            ident: None,
                            params: None,
                        }),
                        &span,
                    );
                    (
                        DefReference {
                            def_id: anonymous_def_id,
                            pattern_bindings: Default::default(),
                        },
                        span.clone(),
                    )
                }
            };

            root_defs.push(self.ast_relationship_to_def(
                (subject_def, &subject_span),
                connection,
                (next_def.clone(), &next_ty_span),
                span.clone(),
            )?);

            connection = chain_item.connection;
            subject_def = next_def;
            subject_span = next_ty_span;
        }

        root_defs.push(self.ast_relationship_to_def(
            (subject_def, &subject_span),
            connection,
            (object_def, &object_span),
            span,
        )?);

        Ok(root_defs)
    }

    fn ast_relationship_to_def(
        &mut self,
        subject: (DefReference, &Span),
        ast_connection: ast::RelConnection,
        object: (DefReference, &Span),
        span: Span,
    ) -> Res<DefId> {
        let ast::RelConnection {
            ty: relation_ty,
            subject_cardinality,
            object_prop_ident,
            object_cardinality,
            rel_params,
        } = ast_connection;

        let (relation_ident, ident_span, index_range_rel_params): (
            _,
            _,
            Option<Range<Option<u16>>>,
        ) = match relation_ty {
            ast::RelType::Type((ty, span)) => {
                let def = self.resolve_type_reference(ty, &span)?;

                match self.compiler.defs.get_def_kind(def.def_id) {
                    Some(DefKind::StringLiteral(_)) => {
                        (RelationIdent::Named(def), span.clone(), None)
                    }
                    Some(DefKind::Relation(relation)) => {
                        (relation.ident.clone(), span.clone(), None)
                    }
                    _ => (RelationIdent::Typed(def), span.clone(), None),
                }
            }
            ast::RelType::IntRange((range, span)) => (RelationIdent::Indexed, span, Some(range)),
        };

        let has_object_prop = object_prop_ident.is_some();

        // This syntax just defines the relation the first time it's used
        let relation_id = match self.define_relation_if_undefined(&relation_ident) {
            ImplicitRelationId::New(relation_id) => {
                let object_prop =
                    object_prop_ident.map(|ident| self.compiler.strings.intern(&ident.0));

                self.set_def_kind(
                    relation_id.0,
                    DefKind::Relation(Relation {
                        ident: relation_ident,
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
            if let Some((_, span)) = rel_params {
                return Err((
                    CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes,
                    span,
                ));
            }

            RelParams::IndexRange(index_range_rel_params)
        } else {
            match rel_params {
                Some(rel_params) => {
                    RelParams::Type(self.resolve_type_reference(rel_params.0, &rel_params.1)?)
                }
                None => RelParams::Unit,
            }
        };

        Ok(self.define(
            DefKind::Relationship(Relationship {
                relation_id,
                subject: (subject.0, self.src.span(subject.1)),
                subject_cardinality: subject_cardinality
                    .map(convert_cardinality)
                    .unwrap_or((PropertyCardinality::Mandatory, ValueCardinality::One)),
                object: (object.0, self.src.span(object.1)),
                object_cardinality: object_cardinality.map(convert_cardinality).unwrap_or_else(
                    || {
                        if has_object_prop {
                            // i.e. no syntax sugar: The object prop is explicit,
                            // therefore the object cardinality is explicit.
                            (PropertyCardinality::Mandatory, ValueCardinality::One)
                        } else {
                            // The syntactic sugar case, which is the default behaviour:
                            // Many incoming edges to the same object:
                            (PropertyCardinality::Optional, ValueCardinality::Many)
                        }
                    },
                ),
                rel_params,
            }),
            &span,
        ))
    }

    fn resolve_type_reference(&mut self, ast_ty: ast::Type, span: &Span) -> Res<DefReference> {
        match ast_ty {
            ast::Type::Unit => Ok(DefReference {
                def_id: self.compiler.defs.unit(),
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
                    "" => self.compiler.defs.empty_string(),
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

    fn lower_eq_type_to_obj(
        &mut self,
        (eq_type, span): (ast::EqType, Span),
        var_table: &mut ExprVarTable,
    ) -> Res<ExprId> {
        let type_def_id = self.lookup_path(&eq_type.path.0, &eq_type.path.1)?;
        let attributes = eq_type
            .attributes
            .into_iter()
            .map(|eq_attr| match eq_attr {
                ast::EqAttribute::Expr((expr, expr_span)) => {
                    let key = (
                        DefReference {
                            def_id: DefId::unit(),
                            pattern_bindings: Default::default(),
                        },
                        self.src.span(&expr_span),
                    );
                    let expr = self.lower_expr((expr, expr_span), var_table)?;

                    Ok((key, expr))
                }
                ast::EqAttribute::Rel((rel, _rel_span)) => {
                    // FIXME: For now:
                    assert!(rel.subject.is_none());

                    let connection = rel.connection;
                    let (object, object_span) = rel.object.unwrap();

                    let def = self.resolve_type_reference(connection.0, &connection.1)?;

                    self.lower_expr((object, object_span), var_table)
                        .map(|expr| ((def, self.src.span(&connection.1)), expr))
                }
            })
            .collect::<Result<_, _>>()?;

        let expr = self.expr(
            ExprKind::Obj(
                TypePath {
                    def_id: type_def_id,
                    span: self.src.span(&eq_type.path.1),
                },
                attributes,
            ),
            &span,
        );
        let expr_id = self.compiler.defs.alloc_expr_id();
        self.compiler.expressions.insert(expr_id, expr);

        Ok(expr_id)
    }

    fn lower_expr(
        &mut self,
        (ast_expr, span): (ast::Expression, Span),
        var_table: &ExprVarTable,
    ) -> Res<Expr> {
        match ast_expr {
            ast::Expression::NumberLiteral(int) => {
                let int = int
                    .parse()
                    .map_err(|_| (CompileError::InvalidInteger, span.clone()))?;
                Ok(self.expr(ExprKind::Constant(int), &span))
            }
            ast::Expression::Binary(left, op, right) => {
                let fn_ident = match op {
                    ast::BinaryOp::Add => "+",
                    ast::BinaryOp::Sub => "-",
                    ast::BinaryOp::Mul => "*",
                    ast::BinaryOp::Div => "/",
                };

                let def_id = self.lookup_ident(fn_ident, &span)?;

                let left = self.lower_expr(*left, var_table)?;
                let right = self.lower_expr(*right, var_table)?;

                Ok(self.expr(ExprKind::Call(def_id, Box::new([left, right])), &span))
            }
            ast::Expression::Variable(var_ident) => {
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
                    Some(def_id) => Ok(*def_id),
                    None => Err((CompileError::TypeNotFound, span.clone())),
                }
            }
        }
    }

    fn define_relation_if_undefined(&mut self, kind: &RelationIdent) -> ImplicitRelationId {
        match kind {
            RelationIdent::Named(def) | RelationIdent::Typed(def) => {
                match self.compiler.relations.relations.entry(def.def_id) {
                    Entry::Vacant(vacant) => ImplicitRelationId::New(*vacant.insert(RelationId(
                        self.compiler.defs.alloc_def_id(self.src.package_id),
                    ))),
                    Entry::Occupied(occupied) => ImplicitRelationId::Reused(*occupied.get()),
                }
            }
            RelationIdent::Id => {
                ImplicitRelationId::Reused(RelationId(self.compiler.defs.id_relation()))
            }
            RelationIdent::Indexed => {
                ImplicitRelationId::Reused(RelationId(self.compiler.defs.indexed_relation()))
            }
        }
    }

    fn named_def_id(&mut self, space: Space, ident: &String) -> Result<DefId, DefId> {
        let def_id = self.compiler.defs.alloc_def_id(self.src.package_id);
        match self
            .compiler
            .namespaces
            .get_mut(self.src.package_id, space)
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
enum BlockContext<T> {
    NoContext,
    Context(T),
}

enum ImplicitRelationId {
    New(RelationId),
    Reused(RelationId),
}

#[derive(Default)]
struct ExprVarTable {
    variables: HashMap<String, ExprId>,
}

impl ExprVarTable {
    fn new_var_id(&mut self, ident: String, compiler: &mut Compiler) -> ExprId {
        *self
            .variables
            .entry(ident)
            .or_insert_with(|| compiler.defs.alloc_expr_id())
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
