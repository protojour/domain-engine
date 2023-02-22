use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Range,
};

use ontol_parser::{ast, Span};
use ontol_runtime::{DefId, RelationId};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    compiler::Compiler,
    def::{
        Def, DefKind, PropertyCardinality, RelParams, Relation, RelationIdent, Relationship,
        ValueCardinality, Variables,
    },
    error::CompileError,
    expr::{Expr, ExprId, ExprKind, TypePath},
    namespace::Space,
    package::{PackageReference, CORE_PKG},
    Src,
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
            ast::Statement::Type(type_stmt) => {
                let def_id = self.named_def_id(Space::Type, &type_stmt.ident.0);
                let ident = self.compiler.strings.intern(&type_stmt.ident.0);
                let kind = match type_stmt.kind.0 {
                    ast::TypeKind::Type => DefKind::DomainType(Some(ident)),
                    ast::TypeKind::Entity => DefKind::DomainEntity(ident),
                };

                self.set_def(def_id, kind, &span);

                let mut root_defs: RootDefs = [def_id].into();

                if let Some(rel_block) = type_stmt.rel_block.0 {
                    // The inherent relation block on the type uses the just defined
                    // type as its context
                    let block_context = BlockContext::Context((def_id, span.clone()));

                    for (rel_stmt, rel_span) in rel_block {
                        match self.ast_relationship_chain_to_def(
                            rel_stmt,
                            rel_span,
                            block_context.clone(),
                        ) {
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
            ast::Statement::Rel(rel_stmt) => {
                self.ast_relationship_chain_to_def(rel_stmt, span, BlockContext::NoContext)
            }
            ast::Statement::Eq(ast::EqStatement {
                kw: _,
                variables: ast_variables,
                first,
                second,
            }) => {
                let mut var_table = VarTable::default();
                let mut variables = vec![];
                for (param, span) in ast_variables.into_iter() {
                    variables.push((
                        var_table.new_var_id(param, self.compiler),
                        self.src.span(&span),
                    ));
                }

                let first = self.lower_eq_type_to_obj(first, &mut var_table)?;
                let second = self.lower_eq_type_to_obj(second, &mut var_table)?;

                Ok([self.def(
                    DefKind::Equation(Variables(variables.into()), first, second),
                    &span,
                )]
                .into())
            }
        }
    }

    fn ast_relationship_chain_to_def(
        &mut self,
        rel: ast::RelStatement,
        span: Span,
        contextual_def: BlockContext<(DefId, Span)>,
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
                    self.ast_type_to_def(subject.0, &subject.1)?,
                    subject.1,
                    self.ast_type_to_def(object.0, &object.1)?,
                    object.1,
                ),
                (Some(subject), None, BlockContext::Context(object)) => (
                    self.ast_type_to_def(subject.0, &subject.1)?,
                    subject.1,
                    object.0,
                    object.1,
                ),
                (None, Some(object), BlockContext::Context(subject)) => (
                    subject.0,
                    subject.1,
                    self.ast_type_to_def(object.0, &object.1)?,
                    object.1,
                ),
                _ => return Err((CompileError::TooMuchContextInContextualRel, span)),
            };

        for chain_item in chain {
            let (next_ty, next_ty_span) = match chain_item.subject {
                Some((ast_ty, ty_span)) => (self.ast_type_to_def(ast_ty, &ty_span)?, ty_span),
                None => {
                    // implicit, anonymous type:
                    let anonymous_def_id = self.compiler.defs.alloc_def_id();
                    self.set_def(anonymous_def_id, DefKind::DomainType(None), &span);
                    (anonymous_def_id, span.clone())
                }
            };

            root_defs.push(self.ast_relationship_to_def(
                (subject_def, &subject_span),
                connection,
                (next_ty, &next_ty_span),
                span.clone(),
            )?);

            connection = chain_item.connection;
            subject_def = next_ty;
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
        subject: (DefId, &Span),
        ast_connection: ast::RelConnection,
        object: (DefId, &Span),
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
                let def_id = self.ast_type_to_def(ty, &span)?;

                match self.compiler.defs.get_def_kind(def_id) {
                    Some(DefKind::StringLiteral(_)) => {
                        (RelationIdent::Named(def_id), span.clone(), None)
                    }
                    Some(DefKind::Relation(relation)) => {
                        (relation.ident.clone(), span.clone(), None)
                    }
                    _ => (RelationIdent::Typed(def_id), span.clone(), None),
                }
            }
            ast::RelType::IntRange((range, span)) => (RelationIdent::Indexed, span, Some(range)),
        };

        let has_object_prop = object_prop_ident.is_some();

        // This syntax just defines the relation the first time it's used
        let relation_id = match self.define_relation_if_undefined(relation_ident) {
            ImplicitRelationId::New(relation_id) => {
                let object_prop =
                    object_prop_ident.map(|ident| self.compiler.strings.intern(&ident.0));

                self.set_def(
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
                    RelParams::Type(self.ast_type_to_def(rel_params.0, &rel_params.1)?)
                }
                None => RelParams::Unit,
            }
        };

        Ok(self.def(
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

    fn ast_type_to_def(&mut self, ast_ty: ast::Type, span: &Span) -> Res<DefId> {
        match ast_ty {
            ast::Type::Unit => Ok(self.compiler.defs.unit()),
            ast::Type::Path(path) => self.lookup_path(&path, span),
            ast::Type::StringLiteral(lit) => match lit.as_str() {
                "" => Ok(self.compiler.defs.empty_string()),
                _ => Ok(self
                    .compiler
                    .defs
                    .def_string_literal(&lit, &mut self.compiler.strings)),
            },
            ast::Type::Regex(lit) => self
                .compiler
                .defs
                .def_regex(&lit, span, &mut self.compiler.strings)
                .map_err(|(compile_error, err_span)| {
                    (CompileError::InvalidRegex(compile_error), err_span)
                }),
            _ => Err((CompileError::InvalidType, span.clone())),
            //ast::Type::EmptySequence => Ok(self.compiler.defs.empty_sequence()),
            // ast::Type::Literal(_) => Err(self.error(CompileError::InvalidType, span)),
        }
    }

    fn lower_eq_type_to_obj(
        &mut self,
        (eq_type, span): (ast::EqType, Span),
        var_table: &mut VarTable,
    ) -> Res<ExprId> {
        let type_def_id = self.lookup_path(&eq_type.path.0, &eq_type.path.1)?;
        let attributes = eq_type
            .attributes
            .into_iter()
            .map(|eq_attr| match eq_attr {
                ast::EqAttribute::Expr((expr, expr_span)) => {
                    let key = (DefId::unit(), self.src.span(&expr_span));
                    let expr = self.lower_expr((expr, expr_span), var_table)?;

                    Ok((key, expr))
                }
                ast::EqAttribute::Rel((rel, _rel_span)) => {
                    // FIXME: For now:
                    assert!(rel.subject.is_none());

                    let connection = rel.connection;
                    let (object, object_span) = rel.object.unwrap();

                    let key = self.ast_type_to_def(connection.0, &connection.1)?;

                    self.lower_expr((object, object_span), var_table)
                        .map(|expr| ((key, self.src.span(&connection.1)), expr))
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
        var_table: &VarTable,
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

    fn define_relation_if_undefined(&mut self, kind: RelationIdent) -> ImplicitRelationId {
        match kind {
            RelationIdent::Named(def_id) | RelationIdent::Typed(def_id) => {
                match self.compiler.relations.relations.entry(def_id) {
                    Entry::Vacant(vacant) => ImplicitRelationId::New(
                        *vacant.insert(RelationId(self.compiler.defs.alloc_def_id())),
                    ),
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

    fn named_def_id(&mut self, space: Space, ident: &String) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id();
        self.compiler
            .namespaces
            .get_mut(self.src.package_id, space)
            .insert(ident.clone(), def_id);
        def_id
    }

    fn set_def(&mut self, def_id: DefId, kind: DefKind<'m>, span: &Span) {
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

    fn def(&mut self, kind: DefKind<'m>, span: &Span) -> DefId {
        let def_id = self.compiler.defs.alloc_def_id();
        self.set_def(def_id, kind, span);
        def_id
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
struct VarTable {
    variables: HashMap<String, ExprId>,
}

impl VarTable {
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
