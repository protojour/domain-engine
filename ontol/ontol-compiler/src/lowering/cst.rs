#![allow(unused)]

use std::{collections::hash_map::Entry, marker::PhantomData, ops::Range};

use ontol_parser::{
    cst::{
        inspect::{self as insp, RelObject, RelSubject},
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt},
    },
    Span, UnescapeError,
};
use ontol_runtime::{
    property::{PropertyCardinality, ValueCardinality},
    DefId, RelationshipId,
};
use tracing::debug_span;

use crate::{
    def::{DefKind, RelParams, Relationship, TypeDef, TypeDefFlags},
    lowering::context::LoweringCtx,
    namespace::Space,
    package::PackageReference,
    pattern::Pattern,
    CompileError, Compiler, SpannedCompileError, Src,
};

use super::context::{
    BlockContext, Extern, MapVarTable, Open, Private, RelationKey, RootDefs, Symbol,
};

pub struct CstLowering<'c, 'm, 's, V: NodeView<'s>> {
    ctx: LoweringCtx<'c, 'm>,
    _phantom: PhantomData<&'s V>,
}

type LoweringError = (CompileError, Span);
type Res<T> = Result<T, LoweringError>;

enum PreDefinedStmt<V> {
    Def(DefId, insp::DefStatement<V>),
    Rel(insp::RelStatement<V>),
    Fmt(insp::FmtStatement<V>),
    Map(insp::MapStatement<V>),
}

impl<'c, 'm, 's, V: NodeView<'s>> CstLowering<'c, 'm, 's, V> {
    pub fn new(compiler: &'c mut Compiler<'m>, src: Src) -> Self {
        Self {
            ctx: LoweringCtx {
                compiler,
                package_id: src.package_id,
                source_id: src.id,
                root_defs: Default::default(),
            },
            _phantom: PhantomData,
        }
    }

    pub fn lower_ontol(mut self, ontol: insp::Node<V>) -> Self {
        let insp::Node::Ontol(ontol) = ontol else {
            return self;
        };

        let mut pre_defined_statements = vec![];

        // first pass: Register all named definitions into the namespace
        for statement in ontol.statements() {
            if let Some(pre_defined) = self.pre_define_statement(statement) {
                pre_defined_statements.push(pre_defined);
            }
        }

        // second pass: Handle inner bodies, etc
        for stmt in pre_defined_statements {
            if let Some(mut defs) = self.lower_pre_defined(stmt, BlockContext::NoContext) {
                self.ctx.root_defs.append(&mut defs);
            }
        }

        self
    }

    pub fn finish(self) -> Vec<DefId> {
        self.ctx.root_defs
    }

    fn pre_define_statement(&mut self, statement: insp::Statement<V>) -> Option<PreDefinedStmt<V>> {
        match statement {
            insp::Statement::UseStatement(use_stmt) => {
                let name = use_stmt.name()?;
                let name_text = name.text().and_then(|result| self.unescape(result))?;

                let reference = PackageReference::Named(name_text);
                let Some(used_package_def_id) =
                    self.ctx.compiler.packages.loaded_packages.get(&reference)
                else {
                    self.report_error((CompileError::PackageNotFound(reference), name.view.span()));
                    return None;
                };

                let type_namespace = self
                    .ctx
                    .compiler
                    .namespaces
                    .get_namespace_mut(self.ctx.package_id, Space::Type);

                let symbol = use_stmt.ident_path()?.symbols().next()?;

                let as_ident = self.ctx.compiler.strings.intern(symbol.slice());
                type_namespace.insert(as_ident, *used_package_def_id);

                None
            }
            insp::Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_, symbol) =
                    self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        &ident_token.span(),
                        private,
                        open,
                        extern_,
                        symbol,
                    )
                })?;

                Some(PreDefinedStmt::Def(def_id, def_stmt))
            }
            insp::Statement::RelStatement(rel_stmt) => Some(PreDefinedStmt::Rel(rel_stmt)),
            insp::Statement::FmtStatement(fmt_stmt) => Some(PreDefinedStmt::Fmt(fmt_stmt)),
            insp::Statement::MapStatement(map_stmt) => Some(PreDefinedStmt::Map(map_stmt)),
        }
    }

    fn lower_pre_defined(
        &mut self,
        stmt: PreDefinedStmt<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match stmt {
            PreDefinedStmt::Def(def_id, def_stmt) => self.lower_def_body(def_id, def_stmt),
            PreDefinedStmt::Rel(rel_stmt) => {
                self.lower_statement(insp::Statement::RelStatement(rel_stmt), block_context)
            }
            PreDefinedStmt::Fmt(fmt_stmt) => {
                self.lower_statement(insp::Statement::FmtStatement(fmt_stmt), block_context)
            }
            PreDefinedStmt::Map(map_stmt) => {
                self.lower_statement(insp::Statement::MapStatement(map_stmt), block_context)
            }
        }
    }

    fn lower_statement(
        &mut self,
        statement: insp::Statement<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match statement {
            insp::Statement::UseStatement(use_stmt) => None,
            insp::Statement::DefStatement(def_stmt) => {
                let ident_token = def_stmt.ident_path()?.symbols().next()?;
                let (private, open, extern_, symbol) =
                    self.read_def_modifiers(def_stmt.modifiers());

                let def_id = self.catch(|zelf| {
                    zelf.ctx.coin_type_definition(
                        ident_token.slice(),
                        &ident_token.span(),
                        private,
                        open,
                        extern_,
                        symbol,
                    )
                })?;
                let mut root_defs: RootDefs = [def_id].into();
                root_defs.extend(self.lower_def_body(def_id, def_stmt)?);
                Some(root_defs)
            }
            insp::Statement::RelStatement(rel_stmt) => {
                self.lower_rel_statement(rel_stmt, block_context)
            }
            insp::Statement::FmtStatement(fmt_stmt) => todo!(),
            insp::Statement::MapStatement(map_stmt) => todo!(),
        }
    }

    fn lower_def_body(&mut self, def_id: DefId, stmt: insp::DefStatement<V>) -> Option<RootDefs> {
        self.append_documentation(def_id, stmt.view);

        let mut root_defs: RootDefs = [def_id].into();

        if let Some(body) = stmt.body() {
            let _entered = debug_span!("def", id = ?def_id).entered();

            // The inherent relation block on the type uses the just defined
            // type as its context
            let context_fn = move || def_id;

            for statement in body.statements() {
                if let Some(mut defs) =
                    self.lower_statement(statement, BlockContext::Context(&context_fn))
                {
                    root_defs.append(&mut defs);
                }
            }
        }

        Some(root_defs)
    }

    fn lower_rel_statement(
        &mut self,
        stmt: insp::RelStatement<V>,
        block: BlockContext,
    ) -> Option<RootDefs> {
        let subject = stmt.subject()?;
        let fwd_set = stmt.fwd_set()?;
        let backwd_set = stmt.backwd_set();
        let object = stmt.object()?;

        let mut root_defs = RootDefs::default();

        let subject_def_id = self.resolve_type_reference(
            subject.type_mod()?.type_ref()?,
            &block,
            Some(&mut root_defs),
        )?;
        let object_def_id = match object.type_mod_or_pattern()? {
            insp::TypeModOrPattern::TypeMod(type_mod) => {
                self.resolve_type_reference(type_mod.type_ref()?, &block, Some(&mut root_defs))?
            }
            insp::TypeModOrPattern::Pattern(pattern) => {
                let mut var_table = MapVarTable::default();
                let lowered = self.lower_any_pattern(pattern, &mut var_table)?;
                let pat_id = self.ctx.compiler.patterns.alloc_pat_id();
                self.ctx.compiler.patterns.table.insert(pat_id, lowered);

                self.ctx
                    .define_anonymous(DefKind::Constant(pat_id), &pattern.view().span())
            }
        };

        for (index, relation) in stmt.fwd_set()?.relations().enumerate() {
            if let Some(mut defs) = self.lower_relationship(
                (subject_def_id, subject),
                relation,
                (object_def_id, object),
                if index == 0 { stmt.backwd_set() } else { None },
                stmt,
            ) {
                root_defs.append(&mut defs);
            }
        }

        None
    }

    fn lower_relationship(
        &mut self,
        subject: (DefId, RelSubject<V>),
        relation: insp::Relation<V>,
        object: (DefId, RelObject<V>),
        backward_relation: Option<insp::RelBackwdSet<V>>,
        rel_stmt: insp::RelStatement<V>,
    ) -> Option<RootDefs> {
        let mut root_defs = RootDefs::new();

        let (key, ident_span, index_range_rel_params): (_, _, Option<Range<Option<u16>>>) =
            match relation.relation_type()? {
                insp::TypeModOrRange::TypeMod(type_mod) => {
                    let def_id = self.resolve_type_reference(
                        type_mod.type_ref()?,
                        &BlockContext::NoContext,
                        Some(&mut root_defs),
                    )?;
                    let span = type_mod.view().span();

                    match self.ctx.compiler.defs.def_kind(def_id) {
                        DefKind::TextLiteral(_) | DefKind::Type(_) => {
                            (RelationKey::Named(def_id), span, None)
                        }
                        DefKind::BuiltinRelType(..) => (RelationKey::Builtin(def_id), span, None),
                        _ => {
                            self.report_error((CompileError::InvalidRelationType, span));
                            return None;
                        }
                    }
                }
                insp::TypeModOrRange::Range(range) => (
                    RelationKey::Indexed,
                    range.view.span(),
                    Some(self.lower_u16_range(range)),
                ),
            };

        let has_object_prop = backward_relation.is_some();

        // This syntax just defines the relation the first time it's used
        let relation_def_id = self.ctx.define_relation_if_undefined(key, &ident_span);

        let relationship_id = self.ctx.compiler.defs.alloc_def_id(self.ctx.package_id);
        self.append_documentation(relationship_id, rel_stmt.view);

        let rel_params = if let Some(index_range_rel_params) = index_range_rel_params {
            if let Some(rp) = relation.rel_params() {
                self.report_error((
                    CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes,
                    rp.view.span(),
                ));
                return None;
            }

            RelParams::IndexRange(index_range_rel_params)
        } else if let Some(rp) = relation.rel_params() {
            let rel_def_id = self.ctx.define_anonymous_type(
                TypeDef {
                    ident: None,
                    rel_type_for: Some(RelationshipId(relationship_id)),
                    flags: TypeDefFlags::CONCRETE,
                },
                &rp.view.span(),
            );
            let context_fn = || rel_def_id;

            root_defs.push(rel_def_id);

            // This type needs to be part of the anonymous part of the namespace
            self.ctx
                .compiler
                .namespaces
                .add_anonymous(self.ctx.package_id, rel_def_id);

            for statement in rp.statements() {
                if let Some(mut defs) =
                    self.lower_statement(statement, BlockContext::Context(&context_fn))
                {
                    root_defs.append(&mut defs);
                }
            }

            RelParams::Type(rel_def_id)
        } else {
            RelParams::Unit
        };

        let mut relationship = {
            let object_prop = backward_relation
                .and_then(|rel| rel.name())
                .and_then(|name| name.text())
                .and_then(|result| self.unescape(result))
                .map(|prop| self.ctx.compiler.strings.intern(&prop));

            let subject_cardinality = (
                property_cardinality(relation.prop_cardinality())
                    .unwrap_or(PropertyCardinality::Mandatory),
                value_cardinality(subject.1.type_mod()).unwrap_or(ValueCardinality::Unit),
            );
            let object_cardinality = {
                let default = if has_object_prop {
                    // i.e. no syntax sugar: The object prop is explicit,
                    // therefore the object cardinality is explicit.
                    (PropertyCardinality::Mandatory, ValueCardinality::Unit)
                } else {
                    // The syntactic sugar case, which is the default behaviour:
                    // Many incoming edges to the same object:
                    (PropertyCardinality::Optional, ValueCardinality::IndexSet)
                };

                (
                    backward_relation
                        .and_then(|rel| property_cardinality(rel.prop_cardinality()))
                        .unwrap_or(default.0),
                    default.1,
                )
            };

            Relationship {
                relation_def_id,
                relation_span: self.ctx.source_span(&relation.view.span()),
                subject: (subject.0, self.ctx.source_span(&subject.1.view.span())),
                subject_cardinality,
                object: (object.0, self.ctx.source_span(&object.1.view.span())),
                object_cardinality,
                object_prop,
                rel_params,
            }
        };

        // HACK(for now): invert relationship
        if relation_def_id == self.ctx.compiler.primitives.relations.id {
            relationship = Relationship {
                relation_def_id: self.ctx.compiler.primitives.relations.identifies,
                relation_span: relationship.relation_span,
                subject: relationship.object,
                subject_cardinality: relationship.object_cardinality,
                object: relationship.subject,
                object_cardinality: relationship.subject_cardinality,
                object_prop: None,
                rel_params: relationship.rel_params,
            };
        }

        self.ctx.set_def_kind(
            relationship_id,
            DefKind::Relationship(relationship),
            &rel_stmt.view.span(),
        );
        root_defs.push(relationship_id);

        Some(root_defs)
    }

    fn resolve_type_reference(
        &mut self,
        type_ref: insp::TypeRef<V>,
        block_context: &BlockContext,
        root_defs: Option<&mut RootDefs>,
    ) -> Option<DefId> {
        match (type_ref, block_context) {
            (insp::TypeRef::IdentPath(path), _) => self.lookup_path(path),
            (insp::TypeRef::This(_), BlockContext::Context(func)) => Some(func()),
            (insp::TypeRef::This(this), BlockContext::NoContext) => {
                self.report_error((CompileError::WildcardNeedsContextualBlock, this.view.span()));
                None
            }
            (insp::TypeRef::DefBody(body), _) => {
                if body.statements().next().is_none() {
                    return Some(self.ctx.compiler.primitives.unit);
                }

                let Some(root_defs) = root_defs else {
                    self.report_error((
                        CompileError::TODO("Anonymous struct not allowed here"),
                        body.view.span(),
                    ));
                    return None;
                };

                let def_id = self.ctx.define_anonymous_type(
                    TypeDef {
                        ident: None,
                        rel_type_for: None,
                        flags: TypeDefFlags::CONCRETE,
                    },
                    &body.view.span(),
                );

                // This type needs to be part of the anonymous part of the namespace
                self.ctx
                    .compiler
                    .namespaces
                    .add_anonymous(self.ctx.package_id, def_id);
                root_defs.push(def_id);

                let context_fn = || def_id;

                for statement in body.statements() {
                    if let Some(mut defs) =
                        self.lower_statement(statement, BlockContext::Context(&context_fn))
                    {
                        root_defs.append(&mut defs);
                    }
                }

                Some(def_id)
            }
        }
    }

    fn lower_any_pattern(
        &mut self,
        pattern: insp::Pattern<V>,
        var_table: &mut MapVarTable,
    ) -> Option<Pattern> {
        None
    }

    fn lookup_path(&mut self, ident_path: insp::IdentPath<V>) -> Option<DefId> {
        self.catch(|zelf| {
            zelf.ctx.lookup_path(
                ident_path
                    .symbols()
                    .map(|token| (token.slice(), token.span())),
                &ident_path.view.span(),
            )
        })
    }

    fn unescape(&mut self, result: Result<String, Vec<UnescapeError>>) -> Option<String> {
        match result {
            Ok(string) => Some(string),
            Err(unescape_errors) => {
                for error in unescape_errors {
                    self.report_error((CompileError::TODO(error.msg), error.span));
                }

                None
            }
        }
    }

    fn read_def_modifiers(
        &mut self,
        modifiers: impl Iterator<Item = V::Token>,
    ) -> (Private, Open, Extern, Symbol) {
        let mut private = Private(None);
        let mut open = Open(None);
        let mut extern_ = Extern(None);
        let mut symbol = Symbol(None);

        for modifier in modifiers {
            match modifier.slice() {
                "@private" => {
                    private.0 = Some(modifier.span());
                }
                "@open" => {
                    open.0 = Some(modifier.span());
                }
                "@extern" => {
                    extern_.0 = Some(modifier.span());
                }
                "@symbol" => {
                    symbol.0 = Some(modifier.span());
                }
                _ => {
                    self.report_error((CompileError::TODO("illegal modifier"), modifier.span()));
                }
            }
        }

        (private, open, extern_, symbol)
    }

    fn lower_u16_range(&mut self, range: insp::NumberRange<V>) -> Range<Option<u16>> {
        let start = range.start().and_then(|token| self.token_to_u16(token));
        let end = range.end().and_then(|token| self.token_to_u16(token));

        start..end
    }

    fn token_to_u16(&mut self, token: V::Token) -> Option<u16> {
        match u16::from_str_radix(token.slice(), 10) {
            Ok(number) => Some(number),
            Err(error) => {
                self.report_error((
                    CompileError::NumberParse("unable to parse number".to_string()),
                    token.span(),
                ));
                None
            }
        }
    }

    fn append_documentation(&mut self, def_id: DefId, node_view: V) {
        let Some(docs) = ontol_parser::join_doc_lines(node_view.local_doc_comments()) else {
            return;
        };

        match self.ctx.compiler.namespaces.docs.entry(def_id) {
            Entry::Vacant(vacant) => {
                vacant.insert(docs);
            }
            Entry::Occupied(mut occupied) => {
                occupied.get_mut().push_str("\n\n");
                occupied.get_mut().push_str(&docs);
            }
        }
    }

    fn catch<T>(&mut self, f: impl FnOnce(&mut Self) -> Res<T>) -> Option<T> {
        match f(self) {
            Ok(value) => Some(value),
            Err((err, span)) => {
                self.report_error((err, span));
                None
            }
        }
    }

    fn report_error(&mut self, (error, span): (CompileError, Range<usize>)) {
        self.ctx
            .compiler
            .push_error(error.spanned(&self.ctx.source_span(&span)));
    }
}

fn value_cardinality<'a, V: NodeView<'a>>(
    type_mod: Option<insp::TypeMod<V>>,
) -> Option<ValueCardinality> {
    Some(match type_mod? {
        insp::TypeMod::TypeModUnit(_) => ValueCardinality::Unit,
        insp::TypeMod::TypeModSet(_) => ValueCardinality::IndexSet,
        insp::TypeMod::TypeModList(_) => ValueCardinality::List,
    })
}

fn property_cardinality<'a, V: NodeView<'a>>(
    prop_cardinality: Option<insp::PropCardinality<V>>,
) -> Option<PropertyCardinality> {
    Some(if prop_cardinality?.question().is_some() {
        PropertyCardinality::Optional
    } else {
        PropertyCardinality::Mandatory
    })
}
