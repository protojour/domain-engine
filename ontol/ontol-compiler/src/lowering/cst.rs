use std::{collections::hash_map::Entry, marker::PhantomData, ops::Range};

use ontol_parser::{
    cst::{
        inspect::{self as insp},
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt},
    },
    lexer::{kind::Kind, unescape::unescape_regex},
    ParserError, Span,
};
use ontol_runtime::{
    property::{PropertyCardinality, ValueCardinality},
    DefId, RelationshipId,
};
use tracing::{debug, debug_span};

use crate::{
    def::{DefKind, FmtFinalState, RelParams, Relationship, TypeDef, TypeDefFlags},
    lowering::context::{LoweringCtx, SetElement},
    namespace::Space,
    package::{PackageReference, ONTOL_PKG},
    pattern::{
        CompoundPatternAttr, CompoundPatternAttrKind, CompoundPatternModifier, PatId, Pattern,
        PatternKind, SetBinaryOperator, SetPatternElement, SpreadLabel, TypePath,
    },
    regex_util::RegexToPatternLowerer,
    CompileError, Compiler, SourceSpan, Src,
};

use super::context::{
    BlockContext, Coinage, Extern, MapVarTable, Open, Private, RelationKey, RootDefs, Symbol,
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

enum LoweredStructPatternParams {
    Attrs {
        attrs: Box<[CompoundPatternAttr]>,
        spread_label: Option<Box<SpreadLabel>>,
    },
    Unit(Pattern),
    Error,
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
            PreDefinedStmt::Rel(rel_stmt) => self.lower_statement(rel_stmt.into(), block_context),
            PreDefinedStmt::Fmt(fmt_stmt) => self.lower_statement(fmt_stmt.into(), block_context),
            PreDefinedStmt::Map(map_stmt) => self.lower_statement(map_stmt.into(), block_context),
        }
    }

    fn lower_statement(
        &mut self,
        statement: insp::Statement<V>,
        block_context: BlockContext,
    ) -> Option<RootDefs> {
        match statement {
            insp::Statement::UseStatement(_use_stmt) => None,
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
            insp::Statement::FmtStatement(fmt_stmt) => {
                self.lower_fmt_statement(fmt_stmt, block_context)
            }
            insp::Statement::MapStatement(map_stmt) => {
                let def_id = self.lower_map_statement(map_stmt, block_context)?;
                Some(vec![def_id])
            }
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
                let lowered = self.lower_pattern(pattern, &mut var_table);
                let pat_id = self.ctx.compiler.patterns.alloc_pat_id();
                self.ctx.compiler.patterns.table.insert(pat_id, lowered);

                self.ctx
                    .define_anonymous(DefKind::Constant(pat_id), &pattern.view().span())
            }
        };

        for (index, relation) in fwd_set.relations().enumerate() {
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

        Some(root_defs)
    }

    fn lower_relationship(
        &mut self,
        subject: (DefId, insp::RelSubject<V>),
        relation: insp::Relation<V>,
        object: (DefId, insp::RelObject<V>),
        backward_relation: Option<insp::RelBackwdSet<V>>,
        rel_stmt: insp::RelStatement<V>,
    ) -> Option<RootDefs> {
        let mut root_defs = RootDefs::new();

        let (key, ident_span, index_range_rel_params): (_, _, Option<Range<Option<u16>>>) = {
            let type_mod = relation.relation_type()?;
            match type_mod.type_ref()? {
                insp::TypeRef::NumberRange(range) => (
                    RelationKey::Indexed,
                    range.view.span(),
                    Some(self.lower_u16_range(range)),
                ),
                type_ref => {
                    let def_id = self.resolve_type_reference(
                        type_ref,
                        &BlockContext::NoContext,
                        Some(&mut root_defs),
                    )?;
                    let span = type_mod.view().span();

                    match self.ctx.compiler.defs.def_kind(def_id) {
                        DefKind::TextLiteral(_) | DefKind::Type(_) => {
                            (RelationKey::Named(def_id), span, None)
                        }
                        DefKind::BuiltinRelType(..) => (RelationKey::Builtin(def_id), span, None),
                        DefKind::NumberLiteral(lit) => match lit.parse::<u16>() {
                            Ok(int) => (RelationKey::Indexed, span, Some(Some(int)..Some(int + 1))),
                            Err(_) => {
                                self.report_error((
                                    CompileError::NumberParse("not an integer".to_string()),
                                    span,
                                ));
                                return None;
                            }
                        },
                        _ => {
                            self.report_error((CompileError::InvalidRelationType, span));
                            return None;
                        }
                    }
                }
            }
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
                value_cardinality(match object.1.type_mod_or_pattern() {
                    Some(insp::TypeModOrPattern::TypeMod(type_mod)) => Some(type_mod),
                    _ => None,
                })
                .unwrap_or(ValueCardinality::Unit),
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
                    value_cardinality(subject.1.type_mod()).unwrap_or(default.1),
                )
            };

            Relationship {
                relation_def_id,
                relation_span: self.ctx.source_span(&ident_span),
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

    fn lower_fmt_statement(
        &mut self,
        stmt: insp::FmtStatement<V>,
        block: BlockContext,
    ) -> Option<RootDefs> {
        let mut root_defs = Vec::new();
        let mut transitions = stmt.transitions().peekable();

        let Some(origin) = transitions.next() else {
            self.report_error((CompileError::TODO("missing origin"), stmt.view.span()));
            return None;
        };
        let mut origin_def_id = self.resolve_type_reference(
            origin.type_ref()?,
            &BlockContext::NoContext,
            Some(&mut root_defs),
        )?;

        let Some(mut transition) = transitions.next() else {
            self.report_error((CompileError::FmtTooFewTransitions, stmt.view.span()));
            return None;
        };

        let target = loop {
            let next_transition = match transitions.next() {
                Some(item) if transitions.peek().is_none() => {
                    // end of iterator, found the final target. Handle this outside the loop:
                    break item;
                }
                Some(item) => item,
                _ => {
                    self.report_error((CompileError::FmtTooFewTransitions, stmt.view.span()));
                    return None;
                }
            };

            let target_def_id = self.ctx.define_anonymous_type(
                TypeDef {
                    ident: None,
                    rel_type_for: None,
                    flags: TypeDefFlags::CONCRETE,
                },
                &next_transition.view().span(),
            );

            root_defs.push(self.lower_fmt_transition(
                (origin_def_id, transition.view().span()),
                transition,
                (target_def_id, next_transition.view().span()),
                FmtFinalState(false),
            )?);

            transition = next_transition;
            origin_def_id = target_def_id;
        };

        let final_def =
            self.resolve_type_reference(target.type_ref()?, &block, Some(&mut root_defs))?;

        root_defs.push(self.lower_fmt_transition(
            (origin_def_id, origin.view().span()),
            transition,
            (final_def, target.view().span()),
            FmtFinalState(true),
        )?);

        Some(root_defs)
    }

    fn lower_fmt_transition(
        &mut self,
        from: (DefId, Span),
        transition: insp::TypeMod<V>,
        to: (DefId, Span),
        final_state: FmtFinalState,
    ) -> Option<DefId> {
        let transition_def =
            self.resolve_type_reference(transition.type_ref()?, &BlockContext::FmtLeading, None)?;
        let relation_key = RelationKey::FmtTransition(transition_def, final_state);

        // This syntax just defines the relation the first time it's used
        let relation_def_id = self.ctx.define_relation_if_undefined(relation_key, &from.1);

        debug!("{:?}: <transition>", relation_def_id.0);

        Some(self.ctx.define_anonymous(
            DefKind::Relationship(Relationship {
                relation_def_id,
                relation_span: self.ctx.source_span(&from.1),
                subject: (from.0, self.ctx.source_span(&from.1)),
                subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::IndexSet),
                object: (to.0, self.ctx.source_span(&to.1)),
                object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::IndexSet),
                object_prop: None,
                rel_params: RelParams::Unit,
            }),
            &to.1,
        ))
    }

    fn lower_map_statement(
        &mut self,
        stmt: insp::MapStatement<V>,
        block_context: BlockContext,
    ) -> Option<DefId> {
        let mut var_table = MapVarTable::default();

        let mut arms = stmt.arms();
        let first = self.lower_map_arm(arms.next()?, &mut var_table)?;
        let second = self.lower_map_arm(arms.next()?, &mut var_table)?;

        let (def_id, ident) = match stmt.ident_path() {
            Some(ident_path) => {
                let symbol = ident_path.symbols().next()?;
                let (def_id, coinage) = self.catch(|zelf| {
                    zelf.ctx
                        .named_def_id(Space::Map, symbol.slice(), &symbol.span())
                })?;
                if matches!(coinage, Coinage::Used) {
                    self.report_error((CompileError::DuplicateMapIdentifier, symbol.span()));
                    return None;
                }
                let ident = self.ctx.compiler.strings.intern(symbol.slice());
                (def_id, Some(ident))
            }
            None => (
                self.ctx.compiler.defs.alloc_def_id(self.ctx.package_id),
                None,
            ),
        };

        self.ctx.set_def_kind(
            def_id,
            DefKind::Mapping {
                ident,
                arms: [first, second],
                var_alloc: var_table.into_allocator(),
                extern_def_id: match block_context {
                    BlockContext::NoContext | BlockContext::FmtLeading => None,
                    BlockContext::Context(context_fn) => {
                        let context_def_id = context_fn();
                        if matches!(
                            self.ctx.compiler.defs.def_kind(context_def_id),
                            DefKind::Extern(_)
                        ) {
                            Some(context_def_id)
                        } else {
                            None
                        }
                    }
                },
            },
            &stmt.view.span(),
        );

        Some(def_id)
    }

    fn lower_map_arm(
        &mut self,
        arm: insp::MapArm<V>,
        var_table: &mut MapVarTable,
    ) -> Option<PatId> {
        let pattern = match arm.pattern() {
            Some(p) => self.lower_pattern(p, var_table),
            None => self.mk_error_pattern(&arm.view.span()),
        };

        let pat_id = self.ctx.compiler.patterns.alloc_pat_id();
        self.ctx.compiler.patterns.table.insert(pat_id, pattern);

        Some(pat_id)
    }

    fn lower_pattern(&mut self, pat: insp::Pattern<V>, var_table: &mut MapVarTable) -> Pattern {
        match pat {
            insp::Pattern::PatStruct(pat) => self.lower_struct_pattern(pat, var_table),
            insp::Pattern::PatSet(pat) => self.lower_set_pattern(pat, var_table),
            insp::Pattern::PatAtom(pat) => self
                .lower_atom_pattern(pat, var_table)
                .unwrap_or_else(|| self.mk_error_pattern(&pat.view.span())),
            insp::Pattern::PatBinary(pat) => self.lower_binary_pattern(pat, var_table),
        }
    }

    fn lower_struct_pattern(
        &mut self,
        pat_struct: insp::PatStruct<V>,
        var_table: &mut MapVarTable,
    ) -> Pattern {
        let span = pat_struct.view.span();
        let type_path = match pat_struct.ident_path() {
            Some(ident_path) => match self.lookup_path(ident_path) {
                Some(def_id) => TypePath::Specified {
                    def_id,
                    span: self.ctx.source_span(&ident_path.view.span()),
                },
                None => return self.ctx.mk_pattern(PatternKind::Error, &span),
            },
            None => {
                let def_id = self.ctx.define_anonymous_type(
                    TypeDef {
                        ident: None,
                        rel_type_for: None,
                        flags: TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC,
                    },
                    &span,
                );
                self.ctx
                    .compiler
                    .namespaces
                    .add_anonymous(self.ctx.package_id, def_id);

                TypePath::Inferred { def_id }
            }
        };

        match self.lower_struct_pattern_params(pat_struct.params(), var_table) {
            LoweredStructPatternParams::Attrs {
                attrs,
                spread_label,
            } => {
                let mut modifier = None;
                for m in pat_struct.modifiers() {
                    if m.slice() == "@match" {
                        modifier = Some(CompoundPatternModifier::Match);
                    } else {
                        self.report_error((
                            CompileError::TODO("invalid struct modifier"),
                            m.span(),
                        ));
                    }
                }

                self.ctx.mk_pattern(
                    PatternKind::Compound {
                        type_path,
                        modifier,
                        is_unit_binding: false,
                        attributes: attrs,
                        spread_label,
                    },
                    &span,
                )
            }
            LoweredStructPatternParams::Unit(unit_pattern) => {
                if let Some(modifier) = pat_struct.modifiers().next() {
                    self.report_error((
                        CompileError::TODO("modifier not supported here"),
                        modifier.span(),
                    ));
                }

                let key = (DefId::unit(), self.ctx.source_span(&span));

                self.ctx.mk_pattern(
                    PatternKind::Compound {
                        type_path,
                        modifier: None,
                        is_unit_binding: true,
                        attributes: [CompoundPatternAttr {
                            key,
                            bind_option: false,
                            kind: CompoundPatternAttrKind::Value {
                                rel: None,
                                val: unit_pattern,
                            },
                        }]
                        .into(),
                        spread_label: None,
                    },
                    &span,
                )
            }
            LoweredStructPatternParams::Error => self.ctx.mk_pattern(PatternKind::Error, &span),
        }
    }

    fn lower_struct_pattern_params(
        &mut self,
        params: impl Iterator<Item = insp::StructParam<V>>,
        var_table: &mut MapVarTable,
    ) -> LoweredStructPatternParams {
        let mut attrs: Vec<CompoundPatternAttr> = vec![];
        let mut spread_label: Option<Box<SpreadLabel>> = None;
        let mut spread_error_reported = false;

        for param in params {
            if spread_label.is_some() && !spread_error_reported {
                self.report_error((
                    CompileError::SpreadLabelMustBeLastArgument,
                    param.view().span(),
                ));
                spread_error_reported = true;
            }

            match param {
                insp::StructParam::StructParamAttrProp(attr_prop) => {
                    if let Some(attr) = self.lower_compound_pattern_attr_prop(attr_prop, var_table)
                    {
                        attrs.push(attr);
                    }
                }
                insp::StructParam::Spread(spread) => {
                    let Some(symbol) = spread.symbol() else {
                        return LoweredStructPatternParams::Error;
                    };
                    spread_label = Some(Box::new(SpreadLabel(
                        symbol.slice().to_string(),
                        self.ctx.source_span(&symbol.span()),
                    )));
                }
                insp::StructParam::StructParamAttrUnit(attr_unit) => {
                    let unit_pattern = match attr_unit.pattern() {
                        Some(insp::Pattern::PatSet(pat_set)) => {
                            self.report_error((
                                CompileError::TODO("set pattern not allowed here"),
                                pat_set.view.span(),
                            ));
                            return LoweredStructPatternParams::Error;
                        }
                        None => return LoweredStructPatternParams::Error,
                        Some(pattern) => self.lower_pattern(pattern, var_table),
                    };
                    return LoweredStructPatternParams::Unit(unit_pattern);
                }
            }
        }

        LoweredStructPatternParams::Attrs {
            attrs: attrs.into_boxed_slice(),
            spread_label,
        }
    }

    fn lower_compound_pattern_attr_prop(
        &mut self,
        attr_prop: insp::StructParamAttrProp<V>,
        var_table: &mut MapVarTable,
    ) -> Option<CompoundPatternAttr> {
        let relation = attr_prop.relation()?;
        let relation_span = self.ctx.source_span(&relation.view().span());
        let relation_def =
            self.resolve_type_reference(relation.type_ref()?, &BlockContext::NoContext, None)?;
        let bind_option = attr_prop
            .prop_cardinality()
            .and_then(|pc| pc.question())
            .is_some();

        let Some(cst_pattern) = attr_prop.pattern() else {
            return Some(CompoundPatternAttr {
                key: (relation_def, relation_span),
                bind_option,
                kind: CompoundPatternAttrKind::Value {
                    rel: None,
                    val: self.mk_error_pattern(&attr_prop.view.span()),
                },
            });
        };

        match cst_pattern {
            insp::Pattern::PatSet(pat_set) => {
                if let Some(rel_args) = attr_prop.rel_args() {
                    self.report_error((
                        CompileError::TODO(
                            "relation arguments must be associated with each element",
                        ),
                        rel_args.view.span(),
                    ));
                    return None;
                }

                let kind = match self.get_set_binary_operator(pat_set.modifier()) {
                    Some((operator, _span)) => {
                        self.lower_set_algebra_pattern(pat_set, operator, var_table)?
                    }
                    None => {
                        let object_pattern = self.lower_pattern(pat_set.into(), var_table);

                        CompoundPatternAttrKind::Value {
                            rel: None,
                            val: object_pattern,
                        }
                    }
                };

                Some(CompoundPatternAttr {
                    key: (relation_def, relation_span),
                    bind_option,
                    kind,
                })
            }
            obj_pattern => {
                let object_pattern = self.lower_pattern(obj_pattern, var_table);
                let rel = self.lower_compound_pattern_attr_rel_args(
                    attr_prop.rel_args(),
                    &object_pattern,
                    var_table,
                )?;

                Some(CompoundPatternAttr {
                    key: (relation_def, relation_span),
                    bind_option,
                    kind: CompoundPatternAttrKind::Value {
                        rel,
                        val: object_pattern,
                    },
                })
            }
        }
    }

    fn lower_compound_pattern_attr_rel_args(
        &mut self,
        rel_args: Option<insp::RelArgs<V>>,
        object_pattern: &Pattern,
        var_table: &mut MapVarTable,
    ) -> Option<Option<Pattern>> {
        let Some(rel_args) = rel_args else {
            return Some(None);
        };
        // Inherit modifier from object pattern
        let modifier = match &object_pattern {
            Pattern {
                kind: PatternKind::Compound { modifier, .. },
                ..
            } => *modifier,
            _ => None,
        };

        match self.lower_struct_pattern_params(rel_args.params(), var_table) {
            LoweredStructPatternParams::Attrs {
                attrs,
                spread_label,
            } => Some(Some(self.ctx.mk_pattern(
                PatternKind::Compound {
                    type_path: TypePath::RelContextual,
                    modifier,
                    is_unit_binding: false,
                    attributes: attrs,
                    spread_label,
                },
                &rel_args.view.span(),
            ))),
            LoweredStructPatternParams::Unit(unit_pattern) => {
                self.ctx.compiler.push_error(
                    CompileError::TODO("unit not supported here").spanned(&unit_pattern.span),
                );
                None
            }
            LoweredStructPatternParams::Error => {
                Some(Some(self.mk_error_pattern(&rel_args.view.span())))
            }
        }
    }

    fn lower_set_pattern(
        &mut self,
        pat_set: insp::PatSet<V>,
        var_table: &mut MapVarTable,
    ) -> Pattern {
        let seq_type = pat_set
            .ident_path()
            .and_then(|ident_path| self.lookup_path(ident_path));

        let mut pattern_elements = vec![];
        for element in pat_set.elements() {
            let pattern = element
                .pattern()
                .map(|pattern| self.lower_pattern(pattern, var_table))
                .unwrap_or_else(|| self.mk_error_pattern(&element.view.span()));

            let rel = if let Some(rel_args) = element.rel_args() {
                let mut lowered_attrs = vec![];

                let mut spread_label = None;

                for param in rel_args.params() {
                    match param {
                        insp::StructParam::StructParamAttrProp(attr_prop) => {
                            if let Some(attr_prop) =
                                self.lower_compound_pattern_attr_prop(attr_prop, var_table)
                            {
                                lowered_attrs.push(attr_prop);
                            }
                        }
                        insp::StructParam::StructParamAttrUnit(_unit) => {
                            self.report_error((
                                CompileError::TODO("unit cannot be used here"),
                                param.view().span(),
                            ));
                        }
                        insp::StructParam::Spread(spread) => {
                            if let Some(symbol) = spread.symbol() {
                                spread_label = Some(Box::new(SpreadLabel(
                                    symbol.slice().to_string(),
                                    self.ctx.source_span(&symbol.span()),
                                )));
                            }
                        }
                    }
                }

                Some(Pattern {
                    id: self.ctx.compiler.patterns.alloc_pat_id(),
                    kind: PatternKind::Compound {
                        type_path: TypePath::RelContextual,
                        modifier: None,
                        is_unit_binding: false,
                        attributes: lowered_attrs.into(),
                        spread_label,
                    },
                    span: self.ctx.source_span(&rel_args.view.span()),
                })
            } else {
                None
            };

            pattern_elements.push(SetPatternElement {
                id: self.ctx.compiler.patterns.alloc_pat_id(),
                is_iter: element.spread().is_some(),
                rel,
                val: pattern,
            })
        }

        self.ctx.mk_pattern(
            PatternKind::Set {
                val_type_def: seq_type,
                elements: pattern_elements.into_boxed_slice(),
            },
            &pat_set.view.span(),
        )
    }

    fn lower_set_algebra_pattern(
        &mut self,
        pat_set: insp::PatSet<V>,
        operator: SetBinaryOperator,
        var_table: &mut MapVarTable,
    ) -> Option<CompoundPatternAttrKind> {
        let mut elements = vec![];

        for element in pat_set.elements() {
            let rel = match element.rel_args() {
                Some(rel_args) => {
                    let params = self.lower_struct_pattern_params(rel_args.params(), var_table);
                    Some(self.ctx.mk_pattern(
                        PatternKind::Compound {
                            type_path: TypePath::RelContextual,
                            modifier: None,
                            is_unit_binding: false,
                            attributes: match params {
                                LoweredStructPatternParams::Attrs { attrs, .. } => attrs,
                                _ => {
                                    continue;
                                }
                            },
                            spread_label: None,
                        },
                        &rel_args.view.span(),
                    ))
                }
                None => None,
            };
            let val = element
                .pattern()
                .map(|pattern| self.lower_pattern(pattern, var_table))
                .unwrap_or_else(|| self.mk_error_pattern(&element.view.span()));

            elements.push(SetElement {
                iter: element.spread().is_some(),
                rel,
                val,
            });
        }

        if elements.is_empty() {
            self.report_error((
                CompileError::TODO("inner set must have at least one element"),
                pat_set.view.span(),
            ));
        }

        Some(CompoundPatternAttrKind::SetOperator {
            operator,
            elements: elements
                .into_iter()
                .map(|SetElement { iter, rel, val }| SetPatternElement {
                    id: self.ctx.compiler.patterns.alloc_pat_id(),
                    is_iter: iter,
                    rel,
                    val,
                })
                .collect(),
        })
    }

    fn lower_atom_pattern(
        &mut self,
        pat_atom: insp::PatAtom<V>,
        var_table: &mut MapVarTable,
    ) -> Option<Pattern> {
        let token = pat_atom.view.local_tokens().next()?;
        let span = token.span();

        match token.kind() {
            Kind::Number => match token.slice().parse::<i64>() {
                Ok(int) => Some(self.ctx.mk_pattern(PatternKind::ConstI64(int), &span)),
                Err(_) => {
                    self.report_error((CompileError::InvalidInteger, span));
                    None
                }
            },
            Kind::SingleQuoteText | Kind::DoubleQuoteText => {
                let text = self.unescape(token.literal_text()?)?;
                Some(self.ctx.mk_pattern(PatternKind::ConstText(text), &span))
            }
            Kind::Regex => {
                let regex_literal = unescape_regex(token.slice());
                let regex_def_id = match self.ctx.compiler.defs.def_regex(
                    &regex_literal,
                    &span,
                    &mut self.ctx.compiler.strings,
                ) {
                    Ok(def_id) => def_id,
                    Err((compile_error, err_span)) => {
                        self.report_error((CompileError::InvalidRegex(compile_error), err_span));
                        return None;
                    }
                };
                let regex_meta = self
                    .ctx
                    .compiler
                    .defs
                    .literal_regex_meta_table
                    .get(&regex_def_id)
                    .unwrap();

                let mut regex_lowerer = RegexToPatternLowerer::new(
                    regex_meta.pattern,
                    &span,
                    self.ctx.source_id,
                    var_table,
                    &mut self.ctx.compiler.patterns,
                );

                regex_syntax::ast::visit(&regex_meta.ast, regex_lowerer.syntax_visitor()).unwrap();
                regex_syntax::hir::visit(&regex_meta.hir, regex_lowerer.syntax_visitor()).unwrap();

                let expr_regex = regex_lowerer.into_expr(regex_def_id);

                Some(self.ctx.mk_pattern(PatternKind::Regex(expr_regex), &span))
            }
            Kind::Sym => match token.slice() {
                "true" => Some(self.ctx.mk_pattern(PatternKind::ConstBool(true), &span)),
                "false" => Some(self.ctx.mk_pattern(PatternKind::ConstBool(false), &span)),
                ident => {
                    let var = var_table.get_or_create_var(ident.to_string());
                    Some(self.ctx.mk_pattern(PatternKind::Variable(var), &span))
                }
            },
            _ => None,
        }
    }

    fn lower_binary_pattern(
        &mut self,
        pat_atom: insp::PatBinary<V>,
        var_table: &mut MapVarTable,
    ) -> Pattern {
        let span = pat_atom.view.span();
        let mut operands = pat_atom.operands();
        let left = operands
            .next()
            .map(|op| self.lower_pattern(op, var_table))
            .unwrap_or_else(|| self.mk_error_pattern(&span));

        let Some(infix_token) = pat_atom.infix_token() else {
            return self.mk_error_pattern(&span);
        };

        let fn_ident = match infix_token.kind() {
            Kind::Plus => "+",
            Kind::Minus => "-",
            Kind::Star => "*",
            Kind::Div => "/",
            _ => {
                self.report_error((
                    CompileError::TODO("invalid infix operator"),
                    infix_token.span(),
                ));
                return self.mk_error_pattern(&span);
            }
        };

        let Some(def_id) = self.catch(|zelf| zelf.ctx.lookup_ident(fn_ident, &infix_token.span()))
        else {
            return self.mk_error_pattern(&span);
        };

        let right = operands
            .next()
            .map(|op| self.lower_pattern(op, var_table))
            .unwrap_or_else(|| self.mk_error_pattern(&span));

        self.ctx.mk_pattern(
            PatternKind::Call(def_id, Box::new([left, right])),
            &pat_atom.view.span(),
        )
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
            (insp::TypeRef::This(this), BlockContext::FmtLeading) => {
                self.report_error((CompileError::FmtMisplacedSelf, this.view.span()));
                None
            }
            (insp::TypeRef::Literal(literal), _) => {
                let token = literal.view.local_tokens().next()?;
                match token.kind() {
                    Kind::Number => {
                        let lit = self.ctx.compiler.strings.intern(token.slice());
                        let def_id = self.ctx.compiler.defs.add_def(
                            DefKind::NumberLiteral(lit),
                            ONTOL_PKG,
                            self.ctx.source_span(&token.span()),
                        );
                        Some(def_id)
                    }
                    Kind::SingleQuoteText | Kind::DoubleQuoteText => {
                        let unescaped = self.unescape(token.literal_text()?)?;
                        match unescaped.as_str() {
                            "" => Some(self.ctx.compiler.primitives.empty_text),
                            other => Some(
                                self.ctx
                                    .compiler
                                    .defs
                                    .def_text_literal(other, &mut self.ctx.compiler.strings),
                            ),
                        }
                    }
                    Kind::Regex => {
                        let regex_literal = unescape_regex(token.slice());
                        match self.ctx.compiler.defs.def_regex(
                            &regex_literal,
                            &token.span(),
                            &mut self.ctx.compiler.strings,
                        ) {
                            Ok(def_id) => Some(def_id),
                            Err((compile_error, span)) => {
                                self.report_error((
                                    CompileError::InvalidRegex(compile_error),
                                    span,
                                ));
                                None
                            }
                        }
                    }
                    kind => unimplemented!("literal type: {kind}"),
                }
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
            (insp::TypeRef::NumberRange(range), _) => {
                self.report_error((
                    CompileError::TODO("number range is not a proper type"),
                    range.view.span(),
                ));
                None
            }
        }
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

    fn unescape(&mut self, result: Result<String, Vec<ParserError>>) -> Option<String> {
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
                    self.report_error((
                        CompileError::TODO("invalid def modifier"),
                        modifier.span(),
                    ));
                }
            }
        }

        (private, open, extern_, symbol)
    }

    fn get_set_binary_operator(
        &mut self,
        modifier: Option<V::Token>,
    ) -> Option<(SetBinaryOperator, SourceSpan)> {
        let modifier = modifier?;
        let span = self.ctx.source_span(&modifier.span());

        match modifier.slice() {
            "@in" => Some((SetBinaryOperator::ElementIn, span)),
            "@all_in" => Some((SetBinaryOperator::AllIn, span)),
            "@contains_all" => Some((SetBinaryOperator::ContainsAll, span)),
            "@intersects" => Some((SetBinaryOperator::Intersects, span)),
            "@equals" => Some((SetBinaryOperator::SetEquals, span)),
            _ => {
                self.report_error((
                    CompileError::TODO("invalid set binary operator"),
                    modifier.span(),
                ));
                None
            }
        }
    }

    fn lower_u16_range(&mut self, range: insp::NumberRange<V>) -> Range<Option<u16>> {
        let start = range.start().and_then(|token| self.token_to_u16(token));
        let end = range.end().and_then(|token| self.token_to_u16(token));

        start..end
    }

    fn token_to_u16(&mut self, token: V::Token) -> Option<u16> {
        match token.slice().parse::<u16>() {
            Ok(number) => Some(number),
            Err(_error) => {
                self.report_error((
                    CompileError::NumberParse("unable to parse integer".to_string()),
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

    fn mk_error_pattern(&mut self, span: &Range<usize>) -> Pattern {
        self.ctx.mk_pattern(PatternKind::Error, span)
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
