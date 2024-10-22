use std::{
    collections::{hash_map::Entry, BTreeSet},
    ops::Range,
};

use arcstr::ArcStr;
use ontol_parser::{
    cst::{
        inspect as insp,
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt},
    },
    lexer::{kind::Kind, unescape::unescape_regex},
    U32Span,
};
use ontol_runtime::{property::ValueCardinality, DefId, DomainIndex, OntolDefTag};
use tracing::debug;

use crate::{
    def::{DefKind, TypeDef, TypeDefFlags},
    namespace::DocId,
    CompileError,
};

use super::context::{BlockContext, CstLowering, Res, RootDefs};

#[derive(Clone, Copy)]
pub struct ResolvedType {
    pub def_id: DefId,
    pub cardinality: ValueCardinality,
    pub span: U32Span,
}

pub enum ReportError {
    Yes,
    No,
}

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub(super) fn catch<T>(&mut self, f: impl FnOnce(&mut Self) -> Res<T>) -> Option<T> {
        match f(self) {
            Ok(value) => Some(value),
            Err((err, span)) => {
                err.span_report(span, &mut self.ctx);
                None
            }
        }
    }

    pub(super) fn resolve_quant_type_reference(
        &mut self,
        type_quant: insp::TypeQuant<V>,
        block_context: &BlockContext,
        root_defs: Option<&mut RootDefs>,
        report_error: ReportError,
    ) -> Option<ResolvedType> {
        let span = type_quant.view().span();
        let resolved_type = match type_quant {
            insp::TypeQuant::TypeQuantUnit(unit) => self.resolve_type_reference(
                unit.type_ref()?,
                ValueCardinality::Unit,
                block_context,
                report_error,
                root_defs,
            ),
            insp::TypeQuant::TypeQuantSet(set) => self.resolve_type_reference(
                set.type_ref()?,
                ValueCardinality::IndexSet,
                block_context,
                report_error,
                root_defs,
            ),
            insp::TypeQuant::TypeQuantList(list) => self.resolve_type_reference(
                list.type_ref()?,
                ValueCardinality::List,
                block_context,
                report_error,
                root_defs,
            ),
        }?;

        // extend span to cover the quantified range
        Some(ResolvedType {
            def_id: resolved_type.def_id,
            cardinality: resolved_type.cardinality,
            span,
        })
    }

    pub(super) fn resolve_type_reference(
        &mut self,
        type_ref: insp::TypeRef<V>,
        mut syntax_cardinality: ValueCardinality,
        block_context: &BlockContext,
        report_error: ReportError,
        root_defs: Option<&mut RootDefs>,
    ) -> Option<ResolvedType> {
        let span = type_ref.view().span();

        let def_id = match (type_ref, block_context) {
            (insp::TypeRef::IdentPath(path), _) => self.lookup_path(&path, report_error),
            (
                insp::TypeRef::ThisUnit(_),
                BlockContext::SubDef(def_fn) | BlockContext::RelParams { def_fn, .. },
            ) => Some(def_fn()),
            (
                insp::TypeRef::ThisSet(_),
                BlockContext::SubDef(def_fn) | BlockContext::RelParams { def_fn, .. },
            ) => {
                syntax_cardinality = ValueCardinality::IndexSet;
                Some(def_fn())
            }
            (insp::TypeRef::ThisUnit(_) | insp::TypeRef::ThisSet(_), BlockContext::NoContext) => {
                CompileError::WildcardNeedsContextualBlock.span_report(span, &mut self.ctx);
                None
            }
            (insp::TypeRef::ThisUnit(_) | insp::TypeRef::ThisSet(_), BlockContext::FmtLeading) => {
                CompileError::FmtMisplacedSelf.span_report(span, &mut self.ctx);
                None
            }
            (insp::TypeRef::Literal(literal), _) => {
                let token = literal.0.local_tokens().next()?;
                match token.kind() {
                    Kind::Number => {
                        let lit = self.ctx.compiler.str_ctx.intern(token.slice());
                        let def_id = self.ctx.compiler.defs.add_def(
                            DefKind::NumberLiteral(lit),
                            DomainIndex::ontol(),
                            self.ctx.source_span(token.span()),
                        );
                        Some(def_id)
                    }
                    Kind::SingleQuoteText | Kind::DoubleQuoteText => {
                        let unescaped = self.ctx.unescape(token.literal_text()?)?;
                        Some(self.unescaped_text_literal_def_id(unescaped.as_str()))
                    }
                    Kind::Regex => {
                        let regex_literal = unescape_regex(token.slice());
                        match self.ctx.compiler.def_regex(&regex_literal, token.span()) {
                            Ok(def_id) => Some(def_id),
                            Err((compile_error, span)) => {
                                CompileError::InvalidRegex(compile_error)
                                    .span_report(span, &mut self.ctx);
                                None
                            }
                        }
                    }
                    kind => unimplemented!("literal type: {kind}"),
                }
            }
            (insp::TypeRef::DefBody(body), _) => {
                if body.statements().next().is_none() {
                    return Some(ResolvedType {
                        def_id: OntolDefTag::Unit.def_id(),
                        cardinality: syntax_cardinality,
                        span: body.view().span(),
                    });
                }

                let Some(root_defs) = root_defs else {
                    CompileError::TODO("Anonymous struct not allowed here")
                        .span_report(body.0.span(), &mut self.ctx);
                    return None;
                };

                let def_id = self.ctx.define_anonymous_type(
                    TypeDef {
                        ident: None,
                        rel_type_for: None,
                        flags: TypeDefFlags::CONCRETE,
                    },
                    body.0.span(),
                );

                // This type needs to be part of the anonymous part of the namespace
                self.ctx
                    .compiler
                    .namespaces
                    .add_anonymous(self.ctx.domain_def_id, def_id);
                root_defs.push(def_id);

                let context_fn = || def_id;

                for statement in body.statements() {
                    if let Some(mut defs) =
                        self.lower_statement(statement, BlockContext::SubDef(&context_fn))
                    {
                        root_defs.append(&mut defs);
                    }
                }

                Some(def_id)
            }
            (insp::TypeRef::NumberRange(range), _) => {
                CompileError::TODO("number range is not a proper type")
                    .span_report(range.0.span(), &mut self.ctx);
                None
            }
            (insp::TypeRef::TypeUnion(type_union), _) => {
                let mut members: BTreeSet<DefId> = Default::default();

                for type_ref in type_union.members() {
                    if let insp::TypeRef::IdentPath(path) = type_ref {
                        if let Some(member_def_id) = self.lookup_path(&path, ReportError::Yes) {
                            members.insert(member_def_id);
                        }
                    } else {
                        CompileError::TODO("union member must be a path")
                            .span_report(type_ref.view().span(), &mut self.ctx);
                    }
                }

                if let Some(union_def_id) = self.ctx.anonymous_unions.get(&members) {
                    Some(*union_def_id)
                } else {
                    let union_def_id = self.ctx.compiler.defs.alloc_def_id(self.ctx.domain_index);
                    self.ctx.set_def_kind(
                        union_def_id,
                        DefKind::InlineUnion(members.clone()),
                        type_union.view().span(),
                    );

                    // add to cache
                    self.ctx.anonymous_unions.insert(members, union_def_id);

                    type_union.view();

                    debug!(
                        "{union_def_id:?}: <inline_union {}>",
                        type_union.view().display()
                    );

                    self.ctx
                        .compiler
                        .namespaces
                        .add_anonymous(self.ctx.domain_def_id, union_def_id);

                    Some(union_def_id)
                }
            }
        };

        Some(ResolvedType {
            def_id: def_id?,
            cardinality: syntax_cardinality,
            span,
        })
    }

    pub(super) fn unescaped_text_literal_def_id(&mut self, unescaped: &str) -> DefId {
        match unescaped {
            "" => OntolDefTag::EmptyText.def_id(),
            other => self
                .ctx
                .compiler
                .defs
                .def_text_literal(other, &mut self.ctx.compiler.str_ctx),
        }
    }

    pub(super) fn lookup_path(
        &mut self,
        ident_path: &insp::IdentPath<V>,
        report_error: ReportError,
    ) -> Option<DefId> {
        match report_error {
            ReportError::Yes => self.catch(|zelf| {
                zelf.ctx
                    .lookup_path::<V>(ident_path.symbols(), ident_path.0.span())
            }),
            ReportError::No => self
                .ctx
                .lookup_path::<V>(ident_path.symbols(), ident_path.0.span())
                .ok(),
        }
    }

    pub(super) fn lower_u16_range(&mut self, range: insp::NumberRange<V>) -> Range<Option<u16>> {
        let start = range.start().and_then(|token| self.token_to_u16(token));
        let end = range.end().and_then(|token| self.token_to_u16(token));

        start..end
    }

    fn token_to_u16(&mut self, token: V::Token) -> Option<u16> {
        match token.slice().parse::<u16>() {
            Ok(number) => Some(number),
            Err(_error) => {
                CompileError::NumberParse("unable to parse integer".to_string())
                    .span_report(token.span(), &mut self.ctx);
                None
            }
        }
    }

    pub(super) fn extract_documentation(node_view: V) -> Option<ArcStr> {
        let doc_comments = node_view
            .local_tokens_filter(Kind::DocComment)
            .map(|token| token.slice().strip_prefix("///").unwrap().to_string());

        ontol_parser::join_doc_lines(doc_comments).map(|s| s.into())
    }

    pub(super) fn append_documentation(&mut self, doc_id: DocId, node_view: V) {
        let Some(docs) = Self::extract_documentation(node_view) else {
            return;
        };

        match self.ctx.compiler.namespaces.docs.entry(doc_id) {
            Entry::Vacant(vacant) => {
                vacant.insert(docs);
            }
            Entry::Occupied(mut occupied) => {
                let mut string = occupied.get().to_string();

                string.push_str("\n\n");
                string.push_str(&docs);

                *occupied.get_mut() = string.into();
            }
        }
    }
}
