use std::{collections::hash_map::Entry, ops::Range};

use ontol_parser::{
    cst::{
        inspect::{self as insp},
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt},
    },
    lexer::{kind::Kind, unescape::unescape_regex},
    ParserError, U32Span,
};
use ontol_runtime::DefId;

use crate::{
    def::{DefKind, TypeDef, TypeDefFlags},
    lowering::context::LoweringCtx,
    package::ONTOL_PKG,
    pattern::{Pattern, PatternKind, SetBinaryOperator},
    CompileError, SourceSpan,
};

use super::context::{BlockContext, CstLowering, Extern, Open, Private, Res, RootDefs, Symbol};

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub(super) fn resolve_type_reference(
        &mut self,
        type_ref: insp::TypeRef<V>,
        block_context: &BlockContext,
        root_defs: Option<&mut RootDefs>,
    ) -> Option<DefId> {
        match (type_ref, block_context) {
            (insp::TypeRef::IdentPath(path), _) => self.lookup_path(&path),
            (insp::TypeRef::This(_), BlockContext::Context(func)) => Some(func()),
            (insp::TypeRef::This(this), BlockContext::NoContext) => {
                CompileError::WildcardNeedsContextualBlock
                    .span_report(this.0.span(), &mut self.ctx);
                None
            }
            (insp::TypeRef::This(this), BlockContext::FmtLeading) => {
                CompileError::FmtMisplacedSelf.span_report(this.0.span(), &mut self.ctx);
                None
            }
            (insp::TypeRef::Literal(literal), _) => {
                let token = literal.0.local_tokens().next()?;
                match token.kind() {
                    Kind::Number => {
                        let lit = self.ctx.compiler.strings.intern(token.slice());
                        let def_id = self.ctx.compiler.defs.add_def(
                            DefKind::NumberLiteral(lit),
                            ONTOL_PKG,
                            self.ctx.source_span(token.span()),
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
                            token.span(),
                            &mut self.ctx.compiler.strings,
                        ) {
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
                    return Some(self.ctx.compiler.primitives.unit);
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
                CompileError::TODO("number range is not a proper type")
                    .span_report(range.0.span(), &mut self.ctx);
                None
            }
        }
    }

    pub(super) fn lookup_path(&mut self, ident_path: &insp::IdentPath<V>) -> Option<DefId> {
        self.catch(|zelf| {
            zelf.ctx
                .lookup_path::<V>(ident_path.symbols(), ident_path.0.span())
        })
    }

    pub(super) fn unescape(&mut self, result: Result<String, Vec<ParserError>>) -> Option<String> {
        match result {
            Ok(string) => Some(string),
            Err(unescape_errors) => {
                for error in unescape_errors {
                    CompileError::TODO(error.msg).span_report(error.span, &mut self.ctx);
                }

                None
            }
        }
    }

    pub(super) fn read_def_modifiers(
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
                    CompileError::InvalidModifier.span_report(modifier.span(), &mut self.ctx);
                }
            }
        }

        (private, open, extern_, symbol)
    }

    pub(super) fn get_set_binary_operator(
        &mut self,
        modifier: Option<V::Token>,
    ) -> Option<(SetBinaryOperator, SourceSpan)> {
        let modifier = modifier?;
        let span = self.ctx.source_span(modifier.span());

        match modifier.slice() {
            "@in" => Some((SetBinaryOperator::ElementIn, span)),
            "@all_in" => Some((SetBinaryOperator::AllIn, span)),
            "@contains_all" => Some((SetBinaryOperator::ContainsAll, span)),
            "@intersects" => Some((SetBinaryOperator::Intersects, span)),
            "@equals" => Some((SetBinaryOperator::SetEquals, span)),
            _ => {
                CompileError::TODO("invalid set binary operator")
                    .span_report(modifier.span(), &mut self.ctx);
                None
            }
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

    pub(super) fn append_documentation(&mut self, def_id: DefId, node_view: V) {
        let doc_comments = node_view
            .local_tokens_filter(Kind::DocComment)
            .map(|token| token.slice().strip_prefix("///").unwrap().to_string());

        let Some(docs) = ontol_parser::join_doc_lines(doc_comments) else {
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

    pub(super) fn mk_error_pattern(&mut self, span: U32Span) -> Pattern {
        self.ctx.mk_pattern(PatternKind::Error, span)
    }

    pub(super) fn catch<T>(&mut self, f: impl FnOnce(&mut Self) -> Res<T>) -> Option<T> {
        match f(self) {
            Ok(value) => Some(value),
            Err((err, span)) => {
                err.span_report(span, &mut self.ctx);
                None
            }
        }
    }
}

impl CompileError {
    pub(super) fn span_report(self, span: U32Span, ctx: &mut LoweringCtx) {
        self.span(ctx.source_span(span)).report(ctx.compiler);
    }
}
