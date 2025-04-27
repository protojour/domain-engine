use std::{backtrace::Backtrace, collections::BTreeSet};

use either::Either;
use ontol_syntax::rowan::GreenToken;

use ontol_core::{LogRef, property::ValueCardinality, tag::OntolDefTag};
use ontol_parser::{
    cst::{
        inspect::{self as insp, TypeQuant},
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt, TypedView},
    },
    lexer::{kind::Kind, unescape::unescape_regex},
};
use ontol_syntax::syntax_view::{RowanChildren, RowanNodeView};
use thin_vec::{ThinVec, thin_vec};
use tracing::{error, warn};

use crate::{
    analyzer::BlockContext,
    error::{OptionExt, SemError},
    log_model::{OntolTree, PathRef, Pattern, TypeRef, TypeRefOrUnionOrPattern, TypeUnion},
    symbol::SymEntryRef,
    tag::Tag,
    token::Token,
    with_span::{SetSpan, WithSpan},
};

#[derive(Clone, Copy)]
pub enum LookupScope {
    Local,
    Foreign(LogRef, Tag),
}

pub enum LookupError {
    Syntax,
}

pub trait LookupTable {
    fn lookup_root(&self, token: &GreenToken) -> Option<SymEntryRef<'_>>;

    fn lookup_next<'s>(
        &'s self,
        scope: LookupScope,
        prev: SymEntryRef<'s>,
        token: &GreenToken,
    ) -> Option<SymEntryRef<'s>>;
}

pub fn resolve_quant_type_reference(
    q: insp::TypeQuant<RowanNodeView>,
    context: BlockContext,
    table: &impl LookupTable,
) -> Result<(Either<WithSpan<TypeRef>, TypeUnion>, ValueCardinality), SemError> {
    match q {
        TypeQuant::TypeQuantUnit(t) => {
            let mut vc = ValueCardinality::Unit;
            let tr = resolve_type_reference(t.type_ref().stx_err()?, &mut vc, context, table)?;
            Ok((tr, vc))
        }
        TypeQuant::TypeQuantSet(t) => {
            let mut vc = ValueCardinality::IndexSet;
            let tr = resolve_type_reference(t.type_ref().stx_err()?, &mut vc, context, table)?;
            Ok((tr, vc))
        }
        TypeQuant::TypeQuantList(t) => {
            let mut vc = ValueCardinality::List;
            let tr = resolve_type_reference(t.type_ref().stx_err()?, &mut vc, context, table)?;
            Ok((tr, vc))
        }
    }
}

pub fn resolve_quant_type_reference_or_pattern(
    qp: insp::TypeQuantOrPattern<RowanNodeView>,
    context: BlockContext,
    table: &impl LookupTable,
) -> Result<(TypeRefOrUnionOrPattern, ValueCardinality), SemError> {
    match qp {
        insp::TypeQuantOrPattern::TypeQuant(q) => {
            let (tp, crd) = resolve_quant_type_reference(q, context, table)?;
            Ok((tp.into(), crd))
        }
        insp::TypeQuantOrPattern::Pattern(pat) => Ok((
            TypeRefOrUnionOrPattern::Pattern(resolve_pattern(pat, table)?),
            ValueCardinality::Unit,
        )),
    }
}

pub fn resolve_type_reference(
    type_ref: insp::TypeRef<RowanNodeView>,
    syntax_cardinality: &mut ValueCardinality,
    context: BlockContext,
    table: &impl LookupTable,
) -> Result<Either<WithSpan<TypeRef>, TypeUnion>, SemError> {
    let span = type_ref.view().span();

    match (type_ref, context) {
        (insp::TypeRef::IdentPath(path), _) => Ok(lookup_type_path(&path, table)?.into()),
        (insp::TypeRef::ThisUnit(_), BlockContext::Def(tag)) => {
            Ok(TypeRef::Path(PathRef::Local(tag)).set_span(span).into())
        }
        (insp::TypeRef::ThisUnit(_), BlockContext::RelParams(_tag)) => {
            Ok(TypeRef::Path(PathRef::ParentRel).set_span(span).into())
        }
        (insp::TypeRef::ThisSet(_), BlockContext::Def(tag)) => {
            *syntax_cardinality = ValueCardinality::IndexSet;
            Ok(TypeRef::Path(PathRef::Local(tag)).set_span(span).into())
        }
        (insp::TypeRef::ThisSet(_), BlockContext::RelParams(_tag)) => {
            *syntax_cardinality = ValueCardinality::IndexSet;
            Ok(TypeRef::Path(PathRef::ParentRel).set_span(span).into())
        }
        (insp::TypeRef::ThisSet(_) | insp::TypeRef::ThisUnit(_), _) => {
            Err(SemError::MissingContext(span))
        }
        (insp::TypeRef::Literal(literal), _) => {
            let token = literal.0.local_tokens().next().stx_err()?;
            match token.kind() {
                Kind::Number => Ok(TypeRef::Number(token.slice().into()).set_span(span).into()),
                Kind::SingleQuoteText | Kind::DoubleQuoteText => {
                    let unescaped = token.literal_text().stx_err()?.ok().stx_err()?;
                    Ok(TypeRef::Text(unescaped.into()).set_span(span).into())
                }
                Kind::Regex => {
                    let regex_literal = unescape_regex(token.slice());
                    Ok(TypeRef::Regex(regex_literal.into()).set_span(span).into())
                }
                kind => unimplemented!("literal type: {kind}"),
            }
        }
        (insp::TypeRef::DefBody(body), _) => {
            if body.statements().next().is_none() {
                Ok(TypeRef::Path(PathRef::Ontol(OntolDefTag::Unit))
                    .set_span(span)
                    .into())
            } else {
                Ok(
                    TypeRef::Anonymous(ontol_node_children(body.view().children()))
                        .set_span(span)
                        .into(),
                )
            }
        }
        (insp::TypeRef::NumberRange(range), _) => {
            let start = range.start().map(|tok| tok.slice().into());
            let end = range.end().map(|tok| tok.slice().into());

            Ok(TypeRef::NumberRange(start, end).set_span(span).into())
        }
        (insp::TypeRef::TypeUnion(type_union), _) => {
            let mut u = BTreeSet::new();

            for member in type_union.members() {
                match resolve_type_reference(member, &mut ValueCardinality::Unit, context, table)? {
                    Either::Left(t) => {
                        u.insert(t);
                    }
                    Either::Right(_) => {
                        return Err(SemError::UnionInUnion);
                    }
                }
            }

            Ok(Either::Right(u))
        }
    }
}

enum PathScope {
    Local,
    ForeignUse(Tag),
}

pub fn lookup_type_path(
    path: &insp::IdentPath<RowanNodeView>,
    table: &impl LookupTable,
) -> Result<WithSpan<TypeRef>, SemError> {
    let span = path.view().span();
    let mut symbols = path.symbols().peekable();

    let first_symbol = symbols.next().stx_err()?;
    let mut current_span = first_symbol.span();
    let first_symbol = first_symbol.0.green().to_owned();

    let mut entry = table.lookup_root(&first_symbol);

    let mut path_scope = PathScope::Local;
    let mut lookup_scope = LookupScope::Local;

    for next in symbols {
        let prev = match entry {
            Some(entry) => entry,
            None => {
                return Err(SemError::Lookup(Token(first_symbol), span));
            }
        };

        match &prev {
            SymEntryRef::Use(use_tag) => {
                path_scope = PathScope::ForeignUse(*use_tag);
                // FIXME: resolve the lookup scope parameters
                lookup_scope = LookupScope::Foreign(LogRef(42), Tag(42));
            }
            SymEntryRef::Ontol => {}
            SymEntryRef::OntolModule(_) => {}
            SymEntryRef::LocalDef(_) | SymEntryRef::EdgeSymbol(..) | SymEntryRef::OntolDef(_) => {
                // sub-scoping not possible
                return Err(SemError::SubScoping(next.span()));
            }
            SymEntryRef::LocalEdge(_edge_tag) => {}
        }

        current_span = next.span();
        entry = match table.lookup_next(lookup_scope, prev, &next.0.green().to_owned()) {
            Some(entry) => Some(entry),
            None => {
                warn!("lookup next error: {:?} prev: {prev:?}", next.0.text());
                return Err(SemError::Lookup(
                    Token(next.0.green().to_owned()),
                    current_span,
                ));
            }
        };
    }

    match entry {
        Some(SymEntryRef::Use(_tag)) => Err(SemError::Lookup(Token(first_symbol), current_span)),
        Some(SymEntryRef::LocalDef(tag)) => Ok(TypeRef::Path(match path_scope {
            PathScope::Local => PathRef::Local(tag),
            PathScope::ForeignUse(use_tag) => PathRef::Foreign(use_tag, tag),
        })
        .set_span(span)),
        Some(SymEntryRef::LocalEdge(_tag)) => {
            Err(SemError::Lookup(Token(first_symbol), current_span))
        }
        Some(SymEntryRef::Ontol | SymEntryRef::OntolModule(_)) => {
            error!("error: lookup ontol module");
            Err(SemError::Lookup(Token(first_symbol), current_span))
        }
        Some(SymEntryRef::EdgeSymbol(tag, coord)) => Ok(TypeRef::Path(match path_scope {
            PathScope::Local => PathRef::LocalArc(tag, coord),
            PathScope::ForeignUse(use_tag) => PathRef::ForeignArc(use_tag, tag, coord),
        })
        .set_span(span)),
        Some(SymEntryRef::OntolDef(tag)) => Ok(TypeRef::Path(PathRef::Ontol(tag)).set_span(span)),
        None => {
            if matches!(path_scope, PathScope::Local) {
                // FIXME: lookup ONTOL type by name
                todo!("failed lookup: {first_symbol:?}");
            } else {
                Err(SemError::Lookup(Token(first_symbol), current_span))
            }
        }
    }
}

pub fn resolve_pattern(
    pat: insp::Pattern<RowanNodeView>,
    _table: &impl LookupTable,
) -> Result<WithSpan<Pattern>, SemError> {
    match pat {
        insp::Pattern::PatStruct(_pat) => todo!(),
        insp::Pattern::PatSet(_pat) => todo!(),
        insp::Pattern::PatAtom(pat) => {
            let token = pat.0.local_tokens().next().stx_err()?;
            let span = token.span();

            let pattern = match token.kind() {
                Kind::Number => match token.slice().parse::<i64>() {
                    Ok(int) => Pattern::I64(int),
                    Err(_) => {
                        // CompileError::InvalidInteger.span_report(span, &mut self.ctx);
                        // None
                        todo!()
                    }
                },
                Kind::SingleQuoteText | Kind::DoubleQuoteText => {
                    let literal = token.literal_text().stx_err()?.ok().stx_err()?;
                    Pattern::Text(literal.into())
                }
                Kind::Regex => {
                    todo!("regex")
                }
                Kind::Symbol => match token.slice() {
                    "true" => Pattern::True,
                    "false" => Pattern::False,
                    _ident => {
                        todo!("symbol")
                    }
                },
                _ => return Err(SemError::Syntax(Backtrace::capture())),
            };

            Ok(pattern.set_span(span))
        }
        insp::Pattern::PatBinary(_pat) => todo!(),
    }
}

fn ontol_node_tree(view: RowanNodeView) -> OntolTree {
    let children = ontol_node_children(view.children());

    OntolTree::Node(view.kind(), children)
}

fn ontol_node_children(input: RowanChildren) -> ThinVec<OntolTree> {
    let mut output = thin_vec![];

    for child in input {
        match child {
            ontol_parser::cst::view::Item::Node(node) => {
                output.push(ontol_node_tree(node));
            }
            ontol_parser::cst::view::Item::Token(token) => {
                if !token.kind().is_trivia() {
                    output.push(OntolTree::Token(token.kind(), token.slice().into()));
                }
            }
        }
    }

    output
}
