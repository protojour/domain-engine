use std::collections::BTreeSet;

use either::Either;
use ontol_syntax::rowan::GreenToken;

use ontol_core::property::ValueCardinality;
use ontol_parser::{
    cst::{
        inspect::{self as insp, TypeQuant},
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt, TypedView},
    },
    lexer::{kind::Kind, unescape::unescape_regex},
};
use ontol_syntax::syntax_view::{RowanChildren, RowanNodeView};
use thin_vec::{ThinVec, thin_vec};
use tracing::error;

use crate::{
    analyzer::BlockContext,
    log_model::{OntolTree, PathRef, Pattern, TypeRef, TypeRefOrUnionOrPattern, TypeUnion},
    symbol::SymEntry,
    tables::{DomainTablesView, LayeredDomainTables},
    tag::{LogRef, Tag},
};

#[derive(Clone, Copy)]
pub enum LookupScope {
    Local,
    Foreign(LogRef, Tag),
}

pub trait LookupTable {
    fn lookup_root(&self, token: &GreenToken) -> Option<&SymEntry>;

    fn lookup_next<'s>(
        &'s self,
        scope: LookupScope,
        prev: &'s SymEntry,
        token: &GreenToken,
    ) -> Option<&'s SymEntry>;
}

impl<'a> LookupTable for DomainTablesView<'a> {
    fn lookup_root(&self, token: &GreenToken) -> Option<&SymEntry> {
        self.local
            .symbols
            .table
            .get(token)
            .or_else(|| self.global.ontol.get(token.text()))
    }

    fn lookup_next<'s>(
        &'s self,
        scope: LookupScope,
        prev: &'s SymEntry,
        token: &GreenToken,
    ) -> Option<&'s SymEntry> {
        let domain_tables = match scope {
            LookupScope::Local => &self.local,
            LookupScope::Foreign(log_ref, domain_tag) => {
                self.global.domains.get(&(log_ref, domain_tag))?
            }
        };

        match prev {
            SymEntry::Use(_tag) => todo!(),
            SymEntry::LocalDef(_) => None,
            SymEntry::LocalEdge(edge_tag) => domain_tables
                .edges
                .get(edge_tag)
                .and_then(|matrix| matrix.symbols.get(token)),
            SymEntry::OntolModule(table) => table.get(token.text()),
            SymEntry::EdgeSymbol(..) | SymEntry::OntolDef(_) => None,
        }
    }
}

impl<'a> LookupTable for LayeredDomainTables<'a> {
    fn lookup_root(&self, token: &GreenToken) -> Option<&SymEntry> {
        match self.front_symbols.get(token) {
            Some(Some(entry)) => Some(entry),
            Some(None) => self.back.global.ontol.get(token.text()),
            None => self
                .back
                .local
                .symbols
                .table
                .get(token)
                .or_else(|| self.back.global.ontol.get(token.text())),
        }
    }

    fn lookup_next<'s>(
        &'s self,
        scope: LookupScope,
        prev: &'s SymEntry,
        token: &GreenToken,
    ) -> Option<&'s SymEntry> {
        match prev {
            SymEntry::Use(tag) => {
                todo!("foreign lookup")
            }
            SymEntry::LocalDef(_) => None,
            SymEntry::LocalEdge(tag) => match scope {
                LookupScope::Local => {
                    match self
                        .front_edges
                        .get(tag)
                        .and_then(|matrix| matrix.symbols.get(token))
                    {
                        Some(entry) => Some(entry),
                        None => self
                            .back
                            .local
                            .edges
                            .get(tag)
                            .and_then(|matrix| matrix.symbols.get(token)),
                    }
                }
                LookupScope::Foreign(..) => self.back.lookup_next(scope, prev, token),
            },
            SymEntry::EdgeSymbol(..) => None,
            SymEntry::OntolModule(m) => m.get(token.text()),
            SymEntry::OntolDef(_) => None,
        }
    }
}

pub fn resolve_quant_type_reference(
    q: insp::TypeQuant<RowanNodeView>,
    context: BlockContext,
    table: &impl LookupTable,
) -> Option<(Either<TypeRef, TypeUnion>, ValueCardinality)> {
    match q {
        TypeQuant::TypeQuantUnit(t) => {
            let mut vc = ValueCardinality::Unit;
            let tr = resolve_type_reference(t.type_ref()?, &mut vc, context, table)?;
            Some((tr, vc))
        }
        TypeQuant::TypeQuantSet(t) => {
            let mut vc = ValueCardinality::IndexSet;
            let tr = resolve_type_reference(t.type_ref()?, &mut vc, context, table)?;
            Some((tr, vc))
        }
        TypeQuant::TypeQuantList(t) => {
            let mut vc = ValueCardinality::List;
            let tr = resolve_type_reference(t.type_ref()?, &mut vc, context, table)?;
            Some((tr, vc))
        }
    }
}

pub fn resolve_quant_type_reference_or_pattern(
    qp: insp::TypeQuantOrPattern<RowanNodeView>,
    context: BlockContext,
    table: &impl LookupTable,
) -> Option<(TypeRefOrUnionOrPattern, ValueCardinality)> {
    match qp {
        insp::TypeQuantOrPattern::TypeQuant(q) => {
            let (tp, crd) = resolve_quant_type_reference(q, context, table)?;
            Some((tp.into(), crd))
        }
        insp::TypeQuantOrPattern::Pattern(pat) => Some((
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
) -> Option<Either<TypeRef, TypeUnion>> {
    match (type_ref, context) {
        (insp::TypeRef::IdentPath(path), _) => Some(lookup_type_path(&path, table)?.into()),
        (insp::TypeRef::ThisUnit(_), BlockContext::Def(tag)) => {
            Some(TypeRef::Path(PathRef::Local(tag)).into())
        }
        (insp::TypeRef::ThisSet(_), BlockContext::Def(tag)) => {
            *syntax_cardinality = ValueCardinality::IndexSet;
            Some(TypeRef::Path(PathRef::Local(tag)).into())
        }
        (insp::TypeRef::ThisSet(_) | insp::TypeRef::ThisUnit(_), _) => None,
        (insp::TypeRef::Literal(literal), _) => {
            let token = literal.0.local_tokens().next()?;
            match token.kind() {
                Kind::Number => Some(TypeRef::Number(token.slice().into()).into()),
                Kind::SingleQuoteText | Kind::DoubleQuoteText => {
                    let unescaped = token.literal_text()?.ok()?;
                    Some(TypeRef::Text(unescaped.into()).into())
                }
                Kind::Regex => {
                    let regex_literal = unescape_regex(token.slice());
                    Some(TypeRef::Regex(regex_literal.into()).into())
                }
                kind => unimplemented!("literal type: {kind}"),
            }
        }
        (insp::TypeRef::DefBody(body), _) => {
            Some(TypeRef::Anonymous(ontol_node_children(body.view().children())).into())
        }
        (insp::TypeRef::NumberRange(range), _) => {
            let start = range.start()?;
            let end = range.end()?;

            Some(TypeRef::NumberRange(start.slice().into(), end.slice().into()).into())
        }
        (insp::TypeRef::TypeUnion(type_union), _) => {
            let mut u = BTreeSet::new();

            for member in type_union.members() {
                match resolve_type_reference(member, &mut ValueCardinality::Unit, context, table)? {
                    Either::Left(t) => {
                        u.insert(t);
                    }
                    Either::Right(_) => {
                        return None;
                    }
                }
            }

            Some(Either::Right(u))
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
) -> Option<TypeRef> {
    let mut symbols = path.symbols().peekable();

    let first_symbol = symbols.next()?.0.green().to_owned();

    let mut entry = table.lookup_root(&first_symbol);

    let mut path_scope = PathScope::Local;
    let mut lookup_scope = LookupScope::Local;

    while let Some(next) = symbols.next() {
        let prev = entry?;

        match &prev {
            SymEntry::Use(use_tag) => {
                path_scope = PathScope::ForeignUse(*use_tag);
                // FIXME: resolve the lookup scope parameters
                lookup_scope = LookupScope::Foreign(LogRef(42), Tag(42));
            }
            SymEntry::OntolModule(_) => {}
            SymEntry::LocalDef(_) | SymEntry::EdgeSymbol(..) | SymEntry::OntolDef(_) => {
                // sub-scoping not possible
                return None;
            }
            SymEntry::LocalEdge(edge_tag) => {}
        }

        entry = Some(table.lookup_next(lookup_scope, prev, &next.0.green().to_owned())?);
    }

    match entry {
        Some(SymEntry::Use(tag)) => {
            error!("error: lookup import");
            None
        }
        Some(SymEntry::LocalDef(tag)) => Some(TypeRef::Path(match path_scope {
            PathScope::Local => PathRef::Local(*tag),
            PathScope::ForeignUse(use_tag) => PathRef::Foreign(use_tag, *tag),
        })),
        Some(SymEntry::LocalEdge(tag)) => {
            error!("error: lookup local edge");
            None
        }
        Some(SymEntry::OntolModule(_)) => None,
        Some(SymEntry::EdgeSymbol(tag, coord)) => Some(TypeRef::Path(match path_scope {
            PathScope::Local => PathRef::LocalArc(*tag, *coord),
            PathScope::ForeignUse(use_tag) => PathRef::ForeignArc(use_tag, *tag, *coord),
        })),
        Some(SymEntry::OntolDef(tag)) => Some(TypeRef::Path(PathRef::Ontol(*tag))),
        None => {
            if matches!(path_scope, PathScope::Local) {
                // FIXME: lookup ONTOL type by name
                todo!("failed lookup: {first_symbol:?}");
            } else {
                None
            }
        }
    }
}

pub fn resolve_pattern(
    pat: insp::Pattern<RowanNodeView>,
    table: &impl LookupTable,
) -> Option<Pattern> {
    todo!()
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
