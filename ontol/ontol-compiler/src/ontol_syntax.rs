use std::{panic::UnwindSafe, str::FromStr, sync::Arc};

use indexmap::IndexMap;
use ontol_parser::{
    cst::{
        inspect::{
            self as insp, DomainStatement, Statement, TypeQuant, TypeQuantOrPattern, TypeRef,
        },
        tree::SyntaxTree,
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt},
    },
    lexer::kind::Kind,
    ParserError, U32Span,
};
use ontol_runtime::DefId;
use ulid::Ulid;

use crate::{
    lower_ontol_syntax,
    lowering::context::LoweringOutcome,
    topology::{DomainUrl, DomainUrlParser},
    CompileError, Session, Src,
};

/// A generalization of an ONTOL source file.
///
/// It's whatever that can produce syntax nodes to fulfill
/// the methods of the trait.
pub trait OntolSyntax: UnwindSafe {
    fn header_data(
        &self,
        with_docs: WithDocs,
        errors: &mut Vec<ontol_parser::Error>,
    ) -> OntolHeaderData;
    fn lower(
        &self,
        url: DomainUrl,
        pkg_def_id: DefId,
        src: Src,
        session: Session,
    ) -> LoweringOutcome;
}

/// An ontol_parser native syntax tree.
pub struct OntolTreeSyntax<S> {
    pub tree: SyntaxTree,
    pub source_text: S,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum HeaderProperty {
    Name,
}

/// Metadata extracted from the header section of a domain
pub struct OntolHeaderData {
    pub domain_docs: Option<String>,
    pub domain_ulid: Option<(Ulid, U32Span)>,
    pub properties: IndexMap<HeaderProperty, String>,
    pub deps: Vec<(DomainUrl, U32Span)>,
}

#[derive(Clone, Copy)]
pub struct WithDocs(pub bool);

impl<S: AsRef<str> + UnwindSafe> OntolSyntax for OntolTreeSyntax<S> {
    fn header_data(
        &self,
        with_docs: WithDocs,
        errors: &mut Vec<ontol_parser::Error>,
    ) -> OntolHeaderData {
        extract_ontol_header_data(self.tree.view(self.source_text.as_ref()), with_docs, errors)
    }

    fn lower(
        &self,
        url: DomainUrl,
        domain_def_id: DefId,
        src: Src,
        session: Session,
    ) -> LoweringOutcome {
        lower_ontol_syntax(
            self.tree.view(self.source_text.as_ref()),
            url,
            domain_def_id,
            src,
            session,
        )
    }
}

/// Thin wrapper around Arc<String> that implements AsRef<str>
pub struct ArcString(pub Arc<String>);

impl AsRef<str> for ArcString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

pub(crate) fn extract_documentation<V: NodeView>(node_view: V) -> Option<String> {
    let doc_comments = node_view
        .local_tokens_filter(Kind::DocComment)
        .map(|token| token.slice().strip_prefix("///").unwrap().to_string());

    ontol_parser::join_doc_lines(doc_comments)
}

fn extract_ontol_header_data<V: NodeView>(
    ontol_view: V,
    with_docs: WithDocs,
    parse_errors: &mut Vec<ontol_parser::Error>,
) -> OntolHeaderData {
    let mut domain_docs: Option<String> = None;
    let mut domain_ulid: Option<(Ulid, U32Span)> = None;
    let mut properties = Default::default();
    let mut deps: Vec<(DomainUrl, U32Span)> = vec![];

    let parser = DomainUrlParser::default();

    if let insp::Node::Ontol(ontol) = ontol_view.node() {
        for statement in ontol.statements() {
            match statement {
                insp::Statement::DomainStatement(stmt) => {
                    let header_data = extract_domain_headerdata(stmt, with_docs, &mut vec![]);
                    domain_docs = header_data.domain_docs;
                    domain_ulid = header_data.domain_ulid;
                    properties = header_data.properties;

                    continue;
                }
                insp::Statement::UseStatement(use_stmt) => {
                    if use_stmt
                        .ident_path()
                        .and_then(|path| path.symbols().next())
                        .is_none()
                    {
                        // avoid processing syntactically invalid statement
                        continue;
                    }

                    let Some(uri) = use_stmt.uri() else {
                        continue;
                    };
                    let Some(Ok(text)) = uri.text() else {
                        continue;
                    };

                    match parser.parse(&text) {
                        Ok(reference) => {
                            deps.push((reference, uri.0.span()));
                        }
                        Err(_error) => {
                            parse_errors.push(ontol_parser::Error::Parse(ParserError {
                                msg: "invalid reference".to_string(),
                                span: uri.0.span(),
                            }));
                        }
                    }
                }
                _ => break,
            }
        }
    }

    OntolHeaderData {
        domain_docs,
        domain_ulid,
        properties,
        deps,
    }
}

pub(crate) fn extract_domain_headerdata<V: NodeView>(
    domain_statement: DomainStatement<V>,
    with_docs: WithDocs,
    errors: &mut Vec<(CompileError, U32Span)>,
) -> OntolHeaderData {
    let mut domain_docs: Option<String> = None;
    let mut properties = IndexMap::<HeaderProperty, String>::default();

    if with_docs.0 {
        domain_docs = extract_documentation(domain_statement.0.clone());
    }

    let domain_ulid = if let Some(domain_id) = domain_statement.domain_id() {
        domain_id
            .try_concat_ulid()
            .and_then(|ulid| Ulid::from_str(&ulid).ok())
            .map(|ulid| (ulid, domain_id.view().span()))
            .or_else(|| {
                errors.push((
                    CompileError::TODO("misformatted domain id"),
                    domain_id.view().span(),
                ));
                None
            })
    } else {
        None
    };

    if let Some(body) = domain_statement.body() {
        for stmt in body.statements() {
            match stmt {
                Statement::RelStatement(rel_stmt) => {
                    let Some(_subject) = rel_stmt.subject() else {
                        // TODO: check the subject
                        continue;
                    };
                    let Some(relation_set) = rel_stmt.relation_set() else {
                        continue;
                    };
                    let Some(object) = rel_stmt.object() else {
                        continue;
                    };

                    let relations: Vec<_> = relation_set.relations().collect();
                    if relations.len() != 1 {
                        errors.push((
                            CompileError::TODO("one relation expected"),
                            rel_stmt.view().span(),
                        ));
                        continue;
                    }
                    let relation = relations.into_iter().next().unwrap();
                    let Some(rel_q) = relation.relation_type() else {
                        continue;
                    };
                    let Some(rel_type_ref) = rel_q.type_ref() else {
                        continue;
                    };
                    let rel_ident_path = match rel_type_ref {
                        TypeRef::IdentPath(ident_path) => ident_path,
                        other => {
                            errors.push((
                                CompileError::TODO("must be a literal"),
                                other.view().span(),
                            ));
                            continue;
                        }
                    };
                    let Some(rel_symbol) = rel_ident_path.symbols().next() else {
                        continue;
                    };
                    let rel_name = rel_symbol.slice().to_string();

                    let Some(TypeQuantOrPattern::TypeQuant(TypeQuant::TypeQuantUnit(object_unit))) =
                        object.type_quant_or_pattern()
                    else {
                        errors.push((
                            CompileError::TODO("must be a text literal"),
                            object.view().span(),
                        ));
                        continue;
                    };
                    let Some(TypeRef::Literal(obj_literal)) = object_unit.type_ref() else {
                        errors.push((
                            CompileError::TODO("must be a text literal"),
                            object_unit.view().span(),
                        ));
                        continue;
                    };

                    let Some(obj_token) = obj_literal.0.local_tokens().next() else {
                        continue;
                    };
                    let Some(Ok(obj_value)) = obj_token.literal_text() else {
                        continue;
                    };

                    match rel_name.as_str() {
                        "name" => {
                            properties.insert(HeaderProperty::Name, obj_value);
                        }
                        _other => {
                            errors.push((
                                CompileError::TODO("unsupported metadata"),
                                rel_ident_path.view().span(),
                            ));
                        }
                    }
                }
                stmt => {
                    errors.push((
                        CompileError::TODO("invalid statement, not supported here"),
                        stmt.view().span(),
                    ));
                }
            }
        }
    }

    OntolHeaderData {
        domain_docs,
        domain_ulid,
        properties,
        deps: vec![],
    }
}

#[test]
fn test_extract_header_data() {
    let ontol = "
    /// docs1
    /// docs2
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()

    use 'a' as b
    ";

    let (tree, errors) = ontol_parser::cst_parse(ontol);
    assert!(errors.is_empty());
    let syntax_tree = tree.unflatten();

    let header_data =
        extract_ontol_header_data(syntax_tree.view(ontol), WithDocs(true), &mut vec![]);

    assert_eq!("docs1\ndocs2", header_data.domain_docs.unwrap());
    assert!(header_data.domain_ulid.is_some());
    assert_eq!(1, header_data.deps.len());
}
