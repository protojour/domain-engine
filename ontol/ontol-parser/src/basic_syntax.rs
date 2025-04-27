use std::{panic::UnwindSafe, str::FromStr};

use ontol_core::{DomainId, OntologyDomainId, span::U32Span, url::DomainUrlParser};
use ulid::Ulid;

use crate::{
    cst::{
        inspect as insp,
        tree::SyntaxTree,
        view::{NodeView, NodeViewExt, TokenView, TokenViewExt, TypedView},
    },
    join_doc_lines,
    lexer::kind::Kind,
    topology::{ExtractHeaderData, MakeParseError, OntolHeaderData, TopologyError, WithDocs},
};

/// An ontol_parser native syntax tree.
pub struct OntolTreeSyntax<S> {
    pub tree: SyntaxTree,
    pub source_text: S,
}

impl<S: AsRef<str> + UnwindSafe> ExtractHeaderData for OntolTreeSyntax<S> {
    fn header_data(
        &self,
        with_docs: WithDocs,
        errors: &mut Vec<impl MakeParseError>,
    ) -> OntolHeaderData {
        extract_ontol_header_data(self.tree.view(self.source_text.as_ref()), with_docs, errors)
    }
}

pub fn extract_ontol_header_data<V: NodeView>(
    ontol_view: V,
    with_docs: WithDocs,
    parse_errors: &mut Vec<impl MakeParseError>,
) -> OntolHeaderData {
    let mut data = OntolHeaderData {
        domain_docs: None,
        domain_id: (
            OntologyDomainId {
                id: DomainId {
                    ulid: Default::default(),
                    subdomain: 0,
                },
                stable: false,
            },
            U32Span::default(),
        ),
        name: (String::new(), U32Span::default()),
        deps: vec![],
    };

    let parser = DomainUrlParser::default();

    if let insp::Node::Ontol(ontol) = ontol_view.node() {
        for statement in ontol.statements() {
            match statement {
                insp::Statement::DomainStatement(stmt) => {
                    data = extract_domain_headerdata(stmt, with_docs, &mut vec![]);

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
                            data.deps.push((reference, uri.0.span()));
                        }
                        Err(_error) => {
                            parse_errors.push(MakeParseError::make_parse_error(
                                "invalid reference".to_string(),
                                uri.0.span(),
                            ));
                        }
                    }
                }
                _ => break,
            }
        }
    }

    data
}

pub fn extract_domain_headerdata<V: NodeView>(
    domain_statement: insp::DomainStatement<V>,
    with_docs: WithDocs,
    errors: &mut Vec<(TopologyError, U32Span)>,
) -> OntolHeaderData {
    let mut domain_docs: Option<String> = None;
    let mut name = None;

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
                    TopologyError::TODO("misformatted domain id"),
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
                insp::Statement::RelStatement(rel_stmt) => {
                    let Some(_subject) = rel_stmt.subject() else {
                        // TODO: check the subject
                        continue;
                    };
                    let Some(relation) = rel_stmt.relation() else {
                        continue;
                    };
                    let Some(object) = rel_stmt.object() else {
                        continue;
                    };

                    let Some(rel_q) = relation.relation_type() else {
                        continue;
                    };
                    let Some(rel_type_ref) = rel_q.type_ref() else {
                        continue;
                    };
                    let rel_ident_path = match rel_type_ref {
                        insp::TypeRef::IdentPath(ident_path) => ident_path,
                        other => {
                            errors.push((
                                TopologyError::TODO("must be a literal"),
                                other.view().span(),
                            ));
                            continue;
                        }
                    };
                    let Some(rel_symbol) = rel_ident_path.symbols().next() else {
                        continue;
                    };
                    let rel_name = rel_symbol.slice().to_string();

                    let Some(insp::TypeQuantOrPattern::TypeQuant(insp::TypeQuant::TypeQuantUnit(
                        object_unit,
                    ))) = object.type_quant_or_pattern()
                    else {
                        errors.push((
                            TopologyError::TODO("must be a text literal"),
                            object.view().span(),
                        ));
                        continue;
                    };
                    let Some(insp::TypeRef::Literal(obj_literal)) = object_unit.type_ref() else {
                        errors.push((
                            TopologyError::TODO("must be a text literal"),
                            object_unit.view().span(),
                        ));
                        continue;
                    };

                    let Some(obj_token) = obj_literal.0.clone().local_tokens().next() else {
                        continue;
                    };
                    let Some(Ok(obj_value)) = obj_token.literal_text() else {
                        continue;
                    };

                    match rel_name.as_str() {
                        "name" => {
                            name = Some((obj_value, obj_literal.0.span()));
                        }
                        _other => {
                            errors.push((
                                TopologyError::TODO("unsupported metadata"),
                                rel_ident_path.view().span(),
                            ));
                        }
                    }
                }
                stmt => {
                    errors.push((
                        TopologyError::TODO("invalid statement, not supported here"),
                        stmt.view().span(),
                    ));
                }
            }
        }
    }

    if let Some((ulid, ulid_span)) = domain_ulid {
        OntolHeaderData {
            domain_docs,
            domain_id: (
                OntologyDomainId {
                    id: DomainId { ulid, subdomain: 0 },
                    stable: true,
                },
                ulid_span,
            ),
            name: name.unwrap_or_else(|| (format!("{}", ulid), ulid_span)),
            deps: vec![],
        }
    } else {
        let mut data = OntolHeaderData::autogenerated();
        data.domain_docs = domain_docs;
        data
    }
}

pub fn extract_documentation<V: NodeView>(node_view: V) -> Option<String> {
    let doc_comments = node_view
        .local_tokens_filter(Kind::DocComment)
        .map(|token| token.slice().strip_prefix("///").unwrap().to_string());

    join_doc_lines(doc_comments)
}

#[test]
fn test_extract_header_data() {
    let ontol = "
    /// docs1
    /// docs2
    domain 7ZZZZZZZZZZTESTZZZZZZZZZZZ ()

    use 'a' as b
    ";

    let (tree, errors) = crate::cst_parse(ontol);
    assert!(errors.is_empty());
    let syntax_tree = tree.unflatten();

    let mut errors: Vec<crate::ParserError> = vec![];
    let header_data =
        extract_ontol_header_data(syntax_tree.view(ontol), WithDocs(true), &mut errors);

    assert_eq!("docs1\ndocs2", header_data.domain_docs.unwrap());
    assert_eq!(
        "7ZZZZZZZZZZTESTZZZZZZZZZZZ§0",
        header_data.domain_id.0.id.to_string()
    );
    assert_eq!(1, header_data.deps.len());
}
