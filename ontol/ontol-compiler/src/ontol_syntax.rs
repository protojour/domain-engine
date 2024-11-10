use std::{panic::UnwindSafe, str::FromStr, sync::Arc};

use ontol_parser::{
    cst::{
        inspect as insp,
        tree::SyntaxTree,
        view::{NodeView, NodeViewExt, TokenView},
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
    Session, Src,
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

/// Metadata extracted from the header section of a domain
pub struct OntolHeaderData {
    pub domain_docs: Option<String>,
    pub domain_ulid: Option<Ulid>,
    pub deps: Vec<(DomainUrl, U32Span)>,
}

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
    let mut domain_ulid: Option<Ulid> = None;
    let mut deps: Vec<(DomainUrl, U32Span)> = vec![];

    let parser = DomainUrlParser::default();

    if let insp::Node::Ontol(ontol) = ontol_view.node() {
        for statement in ontol.statements() {
            match statement {
                insp::Statement::DomainStatement(stmt) => {
                    if with_docs.0 {
                        domain_docs = extract_documentation(stmt.0.clone());
                    }

                    if let Some(domain_id) = stmt.domain_id() {
                        if let Some(ulid) = domain_id.try_concat_ulid() {
                            if let Ok(ulid) = Ulid::from_str(&ulid) {
                                domain_ulid = Some(ulid);
                            }
                        }
                    }

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
        deps,
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
