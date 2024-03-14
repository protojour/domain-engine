use mdbook::errors::Error;
use mdbook::{
    book::Book,
    preprocess::{Preprocessor, PreprocessorContext},
};
use ontol_compiler::mem::Mem;

use ontol_runtime::ontology::{
    domain::{TypeInfo, TypeKind},
    Ontology,
};

use ontol_compiler::package::ONTOL_PKG;
use ontol_compiler::Compiler;

/// A ontol documentation preprocessor.
pub struct OntolDocumentationPreprocessor;

/// Type names to ignore;
/// they might be for internal use, deprecated,
/// or otherwise unwanted in documentation.
const IGNORELIST: [&str; 3] = ["identifies", "route", "doc"];

impl OntolDocumentationPreprocessor {
    pub fn new() -> OntolDocumentationPreprocessor {
        OntolDocumentationPreprocessor
    }
}

impl Default for OntolDocumentationPreprocessor {
    fn default() -> Self {
        Self::new()
    }
}

fn compile() -> Ontology {
    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, Default::default()).with_ontol();
    compiler
        .compile_package_topology(Default::default())
        .unwrap();
    compiler.into_ontology()
}

fn get_ontol_docs_md(ontology: &Ontology, predicate: &dyn Fn(&TypeInfo) -> bool) -> String {
    let ontol_domain = ontology.find_domain(ONTOL_PKG).unwrap();
    let mut docs: Vec<(String, String)> = vec![];
    for t in ontol_domain.type_infos() {
        if !predicate(t) {
            continue;
        }
        let doc = ontology.get_docs(t.def_id);
        if let Some(name) = t.name() {
            let name = &ontology[name];
            docs.push((name.into(), doc.unwrap_or_default()));
        }
    }
    let mut docs_string = String::from("");

    for (type_name, doc_string) in docs {
        if IGNORELIST.contains(&type_name.as_str()) {
            continue;
        }
        docs_string.push_str(&format!("## `{type_name}`\n\n"));
        docs_string.push_str(&format!("{doc_string}\n"));
    }
    docs_string
}

impl Preprocessor for OntolDocumentationPreprocessor {
    fn name(&self) -> &str {
        "ontol-documentation-preprocessor"
    }

    fn run(&self, _ctx: &PreprocessorContext, mut book: Book) -> Result<Book, Error> {
        let ontology = compile();
        book.for_each_mut(|book_item| match book_item {
            mdbook::BookItem::Chapter(c) => {
                if c.content.contains("{{#ontol-primitives}}") {
                    let ontol_docs = get_ontol_docs_md(&ontology, &|t| {
                        matches!(t.kind, TypeKind::Data(_) | TypeKind::Entity(_))
                    });
                    let new_string = c.content.replace("{{#ontol-primitives}}", &ontol_docs);
                    c.content = new_string;
                }
                if c.content.contains("{{#ontol-relation-types}}") {
                    let ontol_docs = get_ontol_docs_md(&ontology, &|t| {
                        matches!(t.kind, TypeKind::Relationship(_))
                    });
                    let new_string = c.content.replace("{{#ontol-relation-types}}", &ontol_docs);
                    c.content = new_string;
                }
                if c.content.contains("{{#ontol-function-types}}") {
                    let ontol_docs =
                        get_ontol_docs_md(&ontology, &|t| matches!(t.kind, TypeKind::Function(_)));
                    let new_string = c.content.replace("{{#ontol-function-types}}", &ontol_docs);
                    c.content = new_string;
                }
                if c.content.contains("{{#ontol-generator-types}}") {
                    let ontol_docs =
                        get_ontol_docs_md(&ontology, &|t| matches!(t.kind, TypeKind::Generator(_)));
                    let new_string = c.content.replace("{{#ontol-generator-types}}", &ontol_docs);
                    c.content = new_string;
                }
            }
            mdbook::BookItem::Separator => {}
            mdbook::BookItem::PartTitle(_) => {}
        });
        Ok(book)
    }

    fn supports_renderer(&self, renderer: &str) -> bool {
        renderer != "not-supported"
    }
}
