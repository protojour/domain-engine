use ontol_runtime::ontology::domain::DefKind;
use tower_lsp::lsp_types::{CompletionItem, CompletionItemKind};

use crate::state::{HoverDoc, State};

#[expect(unused)]
pub fn get_ontol_var(ident: &str) -> HoverDoc {
    HoverDoc::from(ident, "#### Variable\nLocal variable or type reference.")
}

impl State {
    /// Get a HoverDoc for ONTOL core keywords, values and types
    pub fn get_ontol_docs(&self, ident: &str) -> Option<HoverDoc> {
        match ident {
                "domain" => Some(HoverDoc::from(
                    ident,
                    "#### Domain statement\nIdentifies a domain by a global ulid. Doc comments and rel properties serve as documentation and metadata.",
                )),
                "use" => Some(HoverDoc::from(
                    ident,
                    "#### Use statement\nImports a domain by name, and defines a local namespace alias.",
                )),
                "def" => Some(HoverDoc::from(
                    ident,
                    "#### Def statement\nDefines a concept, type or data structure.",
                )),
                "sym" => Some(HoverDoc::from(
                    ident,
                    "#### Sym statement\nDefines a symbol, a label representing the symbol name itself."
                )),
                "arc" => Some(HoverDoc::from(
                    ident,
                    "#### Arc statement\nDefines a relationship map, or a set of hypergraph relations."
                )),
                "rel" => Some(HoverDoc::from(
                    ident,
                    "#### Relation statement\nDefines properties of a definition, properties of a relation, or relationships between entities."
                )),
                "fmt" => Some(HoverDoc::from(
                    ident,
                    "#### Format statement\nDescribes a build/match pattern for strings or sequences."
                )),
                "map" => Some(HoverDoc::from(
                    ident,
                    "#### Map statement\nDescribes a map of the data flow between two definitions."
                )),
                "." => Some(HoverDoc::from(
                    ident,
                    "#### Self reference\nRefers to the identity of the enclosing scope.",
                )),
                ".." => Some(HoverDoc::from(
                    ident,
                    "#### Loop operator\nIndicates looping over the values of the containing set.",
                )),
                "?" => Some(HoverDoc::from(
                    ident,
                    "#### Optional modifier\nMakes the property optional.",
                )),
                "@private" => Some(HoverDoc::from(
                    ident,
                    "#### Def modifier\nThe definition is _private_. It will not be accessible to other domains if the domain is imported in a `use` statement."
                )),
                "@open" => Some(HoverDoc::from(
                    ident,
                    "#### Def modifier\nThe definition and its immediate non-entity relationships are _open_. Arbitrary data can be associated with it."
                )),
                "@extern" => Some(HoverDoc::from(
                    ident,
                    "#### Def modifier\nThe definition describes an _external_ hook."
                )),
                "@match" => Some(HoverDoc::from(
                    ident,
                    "#### Map modifier\nThe map becomes one-way and the `@match` arm tries to match given variables against structures that follow."
                )),
                "@in" => Some(HoverDoc::from(
                    ident,
                    "#### Set modifier\nThe given value must be in the set that follows."
                )),
                "@all_in" => Some(HoverDoc::from(
                    ident,
                    "#### Set modifier\nThe given values must all be in the set that follows."
                )),
                "@contains_all" => Some(HoverDoc::from(
                    ident,
                    "#### Set modifier\nThe set must contain all the values that follow."
                )),
                "@intersects" => Some(HoverDoc::from(
                    ident,
                    "#### Set modifier\nThe set must intersect with the set that follows."
                )),
                "@equals" => Some(HoverDoc::from(
                    ident,
                    "#### Set modifier\nThe values must be equal."
                )),
                ident => {
                    match self.ontol_def.get(ident) {
                        Some(def) => {
                            let doc = self.ontology.get_def_docs(def.id)
                                .map(|doc| doc.as_str())
                                .unwrap_or_default();
                            let kind = match def.kind {
                                DefKind::Entity(_) | DefKind::Data(_) => Some("Primitive"),
                                DefKind::Relation(_) => Some("Relation type"),
                                DefKind::Generator(_) => Some("Generator type"),
                                _ => Some("")
                            };
                            kind.map(|kind| HoverDoc::from(
                                &format!("ontol.{ident}"),
                                &format!("#### {kind}\n{doc}")
                            ))
                        }
                        None => None
                    }
                }
            }
    }
}

/// A list of Completions and their Kind
pub const COMPLETIONS: [(&str, CompletionItemKind); 47] = [
    ("domain", CompletionItemKind::KEYWORD),
    ("def", CompletionItemKind::KEYWORD),
    ("sym", CompletionItemKind::KEYWORD),
    ("fmt", CompletionItemKind::KEYWORD),
    ("map", CompletionItemKind::KEYWORD),
    ("arc", CompletionItemKind::KEYWORD),
    ("rel", CompletionItemKind::KEYWORD),
    ("use", CompletionItemKind::KEYWORD),
    ("as", CompletionItemKind::KEYWORD),
    ("is", CompletionItemKind::PROPERTY),
    ("id", CompletionItemKind::PROPERTY),
    ("min", CompletionItemKind::PROPERTY),
    ("max", CompletionItemKind::PROPERTY),
    ("gen", CompletionItemKind::PROPERTY),
    ("default", CompletionItemKind::PROPERTY),
    ("example", CompletionItemKind::PROPERTY),
    ("auto", CompletionItemKind::FUNCTION),
    ("create_time", CompletionItemKind::FUNCTION),
    ("update_time", CompletionItemKind::FUNCTION),
    ("ontol", CompletionItemKind::MODULE),
    ("boolean", CompletionItemKind::UNIT),
    ("number", CompletionItemKind::UNIT),
    ("integer", CompletionItemKind::UNIT),
    ("octets", CompletionItemKind::UNIT),
    ("i64", CompletionItemKind::UNIT),
    ("float", CompletionItemKind::UNIT),
    ("f32", CompletionItemKind::UNIT),
    ("f64", CompletionItemKind::UNIT),
    ("serial", CompletionItemKind::UNIT),
    ("text", CompletionItemKind::UNIT),
    ("uuid", CompletionItemKind::UNIT),
    ("ulid", CompletionItemKind::UNIT),
    ("datetime", CompletionItemKind::UNIT),
    ("format", CompletionItemKind::FUNCTION),
    ("hex", CompletionItemKind::UNIT),
    ("base64", CompletionItemKind::UNIT),
    ("true", CompletionItemKind::CONSTANT),
    ("false", CompletionItemKind::CONSTANT),
    ("@private", CompletionItemKind::TYPE_PARAMETER),
    ("@open", CompletionItemKind::TYPE_PARAMETER),
    ("@extern", CompletionItemKind::TYPE_PARAMETER),
    ("@match", CompletionItemKind::TYPE_PARAMETER),
    ("@in", CompletionItemKind::TYPE_PARAMETER),
    ("@all_in", CompletionItemKind::TYPE_PARAMETER),
    ("@contains_all", CompletionItemKind::TYPE_PARAMETER),
    ("@intersects", CompletionItemKind::TYPE_PARAMETER),
    ("@equals", CompletionItemKind::TYPE_PARAMETER),
];

/// Completions for built-in keywords and defs
pub fn get_core_completions() -> Vec<CompletionItem> {
    COMPLETIONS
        .map(|(label, kind)| CompletionItem {
            label: label.to_string(),
            kind: Some(kind),
            ..Default::default()
        })
        .to_vec()
}
