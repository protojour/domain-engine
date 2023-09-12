use lsp_types::{CompletionItem, CompletionItemKind};

use crate::state::HoverDoc;

/// Get a HoverDoc for ONTOL core keywords, values and types
pub fn get_ontol_docs(ident: &str) -> Option<HoverDoc> {
    match ident {
        "use" => Some(HoverDoc::from(
            "use",
            "#### Use statement\nImports a domain by name, and defines a local namespace.",
        )),
        "def" => Some(HoverDoc::from(
            "def",
            "#### Def statement\nDefines a type.",
        )),
        "rel" => Some(HoverDoc::from(
            "rel",
            "#### Relation statement\nDefines relations between types, properties of a type, or properties of a relation."
        )),
        "fmt" => Some(HoverDoc::from(
            "fmt",
            "#### Format statement\nDescribes a build/match pattern for strings or sequences."
        )),
        "map" => Some(HoverDoc::from(
            "map",
            "#### Map statement\nDescribes a map between two types."
        )),
        "pub" => Some(HoverDoc::from(
            "pub",
            "#### Public modifier\nMarks a type as public and importable from other domains.",
        )),
        "match" => Some(HoverDoc::from(
            "match",
            "#### Partial modifier\nPartial matching for `map` arm."
        )),
        "id" => Some(HoverDoc::from(
            "ontol.id",
            "#### Relation prop\nBinds an identifier to a type, making instances of a type unique entities.",
        )),
        "is" => Some(HoverDoc::from(
            "ontol.is",
            "#### Relation prop\nThe subject type takes on all properties of the object type, or binds the subject type to a union.",
        )),
        "min" => Some(HoverDoc::from(
            "ontol.min",
            "#### Relation prop\nMinimum value for the subject type.",
        )),
        "max" => Some(HoverDoc::from(
            "ontol.max",
            "#### Relation prop\nMaximum value for the subject type.",
        )),
        "default" => Some(HoverDoc::from(
            "ontol.default",
            "#### Relation prop\nAssigns a default value to a type or property if none is given.",
        )),
        "example" => Some(HoverDoc::from(
            "ontol.example",
            "#### Relation prop\nGives an example value for a type for documentation."
        )),
        "gen" => Some(HoverDoc::from(
            "ontol.gen",
            "#### Relation prop\nUse a generator function.",
        )),
        "auto" => Some(HoverDoc::from(
            "ontol.auto",
            "#### Generator\nAutogenerate value according to type, e.g. autoincrement (integer) or randomization (uuid).",
        )),
        "create_time" => Some(HoverDoc::from(
            "ontol.create_time",
            "#### Generator\nAutogenerate a datetime when instance is created.",
        )),
        "update_time" => Some(HoverDoc::from(
            "ontol.update_time",
            "#### Generator\nAutogenerate a datetime when instance is updated.",
        )),
        "boolean" => Some(HoverDoc::from(
            "ontol.boolean",
            "#### Primitive\nBoolean type."
        )),
        "true" => Some(HoverDoc::from(
            "ontol.true",
            "#### Primitive\nBoolean `true` value."
        )),
        "false" => Some(HoverDoc::from(
            "ontol.false",
            "#### Primitive\nBoolean `false` value."
        )),
        "number" => Some(HoverDoc::from(
            "ontol.number",
            "#### Primitive\nAbstract number type.",
        )),
        "integer" => Some(HoverDoc::from(
            "ontol.integer",
            "#### Primitive\nAbstract integer type.",
        )),
        "i64" => Some(HoverDoc::from(
            "ontol.i64",
            "#### Primitive\n64-bit signed integer.",
        )),
        "float" => Some(HoverDoc::from(
            "ontol.float",
            "#### Primitive\nAbstract floating point number type.",
        )),
        "f64" => Some(HoverDoc::from(
            "ontol.f64",
            "#### Primitive\n64-bit floating point number.",
        )),
        "string" => Some(HoverDoc::from(
            "ontol.string",
            "#### Primitive\nAny UTF-8 string.",
        )),
        "datetime" => Some(HoverDoc::from(
            "ontol.datetime",
            "#### Primitive\nRFC 3339-formatted datetime string.",
        )),
        "date" => Some(HoverDoc::from(
            "ontol.date",
            "#### Primitive\nRFC 3339-formatted date string.",
        )),
        "time" => Some(HoverDoc::from(
            "ontol.time",
            "#### Primitive\nRFC 3339-formatted time string.",
        )),
        "uuid" => Some(HoverDoc::from(
            "ontol.uuid",
            "#### Primitive\nUUID v4 string."
        )),
        "regex" => Some(HoverDoc::from(
            "ontol.regex",
            "#### Primitive\nRegular expression.",
        )),
        "." => Some(HoverDoc::from(
            ".",
            "#### Self reference\nRefers to the identity of the enclosing scope.",
        )),
        "?" => Some(HoverDoc::from(
            "?",
            "#### Optional modifier\nMakes the property optional.",
        )),
        _ => None,
    }
}

pub fn get_ontol_var(ident: &str) -> HoverDoc {
    HoverDoc::from(ident, "#### Variable\nLocal variable or type reference.")
}

/// Completions for built-in keywords and defs
pub fn get_core_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem {
            label: "ontol".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "def".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "rel".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "map".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "use".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "as".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "pub".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "fmt".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "match".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "id".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "is".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "min".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "max".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "gen".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "auto".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "create_time".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "update_time".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "default".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "example".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "boolean".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "true".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "false".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "number".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "integer".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "float".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "i64".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "f64".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "string".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "datetime".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "date".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "time".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "uuid".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "regex".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
    ]
}

/// A list of reserved words in ONTOL, to separate them from user-defined symbols
pub const RESERVED_WORDS: [&str; 34] = [
    "ontol",
    "use",
    "as",
    "pub",
    "def",
    "rel",
    "fmt",
    "map",
    "unify",
    "match",
    "id",
    "is",
    "min",
    "max",
    "default",
    "example",
    "gen",
    "auto",
    "create_time",
    "update_time",
    "number",
    "boolean",
    "true",
    "false",
    "integer",
    "i64",
    "float",
    "f64",
    "string",
    "datetime",
    "date",
    "time",
    "uuid",
    "regex",
];
