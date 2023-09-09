use lsp_types::{CompletionItem, CompletionItemKind};

use crate::state::HoverDoc;

/// Get a HoverDoc for ONTOL core keywords, values and types
pub fn get_ontol_docs(ident: &str) -> HoverDoc {
    match ident {
        "use" => HoverDoc::from(
            "use",
            "#### Use statement\nImports a domain by name, and defines a local namespace.",
        ),
        "def" => HoverDoc::from(
            "def",
            "#### Def statement\nDefines a type.",
        ),
        "rel" => HoverDoc::from(
            "rel",
            "#### Relation statement\nDefines relations between types, properties of a type, or properties of a relation."
        ),
        "fmt" => HoverDoc::from(
            "fmt",
            "#### Format statement\nDescribes a build/match pattern for strings or sequences."
        ),
        "map" => HoverDoc::from(
            "map",
            "#### Map statement\nDescribes a map between two types."
        ),
        "pub" => HoverDoc::from(
            "pub",
            "#### Public modifier\nMarks a type as public and importable from other domains.",
        ),
        "match" => HoverDoc::from(
            "match",
            "#### Partial modifier\nPartial matching for `map` arm."
        ),
        "id" => HoverDoc::from(
            "ontol.id",
            "#### Relation prop\nBinds an identifier to a type, making instances of a type unique entities.",
        ),
        "is" => HoverDoc::from(
            "ontol.is",
            "#### Relation prop\nThe subject type takes on all properties of the object type, or binds the subject type to a union.",
        ),
        "min" => HoverDoc::from(
            "ontol.min",
            "#### Relation prop\nMinimum value for the subject type.",
        ),
        "max" => HoverDoc::from(
            "ontol.max",
            "#### Relation prop\nMaximum value for the subject type.",
        ),
        "default" => HoverDoc::from(
            "ontol.default",
            "#### Relation prop\nAssigns a default value to a type or property if none is given.",
        ),
        "example" => HoverDoc::from(
            "ontol.example",
            "#### Relation prop\nGives an example value for a type for documentation."
        ),
        "gen" => HoverDoc::from(
            "ontol.gen",
            "#### Relation prop\nUse a generator function.",
        ),
        "auto" => HoverDoc::from(
            "ontol.auto",
            "#### Generator\nAutogenerate value according to type.",
        ),
        "create_time" => HoverDoc::from(
            "ontol.create_time",
            "#### Generator\nAutogenerate a datetime when instance is created.",
        ),
        "update_time" => HoverDoc::from(
            "ontol.update_time",
            "#### Generator\nAutogenerate a datetime when instance is updated.",
        ),
        "boolean" => HoverDoc::from(
            "ontol.boolean",
            "#### Primitive\nBoolean type."
        ),
        "true" => HoverDoc::from(
            "ontol.true",
            "#### Primitive\nBoolean `true` value."
        ),
        "false" => HoverDoc::from(
            "ontol.false",
            "#### Primitive\nBoolean `false` value."
        ),
        "number" => HoverDoc::from(
            "ontol.number",
            "#### Primitive\nAbstract number type.",
        ),
        "integer" => HoverDoc::from(
            "ontol.integer",
            "#### Primitive\nAbstract integer type.",
        ),
        "i64" => HoverDoc::from(
            "ontol.i64",
            "#### Primitive\n64-bit signed integer.",
        ),
        "float" => HoverDoc::from(
            "ontol.float",
            "#### Primitive\nAbstract floating point number type.",
        ),
        "f64" => HoverDoc::from(
            "ontol.f64",
            "#### Primitive\n64-bit floating point number.",
        ),
        "string" => HoverDoc::from(
            "ontol.string",
            "#### Primitive\nAny UTF-8 string.",
        ),
        "datetime" => HoverDoc::from(
            "ontol.datetime",
            "#### Primitive\nRFC 3339-formatted datetime string.",
        ),
        "date" => HoverDoc::from(
            "ontol.date",
            "#### Primitive\nRFC 3339-formatted date string.",
        ),
        "time" => HoverDoc::from(
            "ontol.time",
            "#### Primitive\nRFC 3339-formatted time string.",
        ),
        "uuid" => HoverDoc::from(
            "ontol.uuid",
            "#### Primitive\nUUID v4 string."
        ),
        "regex" => HoverDoc::from(
            "ontol.regex",
            "#### Primitive\nRegular expression.",
        ),
        "." => HoverDoc::from(
            ".",
            "#### Self reference\nRefers to the identity of the enclosing scope.",
        ),
        "?" => HoverDoc::from(
            "?",
            "#### Optional modifier\nMakes the property optional.",
        ),
        ident => HoverDoc::from(
            ident,
            "#### Variable\nLocal variable or type reference.",
        ),
    }
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
