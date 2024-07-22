use std::{fmt::Display, ops::Deref};

use ::serde::{Deserialize, Serialize};
use fnv::FnvHashMap;
use serde::de::{value::StrDeserializer, DeserializeSeed};
use tracing::{error, trace};

use crate::{
    attr::Attr,
    interface::serde::processor::ProcessorMode,
    ontology::Ontology,
    value::{FormatValueAsText, Value},
    DefId, DefRelTag, RelId,
};

use super::{text_like_types::ParseError, TextConstant};

#[derive(Debug, Serialize, Deserialize)]
pub struct TextPattern {
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    pub constant_parts: Vec<TextPatternConstantPart>,
}

/// A wrapper around regex_automata
#[derive(Debug)]
pub struct Regex {
    pub regex_impl: regex_automata::meta::Regex,
    pub pattern: String,
}

impl Regex {
    pub fn as_str(&self) -> &str {
        &self.pattern
    }
}

impl Deref for Regex {
    type Target = regex_automata::meta::Regex;

    fn deref(&self) -> &Self::Target {
        &self.regex_impl
    }
}

impl TextPattern {
    pub fn try_capturing_match(
        &self,
        haystack: &str,
        type_def_id: DefId,
        ontology: &Ontology,
    ) -> Result<Value, ParseError> {
        if self.constant_parts.is_empty() {
            if let Some(match_) = self.regex.find(haystack) {
                Ok(Value::Text(
                    haystack[match_.range()].into(),
                    type_def_id.into(),
                ))
            } else {
                Err(ParseError("regular expression did not match".to_string()))
            }
        } else {
            let mut captures = self.regex.create_captures();

            self.regex.captures(haystack, &mut captures);
            if !captures.is_match() {
                return Err(ParseError("regular expression did not match".to_string()));
            }

            let mut attrs = FnvHashMap::default();

            for part in &self.constant_parts {
                match part {
                    TextPatternConstantPart::AnyString { capture_group } => {
                        let text = captures
                            .get_group(*capture_group)
                            .map(|span| &haystack[span.start..span.end])
                            .expect("expected property match");

                        let text_def_id = ontology.ontol_domain_meta().text;

                        attrs.insert(
                            RelId(text_def_id, DefRelTag(0)),
                            Attr::Unit(Value::Text(text.into(), text_def_id.into())),
                        );
                    }
                    TextPatternConstantPart::Literal(_) => {}
                    TextPatternConstantPart::Property(property) => {
                        let capture_group = property.capture_group;
                        trace!("fetching capture group {}", capture_group);

                        let text = captures
                            .get_group(capture_group)
                            .map(|span| &haystack[span.start..span.end])
                            .expect("expected property match");

                        let def = ontology.def(property.type_def_id);
                        let processor = ontology.new_serde_processor(
                            def.operator_addr
                                .expect("No operator addr for pattern constant part"),
                            ProcessorMode::Create,
                        );

                        let attribute = processor
                            .deserialize(StrDeserializer::<serde_json::Error>::new(text))
                            .map_err(|err| {
                                ParseError(format!(
                                    "capture group {capture_group} failed to parse: {err}"
                                ))
                            })?;

                        attrs.insert(property.rel_id, attribute);
                    }
                }
            }

            Ok(Value::Struct(Box::new(attrs), type_def_id.into()))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TextPatternConstantPart {
    AnyString { capture_group: usize },
    Literal(TextConstant),
    Property(TextPatternProperty),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextPatternProperty {
    pub rel_id: RelId,
    pub type_def_id: DefId,
    pub capture_group: usize,
}

pub struct FormatPattern<'d, 'o> {
    pub pattern: &'d TextPattern,
    pub value: &'d Value,
    pub ontology: &'o Ontology,
}

impl<'d, 'o> Display for FormatPattern<'d, 'o> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for constant_part in &self.pattern.constant_parts {
            match (constant_part, self.value) {
                (TextPatternConstantPart::AnyString { .. }, Value::Struct(attrs, _)) => {
                    let rel_id = RelId(self.ontology.ontol_domain_meta().text, DefRelTag(0));
                    let Some(attribute) = attrs.get(&rel_id) else {
                        error!("Attribute {rel_id} missing when formatting capturing text pattern");
                        return Err(std::fmt::Error);
                    };
                    match attribute {
                        Attr::Unit(Value::Text(text, _)) => {
                            write!(f, "{text}")?;
                        }
                        _ => panic!(),
                    }
                }
                (
                    TextPatternConstantPart::Property(TextPatternProperty {
                        rel_id,
                        type_def_id,
                        ..
                    }),
                    Value::Struct(attrs, _),
                ) => {
                    let Some(attribute) = attrs.get(rel_id) else {
                        error!("Attribute {rel_id} missing when formatting capturing text pattern");
                        return Err(std::fmt::Error);
                    };
                    let Attr::Unit(value) = attribute else {
                        error!("Not a unit");
                        return Err(std::fmt::Error);
                    };
                    write!(
                        f,
                        "{}",
                        FormatValueAsText {
                            value,
                            type_def_id: *type_def_id,
                            ontology: self.ontology
                        }
                    )?;
                }
                (TextPatternConstantPart::Literal(constant), _) => {
                    write!(f, "{string}", string = &self.ontology[*constant])?
                }
                (part, data) => {
                    panic!("unable to format pattern - mismatch between {part:?} and {data:?}")
                }
            }
        }
        Ok(())
    }
}

mod serde_regex {
    use super::*;

    pub fn serialize<S>(regex: &Regex, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&regex.pattern)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Regex, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let pattern = String::deserialize(deserializer)?;
        let regex_impl = regex_automata::meta::Regex::new(&pattern)
            .map_err(|e| ::serde::de::Error::custom(format!("{e}")))?;
        Ok(Regex {
            regex_impl,
            pattern,
        })
    }
}
