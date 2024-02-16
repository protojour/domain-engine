use std::{fmt::Display, ops::Deref};

use ::serde::{Deserialize, Serialize};
use fnv::FnvHashMap;
use serde::de::{value::StrDeserializer, DeserializeSeed};
use smartstring::alias::String;
use tracing::{debug, error};

use crate::{
    interface::serde::processor::ProcessorMode,
    ontology::Ontology,
    smart_format,
    text_like_types::ParseError,
    value::{Attribute, FormatValueAsText, PropertyId, Value},
    DefId, RelationshipId,
};

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
                Ok(Value::Text(haystack[match_.range()].into(), type_def_id))
            } else {
                Err(ParseError(smart_format!(
                    "regular expression did not match"
                )))
            }
        } else {
            let mut captures = self.regex.create_captures();

            self.regex.captures(haystack, &mut captures);
            if !captures.is_match() {
                return Err(ParseError(smart_format!(
                    "regular expression did not match"
                )));
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
                            PropertyId::subject(crate::RelationshipId(text_def_id)),
                            Attribute {
                                rel: Value::unit(),
                                val: Value::Text(text.into(), text_def_id),
                            },
                        );
                    }
                    TextPatternConstantPart::Literal(_) => {}
                    TextPatternConstantPart::Property(property) => {
                        let capture_group = property.capture_group;
                        debug!("fetching capture group {}", capture_group);

                        let text = captures
                            .get_group(capture_group)
                            .map(|span| &haystack[span.start..span.end])
                            .expect("expected property match");

                        let type_info = ontology.get_type_info(property.type_def_id);
                        let processor = ontology.new_serde_processor(
                            type_info
                                .operator_addr
                                .expect("No operator addr for pattern constant part"),
                            ProcessorMode::Create,
                        );

                        let attribute = processor
                            .deserialize(StrDeserializer::<serde_json::Error>::new(text))
                            .map_err(|err| {
                                ParseError(smart_format!(
                                    "capture group {capture_group} failed to parse: {err}"
                                ))
                            })?;

                        attrs.insert(property.property_id, attribute);
                    }
                }
            }

            Ok(Value::Struct(Box::new(attrs), type_def_id))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TextPatternConstantPart {
    AnyString { capture_group: usize },
    Literal(String),
    Property(TextPatternProperty),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextPatternProperty {
    pub property_id: PropertyId,
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
                    let property_id =
                        PropertyId::subject(RelationshipId(self.ontology.ontol_domain_meta().text));
                    let Some(attribute) = attrs.get(&property_id) else {
                        error!("Attribute {property_id} missing when formatting capturing text pattern");
                        return Err(std::fmt::Error);
                    };
                    match &attribute.val {
                        Value::Text(text, _) => {
                            write!(f, "{text}")?;
                        }
                        _ => panic!(),
                    }
                }
                (
                    TextPatternConstantPart::Property(TextPatternProperty {
                        property_id,
                        type_def_id,
                        ..
                    }),
                    Value::Struct(attrs, _),
                ) => {
                    let Some(attribute) = attrs.get(property_id) else {
                        error!("Attribute {property_id} missing when formatting capturing text pattern");
                        return Err(std::fmt::Error);
                    };
                    write!(
                        f,
                        "{}",
                        FormatValueAsText {
                            value: &attribute.val,
                            type_def_id: *type_def_id,
                            ontology: self.ontology
                        }
                    )?;
                }
                (TextPatternConstantPart::Literal(string), _) => write!(f, "{string}")?,
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
