use std::fmt::Display;

use ::serde::{Deserialize, Serialize};
use fnv::FnvHashMap;
use regex::Regex;
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

impl TextPattern {
    pub fn try_capturing_match(
        &self,
        input: &str,
        type_def_id: DefId,
        ontology: &Ontology,
    ) -> Result<Value, ParseError> {
        if self.constant_parts.is_empty() {
            if let Some(match_) = self.regex.find(input) {
                Ok(Value::Text(match_.as_str().into(), type_def_id))
            } else {
                Err(ParseError(smart_format!(
                    "regular expression did not match"
                )))
            }
        } else {
            let captures = self.regex.captures(input).ok_or(ParseError(smart_format!(
                "regular expression did not match"
            )))?;
            let mut attrs = FnvHashMap::default();

            for part in &self.constant_parts {
                match part {
                    TextPatternConstantPart::AllStrings { capture_group } => {
                        let text = captures
                            .get(*capture_group)
                            .expect("expected property match")
                            .as_str();

                        let text_def_id = ontology.ontol_domain_meta().text;

                        attrs.insert(
                            PropertyId::subject(crate::RelationshipId(text_def_id)),
                            Attribute {
                                value: Value::Text(text.into(), text_def_id),
                                rel_params: Value::unit(),
                            },
                        );
                    }
                    TextPatternConstantPart::Literal(_) => {}
                    TextPatternConstantPart::Property(property) => {
                        let capture_group = property.capture_group;
                        debug!("fetching capture group {}", capture_group);

                        let text = captures
                            .get(capture_group)
                            .expect("expected property match")
                            .as_str();

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
    AllStrings { capture_group: usize },
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
                (TextPatternConstantPart::AllStrings { .. }, Value::Struct(attrs, _)) => {
                    let property_id =
                        PropertyId::subject(RelationshipId(self.ontology.ontol_domain_meta().text));
                    let Some(attribute) = attrs.get(&property_id) else {
                        error!("Attribute {property_id} missing when formatting capturing text pattern");
                        return Err(std::fmt::Error);
                    };
                    match &attribute.value {
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
                            value: &attribute.value,
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
        serializer.serialize_str(regex.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Regex, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Regex::new(&s).map_err(|e| ::serde::de::Error::custom(format!("{e}")))
    }
}
