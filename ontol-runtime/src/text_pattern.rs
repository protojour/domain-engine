use std::{collections::BTreeMap, fmt::Display};

use ::serde::{Deserialize, Serialize};
use regex::Regex;
use serde::de::{value::StrDeserializer, DeserializeSeed};
use smartstring::alias::String;
use tracing::{debug, error};

use crate::{
    interface::serde::processor::ProcessorMode,
    ontology::Ontology,
    smart_format,
    text_like_types::ParseError,
    value::{Data, FormatDataAsText, PropertyId},
    DefId,
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
        ontology: &Ontology,
    ) -> Result<Data, ParseError> {
        if self.constant_parts.is_empty() {
            if let Some(match_) = self.regex.find(input) {
                Ok(Data::Text(match_.as_str().into()))
            } else {
                Err(ParseError(smart_format!(
                    "regular expression did not match"
                )))
            }
        } else {
            let captures = self.regex.captures(input).ok_or(ParseError(smart_format!(
                "regular expression did not match"
            )))?;
            let mut attrs = BTreeMap::new();

            for part in &self.constant_parts {
                match part {
                    TextPatternConstantPart::AllStrings => {}
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

            Ok(Data::Struct(attrs))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TextPatternConstantPart {
    AllStrings,
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
    pub data: &'d Data,
    pub ontology: &'o Ontology,
}

impl<'d, 'o> Display for FormatPattern<'d, 'o> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for constant_part in &self.pattern.constant_parts {
            match (constant_part, self.data) {
                (
                    TextPatternConstantPart::Property(TextPatternProperty {
                        property_id,
                        type_def_id,
                        ..
                    }),
                    Data::Struct(attrs),
                ) => {
                    let Some(attribute) = attrs.get(property_id) else {
                        error!("Attribute {property_id} missing when formatting capturing text pattern");
                        return Err(std::fmt::Error);
                    };
                    write!(
                        f,
                        "{}",
                        FormatDataAsText {
                            data: &attribute.value.data,
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
