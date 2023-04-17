use std::{collections::BTreeMap, fmt::Display};

use regex::Regex;
use serde::de::{value::StrDeserializer, DeserializeSeed};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    env::Env,
    serde::processor::{ProcessorLevel, ProcessorMode},
    smart_format,
    string_types::ParseError,
    value::{Data, FormatStringData, PropertyId},
    DefId,
};

#[derive(Debug)]
pub struct StringPattern {
    pub regex: Regex,
    pub constant_parts: Vec<StringPatternConstantPart>,
}

impl StringPattern {
    pub fn try_capturing_match(&self, input: &str, env: &Env) -> Result<Data, ParseError> {
        if self.constant_parts.is_empty() {
            if let Some(match_) = self.regex.find(input) {
                Ok(Data::String(match_.as_str().into()))
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
                    StringPatternConstantPart::AllStrings => {}
                    StringPatternConstantPart::Literal(_) => {}
                    StringPatternConstantPart::Property(property) => {
                        let capture_group = property.capture_group;
                        debug!("fetching capture group {}", capture_group);

                        let text = captures
                            .get(capture_group)
                            .expect("expected property match")
                            .as_str();

                        let type_info = env.get_type_info(property.type_def_id);
                        let processor = env.new_serde_processor(
                            type_info
                                .operator_id
                                .expect("No operator id for pattern constant part"),
                            None,
                            ProcessorMode::Create,
                            ProcessorLevel::Root,
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

#[derive(Debug)]
pub enum StringPatternConstantPart {
    AllStrings,
    Literal(String),
    Property(StringPatternProperty),
}

#[derive(Debug)]
pub struct StringPatternProperty {
    pub property_id: PropertyId,
    pub type_def_id: DefId,
    pub capture_group: usize,
}

pub struct FormatPattern<'a> {
    pub pattern: &'a StringPattern,
    pub data: &'a Data,
}

impl<'a> Display for FormatPattern<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for constant_part in &self.pattern.constant_parts {
            match (constant_part, self.data) {
                (
                    StringPatternConstantPart::Property(StringPatternProperty {
                        property_id, ..
                    }),
                    Data::Struct(attrs),
                ) => {
                    let attribute = attrs.get(property_id).unwrap();
                    write!(f, "{}", FormatStringData(&attribute.value.data))?;
                }
                (StringPatternConstantPart::Literal(string), _) => write!(f, "{string}")?,
                (part, data) => {
                    panic!("unable to format pattern - mismatch between {part:?} and {data:?}")
                }
            }
        }
        Ok(())
    }
}
