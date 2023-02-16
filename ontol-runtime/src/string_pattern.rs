use std::{collections::BTreeMap, fmt::Display};

use regex::Regex;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    string_types::{ParseError, StringLikeType},
    value::{Attribute, Data, FormatStringData, PropertyId, Value},
    DefId,
};

#[derive(Debug)]
pub struct StringPattern {
    pub regex: Regex,
    pub constant_parts: Vec<StringPatternConstantPart>,
}

impl StringPattern {
    pub fn try_match(&self, input: &str) -> Result<Data, ParseError> {
        if self.constant_parts.is_empty() {
            if let Some(match_) = self.regex.find(input) {
                Ok(Data::String(match_.as_str().into()))
            } else {
                Err(ParseError)
            }
        } else {
            let captures = self.regex.captures(input).ok_or(ParseError)?;
            let mut properties = BTreeMap::new();

            for part in &self.constant_parts {
                match part {
                    StringPatternConstantPart::Property(property) => {
                        debug!("fetching capture group {}", property.capture_group);

                        let text = captures
                            .get(property.capture_group)
                            .expect("expected property match")
                            .as_str();

                        let value = match &property.string_type {
                            Some(string_like_type) => string_like_type
                                .try_deserialize(property.type_def_id, text)
                                .expect("BUG: string-like type did not match pattern"),
                            None => Value::new(Data::String(text.into()), property.type_def_id),
                        };

                        properties.insert(property.property_id, Attribute::with_unit_params(value));
                    }
                    StringPatternConstantPart::Literal(_) => {}
                }
            }

            Ok(Data::Map(properties))
        }
    }
}

#[derive(Debug)]
pub enum StringPatternConstantPart {
    Literal(String),
    Property(StringPatternProperty),
}

#[derive(Debug)]
pub struct StringPatternProperty {
    pub property_id: PropertyId,
    pub type_def_id: DefId,
    pub capture_group: usize,
    pub string_type: Option<StringLikeType>,
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
                    Data::Map(map),
                ) => {
                    let attribute = map.get(property_id).unwrap();
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
