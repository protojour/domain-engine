use std::{collections::BTreeMap, fmt::Display};

use derive_debug_extras::DebugExtras;
use regex::Regex;
use smartstring::alias::String;

use crate::{
    string_types::{ParseError, StringLikeType},
    value::{Attribute, Data, FormatStringData, PropertyId, Value},
    DefId,
};

/// Extend as required.
///
/// Inspiration:
/// https://docs.rs/regex-syntax/latest/regex_syntax/hir/enum.HirKind.html
///
#[derive(Eq, PartialEq, Hash)]
pub enum StringPattern {
    Empty,
    Literal(String),
    Property(PropertyId, Option<StringLikeType>),
    Concat(Vec<StringPattern>),
}

impl StringPattern {
    pub fn try_match(&self, input: &str) -> Result<Data, ParseError> {
        let mut properties = BTreeMap::new();
        match self.match_internal(input, &mut properties) {
            Ok(_) => Ok(if properties.is_empty() {
                Data::String(input.into())
            } else {
                Data::Map(properties)
            }),
            Err(err) => Err(err),
        }
    }

    fn match_internal<'s>(
        &self,
        input: &'s str,
        _properties: &mut BTreeMap<PropertyId, Attribute>,
    ) -> Result<&'s str, ParseError> {
        match self {
            Self::Empty => Ok(input),
            Self::Literal(lit) => {
                if input == lit {
                    Ok(input)
                } else {
                    Err(ParseError)
                }
            }
            Self::Property(_, _) => todo!(),
            Self::Concat(patterns) => {
                let mut cursor = input;
                for pattern in patterns {
                    cursor = pattern.match_internal(cursor, _properties)?;
                }
                Ok(cursor)
            }
        }
    }
}

#[derive(Debug)]
pub struct StringPattern2 {
    pub regex: Regex,
    pub properties: Vec<StringPatternProperty>,
}

impl StringPattern2 {
    pub fn try_match(&self, input: &str) -> Result<Data, ParseError> {
        if self.properties.is_empty() {
            if let Some(match_) = self.regex.find(input) {
                Ok(Data::String(match_.as_str().into()))
            } else {
                Err(ParseError)
            }
        } else {
            let captures = self.regex.captures(input).ok_or(ParseError)?;
            let mut properties = BTreeMap::new();

            for property in &self.properties {
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

            Ok(Data::Map(properties))
        }
    }
}

#[derive(Debug)]
pub struct StringPatternProperty {
    property_id: PropertyId,
    type_def_id: DefId,
    capture_group: usize,
    string_type: Option<StringLikeType>,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, DebugExtras)]
#[debug_single_tuple_inline]
pub struct StringPatternId(pub u32);

pub struct DisplayPatternRoot<'a>(pub &'a StringPattern);

impl<'a> Display for DisplayPatternRoot<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            StringPattern::Empty => write!(f, "\"\""),
            StringPattern::Literal(lit) => write!(f, "\"{lit}\""),
            StringPattern::Property(_, string_like) => match string_like {
                Some(string_like_type) => write!(f, "`{}`", string_like_type.type_name()),
                None => write!(f, "`string`"),
            },
            _ => write!(f, "`regex`"),
        }
    }
}

pub struct FormatPattern<'a> {
    pub pattern: &'a StringPattern,
    pub data: &'a Data,
}

impl<'a> Display for FormatPattern<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.pattern {
            StringPattern::Empty => Ok(()),
            StringPattern::Literal(s) => write!(f, "{s}"),
            StringPattern::Property(property_id, _) => {
                let attribute = match self.data {
                    Data::Map(map) => map.get(property_id).expect("property not in map"),
                    _ => panic!("not a map"),
                };
                write!(f, "{}", FormatStringData(&attribute.value.data))
            }
            StringPattern::Concat(vec) => {
                for pattern in vec {
                    write!(
                        f,
                        "{}",
                        FormatPattern {
                            pattern,
                            data: self.data
                        }
                    )?;
                }
                Ok(())
            }
        }
    }
}
