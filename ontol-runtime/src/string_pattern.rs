use std::fmt::Display;

use smartstring::alias::String;

use crate::{
    string_types::StringLikeType,
    value::{Data, FormatStringData, PropertyId},
};

/// Extend as required.
///
/// Inspiration:
/// https://docs.rs/regex-syntax/latest/regex_syntax/hir/enum.HirKind.html
pub enum StringPattern {
    Empty,
    Literal(String),
    Property(PropertyId, Option<StringLikeType>),
    Concat(Vec<StringPattern>),
}

pub struct FormatPattern<'a> {
    pattern: &'a StringPattern,
    data: &'a Data,
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
