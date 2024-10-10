use std::{fmt::Display, ops::Deref};

use ::serde::{Deserialize, Serialize};
use fnv::FnvHashMap;
use serde::de::{value::StrDeserializer, DeserializeSeed};
use tracing::{error, trace};

use crate::{
    attr::Attr,
    interface::serde::processor::{ProcessorMode, SerdeProcessor},
    ontology::aspects::{aspect, DefsAspect, ExecutionAspect, SerdeAspect},
    value::{ValueFormatRaw, Value},
    DefId, DefPropTag, OntolDefTag, PropId,
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
        ontology: &(impl AsRef<SerdeAspect> + AsRef<DefsAspect> + AsRef<ExecutionAspect>),
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

                        let text_def_id = OntolDefTag::Text.def_id();

                        attrs.insert(
                            PropId(text_def_id, DefPropTag(0)),
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

                        let def = aspect::<DefsAspect>(ontology).def(property.type_def_id);
                        let processor = SerdeProcessor::new(
                            def.operator_addr
                                .expect("No operator addr for pattern constant part"),
                            ProcessorMode::Create,
                            ontology,
                        );

                        let attribute = processor
                            .deserialize(StrDeserializer::<serde_json::Error>::new(text))
                            .map_err(|err| {
                                ParseError(format!(
                                    "capture group {capture_group} failed to parse: {err}"
                                ))
                            })?;

                        attrs.insert(property.prop_id, attribute);
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
    pub prop_id: PropId,
    pub type_def_id: DefId,
    pub capture_group: usize,
}

pub struct FormatPattern<'d, 'on> {
    value: &'d Value,
    pattern: &'d TextPattern,
    defs: &'on DefsAspect,
}

impl<'d, 'on> FormatPattern<'d, 'on> {
    pub fn new(value: &'d Value, pattern: &'d TextPattern, defs: &'on DefsAspect) -> Self {
        Self {
            value,
            pattern,
            defs,
        }
    }
}

impl<'d, 'on> Display for FormatPattern<'d, 'on> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for constant_part in &self.pattern.constant_parts {
            match (constant_part, self.value) {
                (TextPatternConstantPart::AnyString { .. }, Value::Struct(attrs, _)) => {
                    let prop_id = PropId(OntolDefTag::Text.def_id(), DefPropTag(0));
                    let Some(attribute) = attrs.get(&prop_id) else {
                        error!(
                            "Attribute {prop_id} missing when formatting capturing text pattern"
                        );
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
                        prop_id,
                        type_def_id,
                        ..
                    }),
                    Value::Struct(attrs, _),
                ) => {
                    let Some(attribute) = attrs.get(prop_id) else {
                        error!(
                            "Attribute {prop_id} missing when formatting capturing text pattern"
                        );
                        return Err(std::fmt::Error);
                    };
                    let Attr::Unit(value) = attribute else {
                        error!("Not a unit");
                        return Err(std::fmt::Error);
                    };
                    write!(f, "{}", ValueFormatRaw::new(value, *type_def_id, self.defs))?;
                }
                (TextPatternConstantPart::Literal(constant), _) => {
                    write!(f, "{string}", string = &self.defs[*constant])?
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
