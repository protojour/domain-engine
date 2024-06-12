use fnv::FnvHashMap;
use serde::de::{value::StrDeserializer, DeserializeSeed};
use tracing::error;

use crate::{
    attr::Attr,
    interface::serde::processor::{ProcessorMode, ScalarFormat},
    ontology::{
        ontol::{
            text_pattern::{TextPattern, TextPatternConstantPart},
            ParseError,
        },
        Ontology,
    },
    value::{Value, ValueTag},
    DefId, RelationshipId,
};

use super::ValueMatcher;

/// match any string
pub struct StringMatcher<'on> {
    pub def_id: DefId,
    pub ontology: &'on Ontology,
}

impl<'on> ValueMatcher for StringMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Even though it's called "text" in ONTOL, we can call it "string" in domain interfaces
        write!(f, "string")
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        Ok(Value::Text(str.into(), self.def_id.into()))
    }
}

/// match a constant text
pub struct ConstantStringMatcher<'on> {
    pub constant: &'on str,
    pub def_id: DefId,
}

impl<'on> ValueMatcher for ConstantStringMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "\"{}\"", self.constant)
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        if str == self.constant {
            Ok(Value::Text(str.into(), self.def_id.into()))
        } else {
            Err(())
        }
    }
}

pub struct TextPatternMatcher<'on> {
    pub pattern: &'on TextPattern,
    pub def_id: DefId,
    pub ontology: &'on Ontology,
}

impl<'on> ValueMatcher for TextPatternMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        expecting_custom_string(self.ontology, self.def_id, f)
            .unwrap_or_else(|| write!(f, "string matching /{}/", self.pattern.regex.as_str()))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        if self.pattern.regex.is_match(str) {
            try_deserialize_custom_string(self.ontology, self.def_id, str).map_err(|_| ())
        } else {
            Err(())
        }
    }
}

/// This is a matcher that doesn't necessarily
/// deserialize to a string, but can use capture groups
/// extract various data.
pub struct CapturingTextPatternMatcher<'on> {
    pub pattern: &'on TextPattern,
    pub def_id: DefId,
    pub ontology: &'on Ontology,
    pub scalar_format: ScalarFormat,
}

impl<'on> ValueMatcher for CapturingTextPatternMatcher<'on> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string matching /{}/", self.pattern.regex.as_str())
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        match self.scalar_format {
            ScalarFormat::DomainTransparent => Ok(self
                .pattern
                .try_capturing_match(str, self.def_id, self.ontology)
                .map_err(|_| ())?),

            ScalarFormat::RawText => {
                let mut attrs = FnvHashMap::default();

                for part in &self.pattern.constant_parts {
                    match part {
                        TextPatternConstantPart::Property(property) => {
                            if !attrs.is_empty() {
                                error!("A value was already read");
                                return Err(());
                            }

                            let type_info = self.ontology.get_type_info(property.type_def_id);
                            let processor = self.ontology.new_serde_processor(
                                type_info
                                    .operator_addr
                                    .expect("No operator addr for pattern constant part"),
                                ProcessorMode::Create,
                            );

                            let attribute = processor
                                .deserialize(StrDeserializer::<serde_json::Error>::new(str))
                                .map_err(|_| ())?;

                            attrs.insert(property.rel_id, attribute);
                        }
                        TextPatternConstantPart::AnyString { .. } => {
                            let text_tag: ValueTag = self.ontology.ontol_domain_meta().text.into();
                            let rel_id = RelationshipId(text_tag.def());

                            attrs.insert(rel_id, Attr::Unit(Value::Text(str.into(), text_tag)));
                        }
                        TextPatternConstantPart::Literal(_) => {}
                    }
                }

                Ok(Value::Struct(Box::new(attrs), self.def_id.into()))
            }
        }
    }
}

pub fn try_deserialize_custom_string(
    ontology: &Ontology,
    def_id: DefId,
    str: &str,
) -> Result<Value, ParseError> {
    match ontology.data.text_like_types.get(&def_id) {
        Some(custom_string_deserializer) => custom_string_deserializer.try_deserialize(def_id, str),
        None => Ok(Value::Text(str.into(), def_id.into())),
    }
}

fn expecting_custom_string(
    ontology: &Ontology,
    def_id: DefId,
    f: &mut std::fmt::Formatter,
) -> Option<std::fmt::Result> {
    ontology
        .data
        .text_like_types
        .get(&def_id)
        .map(|custom_string_deserializer| write!(f, "`{}`", custom_string_deserializer.type_name()))
}
