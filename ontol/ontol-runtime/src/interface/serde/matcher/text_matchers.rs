use fnv::FnvHashMap;
use serde::de::{DeserializeSeed, value::StrDeserializer};
use tracing::error;

use crate::{
    DefId, DefPropTag, OntolDefTag, OntolDefTagExt, PropId,
    attr::Attr,
    interface::serde::{
        OntologyCtx,
        processor::{ProcessorMode, ScalarFormat, SerdeProcessor},
    },
    ontology::ontol::{
        ParseError,
        text_pattern::{TextPattern, TextPatternConstantPart},
    },
    value::{Value, ValueTag},
};

use super::ValueMatcher;

/// match any string
pub struct StringMatcher {
    pub def_id: DefId,
}

impl ValueMatcher for StringMatcher {
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

impl ValueMatcher for ConstantStringMatcher<'_> {
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
    pub ontology: OntologyCtx<'on>,
}

impl ValueMatcher for TextPatternMatcher<'_> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        expecting_custom_string(self.ontology, self.def_id, f)
            .unwrap_or_else(|| write!(f, "string matching /{}/", self.pattern.regex.as_str()))
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        if self.pattern.regex.is_match(str) {
            try_deserialize_custom_string(str, self.def_id, self.ontology).map_err(|_| ())
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
    pub ontology: OntologyCtx<'on>,
    pub scalar_format: ScalarFormat,
}

impl ValueMatcher for CapturingTextPatternMatcher<'_> {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "string matching /{}/", self.pattern.regex.as_str())
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        match self.scalar_format {
            ScalarFormat::DomainTransparent => Ok(self
                .pattern
                .try_capturing_match(str, self.def_id, &self.ontology)
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

                            let def = self.ontology.defs.def(property.type_def_id);
                            let processor = SerdeProcessor::new(
                                def.operator_addr
                                    .expect("No operator addr for pattern constant part"),
                                ProcessorMode::Create,
                                &self.ontology,
                            );
                            let attribute = processor
                                .deserialize(StrDeserializer::<serde_json::Error>::new(str))
                                .map_err(|_| ())?;

                            attrs.insert(property.prop_id, attribute);
                        }
                        TextPatternConstantPart::AnyString { .. } => {
                            let text_tag: ValueTag = OntolDefTag::Text.def_id().into();
                            let prop_id = PropId(text_tag.def_id(), DefPropTag(0));

                            attrs.insert(prop_id, Attr::Unit(Value::Text(str.into(), text_tag)));
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
    str: &str,
    def_id: DefId,
    ontology: OntologyCtx,
) -> Result<Value, ParseError> {
    match ontology.defs.text_like_types.get(&def_id) {
        Some(custom_string_deserializer) => custom_string_deserializer.try_deserialize(def_id, str),
        None => Ok(Value::Text(str.into(), def_id.into())),
    }
}

fn expecting_custom_string(
    ontology: OntologyCtx,
    def_id: DefId,
    f: &mut std::fmt::Formatter,
) -> Option<std::fmt::Result> {
    ontology
        .defs
        .text_like_types
        .get(&def_id)
        .map(|custom_string_deserializer| write!(f, "`{}`", custom_string_deserializer.type_name()))
}
