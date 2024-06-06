use serde::{
    de::{DeserializeSeed, Error, Visitor},
    Deserializer,
};

use crate::value::Attr;

use super::processor::SerdeProcessor;

impl<'on, 'p> SerdeProcessor<'on, 'p> {
    pub fn to_option_processor(self) -> OptionProcessor<'on, 'p> {
        OptionProcessor { inner: self }
    }
}

pub struct OptionProcessor<'on, 'p> {
    inner: SerdeProcessor<'on, 'p>,
}

impl<'on, 'p, 'de> DeserializeSeed<'de> for OptionProcessor<'on, 'p> {
    type Value = Option<Attr>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_option(self)
    }
}

impl<'on, 'p, 'de> Visitor<'de> for OptionProcessor<'on, 'p> {
    type Value = Option<Attr>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "option")
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        Ok(Some(self.inner.deserialize(deserializer)?))
    }

    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }
}
