use serde::de::{DeserializeSeed, Visitor};

use crate::value::Attribute;

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
    type Value = Option<Attribute>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_option(self)
    }
}

impl<'on, 'p, 'de> Visitor<'de> for OptionProcessor<'on, 'p> {
    type Value = Option<Attribute>;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "option")
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Some(self.inner.deserialize(deserializer)?))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }
}
