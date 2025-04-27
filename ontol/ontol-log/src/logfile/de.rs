use std::{error::Error, fmt::Display, str::FromStr};

use pest::iterators::{Pair, Pairs};
use serde::{
    Deserializer,
    de::{self, EnumAccess, MapAccess, SeqAccess, VariantAccess},
};

use super::parser::Rule;

#[derive(Clone, Debug)]
pub struct LogfileDeserializer<'de> {
    rule: Rule,
    slice: &'de str,
    pairs: Pairs<'de, Rule>,
}

#[derive(Debug)]
pub struct DeError(String);

impl Display for DeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for DeError {}

impl serde::de::Error for DeError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self(format!("{msg}"))
    }
}

impl<'de> LogfileDeserializer<'de> {
    pub fn new(root: Pair<'de, Rule>) -> Self {
        Self {
            rule: root.as_rule(),
            slice: root.as_str(),
            pairs: root.into_inner(),
        }
    }

    fn assert_rule(&self, rule: Rule) -> Result<(), DeError> {
        if self.rule != rule {
            Err(DeError(format!("expected {rule:?}, found {:?}", self.rule)))
        } else {
            Ok(())
        }
    }

    fn next(&mut self) -> Result<Pair<'de, Rule>, DeError> {
        self.pairs
            .next()
            .ok_or_else(|| DeError("missing item".to_string()))
    }

    fn next_match(&mut self, mut f: impl FnMut(Rule) -> bool) -> Result<Pair<'de, Rule>, DeError> {
        let next = self.next()?;
        if f(next.as_rule()) {
            Ok(next)
        } else {
            Err(DeError(format!(
                "type mismatch: expected {:?}",
                next.as_rule()
            )))
        }
    }

    fn signed_number(mut self) -> Result<Self, DeError> {
        self.assert_rule(Rule::signed_number)?;
        Ok(Self::new(self.pairs.next().unwrap()))
    }

    fn visit_any<V>(self, visitor: V) -> Result<V::Value, DeError>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.rule {
            Rule::list | Rule::logfile => visitor.visit_seq(self),
            Rule::string_lit => visitor.visit_str(self.string_lit_contents()),
            rule => Err(DeError(format!("unsupported dynamic rule: {rule:?}"))),
        }
    }

    fn string_lit_contents(&self) -> &str {
        let slice = self
            .slice
            .strip_prefix("\"")
            .unwrap()
            .strip_suffix("\"")
            .unwrap();

        slice
    }
}

#[allow(unused)]
impl<'de> Deserializer<'de> for LogfileDeserializer<'de> {
    type Error = DeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.visit_any(visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.assert_rule(Rule::bool_lit)?;
        match self.slice {
            "true" => visitor.visit_bool(true),
            "false" => visitor.visit_bool(false),
            _ => panic!(),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_i8(
            i8::from_str(self.signed_number()?.slice)
                .map_err(|_| DeError("invalid int".to_string()))?,
        )
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_i16(
            i16::from_str(self.signed_number()?.slice)
                .map_err(|_| DeError("invalid int".to_string()))?,
        )
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_i32(
            i32::from_str(self.signed_number()?.slice)
                .map_err(|_| DeError("invalid int".to_string()))?,
        )
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_i64(
            i64::from_str(self.signed_number()?.slice)
                .map_err(|_| DeError("invalid int".to_string()))?,
        )
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_u8(
            u8::from_str(self.signed_number()?.slice)
                .map_err(|_| DeError("invalid int".to_string()))?,
        )
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_u16(
            u16::from_str(self.signed_number()?.slice)
                .map_err(|_| DeError("invalid int".to_string()))?,
        )
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_u32(
            u32::from_str(self.signed_number()?.slice)
                .map_err(|_| DeError("invalid int".to_string()))?,
        )
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_u64(
            u64::from_str(self.signed_number()?.slice)
                .map_err(|_| DeError("invalid int".to_string()))?,
        )
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(DeError("float not supported".to_string()))
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(DeError("float not supported".to_string()))
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(DeError("char not supported".to_string()))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.assert_rule(Rule::string_lit)?;
        visitor.visit_str(self.string_lit_contents())
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.assert_rule(Rule::bytes_lit)?;
        let slice = self.slice.strip_prefix("#").unwrap();
        let buf = hex::decode(slice)
            .map_err(|_| DeError("invalid hexadecimal value in bytes".to_string()))?;

        visitor.visit_byte_buf(buf)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(DeError("byte buf not supported".to_string()))
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if self.rule == Rule::list && self.pairs.next().is_none() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(DeError("unit struct not supported".to_string()))
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if matches!(self.rule, Rule::logfile | Rule::list) {
            visitor.visit_seq(self)
        } else {
            Err(DeError("expected list".to_string()))
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.visit_any(visitor)
    }

    fn deserialize_tuple_struct<V>(
        mut self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        mut self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.assert_rule(Rule::symbol)?;
        visitor.visit_str(self.slice)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(DeError("ignored not supported".to_string()))
    }
}

impl<'de> SeqAccess<'de> for LogfileDeserializer<'de> {
    type Error = DeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        let Some(next) = self.pairs.next() else {
            return Ok(None);
        };

        if next.as_rule() == Rule::EOI {
            return Ok(None);
        }

        Ok(Some(seed.deserialize(LogfileDeserializer::new(next))?))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(
            self.pairs
                .clone()
                .filter(|p| !matches!(p.as_rule(), Rule::EOI))
                .count(),
        )
    }
}

impl<'de> MapAccess<'de> for LogfileDeserializer<'de> {
    type Error = DeError;

    fn next_key_seed<K>(&mut self, _seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        Err(DeError("map not supported yet".to_owned()))
    }

    fn next_value_seed<V>(&mut self, _seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        Err(DeError("map not supported yet".to_owned()))
    }
}

impl<'de> EnumAccess<'de> for LogfileDeserializer<'de> {
    type Error = DeError;
    type Variant = LogfileDeserializer<'de>;

    fn variant_seed<V>(mut self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        match self.rule {
            Rule::symbol => {
                let val = seed.deserialize(self.clone())?;
                Ok((val, self))
            }
            Rule::list => {
                let next = self.next_match(|r| matches!(r, Rule::symbol))?;
                let val = seed.deserialize(LogfileDeserializer::new(next))?;

                Ok((val, self))
            }
            _ => Err(DeError("invalid variant".to_string())),
        }
    }
}

impl<'de> VariantAccess<'de> for LogfileDeserializer<'de> {
    type Error = DeError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(mut self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        let item = self.next()?;
        seed.deserialize(LogfileDeserializer::new(item))
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        de::Deserializer::deserialize_seq(self, visitor)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(DeError("struct variant not supported yet".to_owned()))
    }
}
