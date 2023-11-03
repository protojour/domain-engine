use std::collections::HashMap;

use serde::{
    de::{DeserializeSeed, Error, MapAccess, SeqAccess, Visitor},
    Deserializer,
};

use crate::{
    ontology::Ontology,
    sequence::Sequence,
    value::{Attribute, Data, Value},
    DefId,
};

use super::processor::{ProcessorLevel, RecursionLimitError};

pub fn deserialize_raw<'de, D: Deserializer<'de>>(
    ontology: &Ontology,
    level: ProcessorLevel,
    deserializer: D,
) -> Result<Value, D::Error> {
    RawVisitor { ontology, level }.deserialize(deserializer)
}

#[derive(Clone, Copy)]
pub(crate) struct RawVisitor<'o> {
    ontology: &'o Ontology,
    level: ProcessorLevel,
}

impl<'o> RawVisitor<'o> {
    pub fn new(ontology: &'o Ontology, level: ProcessorLevel) -> Result<Self, RecursionLimitError> {
        Ok(Self {
            ontology,
            level: level.child()?,
        })
    }

    fn child(self) -> Result<Self, RecursionLimitError> {
        Self::new(self.ontology, self.level)
    }
}

impl<'o, 'de> DeserializeSeed<'de> for RawVisitor<'o> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'o, 'de> Visitor<'de> for RawVisitor<'o> {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ONTOL value")
    }

    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
        Ok(Value::unit())
    }

    fn visit_bool<E: Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(Value {
            data: Data::I64(if v { 1 } else { 0 }),
            type_def_id: self.ontology.ontol_domain_meta.bool,
        })
    }

    fn visit_u64<E: Error>(self, v: u64) -> Result<Self::Value, E> {
        Ok(Value {
            data: Data::I64(v.try_into().map_err(|_| E::custom("integer overflow"))?),
            type_def_id: self.ontology.ontol_domain_meta.i64,
        })
    }

    fn visit_i64<E: Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(Value {
            data: Data::I64(v),
            type_def_id: self.ontology.ontol_domain_meta.i64,
        })
    }

    fn visit_f64<E: Error>(self, v: f64) -> Result<Self::Value, E> {
        Ok(Value {
            data: Data::F64(v),
            type_def_id: self.ontology.ontol_domain_meta.f64,
        })
    }

    fn visit_f32<E: Error>(self, v: f32) -> Result<Self::Value, E> {
        Ok(Value {
            data: Data::F64(v as f64),
            type_def_id: self.ontology.ontol_domain_meta.f64,
        })
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(Value {
            data: Data::Text(v.into()),
            type_def_id: self.ontology.ontol_domain_meta.text,
        })
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq_access: A) -> Result<Self::Value, A::Error> {
        let mut sequence = match seq_access.size_hint() {
            Some(cap) => Sequence::new_with_capacity(cap),
            None => Sequence::new([]),
        };

        while let Some(value) =
            seq_access.next_element_seed(self.child().map_err(RecursionLimitError::to_de_error)?)?
        {
            sequence.attrs.push(Attribute::from(value));
        }

        Ok(Value {
            data: Data::Sequence(sequence),
            type_def_id: DefId::unit(),
        })
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map_access: A) -> Result<Self::Value, A::Error> {
        let mut dict = HashMap::default();

        let child = self.child().map_err(RecursionLimitError::to_de_error)?;

        while let Some(key) = map_access.next_key_seed(child)? {
            let Data::Text(key) = key.data else {
                return Err(A::Error::custom("Dictionary keys must be text"));
            };

            dict.insert(key, map_access.next_value_seed(child)?);
        }

        Ok(Value {
            data: Data::Dict(Box::new(dict)),
            type_def_id: DefId::unit(),
        })
    }
}
