use serde::{
    ser::{Error, SerializeMap, SerializeSeq},
    Serializer,
};
use tracing::error;

use crate::{ontology::Ontology, value::Value};

use super::processor::{ProcessorLevel, RecursionLimitError};

type Res<S> = Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>;

/// Serialize in raw mode.
///
/// This is essentially domainless serialization.
///
/// It does _not_ support structs, so use with care.
pub fn serialize_raw<S: Serializer>(
    value: &Value,
    ontology: &Ontology,
    level: ProcessorLevel,
    serializer: S,
) -> Res<S> {
    match value {
        Value::Unit(_) => serializer.serialize_unit(),
        Value::I64(int, type_id) => {
            if type_id == &ontology.ontol_domain_meta().bool {
                if *int == 0 {
                    serializer.serialize_bool(false)
                } else {
                    serializer.serialize_bool(true)
                }
            } else {
                serializer.serialize_i64(*int)
            }
        }
        Value::F64(float, _) => serializer.serialize_f64(*float),
        Value::Text(text, _) => serializer.serialize_str(text),
        Value::Dict(dict, _) => {
            let mut map_access = serializer.serialize_map(None)?;

            for (key, value) in dict.iter() {
                map_access.serialize_entry(
                    key,
                    &RawProxy::new_as_child(value, ontology, level)
                        .map_err(RecursionLimitError::to_ser_error)?,
                )?;
            }

            map_access.end()
        }
        Value::Sequence(seq, _) => {
            let mut seq_access = serializer.serialize_seq(Some(seq.attrs.len()))?;

            for attr in &seq.attrs {
                seq_access.serialize_element(
                    &RawProxy::new_as_child(&attr.val, ontology, level)
                        .map_err(RecursionLimitError::to_ser_error)?,
                )?;
            }

            seq_access.end()
        }
        data => {
            error!("Serialize open data {data:?}");
            Err(S::Error::custom("Type not supported in open serialization"))
        }
    }
}

pub(crate) struct RawProxy<'v, 'on> {
    value: &'v Value,
    ontology: &'on Ontology,
    level: ProcessorLevel,
}

impl<'v, 'on> RawProxy<'v, 'on> {
    pub fn new_as_child(
        value: &'v Value,
        ontology: &'on Ontology,
        level: ProcessorLevel,
    ) -> Result<Self, RecursionLimitError> {
        Ok(Self {
            value,
            ontology,
            level: level.child()?,
        })
    }
}

impl<'v, 'on> serde::Serialize for RawProxy<'v, 'on> {
    fn serialize<S>(&self, serializer: S) -> Res<S>
    where
        S: serde::Serializer,
    {
        serialize_raw(self.value, self.ontology, self.level, serializer)
    }
}
