use ::serde::{Deserialize, Serialize};
use smartstring::alias::String;
use tracing::error;
use uuid::Uuid;

use crate::{
    smart_format,
    value::{Data, Value},
    DefId,
};

#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum TextLikeType {
    Uuid,
    DateTime,
}

#[derive(Debug)]
pub struct ParseError(pub String);

impl TextLikeType {
    pub fn try_deserialize(&self, def_id: DefId, str: &str) -> Result<Value, ParseError> {
        match self {
            Self::Uuid => {
                let uuid =
                    Uuid::parse_str(str).map_err(|error| ParseError(smart_format!("{}", error)))?;
                Ok(Value::new(
                    Data::OctetSequence(uuid.as_bytes().to_vec()),
                    def_id,
                ))
            }
            Self::DateTime => {
                let datetime = chrono::DateTime::parse_from_rfc3339(str)
                    .map_err(|error| ParseError(smart_format!("{}", error)))?;
                Ok(Value::new(Data::ChronoDateTime(datetime.into()), def_id))
            }
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Uuid => "uuid",
            Self::DateTime => "datetime",
        }
    }
    pub fn format(&self, data: &Data, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self, data) {
            (Self::Uuid, Data::OctetSequence(octets)) => {
                let uuid = Uuid::from_slice(octets).map_err(|error| {
                    error!("Uuid not constructable from octets: {error:?}");
                    std::fmt::Error
                })?;

                write!(f, "{uuid}")
            }
            (Self::DateTime, Data::ChronoDate(naive_date)) => {
                write!(f, "{naive_date}")
            }
            (Self::DateTime, Data::ChronoDateTime(datetime)) => {
                // FIXME: A way to not do this via String
                // Chrono 0.5 hopefully fixes this
                write!(f, "{}", datetime.to_rfc3339())
            }
            _ => Err(std::fmt::Error),
        }
    }
}
