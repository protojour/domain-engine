use std::str::FromStr;

use ::serde::{Deserialize, Serialize};
use ontol_macros::RustDoc;
use tracing::error;
use ulid::Ulid;
use uuid::Uuid;

use crate::{
    value::{Value, ValueTagError},
    DefId,
};

#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize, RustDoc)]
pub enum TextLikeType {
    /// A UUID value, e.g. `'018e0f80-7bb4-7475-ac1d-7a0ada330122'`.
    ///
    /// The input deserializer accepts any UUID-conformant value, but [generated values](generator_types.md) are
    /// [UUIDv7](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)
    /// (time-ordered), making them naturally sortable and ideal as database primary keys.
    /// ```ontol
    /// rel* is: uuid
    /// ```
    Uuid,
    /// A [Universally Unique Lexicographically Sortable Identifier](https://github.com/ulid/spec).
    Ulid,
    /// An [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339)-formatted
    /// datetime string, e.g. `'1970-01-01T00:00:00.000Z'`.
    ///
    /// The input deserializer accepts any RFC3339-conformant value, but [generated values](generator_types.md)
    /// are in UTC, and the output serializer will format values with the structure
    /// `'1970-01-01T00:00:00+00:00'`
    /// ```ontol
    /// rel* is: datetime
    /// ```
    DateTime,
}

#[derive(Debug)]
pub struct ParseError(pub String);

impl TextLikeType {
    pub fn try_deserialize(&self, def_id: DefId, str: &str) -> Result<Value, ParseError> {
        match self {
            Self::Uuid => {
                let uuid =
                    Uuid::parse_str(str).map_err(|error| ParseError(format!("{}", error)))?;
                Ok(Value::OctetSequence(
                    uuid.into_bytes().into_iter().collect(),
                    def_id.into(),
                ))
            }
            Self::Ulid => {
                let ulid = Ulid::from_str(str).map_err(|error| ParseError(format!("{}", error)))?;
                Ok(Value::OctetSequence(
                    ulid.to_bytes().into_iter().collect(),
                    def_id.into(),
                ))
            }
            Self::DateTime => {
                let datetime = chrono::DateTime::parse_from_rfc3339(str)
                    .map_err(|error| ParseError(format!("{}", error)))?;
                Ok(Value::ChronoDateTime(datetime.into(), def_id.into()))
            }
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Uuid => "uuid",
            Self::Ulid => "ulid",
            Self::DateTime => "datetime",
        }
    }
    pub fn format(&self, value: &Value, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self, value) {
            (Self::Uuid, Value::OctetSequence(octets, _)) => {
                let uuid = Uuid::from_slice(octets).map_err(|error| {
                    error!("Uuid not constructable from octets: {error:?}");
                    std::fmt::Error
                })?;

                write!(f, "{uuid}")
            }
            (Self::Ulid, Value::OctetSequence(octets, _)) => {
                if octets.len() != 16 {
                    return Err(std::fmt::Error);
                }

                let mut bytes = [0u8; 16];
                bytes.clone_from_slice(octets);
                let ulid = Ulid::from_bytes(bytes);

                write!(f, "{ulid}")
            }
            (Self::DateTime, Value::ChronoDate(naive_date, _)) => {
                write!(f, "{naive_date}")
            }
            (Self::DateTime, Value::ChronoDateTime(datetime, _)) => {
                // FIXME: A way to not do this via String
                // Chrono 0.5 hopefully fixes this
                let use_z = true;
                write!(
                    f,
                    "{}",
                    datetime.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, use_z)
                )
            }
            _ => Err(std::fmt::Error),
        }
    }
}

impl From<ValueTagError> for ParseError {
    fn from(_value: ValueTagError) -> Self {
        ParseError("invalid package id".to_string())
    }
}
