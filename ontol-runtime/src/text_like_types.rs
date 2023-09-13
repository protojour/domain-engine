use ::serde::{Deserialize, Serialize};
use smartstring::alias::String;
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
                Ok(Value::new(Data::Uuid(uuid), def_id))
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
}
