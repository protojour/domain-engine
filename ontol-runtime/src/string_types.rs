use uuid::Uuid;

use crate::{
    value::{Data, Value},
    DefId,
};

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum StringLikeType {
    Uuid,
}

#[derive(Debug)]
pub struct ParseError;

impl StringLikeType {
    pub fn try_deserialize(&self, def_id: DefId, str: &str) -> Result<Value, ParseError> {
        match self {
            Self::Uuid => {
                let uuid = Uuid::parse_str(str).map_err(|_| ParseError)?;
                Ok(Value::new(Data::Uuid(uuid), def_id))
            }
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Uuid => "uuid",
        }
    }
}
