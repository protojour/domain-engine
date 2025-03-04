use base64::Engine;

use crate::{
    DefId, OntolDefTag,
    value::{OctetSequence, Value},
};

use super::ValueMatcher;

pub struct OctetsMatcher {
    def_id: DefId,
    format: OctetsFormat,
}

pub enum OctetsFormat {
    Unknown,
    Hex,
    Base64,
}

impl OctetsFormat {
    pub fn from_format_def(format_def_id: DefId) -> Self {
        if format_def_id == OntolDefTag::FormatHex.def_id() {
            Self::Hex
        } else if format_def_id == OntolDefTag::FormatBase64.def_id() {
            Self::Base64
        } else {
            Self::Unknown
        }
    }
}

impl OctetsMatcher {
    pub fn new(def_id: DefId, format_def_id: DefId) -> Self {
        Self {
            def_id,
            format: OctetsFormat::from_format_def(format_def_id),
        }
    }
}

impl ValueMatcher for OctetsMatcher {
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.format {
            OctetsFormat::Unknown => write!(f, "string"),
            OctetsFormat::Hex => write!(f, "hex"),
            OctetsFormat::Base64 => write!(f, "base64"),
        }
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        match &self.format {
            OctetsFormat::Hex => {
                let bytes = hex::decode(str).map_err(|_| ())?;
                Ok(Value::OctetSequence(
                    OctetSequence(bytes.into()),
                    self.def_id.into(),
                ))
            }
            OctetsFormat::Base64 => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(str)
                    .map_err(|_| ())?;
                Ok(Value::OctetSequence(
                    OctetSequence(bytes.into()),
                    self.def_id.into(),
                ))
            }
            OctetsFormat::Unknown => Err(()),
        }
    }
}
