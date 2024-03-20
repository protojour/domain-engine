use arcstr::ArcStr;
use serde::{Deserialize, Serialize, Serializer};

use crate::ontology::ontol::TextConstant;

#[derive(Clone, Debug)]
pub struct PhfKey {
    pub string: ArcStr,
    pub constant: TextConstant,
}

impl PhfKey {
    pub fn arc_str(&self) -> &ArcStr {
        &self.string
    }

    pub fn constant(&self) -> TextConstant {
        self.constant
    }
}

impl Serialize for PhfKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.constant.0)
    }
}

/// Note: The PhfKey will be post-processed by yhe Ontology before use
impl<'de> Deserialize<'de> for PhfKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let constant = <u32 as Deserialize>::deserialize(deserializer)?;
        Ok(Self {
            string: ArcStr::default(),
            constant: TextConstant(constant),
        })
    }
}
