use std::{fmt::Debug, hash::Hash};

pub use automerge::Automerge;
use serde::{Deserialize, Serialize};

use crate::equality::OntolHash;

#[derive(Clone)]
pub struct CrdtStruct(pub Box<Automerge>);

impl Debug for CrdtStruct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrdtStruct").finish()
    }
}

impl Serialize for CrdtStruct {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("cannot serialize a CrdtStruct"))
    }
}

impl<'de> Deserialize<'de> for CrdtStruct {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(serde::de::Error::custom("cannot deserialize a CrdtStruct"))
    }
}

impl OntolHash for CrdtStruct {
    fn ontol_hash(&self, h: &mut ahash::AHasher, _builder: &ahash::RandomState) {
        for change_hash in self.0.get_heads() {
            change_hash.0.hash(h);
        }
    }
}
