//! High level models related to the map statement

use ontol_runtime::MapKey;

/// Key identifying a data mapping
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct MapKeyPair([MapKey; 2]);

impl MapKeyPair {
    pub fn new([a, b]: [MapKey; 2]) -> Self {
        if a < b {
            Self([a, b])
        } else {
            Self([b, a])
        }
    }
}

impl std::ops::Deref for MapKeyPair {
    type Target = [MapKey; 2];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
