//! High level models related to the map statement

use ontol_runtime::MapDef;

/// Key identifying a data mapping.
///
/// Order of input and output does not matter.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct UndirectedMapKey([MapDef; 2]);

impl UndirectedMapKey {
    pub fn new([a, b]: [MapDef; 2]) -> Self {
        if a < b {
            Self([a, b])
        } else {
            Self([b, a])
        }
    }

    pub fn first(&self) -> MapDef {
        self.0[0]
    }

    pub fn second(&self) -> MapDef {
        self.0[1]
    }
}

impl std::ops::Deref for UndirectedMapKey {
    type Target = [MapDef; 2];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
