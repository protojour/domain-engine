//! High level models related to the map statement

use ontol_runtime::MapKey;

use crate::typed_hir::TypedHir;

/// Intermediate representation of one arm in a map statement
pub struct MapArm<'m> {
    pub node: ontol_hir::RootNode<'m, TypedHir>,
    pub class: MapOutputClass,
}

/// The class of a map arm when interpreted as the output of the mapping
pub enum MapOutputClass {
    /// The output is interpreted as a pure function of the opposing arm.
    /// All output data can be derived and computed from from input data.
    Pure,
    /// The output is interpreted as match on some entity storage, meant to produce one value.
    /// Requires some form of runtime datastore to be computed.
    FindMatch,
    /// The output is interpreted as a match on some entity store meant to produce several values.
    /// Requires some form of runtime datastore to be computed.
    FilterMatch,
}

/// Key identifying a data mapping
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct MapKeyPair {
    first: MapKey,
    second: MapKey,
}

impl MapKeyPair {
    pub fn new(a: MapKey, b: MapKey) -> Self {
        if a < b {
            Self {
                first: a,
                second: b,
            }
        } else {
            Self {
                first: b,
                second: a,
            }
        }
    }

    pub fn first(&self) -> MapKey {
        self.first
    }

    pub fn second(&self) -> MapKey {
        self.second
    }
}
