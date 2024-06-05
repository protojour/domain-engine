use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{format_utils::AsAlpha, impl_ontol_debug};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CardinalIdx(pub u8);

impl_ontol_debug!(CardinalIdx);

impl Debug for CardinalIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", AsAlpha(self.0 as u32, 'A'))
    }
}

impl Display for CardinalIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", AsAlpha(self.0 as u32, 'A'))
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EndoTuple<T> {
    pub origin: CardinalIdx,
    pub elements: EndoTupleElements<T>,
}

pub type EndoTupleElements<T> = SmallVec<T, 1>;
