use std::collections::BTreeSet;

use crate::{DataModifier, DefId, DefVariant};

use self::operator::{MapType, SerdeOperatorId};

mod deserialize;
mod deserialize_matcher;
mod serialize;

pub mod operator;
pub mod processor;

const EDGE_PROPERTY: &str = "_edge";
const ID_PROPERTY: &str = "_id";

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SerdeKey {
    Def(DefVariant),
    Intersection(Box<BTreeSet<SerdeKey>>),
}

impl SerdeKey {
    pub const fn identity(def_id: DefId) -> Self {
        Self::Def(DefVariant::new(def_id, DataModifier::IDENTITY))
    }
}
